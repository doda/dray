package protocol

import (
	"context"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for OffsetFetch responses.
const (
	errOffsetFetchNone                  int16 = 0
	errOffsetFetchCoordinatorNotAvail   int16 = 15
	errOffsetFetchNotCoordinator        int16 = 16
	errOffsetFetchInvalidGroupID        int16 = 24
	errOffsetFetchGroupAuthFailed       int16 = 30
	errOffsetFetchUnstableOffsetCommit  int16 = 60
	errOffsetFetchGroupIDNotFound       int16 = 69
)

// OffsetFetchHandler handles OffsetFetch (key 9) requests.
type OffsetFetchHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
	enforcer     *auth.Enforcer
}

// NewOffsetFetchHandler creates a new OffsetFetch handler.
func NewOffsetFetchHandler(store *groups.Store, leaseManager *groups.LeaseManager) *OffsetFetchHandler {
	return &OffsetFetchHandler{
		store:        store,
		leaseManager: leaseManager,
	}
}

// WithEnforcer sets the ACL enforcer for this handler.
func (h *OffsetFetchHandler) WithEnforcer(enforcer *auth.Enforcer) *OffsetFetchHandler {
	h.enforcer = enforcer
	return h
}

// Handle processes an OffsetFetch request.
func (h *OffsetFetchHandler) Handle(ctx context.Context, version int16, req *kmsg.OffsetFetchRequest) *kmsg.OffsetFetchResponse {
	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.SetVersion(version)

	logger := logging.FromCtx(ctx)

	// Handle v8+ batched groups request
	if version >= 8 && len(req.Groups) > 0 {
		return h.handleBatchedRequest(ctx, version, req)
	}

	// Validate group ID (for non-batched requests)
	if req.Group == "" {
		resp.ErrorCode = errOffsetFetchInvalidGroupID
		return resp
	}

	// Check ACL before processing - need READ permission on group
	if h.enforcer != nil {
		if errCode := h.enforcer.AuthorizeGroupFromCtx(ctx, req.Group, auth.OperationRead); errCode != nil {
			return h.buildErrorResponse(version, req, *errCode)
		}
	}

	// Check coordinator lease if available
	if h.leaseManager != nil {
		result, err := h.leaseManager.AcquireLease(ctx, req.Group)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			resp.ErrorCode = errOffsetFetchCoordinatorNotAvail
			return resp
		}
		if !result.Acquired {
			resp.ErrorCode = errOffsetFetchNotCoordinator
			return resp
		}
	}

	// Determine whether to fetch all topics or specific topics
	// For v2+, a null/empty Topics list means fetch all topics for the group
	fetchAllTopics := version >= 2 && len(req.Topics) == 0

	var offsets []groups.CommittedOffset
	var err error

	if fetchAllTopics {
		// Fetch all committed offsets for the group
		offsets, err = h.store.ListCommittedOffsets(ctx, req.Group)
		if err != nil {
			logger.Warnf("failed to list committed offsets", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			resp.ErrorCode = errOffsetFetchCoordinatorNotAvail
			return resp
		}

		// Build response from all offsets
		resp.Topics = h.buildTopicsFromOffsets(offsets, version)
	} else {
		// Fetch specific topics and partitions
		resp.Topics = h.fetchSpecificTopics(ctx, req.Group, req.Topics, version)
	}

	// For v3+, include top-level error code and throttle time
	if version >= 3 {
		resp.ErrorCode = errOffsetFetchNone
		resp.ThrottleMillis = 0
	}

	return resp
}

// handleBatchedRequest handles v8+ batched groups requests.
func (h *OffsetFetchHandler) handleBatchedRequest(ctx context.Context, version int16, req *kmsg.OffsetFetchRequest) *kmsg.OffsetFetchResponse {
	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.SetVersion(version)
	resp.ThrottleMillis = 0

	logger := logging.FromCtx(ctx)

	for _, grp := range req.Groups {
		respGroup := kmsg.NewOffsetFetchResponseGroup()
		respGroup.Group = grp.Group

		if grp.Group == "" {
			respGroup.ErrorCode = errOffsetFetchInvalidGroupID
			resp.Groups = append(resp.Groups, respGroup)
			continue
		}

		// Check ACL before processing - need READ permission on group
		if h.enforcer != nil {
			if errCode := h.enforcer.AuthorizeGroupFromCtx(ctx, grp.Group, auth.OperationRead); errCode != nil {
				respGroup.ErrorCode = *errCode
				respGroup.Topics = h.buildGroupErrorTopics(version, grp.Topics, *errCode)
				resp.Groups = append(resp.Groups, respGroup)
				continue
			}
		}

		// Check coordinator lease if available
		if h.leaseManager != nil {
			result, err := h.leaseManager.AcquireLease(ctx, grp.Group)
			if err != nil {
				logger.Warnf("failed to acquire lease", map[string]any{
					"group": grp.Group,
					"error": err.Error(),
				})
				respGroup.ErrorCode = errOffsetFetchCoordinatorNotAvail
				resp.Groups = append(resp.Groups, respGroup)
				continue
			}
			if !result.Acquired {
				respGroup.ErrorCode = errOffsetFetchNotCoordinator
				resp.Groups = append(resp.Groups, respGroup)
				continue
			}
		}

		// Determine whether to fetch all topics
		fetchAllTopics := len(grp.Topics) == 0

		if fetchAllTopics {
			offsets, err := h.store.ListCommittedOffsets(ctx, grp.Group)
			if err != nil {
				logger.Warnf("failed to list committed offsets", map[string]any{
					"group": grp.Group,
					"error": err.Error(),
				})
				respGroup.ErrorCode = errOffsetFetchCoordinatorNotAvail
				resp.Groups = append(resp.Groups, respGroup)
				continue
			}
			respGroup.Topics = h.buildGroupTopicsFromOffsets(offsets, version)
		} else {
			respGroup.Topics = h.fetchSpecificGroupTopics(ctx, grp.Group, grp.Topics, version)
		}

		respGroup.ErrorCode = errOffsetFetchNone
		resp.Groups = append(resp.Groups, respGroup)
	}

	return resp
}

// fetchSpecificTopics fetches offsets for specific topics and partitions.
func (h *OffsetFetchHandler) fetchSpecificTopics(ctx context.Context, groupID string, topics []kmsg.OffsetFetchRequestTopic, version int16) []kmsg.OffsetFetchResponseTopic {
	var respTopics []kmsg.OffsetFetchResponseTopic

	for _, topic := range topics {
		respTopic := kmsg.NewOffsetFetchResponseTopic()
		respTopic.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			respPartition := kmsg.NewOffsetFetchResponseTopicPartition()
			respPartition.Partition = partition

			offset, err := h.store.GetCommittedOffset(ctx, groupID, topic.Topic, partition)
			if err != nil {
				// Error fetching offset - set to error state
				respPartition.Offset = -1
				respPartition.ErrorCode = errOffsetFetchCoordinatorNotAvail
			} else if offset == nil {
				// No committed offset for this partition
				respPartition.Offset = -1
				respPartition.ErrorCode = errOffsetFetchNone
				// For v5+, include leader epoch
				if version >= 5 {
					respPartition.LeaderEpoch = -1
				}
			} else {
				respPartition.Offset = offset.Offset
				respPartition.ErrorCode = errOffsetFetchNone
				if offset.Metadata != "" {
					respPartition.Metadata = &offset.Metadata
				}
				// For v5+, include leader epoch
				if version >= 5 {
					respPartition.LeaderEpoch = offset.LeaderEpoch
				}
			}

			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		respTopics = append(respTopics, respTopic)
	}

	return respTopics
}

// fetchSpecificGroupTopics fetches offsets for specific topics in a batched group request.
func (h *OffsetFetchHandler) fetchSpecificGroupTopics(ctx context.Context, groupID string, topics []kmsg.OffsetFetchRequestGroupTopic, version int16) []kmsg.OffsetFetchResponseGroupTopic {
	var respTopics []kmsg.OffsetFetchResponseGroupTopic

	for _, topic := range topics {
		respTopic := kmsg.NewOffsetFetchResponseGroupTopic()
		respTopic.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			respPartition := kmsg.NewOffsetFetchResponseGroupTopicPartition()
			respPartition.Partition = partition

			offset, err := h.store.GetCommittedOffset(ctx, groupID, topic.Topic, partition)
			if err != nil {
				respPartition.Offset = -1
				respPartition.ErrorCode = errOffsetFetchCoordinatorNotAvail
			} else if offset == nil {
				respPartition.Offset = -1
				respPartition.ErrorCode = errOffsetFetchNone
				respPartition.LeaderEpoch = -1
			} else {
				respPartition.Offset = offset.Offset
				respPartition.ErrorCode = errOffsetFetchNone
				if offset.Metadata != "" {
					respPartition.Metadata = &offset.Metadata
				}
				respPartition.LeaderEpoch = offset.LeaderEpoch
			}

			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		respTopics = append(respTopics, respTopic)
	}

	return respTopics
}

// buildTopicsFromOffsets constructs the response topics from a list of committed offsets.
func (h *OffsetFetchHandler) buildTopicsFromOffsets(offsets []groups.CommittedOffset, version int16) []kmsg.OffsetFetchResponseTopic {
	// Group offsets by topic
	topicOffsets := make(map[string][]groups.CommittedOffset)
	for _, offset := range offsets {
		topicOffsets[offset.Topic] = append(topicOffsets[offset.Topic], offset)
	}

	var respTopics []kmsg.OffsetFetchResponseTopic
	for topicName, offsets := range topicOffsets {
		respTopic := kmsg.NewOffsetFetchResponseTopic()
		respTopic.Topic = topicName

		for _, offset := range offsets {
			respPartition := kmsg.NewOffsetFetchResponseTopicPartition()
			respPartition.Partition = offset.Partition
			respPartition.Offset = offset.Offset
			respPartition.ErrorCode = errOffsetFetchNone
			if offset.Metadata != "" {
				respPartition.Metadata = &offset.Metadata
			}
			if version >= 5 {
				respPartition.LeaderEpoch = offset.LeaderEpoch
			}

			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		respTopics = append(respTopics, respTopic)
	}

	return respTopics
}

// buildGroupTopicsFromOffsets constructs the group response topics from a list of committed offsets.
func (h *OffsetFetchHandler) buildGroupTopicsFromOffsets(offsets []groups.CommittedOffset, version int16) []kmsg.OffsetFetchResponseGroupTopic {
	// Group offsets by topic
	topicOffsets := make(map[string][]groups.CommittedOffset)
	for _, offset := range offsets {
		topicOffsets[offset.Topic] = append(topicOffsets[offset.Topic], offset)
	}

	var respTopics []kmsg.OffsetFetchResponseGroupTopic
	for topicName, offsets := range topicOffsets {
		respTopic := kmsg.NewOffsetFetchResponseGroupTopic()
		respTopic.Topic = topicName

		for _, offset := range offsets {
			respPartition := kmsg.NewOffsetFetchResponseGroupTopicPartition()
			respPartition.Partition = offset.Partition
			respPartition.Offset = offset.Offset
			respPartition.ErrorCode = errOffsetFetchNone
			if offset.Metadata != "" {
				respPartition.Metadata = &offset.Metadata
			}
			respPartition.LeaderEpoch = offset.LeaderEpoch

			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		respTopics = append(respTopics, respTopic)
	}

	return respTopics
}

// buildErrorResponse creates an OffsetFetch response with all requested partitions having the same error.
func (h *OffsetFetchHandler) buildErrorResponse(version int16, req *kmsg.OffsetFetchRequest, errorCode int16) *kmsg.OffsetFetchResponse {
	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.SetVersion(version)
	resp.ErrorCode = errorCode
	if version >= 3 {
		resp.ThrottleMillis = 0
	}

	for _, topic := range req.Topics {
		respTopic := kmsg.NewOffsetFetchResponseTopic()
		respTopic.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			respPartition := kmsg.NewOffsetFetchResponseTopicPartition()
			respPartition.Partition = partition
			respPartition.Offset = -1
			respPartition.ErrorCode = errorCode
			if version >= 5 {
				respPartition.LeaderEpoch = -1
			}

			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		resp.Topics = append(resp.Topics, respTopic)
	}

	return resp
}

func (h *OffsetFetchHandler) buildGroupErrorTopics(version int16, topics []kmsg.OffsetFetchRequestGroupTopic, errorCode int16) []kmsg.OffsetFetchResponseGroupTopic {
	var respTopics []kmsg.OffsetFetchResponseGroupTopic

	for _, topic := range topics {
		respTopic := kmsg.NewOffsetFetchResponseGroupTopic()
		respTopic.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			respPartition := kmsg.NewOffsetFetchResponseGroupTopicPartition()
			respPartition.Partition = partition
			respPartition.Offset = -1
			respPartition.ErrorCode = errorCode
			respPartition.LeaderEpoch = -1

			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		respTopics = append(respTopics, respTopic)
	}

	return respTopics
}
