package protocol

import (
	"context"
	"errors"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for OffsetCommit responses.
const (
	errOffsetCommitNone                  int16 = 0
	errOffsetCommitCoordinatorNotAvail   int16 = 15
	errOffsetCommitNotCoordinator        int16 = 16
	errOffsetCommitIllegalGeneration     int16 = 22
	errOffsetCommitInvalidGroupID        int16 = 24
	errOffsetCommitUnknownMemberID       int16 = 25
	errOffsetCommitRebalanceInProgress   int16 = 27
	errOffsetCommitGroupAuthFailed       int16 = 30
	errOffsetCommitUnknownTopicOrPart    int16 = 3
	errOffsetCommitFencedInstanceID      int16 = 82
	errOffsetCommitStaleMemberEpoch      int16 = 110
)

// OffsetCommitHandler handles OffsetCommit (key 8) requests.
type OffsetCommitHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
}

// NewOffsetCommitHandler creates a new OffsetCommit handler.
func NewOffsetCommitHandler(store *groups.Store, leaseManager *groups.LeaseManager) *OffsetCommitHandler {
	return &OffsetCommitHandler{
		store:        store,
		leaseManager: leaseManager,
	}
}

// Handle processes an OffsetCommit request.
func (h *OffsetCommitHandler) Handle(ctx context.Context, version int16, req *kmsg.OffsetCommitRequest) *kmsg.OffsetCommitResponse {
	resp := kmsg.NewPtrOffsetCommitResponse()
	resp.SetVersion(version)

	logger := logging.FromCtx(ctx)

	// Validate group ID
	if req.Group == "" {
		return h.buildErrorResponse(version, req, errOffsetCommitInvalidGroupID)
	}

	// Check coordinator lease if available
	if h.leaseManager != nil {
		result, err := h.leaseManager.AcquireLease(ctx, req.Group)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			return h.buildErrorResponse(version, req, errOffsetCommitCoordinatorNotAvail)
		}
		if !result.Acquired {
			return h.buildErrorResponse(version, req, errOffsetCommitNotCoordinator)
		}
	}

	// For v1+, we need to validate generation/member epoch and member if they are provided
	// Generation -1 and empty member ID means "simple" mode (storing offsets only, no group validation)
	simpleMode := req.Generation == -1 && req.MemberID == ""

	var groupState *groups.GroupState
	if !simpleMode && version >= 1 {
		// Validate member ID for v1+
		if req.MemberID == "" {
			return h.buildErrorResponse(version, req, errOffsetCommitUnknownMemberID)
		}

		// Determine group type for generation vs member epoch validation
		groupType, err := h.store.GetGroupType(ctx, req.Group)
		if err != nil {
			if errors.Is(err, groups.ErrGroupNotFound) {
				// For OffsetCommit, an unknown group is not an error - we can auto-create
				// But we need to verify the member exists if not in simple mode
				return h.buildErrorResponse(version, req, errOffsetCommitCoordinatorNotAvail)
			}
			logger.Warnf("failed to get group type", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			return h.buildErrorResponse(version, req, errOffsetCommitCoordinatorNotAvail)
		}

		if groupType == groups.GroupTypeConsumer {
			// Validate member epoch for consumer groups (KIP-848)
			member, err := h.store.GetMember(ctx, req.Group, req.MemberID)
			if err != nil {
				if errors.Is(err, groups.ErrMemberNotFound) {
					return h.buildErrorResponse(version, req, errOffsetCommitUnknownMemberID)
				}
				logger.Warnf("failed to get member", map[string]any{
					"group":  req.Group,
					"member": req.MemberID,
					"error":  err.Error(),
				})
				return h.buildErrorResponse(version, req, errOffsetCommitCoordinatorNotAvail)
			}

			if req.Generation != member.MemberEpoch {
				return h.buildErrorResponse(version, req, errOffsetCommitStaleMemberEpoch)
			}

			// For v7+, validate instance ID if provided
			if version >= 7 && req.InstanceID != nil && *req.InstanceID != "" {
				if member.GroupInstanceID != "" && member.GroupInstanceID != *req.InstanceID {
					return h.buildErrorResponse(version, req, errOffsetCommitFencedInstanceID)
				}
			}
		} else {
			// Get group state to validate generation for classic groups
			state, err := h.store.GetGroupState(ctx, req.Group)
			if err != nil {
				if errors.Is(err, groups.ErrGroupNotFound) {
					// For OffsetCommit, an unknown group is not an error - we can auto-create
					// But we need to verify the member exists if not in simple mode
					return h.buildErrorResponse(version, req, errOffsetCommitCoordinatorNotAvail)
				}
				logger.Warnf("failed to get group state", map[string]any{
					"group": req.Group,
					"error": err.Error(),
				})
				return h.buildErrorResponse(version, req, errOffsetCommitCoordinatorNotAvail)
			}
			groupState = state

			// Check if group is Dead
			if state.State == groups.GroupStateDead {
				return h.buildErrorResponse(version, req, errOffsetCommitUnknownMemberID)
			}

			// Validate generation - must match current group generation
			if req.Generation != state.Generation {
				return h.buildErrorResponse(version, req, errOffsetCommitIllegalGeneration)
			}

			// Verify member exists in the group
			member, err := h.store.GetMember(ctx, req.Group, req.MemberID)
			if err != nil {
				if errors.Is(err, groups.ErrMemberNotFound) {
					return h.buildErrorResponse(version, req, errOffsetCommitUnknownMemberID)
				}
				logger.Warnf("failed to get member", map[string]any{
					"group":  req.Group,
					"member": req.MemberID,
					"error":  err.Error(),
				})
				return h.buildErrorResponse(version, req, errOffsetCommitCoordinatorNotAvail)
			}

			// For v7+, validate instance ID if provided
			if version >= 7 && req.InstanceID != nil && *req.InstanceID != "" {
				if member.GroupInstanceID != "" && member.GroupInstanceID != *req.InstanceID {
					return h.buildErrorResponse(version, req, errOffsetCommitFencedInstanceID)
				}
			}

			// Check if rebalance is in progress
			if state.State == groups.GroupStatePreparingRebalance || state.State == groups.GroupStateCompletingRebalance {
				return h.buildErrorResponse(version, req, errOffsetCommitRebalanceInProgress)
			}
		}
	}

	// Process the offset commits
	nowMs := time.Now().UnixMilli()

	// Retention time handling:
	// - v2-v4: use RetentionTimeMillis from request (-1 means default)
	// - v5+: retention is controlled by broker config only, ignore request value
	retentionMs := int64(-1)
	if version >= 2 && version <= 4 && req.RetentionTimeMillis > 0 {
		retentionMs = req.RetentionTimeMillis
	}

	// Build response topics
	for _, topic := range req.Topics {
		respTopic := kmsg.NewOffsetCommitResponseTopic()
		respTopic.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			respPartition := kmsg.NewOffsetCommitResponseTopicPartition()
			respPartition.Partition = partition.Partition

			// Get leader epoch (v6+)
			leaderEpoch := int32(-1)
			if version >= 6 {
				leaderEpoch = partition.LeaderEpoch
			}

			// Get metadata (nullable string)
			metadata := ""
			if partition.Metadata != nil {
				metadata = *partition.Metadata
			}

			// Commit the offset
			_, err := h.store.CommitOffset(ctx, groups.CommitOffsetRequest{
				GroupID:         req.Group,
				Topic:           topic.Topic,
				Partition:       partition.Partition,
				Offset:          partition.Offset,
				LeaderEpoch:     leaderEpoch,
				Metadata:        metadata,
				RetentionTimeMs: retentionMs,
				NowMs:           nowMs,
			})

			if err != nil {
				logger.Warnf("failed to commit offset", map[string]any{
					"group":     req.Group,
					"topic":     topic.Topic,
					"partition": partition.Partition,
					"error":     err.Error(),
				})
				respPartition.ErrorCode = errOffsetCommitCoordinatorNotAvail
			} else {
				respPartition.ErrorCode = errOffsetCommitNone
			}

			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		resp.Topics = append(resp.Topics, respTopic)
	}

	// Update member heartbeat if we validated it
	if groupState != nil && req.MemberID != "" {
		if err := h.store.UpdateMemberHeartbeat(ctx, req.Group, req.MemberID, nowMs); err != nil {
			// Log but don't fail the commit
			logger.Warnf("failed to update heartbeat after offset commit", map[string]any{
				"group":  req.Group,
				"member": req.MemberID,
				"error":  err.Error(),
			})
		}
	}

	return resp
}

// buildErrorResponse creates an OffsetCommit response with all partitions having the same error.
func (h *OffsetCommitHandler) buildErrorResponse(version int16, req *kmsg.OffsetCommitRequest, errorCode int16) *kmsg.OffsetCommitResponse {
	resp := kmsg.NewPtrOffsetCommitResponse()
	resp.SetVersion(version)

	for _, topic := range req.Topics {
		respTopic := kmsg.NewOffsetCommitResponseTopic()
		respTopic.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			respPartition := kmsg.NewOffsetCommitResponseTopicPartition()
			respPartition.Partition = partition.Partition
			respPartition.ErrorCode = errorCode
			respTopic.Partitions = append(respTopic.Partitions, respPartition)
		}

		resp.Topics = append(resp.Topics, respTopic)
	}

	return resp
}
