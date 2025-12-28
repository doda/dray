package protocol

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for ConsumerGroupDescribe responses.
const (
	errCGDNone                   int16 = 0
	errCGDCoordinatorNotAvail    int16 = 15
	errCGDNotCoordinator         int16 = 16
	errCGDInvalidGroupID         int16 = 24
	errCGDGroupAuthFailed        int16 = 30
	errCGDGroupIDNotFound        int16 = 69 // KIP-1043 GROUP_ID_NOT_FOUND
)

// ConsumerGroupDescribeHandler handles ConsumerGroupDescribe (key 69) requests
// for KIP-848 consumer groups.
type ConsumerGroupDescribeHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
}

// NewConsumerGroupDescribeHandler creates a new ConsumerGroupDescribe handler.
func NewConsumerGroupDescribeHandler(store *groups.Store, leaseManager *groups.LeaseManager) *ConsumerGroupDescribeHandler {
	return &ConsumerGroupDescribeHandler{
		store:        store,
		leaseManager: leaseManager,
	}
}

// Handle processes a ConsumerGroupDescribe request.
func (h *ConsumerGroupDescribeHandler) Handle(ctx context.Context, version int16, req *kmsg.ConsumerGroupDescribeRequest) *kmsg.ConsumerGroupDescribeResponse {
	resp := kmsg.NewPtrConsumerGroupDescribeResponse()
	resp.SetVersion(version)
	resp.ThrottleMillis = 0

	logger := logging.FromCtx(ctx)

	resp.Groups = make([]kmsg.ConsumerGroupDescribeResponseGroup, len(req.Groups))
	for i, groupID := range req.Groups {
		resp.Groups[i] = h.describeGroup(ctx, version, groupID, req.IncludeAuthorizedOperations, logger)
	}

	return resp
}

// describeGroup describes a single KIP-848 consumer group.
func (h *ConsumerGroupDescribeHandler) describeGroup(ctx context.Context, version int16, groupID string, includeAuthOps bool, logger *logging.Logger) kmsg.ConsumerGroupDescribeResponseGroup {
	group := kmsg.NewConsumerGroupDescribeResponseGroup()
	group.Group = groupID
	group.AuthorizedOperations = -2147483648 // Default value per Kafka protocol

	if groupID == "" {
		group.ErrorCode = errCGDInvalidGroupID
		return group
	}

	// Check coordinator lease if available
	if h.leaseManager != nil {
		result, err := h.leaseManager.AcquireLease(ctx, groupID)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": groupID,
				"error": err.Error(),
			})
			group.ErrorCode = errCGDCoordinatorNotAvail
			return group
		}
		if !result.Acquired {
			group.ErrorCode = errCGDNotCoordinator
			return group
		}
	}

	// Get group type to verify it's a consumer group
	groupType, err := h.store.GetGroupType(ctx, groupID)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			group.ErrorCode = errCGDGroupIDNotFound
			errMsg := "Group does not exist"
			group.ErrorMessage = &errMsg
			return group
		}
		logger.Warnf("failed to get group type", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		group.ErrorCode = errCGDCoordinatorNotAvail
		return group
	}

	// Check that this is a KIP-848 consumer group, not a classic group
	if groupType != groups.GroupTypeConsumer {
		group.ErrorCode = errCGDGroupIDNotFound
		errMsg := "Group uses classic protocol, not consumer protocol"
		group.ErrorMessage = &errMsg
		return group
	}

	// Get group state
	state, err := h.store.GetGroupState(ctx, groupID)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			group.ErrorCode = errCGDGroupIDNotFound
			errMsg := "Group does not exist"
			group.ErrorMessage = &errMsg
			return group
		}
		logger.Warnf("failed to get group state", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		group.ErrorCode = errCGDCoordinatorNotAvail
		return group
	}

	// Set group-level fields
	group.State = string(state.State)
	group.Epoch = state.Generation
	group.AssignmentEpoch = state.LeaderEpoch
	group.AssignorName = state.Protocol
	if group.AssignorName == "" {
		group.AssignorName = "uniform" // default KIP-848 assignor
	}

	// Get members
	members, err := h.store.ListMembers(ctx, groupID)
	if err != nil {
		logger.Warnf("failed to list members", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		group.ErrorCode = errCGDCoordinatorNotAvail
		return group
	}

	// Get assignments for all members
	assignmentMap := make(map[string]*groups.Assignment)
	for _, m := range members {
		a, err := h.store.GetAssignment(ctx, groupID, m.MemberID)
		if err == nil && a != nil {
			assignmentMap[m.MemberID] = a
		}
	}

	// Build member responses
	group.Members = make([]kmsg.ConsumerGroupDescribeResponseGroupMember, len(members))
	for i, m := range members {
		group.Members[i] = h.buildMemberResponse(m, assignmentMap[m.MemberID])
	}

	// Include authorized operations if requested
	if includeAuthOps {
		// Return READ (bit 8) and DESCRIBE (bit 10) operations
		// 0x400 | 0x100 = 0x500 = 1280
		group.AuthorizedOperations = 0x500
	}

	group.ErrorCode = errCGDNone
	return group
}

// buildMemberResponse builds a single member response.
func (h *ConsumerGroupDescribeHandler) buildMemberResponse(m groups.GroupMember, assignment *groups.Assignment) kmsg.ConsumerGroupDescribeResponseGroupMember {
	member := kmsg.NewConsumerGroupDescribeResponseGroupMember()
	member.MemberID = m.MemberID
	member.MemberEpoch = m.MemberEpoch
	member.ClientID = m.ClientID
	member.ClientHost = m.ClientHost
	member.SubscribedTopics = m.SubscribedTopics

	// Set instance ID if present (static membership)
	if m.GroupInstanceID != "" {
		member.InstanceID = &m.GroupInstanceID
	}

	// Set rack ID if present
	if m.RackID != "" {
		member.RackID = &m.RackID
	}

	// Build assignment if present
	if assignment != nil && len(assignment.Data) > 0 {
		member.Assignment = parseAssignmentData(assignment.Data)
	}

	// For KIP-848, MemberType defaults to 0 (REGULAR)
	member.MemberType = 0

	return member
}

// parseAssignmentData converts stored assignment bytes to kmsg.Assignment.
// The stored format follows the classic consumer protocol encoding.
func parseAssignmentData(data []byte) kmsg.Assignment {
	result := kmsg.NewAssignment()
	if len(data) < 2 {
		return result
	}

	// The assignment data is encoded in the classic consumer protocol format:
	// Version: int16
	// NumTopics: int32
	// For each topic:
	//   TopicName: string (int16 len + bytes)
	//   NumPartitions: int32
	//   Partitions: []int32
	// UserData: bytes (int32 len + bytes)

	offset := 0

	// Skip version
	if len(data) < offset+2 {
		return result
	}
	offset += 2

	// Read number of topics
	if len(data) < offset+4 {
		return result
	}
	numTopics := int32(data[offset])<<24 | int32(data[offset+1])<<16 | int32(data[offset+2])<<8 | int32(data[offset+3])
	offset += 4

	for i := int32(0); i < numTopics && offset < len(data); i++ {
		// Read topic name length
		if len(data) < offset+2 {
			break
		}
		topicLen := int(data[offset])<<8 | int(data[offset+1])
		offset += 2

		if topicLen < 0 || len(data) < offset+topicLen {
			break
		}
		topicName := string(data[offset : offset+topicLen])
		offset += topicLen

		// Read number of partitions
		if len(data) < offset+4 {
			break
		}
		numPartitions := int32(data[offset])<<24 | int32(data[offset+1])<<16 | int32(data[offset+2])<<8 | int32(data[offset+3])
		offset += 4

		partitions := make([]int32, 0, numPartitions)
		for j := int32(0); j < numPartitions && len(data) >= offset+4; j++ {
			partition := int32(data[offset])<<24 | int32(data[offset+1])<<16 | int32(data[offset+2])<<8 | int32(data[offset+3])
			partitions = append(partitions, partition)
			offset += 4
		}

		tp := kmsg.NewAssignmentTopicPartition()
		tp.Topic = topicName
		tp.Partitions = partitions
		result.TopicPartitions = append(result.TopicPartitions, tp)
	}

	return result
}
