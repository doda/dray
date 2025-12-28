package protocol

import (
	"context"
	"errors"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/topics"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KIP-848 error codes for ConsumerGroupHeartbeat responses.
const (
	errCGHBNone                     int16 = 0
	errCGHBCoordinatorNotAvailable  int16 = 15
	errCGHBNotCoordinator           int16 = 16
	errCGHBInvalidGroupID           int16 = 24
	errCGHBUnknownMemberID          int16 = 25
	errCGHBGroupAuthFailed          int16 = 30
	errCGHBFencedMemberEpoch        int16 = 110 // FENCED_MEMBER_EPOCH
	errCGHBUnreleasedInstanceID     int16 = 112 // UNRELEASED_INSTANCE_ID
	errCGHBUnsupportedAssignor      int16 = 113 // UNSUPPORTED_ASSIGNOR
	errCGHBMismatchedEndpointType   int16 = 114 // MISMATCHED_ENDPOINT_TYPE
	errCGHBGroupMaxSizeReached      int16 = 115 // GROUP_MAX_SIZE_REACHED
)

// Default server-driven configuration for KIP-848 consumer groups.
const (
	defaultHeartbeatIntervalMs = 5000  // 5 seconds
	defaultSessionTimeoutMs    = 45000 // 45 seconds
	defaultRebalanceTimeoutMs  = 60000 // 60 seconds
	maxGroupSize               = 1000  // maximum number of members in a group
)

// ConsumerGroupHeartbeatHandler handles ConsumerGroupHeartbeat (key 68) requests
// for KIP-848 consumer groups.
type ConsumerGroupHeartbeatHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
	topicStore   *topics.Store
}

// NewConsumerGroupHeartbeatHandler creates a new ConsumerGroupHeartbeat handler.
func NewConsumerGroupHeartbeatHandler(store *groups.Store, leaseManager *groups.LeaseManager, topicStore *topics.Store) *ConsumerGroupHeartbeatHandler {
	return &ConsumerGroupHeartbeatHandler{
		store:        store,
		leaseManager: leaseManager,
		topicStore:   topicStore,
	}
}

// Handle processes a ConsumerGroupHeartbeat request.
func (h *ConsumerGroupHeartbeatHandler) Handle(ctx context.Context, version int16, req *kmsg.ConsumerGroupHeartbeatRequest) *kmsg.ConsumerGroupHeartbeatResponse {
	resp := kmsg.NewPtrConsumerGroupHeartbeatResponse()
	resp.SetVersion(version)

	logger := logging.FromCtx(ctx)

	// Validate group ID
	if req.Group == "" {
		resp.ErrorCode = errCGHBInvalidGroupID
		return resp
	}

	nowMs := time.Now().UnixMilli()

	// Check coordinator lease
	if h.leaseManager != nil {
		result, err := h.leaseManager.AcquireLease(ctx, req.Group)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			resp.ErrorCode = errCGHBCoordinatorNotAvailable
			return resp
		}
		if !result.Acquired {
			resp.ErrorCode = errCGHBNotCoordinator
			return resp
		}
	}

	// Handle leave group: MemberEpoch == -1 means member is leaving
	if req.MemberEpoch == -1 {
		return h.handleLeaveGroup(ctx, req, resp, nowMs)
	}

	// Handle join (MemberID empty or MemberEpoch == 0)
	if req.MemberID == "" || req.MemberEpoch == 0 {
		return h.handleJoinGroup(ctx, req, resp, nowMs)
	}

	// Handle regular heartbeat
	return h.handleHeartbeat(ctx, req, resp, nowMs)
}

// handleJoinGroup handles a new member joining the group.
func (h *ConsumerGroupHeartbeatHandler) handleJoinGroup(ctx context.Context, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse, nowMs int64) *kmsg.ConsumerGroupHeartbeatResponse {
	logger := logging.FromCtx(ctx)

	// Check if group exists
	groupType, err := h.store.GetGroupType(ctx, req.Group)
	if err != nil && !errors.Is(err, groups.ErrGroupNotFound) {
		logger.Warnf("failed to get group type", map[string]any{
			"group": req.Group,
			"error": err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// If group exists, verify it's a consumer group (not classic)
	if err == nil && groupType != groups.GroupTypeConsumer {
		// Check if group is empty - if so, we can convert it
		members, listErr := h.store.ListMembers(ctx, req.Group)
		if listErr != nil {
			logger.Warnf("failed to list members for protocol conversion", map[string]any{
				"group": req.Group,
				"error": listErr.Error(),
			})
			resp.ErrorCode = errCGHBCoordinatorNotAvailable
			return resp
		}

		if len(members) > 0 {
			// Group has members using classic protocol - return MISMATCHED_ENDPOINT_TYPE
			logger.Warnf("consumer group heartbeat received for classic protocol group with active members", map[string]any{
				"group":       req.Group,
				"groupType":   groupType,
				"memberCount": len(members),
			})
			resp.ErrorCode = errCGHBMismatchedEndpointType
			errMsg := "group uses classic protocol and has active members; cannot use consumer protocol"
			resp.ErrorMessage = &errMsg
			return resp
		}

		// Group is empty - convert to consumer protocol
		logger.Infof("converting empty group from classic to consumer protocol", map[string]any{
			"group": req.Group,
		})
		if convertErr := h.store.ConvertGroupType(ctx, req.Group, groups.GroupTypeConsumer); convertErr != nil {
			logger.Warnf("failed to convert group type", map[string]any{
				"group": req.Group,
				"error": convertErr.Error(),
			})
			resp.ErrorCode = errCGHBCoordinatorNotAvailable
			return resp
		}
	}

	// Create group if it doesn't exist
	if errors.Is(err, groups.ErrGroupNotFound) {
		_, err = h.store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:          req.Group,
			Type:             groups.GroupTypeConsumer,
			ProtocolType:     "consumer",
			SessionTimeoutMs: defaultSessionTimeoutMs,
			NowMs:            nowMs,
		})
		if err != nil && !errors.Is(err, groups.ErrGroupExists) {
			logger.Warnf("failed to create group", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			resp.ErrorCode = errCGHBCoordinatorNotAvailable
			return resp
		}
	}

	// Check current member count
	members, err := h.store.ListMembers(ctx, req.Group)
	if err != nil {
		logger.Warnf("failed to list members", map[string]any{
			"group": req.Group,
			"error": err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// Check for instance ID conflicts (static membership)
	if req.InstanceID != nil && *req.InstanceID != "" {
		for _, m := range members {
			if m.GroupInstanceID == *req.InstanceID {
				// Same instance ID is re-joining, use existing member ID
				return h.handleRejoin(ctx, req, resp, &m, nowMs)
			}
		}
	}

	// Check max group size
	if len(members) >= maxGroupSize {
		resp.ErrorCode = errCGHBGroupMaxSizeReached
		return resp
	}

	// Validate assignor if specified
	if req.ServerAssignor != nil && *req.ServerAssignor != "" {
		assignor := *req.ServerAssignor
		if assignor != "uniform" && assignor != "range" {
			resp.ErrorCode = errCGHBUnsupportedAssignor
			return resp
		}
	}

	// Generate new member ID
	memberID := uuid.New().String()

	// Determine rebalance timeout
	rebalanceTimeoutMs := req.RebalanceTimeoutMillis
	if rebalanceTimeoutMs <= 0 {
		rebalanceTimeoutMs = defaultRebalanceTimeoutMs
	}

	// Extract rack ID
	var rackID string
	if req.RackID != nil {
		rackID = *req.RackID
	}

	// Add member with initial epoch of 1
	memberEpoch := int32(1)
	_, err = h.store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:            req.Group,
		MemberID:           memberID,
		ClientID:           "", // Will be set from request context if available
		ClientHost:         "", // Will be set from connection
		ProtocolType:       "consumer",
		SessionTimeoutMs:   defaultSessionTimeoutMs,
		RebalanceTimeoutMs: rebalanceTimeoutMs,
		GroupInstanceID:    ptrStringValue(req.InstanceID),
		NowMs:              nowMs,
		MemberEpoch:        memberEpoch,
		RackID:             rackID,
		SubscribedTopics:   req.SubscribedTopicNames,
	})
	if err != nil {
		logger.Warnf("failed to add member", map[string]any{
			"group":  req.Group,
			"member": memberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// Transition group to stable if it's empty
	state, err := h.store.GetGroupState(ctx, req.Group)
	if err == nil && state.State == groups.GroupStateEmpty {
		_, _ = h.store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      req.Group,
			FromState:    groups.GroupStateEmpty,
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
	}

	// Build success response
	resp.ErrorCode = errCGHBNone
	resp.MemberID = &memberID
	resp.MemberEpoch = memberEpoch
	resp.HeartbeatIntervalMillis = defaultHeartbeatIntervalMs

	// Build initial assignment (empty for new member, will be populated by assignor)
	resp.Assignment = h.buildAssignment(ctx, req.Group, memberID)

	return resp
}

// handleRejoin handles a static member rejoining with same instance ID.
func (h *ConsumerGroupHeartbeatHandler) handleRejoin(ctx context.Context, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse, existingMember *groups.GroupMember, nowMs int64) *kmsg.ConsumerGroupHeartbeatResponse {
	logger := logging.FromCtx(ctx)

	// Increment member epoch
	newEpoch := existingMember.MemberEpoch + 1

	// Extract rack ID
	var rackID string
	if req.RackID != nil {
		rackID = *req.RackID
	}

	// Update member
	updatedMember, err := h.store.UpdateConsumerMember(ctx, groups.UpdateConsumerMemberRequest{
		GroupID:          req.Group,
		MemberID:         existingMember.MemberID,
		MemberEpoch:      newEpoch,
		LastHeartbeatMs:  nowMs,
		SubscribedTopics: req.SubscribedTopicNames,
		RackID:           rackID,
	})
	if err != nil {
		logger.Warnf("failed to update member", map[string]any{
			"group":  req.Group,
			"member": existingMember.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// Build success response
	resp.ErrorCode = errCGHBNone
	resp.MemberID = &updatedMember.MemberID
	resp.MemberEpoch = updatedMember.MemberEpoch
	resp.HeartbeatIntervalMillis = defaultHeartbeatIntervalMs
	resp.Assignment = h.buildAssignment(ctx, req.Group, updatedMember.MemberID)

	return resp
}

// handleHeartbeat handles a regular heartbeat from an existing member.
func (h *ConsumerGroupHeartbeatHandler) handleHeartbeat(ctx context.Context, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse, nowMs int64) *kmsg.ConsumerGroupHeartbeatResponse {
	logger := logging.FromCtx(ctx)

	// Get existing member
	member, err := h.store.GetMember(ctx, req.Group, req.MemberID)
	if err != nil {
		if errors.Is(err, groups.ErrMemberNotFound) {
			resp.ErrorCode = errCGHBUnknownMemberID
			return resp
		}
		logger.Warnf("failed to get member", map[string]any{
			"group":  req.Group,
			"member": req.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// Validate member epoch
	// Member epoch 0 means the client is acknowledging a reconciliation
	if req.MemberEpoch > 0 && req.MemberEpoch != member.MemberEpoch {
		resp.ErrorCode = errCGHBFencedMemberEpoch
		resp.MemberEpoch = member.MemberEpoch
		return resp
	}

	// Check instance ID if static membership
	if req.InstanceID != nil && *req.InstanceID != "" {
		if member.GroupInstanceID != *req.InstanceID {
			resp.ErrorCode = errCGHBFencedMemberEpoch
			return resp
		}
	}

	// Check if subscription changed (triggers rebalance)
	subscriptionChanged := !stringSlicesEqual(member.SubscribedTopics, req.SubscribedTopicNames)

	// Determine if we need to bump the epoch
	newEpoch := member.MemberEpoch
	if subscriptionChanged && len(req.SubscribedTopicNames) > 0 {
		newEpoch = member.MemberEpoch + 1
	}

	// Extract rack ID
	var rackID string
	if req.RackID != nil {
		rackID = *req.RackID
	}

	// Update member
	var subscriptionUpdate []string
	if len(req.SubscribedTopicNames) > 0 {
		subscriptionUpdate = req.SubscribedTopicNames
	}
	_, err = h.store.UpdateConsumerMember(ctx, groups.UpdateConsumerMemberRequest{
		GroupID:          req.Group,
		MemberID:         req.MemberID,
		MemberEpoch:      newEpoch,
		LastHeartbeatMs:  nowMs,
		SubscribedTopics: subscriptionUpdate,
		RackID:           rackID,
	})
	if err != nil {
		logger.Warnf("failed to update member heartbeat", map[string]any{
			"group":  req.Group,
			"member": req.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// Build success response
	resp.ErrorCode = errCGHBNone
	resp.MemberID = &req.MemberID
	resp.MemberEpoch = newEpoch
	resp.HeartbeatIntervalMillis = defaultHeartbeatIntervalMs
	resp.Assignment = h.buildAssignment(ctx, req.Group, req.MemberID)

	return resp
}

// handleLeaveGroup handles a member leaving the group (MemberEpoch == -1).
func (h *ConsumerGroupHeartbeatHandler) handleLeaveGroup(ctx context.Context, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse, nowMs int64) *kmsg.ConsumerGroupHeartbeatResponse {
	logger := logging.FromCtx(ctx)

	// Validate member ID is provided when leaving
	if req.MemberID == "" {
		resp.ErrorCode = errCGHBUnknownMemberID
		return resp
	}

	// Verify member exists
	member, err := h.store.GetMember(ctx, req.Group, req.MemberID)
	if err != nil {
		if errors.Is(err, groups.ErrMemberNotFound) {
			// Member already gone, treat as success
			resp.ErrorCode = errCGHBNone
			resp.MemberEpoch = -1
			return resp
		}
		logger.Warnf("failed to get member", map[string]any{
			"group":  req.Group,
			"member": req.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// Check instance ID for static membership
	if req.InstanceID != nil && *req.InstanceID != "" {
		if member.GroupInstanceID != *req.InstanceID {
			resp.ErrorCode = errCGHBFencedMemberEpoch
			return resp
		}
	}

	// Remove member
	err = h.store.RemoveMember(ctx, req.Group, req.MemberID)
	if err != nil {
		logger.Warnf("failed to remove member", map[string]any{
			"group":  req.Group,
			"member": req.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errCGHBCoordinatorNotAvailable
		return resp
	}

	// Check if group is now empty and transition state
	members, _ := h.store.ListMembers(ctx, req.Group)
	if len(members) == 0 {
		_, _ = h.store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:   req.Group,
			ToState:   groups.GroupStateEmpty,
			NowMs:     nowMs,
		})
	}

	// Success - member epoch -1 confirms leave
	resp.ErrorCode = errCGHBNone
	resp.MemberEpoch = -1

	return resp
}

// buildAssignment builds the assignment for a member.
// It performs server-side assignment using the configured assignor.
func (h *ConsumerGroupHeartbeatHandler) buildAssignment(ctx context.Context, groupID, memberID string) *kmsg.ConsumerGroupHeartbeatResponseAssignment {
	// Topic store is required for server-side assignment
	if h.topicStore == nil {
		return nil
	}

	// Get all members to perform assignment
	members, err := h.store.ListMembers(ctx, groupID)
	if err != nil || len(members) == 0 {
		return nil
	}

	// Use range assignor by default (or uniform if preferred)
	// For KIP-848, the server performs the assignment
	assignor := groups.GetKIP848Assignor("range")
	if assignor == nil {
		return nil
	}

	// Perform server-side assignment
	assignments, err := assignor.Assign(ctx, members, h.topicStore)
	if err != nil {
		return nil
	}

	// Get the assignment for this specific member
	memberAssignment, ok := assignments[memberID]
	if !ok || len(memberAssignment) == 0 {
		return &kmsg.ConsumerGroupHeartbeatResponseAssignment{
			Topics: []kmsg.ConsumerGroupHeartbeatResponseAssignmentTopic{},
		}
	}

	// Build response assignment
	respAssignment := &kmsg.ConsumerGroupHeartbeatResponseAssignment{
		Topics: make([]kmsg.ConsumerGroupHeartbeatResponseAssignmentTopic, 0, len(memberAssignment)),
	}

	for _, a := range memberAssignment {
		respAssignment.Topics = append(respAssignment.Topics, kmsg.ConsumerGroupHeartbeatResponseAssignmentTopic{
			TopicID:    a.TopicID,
			Partitions: a.Partitions,
		})
	}

	return respAssignment
}

// Helper function to safely get string pointer value
func ptrStringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// stringSlicesEqual compares two string slices for equality.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
