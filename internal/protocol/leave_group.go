package protocol

import (
	"context"
	"errors"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for LeaveGroup responses.
const (
	errLeaveGroupNone                int16 = 0
	errLeaveGroupCoordinatorNotAvail int16 = 15
	errLeaveGroupNotCoordinator      int16 = 16
	errLeaveGroupUnknownMemberID     int16 = 25
	errLeaveGroupInvalidGroupID      int16 = 24
	errLeaveGroupGroupAuthFailed     int16 = 30
	errLeaveGroupFencedInstanceID    int16 = 82
)

// LeaveGroupHandler handles LeaveGroup (key 13) requests for classic consumer groups.
type LeaveGroupHandler struct {
	store            *groups.Store
	leaseManager     *groups.LeaseManager
	joinGroupHandler *JoinGroupHandler
}

// NewLeaveGroupHandler creates a new LeaveGroup handler.
func NewLeaveGroupHandler(store *groups.Store, leaseManager *groups.LeaseManager, joinGroupHandler *JoinGroupHandler) *LeaveGroupHandler {
	return &LeaveGroupHandler{
		store:            store,
		leaseManager:     leaseManager,
		joinGroupHandler: joinGroupHandler,
	}
}

// Handle processes a LeaveGroup request.
func (h *LeaveGroupHandler) Handle(ctx context.Context, version int16, req *kmsg.LeaveGroupRequest) *kmsg.LeaveGroupResponse {
	resp := kmsg.NewPtrLeaveGroupResponse()
	resp.SetVersion(version)

	logger := logging.FromCtx(ctx)

	// Validate group ID
	if req.Group == "" {
		resp.ErrorCode = errLeaveGroupInvalidGroupID
		return resp
	}

	// Check coordinator lease if available
	if h.leaseManager != nil {
		result, err := h.leaseManager.AcquireLease(ctx, req.Group)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			resp.ErrorCode = errLeaveGroupCoordinatorNotAvail
			return resp
		}
		if !result.Acquired {
			resp.ErrorCode = errLeaveGroupNotCoordinator
			return resp
		}
	}

	// Get group state
	state, err := h.store.GetGroupState(ctx, req.Group)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			resp.ErrorCode = errLeaveGroupCoordinatorNotAvail
			return resp
		}
		logger.Warnf("failed to get group state", map[string]any{
			"group": req.Group,
			"error": err.Error(),
		})
		resp.ErrorCode = errLeaveGroupCoordinatorNotAvail
		return resp
	}

	// Check if group is Dead
	if state.State == groups.GroupStateDead {
		resp.ErrorCode = errLeaveGroupUnknownMemberID
		return resp
	}

	// Handle v0-v2 (single member) vs v3+ (batched members)
	var membersToLeave []memberLeaveRequest
	if version < 3 {
		// v0-v2: single member in request
		if req.MemberID == "" {
			resp.ErrorCode = errLeaveGroupUnknownMemberID
			return resp
		}
		membersToLeave = []memberLeaveRequest{{
			memberID:   req.MemberID,
			instanceID: nil,
		}}
	} else {
		// v3+: batched members
		membersToLeave = make([]memberLeaveRequest, len(req.Members))
		for i, m := range req.Members {
			membersToLeave[i] = memberLeaveRequest{
				memberID:   m.MemberID,
				instanceID: m.InstanceID,
			}
		}
	}

	// Process each member
	results := make([]memberLeaveResult, len(membersToLeave))
	anyRemoved := false

	for i, m := range membersToLeave {
		results[i] = h.processMemberLeave(ctx, req.Group, m, state)
		if results[i].errorCode == errLeaveGroupNone {
			anyRemoved = true
		}
	}

	// If any members were successfully removed, trigger a rebalance
	if anyRemoved {
		h.triggerRebalance(ctx, req.Group, logger)
	}

	// Build response
	if version < 3 {
		// v0-v2: single error code for the whole response
		if len(results) > 0 {
			resp.ErrorCode = results[0].errorCode
		}
	} else {
		// v3+: per-member results
		resp.ErrorCode = errLeaveGroupNone
		resp.Members = make([]kmsg.LeaveGroupResponseMember, len(results))
		for i, r := range results {
			member := kmsg.NewLeaveGroupResponseMember()
			member.MemberID = membersToLeave[i].memberID
			member.InstanceID = membersToLeave[i].instanceID
			member.ErrorCode = r.errorCode
			resp.Members[i] = member
		}
	}

	return resp
}

// memberLeaveRequest represents a member to leave.
type memberLeaveRequest struct {
	memberID   string
	instanceID *string
}

// memberLeaveResult represents the result of a member leave.
type memberLeaveResult struct {
	errorCode int16
}

// processMemberLeave processes a single member leaving the group.
func (h *LeaveGroupHandler) processMemberLeave(ctx context.Context, groupID string, req memberLeaveRequest, state *groups.GroupState) memberLeaveResult {
	logger := logging.FromCtx(ctx)

	// Validate member ID
	if req.memberID == "" {
		return memberLeaveResult{errorCode: errLeaveGroupUnknownMemberID}
	}

	// Verify member exists
	member, err := h.store.GetMember(ctx, groupID, req.memberID)
	if err != nil {
		if errors.Is(err, groups.ErrMemberNotFound) {
			return memberLeaveResult{errorCode: errLeaveGroupUnknownMemberID}
		}
		logger.Warnf("failed to get member", map[string]any{
			"group":  groupID,
			"member": req.memberID,
			"error":  err.Error(),
		})
		return memberLeaveResult{errorCode: errLeaveGroupCoordinatorNotAvail}
	}

	// If instance ID is provided, validate it matches
	if req.instanceID != nil && *req.instanceID != "" {
		if member.GroupInstanceID == "" {
			// Member doesn't have an instance ID but one was provided
			return memberLeaveResult{errorCode: errLeaveGroupUnknownMemberID}
		}
		if member.GroupInstanceID != *req.instanceID {
			// Instance ID mismatch - this could be a fencing situation
			return memberLeaveResult{errorCode: errLeaveGroupFencedInstanceID}
		}
	}

	// Remove the member (this also deletes the assignment)
	err = h.store.RemoveMember(ctx, groupID, req.memberID)
	if err != nil {
		logger.Warnf("failed to remove member", map[string]any{
			"group":  groupID,
			"member": req.memberID,
			"error":  err.Error(),
		})
		return memberLeaveResult{errorCode: errLeaveGroupCoordinatorNotAvail}
	}

	logger.Infof("member left group", map[string]any{
		"group":  groupID,
		"member": req.memberID,
	})

	return memberLeaveResult{errorCode: errLeaveGroupNone}
}

// triggerRebalance triggers a rebalance for the group.
func (h *LeaveGroupHandler) triggerRebalance(ctx context.Context, groupID string, logger *logging.Logger) {
	nowMs := time.Now().UnixMilli()

	// Check if there are any remaining members
	members, err := h.store.ListMembers(ctx, groupID)
	if err != nil {
		logger.Warnf("failed to list members for rebalance", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		return
	}

	if len(members) == 0 {
		// No more members - transition to Empty state
		_, err = h.store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:          groupID,
			ToState:          groups.GroupStateEmpty,
			ClearAssignments: true,
			Leader:           "",
			NowMs:            nowMs,
		})
		if err != nil {
			logger.Warnf("failed to transition to empty state", map[string]any{
				"group": groupID,
				"error": err.Error(),
			})
		}
		return
	}

	// Transition to PreparingRebalance state
	_, err = h.store.TransitionState(ctx, groups.TransitionStateRequest{
		GroupID:          groupID,
		ToState:          groups.GroupStatePreparingRebalance,
		ClearAssignments: true,
		NowMs:            nowMs,
	})
	if err != nil {
		logger.Warnf("failed to trigger rebalance", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
	}
}
