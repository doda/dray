package protocol

import (
	"context"
	"errors"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for Heartbeat responses.
const (
	errHeartbeatNone                  int16 = 0
	errHeartbeatCoordinatorNotAvail   int16 = 15
	errHeartbeatNotCoordinator        int16 = 16
	errHeartbeatIllegalGeneration     int16 = 22
	errHeartbeatUnknownMemberID       int16 = 25
	errHeartbeatRebalanceInProgress   int16 = 27
	errHeartbeatInvalidGroupID        int16 = 24
	errHeartbeatFencedInstanceID      int16 = 82
	errHeartbeatGroupAuthFailed       int16 = 30
)

// HeartbeatHandler handles Heartbeat (key 12) requests for classic consumer groups.
type HeartbeatHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
	enforcer     *auth.Enforcer
}

// NewHeartbeatHandler creates a new Heartbeat handler.
func NewHeartbeatHandler(store *groups.Store, leaseManager *groups.LeaseManager) *HeartbeatHandler {
	return &HeartbeatHandler{
		store:        store,
		leaseManager: leaseManager,
	}
}

// WithEnforcer sets the ACL enforcer for this handler.
func (h *HeartbeatHandler) WithEnforcer(enforcer *auth.Enforcer) *HeartbeatHandler {
	h.enforcer = enforcer
	return h
}

// Handle processes a Heartbeat request.
func (h *HeartbeatHandler) Handle(ctx context.Context, version int16, req *kmsg.HeartbeatRequest) *kmsg.HeartbeatResponse {
	resp := kmsg.NewPtrHeartbeatResponse()
	resp.SetVersion(version)

	logger := logging.FromCtx(ctx)

	// Validate group ID
	if req.Group == "" {
		resp.ErrorCode = errHeartbeatInvalidGroupID
		return resp
	}

	// Check ACL before processing - need READ permission on group
	if h.enforcer != nil {
		if errCode := h.enforcer.AuthorizeGroupFromCtx(ctx, req.Group, auth.OperationRead); errCode != nil {
			resp.ErrorCode = *errCode
			return resp
		}
	}

	// Validate member ID
	if req.MemberID == "" {
		resp.ErrorCode = errHeartbeatUnknownMemberID
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
			resp.ErrorCode = errHeartbeatCoordinatorNotAvail
			return resp
		}
		if !result.Acquired {
			resp.ErrorCode = errHeartbeatNotCoordinator
			return resp
		}
	}

	// Get group state
	state, err := h.store.GetGroupState(ctx, req.Group)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			resp.ErrorCode = errHeartbeatCoordinatorNotAvail
			return resp
		}
		logger.Warnf("failed to get group state", map[string]any{
			"group": req.Group,
			"error": err.Error(),
		})
		resp.ErrorCode = errHeartbeatCoordinatorNotAvail
		return resp
	}

	// Check if group is Dead
	if state.State == groups.GroupStateDead {
		resp.ErrorCode = errHeartbeatUnknownMemberID
		return resp
	}

	// Check generation - Heartbeat generation must match current group generation
	if req.Generation != state.Generation {
		resp.ErrorCode = errHeartbeatIllegalGeneration
		return resp
	}

	// Verify member exists in the group
	member, err := h.store.GetMember(ctx, req.Group, req.MemberID)
	if err != nil {
		if errors.Is(err, groups.ErrMemberNotFound) {
			resp.ErrorCode = errHeartbeatUnknownMemberID
			return resp
		}
		logger.Warnf("failed to get member", map[string]any{
			"group":  req.Group,
			"member": req.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errHeartbeatCoordinatorNotAvail
		return resp
	}

	// For v3+, validate instance ID if provided
	if version >= 3 && req.InstanceID != nil && *req.InstanceID != "" {
		if member.GroupInstanceID != "" && member.GroupInstanceID != *req.InstanceID {
			resp.ErrorCode = errHeartbeatFencedInstanceID
			return resp
		}
	}

	// Check if rebalance is in progress
	// PreparingRebalance means waiting for all members to join
	if state.State == groups.GroupStatePreparingRebalance {
		resp.ErrorCode = errHeartbeatRebalanceInProgress
		return resp
	}

	// Update member's last heartbeat time
	nowMs := time.Now().UnixMilli()
	err = h.store.UpdateMemberHeartbeat(ctx, req.Group, req.MemberID, nowMs)
	if err != nil {
		if errors.Is(err, groups.ErrMemberNotFound) {
			resp.ErrorCode = errHeartbeatUnknownMemberID
			return resp
		}
		logger.Warnf("failed to update heartbeat", map[string]any{
			"group":  req.Group,
			"member": req.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errHeartbeatCoordinatorNotAvail
		return resp
	}

	// Success
	resp.ErrorCode = errHeartbeatNone
	return resp
}
