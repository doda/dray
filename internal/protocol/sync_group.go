package protocol

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for SyncGroup responses.
const (
	errSyncGroupNone                  int16 = 0
	errSyncGroupCoordinatorNotAvail   int16 = 15
	errSyncGroupNotCoordinator        int16 = 16
	errSyncGroupIllegalGeneration     int16 = 22
	errSyncGroupUnknownMemberID       int16 = 25
	errSyncGroupRebalanceInProgress   int16 = 27
	errSyncGroupCoordinatorLoadInProg int16 = 14
	errSyncGroupFencedInstanceID      int16 = 82
)

var errSyncTimeout = errors.New("sync group: timeout waiting for leader assignment")

// pendingSync tracks a member waiting for synchronization to complete.
type pendingSync struct {
	ch         chan *kmsg.SyncGroupResponse
	memberID   string
	instanceID string
}

// syncState tracks the synchronization phase for a group.
type syncState struct {
	mu          sync.Mutex
	pending     map[string]*pendingSync // memberID -> pending sync
	assignments map[string][]byte       // memberID -> assignment data (from leader)
	generation  int32
	leaderID    string
	timer       *time.Timer
	received    bool // true if leader's assignments have been received
}

// SyncGroupHandler handles SyncGroup (key 14) requests for classic consumer groups.
type SyncGroupHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
	enforcer     *auth.Enforcer

	mu         sync.Mutex
	syncStates map[string]*syncState // groupID -> state
}

// NewSyncGroupHandler creates a new SyncGroup handler.
func NewSyncGroupHandler(store *groups.Store, leaseManager *groups.LeaseManager) *SyncGroupHandler {
	return &SyncGroupHandler{
		store:        store,
		leaseManager: leaseManager,
		syncStates:   make(map[string]*syncState),
	}
}

// WithEnforcer sets the ACL enforcer for this handler.
func (h *SyncGroupHandler) WithEnforcer(enforcer *auth.Enforcer) *SyncGroupHandler {
	h.enforcer = enforcer
	return h
}

// Handle processes a SyncGroup request.
func (h *SyncGroupHandler) Handle(ctx context.Context, version int16, req *kmsg.SyncGroupRequest) *kmsg.SyncGroupResponse {
	resp := kmsg.NewPtrSyncGroupResponse()
	resp.SetVersion(version)
	resp.MemberAssignment = nil

	logger := logging.FromCtx(ctx)

	// Validate group ID
	if req.Group == "" {
		resp.ErrorCode = errInvalidGroupID
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
		resp.ErrorCode = errSyncGroupUnknownMemberID
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
			resp.ErrorCode = errSyncGroupCoordinatorNotAvail
			return resp
		}
		if !result.Acquired {
			resp.ErrorCode = errSyncGroupNotCoordinator
			return resp
		}
	}

	// Get group state
	state, err := h.store.GetGroupState(ctx, req.Group)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			resp.ErrorCode = errSyncGroupCoordinatorNotAvail
			return resp
		}
		logger.Warnf("failed to get group state", map[string]any{
			"group": req.Group,
			"error": err.Error(),
		})
		resp.ErrorCode = errSyncGroupCoordinatorNotAvail
		return resp
	}

	// Validate generation
	if req.Generation != state.Generation {
		resp.ErrorCode = errSyncGroupIllegalGeneration
		return resp
	}

	// Validate group state - SyncGroup is only valid during CompletingRebalance or Stable
	if state.State != groups.GroupStateCompletingRebalance && state.State != groups.GroupStateStable {
		resp.ErrorCode = errSyncGroupRebalanceInProgress
		return resp
	}

	// Verify member exists in the group
	member, err := h.store.GetMember(ctx, req.Group, req.MemberID)
	if err != nil {
		if errors.Is(err, groups.ErrMemberNotFound) {
			resp.ErrorCode = errSyncGroupUnknownMemberID
			return resp
		}
		logger.Warnf("failed to get member", map[string]any{
			"group":  req.Group,
			"member": req.MemberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errSyncGroupCoordinatorNotAvail
		return resp
	}

	// For v3+, validate instance ID if provided
	if version >= 3 && req.InstanceID != nil && *req.InstanceID != "" {
		if member.GroupInstanceID != "" && member.GroupInstanceID != *req.InstanceID {
			resp.ErrorCode = errSyncGroupFencedInstanceID
			return resp
		}
	}

	// Update member's heartbeat
	nowMs := time.Now().UnixMilli()
	_ = h.store.UpdateMemberHeartbeat(ctx, req.Group, req.MemberID, nowMs)

	isLeader := req.MemberID == state.Leader

	// If the group is already stable, return stored assignment if available.
	if state.State == groups.GroupStateStable {
		assignment, err := h.store.GetAssignment(ctx, req.Group, req.MemberID)
		if err != nil {
			logger.Warnf("failed to get assignment", map[string]any{
				"group":  req.Group,
				"member": req.MemberID,
				"error":  err.Error(),
			})
			resp.ErrorCode = errSyncGroupCoordinatorNotAvail
			return resp
		}
		if assignment != nil && assignment.Generation == req.Generation {
			resp := kmsg.NewPtrSyncGroupResponse()
			resp.SetVersion(version)
			resp.ErrorCode = errSyncGroupNone
			resp.MemberAssignment = assignment.Data
			return resp
		}
	}

	// Process the request based on whether this is the leader or a follower
	return h.handleSync(ctx, req.Group, req.MemberID, isLeader, req.Generation, req.GroupAssignment, version, state.SessionTimeoutMs)
}

// handleSync manages the sync phase for a member.
func (h *SyncGroupHandler) handleSync(
	ctx context.Context,
	groupID, memberID string,
	isLeader bool,
	generation int32,
	assignments []kmsg.SyncGroupRequestGroupAssignment,
	version int16,
	sessionTimeoutMs int32,
) *kmsg.SyncGroupResponse {
	h.mu.Lock()
	ss, exists := h.syncStates[groupID]
	if !exists || ss.generation != generation {
		// Create new sync state for this generation
		ss = &syncState{
			pending:     make(map[string]*pendingSync),
			assignments: make(map[string][]byte),
			generation:  generation,
		}
		h.syncStates[groupID] = ss
	}
	h.mu.Unlock()

	ss.mu.Lock()

	// If leader, store the assignments
	if isLeader && len(assignments) > 0 {
		for _, a := range assignments {
			ss.assignments[a.MemberID] = a.MemberAssignment
		}
		ss.leaderID = memberID
		ss.received = true

		// Immediately distribute to any waiting members
		h.distributeAssignments(ctx, groupID, ss, version)
	}

	// Check if assignments are already available for this member
	if ss.received {
		if assignment, ok := ss.assignments[memberID]; ok {
			ss.mu.Unlock()

			// Store assignment in metadata
			h.storeAssignment(ctx, groupID, memberID, generation, assignment)

			// Transition group to Stable state if needed
			h.transitionToStable(ctx, groupID, generation)

			resp := kmsg.NewPtrSyncGroupResponse()
			resp.SetVersion(version)
			resp.ErrorCode = errSyncGroupNone
			resp.MemberAssignment = assignment
			return resp
		}

		// Leader sent assignments but this member wasn't included
		// This shouldn't happen in a correct implementation
		ss.mu.Unlock()
		resp := kmsg.NewPtrSyncGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errSyncGroupUnknownMemberID
		return resp
	}

	// Leader hasn't sent assignments yet - wait for them
	ch := make(chan *kmsg.SyncGroupResponse, 1)
	ss.pending[memberID] = &pendingSync{
		ch:       ch,
		memberID: memberID,
	}

	// Start timeout timer if this is the first waiter
	if len(ss.pending) == 1 && ss.timer == nil {
		timeout := time.Duration(sessionTimeoutMs) * time.Millisecond
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		syncCtx := context.WithoutCancel(ctx)
		ss.timer = time.AfterFunc(timeout, func() {
			h.handleSyncTimeout(syncCtx, groupID, generation, version)
		})
	}

	ss.mu.Unlock()

	// Wait for assignments or context cancellation
	select {
	case resp := <-ch:
		return resp
	case <-ctx.Done():
		// Remove from pending
		h.mu.Lock()
		if ss, ok := h.syncStates[groupID]; ok {
			ss.mu.Lock()
			delete(ss.pending, memberID)
			ss.mu.Unlock()
		}
		h.mu.Unlock()

		resp := kmsg.NewPtrSyncGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errSyncGroupRebalanceInProgress
		return resp
	}
}

// distributeAssignments sends assignments to all waiting members.
func (h *SyncGroupHandler) distributeAssignments(ctx context.Context, groupID string, ss *syncState, version int16) {
	for memberID, ps := range ss.pending {
		assignment, ok := ss.assignments[memberID]
		if !ok {
			// Member not in assignment - shouldn't happen
			resp := kmsg.NewPtrSyncGroupResponse()
			resp.SetVersion(version)
			resp.ErrorCode = errSyncGroupUnknownMemberID
			select {
			case ps.ch <- resp:
			default:
			}
			continue
		}

		// Store assignment in metadata
		h.storeAssignment(ctx, groupID, memberID, ss.generation, assignment)

		resp := kmsg.NewPtrSyncGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errSyncGroupNone
		resp.MemberAssignment = assignment
		select {
		case ps.ch <- resp:
		default:
		}
	}

	// Clear pending since we've distributed
	ss.pending = make(map[string]*pendingSync)

	// Stop timer
	if ss.timer != nil {
		ss.timer.Stop()
		ss.timer = nil
	}
}

// storeAssignment stores a member's assignment in metadata.
func (h *SyncGroupHandler) storeAssignment(ctx context.Context, groupID, memberID string, generation int32, data []byte) {
	nowMs := time.Now().UnixMilli()
	_, err := h.store.SetAssignment(ctx, groups.SetAssignmentRequest{
		GroupID:    groupID,
		MemberID:   memberID,
		Generation: generation,
		Data:       data,
		NowMs:      nowMs,
	})
	if err != nil {
		logger := logging.FromCtx(ctx)
		logger.Warnf("failed to store assignment", map[string]any{
			"group":  groupID,
			"member": memberID,
			"error":  err.Error(),
		})
	}
}

// transitionToStable transitions the group to Stable state.
func (h *SyncGroupHandler) transitionToStable(ctx context.Context, groupID string, generation int32) {
	nowMs := time.Now().UnixMilli()
	_, err := h.store.TransitionState(ctx, groups.TransitionStateRequest{
		GroupID:            groupID,
		FromState:          groups.GroupStateCompletingRebalance,
		ToState:            groups.GroupStateStable,
		ExpectedGeneration: generation,
		NowMs:              nowMs,
	})
	if err != nil && !errors.Is(err, groups.ErrGroupNotFound) {
		// Log but don't fail - might already be stable or in another state
		logger := logging.FromCtx(ctx)
		logger.Debugf("transition to stable", map[string]any{
			"group":  groupID,
			"result": err.Error(),
		})
	}
}

// handleSyncTimeout handles the case where the sync phase times out.
func (h *SyncGroupHandler) handleSyncTimeout(ctx context.Context, groupID string, generation int32, version int16) {
	h.mu.Lock()
	ss, exists := h.syncStates[groupID]
	if !exists || ss.generation != generation {
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()

	ss.mu.Lock()
	defer ss.mu.Unlock()

	// If we already received assignments, nothing to do
	if ss.received {
		return
	}

	// Notify all waiting members of the timeout
	for _, ps := range ss.pending {
		resp := kmsg.NewPtrSyncGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errSyncGroupRebalanceInProgress
		select {
		case ps.ch <- resp:
		default:
		}
	}

	ss.pending = make(map[string]*pendingSync)
	ss.timer = nil
}

// ClearSyncState clears the sync state for a group (called when group is deleted or rebalance starts).
func (h *SyncGroupHandler) ClearSyncState(groupID string) {
	h.mu.Lock()
	delete(h.syncStates, groupID)
	h.mu.Unlock()
}
