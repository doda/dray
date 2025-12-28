// Package groups implements the group coordinator for consumer groups.
// This file implements session timeout sweep for expired group members.
package groups

import (
	"context"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/logging"
)

// SessionSweeper periodically checks for members that have exceeded their
// session timeout and removes them, triggering rebalances as needed.
// Only the lease-holder broker for each group runs the sweep.
type SessionSweeper struct {
	store        *Store
	leaseManager *LeaseManager

	// interval is how often to run the sweep.
	interval time.Duration

	// clock provides time functions for testing.
	clock Clock

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// Clock provides time functions for testing.
type Clock interface {
	Now() time.Time
}

// realClock implements Clock using real time.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// SweepResult represents the result of a session sweep.
type SweepResult struct {
	// GroupsChecked is the number of groups that were checked.
	GroupsChecked int
	// MembersRemoved is the number of members that were removed.
	MembersRemoved int
	// RebalancesTriggered is the number of rebalances that were triggered.
	RebalancesTriggered int
}

// NewSessionSweeper creates a new session sweeper.
func NewSessionSweeper(store *Store, leaseManager *LeaseManager, interval time.Duration) *SessionSweeper {
	return &SessionSweeper{
		store:        store,
		leaseManager: leaseManager,
		interval:     interval,
		clock:        realClock{},
	}
}

// SetClock sets the clock for testing.
func (s *SessionSweeper) SetClock(c Clock) {
	s.clock = c
}

// Start starts the session sweeper in the background.
// It runs the sweep every interval until Stop is called.
func (s *SessionSweeper) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})
	s.mu.Unlock()

	go s.run(ctx)
}

// Stop stops the session sweeper and waits for it to finish.
func (s *SessionSweeper) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	close(s.stopCh)
	s.mu.Unlock()

	<-s.doneCh

	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
}

// run is the main loop that runs the sweep periodically.
func (s *SessionSweeper) run(ctx context.Context) {
	defer close(s.doneCh)

	logger := logging.FromCtx(ctx)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := s.Sweep(ctx)
			if err != nil {
				logger.Warnf("session sweep error", map[string]any{
					"error": err.Error(),
				})
			} else if result.MembersRemoved > 0 {
				logger.Infof("session sweep completed", map[string]any{
					"groupsChecked":       result.GroupsChecked,
					"membersRemoved":      result.MembersRemoved,
					"rebalancesTriggered": result.RebalancesTriggered,
				})
			}
		}
	}
}

// Sweep runs a single session timeout sweep across all groups this broker
// is the lease holder for. Returns the sweep result.
func (s *SessionSweeper) Sweep(ctx context.Context) (*SweepResult, error) {
	result := &SweepResult{}

	// Get all leases held by this broker
	heldLeases := s.leaseManager.ListHeldLeases()
	if len(heldLeases) == 0 {
		return result, nil
	}

	nowMs := s.clock.Now().UnixMilli()

	for _, lease := range heldLeases {
		// Verify we still hold the lease
		isHolder, err := s.leaseManager.IsLeaseHolder(ctx, lease.GroupID)
		if err != nil {
			continue
		}
		if !isHolder {
			continue
		}

		result.GroupsChecked++

		removed, err := s.sweepGroup(ctx, lease.GroupID, nowMs)
		if err != nil {
			continue
		}

		result.MembersRemoved += removed
		if removed > 0 {
			result.RebalancesTriggered++
		}
	}

	return result, nil
}

// SweepGroup runs a session timeout sweep for a specific group.
// Only runs if this broker holds the lease for the group.
// Returns the number of members removed.
func (s *SessionSweeper) SweepGroup(ctx context.Context, groupID string) (int, error) {
	// Verify we hold the lease
	isHolder, err := s.leaseManager.IsLeaseHolder(ctx, groupID)
	if err != nil {
		return 0, err
	}
	if !isHolder {
		return 0, ErrLeaseNotHeld
	}

	nowMs := s.clock.Now().UnixMilli()
	return s.sweepGroup(ctx, groupID, nowMs)
}

// sweepGroup performs the actual sweep logic for a group.
func (s *SessionSweeper) sweepGroup(ctx context.Context, groupID string, nowMs int64) (int, error) {
	logger := logging.FromCtx(ctx)

	// Get group state
	state, err := s.store.GetGroupState(ctx, groupID)
	if err != nil {
		return 0, err
	}

	// Don't sweep dead or empty groups
	if state.State == GroupStateDead || state.State == GroupStateEmpty {
		return 0, nil
	}

	// Get all members
	members, err := s.store.ListMembers(ctx, groupID)
	if err != nil {
		return 0, err
	}

	if len(members) == 0 {
		return 0, nil
	}

	// Find expired members
	var expiredMembers []string
	for _, member := range members {
		if s.isMemberExpired(member, nowMs) {
			expiredMembers = append(expiredMembers, member.MemberID)
		}
	}

	if len(expiredMembers) == 0 {
		return 0, nil
	}

	// Remove expired members
	for _, memberID := range expiredMembers {
		logger.Infof("removing expired member", map[string]any{
			"group":  groupID,
			"member": memberID,
		})
		if err := s.store.RemoveMember(ctx, groupID, memberID); err != nil {
			logger.Warnf("failed to remove expired member", map[string]any{
				"group":  groupID,
				"member": memberID,
				"error":  err.Error(),
			})
		}
	}

	// Trigger rebalance
	s.triggerRebalance(ctx, groupID, nowMs, logger)

	return len(expiredMembers), nil
}

// isMemberExpired checks if a member has exceeded its session timeout.
func (s *SessionSweeper) isMemberExpired(member GroupMember, nowMs int64) bool {
	// Member's session timeout is stored in milliseconds
	timeoutMs := int64(member.SessionTimeoutMs)
	if timeoutMs <= 0 {
		// If no session timeout is set, don't expire
		return false
	}

	elapsed := nowMs - member.LastHeartbeatMs
	return elapsed > timeoutMs
}

// triggerRebalance triggers a rebalance for the group after members are removed.
func (s *SessionSweeper) triggerRebalance(ctx context.Context, groupID string, nowMs int64, logger *logging.Logger) {
	// Check if there are any remaining members
	members, err := s.store.ListMembers(ctx, groupID)
	if err != nil {
		logger.Warnf("failed to list members for rebalance", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		return
	}

	if len(members) == 0 {
		// No more members - transition to Empty state
		_, err = s.store.TransitionState(ctx, TransitionStateRequest{
			GroupID:          groupID,
			ToState:          GroupStateEmpty,
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
	_, err = s.store.TransitionState(ctx, TransitionStateRequest{
		GroupID:          groupID,
		ToState:          GroupStatePreparingRebalance,
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
