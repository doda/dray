// Package groups implements the group coordinator for consumer groups.
// This file implements offset retention sweep for expired committed offsets.
package groups

import (
	"context"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/logging"
)

// OffsetRetentionSweeper periodically scans committed offsets and deletes
// those that have exceeded their retention time (ExpireTimestamp).
// Only the lease-holder broker for each group runs the sweep.
type OffsetRetentionSweeper struct {
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

// OffsetRetentionSweepResult represents the result of an offset retention sweep.
type OffsetRetentionSweepResult struct {
	// GroupsChecked is the number of groups that were checked.
	GroupsChecked int
	// OffsetsDeleted is the number of expired offsets that were deleted.
	OffsetsDeleted int
}

// NewOffsetRetentionSweeper creates a new offset retention sweeper.
func NewOffsetRetentionSweeper(store *Store, leaseManager *LeaseManager, interval time.Duration) *OffsetRetentionSweeper {
	return &OffsetRetentionSweeper{
		store:        store,
		leaseManager: leaseManager,
		interval:     interval,
		clock:        realClock{},
	}
}

// SetClock sets the clock for testing.
func (s *OffsetRetentionSweeper) SetClock(c Clock) {
	s.clock = c
}

// Start starts the offset retention sweeper in the background.
// It runs the sweep every interval until Stop is called.
func (s *OffsetRetentionSweeper) Start(ctx context.Context) {
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

// Stop stops the offset retention sweeper and waits for it to finish.
func (s *OffsetRetentionSweeper) Stop() {
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
func (s *OffsetRetentionSweeper) run(ctx context.Context) {
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
				logger.Warnf("offset retention sweep error", map[string]any{
					"error": err.Error(),
				})
			} else if result.OffsetsDeleted > 0 {
				logger.Infof("offset retention sweep completed", map[string]any{
					"groupsChecked":  result.GroupsChecked,
					"offsetsDeleted": result.OffsetsDeleted,
				})
			}
		}
	}
}

// Sweep runs a single offset retention sweep across all groups this broker
// is the lease holder for. Returns the sweep result.
func (s *OffsetRetentionSweeper) Sweep(ctx context.Context) (*OffsetRetentionSweepResult, error) {
	result := &OffsetRetentionSweepResult{}

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

		deleted, err := s.sweepGroup(ctx, lease.GroupID, nowMs)
		if err != nil {
			continue
		}

		result.OffsetsDeleted += deleted
	}

	return result, nil
}

// SweepGroup runs an offset retention sweep for a specific group.
// Only runs if this broker holds the lease for the group.
// Returns the number of offsets deleted.
func (s *OffsetRetentionSweeper) SweepGroup(ctx context.Context, groupID string) (int, error) {
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
func (s *OffsetRetentionSweeper) sweepGroup(ctx context.Context, groupID string, nowMs int64) (int, error) {
	logger := logging.FromCtx(ctx)

	// Get all committed offsets for this group
	offsets, err := s.store.ListCommittedOffsets(ctx, groupID)
	if err != nil {
		return 0, err
	}

	if len(offsets) == 0 {
		return 0, nil
	}

	// Find expired offsets
	var expiredCount int
	for _, offset := range offsets {
		if s.isOffsetExpired(offset, nowMs) {
			logger.Infof("deleting expired offset", map[string]any{
				"group":     groupID,
				"topic":     offset.Topic,
				"partition": offset.Partition,
				"expiredAt": offset.ExpireTimestamp,
			})
			if err := s.store.DeleteCommittedOffset(ctx, groupID, offset.Topic, offset.Partition); err != nil {
				logger.Warnf("failed to delete expired offset", map[string]any{
					"group":     groupID,
					"topic":     offset.Topic,
					"partition": offset.Partition,
					"error":     err.Error(),
				})
				continue
			}
			expiredCount++
		}
	}

	return expiredCount, nil
}

// isOffsetExpired checks if an offset has exceeded its retention time.
func (s *OffsetRetentionSweeper) isOffsetExpired(offset CommittedOffset, nowMs int64) bool {
	// ExpireTimestamp of -1 means no expiry
	if offset.ExpireTimestamp < 0 {
		return false
	}

	return nowMs > offset.ExpireTimestamp
}
