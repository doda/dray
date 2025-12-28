package groups

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

// mockClock is defined in session_sweep_test.go

func TestOffsetRetentionSweeper_SweepGroup_DeletesExpiredOffsets(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	// Create a group
	_, err := groupStore.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "test-group",
		Type:    GroupTypeClassic,
		NowMs:   1000,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Acquire lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !result.Acquired {
		t.Fatalf("failed to acquire lease: not acquired")
	}

	// Create sweeper with mock clock
	clock := &mockClock{now: time.Unix(0, 1000*int64(time.Millisecond))}
	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, time.Second)
	sweeper.SetClock(clock)

	// Commit offsets with various expiry times
	_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "topic1",
		Partition:       0,
		Offset:          100,
		RetentionTimeMs: 5000, // Expires at 6000
		NowMs:           1000,
	})
	if err != nil {
		t.Fatalf("failed to commit offset 1: %v", err)
	}

	_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "topic1",
		Partition:       1,
		Offset:          200,
		RetentionTimeMs: 10000, // Expires at 11000
		NowMs:           1000,
	})
	if err != nil {
		t.Fatalf("failed to commit offset 2: %v", err)
	}

	_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "topic2",
		Partition:       0,
		Offset:          300,
		RetentionTimeMs: -1, // Never expires
		NowMs:           1000,
	})
	if err != nil {
		t.Fatalf("failed to commit offset 3: %v", err)
	}

	// At time 1000, nothing should be expired
	deleted, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", deleted)
	}

	// Advance clock to 7000 - first offset should expire
	clock.now = time.Unix(0, 7000*int64(time.Millisecond))
	deleted, err = sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}

	// Verify offset was deleted
	offset, err := groupStore.GetCommittedOffset(ctx, "test-group", "topic1", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset != nil {
		t.Errorf("expected offset to be deleted, got %+v", offset)
	}

	// Verify other offsets still exist
	offset, err = groupStore.GetCommittedOffset(ctx, "test-group", "topic1", 1)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset == nil {
		t.Error("expected offset to exist")
	}

	offset, err = groupStore.GetCommittedOffset(ctx, "test-group", "topic2", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset == nil {
		t.Error("expected offset with no expiry to exist")
	}

	// Advance clock to 12000 - second offset should expire
	clock.now = time.Unix(0, 12000*int64(time.Millisecond))
	deleted, err = sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}

	// Offset with no expiry should still exist
	offset, err = groupStore.GetCommittedOffset(ctx, "test-group", "topic2", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset == nil {
		t.Error("expected offset with no expiry to still exist")
	}
}

func TestOffsetRetentionSweeper_Sweep_OnlyRunsOnLeaseHolder(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	groupStore := NewStore(store)

	// Create two lease managers for different brokers
	leaseManager1 := NewLeaseManager(store, "broker-1")
	leaseManager2 := NewLeaseManager(store, "broker-2")

	// Create a group
	_, err := groupStore.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "test-group",
		Type:    GroupTypeClassic,
		NowMs:   1000,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Broker 1 acquires lease
	result, err := leaseManager1.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !result.Acquired {
		t.Fatalf("failed to acquire lease: not acquired")
	}

	// Commit an expired offset
	_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "topic1",
		Partition:       0,
		Offset:          100,
		RetentionTimeMs: 1000, // Expires at 2000
		NowMs:           1000,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Create sweeper for broker 2 with clock past expiry
	clock := &mockClock{now: time.Unix(0, 5000*int64(time.Millisecond))}
	sweeper2 := NewOffsetRetentionSweeper(groupStore, leaseManager2, time.Second)
	sweeper2.SetClock(clock)

	// Broker 2 tries to sweep, should fail since it doesn't hold lease
	_, err = sweeper2.SweepGroup(ctx, "test-group")
	if err != ErrLeaseNotHeld {
		t.Errorf("expected ErrLeaseNotHeld, got %v", err)
	}

	// Offset should still exist
	offset, err := groupStore.GetCommittedOffset(ctx, "test-group", "topic1", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset == nil {
		t.Error("expected offset to still exist since broker 2 doesn't hold lease")
	}

	// Broker 1 sweeps - should succeed
	sweeper1 := NewOffsetRetentionSweeper(groupStore, leaseManager1, time.Second)
	sweeper1.SetClock(clock)

	deleted, err := sweeper1.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}
}

func TestOffsetRetentionSweeper_Sweep_MultipleGroups(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	// Create multiple groups
	for i := 1; i <= 3; i++ {
		groupID := "test-group-" + string(rune('0'+i))
		_, err := groupStore.CreateGroup(ctx, CreateGroupRequest{
			GroupID: groupID,
			Type:    GroupTypeClassic,
			NowMs:   1000,
		})
		if err != nil {
			t.Fatalf("failed to create group %s: %v", groupID, err)
		}

		// Acquire lease
		result, err := leaseManager.AcquireLease(ctx, groupID)
		if err != nil {
			t.Fatalf("failed to acquire lease for %s: %v", groupID, err)
		}
		if !result.Acquired {
			t.Fatalf("failed to acquire lease for %s: not acquired", groupID)
		}

		// Commit expired offset
		_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
			GroupID:         groupID,
			Topic:           "topic1",
			Partition:       0,
			Offset:          int64(i * 100),
			RetentionTimeMs: 1000,
			NowMs:           1000,
		})
		if err != nil {
			t.Fatalf("failed to commit offset for %s: %v", groupID, err)
		}
	}

	// Create sweeper with clock past expiry
	clock := &mockClock{now: time.Unix(0, 5000*int64(time.Millisecond))}
	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, time.Second)
	sweeper.SetClock(clock)

	// Run sweep
	result, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}

	if result.GroupsChecked != 3 {
		t.Errorf("expected 3 groups checked, got %d", result.GroupsChecked)
	}
	if result.OffsetsDeleted != 3 {
		t.Errorf("expected 3 offsets deleted, got %d", result.OffsetsDeleted)
	}
}

func TestOffsetRetentionSweeper_Sweep_NoExpiredOffsets(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	// Create a group
	_, err := groupStore.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "test-group",
		Type:    GroupTypeClassic,
		NowMs:   1000,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Acquire lease
	_, err = leaseManager.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Commit offset that won't expire for a long time
	_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "topic1",
		Partition:       0,
		Offset:          100,
		RetentionTimeMs: 1000000, // Expires far in future
		NowMs:           1000,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Create sweeper at time 2000 - offset not yet expired
	clock := &mockClock{now: time.Unix(0, 2000*int64(time.Millisecond))}
	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, time.Second)
	sweeper.SetClock(clock)

	result, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}

	if result.GroupsChecked != 1 {
		t.Errorf("expected 1 group checked, got %d", result.GroupsChecked)
	}
	if result.OffsetsDeleted != 0 {
		t.Errorf("expected 0 offsets deleted, got %d", result.OffsetsDeleted)
	}
}

func TestOffsetRetentionSweeper_Sweep_NoHeldLeases(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	// Don't acquire any leases

	// Create sweeper
	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, time.Second)

	result, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}

	if result.GroupsChecked != 0 {
		t.Errorf("expected 0 groups checked, got %d", result.GroupsChecked)
	}
	if result.OffsetsDeleted != 0 {
		t.Errorf("expected 0 offsets deleted, got %d", result.OffsetsDeleted)
	}
}

func TestOffsetRetentionSweeper_StartStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, 10*time.Millisecond)

	// Start should work
	sweeper.Start(ctx)

	// Starting again should be a no-op
	sweeper.Start(ctx)

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Stop should work
	sweeper.Stop()

	// Stopping again should be a no-op
	sweeper.Stop()
}

func TestOffsetRetentionSweeper_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, 10*time.Millisecond)

	sweeper.Start(ctx)

	// Cancel context - sweeper should stop
	cancel()

	// Give it time to stop
	time.Sleep(50 * time.Millisecond)

	// Should be able to stop cleanly (no deadlock)
	sweeper.Stop()
}

func TestOffsetRetentionSweeper_NoExpiryOffsets(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	// Create a group
	_, err := groupStore.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "test-group",
		Type:    GroupTypeClassic,
		NowMs:   1000,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Acquire lease
	_, err = leaseManager.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Commit offset with no expiry (RetentionTimeMs = -1)
	_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "topic1",
		Partition:       0,
		Offset:          100,
		RetentionTimeMs: -1, // No expiry
		NowMs:           1000,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Create sweeper with clock far in future (100 years from commit time)
	clock := &mockClock{now: time.UnixMilli(int64(1000) + 100*365*24*60*60*1000)}
	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, time.Second)
	sweeper.SetClock(clock)

	deleted, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted (no expiry), got %d", deleted)
	}

	// Offset should still exist
	offset, err := groupStore.GetCommittedOffset(ctx, "test-group", "topic1", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset == nil {
		t.Error("expected offset with no expiry to still exist")
	}
}

func TestOffsetRetentionSweeper_EdgeCaseExpiryExact(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	groupStore := NewStore(store)
	leaseManager := NewLeaseManager(store, "broker-1")

	// Create a group
	_, err := groupStore.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "test-group",
		Type:    GroupTypeClassic,
		NowMs:   1000,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Acquire lease
	_, err = leaseManager.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Commit offset that expires at exactly 2000
	_, err = groupStore.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "topic1",
		Partition:       0,
		Offset:          100,
		RetentionTimeMs: 1000, // Committed at 1000, expires at 2000
		NowMs:           1000,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	sweeper := NewOffsetRetentionSweeper(groupStore, leaseManager, time.Second)

	// At exactly expiry time (2000), should NOT expire yet (nowMs > expireTimestamp, not >=)
	clock := &mockClock{now: time.Unix(0, 2000*int64(time.Millisecond))}
	sweeper.SetClock(clock)

	deleted, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted at exact expiry time, got %d", deleted)
	}

	// At 2001ms, should expire
	clock.now = time.Unix(0, 2001*int64(time.Millisecond))
	deleted, err = sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted after expiry, got %d", deleted)
	}
}
