package groups

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

func TestLeaseManager_AcquireLease(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	result, err := lm.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	if !result.Acquired {
		t.Error("expected lease to be acquired")
	}
	if result.Lease == nil {
		t.Fatal("expected lease to be returned")
	}
	if result.Lease.GroupID != "test-group" {
		t.Errorf("expected group id 'test-group', got %s", result.Lease.GroupID)
	}
	if result.Lease.BrokerID != "broker-1" {
		t.Errorf("expected broker id 'broker-1', got %s", result.Lease.BrokerID)
	}
	if result.Lease.Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", result.Lease.Epoch)
	}
	if result.Lease.AcquiredAtMs == 0 {
		t.Error("expected acquired_at to be set")
	}
}

func TestLeaseManager_AcquireLeaseRenewal(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// First acquire
	result1, err := lm.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !result1.Acquired {
		t.Fatal("expected first acquire to succeed")
	}
	originalTime := result1.Lease.LastRenewedAtMs

	// Wait a bit to ensure time difference
	time.Sleep(2 * time.Millisecond)

	// Second acquire should renew
	result2, err := lm.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to renew lease: %v", err)
	}
	if !result2.Acquired {
		t.Error("expected renewal to succeed")
	}
	if result2.Lease.LastRenewedAtMs <= originalTime {
		t.Error("expected last_renewed_at to be updated")
	}
	// Epoch stays the same for renewals
	if result2.Lease.Epoch != 1 {
		t.Errorf("expected epoch to remain 1 on renewal, got %d", result2.Lease.Epoch)
	}
}

func TestLeaseManager_AcquireLeaseConflict(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// Broker 1 acquires first
	result1, err := lm1.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-1 failed to acquire lease: %v", err)
	}
	if !result1.Acquired {
		t.Fatal("expected broker-1 to acquire lease")
	}

	// Broker 2 should fail to acquire
	result2, err := lm2.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-2 acquire returned error: %v", err)
	}
	if result2.Acquired {
		t.Error("expected broker-2 to NOT acquire lease")
	}
	if result2.Lease == nil {
		t.Fatal("expected existing lease to be returned")
	}
	if result2.Lease.BrokerID != "broker-1" {
		t.Errorf("expected lease holder to be 'broker-1', got %s", result2.Lease.BrokerID)
	}
}

func TestLeaseManager_ReleaseLease(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Acquire first
	_, err := lm.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Release
	err = lm.ReleaseLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to release lease: %v", err)
	}

	// Verify lease is gone
	lease, err := lm.GetLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to get lease after release: %v", err)
	}
	if lease != nil {
		t.Error("expected lease to be nil after release")
	}

	// Local tracking should also be cleared
	if lm.HoldsLease("test-group") {
		t.Error("expected local holds check to return false after release")
	}
}

func TestLeaseManager_ReleaseLeaseNotHeld(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// Broker 1 acquires
	_, err := lm1.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-1 failed to acquire lease: %v", err)
	}

	// Broker 2 tries to release (should not error, just no-op)
	err = lm2.ReleaseLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("release by non-holder should not error: %v", err)
	}

	// Lease should still be held by broker-1
	lease, err := lm1.GetLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	if lease == nil || lease.BrokerID != "broker-1" {
		t.Error("expected lease to still be held by broker-1")
	}
}

func TestLeaseManager_ReleaseNonexistent(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Release a lease that was never acquired should not error
	err := lm.ReleaseLease(ctx, "nonexistent-group")
	if err != nil {
		t.Fatalf("release of nonexistent lease should not error: %v", err)
	}
}

func TestLeaseManager_GetLease(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Get nonexistent lease
	lease, err := lm.GetLease(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("get nonexistent lease error: %v", err)
	}
	if lease != nil {
		t.Error("expected nil lease for nonexistent group")
	}

	// Acquire and get
	_, err = lm.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	lease, err = lm.GetLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected lease to be returned")
	}
	if lease.GroupID != "test-group" {
		t.Errorf("expected group id 'test-group', got %s", lease.GroupID)
	}
	if lease.BrokerID != "broker-1" {
		t.Errorf("expected broker id 'broker-1', got %s", lease.BrokerID)
	}
}

func TestLeaseManager_HoldsLease(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Before acquiring
	if lm.HoldsLease("test-group") {
		t.Error("expected HoldsLease to return false before acquiring")
	}

	// After acquiring
	_, err := lm.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !lm.HoldsLease("test-group") {
		t.Error("expected HoldsLease to return true after acquiring")
	}

	// After release
	err = lm.ReleaseLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to release lease: %v", err)
	}
	if lm.HoldsLease("test-group") {
		t.Error("expected HoldsLease to return false after releasing")
	}
}

func TestLeaseManager_IsLeaseHolder(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// No lease exists
	isHolder, err := lm1.IsLeaseHolder(ctx, "test-group")
	if err != nil {
		t.Fatalf("IsLeaseHolder error: %v", err)
	}
	if isHolder {
		t.Error("expected false when no lease exists")
	}

	// Broker 1 acquires
	_, err = lm1.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Broker 1 should be holder
	isHolder, err = lm1.IsLeaseHolder(ctx, "test-group")
	if err != nil {
		t.Fatalf("IsLeaseHolder error: %v", err)
	}
	if !isHolder {
		t.Error("expected broker-1 to be lease holder")
	}

	// Broker 2 should not be holder
	isHolder, err = lm2.IsLeaseHolder(ctx, "test-group")
	if err != nil {
		t.Fatalf("IsLeaseHolder error: %v", err)
	}
	if isHolder {
		t.Error("expected broker-2 to NOT be lease holder")
	}
}

func TestLeaseManager_ReleaseAllLeases(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Acquire multiple leases
	groups := []string{"group-1", "group-2", "group-3"}
	for _, g := range groups {
		_, err := lm.AcquireLease(ctx, g)
		if err != nil {
			t.Fatalf("failed to acquire lease for %s: %v", g, err)
		}
	}

	// Verify all are held
	for _, g := range groups {
		if !lm.HoldsLease(g) {
			t.Errorf("expected to hold lease for %s", g)
		}
	}

	// Release all
	err := lm.ReleaseAllLeases(ctx)
	if err != nil {
		t.Fatalf("failed to release all leases: %v", err)
	}

	// Verify all are released
	for _, g := range groups {
		if lm.HoldsLease(g) {
			t.Errorf("expected lease for %s to be released", g)
		}
		lease, err := lm.GetLease(ctx, g)
		if err != nil {
			t.Fatalf("get lease error: %v", err)
		}
		if lease != nil {
			t.Errorf("expected lease for %s to be nil", g)
		}
	}
}

func TestLeaseManager_ListHeldLeases(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Initially empty
	leases := lm.ListHeldLeases()
	if len(leases) != 0 {
		t.Errorf("expected 0 leases, got %d", len(leases))
	}

	// Acquire some leases
	groups := []string{"group-1", "group-2"}
	for _, g := range groups {
		_, err := lm.AcquireLease(ctx, g)
		if err != nil {
			t.Fatalf("failed to acquire lease for %s: %v", g, err)
		}
	}

	// List should return 2
	leases = lm.ListHeldLeases()
	if len(leases) != 2 {
		t.Errorf("expected 2 leases, got %d", len(leases))
	}

	// Verify they are copies (modifying shouldn't affect internal state)
	for _, l := range leases {
		l.BrokerID = "modified"
	}
	leases2 := lm.ListHeldLeases()
	for _, l := range leases2 {
		if l.BrokerID != "broker-1" {
			t.Error("ListHeldLeases returned references instead of copies")
		}
	}
}

func TestLeaseManager_BrokerID(t *testing.T) {
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "my-broker-id")

	if lm.BrokerID() != "my-broker-id" {
		t.Errorf("expected broker id 'my-broker-id', got %s", lm.BrokerID())
	}
}

func TestLeaseManager_InvalidGroupID(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Acquire with empty group ID
	_, err := lm.AcquireLease(ctx, "")
	if err != ErrInvalidGroupID {
		t.Errorf("expected ErrInvalidGroupID, got %v", err)
	}

	// Release with empty group ID
	err = lm.ReleaseLease(ctx, "")
	if err != ErrInvalidGroupID {
		t.Errorf("expected ErrInvalidGroupID, got %v", err)
	}

	// Get with empty group ID
	_, err = lm.GetLease(ctx, "")
	if err != ErrInvalidGroupID {
		t.Errorf("expected ErrInvalidGroupID, got %v", err)
	}

	// IsLeaseHolder with empty group ID
	_, err = lm.IsLeaseHolder(ctx, "")
	if err != ErrInvalidGroupID {
		t.Errorf("expected ErrInvalidGroupID, got %v", err)
	}
}

func TestLeaseManager_LeaseTransferOnFailure(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// Broker 1 acquires lease
	result, err := lm1.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-1 failed to acquire lease: %v", err)
	}
	if !result.Acquired {
		t.Fatal("expected broker-1 to acquire lease")
	}

	// Simulate broker-1 failure by releasing the lease
	// (In practice, this would happen automatically via ephemeral key expiry)
	err = lm1.ReleaseLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to simulate broker failure: %v", err)
	}

	// Broker 2 should now be able to acquire
	result, err = lm2.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-2 failed to acquire lease: %v", err)
	}
	if !result.Acquired {
		t.Error("expected broker-2 to acquire lease after broker-1 failure")
	}
	if result.Lease.BrokerID != "broker-2" {
		t.Errorf("expected broker-2 to be lease holder, got %s", result.Lease.BrokerID)
	}
}

func TestLeaseManager_MultipleGroupsIsolation(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// Broker 1 acquires group-1
	_, err := lm1.AcquireLease(ctx, "group-1")
	if err != nil {
		t.Fatalf("failed to acquire group-1: %v", err)
	}

	// Broker 2 should still be able to acquire group-2
	result, err := lm2.AcquireLease(ctx, "group-2")
	if err != nil {
		t.Fatalf("failed to acquire group-2: %v", err)
	}
	if !result.Acquired {
		t.Error("expected broker-2 to acquire group-2")
	}

	// Verify isolation
	isHolder1, _ := lm1.IsLeaseHolder(ctx, "group-1")
	isHolder2, _ := lm1.IsLeaseHolder(ctx, "group-2")
	if !isHolder1 {
		t.Error("broker-1 should hold group-1")
	}
	if isHolder2 {
		t.Error("broker-1 should not hold group-2")
	}

	isHolder1, _ = lm2.IsLeaseHolder(ctx, "group-1")
	isHolder2, _ = lm2.IsLeaseHolder(ctx, "group-2")
	if isHolder1 {
		t.Error("broker-2 should not hold group-1")
	}
	if !isHolder2 {
		t.Error("broker-2 should hold group-2")
	}
}

func TestLeaseManager_ReacquireLease(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// First acquire
	result1, err := lm.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !result1.Acquired {
		t.Fatal("expected first acquire to succeed")
	}
	if result1.Lease.Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", result1.Lease.Epoch)
	}

	// Reacquire should increment epoch
	result2, err := lm.ReacquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to reacquire lease: %v", err)
	}
	if !result2.Acquired {
		t.Error("expected reacquire to succeed")
	}
	if result2.Lease.Epoch != 2 {
		t.Errorf("expected epoch 2 after reacquire, got %d", result2.Lease.Epoch)
	}

	// Reacquire again should increment to 3
	result3, err := lm.ReacquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to reacquire lease again: %v", err)
	}
	if result3.Lease.Epoch != 3 {
		t.Errorf("expected epoch 3 after second reacquire, got %d", result3.Lease.Epoch)
	}
}

func TestLeaseManager_ReacquireLeaseNewGroup(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	// Reacquire on a group that doesn't have a lease yet should acquire with epoch 1
	result, err := lm.ReacquireLease(ctx, "new-group")
	if err != nil {
		t.Fatalf("failed to reacquire new group: %v", err)
	}
	if !result.Acquired {
		t.Error("expected reacquire to succeed for new group")
	}
	if result.Lease.Epoch != 1 {
		t.Errorf("expected epoch 1 for new group, got %d", result.Lease.Epoch)
	}
}

func TestLeaseManager_ReacquireLeaseConflict(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// Broker 1 acquires
	_, err := lm1.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-1 failed to acquire: %v", err)
	}

	// Broker 2 tries to reacquire - should fail
	result, err := lm2.ReacquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-2 reacquire returned error: %v", err)
	}
	if result.Acquired {
		t.Error("expected broker-2 reacquire to NOT succeed")
	}
	if result.Lease.BrokerID != "broker-1" {
		t.Errorf("expected lease holder to be broker-1, got %s", result.Lease.BrokerID)
	}
}

func TestLeaseManager_AtomicAcquisition(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// Both brokers try to acquire at the same time
	// Due to the atomic ExpectNotExists, only one should succeed
	result1, err1 := lm1.AcquireLease(ctx, "test-group")
	if err1 != nil {
		t.Fatalf("broker-1 acquire error: %v", err1)
	}
	if !result1.Acquired {
		t.Fatal("broker-1 should acquire since it went first")
	}

	// Broker 2's attempt should fail because the key now exists
	result2, err2 := lm2.AcquireLease(ctx, "test-group")
	if err2 != nil {
		t.Fatalf("broker-2 acquire error: %v", err2)
	}
	if result2.Acquired {
		t.Error("broker-2 should NOT acquire since broker-1 has it")
	}
	if result2.Lease.BrokerID != "broker-1" {
		t.Errorf("expected lease to be held by broker-1, got %s", result2.Lease.BrokerID)
	}
}

func TestLeaseManager_InvalidGroupIDForReacquire(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	_, err := lm.ReacquireLease(ctx, "")
	if err != ErrInvalidGroupID {
		t.Errorf("expected ErrInvalidGroupID for empty group, got %v", err)
	}
}

func TestLeaseManager_ReleaseDoesNotClobberNewLease(t *testing.T) {
	// This test verifies that ReleaseLease uses version-checked delete
	// to avoid accidentally deleting another broker's lease if there's
	// a race condition between Get and Delete.
	ctx := context.Background()
	mockStore := metadata.NewMockStore()

	lm1 := NewLeaseManager(mockStore, "broker-1")
	lm2 := NewLeaseManager(mockStore, "broker-2")

	// Broker 1 acquires the lease
	result1, err := lm1.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-1 failed to acquire lease: %v", err)
	}
	if !result1.Acquired {
		t.Fatal("broker-1 should have acquired the lease")
	}

	// Simulate the race: broker-1's lease expires (ephemeral key deleted),
	// then broker-2 acquires a new lease, all before broker-1's Delete runs.
	// We simulate this by:
	// 1. Deleting broker-1's lease directly from the store
	// 2. Having broker-2 acquire a new lease
	// 3. Then having broker-1 try to release (should not clobber broker-2's lease)

	key := "/dray/v1/groups/test-group/lease"
	_ = mockStore.Delete(ctx, key) // Simulate ephemeral key expiration

	// Broker 2 acquires the lease
	result2, err := lm2.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-2 failed to acquire lease: %v", err)
	}
	if !result2.Acquired {
		t.Fatal("broker-2 should have acquired the lease")
	}

	// Now broker-1 tries to release (its local state still thinks it holds the lease)
	// Because the version changed, the delete should fail silently (not clobber broker-2's lease)
	err = lm1.ReleaseLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("broker-1 release should not error (graceful handling): %v", err)
	}

	// Verify broker-2 still holds the lease
	lease, err := lm2.GetLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected lease to still exist")
	}
	if lease.BrokerID != "broker-2" {
		t.Errorf("expected lease to be held by broker-2, got %s", lease.BrokerID)
	}

	// Verify broker-1's local state is cleared
	if lm1.HoldsLease("test-group") {
		t.Error("broker-1 should not think it holds the lease after release")
	}
}

func TestLeaseManager_ConcurrentHeldLeaseAccess(t *testing.T) {
	ctx := context.Background()
	mockStore := metadata.NewMockStore()
	lm := NewLeaseManager(mockStore, "broker-1")

	groupIDs := []string{"group-a", "group-b", "group-c"}
	for _, groupID := range groupIDs {
		if _, err := lm.AcquireLease(ctx, groupID); err != nil {
			t.Fatalf("failed to acquire lease for %s: %v", groupID, err)
		}
	}

	start := make(chan struct{})
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	readerCount := 4
	readerLoops := 200
	writerLoops := 150

	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			for j := 0; j < readerLoops; j++ {
				_ = lm.HoldsLease(groupIDs[(idx+j)%len(groupIDs)])
				_ = lm.ListHeldLeases()
				runtime.Gosched()
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < writerLoops; i++ {
			groupID := groupIDs[i%len(groupIDs)]
			if err := lm.ReleaseLease(ctx, groupID); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if _, err := lm.AcquireLease(ctx, groupID); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			runtime.Gosched()
		}
	}()

	close(start)
	wg.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("concurrent lease operations failed: %v", err)
	default:
	}
}
