package compaction

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

func TestLockManager_AcquireLock_Success(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	result, err := lm.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !result.Acquired {
		t.Fatal("Expected lock to be acquired")
	}
	if result.Lock.StreamID != "stream-abc" {
		t.Errorf("Expected StreamID stream-abc, got %s", result.Lock.StreamID)
	}
	if result.Lock.CompactorID != "compactor-1" {
		t.Errorf("Expected CompactorID compactor-1, got %s", result.Lock.CompactorID)
	}
	if result.Lock.AcquiredAtMs == 0 {
		t.Error("Expected AcquiredAtMs to be set")
	}

	// Verify in metadata store
	key := keys.CompactionLockKeyPath("stream-abc")
	getResult, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !getResult.Exists {
		t.Error("Expected lock to exist in store")
	}
}

func TestLockManager_AcquireLock_WithJobID(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	result, err := lm.AcquireLockWithJob(ctx, "stream-abc", "job-123")
	if err != nil {
		t.Fatalf("AcquireLockWithJob failed: %v", err)
	}
	if !result.Acquired {
		t.Fatal("Expected lock to be acquired")
	}
	if result.Lock.JobID != "job-123" {
		t.Errorf("Expected JobID job-123, got %s", result.Lock.JobID)
	}
}

func TestLockManager_AcquireLock_InvalidStreamID(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	_, err := lm.AcquireLock(ctx, "")
	if err != ErrInvalidStreamID {
		t.Errorf("Expected ErrInvalidStreamID, got %v", err)
	}
}

func TestLockManager_AcquireLock_HeldByOther(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()

	// First compactor acquires lock
	lm1 := NewLockManager(store, "compactor-1")
	result1, err := lm1.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("First AcquireLock failed: %v", err)
	}
	if !result1.Acquired {
		t.Fatal("Expected first lock to be acquired")
	}

	// Second compactor tries to acquire same lock
	lm2 := NewLockManager(store, "compactor-2")
	result2, err := lm2.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("Second AcquireLock failed: %v", err)
	}
	if result2.Acquired {
		t.Fatal("Expected second lock acquisition to fail")
	}
	if result2.Lock.CompactorID != "compactor-1" {
		t.Errorf("Expected lock holder to be compactor-1, got %s", result2.Lock.CompactorID)
	}
}

func TestLockManager_AcquireLock_Renew(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// First acquisition
	result1, err := lm.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("First AcquireLock failed: %v", err)
	}
	firstAcquiredAt := result1.Lock.AcquiredAtMs

	// Small delay to ensure different timestamps
	time.Sleep(2 * time.Millisecond)

	// Renew (re-acquire by same compactor)
	result2, err := lm.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("Renew AcquireLock failed: %v", err)
	}
	if !result2.Acquired {
		t.Fatal("Expected renewal to succeed")
	}
	if result2.Lock.AcquiredAtMs <= firstAcquiredAt {
		t.Error("Expected AcquiredAtMs to be updated on renewal")
	}
}

func TestLockManager_ReleaseLock_Success(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// Acquire lock
	_, err := lm.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Release lock
	if err := lm.ReleaseLock(ctx, "stream-abc"); err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Verify lock is gone
	key := keys.CompactionLockKeyPath("stream-abc")
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("Expected lock to be deleted")
	}

	// Verify local cache is cleared
	if lm.HoldsLock("stream-abc") {
		t.Error("Expected HoldsLock to return false")
	}
}

func TestLockManager_ReleaseLock_NotHeld(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// Release lock that was never held
	if err := lm.ReleaseLock(ctx, "stream-abc"); err != nil {
		t.Fatalf("ReleaseLock should succeed even if lock was never held: %v", err)
	}
}

func TestLockManager_ReleaseLock_HeldByOther(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()

	// First compactor acquires lock
	lm1 := NewLockManager(store, "compactor-1")
	_, err := lm1.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Second compactor tries to release it
	lm2 := NewLockManager(store, "compactor-2")
	if err := lm2.ReleaseLock(ctx, "stream-abc"); err != nil {
		t.Fatalf("ReleaseLock should succeed silently when lock is held by other: %v", err)
	}

	// Lock should still exist (held by compactor-1)
	key := keys.CompactionLockKeyPath("stream-abc")
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("Expected lock to still exist")
	}
}

func TestLockManager_GetLock(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// No lock exists
	lock, err := lm.GetLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("GetLock failed: %v", err)
	}
	if lock != nil {
		t.Error("Expected nil lock")
	}

	// Acquire lock
	_, err = lm.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Lock exists
	lock, err = lm.GetLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("GetLock failed: %v", err)
	}
	if lock == nil {
		t.Fatal("Expected lock to exist")
	}
	if lock.CompactorID != "compactor-1" {
		t.Errorf("Expected CompactorID compactor-1, got %s", lock.CompactorID)
	}
}

func TestLockManager_GetLock_InvalidStreamID(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	_, err := lm.GetLock(ctx, "")
	if err != ErrInvalidStreamID {
		t.Errorf("Expected ErrInvalidStreamID, got %v", err)
	}
}

func TestLockManager_HoldsLock(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// Not held initially
	if lm.HoldsLock("stream-abc") {
		t.Error("Expected HoldsLock to return false initially")
	}

	// Acquire lock
	_, err := lm.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Now held
	if !lm.HoldsLock("stream-abc") {
		t.Error("Expected HoldsLock to return true after acquire")
	}

	// Release lock
	if err := lm.ReleaseLock(ctx, "stream-abc"); err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Not held anymore
	if lm.HoldsLock("stream-abc") {
		t.Error("Expected HoldsLock to return false after release")
	}
}

func TestLockManager_IsLockHolder(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// Not holder initially
	isHolder, err := lm.IsLockHolder(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("IsLockHolder failed: %v", err)
	}
	if isHolder {
		t.Error("Expected not to be lock holder initially")
	}

	// Acquire lock
	_, err = lm.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}

	// Now holder
	isHolder, err = lm.IsLockHolder(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("IsLockHolder failed: %v", err)
	}
	if !isHolder {
		t.Error("Expected to be lock holder after acquire")
	}

	// Different compactor checks
	lm2 := NewLockManager(store, "compactor-2")
	isHolder2, err := lm2.IsLockHolder(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("IsLockHolder failed: %v", err)
	}
	if isHolder2 {
		t.Error("Expected compactor-2 not to be lock holder")
	}
}

func TestLockManager_ReleaseAllLocks(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// Acquire multiple locks
	for _, streamID := range []string{"stream-1", "stream-2", "stream-3"} {
		_, err := lm.AcquireLock(ctx, streamID)
		if err != nil {
			t.Fatalf("AcquireLock for %s failed: %v", streamID, err)
		}
	}

	// Verify all held
	if len(lm.ListHeldLocks()) != 3 {
		t.Errorf("Expected 3 held locks, got %d", len(lm.ListHeldLocks()))
	}

	// Release all
	if err := lm.ReleaseAllLocks(ctx); err != nil {
		t.Fatalf("ReleaseAllLocks failed: %v", err)
	}

	// Verify all released
	if len(lm.ListHeldLocks()) != 0 {
		t.Errorf("Expected 0 held locks after release, got %d", len(lm.ListHeldLocks()))
	}

	// Verify locks are gone from store
	for _, streamID := range []string{"stream-1", "stream-2", "stream-3"} {
		key := keys.CompactionLockKeyPath(streamID)
		result, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if result.Exists {
			t.Errorf("Expected lock for %s to be deleted", streamID)
		}
	}
}

func TestLockManager_ListHeldLocks(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "compactor-1")

	// Initially empty
	if len(lm.ListHeldLocks()) != 0 {
		t.Error("Expected empty held locks initially")
	}

	// Acquire locks
	for _, streamID := range []string{"stream-a", "stream-b"} {
		_, err := lm.AcquireLock(ctx, streamID)
		if err != nil {
			t.Fatalf("AcquireLock for %s failed: %v", streamID, err)
		}
	}

	// List should have 2
	locks := lm.ListHeldLocks()
	if len(locks) != 2 {
		t.Errorf("Expected 2 held locks, got %d", len(locks))
	}

	// Verify these are copies (modification shouldn't affect internal state)
	for _, lock := range locks {
		lock.CompactorID = "modified"
	}
	locks2 := lm.ListHeldLocks()
	for _, lock := range locks2 {
		if lock.CompactorID != "compactor-1" {
			t.Error("Expected ListHeldLocks to return copies")
		}
	}
}

func TestLockManager_CompactorID(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewLockManager(store, "my-compactor")
	if lm.CompactorID() != "my-compactor" {
		t.Errorf("Expected CompactorID my-compactor, got %s", lm.CompactorID())
	}
}

func TestLockManager_ConcurrentAcquisition(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()

	const numCompactors = 10
	streamID := "contested-stream"

	var wg sync.WaitGroup
	acquiredBy := make(chan string, numCompactors)

	for i := 0; i < numCompactors; i++ {
		compactorID := string(rune('A' + i))
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			lm := NewLockManager(store, id)
			result, err := lm.AcquireLock(ctx, streamID)
			if err != nil {
				t.Errorf("AcquireLock for %s failed: %v", id, err)
				return
			}
			if result.Acquired {
				acquiredBy <- id
			}
		}(compactorID)
	}

	wg.Wait()
	close(acquiredBy)

	// Exactly one compactor should have acquired the lock
	winners := []string{}
	for id := range acquiredBy {
		winners = append(winners, id)
	}

	if len(winners) != 1 {
		t.Errorf("Expected exactly 1 winner, got %d: %v", len(winners), winners)
	}

	// Verify lock is held by the winner
	if len(winners) == 1 {
		key := keys.CompactionLockKeyPath(streamID)
		result, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !result.Exists {
			t.Fatal("Expected lock to exist")
		}
	}
}

func TestLockManager_MultipleStreams(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()

	// Two compactors each acquire different streams
	lm1 := NewLockManager(store, "compactor-1")
	lm2 := NewLockManager(store, "compactor-2")

	// compactor-1 acquires stream-a
	result1, err := lm1.AcquireLock(ctx, "stream-a")
	if err != nil {
		t.Fatalf("AcquireLock for stream-a failed: %v", err)
	}
	if !result1.Acquired {
		t.Fatal("Expected compactor-1 to acquire stream-a")
	}

	// compactor-2 acquires stream-b (different stream, should succeed)
	result2, err := lm2.AcquireLock(ctx, "stream-b")
	if err != nil {
		t.Fatalf("AcquireLock for stream-b failed: %v", err)
	}
	if !result2.Acquired {
		t.Fatal("Expected compactor-2 to acquire stream-b")
	}

	// compactor-2 tries stream-a (should fail)
	result3, err := lm2.AcquireLock(ctx, "stream-a")
	if err != nil {
		t.Fatalf("AcquireLock for stream-a by compactor-2 failed: %v", err)
	}
	if result3.Acquired {
		t.Fatal("Expected compactor-2 to fail acquiring stream-a")
	}

	// Verify both compactors hold correct locks
	if !lm1.HoldsLock("stream-a") {
		t.Error("Expected compactor-1 to hold stream-a")
	}
	if lm1.HoldsLock("stream-b") {
		t.Error("Expected compactor-1 not to hold stream-b")
	}
	if lm2.HoldsLock("stream-a") {
		t.Error("Expected compactor-2 not to hold stream-a")
	}
	if !lm2.HoldsLock("stream-b") {
		t.Error("Expected compactor-2 to hold stream-b")
	}
}

func TestLockManager_ReleaseAndReacquire(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()

	lm1 := NewLockManager(store, "compactor-1")
	lm2 := NewLockManager(store, "compactor-2")

	// compactor-1 acquires
	result1, err := lm1.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("First acquire failed: %v", err)
	}
	if !result1.Acquired {
		t.Fatal("Expected first acquire to succeed")
	}

	// compactor-2 fails to acquire
	result2, err := lm2.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("Second acquire failed: %v", err)
	}
	if result2.Acquired {
		t.Fatal("Expected second acquire to fail")
	}

	// compactor-1 releases
	if err := lm1.ReleaseLock(ctx, "stream-abc"); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	// compactor-2 can now acquire
	result3, err := lm2.AcquireLock(ctx, "stream-abc")
	if err != nil {
		t.Fatalf("Third acquire failed: %v", err)
	}
	if !result3.Acquired {
		t.Fatal("Expected compactor-2 to acquire after release")
	}
	if result3.Lock.CompactorID != "compactor-2" {
		t.Errorf("Expected lock holder to be compactor-2, got %s", result3.Lock.CompactorID)
	}
}
