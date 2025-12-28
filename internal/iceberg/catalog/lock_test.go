package catalog

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

func TestIcebergLockManager_AcquireLock_Success(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()
	result, err := lm.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Acquired {
		t.Error("expected lock to be acquired")
	}
	if result.Lock == nil {
		t.Fatal("expected lock to be non-nil")
	}
	if result.Lock.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %q", result.Lock.Topic)
	}
	if result.Lock.WriterID != "writer-1" {
		t.Errorf("expected writer ID 'writer-1', got %q", result.Lock.WriterID)
	}
	if result.Lock.AcquiredAtMs <= 0 {
		t.Error("expected acquired time to be set")
	}
}

func TestIcebergLockManager_AcquireLock_WithJobID(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()
	result, err := lm.AcquireLockWithJob(ctx, "test-topic", "job-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Acquired {
		t.Error("expected lock to be acquired")
	}
	if result.Lock.JobID != "job-123" {
		t.Errorf("expected job ID 'job-123', got %q", result.Lock.JobID)
	}
}

func TestIcebergLockManager_AcquireLock_InvalidTopic(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()
	_, err := lm.AcquireLock(ctx, "")
	if err != ErrInvalidTopic {
		t.Errorf("expected ErrInvalidTopic, got %v", err)
	}
}

func TestIcebergLockManager_AcquireLock_HeldByOther(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// writer-1 acquires the lock
	result1, err := lm1.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-1: unexpected error: %v", err)
	}
	if !result1.Acquired {
		t.Error("writer-1: expected lock to be acquired")
	}

	// writer-2 tries to acquire the same lock
	result2, err := lm2.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-2: unexpected error: %v", err)
	}
	if result2.Acquired {
		t.Error("writer-2: expected lock to NOT be acquired")
	}
	if result2.Lock.WriterID != "writer-1" {
		t.Errorf("expected lock holder 'writer-1', got %q", result2.Lock.WriterID)
	}
}

func TestIcebergLockManager_AcquireLock_Renew(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()

	// First acquisition
	result1, err := lm.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("first acquisition: unexpected error: %v", err)
	}
	if !result1.Acquired {
		t.Error("first acquisition: expected lock to be acquired")
	}
	firstAcquiredAt := result1.Lock.AcquiredAtMs

	// Wait a bit to ensure timestamp changes
	time.Sleep(2 * time.Millisecond)

	// Second acquisition (renewal)
	result2, err := lm.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("renewal: unexpected error: %v", err)
	}
	if !result2.Acquired {
		t.Error("renewal: expected lock to be acquired")
	}
	if result2.Lock.AcquiredAtMs <= firstAcquiredAt {
		t.Error("renewal: expected acquired time to be updated")
	}
}

func TestIcebergLockManager_ReleaseLock_Success(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()

	// Acquire lock
	_, err := lm.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("acquire: unexpected error: %v", err)
	}

	// Release lock
	if err := lm.ReleaseLock(ctx, "test-topic"); err != nil {
		t.Fatalf("release: unexpected error: %v", err)
	}

	// Verify lock is released
	lock, err := lm.GetLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("get lock: unexpected error: %v", err)
	}
	if lock != nil {
		t.Error("expected lock to be nil after release")
	}

	// Verify local state is cleared
	if lm.HoldsLock("test-topic") {
		t.Error("expected HoldsLock to return false after release")
	}
}

func TestIcebergLockManager_ReleaseLock_NotHeld(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()

	// Release lock that was never held
	if err := lm.ReleaseLock(ctx, "test-topic"); err != nil {
		t.Fatalf("release: unexpected error: %v", err)
	}
}

func TestIcebergLockManager_ReleaseLock_HeldByOther(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// writer-1 acquires the lock
	_, err := lm1.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("acquire: unexpected error: %v", err)
	}

	// writer-2 tries to release it
	if err := lm2.ReleaseLock(ctx, "test-topic"); err != nil {
		t.Fatalf("release: unexpected error: %v", err)
	}

	// Lock should still be held by writer-1
	lock, err := lm1.GetLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("get lock: unexpected error: %v", err)
	}
	if lock == nil {
		t.Fatal("expected lock to still exist")
	}
	if lock.WriterID != "writer-1" {
		t.Errorf("expected writer ID 'writer-1', got %q", lock.WriterID)
	}
}

func TestIcebergLockManager_GetLock(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()

	// No lock exists
	lock, err := lm.GetLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("get lock: unexpected error: %v", err)
	}
	if lock != nil {
		t.Error("expected nil for non-existent lock")
	}

	// Acquire lock
	_, err = lm.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("acquire: unexpected error: %v", err)
	}

	// Get lock
	lock, err = lm.GetLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("get lock: unexpected error: %v", err)
	}
	if lock == nil {
		t.Fatal("expected lock to exist")
	}
	if lock.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %q", lock.Topic)
	}
}

func TestIcebergLockManager_GetLock_InvalidTopic(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()
	_, err := lm.GetLock(ctx, "")
	if err != ErrInvalidTopic {
		t.Errorf("expected ErrInvalidTopic, got %v", err)
	}
}

func TestIcebergLockManager_HoldsLock(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()

	// Not held
	if lm.HoldsLock("test-topic") {
		t.Error("expected HoldsLock to return false before acquisition")
	}

	// Acquire
	_, err := lm.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("acquire: unexpected error: %v", err)
	}

	// Held
	if !lm.HoldsLock("test-topic") {
		t.Error("expected HoldsLock to return true after acquisition")
	}
}

func TestIcebergLockManager_IsLockHolder(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// Not held
	isHolder, err := lm1.IsLockHolder(ctx, "test-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isHolder {
		t.Error("expected IsLockHolder to return false before acquisition")
	}

	// Acquire
	_, err = lm1.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("acquire: unexpected error: %v", err)
	}

	// writer-1 is holder
	isHolder, err = lm1.IsLockHolder(ctx, "test-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isHolder {
		t.Error("expected IsLockHolder to return true for writer-1")
	}

	// writer-2 is not holder
	isHolder, err = lm2.IsLockHolder(ctx, "test-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isHolder {
		t.Error("expected IsLockHolder to return false for writer-2")
	}
}

func TestIcebergLockManager_ReleaseAllLocks(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()

	// Acquire multiple locks
	topics := []string{"topic-a", "topic-b", "topic-c"}
	for _, topic := range topics {
		_, err := lm.AcquireLock(ctx, topic)
		if err != nil {
			t.Fatalf("acquire %s: unexpected error: %v", topic, err)
		}
	}

	// Verify all held
	for _, topic := range topics {
		if !lm.HoldsLock(topic) {
			t.Errorf("expected to hold lock for %s", topic)
		}
	}

	// Release all
	if err := lm.ReleaseAllLocks(ctx); err != nil {
		t.Fatalf("release all: unexpected error: %v", err)
	}

	// Verify all released
	for _, topic := range topics {
		if lm.HoldsLock(topic) {
			t.Errorf("expected to NOT hold lock for %s after release", topic)
		}
		lock, err := lm.GetLock(ctx, topic)
		if err != nil {
			t.Fatalf("get lock %s: unexpected error: %v", topic, err)
		}
		if lock != nil {
			t.Errorf("expected lock for %s to be nil", topic)
		}
	}
}

func TestIcebergLockManager_ListHeldLocks(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	ctx := context.Background()

	// Empty initially
	locks := lm.ListHeldLocks()
	if len(locks) != 0 {
		t.Errorf("expected 0 locks, got %d", len(locks))
	}

	// Acquire multiple locks
	topics := []string{"topic-a", "topic-b", "topic-c"}
	for _, topic := range topics {
		_, err := lm.AcquireLock(ctx, topic)
		if err != nil {
			t.Fatalf("acquire %s: unexpected error: %v", topic, err)
		}
	}

	// List locks
	locks = lm.ListHeldLocks()
	if len(locks) != 3 {
		t.Errorf("expected 3 locks, got %d", len(locks))
	}

	// Verify topics
	foundTopics := make(map[string]bool)
	for _, lock := range locks {
		foundTopics[lock.Topic] = true
	}
	for _, topic := range topics {
		if !foundTopics[topic] {
			t.Errorf("missing lock for %s", topic)
		}
	}
}

func TestIcebergLockManager_WriterID(t *testing.T) {
	store := metadata.NewMockStore()
	lm := NewIcebergLockManager(store, "writer-1")

	if lm.WriterID() != "writer-1" {
		t.Errorf("expected writer ID 'writer-1', got %q", lm.WriterID())
	}
}

func TestIcebergLockManager_ConcurrentAcquisition(t *testing.T) {
	store := metadata.NewMockStore()

	const numWriters = 10
	var wg sync.WaitGroup
	wg.Add(numWriters)

	results := make(chan *IcebergAcquireResult, numWriters)

	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()
			lm := NewIcebergLockManager(store, "writer-"+string(rune('0'+writerID)))
			result, err := lm.AcquireLock(context.Background(), "test-topic")
			if err != nil {
				t.Errorf("writer-%d: unexpected error: %v", writerID, err)
				return
			}
			results <- result
		}(i)
	}

	wg.Wait()
	close(results)

	// Exactly one writer should acquire the lock
	acquiredCount := 0
	for result := range results {
		if result.Acquired {
			acquiredCount++
		}
	}

	if acquiredCount != 1 {
		t.Errorf("expected exactly 1 acquisition, got %d", acquiredCount)
	}
}

func TestIcebergLockManager_MultipleTopics(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// writer-1 acquires topic-a, writer-2 acquires topic-b
	result1, err := lm1.AcquireLock(ctx, "topic-a")
	if err != nil {
		t.Fatalf("writer-1 topic-a: unexpected error: %v", err)
	}
	if !result1.Acquired {
		t.Error("writer-1 should acquire topic-a")
	}

	result2, err := lm2.AcquireLock(ctx, "topic-b")
	if err != nil {
		t.Fatalf("writer-2 topic-b: unexpected error: %v", err)
	}
	if !result2.Acquired {
		t.Error("writer-2 should acquire topic-b")
	}

	// Each writer cannot acquire the other's topic
	result3, err := lm1.AcquireLock(ctx, "topic-b")
	if err != nil {
		t.Fatalf("writer-1 topic-b: unexpected error: %v", err)
	}
	if result3.Acquired {
		t.Error("writer-1 should NOT acquire topic-b")
	}

	result4, err := lm2.AcquireLock(ctx, "topic-a")
	if err != nil {
		t.Fatalf("writer-2 topic-a: unexpected error: %v", err)
	}
	if result4.Acquired {
		t.Error("writer-2 should NOT acquire topic-a")
	}
}

func TestIcebergLockManager_ReleaseAndReacquire(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// writer-1 acquires
	result1, err := lm1.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-1 acquire: unexpected error: %v", err)
	}
	if !result1.Acquired {
		t.Error("writer-1 should acquire lock")
	}

	// writer-2 cannot acquire
	result2, err := lm2.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-2 acquire: unexpected error: %v", err)
	}
	if result2.Acquired {
		t.Error("writer-2 should NOT acquire lock (held by writer-1)")
	}

	// writer-1 releases
	if err := lm1.ReleaseLock(ctx, "test-topic"); err != nil {
		t.Fatalf("writer-1 release: unexpected error: %v", err)
	}

	// writer-2 can now acquire
	result3, err := lm2.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-2 acquire after release: unexpected error: %v", err)
	}
	if !result3.Acquired {
		t.Error("writer-2 should acquire lock after writer-1 releases")
	}
}

func TestIcebergLockManager_AcquireWithRetry(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// writer-1 acquires
	_, err := lm1.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-1 acquire: unexpected error: %v", err)
	}

	// writer-2 tries with retry but should fail (lock is still held)
	result, err := lm2.AcquireWithRetry(ctx, "test-topic", 3, 10*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("writer-2 acquire with retry: unexpected error: %v", err)
	}
	if result.Acquired {
		t.Error("writer-2 should NOT acquire lock (held by writer-1)")
	}
	if result.Lock.WriterID != "writer-1" {
		t.Errorf("expected lock holder 'writer-1', got %q", result.Lock.WriterID)
	}
}

func TestIcebergLockManager_AcquireWithRetry_Success(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// writer-1 acquires
	_, err := lm1.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-1 acquire: unexpected error: %v", err)
	}

	// Release after a short delay in a goroutine
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = lm1.ReleaseLock(ctx, "test-topic")
	}()

	// writer-2 tries with retry and should eventually succeed
	result, err := lm2.AcquireWithRetry(ctx, "test-topic", 10, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("writer-2 acquire with retry: unexpected error: %v", err)
	}
	if !result.Acquired {
		t.Error("writer-2 should acquire lock after writer-1 releases")
	}
}

func TestIcebergLockManager_AcquireWithRetry_ContextCancel(t *testing.T) {
	store := metadata.NewMockStore()
	lm1 := NewIcebergLockManager(store, "writer-1")
	lm2 := NewIcebergLockManager(store, "writer-2")

	ctx := context.Background()

	// writer-1 acquires
	_, err := lm1.AcquireLock(ctx, "test-topic")
	if err != nil {
		t.Fatalf("writer-1 acquire: unexpected error: %v", err)
	}

	// Create a context that cancels quickly
	cancelCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()

	// writer-2 tries with retry but context should cancel
	_, err = lm2.AcquireWithRetry(cancelCtx, "test-topic", 10, 10*time.Millisecond, 100*time.Millisecond)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}
