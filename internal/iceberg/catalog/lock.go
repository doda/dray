// Package catalog implements Iceberg catalog clients for Dray's stream-table duality.
// This file implements single-writer locking for Iceberg commits using ephemeral keys.
//
// The lock mechanism ensures that at most one compactor commits to an Iceberg table
// at any time. Locks are implemented using Oxia ephemeral keys, which are
// automatically deleted when the compactor's session ends (crash or disconnect).
//
// Key format: /iceberg/<topic>/lock
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// Lock-related errors.
var (
	// ErrIcebergLockNotHeld is returned when an operation requires holding the lock
	// but the caller does not hold it.
	ErrIcebergLockNotHeld = errors.New("iceberg: lock not held")

	// ErrIcebergLockHeldByOther is returned when attempting to acquire a lock that
	// is already held by another writer.
	ErrIcebergLockHeldByOther = errors.New("iceberg: lock held by another writer")

	// ErrInvalidTopic is returned when a topic name is empty.
	ErrInvalidTopic = errors.New("iceberg: invalid topic name")
)

// IcebergLock represents the ephemeral lock for Iceberg commits.
// The lock is stored at /iceberg/<topic>/lock.
type IcebergLock struct {
	// Topic is the topic/table this lock is for.
	Topic string `json:"topic"`

	// WriterID is the writer that holds this lock.
	WriterID string `json:"writerId"`

	// AcquiredAtMs is when the lock was acquired (unix milliseconds).
	AcquiredAtMs int64 `json:"acquiredAtMs"`

	// JobID is an optional identifier for the current compaction job.
	// This helps correlate lock ownership with specific compaction operations.
	JobID string `json:"jobId,omitempty"`
}

// IcebergLockManager manages locks for Iceberg commits.
// It uses ephemeral keys in the metadata store to ensure automatic
// lock release on writer failure.
type IcebergLockManager struct {
	meta     metadata.MetadataStore
	writerID string
	mu       sync.RWMutex

	// heldLocks tracks locks this writer currently holds.
	// Key is topic, value is the lock.
	heldLocks map[string]*IcebergLock
}

// NewIcebergLockManager creates a new lock manager for the given writer.
func NewIcebergLockManager(meta metadata.MetadataStore, writerID string) *IcebergLockManager {
	return &IcebergLockManager{
		meta:      meta,
		writerID:  writerID,
		heldLocks: make(map[string]*IcebergLock),
	}
}

// IcebergAcquireResult represents the result of attempting to acquire a lock.
type IcebergAcquireResult struct {
	// Acquired is true if the lock was successfully acquired.
	Acquired bool

	// Lock is the lock that was acquired (if Acquired is true) or
	// the existing lock held by another writer (if Acquired is false).
	Lock *IcebergLock
}

// AcquireLock attempts to acquire the Iceberg commit lock for a topic.
// If the lock is already held by this writer, it is renewed.
// If the lock is held by another writer, returns IcebergAcquireResult with Acquired=false.
//
// The lock is stored as an ephemeral key, so it will be automatically
// released if this writer's session ends.
//
// This method uses atomic CAS operations:
// - For new acquisitions: uses ExpectNotExists to ensure no concurrent acquisition
// - For renewals: uses ExpectedVersion to ensure the lock hasn't been taken over
func (lm *IcebergLockManager) AcquireLock(ctx context.Context, topic string) (*IcebergAcquireResult, error) {
	return lm.AcquireLockWithJob(ctx, topic, "")
}

// AcquireLockWithJob is like AcquireLock but also records a job ID.
func (lm *IcebergLockManager) AcquireLockWithJob(ctx context.Context, topic, jobID string) (*IcebergAcquireResult, error) {
	if topic == "" {
		return nil, ErrInvalidTopic
	}

	key := keys.IcebergLockKeyPath(topic)
	now := time.Now().UnixMilli()

	// Check if there's an existing lock
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("iceberg: get lock: %w", err)
	}

	if result.Exists {
		var existingLock IcebergLock
		if err := json.Unmarshal(result.Value, &existingLock); err != nil {
			return nil, fmt.Errorf("iceberg: unmarshal lock: %w", err)
		}

		// If we already hold the lock, renew it atomically with version check
		if existingLock.WriterID == lm.writerID {
			existingLock.AcquiredAtMs = now
			if jobID != "" {
				existingLock.JobID = jobID
			}

			lockData, err := json.Marshal(existingLock)
			if err != nil {
				return nil, fmt.Errorf("iceberg: marshal lock: %w", err)
			}

			// Renew with expected version to ensure atomicity
			_, err = lm.meta.PutEphemeral(ctx, key, lockData,
				metadata.WithEphemeralExpectedVersion(result.Version))
			if err != nil {
				if errors.Is(err, metadata.ErrVersionMismatch) {
					// Lock was modified/taken over, re-read and return actual holder
					return lm.handleLockConflict(ctx, key)
				}
				return nil, fmt.Errorf("iceberg: renew lock: %w", err)
			}

			lm.mu.Lock()
			lm.heldLocks[topic] = &existingLock
			lm.mu.Unlock()
			return &IcebergAcquireResult{
				Acquired: true,
				Lock:     &existingLock,
			}, nil
		}

		// Lock is held by another writer
		return &IcebergAcquireResult{
			Acquired: false,
			Lock:     &existingLock,
		}, nil
	}

	// No existing lock, try to acquire it atomically
	lock := IcebergLock{
		Topic:        topic,
		WriterID:     lm.writerID,
		AcquiredAtMs: now,
		JobID:        jobID,
	}

	lockData, err := json.Marshal(lock)
	if err != nil {
		return nil, fmt.Errorf("iceberg: marshal lock: %w", err)
	}

	// Create ephemeral key with ExpectNotExists for atomic acquisition
	_, err = lm.meta.PutEphemeral(ctx, key, lockData,
		metadata.WithEphemeralExpectNotExists())
	if err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
			// Another writer acquired the lock concurrently
			return lm.handleLockConflict(ctx, key)
		}
		return nil, fmt.Errorf("iceberg: acquire lock: %w", err)
	}

	lm.mu.Lock()
	lm.heldLocks[topic] = &lock
	lm.mu.Unlock()
	return &IcebergAcquireResult{
		Acquired: true,
		Lock:     &lock,
	}, nil
}

// handleLockConflict re-reads the lock after a conflict and returns the actual holder.
func (lm *IcebergLockManager) handleLockConflict(ctx context.Context, key string) (*IcebergAcquireResult, error) {
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("iceberg: get lock after conflict: %w", err)
	}
	if result.Exists {
		var existingLock IcebergLock
		if unmarshalErr := json.Unmarshal(result.Value, &existingLock); unmarshalErr != nil {
			return nil, fmt.Errorf("iceberg: unmarshal lock after conflict: %w", unmarshalErr)
		}
		return &IcebergAcquireResult{
			Acquired: false,
			Lock:     &existingLock,
		}, nil
	}
	// Lock disappeared - this is a transient state, caller should retry
	return nil, fmt.Errorf("iceberg: lock disappeared during conflict resolution")
}

// ReleaseLock explicitly releases the Iceberg commit lock for a topic.
// This should be called when the commit completes (success or failure).
// Returns nil if the lock was not held by this writer.
func (lm *IcebergLockManager) ReleaseLock(ctx context.Context, topic string) error {
	if topic == "" {
		return ErrInvalidTopic
	}

	key := keys.IcebergLockKeyPath(topic)

	// Check if we hold the lock
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("iceberg: get lock for release: %w", err)
	}

	if !result.Exists {
		lm.mu.Lock()
		delete(lm.heldLocks, topic)
		lm.mu.Unlock()
		return nil
	}

	var lock IcebergLock
	if err := json.Unmarshal(result.Value, &lock); err != nil {
		return fmt.Errorf("iceberg: unmarshal lock for release: %w", err)
	}

	// Only release if we hold it
	if lock.WriterID != lm.writerID {
		lm.mu.Lock()
		delete(lm.heldLocks, topic)
		lm.mu.Unlock()
		return nil
	}

	// Delete the ephemeral key with version check to avoid clobbering
	// a new lock if another writer acquired between Get and Delete.
	if err := lm.meta.Delete(ctx, key, metadata.WithDeleteExpectedVersion(result.Version)); err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) {
			// The lock was modified (likely taken over by another writer).
			// This is not an error - it just means we no longer hold it.
			lm.mu.Lock()
			delete(lm.heldLocks, topic)
			lm.mu.Unlock()
			return nil
		}
		return fmt.Errorf("iceberg: delete lock: %w", err)
	}

	lm.mu.Lock()
	delete(lm.heldLocks, topic)
	lm.mu.Unlock()
	return nil
}

// GetLock retrieves the current lock holder for a topic.
// Returns nil if no lock exists.
func (lm *IcebergLockManager) GetLock(ctx context.Context, topic string) (*IcebergLock, error) {
	if topic == "" {
		return nil, ErrInvalidTopic
	}

	key := keys.IcebergLockKeyPath(topic)
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("iceberg: get lock: %w", err)
	}

	if !result.Exists {
		return nil, nil
	}

	var lock IcebergLock
	if err := json.Unmarshal(result.Value, &lock); err != nil {
		return nil, fmt.Errorf("iceberg: unmarshal lock: %w", err)
	}

	return &lock, nil
}

// HoldsLock checks if this writer holds the lock for a topic.
// This is a quick local check using the cached lock state.
// For authoritative checks, use GetLock and compare WriterID.
func (lm *IcebergLockManager) HoldsLock(topic string) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	_, held := lm.heldLocks[topic]
	return held
}

// IsLockHolder checks if this writer is the authoritative lock holder.
// Unlike HoldsLock, this reads from the metadata store to get the
// current state. Use this before starting an Iceberg commit.
func (lm *IcebergLockManager) IsLockHolder(ctx context.Context, topic string) (bool, error) {
	lock, err := lm.GetLock(ctx, topic)
	if err != nil {
		return false, err
	}
	if lock == nil {
		return false, nil
	}
	return lock.WriterID == lm.writerID, nil
}

// ReleaseAllLocks releases all locks held by this writer.
// This should be called during graceful shutdown.
func (lm *IcebergLockManager) ReleaseAllLocks(ctx context.Context) error {
	var lastErr error
	topics := lm.listHeldTopics()
	for _, topic := range topics {
		if err := lm.ReleaseLock(ctx, topic); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ListHeldLocks returns a copy of all locks currently held by this writer.
func (lm *IcebergLockManager) ListHeldLocks() []*IcebergLock {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	locks := make([]*IcebergLock, 0, len(lm.heldLocks))
	for _, lock := range lm.heldLocks {
		lockCopy := *lock
		locks = append(locks, &lockCopy)
	}
	return locks
}

func (lm *IcebergLockManager) listHeldTopics() []string {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	topics := make([]string, 0, len(lm.heldLocks))
	for topic := range lm.heldLocks {
		topics = append(topics, topic)
	}
	return topics
}

// WriterID returns the writer ID of this lock manager.
func (lm *IcebergLockManager) WriterID() string {
	return lm.writerID
}

// AcquireWithRetry attempts to acquire the lock with exponential backoff retry.
// This is useful when lock contention is expected.
func (lm *IcebergLockManager) AcquireWithRetry(ctx context.Context, topic string, maxRetries int, initialBackoff, maxBackoff time.Duration) (*IcebergAcquireResult, error) {
	return lm.AcquireWithRetryAndJob(ctx, topic, "", maxRetries, initialBackoff, maxBackoff)
}

// AcquireWithRetryAndJob is like AcquireWithRetry but also records a job ID.
func (lm *IcebergLockManager) AcquireWithRetryAndJob(ctx context.Context, topic, jobID string, maxRetries int, initialBackoff, maxBackoff time.Duration) (*IcebergAcquireResult, error) {
	if maxRetries <= 0 {
		maxRetries = 5
	}
	if initialBackoff <= 0 {
		initialBackoff = 100 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 10 * time.Second
	}

	backoff := initialBackoff
	var lastResult *IcebergAcquireResult

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result, err := lm.AcquireLockWithJob(ctx, topic, jobID)
		if err != nil {
			return nil, err
		}

		if result.Acquired {
			return result, nil
		}

		lastResult = result

		// Don't retry if this was the last attempt
		if attempt == maxRetries {
			break
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		// Increase backoff for next iteration
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	// Return the last result with Acquired=false
	return lastResult, nil
}
