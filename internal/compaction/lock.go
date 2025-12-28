// Package compaction implements stream compaction for the Dray broker.
// This file implements one-compactor-per-stream locking using ephemeral keys.
//
// The lock mechanism ensures that at most one compactor operates on a stream
// at any time. Locks are implemented using Oxia ephemeral keys, which are
// automatically deleted when the compactor's session ends (crash or disconnect).
//
// Key format: /compaction/locks/<streamId>
package compaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// Lock-related errors.
var (
	// ErrLockNotHeld is returned when an operation requires holding the lock
	// but the caller does not hold it.
	ErrLockNotHeld = errors.New("compaction: lock not held")

	// ErrLockHeldByOther is returned when attempting to acquire a lock that
	// is already held by another compactor.
	ErrLockHeldByOther = errors.New("compaction: lock held by another compactor")

	// ErrInvalidStreamID is returned when a stream ID is empty.
	ErrInvalidStreamID = errors.New("compaction: invalid stream ID")
)

// Lock represents the ephemeral lock for stream compaction.
// The lock is stored at /compaction/locks/<streamId>.
type Lock struct {
	// StreamID is the stream this lock is for.
	StreamID string `json:"streamId"`

	// CompactorID is the compactor that holds this lock.
	CompactorID string `json:"compactorId"`

	// AcquiredAtMs is when the lock was acquired (unix milliseconds).
	AcquiredAtMs int64 `json:"acquiredAtMs"`

	// JobID is an optional identifier for the current compaction job.
	// This helps correlate lock ownership with specific compaction operations.
	JobID string `json:"jobId,omitempty"`
}

// LockManager manages compaction locks for streams.
// It uses ephemeral keys in the metadata store to ensure automatic
// lock release on compactor failure.
type LockManager struct {
	meta        metadata.MetadataStore
	compactorID string

	// heldLocks tracks locks this compactor currently holds.
	// Key is streamID, value is the lock.
	heldLocks map[string]*Lock
}

// NewLockManager creates a new lock manager for the given compactor.
func NewLockManager(meta metadata.MetadataStore, compactorID string) *LockManager {
	return &LockManager{
		meta:        meta,
		compactorID: compactorID,
		heldLocks:   make(map[string]*Lock),
	}
}

// AcquireResult represents the result of attempting to acquire a lock.
type AcquireResult struct {
	// Acquired is true if the lock was successfully acquired.
	Acquired bool

	// Lock is the lock that was acquired (if Acquired is true) or
	// the existing lock held by another compactor (if Acquired is false).
	Lock *Lock
}

// AcquireLock attempts to acquire the compaction lock for a stream.
// If the lock is already held by this compactor, it is renewed.
// If the lock is held by another compactor, returns AcquireResult with Acquired=false.
//
// The lock is stored as an ephemeral key, so it will be automatically
// released if this compactor's session ends.
//
// This method uses atomic CAS operations:
// - For new acquisitions: uses ExpectNotExists to ensure no concurrent acquisition
// - For renewals: uses ExpectedVersion to ensure the lock hasn't been taken over
func (lm *LockManager) AcquireLock(ctx context.Context, streamID string) (*AcquireResult, error) {
	return lm.AcquireLockWithJob(ctx, streamID, "")
}

// AcquireLockWithJob is like AcquireLock but also records a job ID.
func (lm *LockManager) AcquireLockWithJob(ctx context.Context, streamID, jobID string) (*AcquireResult, error) {
	if streamID == "" {
		return nil, ErrInvalidStreamID
	}

	key := keys.CompactionLockKeyPath(streamID)
	now := time.Now().UnixMilli()

	// Check if there's an existing lock
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("compaction: get lock: %w", err)
	}

	if result.Exists {
		var existingLock Lock
		if err := json.Unmarshal(result.Value, &existingLock); err != nil {
			return nil, fmt.Errorf("compaction: unmarshal lock: %w", err)
		}

		// If we already hold the lock, renew it atomically with version check
		if existingLock.CompactorID == lm.compactorID {
			existingLock.AcquiredAtMs = now
			if jobID != "" {
				existingLock.JobID = jobID
			}

			lockData, err := json.Marshal(existingLock)
			if err != nil {
				return nil, fmt.Errorf("compaction: marshal lock: %w", err)
			}

			// Renew with expected version to ensure atomicity
			_, err = lm.meta.PutEphemeral(ctx, key, lockData,
				metadata.WithEphemeralExpectedVersion(result.Version))
			if err != nil {
				if errors.Is(err, metadata.ErrVersionMismatch) {
					// Lock was modified/taken over, re-read and return actual holder
					return lm.handleLockConflict(ctx, key)
				}
				return nil, fmt.Errorf("compaction: renew lock: %w", err)
			}

			lm.heldLocks[streamID] = &existingLock
			return &AcquireResult{
				Acquired: true,
				Lock:     &existingLock,
			}, nil
		}

		// Lock is held by another compactor
		return &AcquireResult{
			Acquired: false,
			Lock:     &existingLock,
		}, nil
	}

	// No existing lock, try to acquire it atomically
	lock := Lock{
		StreamID:     streamID,
		CompactorID:  lm.compactorID,
		AcquiredAtMs: now,
		JobID:        jobID,
	}

	lockData, err := json.Marshal(lock)
	if err != nil {
		return nil, fmt.Errorf("compaction: marshal lock: %w", err)
	}

	// Create ephemeral key with ExpectNotExists for atomic acquisition
	_, err = lm.meta.PutEphemeral(ctx, key, lockData,
		metadata.WithEphemeralExpectNotExists())
	if err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
			// Another compactor acquired the lock concurrently
			return lm.handleLockConflict(ctx, key)
		}
		return nil, fmt.Errorf("compaction: acquire lock: %w", err)
	}

	lm.heldLocks[streamID] = &lock
	return &AcquireResult{
		Acquired: true,
		Lock:     &lock,
	}, nil
}

// handleLockConflict re-reads the lock after a conflict and returns the actual holder.
func (lm *LockManager) handleLockConflict(ctx context.Context, key string) (*AcquireResult, error) {
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("compaction: get lock after conflict: %w", err)
	}
	if result.Exists {
		var existingLock Lock
		if unmarshalErr := json.Unmarshal(result.Value, &existingLock); unmarshalErr != nil {
			return nil, fmt.Errorf("compaction: unmarshal lock after conflict: %w", unmarshalErr)
		}
		return &AcquireResult{
			Acquired: false,
			Lock:     &existingLock,
		}, nil
	}
	// Lock disappeared - this is a transient state, caller should retry
	return nil, fmt.Errorf("compaction: lock disappeared during conflict resolution")
}

// ReleaseLock explicitly releases the compaction lock for a stream.
// This should be called when compaction completes (success or failure).
// Returns nil if the lock was not held by this compactor.
func (lm *LockManager) ReleaseLock(ctx context.Context, streamID string) error {
	if streamID == "" {
		return ErrInvalidStreamID
	}

	key := keys.CompactionLockKeyPath(streamID)

	// Check if we hold the lock
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("compaction: get lock for release: %w", err)
	}

	if !result.Exists {
		delete(lm.heldLocks, streamID)
		return nil
	}

	var lock Lock
	if err := json.Unmarshal(result.Value, &lock); err != nil {
		return fmt.Errorf("compaction: unmarshal lock for release: %w", err)
	}

	// Only release if we hold it
	if lock.CompactorID != lm.compactorID {
		delete(lm.heldLocks, streamID)
		return nil
	}

	// Delete the ephemeral key with version check to avoid clobbering
	// a new lock if another compactor acquired between Get and Delete.
	if err := lm.meta.Delete(ctx, key, metadata.WithDeleteExpectedVersion(result.Version)); err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) {
			// The lock was modified (likely taken over by another compactor).
			// This is not an error - it just means we no longer hold it.
			delete(lm.heldLocks, streamID)
			return nil
		}
		return fmt.Errorf("compaction: delete lock: %w", err)
	}

	delete(lm.heldLocks, streamID)
	return nil
}

// GetLock retrieves the current lock holder for a stream.
// Returns nil if no lock exists.
func (lm *LockManager) GetLock(ctx context.Context, streamID string) (*Lock, error) {
	if streamID == "" {
		return nil, ErrInvalidStreamID
	}

	key := keys.CompactionLockKeyPath(streamID)
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("compaction: get lock: %w", err)
	}

	if !result.Exists {
		return nil, nil
	}

	var lock Lock
	if err := json.Unmarshal(result.Value, &lock); err != nil {
		return nil, fmt.Errorf("compaction: unmarshal lock: %w", err)
	}

	return &lock, nil
}

// HoldsLock checks if this compactor holds the lock for a stream.
// This is a quick local check using the cached lock state.
// For authoritative checks, use GetLock and compare CompactorID.
func (lm *LockManager) HoldsLock(streamID string) bool {
	_, held := lm.heldLocks[streamID]
	return held
}

// IsLockHolder checks if this compactor is the authoritative lock holder.
// Unlike HoldsLock, this reads from the metadata store to get the
// current state. Use this before starting compaction.
func (lm *LockManager) IsLockHolder(ctx context.Context, streamID string) (bool, error) {
	lock, err := lm.GetLock(ctx, streamID)
	if err != nil {
		return false, err
	}
	if lock == nil {
		return false, nil
	}
	return lock.CompactorID == lm.compactorID, nil
}

// ReleaseAllLocks releases all locks held by this compactor.
// This should be called during graceful shutdown.
func (lm *LockManager) ReleaseAllLocks(ctx context.Context) error {
	var lastErr error
	for streamID := range lm.heldLocks {
		if err := lm.ReleaseLock(ctx, streamID); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ListHeldLocks returns a copy of all locks currently held by this compactor.
func (lm *LockManager) ListHeldLocks() []*Lock {
	locks := make([]*Lock, 0, len(lm.heldLocks))
	for _, lock := range lm.heldLocks {
		lockCopy := *lock
		locks = append(locks, &lockCopy)
	}
	return locks
}

// CompactorID returns the compactor ID of this lock manager.
func (lm *LockManager) CompactorID() string {
	return lm.compactorID
}
