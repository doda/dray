// Package groups implements the group coordinator for consumer groups.
// This file implements lease-based coordinator ownership per spec section 10.3.
//
// The lease mechanism ensures that only one broker at a time runs session
// timeouts for a consumer group. When a broker acquires a lease for a group,
// it becomes the "active coordinator" and is responsible for:
//   - Running session timeout sweeps
//   - Managing group state transitions
//   - Handling rebalances
//
// Leases are implemented using Oxia ephemeral keys, which are automatically
// deleted when the broker's session ends (crash or disconnect). This provides
// automatic failover: when a broker fails, its leases are released and other
// brokers can acquire them.
package groups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// Lease-related errors.
var (
	// ErrLeaseNotHeld is returned when an operation requires holding the lease
	// but the caller does not hold it.
	ErrLeaseNotHeld = errors.New("groups: lease not held")

	// ErrLeaseHeldByOther is returned when attempting to acquire a lease that
	// is already held by another broker.
	ErrLeaseHeldByOther = errors.New("groups: lease held by another broker")
)

// Lease represents the ephemeral lease for a consumer group coordinator.
// The lease is stored at /dray/v1/groups/<groupId>/lease.
type Lease struct {
	// GroupID is the consumer group this lease is for.
	GroupID string `json:"groupId"`

	// BrokerID is the broker that holds this lease.
	BrokerID string `json:"brokerId"`

	// Epoch is incremented each time a lease is acquired.
	// This helps detect stale lease holders.
	Epoch int64 `json:"epoch"`

	// AcquiredAtMs is when the lease was acquired (unix milliseconds).
	AcquiredAtMs int64 `json:"acquiredAtMs"`

	// LastRenewedAtMs is when the lease was last renewed (unix milliseconds).
	// For ephemeral keys, this is updated on each renewal.
	LastRenewedAtMs int64 `json:"lastRenewedAtMs"`
}

// LeaseManager manages coordinator leases for consumer groups.
// It uses ephemeral keys in the metadata store to ensure automatic
// lease release on broker failure.
type LeaseManager struct {
	meta     metadata.MetadataStore
	brokerID string

	// heldLeases tracks leases this broker currently holds.
	// Key is groupID, value is the lease.
	heldLeases map[string]*Lease
}

// NewLeaseManager creates a new lease manager for the given broker.
func NewLeaseManager(meta metadata.MetadataStore, brokerID string) *LeaseManager {
	return &LeaseManager{
		meta:       meta,
		brokerID:   brokerID,
		heldLeases: make(map[string]*Lease),
	}
}

// AcquireResult represents the result of attempting to acquire a lease.
type AcquireResult struct {
	// Acquired is true if the lease was successfully acquired.
	Acquired bool

	// Lease is the lease that was acquired (if Acquired is true) or
	// the existing lease held by another broker (if Acquired is false).
	Lease *Lease
}

// AcquireLease attempts to acquire the coordinator lease for a group.
// If the lease is already held by this broker, it is renewed with version checking.
// If the lease is held by another broker, returns AcquireResult with Acquired=false.
//
// The lease is stored as an ephemeral key, so it will be automatically
// released if this broker's session ends.
//
// This method uses atomic CAS operations:
// - For new acquisitions: uses ExpectNotExists to ensure no concurrent acquisition
// - For renewals: uses ExpectedVersion to ensure the lease hasn't been taken over
func (lm *LeaseManager) AcquireLease(ctx context.Context, groupID string) (*AcquireResult, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	key := keys.GroupLeaseKeyPath(groupID)
	now := time.Now().UnixMilli()

	// Check if there's an existing lease
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get lease: %w", err)
	}

	if result.Exists {
		var existingLease Lease
		if err := json.Unmarshal(result.Value, &existingLease); err != nil {
			return nil, fmt.Errorf("groups: unmarshal lease: %w", err)
		}

		// If we already hold the lease, renew it atomically with version check
		if existingLease.BrokerID == lm.brokerID {
			existingLease.LastRenewedAtMs = now

			leaseData, err := json.Marshal(existingLease)
			if err != nil {
				return nil, fmt.Errorf("groups: marshal lease: %w", err)
			}

			// Renew with expected version to ensure atomicity
			_, err = lm.meta.PutEphemeral(ctx, key, leaseData,
				metadata.WithEphemeralExpectedVersion(result.Version))
			if err != nil {
				if errors.Is(err, metadata.ErrVersionMismatch) {
					// Lease was modified/taken over, re-read and return actual holder
					return lm.handleLeaseConflict(ctx, key)
				}
				return nil, fmt.Errorf("groups: renew lease: %w", err)
			}

			lm.heldLeases[groupID] = &existingLease
			return &AcquireResult{
				Acquired: true,
				Lease:    &existingLease,
			}, nil
		}

		// Lease is held by another broker
		return &AcquireResult{
			Acquired: false,
			Lease:    &existingLease,
		}, nil
	}

	// No existing lease, try to acquire it atomically
	lease := Lease{
		GroupID:         groupID,
		BrokerID:        lm.brokerID,
		Epoch:           1,
		AcquiredAtMs:    now,
		LastRenewedAtMs: now,
	}

	leaseData, err := json.Marshal(lease)
	if err != nil {
		return nil, fmt.Errorf("groups: marshal lease: %w", err)
	}

	// Create ephemeral key with ExpectNotExists for atomic acquisition
	_, err = lm.meta.PutEphemeral(ctx, key, leaseData,
		metadata.WithEphemeralExpectNotExists())
	if err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
			// Another broker acquired the lease concurrently
			return lm.handleLeaseConflict(ctx, key)
		}
		return nil, fmt.Errorf("groups: acquire lease: %w", err)
	}

	lm.heldLeases[groupID] = &lease
	return &AcquireResult{
		Acquired: true,
		Lease:    &lease,
	}, nil
}

// handleLeaseConflict re-reads the lease after a conflict and returns the actual holder.
func (lm *LeaseManager) handleLeaseConflict(ctx context.Context, key string) (*AcquireResult, error) {
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get lease after conflict: %w", err)
	}
	if result.Exists {
		var existingLease Lease
		if unmarshalErr := json.Unmarshal(result.Value, &existingLease); unmarshalErr != nil {
			return nil, fmt.Errorf("groups: unmarshal lease after conflict: %w", unmarshalErr)
		}
		return &AcquireResult{
			Acquired: false,
			Lease:    &existingLease,
		}, nil
	}
	// Lease disappeared, this is a transient state
	return nil, fmt.Errorf("groups: lease disappeared during conflict resolution")
}

// ReacquireLease attempts to re-acquire a lease that was previously released
// (e.g., due to broker failover). Unlike AcquireLease for renewal, this
// increments the epoch to indicate a new acquisition.
func (lm *LeaseManager) ReacquireLease(ctx context.Context, groupID string) (*AcquireResult, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	key := keys.GroupLeaseKeyPath(groupID)
	now := time.Now().UnixMilli()

	// Check if there's an existing lease
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get lease: %w", err)
	}

	if result.Exists {
		var existingLease Lease
		if err := json.Unmarshal(result.Value, &existingLease); err != nil {
			return nil, fmt.Errorf("groups: unmarshal lease: %w", err)
		}

		// If we already hold the lease, this is a re-acquisition - increment epoch
		if existingLease.BrokerID == lm.brokerID {
			existingLease.Epoch++
			existingLease.AcquiredAtMs = now
			existingLease.LastRenewedAtMs = now

			leaseData, err := json.Marshal(existingLease)
			if err != nil {
				return nil, fmt.Errorf("groups: marshal lease: %w", err)
			}

			// Re-acquire with expected version to ensure atomicity
			_, err = lm.meta.PutEphemeral(ctx, key, leaseData,
				metadata.WithEphemeralExpectedVersion(result.Version))
			if err != nil {
				if errors.Is(err, metadata.ErrVersionMismatch) {
					return lm.handleLeaseConflict(ctx, key)
				}
				return nil, fmt.Errorf("groups: reacquire lease: %w", err)
			}

			lm.heldLeases[groupID] = &existingLease
			return &AcquireResult{
				Acquired: true,
				Lease:    &existingLease,
			}, nil
		}

		// Lease is held by another broker
		return &AcquireResult{
			Acquired: false,
			Lease:    &existingLease,
		}, nil
	}

	// No existing lease, acquire it with epoch 1
	lease := Lease{
		GroupID:         groupID,
		BrokerID:        lm.brokerID,
		Epoch:           1,
		AcquiredAtMs:    now,
		LastRenewedAtMs: now,
	}

	leaseData, err := json.Marshal(lease)
	if err != nil {
		return nil, fmt.Errorf("groups: marshal lease: %w", err)
	}

	_, err = lm.meta.PutEphemeral(ctx, key, leaseData,
		metadata.WithEphemeralExpectNotExists())
	if err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
			return lm.handleLeaseConflict(ctx, key)
		}
		return nil, fmt.Errorf("groups: acquire lease: %w", err)
	}

	lm.heldLeases[groupID] = &lease
	return &AcquireResult{
		Acquired: true,
		Lease:    &lease,
	}, nil
}

// ReleaseLease explicitly releases the coordinator lease for a group.
// This is typically called during graceful shutdown.
// Returns nil if the lease was not held by this broker.
func (lm *LeaseManager) ReleaseLease(ctx context.Context, groupID string) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}

	key := keys.GroupLeaseKeyPath(groupID)

	// Check if we hold the lease
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("groups: get lease for release: %w", err)
	}

	if !result.Exists {
		delete(lm.heldLeases, groupID)
		return nil
	}

	var lease Lease
	if err := json.Unmarshal(result.Value, &lease); err != nil {
		return fmt.Errorf("groups: unmarshal lease for release: %w", err)
	}

	// Only release if we hold it
	if lease.BrokerID != lm.brokerID {
		delete(lm.heldLeases, groupID)
		return nil
	}

	// Delete the ephemeral key with version check to avoid clobbering
	// a new lease if another broker acquired between Get and Delete.
	if err := lm.meta.Delete(ctx, key, metadata.WithDeleteExpectedVersion(result.Version)); err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) {
			// The lease was modified (likely taken over by another broker).
			// This is not an error - it just means we no longer hold it.
			delete(lm.heldLeases, groupID)
			return nil
		}
		return fmt.Errorf("groups: delete lease: %w", err)
	}

	delete(lm.heldLeases, groupID)
	return nil
}

// GetLease retrieves the current lease holder for a group.
// Returns nil if no lease exists.
func (lm *LeaseManager) GetLease(ctx context.Context, groupID string) (*Lease, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	key := keys.GroupLeaseKeyPath(groupID)
	result, err := lm.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get lease: %w", err)
	}

	if !result.Exists {
		return nil, nil
	}

	var lease Lease
	if err := json.Unmarshal(result.Value, &lease); err != nil {
		return nil, fmt.Errorf("groups: unmarshal lease: %w", err)
	}

	return &lease, nil
}

// HoldsLease checks if this broker holds the lease for a group.
// This is a quick local check using the cached lease state.
// For authoritative checks, use GetLease and compare BrokerID.
func (lm *LeaseManager) HoldsLease(groupID string) bool {
	_, held := lm.heldLeases[groupID]
	return held
}

// IsLeaseHolder checks if this broker is the authoritative lease holder.
// Unlike HoldsLease, this reads from the metadata store to get the
// current state. Use this before running session timeouts.
func (lm *LeaseManager) IsLeaseHolder(ctx context.Context, groupID string) (bool, error) {
	lease, err := lm.GetLease(ctx, groupID)
	if err != nil {
		return false, err
	}
	if lease == nil {
		return false, nil
	}
	return lease.BrokerID == lm.brokerID, nil
}

// ReleaseAllLeases releases all leases held by this broker.
// This should be called during graceful shutdown.
func (lm *LeaseManager) ReleaseAllLeases(ctx context.Context) error {
	var lastErr error
	for groupID := range lm.heldLeases {
		if err := lm.ReleaseLease(ctx, groupID); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ListHeldLeases returns a copy of all leases currently held by this broker.
func (lm *LeaseManager) ListHeldLeases() []*Lease {
	leases := make([]*Lease, 0, len(lm.heldLeases))
	for _, lease := range lm.heldLeases {
		leaseCopy := *lease
		leases = append(leases, &leaseCopy)
	}
	return leases
}

// BrokerID returns the broker ID of this lease manager.
func (lm *LeaseManager) BrokerID() string {
	return lm.brokerID
}
