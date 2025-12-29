// Package metadata defines the MetadataStore interface and related types
// for metadata storage operations. The default implementation uses Oxia.
//
// The MetadataStore interface provides the foundation for all metadata
// operations in Dray, including key-value storage, atomic transactions,
// change notifications, and ephemeral keys for service discovery.
//
// See SPEC.md section 6.1 for the full specification.
package metadata

import (
	"context"
	"errors"
)

// Common errors returned by MetadataStore operations.
var (
	// ErrKeyNotFound is returned when a key does not exist.
	ErrKeyNotFound = errors.New("metadata: key not found")

	// ErrVersionMismatch is returned when the expected version does not match
	// the current version during a CAS (compare-and-set) operation.
	ErrVersionMismatch = errors.New("metadata: version mismatch")

	// ErrTxnConflict is returned when a transaction cannot be committed
	// due to concurrent modifications.
	ErrTxnConflict = errors.New("metadata: transaction conflict")

	// ErrSessionExpired is returned when an ephemeral key's session has expired.
	ErrSessionExpired = errors.New("metadata: session expired")

	// ErrStoreClosed is returned when operations are attempted on a closed store.
	ErrStoreClosed = errors.New("metadata: store closed")
)

// Version represents a key's version in the metadata store.
// Versions are monotonically increasing and can be used for
// optimistic concurrency control via compare-and-set operations.
//
// A zero version indicates the key has never been written.
// Versions are assigned by the metadata store on each write.
type Version int64

// NoVersion is a sentinel value indicating no version constraint.
const NoVersion Version = -1

// KV represents a key-value pair with its version.
type KV struct {
	Key     string
	Value   []byte
	Version Version
}

// GetResult is the result of a Get operation.
type GetResult struct {
	Value   []byte
	Version Version
	Exists  bool
}

// Notification represents a change notification from the metadata store.
// Notifications are delivered for key modifications after a subscription
// is established. The metadata store guarantees that once subscribed,
// all subsequent changes will be delivered even across failures.
type Notification struct {
	// Key is the key that was modified.
	Key string
	// Value is the new value, or nil if the key was deleted.
	Value []byte
	// Version is the version after the modification.
	Version Version
	// Deleted is true if the key was deleted.
	Deleted bool
}

// NotificationStream provides an iterator interface for receiving
// change notifications from the metadata store.
//
// Example usage:
//
//	stream, err := store.Notifications(ctx)
//	if err != nil {
//	    return err
//	}
//	defer stream.Close()
//
//	for {
//	    notification, err := stream.Next(ctx)
//	    if err != nil {
//	        return err
//	    }
//	    // Process notification...
//	}
type NotificationStream interface {
	// Next blocks until the next notification is available or the context
	// is cancelled. Returns the notification or an error.
	Next(ctx context.Context) (Notification, error)

	// Close releases resources associated with the stream.
	// After Close is called, Next will return an error.
	Close() error
}

// PutOption configures a Put operation.
type PutOption func(*putOptions)

type putOptions struct {
	expectedVersion *Version
}

// WithExpectedVersion specifies the expected version for a CAS operation.
// If the current version does not match, the Put will fail with ErrVersionMismatch.
// Use nil or omit this option to create a new key unconditionally.
func WithExpectedVersion(v Version) PutOption {
	return func(o *putOptions) {
		o.expectedVersion = &v
	}
}

// DeleteOption configures a Delete operation.
type DeleteOption func(*deleteOptions)

type deleteOptions struct {
	expectedVersion *Version
}

// WithDeleteExpectedVersion specifies the expected version for a conditional delete.
// If the current version does not match, the Delete will fail with ErrVersionMismatch.
func WithDeleteExpectedVersion(v Version) DeleteOption {
	return func(o *deleteOptions) {
		o.expectedVersion = &v
	}
}

// ExtractExpectedVersion extracts the expected version from Put options.
// Returns nil if no expected version was specified.
func ExtractExpectedVersion(opts []PutOption) *Version {
	var pOpts putOptions
	for _, opt := range opts {
		opt(&pOpts)
	}
	return pOpts.expectedVersion
}

// ExtractDeleteExpectedVersion extracts the expected version from Delete options.
// Returns nil if no expected version was specified.
func ExtractDeleteExpectedVersion(opts []DeleteOption) *Version {
	var dOpts deleteOptions
	for _, opt := range opts {
		opt(&dOpts)
	}
	return dOpts.expectedVersion
}

// EphemeralOption configures a PutEphemeral operation.
type EphemeralOption func(*ephemeralOptions)

type ephemeralOptions struct {
	expectNotExists bool
	expectedVersion *Version
}

// WithEphemeralExpectNotExists configures PutEphemeral to fail with
// ErrVersionMismatch if the key already exists. Use this for acquiring
// a new lease when you want to ensure no other process has the lease.
func WithEphemeralExpectNotExists() EphemeralOption {
	return func(o *ephemeralOptions) {
		o.expectNotExists = true
	}
}

// WithEphemeralExpectedVersion configures PutEphemeral to fail with
// ErrVersionMismatch if the key's current version doesn't match.
// Use this for renewing a lease you already hold.
func WithEphemeralExpectedVersion(v Version) EphemeralOption {
	return func(o *ephemeralOptions) {
		o.expectedVersion = &v
	}
}

// ExtractEphemeralOptions extracts options from EphemeralOption slice.
func ExtractEphemeralOptions(opts []EphemeralOption) (expectNotExists bool, expectedVersion *Version) {
	var o ephemeralOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o.expectNotExists, o.expectedVersion
}

// Txn represents an atomic transaction on the metadata store.
// All operations within a transaction are executed atomically;
// either all succeed or none are applied.
//
// Transactions are scoped to a single shard domain to ensure
// atomicity. The domain is determined by the scopeKey provided
// to MetadataStore.Txn().
//
// Example usage:
//
//	err := store.Txn(ctx, "/dray/v1/streams/abc123", func(txn metadata.Txn) error {
//	    result, err := txn.Get("/dray/v1/streams/abc123/hwm")
//	    if err != nil {
//	        return err
//	    }
//	    newHwm := parseHwm(result.Value) + recordCount
//	    txn.Put("/dray/v1/streams/abc123/hwm", encodeHwm(newHwm))
//	    txn.Put("/dray/v1/streams/abc123/offset-index/...", indexEntry)
//	    return nil
//	})
type Txn interface {
	// Get retrieves a value within the transaction.
	// Returns the value and version if the key exists.
	// Returns ErrKeyNotFound if the key does not exist.
	Get(key string) (value []byte, version Version, err error)

	// Put queues a write operation within the transaction.
	// The write is applied atomically when the transaction commits.
	Put(key string, value []byte)

	// PutWithVersion queues a conditional write within the transaction.
	// The transaction will fail with ErrVersionMismatch if the current
	// version does not match expectedVersion when the transaction commits.
	PutWithVersion(key string, value []byte, expectedVersion Version)

	// Delete queues a delete operation within the transaction.
	// The delete is applied atomically when the transaction commits.
	Delete(key string)

	// DeleteWithVersion queues a conditional delete within the transaction.
	// The transaction will fail with ErrVersionMismatch if the current
	// version does not match expectedVersion when the transaction commits.
	DeleteWithVersion(key string, expectedVersion Version)
}

// MetadataStore is the interface for metadata storage operations.
// The default implementation uses Oxia as the backing store.
//
// All operations accept a context.Context for cancellation and timeouts.
// Operations may return context.Canceled or context.DeadlineExceeded
// if the context is cancelled or times out.
//
// The MetadataStore interface provides:
//   - Basic key-value operations with optimistic concurrency control
//   - Ordered range queries for offset index lookups
//   - Atomic multi-key transactions within a shard domain
//   - Change notifications for cache invalidation and long-polling
//   - Ephemeral keys for service discovery and lease-based ownership
//
// Example usage:
//
//	// Create store
//	store, err := oxia.New(ctx, oxia.Config{
//	    ServiceAddress: "localhost:6648",
//	    Namespace:      "dray/cluster-1",
//	})
//	if err != nil {
//	    return err
//	}
//	defer store.Close()
//
//	// Write with CAS
//	v, err := store.Put(ctx, "/dray/v1/topics/my-topic", topicData)
//	if err != nil {
//	    return err
//	}
//
//	// Read
//	result, err := store.Get(ctx, "/dray/v1/topics/my-topic")
//	if err != nil {
//	    return err
//	}
//	if !result.Exists {
//	    // Key not found
//	}
//
//	// Range query for offset index
//	entries, err := store.List(ctx, "/dray/v1/streams/abc/offset-index/00000000000000000100", "", 10)
type MetadataStore interface {
	// Get retrieves a value by key.
	// Returns GetResult with Exists=false if the key does not exist (not an error).
	Get(ctx context.Context, key string) (GetResult, error)

	// Put stores a value, optionally with version checking for CAS operations.
	// Returns the new version assigned to the key.
	//
	// Use WithExpectedVersion to require a specific version for the update.
	// If the version does not match, returns ErrVersionMismatch.
	Put(ctx context.Context, key string, value []byte, opts ...PutOption) (Version, error)

	// Delete removes a key, optionally with version checking.
	// Returns nil if the key does not exist (idempotent).
	//
	// Use WithDeleteExpectedVersion to require a specific version for the delete.
	// If the version does not match, returns ErrVersionMismatch.
	Delete(ctx context.Context, key string, opts ...DeleteOption) error

	// List returns keys in the range [startKey, endKey) in lexicographic order.
	// If endKey is empty, returns all keys with the prefix startKey.
	// If limit is 0 or negative, returns all matching keys.
	//
	// Keys are sorted lexicographically, which enables efficient offset index
	// lookups when using zero-padded numeric key components.
	//
	// Example: Find the smallest offset entry > fetchOffset:
	//   entries, _ := store.List(ctx, "/streams/abc/offset-index/00000000000000000100", "", 1)
	List(ctx context.Context, startKey, endKey string, limit int) ([]KV, error)

	// Txn executes an atomic transaction within a single shard domain.
	// The scopeKey determines which shard the transaction runs on;
	// all keys accessed within the transaction must belong to the same shard.
	//
	// The transaction function receives a Txn interface for reading and
	// queueing writes. When the function returns nil, the transaction
	// is committed atomically. If the function returns an error, the
	// transaction is aborted.
	//
	// Returns ErrTxnConflict if the transaction cannot be committed due
	// to concurrent modifications (the caller should retry).
	Txn(ctx context.Context, scopeKey string, fn func(Txn) error) error

	// Notifications returns a stream of change notifications for the
	// namespace. Once subscribed, the caller is guaranteed to receive
	// all subsequent changes even across failures.
	//
	// The notification stream covers the entire namespace configured
	// for this MetadataStore instance (e.g., "dray/<cluster_id>").
	//
	// Use this for:
	//   - Cache invalidation
	//   - Long-poll fetch waiting (wake on hwm increase)
	//   - Service discovery
	Notifications(ctx context.Context) (NotificationStream, error)

	// PutEphemeral stores a value that is automatically deleted when
	// the client session ends (e.g., due to broker crash or disconnect).
	//
	// Use this for:
	//   - Broker registration (/dray/v1/cluster/<id>/brokers/<brokerId>)
	//   - Group coordinator leases (/dray/v1/groups/<groupId>/lease)
	//   - Compaction locks (/dray/v1/compaction/locks/<streamId>)
	PutEphemeral(ctx context.Context, key string, value []byte, opts ...EphemeralOption) (Version, error)

	// Close releases resources held by the store.
	// After Close is called, all operations will return ErrStoreClosed.
	Close() error
}
