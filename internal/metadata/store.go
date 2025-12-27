// Package metadata defines the MetadataStore interface and related types.
package metadata

import "context"

// Version represents a key's version in the metadata store.
type Version int64

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
type Notification struct {
	Key     string
	Value   []byte
	Version Version
	Deleted bool
}

// NotificationStream is an iterator over notifications.
type NotificationStream interface {
	Next(ctx context.Context) (Notification, error)
	Close() error
}

// TxnOp represents an operation in a transaction.
type TxnOp interface {
	isTxnOp()
}

// PutOp is a Put operation in a transaction.
type PutOp struct {
	Key             string
	Value           []byte
	ExpectedVersion *Version // nil means no version check
}

func (PutOp) isTxnOp() {}

// DeleteOp is a Delete operation in a transaction.
type DeleteOp struct {
	Key             string
	ExpectedVersion *Version
}

func (DeleteOp) isTxnOp() {}

// Store is the interface for metadata storage operations.
// The default implementation uses Oxia.
type Store interface {
	// Get retrieves a value by key.
	Get(ctx context.Context, key string) (GetResult, error)

	// Put stores a value, optionally with version checking.
	Put(ctx context.Context, key string, value []byte, expectedVersion *Version) (Version, error)

	// Delete removes a key, optionally with version checking.
	Delete(ctx context.Context, key string, expectedVersion *Version) error

	// List returns all keys matching the prefix.
	List(ctx context.Context, prefix string) ([]KV, error)

	// ListRange returns keys in the range [start, end).
	ListRange(ctx context.Context, start, end string) ([]KV, error)

	// Txn executes multiple operations atomically.
	Txn(ctx context.Context, ops []TxnOp) error

	// PutEphemeral stores a value that is automatically deleted when the session ends.
	PutEphemeral(ctx context.Context, key string, value []byte) (Version, error)

	// Notifications returns a stream of change notifications for keys matching the prefix.
	Notifications(ctx context.Context, prefix string) (NotificationStream, error)

	// Close releases resources.
	Close() error
}
