// Package oxia implements the MetadataStore interface using Oxia.
package oxia

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	oxiaclient "github.com/oxia-db/oxia/oxia"

	"github.com/dray-io/dray/internal/metadata"
)

// Config configures the Oxia metadata store.
type Config struct {
	// ServiceAddress is the Oxia service endpoint (e.g., "localhost:6648").
	ServiceAddress string

	// Namespace is the Oxia namespace to use (e.g., "dray/cluster-1").
	// All keys will be scoped to this namespace.
	Namespace string

	// RequestTimeout is the timeout for individual requests.
	// Default: 30 seconds.
	RequestTimeout time.Duration

	// SessionTimeout is the timeout for ephemeral key sessions.
	// When the session expires, all ephemeral keys are deleted.
	// Default: 15 seconds.
	SessionTimeout time.Duration
}

// Store implements MetadataStore using Oxia.
type Store struct {
	client oxiaclient.SyncClient
	config Config

	txnCoordinator *txnCoordinator

	mu     sync.RWMutex
	closed bool
}

// New creates a new Oxia metadata store.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.ServiceAddress == "" {
		return nil, errors.New("oxia: service address is required")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("oxia: namespace is required")
	}

	opts := []oxiaclient.ClientOption{
		oxiaclient.WithNamespace(cfg.Namespace),
	}

	if cfg.RequestTimeout > 0 {
		opts = append(opts, oxiaclient.WithRequestTimeout(cfg.RequestTimeout))
	}
	if cfg.SessionTimeout > 0 {
		opts = append(opts, oxiaclient.WithSessionTimeout(cfg.SessionTimeout))
	}

	client, err := oxiaclient.NewSyncClient(cfg.ServiceAddress, opts...)
	if err != nil {
		return nil, fmt.Errorf("oxia: failed to create client: %w", err)
	}

	txnCoordinator, err := newTxnCoordinator(context.Background(), cfg)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("oxia: failed to create transaction coordinator: %w", err)
	}

	return &Store{
		client:         client,
		config:         cfg,
		txnCoordinator: txnCoordinator,
	}, nil
}

// oxiaToMetadataVersion converts Oxia's 0-based version to our 1-based version.
// Oxia versions start at 0, but our interface uses 0 to mean "key doesn't exist".
func oxiaToMetadataVersion(oxiaVersion int64) metadata.Version {
	return metadata.Version(oxiaVersion + 1)
}

// metadataToOxiaVersion converts our 1-based version to Oxia's 0-based version.
func metadataToOxiaVersion(metaVersion metadata.Version) int64 {
	return int64(metaVersion - 1)
}

// Get retrieves a value by key.
func (s *Store) Get(ctx context.Context, key string) (metadata.GetResult, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return metadata.GetResult{}, metadata.ErrStoreClosed
	}
	s.mu.RUnlock()

	_, value, version, err := s.client.Get(ctx, key)
	if err != nil {
		if errors.Is(err, oxiaclient.ErrKeyNotFound) {
			return metadata.GetResult{Exists: false}, nil
		}
		return metadata.GetResult{}, fmt.Errorf("oxia: get failed: %w", err)
	}

	return metadata.GetResult{
		Value:   value,
		Version: oxiaToMetadataVersion(version.VersionId),
		Exists:  true,
	}, nil
}

// Put stores a value with optional version checking for CAS operations.
func (s *Store) Put(ctx context.Context, key string, value []byte, opts ...metadata.PutOption) (metadata.Version, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, metadata.ErrStoreClosed
	}
	s.mu.RUnlock()

	// Extract expected version from options using the helper
	expectedVersion := metadata.ExtractExpectedVersion(opts)

	var oxiaOpts []oxiaclient.PutOption
	if expectedVersion != nil {
		if *expectedVersion == 0 {
			// Version 0 in our interface means key should not exist
			oxiaOpts = append(oxiaOpts, oxiaclient.ExpectedRecordNotExists())
		} else {
			// Convert from our 1-based version to Oxia's 0-based version
			oxiaOpts = append(oxiaOpts, oxiaclient.ExpectedVersionId(metadataToOxiaVersion(*expectedVersion)))
		}
	}

	_, version, err := s.client.Put(ctx, key, value, oxiaOpts...)
	if err != nil {
		if errors.Is(err, oxiaclient.ErrUnexpectedVersionId) {
			return 0, metadata.ErrVersionMismatch
		}
		return 0, fmt.Errorf("oxia: put failed: %w", err)
	}

	return oxiaToMetadataVersion(version.VersionId), nil
}

// Delete removes a key.
func (s *Store) Delete(ctx context.Context, key string, opts ...metadata.DeleteOption) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return metadata.ErrStoreClosed
	}
	s.mu.RUnlock()

	// Extract expected version from options using the helper
	expectedVersion := metadata.ExtractDeleteExpectedVersion(opts)

	var oxiaOpts []oxiaclient.DeleteOption
	if expectedVersion != nil {
		// Convert from our 1-based version to Oxia's 0-based version
		oxiaOpts = append(oxiaOpts, oxiaclient.ExpectedVersionId(metadataToOxiaVersion(*expectedVersion)))
	}

	err := s.client.Delete(ctx, key, oxiaOpts...)
	if err != nil {
		if errors.Is(err, oxiaclient.ErrKeyNotFound) {
			// Delete is idempotent - key not found is not an error
			return nil
		}
		if errors.Is(err, oxiaclient.ErrUnexpectedVersionId) {
			return metadata.ErrVersionMismatch
		}
		return fmt.Errorf("oxia: delete failed: %w", err)
	}

	return nil
}

// List returns keys in the range [startKey, endKey) in lexicographic order.
func (s *Store) List(ctx context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, metadata.ErrStoreClosed
	}
	s.mu.RUnlock()

	// If endKey is empty, use startKey as a prefix and list all keys with that prefix.
	// Oxia uses a custom key sorting that treats '/' specially.
	// For prefix listing ending with '/', we use the Oxia convention of double slash
	// as the end key to get all direct children. Otherwise, use prefixEnd.
	if endKey == "" {
		if len(startKey) > 0 && startKey[len(startKey)-1] == '/' {
			// Use Oxia's convention for hierarchical key scanning
			endKey = startKey + "/"
		} else {
			endKey = prefixEnd(startKey)
		}
	}

	// Use RangeScan to get keys with values
	results := s.client.RangeScan(ctx, startKey, endKey)

	var kvs []metadata.KV
	for result := range results {
		if result.Err != nil {
			return nil, fmt.Errorf("oxia: list failed: %w", result.Err)
		}

		kvs = append(kvs, metadata.KV{
			Key:     result.Key,
			Value:   result.Value,
			Version: oxiaToMetadataVersion(result.Version.VersionId),
		})

		if limit > 0 && len(kvs) >= limit {
			go drainRangeScan(results)
			return kvs, nil
		}
	}

	return kvs, nil
}

// Txn executes an atomic transaction within a single shard domain.
func (s *Store) Txn(ctx context.Context, scopeKey string, fn func(metadata.Txn) error) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return metadata.ErrStoreClosed
	}
	s.mu.RUnlock()

	txn := &transaction{
		store:    s,
		ctx:      ctx,
		scopeKey: scopeKey,
		reads:    make(map[string]txnRead),
	}

	// Execute the transaction function
	if err := fn(txn); err != nil {
		return err
	}

	// Commit the transaction
	return txn.commit()
}

// Notifications returns a stream of change notifications.
func (s *Store) Notifications(ctx context.Context) (metadata.NotificationStream, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, metadata.ErrStoreClosed
	}
	s.mu.RUnlock()

	oxiaNotifications, err := s.client.GetNotifications()
	if err != nil {
		return nil, fmt.Errorf("oxia: failed to get notifications: %w", err)
	}

	return &notificationStream{
		notifications: oxiaNotifications,
		ctx:           ctx,
	}, nil
}

// PutEphemeral stores a value that is automatically deleted when the client session ends.
func (s *Store) PutEphemeral(ctx context.Context, key string, value []byte, opts ...metadata.EphemeralOption) (metadata.Version, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, metadata.ErrStoreClosed
	}
	s.mu.RUnlock()

	expectNotExists, expectedVersion := metadata.ExtractEphemeralOptions(opts)

	// Build Oxia options
	oxiaOpts := []oxiaclient.PutOption{oxiaclient.Ephemeral()}

	if expectNotExists {
		oxiaOpts = append(oxiaOpts, oxiaclient.ExpectedRecordNotExists())
	} else if expectedVersion != nil {
		oxiaOpts = append(oxiaOpts, oxiaclient.ExpectedVersionId(metadataToOxiaVersion(*expectedVersion)))
	}

	_, version, err := s.client.Put(ctx, key, value, oxiaOpts...)
	if err != nil {
		// Map Oxia errors to metadata errors
		if errors.Is(err, oxiaclient.ErrUnexpectedVersionId) {
			return 0, metadata.ErrVersionMismatch
		}
		return 0, fmt.Errorf("oxia: put ephemeral failed: %w", err)
	}

	return oxiaToMetadataVersion(version.VersionId), nil
}

// Close releases resources held by the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	txnErr := s.txnCoordinator.Close()
	clientErr := s.client.Close()
	if txnErr != nil {
		return txnErr
	}
	return clientErr
}

// prefixEnd returns the key that is lexicographically greater than all keys
// with the given prefix.
func prefixEnd(prefix string) string {
	if prefix == "" {
		return ""
	}

	// Find the last byte that is not 0xFF
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1])
		}
	}

	// All bytes are 0xFF, no end key possible
	return ""
}

func drainRangeScan(results <-chan oxiaclient.GetResult) {
	for range results {
	}
}
