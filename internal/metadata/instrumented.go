package metadata

import (
	"context"
	"time"
)

// OxiaMetricsRecorder is the interface for recording Oxia operation metrics.
// This allows the metadata package to be decoupled from the metrics package.
type OxiaMetricsRecorder interface {
	RecordGet(durationSeconds float64, success bool)
	RecordPut(durationSeconds float64, success bool)
	RecordDelete(durationSeconds float64, success bool)
	RecordList(durationSeconds float64, success bool)
	RecordTxn(durationSeconds float64, success bool)
	RecordPutEphemeral(durationSeconds float64, success bool)
	RecordRetry(operation string)
}

// InstrumentedStore wraps a MetadataStore and records metrics for each operation.
type InstrumentedStore struct {
	store   MetadataStore
	metrics OxiaMetricsRecorder
}

// NewInstrumentedStore creates an instrumented wrapper around a MetadataStore.
// If metrics is nil, no metrics are recorded and operations pass through directly.
func NewInstrumentedStore(store MetadataStore, metrics OxiaMetricsRecorder) *InstrumentedStore {
	return &InstrumentedStore{
		store:   store,
		metrics: metrics,
	}
}

// Get retrieves a value by key.
func (s *InstrumentedStore) Get(ctx context.Context, key string) (GetResult, error) {
	start := time.Now()
	result, err := s.store.Get(ctx, key)
	if s.metrics != nil {
		s.metrics.RecordGet(time.Since(start).Seconds(), err == nil)
	}
	return result, err
}

// Put stores a value with optional version checking for CAS operations.
func (s *InstrumentedStore) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (Version, error) {
	start := time.Now()
	v, err := s.store.Put(ctx, key, value, opts...)
	if s.metrics != nil {
		s.metrics.RecordPut(time.Since(start).Seconds(), err == nil)
	}
	return v, err
}

// Delete removes a key.
func (s *InstrumentedStore) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	start := time.Now()
	err := s.store.Delete(ctx, key, opts...)
	if s.metrics != nil {
		s.metrics.RecordDelete(time.Since(start).Seconds(), err == nil)
	}
	return err
}

// List returns keys in the range [startKey, endKey) in lexicographic order.
func (s *InstrumentedStore) List(ctx context.Context, startKey, endKey string, limit int) ([]KV, error) {
	start := time.Now()
	result, err := s.store.List(ctx, startKey, endKey, limit)
	if s.metrics != nil {
		s.metrics.RecordList(time.Since(start).Seconds(), err == nil)
	}
	return result, err
}

// Txn executes an atomic transaction within a single shard domain.
func (s *InstrumentedStore) Txn(ctx context.Context, scopeKey string, fn func(Txn) error) error {
	start := time.Now()
	err := s.store.Txn(ctx, scopeKey, fn)
	if s.metrics != nil {
		s.metrics.RecordTxn(time.Since(start).Seconds(), err == nil)
	}
	return err
}

// Notifications returns a stream of change notifications.
func (s *InstrumentedStore) Notifications(ctx context.Context) (NotificationStream, error) {
	// Notifications don't need latency tracking since they're long-lived streams
	return s.store.Notifications(ctx)
}

// PutEphemeral stores a value that is automatically deleted when the client session ends.
func (s *InstrumentedStore) PutEphemeral(ctx context.Context, key string, value []byte, opts ...EphemeralOption) (Version, error) {
	start := time.Now()
	v, err := s.store.PutEphemeral(ctx, key, value, opts...)
	if s.metrics != nil {
		s.metrics.RecordPutEphemeral(time.Since(start).Seconds(), err == nil)
	}
	return v, err
}

// Close releases resources held by the store.
func (s *InstrumentedStore) Close() error {
	return s.store.Close()
}

// Ensure InstrumentedStore implements MetadataStore.
var _ MetadataStore = (*InstrumentedStore)(nil)
