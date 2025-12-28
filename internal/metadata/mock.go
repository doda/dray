package metadata

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
)

// MockStore implements MetadataStore for testing.
// It is exported so that tests in other packages can use it.
type MockStore struct {
	mu       sync.RWMutex
	data     map[string]KV
	closed   bool
	nextVer  Version
	txnCalls int
	notifyCh chan Notification
	closeErr error
}

// NewMockStore creates a new MockStore for testing.
func NewMockStore() *MockStore {
	return &MockStore{
		data:     make(map[string]KV),
		nextVer:  1,
		notifyCh: make(chan Notification, 100),
	}
}

func (m *MockStore) Get(_ context.Context, key string) (GetResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return GetResult{}, ErrStoreClosed
	}
	kv, ok := m.data[key]
	if !ok {
		return GetResult{Exists: false}, nil
	}
	return GetResult{Value: kv.Value, Version: kv.Version, Exists: true}, nil
}

func (m *MockStore) Put(_ context.Context, key string, value []byte, opts ...PutOption) (Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, ErrStoreClosed
	}

	var o putOptions
	for _, opt := range opts {
		opt(&o)
	}

	if o.expectedVersion != nil {
		existing, ok := m.data[key]
		if !ok && *o.expectedVersion != 0 {
			return 0, ErrVersionMismatch
		}
		if ok && existing.Version != *o.expectedVersion {
			return 0, ErrVersionMismatch
		}
	}

	ver := m.nextVer
	m.nextVer++
	m.data[key] = KV{Key: key, Value: value, Version: ver}
	return ver, nil
}

func (m *MockStore) Delete(_ context.Context, key string, opts ...DeleteOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStoreClosed
	}

	var o deleteOptions
	for _, opt := range opts {
		opt(&o)
	}

	if o.expectedVersion != nil {
		existing, ok := m.data[key]
		if !ok {
			return nil // Idempotent delete
		}
		if existing.Version != *o.expectedVersion {
			return ErrVersionMismatch
		}
	}

	delete(m.data, key)
	return nil
}

func (m *MockStore) List(_ context.Context, startKey, endKey string, limit int) ([]KV, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStoreClosed
	}

	// Collect matching keys
	var keys []string
	for k := range m.data {
		if endKey == "" {
			// When endKey is empty, treat startKey as a prefix
			if strings.HasPrefix(k, startKey) {
				keys = append(keys, k)
			}
		} else {
			// Range query: [startKey, endKey)
			if k >= startKey && k < endKey {
				keys = append(keys, k)
			}
		}
	}

	// Sort lexicographically to match MetadataStore contract
	sort.Strings(keys)

	// Apply limit
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	// Build result
	result := make([]KV, len(keys))
	for i, k := range keys {
		result[i] = m.data[k]
	}

	return result, nil
}

func (m *MockStore) Txn(_ context.Context, _ string, fn func(Txn) error) error {
	// Note: We don't hold the lock during the callback to avoid deadlocks
	// when the callback calls other MockStore methods.
	// This differs slightly from a real transactional store but is sufficient for testing.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrStoreClosed
	}
	m.txnCalls++
	m.mu.Unlock()

	txn := &mockTxnPublic{store: m, pending: make(map[string]txnOpPublic)}
	if err := fn(txn); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify all version constraints before applying
	for key, op := range txn.pending {
		if op.expectedVersion != nil {
			existing, ok := m.data[key]
			if !ok && *op.expectedVersion != 0 {
				return ErrVersionMismatch
			}
			if ok && existing.Version != *op.expectedVersion {
				return ErrVersionMismatch
			}
		}
	}

	// Apply pending operations
	for key, op := range txn.pending {
		if op.delete {
			delete(m.data, key)
		} else {
			ver := m.nextVer
			m.nextVer++
			m.data[key] = KV{Key: key, Value: op.value, Version: ver}
		}
	}
	return nil
}

func (m *MockStore) PutEphemeral(_ context.Context, key string, value []byte, _ ...EphemeralOption) (Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, ErrStoreClosed
	}
	ver := m.nextVer
	m.nextVer++
	m.data[key] = KV{Key: key, Value: value, Version: ver}
	return ver, nil
}

func (m *MockStore) Notifications(_ context.Context) (NotificationStream, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStoreClosed
	}
	return &mockNotificationStreamPublic{ch: m.notifyCh}, nil
}

func (m *MockStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true
	close(m.notifyCh)
	return m.closeErr
}

// SimulateNotification sends a notification through the store's notification channel.
// This is useful for testing cache invalidation behavior.
func (m *MockStore) SimulateNotification(n Notification) {
	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()

	if !closed {
		m.notifyCh <- n
	}
}

// TxnCallCount returns the number of times Txn was called (for testing).
func (m *MockStore) TxnCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.txnCalls
}

type txnOpPublic struct {
	value           []byte
	delete          bool
	expectedVersion *Version
}

type mockTxnPublic struct {
	store   *MockStore
	pending map[string]txnOpPublic
}

func (t *mockTxnPublic) Get(key string) ([]byte, Version, error) {
	t.store.mu.RLock()
	defer t.store.mu.RUnlock()
	kv, ok := t.store.data[key]
	if !ok {
		return nil, 0, ErrKeyNotFound
	}
	return kv.Value, kv.Version, nil
}

func (t *mockTxnPublic) Put(key string, value []byte) {
	t.pending[key] = txnOpPublic{value: value}
}

func (t *mockTxnPublic) PutWithVersion(key string, value []byte, expectedVersion Version) {
	t.pending[key] = txnOpPublic{value: value, expectedVersion: &expectedVersion}
}

func (t *mockTxnPublic) Delete(key string) {
	t.pending[key] = txnOpPublic{delete: true}
}

func (t *mockTxnPublic) DeleteWithVersion(key string, expectedVersion Version) {
	t.pending[key] = txnOpPublic{delete: true, expectedVersion: &expectedVersion}
}

type mockNotificationStreamPublic struct {
	ch     chan Notification
	closed bool
	mu     sync.Mutex
}

func (s *mockNotificationStreamPublic) Next(ctx context.Context) (Notification, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Notification{}, errors.New("stream closed")
	}
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return Notification{}, ctx.Err()
	case n, ok := <-s.ch:
		if !ok {
			return Notification{}, errors.New("stream closed")
		}
		return n, nil
	}
}

func (s *mockNotificationStreamPublic) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// Ensure MockStore implements MetadataStore
var _ MetadataStore = (*MockStore)(nil)
