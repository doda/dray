package metadata

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
)

// mockStore implements MetadataStore for testing.
type mockStore struct {
	data     map[string]KV
	closed   bool
	nextVer  Version
	txnCalls int
	notifyCh chan Notification
	closeErr error
}

func newMockStore() *mockStore {
	return &mockStore{
		data:     make(map[string]KV),
		nextVer:  1,
		notifyCh: make(chan Notification, 10),
	}
}

func (m *mockStore) Get(_ context.Context, key string) (GetResult, error) {
	if m.closed {
		return GetResult{}, ErrStoreClosed
	}
	kv, ok := m.data[key]
	if !ok {
		return GetResult{Exists: false}, nil
	}
	return GetResult{Value: kv.Value, Version: kv.Version, Exists: true}, nil
}

func (m *mockStore) Put(_ context.Context, key string, value []byte, opts ...PutOption) (Version, error) {
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

func (m *mockStore) Delete(_ context.Context, key string, opts ...DeleteOption) error {
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

func (m *mockStore) List(_ context.Context, startKey, endKey string, limit int) ([]KV, error) {
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

func (m *mockStore) Txn(_ context.Context, _ string, fn func(Txn) error) error {
	if m.closed {
		return ErrStoreClosed
	}
	m.txnCalls++
	txn := &mockTxn{store: m, pending: make(map[string]txnOp)}
	if err := fn(txn); err != nil {
		return err
	}

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

func (m *mockStore) PutEphemeral(_ context.Context, key string, value []byte, _ ...EphemeralOption) (Version, error) {
	if m.closed {
		return 0, ErrStoreClosed
	}
	ver := m.nextVer
	m.nextVer++
	m.data[key] = KV{Key: key, Value: value, Version: ver}
	return ver, nil
}

func (m *mockStore) Notifications(_ context.Context) (NotificationStream, error) {
	if m.closed {
		return nil, ErrStoreClosed
	}
	return &mockNotificationStream{ch: m.notifyCh}, nil
}

func (m *mockStore) Close() error {
	m.closed = true
	close(m.notifyCh)
	return m.closeErr
}

type txnOp struct {
	value           []byte
	delete          bool
	expectedVersion *Version
}

type mockTxn struct {
	store   *mockStore
	pending map[string]txnOp
}

func (t *mockTxn) Get(key string) ([]byte, Version, error) {
	kv, ok := t.store.data[key]
	if !ok {
		return nil, 0, ErrKeyNotFound
	}
	return kv.Value, kv.Version, nil
}

func (t *mockTxn) Put(key string, value []byte) {
	t.pending[key] = txnOp{value: value}
}

func (t *mockTxn) PutWithVersion(key string, value []byte, expectedVersion Version) {
	t.pending[key] = txnOp{value: value, expectedVersion: &expectedVersion}
}

func (t *mockTxn) Delete(key string) {
	t.pending[key] = txnOp{delete: true}
}

func (t *mockTxn) DeleteWithVersion(key string, expectedVersion Version) {
	t.pending[key] = txnOp{delete: true, expectedVersion: &expectedVersion}
}

type mockNotificationStream struct {
	ch     chan Notification
	closed bool
}

func (s *mockNotificationStream) Next(ctx context.Context) (Notification, error) {
	if s.closed {
		return Notification{}, errors.New("stream closed")
	}
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

func (s *mockNotificationStream) Close() error {
	s.closed = true
	return nil
}

// Tests

func TestVersionType(t *testing.T) {
	var v Version = 123
	if v != 123 {
		t.Errorf("Version mismatch: got %d, want 123", v)
	}
	if NoVersion != -1 {
		t.Errorf("NoVersion should be -1, got %d", NoVersion)
	}
}

func TestKVType(t *testing.T) {
	kv := KV{
		Key:     "test-key",
		Value:   []byte("test-value"),
		Version: 42,
	}
	if kv.Key != "test-key" {
		t.Errorf("Key mismatch: got %s", kv.Key)
	}
	if string(kv.Value) != "test-value" {
		t.Errorf("Value mismatch: got %s", kv.Value)
	}
	if kv.Version != 42 {
		t.Errorf("Version mismatch: got %d", kv.Version)
	}
}

func TestGetResult(t *testing.T) {
	result := GetResult{
		Value:   []byte("data"),
		Version: 1,
		Exists:  true,
	}
	if !result.Exists {
		t.Error("Exists should be true")
	}

	emptyResult := GetResult{Exists: false}
	if emptyResult.Exists {
		t.Error("Exists should be false for empty result")
	}
}

func TestNotification(t *testing.T) {
	n := Notification{
		Key:     "/dray/v1/streams/abc/hwm",
		Value:   []byte("100"),
		Version: 5,
		Deleted: false,
	}
	if n.Deleted {
		t.Error("Deleted should be false")
	}

	deleteNotification := Notification{
		Key:     "/dray/v1/streams/abc/hwm",
		Version: 6,
		Deleted: true,
	}
	if !deleteNotification.Deleted {
		t.Error("Deleted should be true")
	}
}

func TestMetadataStoreGetPut(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Put a value
	ver, err := store.Put(ctx, "/test/key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if ver <= 0 {
		t.Errorf("Expected positive version, got %d", ver)
	}

	// Get the value
	result, err := store.Get(ctx, "/test/key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("Key should exist")
	}
	if string(result.Value) != "value1" {
		t.Errorf("Value mismatch: got %s", result.Value)
	}
}

func TestMetadataStoreCAS(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Initial put
	ver1, _ := store.Put(ctx, "/test/cas", []byte("v1"))

	// CAS with correct version
	ver2, err := store.Put(ctx, "/test/cas", []byte("v2"), WithExpectedVersion(ver1))
	if err != nil {
		t.Fatalf("CAS with correct version should succeed: %v", err)
	}
	if ver2 <= ver1 {
		t.Error("New version should be greater than old version")
	}

	// CAS with wrong version
	_, err = store.Put(ctx, "/test/cas", []byte("v3"), WithExpectedVersion(ver1))
	if !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("CAS with wrong version should return ErrVersionMismatch, got %v", err)
	}
}

func TestMetadataStoreDelete(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Create and then delete
	ver, _ := store.Put(ctx, "/test/del", []byte("data"))
	err := store.Delete(ctx, "/test/del")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	result, _ := store.Get(ctx, "/test/del")
	if result.Exists {
		t.Error("Key should not exist after delete")
	}

	// Delete with version check
	store.Put(ctx, "/test/del2", []byte("data"))
	err = store.Delete(ctx, "/test/del2", WithDeleteExpectedVersion(ver))
	if !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("Delete with wrong version should return ErrVersionMismatch, got %v", err)
	}
}

func TestMetadataStoreList(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Add some keys
	store.Put(ctx, "/dray/v1/topics/a", []byte("a"))
	store.Put(ctx, "/dray/v1/topics/b", []byte("b"))
	store.Put(ctx, "/dray/v1/topics/c", []byte("c"))
	store.Put(ctx, "/dray/v1/streams/x", []byte("x"))

	// List with prefix
	result, err := store.List(ctx, "/dray/v1/topics/", "", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 results, got %d", len(result))
	}

	// List with limit
	result, err = store.List(ctx, "/dray/v1/topics/", "", 1)
	if err != nil {
		t.Fatalf("List with limit failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("Expected 1 result with limit, got %d", len(result))
	}
}

func TestMetadataStoreListPrefixOnly(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Add keys with different prefixes
	store.Put(ctx, "/dray/v1/topics/a", []byte("a"))
	store.Put(ctx, "/dray/v1/topics/b", []byte("b"))
	store.Put(ctx, "/dray/v1/topics/c", []byte("c"))
	store.Put(ctx, "/dray/v1/topicsX/d", []byte("d"))  // Note: "topicsX" > "topics/" but doesn't have prefix "/dray/v1/topics/"
	store.Put(ctx, "/dray/v1/streams/x", []byte("x"))  // Different prefix entirely
	store.Put(ctx, "/dray/v2/topics/z", []byte("z"))   // Different version prefix

	// List with prefix should only return keys with exact prefix match
	result, err := store.List(ctx, "/dray/v1/topics/", "", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("Expected 3 results for prefix '/dray/v1/topics/', got %d", len(result))
		for _, kv := range result {
			t.Logf("  Got key: %s", kv.Key)
		}
	}

	// Verify all returned keys have the correct prefix
	for _, kv := range result {
		if !strings.HasPrefix(kv.Key, "/dray/v1/topics/") {
			t.Errorf("Key %s does not have prefix '/dray/v1/topics/'", kv.Key)
		}
	}
}

func TestMetadataStoreListLexicographicOrder(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Add keys in non-sorted order
	store.Put(ctx, "/dray/v1/index/00000000000000000300", []byte("third"))
	store.Put(ctx, "/dray/v1/index/00000000000000000100", []byte("first"))
	store.Put(ctx, "/dray/v1/index/00000000000000000200", []byte("second"))

	// List should return in lexicographic order
	result, err := store.List(ctx, "/dray/v1/index/", "", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(result))
	}

	// Verify order
	if result[0].Key != "/dray/v1/index/00000000000000000100" {
		t.Errorf("First key should be 100, got %s", result[0].Key)
	}
	if result[1].Key != "/dray/v1/index/00000000000000000200" {
		t.Errorf("Second key should be 200, got %s", result[1].Key)
	}
	if result[2].Key != "/dray/v1/index/00000000000000000300" {
		t.Errorf("Third key should be 300, got %s", result[2].Key)
	}

	// List with limit should return first entries in order
	result, err = store.List(ctx, "/dray/v1/index/", "", 2)
	if err != nil {
		t.Fatalf("List with limit failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(result))
	}
	if result[0].Key != "/dray/v1/index/00000000000000000100" {
		t.Errorf("First key should be 100, got %s", result[0].Key)
	}
	if result[1].Key != "/dray/v1/index/00000000000000000200" {
		t.Errorf("Second key should be 200, got %s", result[1].Key)
	}
}

func TestMetadataStoreTxn(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Initial state
	store.Put(ctx, "/dray/v1/streams/abc/hwm", []byte("0"))

	// Run transaction
	err := store.Txn(ctx, "/dray/v1/streams/abc", func(txn Txn) error {
		value, _, err := txn.Get("/dray/v1/streams/abc/hwm")
		if err != nil {
			return err
		}
		if string(value) != "0" {
			t.Errorf("Initial value should be 0, got %s", value)
		}
		txn.Put("/dray/v1/streams/abc/hwm", []byte("100"))
		txn.Put("/dray/v1/streams/abc/offset-index/00100", []byte("entry"))
		return nil
	})
	if err != nil {
		t.Fatalf("Txn failed: %v", err)
	}

	// Verify changes
	result, _ := store.Get(ctx, "/dray/v1/streams/abc/hwm")
	if string(result.Value) != "100" {
		t.Errorf("HWM should be 100 after txn, got %s", result.Value)
	}

	result, _ = store.Get(ctx, "/dray/v1/streams/abc/offset-index/00100")
	if !result.Exists {
		t.Error("Index entry should exist after txn")
	}
}

func TestTxnGetMissingKey(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Run transaction that tries to get a missing key
	err := store.Txn(ctx, "/dray/v1/streams/abc", func(txn Txn) error {
		_, _, err := txn.Get("/dray/v1/streams/missing")
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("Get for missing key should return ErrKeyNotFound, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Txn failed: %v", err)
	}
}

func TestTxnPutWithVersionSuccess(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Create initial key
	store.Put(ctx, "/dray/v1/streams/abc/hwm", []byte("0"))
	result, _ := store.Get(ctx, "/dray/v1/streams/abc/hwm")
	initialVersion := result.Version

	// Run transaction with correct version
	err := store.Txn(ctx, "/dray/v1/streams/abc", func(txn Txn) error {
		txn.PutWithVersion("/dray/v1/streams/abc/hwm", []byte("100"), initialVersion)
		return nil
	})
	if err != nil {
		t.Fatalf("Txn with correct version should succeed: %v", err)
	}

	// Verify the update was applied
	result, _ = store.Get(ctx, "/dray/v1/streams/abc/hwm")
	if string(result.Value) != "100" {
		t.Errorf("Value should be 100, got %s", result.Value)
	}
}

func TestTxnPutWithVersionMismatch(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Create initial key
	store.Put(ctx, "/dray/v1/streams/abc/hwm", []byte("0"))

	// Run transaction with wrong version
	wrongVersion := Version(999)
	err := store.Txn(ctx, "/dray/v1/streams/abc", func(txn Txn) error {
		txn.PutWithVersion("/dray/v1/streams/abc/hwm", []byte("100"), wrongVersion)
		return nil
	})
	if !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("Txn with wrong version should return ErrVersionMismatch, got %v", err)
	}

	// Verify the value was NOT changed
	result, _ := store.Get(ctx, "/dray/v1/streams/abc/hwm")
	if string(result.Value) != "0" {
		t.Errorf("Value should still be 0 after failed txn, got %s", result.Value)
	}
}

func TestTxnDeleteWithVersionSuccess(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Create initial key
	store.Put(ctx, "/dray/v1/streams/abc/hwm", []byte("0"))
	result, _ := store.Get(ctx, "/dray/v1/streams/abc/hwm")
	initialVersion := result.Version

	// Run transaction with correct version
	err := store.Txn(ctx, "/dray/v1/streams/abc", func(txn Txn) error {
		txn.DeleteWithVersion("/dray/v1/streams/abc/hwm", initialVersion)
		return nil
	})
	if err != nil {
		t.Fatalf("Txn delete with correct version should succeed: %v", err)
	}

	// Verify the key was deleted
	result, _ = store.Get(ctx, "/dray/v1/streams/abc/hwm")
	if result.Exists {
		t.Error("Key should not exist after delete")
	}
}

func TestTxnDeleteWithVersionMismatch(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Create initial key
	store.Put(ctx, "/dray/v1/streams/abc/hwm", []byte("0"))

	// Run transaction with wrong version
	wrongVersion := Version(999)
	err := store.Txn(ctx, "/dray/v1/streams/abc", func(txn Txn) error {
		txn.DeleteWithVersion("/dray/v1/streams/abc/hwm", wrongVersion)
		return nil
	})
	if !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("Txn delete with wrong version should return ErrVersionMismatch, got %v", err)
	}

	// Verify the key was NOT deleted
	result, _ := store.Get(ctx, "/dray/v1/streams/abc/hwm")
	if !result.Exists {
		t.Error("Key should still exist after failed delete txn")
	}
}

func TestMetadataStorePutEphemeral(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	ver, err := store.PutEphemeral(ctx, "/dray/v1/cluster/c1/brokers/b1", []byte(`{"zoneId":"us-east-1a"}`))
	if err != nil {
		t.Fatalf("PutEphemeral failed: %v", err)
	}
	if ver <= 0 {
		t.Errorf("Expected positive version, got %d", ver)
	}

	result, _ := store.Get(ctx, "/dray/v1/cluster/c1/brokers/b1")
	if !result.Exists {
		t.Error("Ephemeral key should exist")
	}
}

func TestNotificationStream(t *testing.T) {
	store := newMockStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := store.Notifications(ctx)
	if err != nil {
		t.Fatalf("Notifications failed: %v", err)
	}
	defer stream.Close()

	// Send a notification
	store.notifyCh <- Notification{
		Key:     "/dray/v1/streams/abc/hwm",
		Value:   []byte("200"),
		Version: 10,
	}

	// Receive notification
	notification, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if notification.Key != "/dray/v1/streams/abc/hwm" {
		t.Errorf("Wrong notification key: %s", notification.Key)
	}
}

func TestMetadataStoreClose(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	store.Close()

	_, err := store.Get(ctx, "/test")
	if !errors.Is(err, ErrStoreClosed) {
		t.Errorf("Get after close should return ErrStoreClosed, got %v", err)
	}

	_, err = store.Put(ctx, "/test", []byte("value"))
	if !errors.Is(err, ErrStoreClosed) {
		t.Errorf("Put after close should return ErrStoreClosed, got %v", err)
	}
}

func TestErrors(t *testing.T) {
	// Verify error definitions exist and are distinct
	errs := []error{
		ErrKeyNotFound,
		ErrVersionMismatch,
		ErrTxnConflict,
		ErrSessionExpired,
		ErrStoreClosed,
	}

	for i, e1 := range errs {
		if e1 == nil {
			t.Errorf("Error %d should not be nil", i)
		}
		for j, e2 := range errs {
			if i != j && errors.Is(e1, e2) {
				t.Errorf("Errors %d and %d should be distinct", i, j)
			}
		}
	}
}

// TestMetadataStoreInterfaceCompliance ensures the mock implements MetadataStore correctly.
func TestMetadataStoreInterfaceCompliance(t *testing.T) {
	var _ MetadataStore = (*mockStore)(nil)
}

// TestTxnInterfaceCompliance ensures the mock transaction implements Txn correctly.
func TestTxnInterfaceCompliance(t *testing.T) {
	var _ Txn = (*mockTxn)(nil)
}

// TestNotificationStreamInterfaceCompliance ensures the mock stream implements NotificationStream.
func TestNotificationStreamInterfaceCompliance(t *testing.T) {
	var _ NotificationStream = (*mockNotificationStream)(nil)
}
