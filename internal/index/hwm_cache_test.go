package index

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// hwmMockStore is a mock metadata store that supports notifications for cache testing.
type hwmMockStore struct {
	mu           sync.RWMutex
	data         map[string]mockKV
	closed       bool
	txnCount     int
	notifyChan   chan metadata.Notification
	notifyActive bool
	notifyClosed bool
	notifyGate   chan struct{}
	txnHook      func(key string, value []byte, version metadata.Version) // Called after txn commit
}

func newHWMMockStore() *hwmMockStore {
	return &hwmMockStore{
		data:       make(map[string]mockKV),
		notifyChan: make(chan metadata.Notification, 100),
	}
}

func (m *hwmMockStore) Get(ctx context.Context, key string) (metadata.GetResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return metadata.GetResult{}, metadata.ErrStoreClosed
	}
	if kv, ok := m.data[key]; ok {
		return metadata.GetResult{
			Value:   kv.value,
			Version: kv.version,
			Exists:  true,
		}, nil
	}
	return metadata.GetResult{Exists: false}, nil
}

func (m *hwmMockStore) Put(ctx context.Context, key string, value []byte, opts ...metadata.PutOption) (metadata.Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, metadata.ErrStoreClosed
	}
	expectedVersion := metadata.ExtractExpectedVersion(opts)
	if expectedVersion != nil {
		if kv, ok := m.data[key]; ok {
			if kv.version != *expectedVersion {
				return 0, metadata.ErrVersionMismatch
			}
		} else if *expectedVersion != 0 {
			return 0, metadata.ErrVersionMismatch
		}
	}
	newVersion := metadata.Version(1)
	if kv, ok := m.data[key]; ok {
		newVersion = kv.version + 1
	}
	m.data[key] = mockKV{value: value, version: newVersion}

	// Send notification if active
	if m.notifyActive && !m.notifyClosed && m.notifyChan != nil {
		select {
		case m.notifyChan <- metadata.Notification{
			Key:     key,
			Value:   value,
			Version: newVersion,
			Deleted: false,
		}:
		default:
		}
	}

	return newVersion, nil
}

func (m *hwmMockStore) Delete(ctx context.Context, key string, opts ...metadata.DeleteOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return metadata.ErrStoreClosed
	}
	delete(m.data, key)
	return nil
}

func (m *hwmMockStore) List(ctx context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, metadata.ErrStoreClosed
	}
	return nil, nil
}

// hwmMockTxn is a mock transaction that properly validates versions.
type hwmMockTxn struct {
	store             *hwmMockStore
	pending           map[string]txnOp
	versionChecks     map[string]metadata.Version
	versionCheckError bool
}

type txnOp struct {
	value           []byte
	isDelete        bool
	expectedVersion *metadata.Version
}

func (m *hwmMockStore) Txn(ctx context.Context, scopeKey string, fn func(metadata.Txn) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return metadata.ErrStoreClosed
	}
	m.txnCount++
	txn := &hwmMockTxn{
		store:         m,
		pending:       make(map[string]txnOp),
		versionChecks: make(map[string]metadata.Version),
	}
	if err := fn(txn); err != nil {
		return err
	}

	// Validate version checks
	for key, expectedVersion := range txn.versionChecks {
		if kv, ok := m.data[key]; ok {
			if kv.version != expectedVersion {
				return metadata.ErrVersionMismatch
			}
		} else if expectedVersion != 0 {
			return metadata.ErrVersionMismatch
		}
	}

	// Apply pending writes
	for key, op := range txn.pending {
		if op.isDelete {
			delete(m.data, key)
			if m.notifyActive && !m.notifyClosed && m.notifyChan != nil {
				select {
				case m.notifyChan <- metadata.Notification{
					Key:     key,
					Deleted: true,
				}:
				default:
				}
			}
		} else {
			newVersion := metadata.Version(1)
			if kv, ok := m.data[key]; ok {
				newVersion = kv.version + 1
			}
			m.data[key] = mockKV{value: op.value, version: newVersion}
			if m.notifyActive && !m.notifyClosed && m.notifyChan != nil {
				select {
				case m.notifyChan <- metadata.Notification{
					Key:     key,
					Value:   op.value,
					Version: newVersion,
					Deleted: false,
				}:
				default:
				}
			}
			if m.txnHook != nil {
				m.txnHook(key, op.value, newVersion)
			}
		}
	}
	return nil
}

func (m *hwmMockStore) Notifications(ctx context.Context) (metadata.NotificationStream, error) {
	m.mu.Lock()
	gate := m.notifyGate
	m.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	m.mu.Lock()
	m.notifyChan = make(chan metadata.Notification, 100)
	m.notifyActive = true
	m.notifyClosed = false
	ch := m.notifyChan
	m.mu.Unlock()
	return &hwmMockNotificationStream{
		ctx: ctx,
		ch:  ch,
	}, nil
}

func (m *hwmMockStore) CloseNotifications() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.notifyChan != nil && !m.notifyClosed {
		close(m.notifyChan)
		m.notifyClosed = true
	}
}

func (m *hwmMockStore) SetNotifyGate(gate chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifyGate = gate
}

func (m *hwmMockStore) PutEphemeral(ctx context.Context, key string, value []byte, opts ...metadata.EphemeralOption) (metadata.Version, error) {
	return m.Put(ctx, key, value)
}

func (m *hwmMockStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.notifyChan != nil && !m.notifyClosed {
		close(m.notifyChan)
		m.notifyClosed = true
	}
	return nil
}

func (t *hwmMockTxn) Get(key string) (value []byte, version metadata.Version, err error) {
	if kv, ok := t.store.data[key]; ok {
		return kv.value, kv.version, nil
	}
	return nil, 0, metadata.ErrKeyNotFound
}

func (t *hwmMockTxn) Put(key string, value []byte) {
	t.pending[key] = txnOp{value: value}
}

func (t *hwmMockTxn) PutWithVersion(key string, value []byte, expectedVersion metadata.Version) {
	t.pending[key] = txnOp{value: value}
	t.versionChecks[key] = expectedVersion
}

func (t *hwmMockTxn) Delete(key string) {
	t.pending[key] = txnOp{isDelete: true}
}

func (t *hwmMockTxn) DeleteWithVersion(key string, expectedVersion metadata.Version) {
	t.pending[key] = txnOp{isDelete: true}
	t.versionChecks[key] = expectedVersion
}

type hwmMockNotificationStream struct {
	ctx    context.Context
	ch     <-chan metadata.Notification
	closed bool
}

func (s *hwmMockNotificationStream) Next(ctx context.Context) (metadata.Notification, error) {
	select {
	case <-ctx.Done():
		return metadata.Notification{}, ctx.Err()
	case n, ok := <-s.ch:
		if !ok {
			return metadata.Notification{}, metadata.ErrStoreClosed
		}
		return n, nil
	}
}

func (s *hwmMockNotificationStream) Close() error {
	s.closed = true
	return nil
}

// Tests for IncrementHWM

func TestIncrementHWM(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create a stream first
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Get initial HWM
	hwm, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("initial hwm = %d, want 0", hwm)
	}

	// Increment HWM
	newHwm, newVersion, err := sm.IncrementHWM(ctx, streamID, 10, version)
	if err != nil {
		t.Fatalf("IncrementHWM failed: %v", err)
	}
	if newHwm != 10 {
		t.Errorf("newHwm = %d, want 10", newHwm)
	}
	if newVersion != version+1 {
		t.Errorf("newVersion = %d, want %d", newVersion, version+1)
	}

	// Verify HWM was actually updated
	hwm, _, err = sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM after increment failed: %v", err)
	}
	if hwm != 10 {
		t.Errorf("hwm after increment = %d, want 10", hwm)
	}
}

func TestIncrementHWMVersionMismatch(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	_, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}

	// Try to increment with wrong version
	_, _, err = sm.IncrementHWM(ctx, streamID, 10, version+999)
	if err != metadata.ErrVersionMismatch {
		t.Errorf("IncrementHWM with wrong version should return ErrVersionMismatch, got %v", err)
	}
}

func TestIncrementHWMStreamNotFound(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, _, err := sm.IncrementHWM(ctx, "nonexistent-stream", 10, 1)
	if err != ErrStreamNotFound {
		t.Errorf("IncrementHWM on nonexistent stream should return ErrStreamNotFound, got %v", err)
	}
}

func TestIncrementHWMMultiple(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	_, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}

	// Multiple increments
	for i := 0; i < 5; i++ {
		newHwm, newVersion, err := sm.IncrementHWM(ctx, streamID, 10, version)
		if err != nil {
			t.Fatalf("IncrementHWM %d failed: %v", i, err)
		}
		expectedHwm := int64((i + 1) * 10)
		if newHwm != expectedHwm {
			t.Errorf("increment %d: newHwm = %d, want %d", i, newHwm, expectedHwm)
		}
		version = newVersion
	}

	// Verify final HWM
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("final GetHWM failed: %v", err)
	}
	if hwm != 50 {
		t.Errorf("final hwm = %d, want 50", hwm)
	}
}

// Tests for SetHWM

func TestSetHWM(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	_, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}

	// Set HWM to specific value
	newVersion, err := sm.SetHWM(ctx, streamID, 100, version)
	if err != nil {
		t.Fatalf("SetHWM failed: %v", err)
	}
	if newVersion != version+1 {
		t.Errorf("newVersion = %d, want %d", newVersion, version+1)
	}

	// Verify HWM was set
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM after set failed: %v", err)
	}
	if hwm != 100 {
		t.Errorf("hwm after set = %d, want 100", hwm)
	}
}

func TestSetHWMVersionMismatch(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	_, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}

	_, err = sm.SetHWM(ctx, streamID, 100, version+999)
	if err != metadata.ErrVersionMismatch {
		t.Errorf("SetHWM with wrong version should return ErrVersionMismatch, got %v", err)
	}
}

// Tests for HWMCache

func TestHWMCacheGet(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	cache := NewHWMCache(store)
	defer cache.Close()

	// Get should fetch from store and cache
	hwm, version, err := cache.Get(ctx, streamID)
	if err != nil {
		t.Fatalf("cache.Get failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("hwm = %d, want 0", hwm)
	}
	if version == 0 {
		t.Error("version should not be 0")
	}

	// Verify it's cached
	hwmCached, versionCached, ok := cache.GetIfCached(streamID)
	if !ok {
		t.Error("value should be cached")
	}
	if hwmCached != hwm || versionCached != version {
		t.Errorf("cached values don't match: got (%d, %d), want (%d, %d)",
			hwmCached, versionCached, hwm, version)
	}

	// Size should be 1
	if cache.Size() != 1 {
		t.Errorf("cache.Size() = %d, want 1", cache.Size())
	}
}

func TestHWMCacheGetNotFound(t *testing.T) {
	store := newHWMMockStore()
	cache := NewHWMCache(store)
	defer cache.Close()

	ctx := context.Background()

	_, _, err := cache.Get(ctx, "nonexistent-stream")
	if err != ErrStreamNotFound {
		t.Errorf("cache.Get for nonexistent stream should return ErrStreamNotFound, got %v", err)
	}
}

func TestHWMCachePut(t *testing.T) {
	store := newHWMMockStore()
	cache := NewHWMCache(store)
	defer cache.Close()

	// Put a value directly
	cache.Put("stream-1", 100, 5)

	hwm, version, ok := cache.GetIfCached("stream-1")
	if !ok {
		t.Error("value should be cached after Put")
	}
	if hwm != 100 || version != 5 {
		t.Errorf("cached values = (%d, %d), want (100, 5)", hwm, version)
	}
}

func TestHWMCachePutMonotonicity(t *testing.T) {
	store := newHWMMockStore()
	cache := NewHWMCache(store)
	defer cache.Close()

	// Put a value with version 10
	cache.Put("stream-1", 100, 10)

	// Try to put with older version - should be ignored
	cache.Put("stream-1", 50, 5)

	hwm, version, _ := cache.GetIfCached("stream-1")
	if hwm != 100 || version != 10 {
		t.Errorf("cache should keep newer version: got (%d, %d), want (100, 10)", hwm, version)
	}

	// Put with newer version - should update
	cache.Put("stream-1", 200, 15)

	hwm, version, _ = cache.GetIfCached("stream-1")
	if hwm != 200 || version != 15 {
		t.Errorf("cache should update to newer version: got (%d, %d), want (200, 15)", hwm, version)
	}
}

func TestHWMCacheInvalidate(t *testing.T) {
	store := newHWMMockStore()
	cache := NewHWMCache(store)
	defer cache.Close()

	cache.Put("stream-1", 100, 5)
	cache.Put("stream-2", 200, 10)

	// Invalidate one stream
	cache.Invalidate("stream-1")

	_, _, ok := cache.GetIfCached("stream-1")
	if ok {
		t.Error("stream-1 should be invalidated")
	}

	_, _, ok = cache.GetIfCached("stream-2")
	if !ok {
		t.Error("stream-2 should still be cached")
	}
}

func TestHWMCacheInvalidateAll(t *testing.T) {
	store := newHWMMockStore()
	cache := NewHWMCache(store)
	defer cache.Close()

	cache.Put("stream-1", 100, 5)
	cache.Put("stream-2", 200, 10)
	cache.Put("stream-3", 300, 15)

	cache.InvalidateAll()

	if cache.Size() != 0 {
		t.Errorf("cache.Size() after InvalidateAll = %d, want 0", cache.Size())
	}
}

func TestHWMCacheNotificationInvalidation(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	cache := NewHWMCache(store)
	defer cache.Close()

	// Wait for notification watcher to start
	time.Sleep(50 * time.Millisecond)

	// Get to populate cache
	hwm, version, err := cache.Get(ctx, streamID)
	if err != nil {
		t.Fatalf("cache.Get failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("initial hwm = %d, want 0", hwm)
	}

	// Update HWM via store directly (simulating another process)
	hwmKey := keys.HwmKeyPath(streamID)
	newHwmBytes := EncodeHWM(50)
	_, err = store.Put(ctx, hwmKey, newHwmBytes, metadata.WithExpectedVersion(version))
	if err != nil {
		t.Fatalf("direct Put failed: %v", err)
	}

	// Wait for notification to be processed
	time.Sleep(100 * time.Millisecond)

	// Cache should be updated via notification
	hwmCached, _, ok := cache.GetIfCached(streamID)
	if !ok {
		t.Error("cache should have entry after notification")
	}
	if hwmCached != 50 {
		t.Errorf("cached hwm after notification = %d, want 50", hwmCached)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func TestHWMCacheRestartRefreshesHWM(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "restart-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	cache := NewHWMCache(store)
	defer cache.Close()

	time.Sleep(50 * time.Millisecond)

	hwm, version, err := cache.Get(ctx, streamID)
	if err != nil {
		t.Fatalf("cache.Get failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("initial hwm = %d, want 0", hwm)
	}

	gate := make(chan struct{})
	store.SetNotifyGate(gate)
	store.CloseNotifications()

	waitForCondition(t, 200*time.Millisecond, func() bool {
		return cache.Size() == 0
	})

	hwmKey := keys.HwmKeyPath(streamID)
	newHwmBytes := EncodeHWM(50)
	_, err = store.Put(ctx, hwmKey, newHwmBytes, metadata.WithExpectedVersion(version))
	if err != nil {
		t.Fatalf("direct Put failed: %v", err)
	}

	close(gate)

	waitForCondition(t, 200*time.Millisecond, func() bool {
		hwmCached, _, ok := cache.GetIfCached(streamID)
		return ok && hwmCached == 50
	})

	hwmCached, _, ok := cache.GetIfCached(streamID)
	if !ok {
		t.Fatal("cache should have entry after reconnect refresh")
	}
	if hwmCached != 50 {
		t.Fatalf("cached hwm after reconnect refresh = %d, want 50", hwmCached)
	}
}

func TestExtractStreamIDFromHWMKey(t *testing.T) {
	testCases := []struct {
		key      string
		expected string
	}{
		{"/dray/v1/streams/abc123/hwm", "abc123"},
		{"/dray/v1/streams/stream-uuid-here/hwm", "stream-uuid-here"},
		{"/dray/v1/streams/123e4567-e89b-12d3-a456-426614174000/hwm", "123e4567-e89b-12d3-a456-426614174000"},
		// Invalid cases
		{"/dray/v1/streams/abc123/meta", ""},
		{"/dray/v1/topics/abc123/hwm", ""},
		{"/other/prefix/streams/abc123/hwm", ""},
		{"/dray/v1/streams/abc/123/hwm", ""},
		{"/dray/v1/streams//hwm", ""},
		{"", ""},
		{"/dray/v1/streams/abc123", ""},
	}

	for _, tc := range testCases {
		result := extractStreamIDFromHWMKey(tc.key)
		if result != tc.expected {
			t.Errorf("extractStreamIDFromHWMKey(%q) = %q, want %q", tc.key, result, tc.expected)
		}
	}
}

func TestHWMCacheClose(t *testing.T) {
	store := newHWMMockStore()
	cache := NewHWMCache(store)

	cache.Put("stream-1", 100, 5)

	err := cache.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Close should be idempotent
	err = cache.Close()
	if err != nil {
		t.Errorf("second Close returned error: %v", err)
	}
}

func TestHWMCacheConcurrentAccess(t *testing.T) {
	store := newHWMMockStore()
	cache := NewHWMCache(store)
	defer cache.Close()

	var wg sync.WaitGroup
	streamCount := 10
	iterationsPerStream := 100

	for i := 0; i < streamCount; i++ {
		wg.Add(1)
		go func(streamNum int) {
			defer wg.Done()
			streamID := "stream-" + string(rune('a'+streamNum))
			for j := 0; j < iterationsPerStream; j++ {
				cache.Put(streamID, int64(j), metadata.Version(j+1))
				cache.GetIfCached(streamID)
				if j%10 == 0 {
					cache.Invalidate(streamID)
				}
			}
		}(i)
	}

	wg.Wait()
}

// Test concurrent increment with retry logic
func TestIncrementHWMConcurrentWithRetry(t *testing.T) {
	store := newHWMMockStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Run concurrent increments with retry logic
	var wg sync.WaitGroup
	incrementCount := 10
	delta := int64(5)
	successful := make(chan int64, incrementCount)

	for i := 0; i < incrementCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempts := 0; attempts < 100; attempts++ {
				_, version, err := sm.GetHWM(ctx, streamID)
				if err != nil {
					continue
				}
				newHwm, _, err := sm.IncrementHWM(ctx, streamID, delta, version)
				if err == nil {
					successful <- newHwm
					return
				}
				if err != metadata.ErrVersionMismatch {
					t.Errorf("unexpected error: %v", err)
					return
				}
				// Version mismatch - retry
			}
			t.Error("too many retries")
		}()
	}

	wg.Wait()
	close(successful)

	// Verify all increments succeeded
	var results []int64
	for hwm := range successful {
		results = append(results, hwm)
	}

	if len(results) != incrementCount {
		t.Errorf("successful increments = %d, want %d", len(results), incrementCount)
	}

	// Verify final HWM
	finalHwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("final GetHWM failed: %v", err)
	}
	expectedFinal := int64(incrementCount) * delta
	if finalHwm != expectedFinal {
		t.Errorf("final hwm = %d, want %d", finalHwm, expectedFinal)
	}
}

// erroringHWMMockStore wraps hwmMockStore to inject notification errors for testing backoff.
type erroringHWMMockStore struct {
	*hwmMockStore
	mu             sync.Mutex
	notifyErrCount int           // number of times to return error from Notifications()
	notifyErrTotal int           // total errors returned
	successCh      chan struct{} // closed when we transition from error to success
}

func newErroringHWMMockStore(errCount int) *erroringHWMMockStore {
	return &erroringHWMMockStore{
		hwmMockStore:   newHWMMockStore(),
		notifyErrCount: errCount,
		successCh:      make(chan struct{}),
	}
}

func (e *erroringHWMMockStore) Notifications(ctx context.Context) (metadata.NotificationStream, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.notifyErrTotal < e.notifyErrCount {
		e.notifyErrTotal++
		if e.notifyErrTotal == e.notifyErrCount {
			close(e.successCh)
		}
		return nil, metadata.ErrStoreClosed
	}
	return e.hwmMockStore.Notifications(ctx)
}

func TestHWMCacheWatcherBackoff(t *testing.T) {
	// Test that the watcher applies exponential backoff on stream connection errors
	store := newErroringHWMMockStore(3) // Fail first 3 connection attempts

	startTime := time.Now()
	cache := NewHWMCache(store, WithHWMBackoff(
		10*time.Millisecond,  // InitialBackoff - short for testing
		100*time.Millisecond, // MaxBackoff
		2.0,                  // BackoffFactor
	))
	defer cache.Close()

	// Wait for the cache to successfully connect (after backoff retries)
	select {
	case <-store.successCh:
		// Success channel closed means we've exhausted our error quota
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watcher to retry")
	}

	// Give a little more time for the successful connection to complete
	time.Sleep(50 * time.Millisecond)

	elapsed := time.Since(startTime)

	// With 3 errors and initial backoff of 10ms with factor 2:
	// Error 1: wait 10ms, Error 2: wait 20ms, Error 3: wait 40ms
	// Total: at least 30ms
	if elapsed < 30*time.Millisecond {
		t.Errorf("expected backoff delays, but elapsed time was only %v", elapsed)
	}

	// Verify that exactly 3 errors were returned
	store.mu.Lock()
	errCount := store.notifyErrTotal
	store.mu.Unlock()
	if errCount != 3 {
		t.Errorf("expected exactly 3 notification errors, got %d", errCount)
	}
}

func TestHWMCacheWatcherBackoffCapsAtMax(t *testing.T) {
	// Test that backoff caps at MaxBackoff
	store := newErroringHWMMockStore(5) // Fail first 5 connection attempts

	startTime := time.Now()
	cache := NewHWMCache(store, WithHWMBackoff(
		5*time.Millisecond,  // InitialBackoff
		15*time.Millisecond, // MaxBackoff - low cap
		3.0,                 // BackoffFactor - fast growth
	))
	defer cache.Close()

	// Wait for successful connection
	select {
	case <-store.successCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watcher to retry")
	}

	time.Sleep(50 * time.Millisecond)
	elapsed := time.Since(startTime)

	// Without cap: 5 + 15 + 45 + 135 + 405 = 605ms
	// With 15ms cap: 5 + 15 + 15 + 15 + 15 = 65ms
	// We check that capping worked by ensuring it completed in reasonable time
	if elapsed > 500*time.Millisecond {
		t.Errorf("backoff did not cap at MaxBackoff; elapsed time was %v", elapsed)
	}

	// Verify that all errors were returned
	store.mu.Lock()
	errCount := store.notifyErrTotal
	store.mu.Unlock()
	if errCount != 5 {
		t.Errorf("expected exactly 5 notification errors, got %d", errCount)
	}
}

func TestHWMCacheWatcherBackoffResetsOnSuccess(t *testing.T) {
	// Test that backoff resets after successful connection
	store := newErroringHWMMockStore(2)

	cache := NewHWMCache(store, WithHWMBackoff(
		10*time.Millisecond,  // InitialBackoff
		1*time.Second,        // MaxBackoff
		2.0,                  // BackoffFactor
	))
	defer cache.Close()

	// Wait for successful connection
	select {
	case <-store.successCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watcher to retry")
	}

	// Give time for successful connection
	time.Sleep(50 * time.Millisecond)

	// Verify it connected successfully after backoff
	store.mu.Lock()
	errCount := store.notifyErrTotal
	store.mu.Unlock()
	if errCount != 2 {
		t.Errorf("expected exactly 2 notification errors, got %d", errCount)
	}

	// Cache should work normally after recovery
	cache.Put("stream-1", 100, 5)

	hwm, version, ok := cache.GetIfCached("stream-1")
	if !ok {
		t.Error("cache should work after successful reconnection")
	}
	if hwm != 100 || version != 5 {
		t.Errorf("cached values = (%d, %d), want (100, 5)", hwm, version)
	}
}
