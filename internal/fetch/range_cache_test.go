package fetch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// TestRangeCacheBasicOperations tests basic get/put operations.
func TestRangeCacheBasicOperations(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()

	walPath := "wal/0/test.wal"
	startByte := int64(100)
	endByte := int64(199)
	data := []byte("test chunk data")

	// Initially should be a miss
	if _, ok := cache.Get(walPath, startByte, endByte); ok {
		t.Error("Expected cache miss for new key")
	}

	// Put data
	cache.Put(walPath, startByte, endByte, data)

	// Now should hit
	got, ok := cache.Get(walPath, startByte, endByte)
	if !ok {
		t.Fatal("Expected cache hit after Put")
	}
	if string(got) != string(data) {
		t.Errorf("Cache returned wrong data: got %q, want %q", got, data)
	}
}

// TestRangeCacheDataIsolation ensures Get returns a copy, not the original slice.
func TestRangeCacheDataIsolation(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()

	walPath := "wal/0/test.wal"
	data := []byte("original data")
	cache.Put(walPath, 0, 100, data)

	// Modify the original data
	data[0] = 'X'

	// Cache should still have original
	got, ok := cache.Get(walPath, 0, 100)
	if !ok {
		t.Fatal("Expected cache hit")
	}
	if got[0] == 'X' {
		t.Error("Cache data was modified when original was changed")
	}

	// Modify the returned data
	got[0] = 'Y'

	// Get again - should still be original
	got2, _ := cache.Get(walPath, 0, 100)
	if got2[0] == 'Y' {
		t.Error("Cache data was modified when returned slice was changed")
	}
}

// TestRangeCacheExactRangeMatch ensures only exact range matches return data.
func TestRangeCacheExactRangeMatch(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()

	walPath := "wal/0/test.wal"
	cache.Put(walPath, 100, 199, []byte("data"))

	// Exact match should hit
	if _, ok := cache.Get(walPath, 100, 199); !ok {
		t.Error("Expected hit for exact range match")
	}

	// Different start should miss
	if _, ok := cache.Get(walPath, 99, 199); ok {
		t.Error("Expected miss for different start")
	}

	// Different end should miss
	if _, ok := cache.Get(walPath, 100, 200); ok {
		t.Error("Expected miss for different end")
	}

	// Different path should miss
	if _, ok := cache.Get("wal/0/other.wal", 100, 199); ok {
		t.Error("Expected miss for different path")
	}
}

// TestRangeCacheUpdate tests that Put updates existing entries.
func TestRangeCacheUpdate(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()

	walPath := "wal/0/test.wal"
	cache.Put(walPath, 0, 99, []byte("original"))
	cache.Put(walPath, 0, 99, []byte("updated"))

	got, ok := cache.Get(walPath, 0, 99)
	if !ok {
		t.Fatal("Expected cache hit")
	}
	if string(got) != "updated" {
		t.Errorf("Cache not updated: got %q, want %q", got, "updated")
	}
}

// TestRangeCacheMemoryBound tests that memory limits are enforced.
func TestRangeCacheMemoryBound(t *testing.T) {
	config := RangeCacheConfig{
		MaxMemoryBytes:   100, // Very small limit
		MaxEntriesPerWAL: 100,
	}
	cache := NewObjectRangeCache(nil, config)
	defer cache.Close()

	// Add entries that exceed the memory limit
	for i := 0; i < 10; i++ {
		cache.Put("wal/0/test.wal", int64(i*50), int64(i*50+49), make([]byte, 30))
	}

	stats := cache.Stats()
	if stats.TotalSizeBytes > config.MaxMemoryBytes {
		t.Errorf("Cache exceeded memory limit: %d > %d", stats.TotalSizeBytes, config.MaxMemoryBytes)
	}
}

// TestRangeCacheLRUEviction tests that LRU eviction works correctly.
func TestRangeCacheLRUEviction(t *testing.T) {
	config := RangeCacheConfig{
		MaxMemoryBytes:   100, // Small limit
		MaxEntriesPerWAL: 100,
	}
	cache := NewObjectRangeCache(nil, config)
	defer cache.Close()

	// Add three entries (each ~30 bytes, total ~90 bytes fits)
	cache.Put("wal/0/test.wal", 0, 29, make([]byte, 30))
	cache.Put("wal/0/test.wal", 30, 59, make([]byte, 30))
	cache.Put("wal/0/test.wal", 60, 89, make([]byte, 30))

	// Access the first entry to make it "recently used"
	cache.Get("wal/0/test.wal", 0, 29)

	// Add a new entry that will cause eviction
	cache.Put("wal/0/test.wal", 90, 119, make([]byte, 30))

	// First entry should still be present (was accessed)
	if _, ok := cache.Get("wal/0/test.wal", 0, 29); !ok {
		t.Error("Recently accessed entry was evicted")
	}

	// Second entry (oldest, not accessed) might be evicted
	// Third entry or fourth might be present
	stats := cache.Stats()
	if stats.TotalSizeBytes > config.MaxMemoryBytes {
		t.Errorf("Cache exceeded limit after eviction: %d > %d", stats.TotalSizeBytes, config.MaxMemoryBytes)
	}
}

// TestRangeCachePerWALLimit tests max entries per WAL enforcement.
func TestRangeCachePerWALLimit(t *testing.T) {
	config := RangeCacheConfig{
		MaxMemoryBytes:   1024 * 1024, // Large enough
		MaxEntriesPerWAL: 3,           // Small limit
	}
	cache := NewObjectRangeCache(nil, config)
	defer cache.Close()

	walPath := "wal/0/test.wal"

	// Add more entries than the limit
	for i := 0; i < 5; i++ {
		cache.Put(walPath, int64(i*10), int64(i*10+9), []byte("data"))
	}

	// Should have at most MaxEntriesPerWAL entries
	stats := cache.Stats()
	if stats.RangeCount > config.MaxEntriesPerWAL {
		t.Errorf("WAL entry limit exceeded: %d > %d", stats.RangeCount, config.MaxEntriesPerWAL)
	}
}

// TestRangeCacheInvalidateWAL tests WAL-level invalidation.
func TestRangeCacheInvalidateWAL(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()

	wal1 := "wal/0/test1.wal"
	wal2 := "wal/0/test2.wal"

	cache.Put(wal1, 0, 99, []byte("data1"))
	cache.Put(wal1, 100, 199, []byte("data2"))
	cache.Put(wal2, 0, 99, []byte("data3"))

	// Invalidate wal1
	cache.InvalidateWAL(wal1)

	// wal1 entries should be gone
	if _, ok := cache.Get(wal1, 0, 99); ok {
		t.Error("Expected miss after InvalidateWAL")
	}
	if _, ok := cache.Get(wal1, 100, 199); ok {
		t.Error("Expected miss after InvalidateWAL")
	}

	// wal2 should still be present
	if _, ok := cache.Get(wal2, 0, 99); !ok {
		t.Error("Wrong WAL was invalidated")
	}
}

// TestRangeCacheInvalidateAll tests full cache invalidation.
func TestRangeCacheInvalidateAll(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()

	cache.Put("wal/0/test1.wal", 0, 99, []byte("data1"))
	cache.Put("wal/0/test2.wal", 0, 99, []byte("data2"))

	cache.InvalidateAll()

	stats := cache.Stats()
	if stats.RangeCount != 0 {
		t.Errorf("Cache not empty after InvalidateAll: %d entries", stats.RangeCount)
	}
	if stats.TotalSizeBytes != 0 {
		t.Errorf("Cache size not zero after InvalidateAll: %d bytes", stats.TotalSizeBytes)
	}
}

// TestRangeCacheStats tests statistics reporting.
func TestRangeCacheStats(t *testing.T) {
	config := RangeCacheConfig{
		MaxMemoryBytes:   1024,
		MaxEntriesPerWAL: 100,
	}
	cache := NewObjectRangeCache(nil, config)
	defer cache.Close()

	// Empty cache
	stats := cache.Stats()
	if stats.WALCount != 0 || stats.RangeCount != 0 || stats.TotalSizeBytes != 0 {
		t.Error("Empty cache should have zero stats")
	}
	if stats.MaxSizeBytes != config.MaxMemoryBytes {
		t.Errorf("MaxSizeBytes wrong: got %d, want %d", stats.MaxSizeBytes, config.MaxMemoryBytes)
	}

	// Add some entries
	cache.Put("wal/0/test1.wal", 0, 99, []byte("data1"))
	cache.Put("wal/0/test1.wal", 100, 199, []byte("data2"))
	cache.Put("wal/1/test2.wal", 0, 99, []byte("data3"))

	stats = cache.Stats()
	if stats.WALCount != 2 {
		t.Errorf("WALCount wrong: got %d, want 2", stats.WALCount)
	}
	if stats.RangeCount != 3 {
		t.Errorf("RangeCount wrong: got %d, want 3", stats.RangeCount)
	}
	if stats.TotalSizeBytes <= 0 {
		t.Error("TotalSizeBytes should be positive")
	}
}

// TestRangeCacheConcurrentAccess tests thread safety.
func TestRangeCacheConcurrentAccess(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				walPath := "wal/0/test.wal"
				start := int64(j * 10)
				end := start + 9
				data := make([]byte, 10)
				data[0] = byte(id)

				cache.Put(walPath, start, end, data)
				cache.Get(walPath, start, end)

				if j%10 == 0 {
					cache.Stats()
				}
			}
		}(i)
	}

	wg.Wait()

	// Should not panic or deadlock
	stats := cache.Stats()
	if stats.RangeCount < 0 {
		t.Error("Negative range count after concurrent access")
	}
}

// TestRangeCacheClose tests cache shutdown.
func TestRangeCacheClose(t *testing.T) {
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())

	// Add some data
	cache.Put("wal/0/test.wal", 0, 99, []byte("data"))

	// Close should complete without error
	if err := cache.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Multiple closes should be safe
	if err := cache.Close(); err != nil {
		t.Errorf("Second Close returned error: %v", err)
	}
}

// TestRangeCacheWithNotifications tests notification-based invalidation.
func TestRangeCacheWithNotifications(t *testing.T) {
	store := &mockMetadataStore{
		notifyCh: make(chan metadata.Notification, 10),
	}
	cache := NewObjectRangeCache(store, DefaultRangeCacheConfig())
	defer cache.Close()

	// Add data
	walPath := "wal/5/abc123.wal"
	cache.Put(walPath, 0, 99, []byte("data"))

	// Verify it's cached
	if _, ok := cache.Get(walPath, 0, 99); !ok {
		t.Fatal("Data should be cached")
	}

	// Simulate WAL object deletion notification
	store.notifyCh <- metadata.Notification{
		Key:     keys.WALObjectsPrefix + "/5/abc123",
		Deleted: true,
	}

	// Give the notification time to be processed
	time.Sleep(50 * time.Millisecond)

	// Cache should be invalidated
	if _, ok := cache.Get(walPath, 0, 99); ok {
		t.Error("Cache should have been invalidated by notification")
	}
}

// TestRangeCacheGCNotification tests invalidation on GC marker notifications.
func TestRangeCacheGCNotification(t *testing.T) {
	store := &mockMetadataStore{
		notifyCh: make(chan metadata.Notification, 10),
	}
	cache := NewObjectRangeCache(store, DefaultRangeCacheConfig())
	defer cache.Close()

	// Add data
	walPath := "wal/3/def456.wal"
	cache.Put(walPath, 0, 99, []byte("data"))

	// Simulate WAL GC marker deletion (meaning WAL was deleted)
	store.notifyCh <- metadata.Notification{
		Key:     keys.WALGCPrefix + "/3/def456",
		Deleted: true,
	}

	time.Sleep(50 * time.Millisecond)

	// Cache should be invalidated
	if _, ok := cache.Get(walPath, 0, 99); ok {
		t.Error("Cache should have been invalidated by GC notification")
	}
}

// TestExtractWALIDFromNotification tests the WAL ID extraction helper.
func TestExtractWALIDFromNotification(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "WAL object key",
			key:      keys.WALObjectsPrefix + "/5/abc123",
			expected: "abc123",
		},
		{
			name:     "WAL GC key",
			key:      keys.WALGCPrefix + "/3/def456",
			expected: "def456",
		},
		{
			name:     "unrelated key",
			key:      "/dray/v1/topics/test",
			expected: "",
		},
		{
			name:     "partial WAL key",
			key:      keys.WALObjectsPrefix + "/5",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractWALIDFromNotification(tt.key)
			if got != tt.expected {
				t.Errorf("extractWALIDFromNotification(%q) = %q, want %q", tt.key, got, tt.expected)
			}
		})
	}
}

// TestDefaultRangeCacheConfig tests default configuration values.
func TestDefaultRangeCacheConfig(t *testing.T) {
	config := DefaultRangeCacheConfig()

	if config.MaxMemoryBytes != 128*1024*1024 {
		t.Errorf("Default MaxMemoryBytes wrong: got %d, want %d", config.MaxMemoryBytes, 128*1024*1024)
	}
	if config.MaxEntriesPerWAL != 100 {
		t.Errorf("Default MaxEntriesPerWAL wrong: got %d, want 100", config.MaxEntriesPerWAL)
	}
}

// TestRangeCacheDefaultsApplied tests that defaults are applied for zero config values.
func TestRangeCacheDefaultsApplied(t *testing.T) {
	cache := NewObjectRangeCache(nil, RangeCacheConfig{})
	defer cache.Close()

	stats := cache.Stats()
	if stats.MaxSizeBytes != DefaultRangeCacheConfig().MaxMemoryBytes {
		t.Errorf("Default not applied: got %d, want %d", stats.MaxSizeBytes, DefaultRangeCacheConfig().MaxMemoryBytes)
	}
}

// mockMetadataStore is a minimal mock for testing notifications.
type mockMetadataStore struct {
	notifyCh chan metadata.Notification
}

func (m *mockMetadataStore) Get(ctx context.Context, key string) (metadata.GetResult, error) {
	return metadata.GetResult{}, nil
}

func (m *mockMetadataStore) Put(ctx context.Context, key string, value []byte, opts ...metadata.PutOption) (metadata.Version, error) {
	return 1, nil
}

func (m *mockMetadataStore) Delete(ctx context.Context, key string, opts ...metadata.DeleteOption) error {
	return nil
}

func (m *mockMetadataStore) List(ctx context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	return nil, nil
}

func (m *mockMetadataStore) Txn(ctx context.Context, scopeKey string, fn func(metadata.Txn) error) error {
	return nil
}

func (m *mockMetadataStore) Notifications(ctx context.Context) (metadata.NotificationStream, error) {
	return &mockNotificationStream{ch: m.notifyCh, ctx: ctx}, nil
}

func (m *mockMetadataStore) PutEphemeral(ctx context.Context, key string, value []byte, opts ...metadata.EphemeralOption) (metadata.Version, error) {
	return 1, nil
}

func (m *mockMetadataStore) Close() error {
	return nil
}

type mockNotificationStream struct {
	ch     chan metadata.Notification
	ctx    context.Context
	closed bool
}

func (s *mockNotificationStream) Next(ctx context.Context) (metadata.Notification, error) {
	select {
	case <-ctx.Done():
		return metadata.Notification{}, ctx.Err()
	case n, ok := <-s.ch:
		if !ok {
			return metadata.Notification{}, context.Canceled
		}
		return n, nil
	}
}

func (s *mockNotificationStream) Close() error {
	s.closed = true
	return nil
}
