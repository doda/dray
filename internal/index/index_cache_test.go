package index

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexCacheGet(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "stream-1"
	entry := &IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
		WalID:          "wal-1",
	}

	indexKey, err := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
	require.NoError(t, err)

	// Initially not found
	_, _, ok := cache.Get(streamID, indexKey)
	assert.False(t, ok)

	// Put and get
	cache.Put(streamID, indexKey, entry, 1)

	retrieved, version, ok := cache.Get(streamID, indexKey)
	assert.True(t, ok)
	assert.Equal(t, metadata.Version(1), version)
	assert.Equal(t, entry.EndOffset, retrieved.EndOffset)
}

func TestIndexCacheVersionMonotonicity(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "stream-1"
	indexKey := "/dray/v1/streams/stream-1/offset-index/00000000000000000100/00000000000000001024"

	entry1 := &IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
		WalID:          "wal-1",
	}

	entry2 := &IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
		WalID:          "wal-2", // Different WAL ID
	}

	// Put with version 5
	cache.Put(streamID, indexKey, entry1, 5)

	// Try to put with older version 3 - should be ignored
	cache.Put(streamID, indexKey, entry2, 3)

	retrieved, version, ok := cache.Get(streamID, indexKey)
	assert.True(t, ok)
	assert.Equal(t, metadata.Version(5), version)
	assert.Equal(t, "wal-1", retrieved.WalID) // Still has original

	// Put with newer version 7 - should update
	cache.Put(streamID, indexKey, entry2, 7)

	retrieved, version, ok = cache.Get(streamID, indexKey)
	assert.True(t, ok)
	assert.Equal(t, metadata.Version(7), version)
	assert.Equal(t, "wal-2", retrieved.WalID) // Updated
}

func TestIndexCacheInvalidateStream(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "stream-1"

	// Add multiple entries
	for i := 0; i < 10; i++ {
		entry := &IndexEntry{
			StreamID:       streamID,
			StartOffset:    int64(i * 100),
			EndOffset:      int64((i + 1) * 100),
			CumulativeSize: int64((i + 1) * 1024),
			FileType:       FileTypeWAL,
		}
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		cache.Put(streamID, key, entry, metadata.Version(i+1))
	}

	stats := cache.Stats()
	assert.Equal(t, 1, stats.StreamCount)
	assert.Equal(t, 10, stats.TotalEntries)

	// Invalidate stream
	cache.InvalidateStream(streamID)

	stats = cache.Stats()
	assert.Equal(t, 0, stats.StreamCount)
	assert.Equal(t, 0, stats.TotalEntries)
}

func TestIndexCacheInvalidateEntry(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "stream-1"
	entry := &IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
	}

	indexKey, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
	cache.Put(streamID, indexKey, entry, 1)

	_, _, ok := cache.Get(streamID, indexKey)
	assert.True(t, ok)

	cache.InvalidateEntry(streamID, indexKey)

	_, _, ok = cache.Get(streamID, indexKey)
	assert.False(t, ok)
}

func TestIndexCacheInvalidateAll(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	// Add entries for multiple streams
	for s := 0; s < 3; s++ {
		streamID := "stream-" + string(rune('a'+s))
		for i := 0; i < 5; i++ {
			entry := &IndexEntry{
				StreamID:       streamID,
				StartOffset:    int64(i * 100),
				EndOffset:      int64((i + 1) * 100),
				CumulativeSize: int64((i + 1) * 1024),
				FileType:       FileTypeWAL,
			}
			key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
			cache.Put(streamID, key, entry, metadata.Version(i+1))
		}
	}

	stats := cache.Stats()
	assert.Equal(t, 3, stats.StreamCount)
	assert.Equal(t, 15, stats.TotalEntries)

	cache.InvalidateAll()

	stats = cache.Stats()
	assert.Equal(t, 0, stats.StreamCount)
	assert.Equal(t, 0, stats.TotalEntries)
}

func TestIndexCacheMemoryBound(t *testing.T) {
	store := metadata.NewMockStore()
	config := IndexCacheConfig{
		MaxMemoryBytes:      2000, // Very small limit
		MaxEntriesPerStream: 1000,
	}
	cache := NewIndexCache(store, config)
	defer cache.Close()

	// Add many entries - should trigger eviction
	for i := 0; i < 100; i++ {
		streamID := "stream-" + string(rune('a'+(i%5)))
		entry := &IndexEntry{
			StreamID:       streamID,
			StartOffset:    int64(i * 100),
			EndOffset:      int64((i + 1) * 100),
			CumulativeSize: int64((i + 1) * 1024),
			FileType:       FileTypeWAL,
			WalPath:        "/path/to/wal/with/some/length",
		}
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		cache.Put(streamID, key, entry, metadata.Version(i+1))
	}

	stats := cache.Stats()
	assert.LessOrEqual(t, stats.TotalSizeBytes, config.MaxMemoryBytes)
}

func TestIndexCachePerStreamLimit(t *testing.T) {
	store := metadata.NewMockStore()
	config := IndexCacheConfig{
		MaxMemoryBytes:      64 * 1024 * 1024,
		MaxEntriesPerStream: 5, // Very small limit
	}
	cache := NewIndexCache(store, config)
	defer cache.Close()

	streamID := "stream-1"

	// Add more entries than the limit
	for i := 0; i < 20; i++ {
		entry := &IndexEntry{
			StreamID:       streamID,
			StartOffset:    int64(i * 100),
			EndOffset:      int64((i + 1) * 100),
			CumulativeSize: int64((i + 1) * 1024),
			FileType:       FileTypeWAL,
		}
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		cache.Put(streamID, key, entry, metadata.Version(i+1))
	}

	stats := cache.Stats()
	assert.LessOrEqual(t, stats.TotalEntries, 5)
}

func TestIndexCacheGetEntriesInRange(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "stream-1"

	// Add entries: [0,100), [100,200), [200,300)
	for i := 0; i < 3; i++ {
		entry := &IndexEntry{
			StreamID:       streamID,
			StartOffset:    int64(i * 100),
			EndOffset:      int64((i + 1) * 100),
			CumulativeSize: int64((i + 1) * 1024),
			FileType:       FileTypeWAL,
		}
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		cache.Put(streamID, key, entry, metadata.Version(i+1))
	}

	// Query for offset 50 - should return entry [0,100)
	entries := cache.GetEntriesInRange(streamID, 50, 1)
	require.Len(t, entries, 1)
	assert.Equal(t, int64(0), entries[0].Entry.StartOffset)
	assert.Equal(t, int64(100), entries[0].Entry.EndOffset)

	// Query for offset 150 - should return entry [100,200)
	entries = cache.GetEntriesInRange(streamID, 150, 1)
	require.Len(t, entries, 1)
	assert.Equal(t, int64(100), entries[0].Entry.StartOffset)
}

func TestIndexCacheNotificationInvalidation(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "test-stream-uuid"
	entry := &IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
		WalID:          "wal-1",
	}

	indexKey, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
	cache.Put(streamID, indexKey, entry, 1)

	// Verify entry is cached
	_, _, ok := cache.Get(streamID, indexKey)
	assert.True(t, ok)

	// Simulate notification for updated entry
	updatedEntry := *entry
	updatedEntry.WalID = "wal-2"
	updatedBytes, _ := json.Marshal(updatedEntry)

	store.SimulateNotification(metadata.Notification{
		Key:     indexKey,
		Value:   updatedBytes,
		Version: 2,
		Deleted: false,
	})

	// Give the notification goroutine time to process
	time.Sleep(100 * time.Millisecond)

	// Check cache was updated
	cached, version, ok := cache.Get(streamID, indexKey)
	assert.True(t, ok)
	assert.Equal(t, metadata.Version(2), version)
	assert.Equal(t, "wal-2", cached.WalID)
}

func TestIndexCacheNotificationDelete(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "test-stream-uuid"
	entry := &IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
	}

	indexKey, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
	cache.Put(streamID, indexKey, entry, 1)

	// Verify entry is cached
	_, _, ok := cache.Get(streamID, indexKey)
	assert.True(t, ok)

	// Simulate delete notification
	store.SimulateNotification(metadata.Notification{
		Key:     indexKey,
		Version: 2,
		Deleted: true,
	})

	// Give the notification goroutine time to process
	time.Sleep(100 * time.Millisecond)

	// Verify entry was removed
	_, _, ok = cache.Get(streamID, indexKey)
	assert.False(t, ok)
}

func TestIndexCacheConcurrentAccess(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	const numGoroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			streamID := "stream-" + string(rune('a'+goroutineID%5))

			for i := 0; i < opsPerGoroutine; i++ {
				entry := &IndexEntry{
					StreamID:       streamID,
					StartOffset:    int64(i * 100),
					EndOffset:      int64((i + 1) * 100),
					CumulativeSize: int64((i + 1) * 1024),
					FileType:       FileTypeWAL,
				}
				key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)

				// Mix of operations
				switch i % 3 {
				case 0:
					cache.Put(streamID, key, entry, metadata.Version(i+1))
				case 1:
					cache.Get(streamID, key)
				case 2:
					cache.GetEntriesInRange(streamID, int64(i*50), 5)
				}
			}
		}(g)
	}

	wg.Wait()
	// Test passes if no data races detected
}

func TestIndexCacheClose(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())

	// Add some entries
	entry := &IndexEntry{
		StreamID:       "stream-1",
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
	}
	key, _ := keys.OffsetIndexKeyPath("stream-1", 100, 1024)
	cache.Put("stream-1", key, entry, 1)

	// Close should not block
	err := cache.Close()
	assert.NoError(t, err)

	// Double close should be safe
	err = cache.Close()
	assert.NoError(t, err)
}

func TestIndexCachePutBatch(t *testing.T) {
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	streamID := "stream-1"

	entries := make([]CachedIndexEntry, 5)
	for i := 0; i < 5; i++ {
		entries[i] = CachedIndexEntry{
			Entry: IndexEntry{
				StreamID:       streamID,
				StartOffset:    int64(i * 100),
				EndOffset:      int64((i + 1) * 100),
				CumulativeSize: int64((i + 1) * 1024),
				FileType:       FileTypeWAL,
			},
			Version: metadata.Version(i + 1),
		}
	}

	cache.PutBatch(streamID, entries)

	stats := cache.Stats()
	assert.Equal(t, 1, stats.StreamCount)
	assert.Equal(t, 5, stats.TotalEntries)
}

func TestIndexCacheStats(t *testing.T) {
	store := metadata.NewMockStore()
	config := IndexCacheConfig{
		MaxMemoryBytes:      1024 * 1024,
		MaxEntriesPerStream: 100,
	}
	cache := NewIndexCache(store, config)
	defer cache.Close()

	stats := cache.Stats()
	assert.Equal(t, 0, stats.StreamCount)
	assert.Equal(t, 0, stats.TotalEntries)
	assert.Equal(t, int64(0), stats.TotalSizeBytes)
	assert.Equal(t, int64(1024*1024), stats.MaxSizeBytes)

	// Add entries
	for s := 0; s < 3; s++ {
		streamID := "stream-" + string(rune('a'+s))
		for i := 0; i < 10; i++ {
			entry := &IndexEntry{
				StreamID:       streamID,
				StartOffset:    int64(i * 100),
				EndOffset:      int64((i + 1) * 100),
				CumulativeSize: int64((i + 1) * 1024),
				FileType:       FileTypeWAL,
			}
			key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
			cache.Put(streamID, key, entry, metadata.Version(i+1))
		}
	}

	stats = cache.Stats()
	assert.Equal(t, 3, stats.StreamCount)
	assert.Equal(t, 30, stats.TotalEntries)
	assert.Greater(t, stats.TotalSizeBytes, int64(0))
}

func TestEstimateEntrySize(t *testing.T) {
	entry := &IndexEntry{
		StreamID:       "abc123-uuid-here",
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1024,
		FileType:       FileTypeWAL,
		WalID:          "wal-uuid",
		WalPath:        "/wal/path/to/object",
		BatchIndex: []BatchIndexEntry{
			{BatchStartOffsetDelta: 0, BatchLastOffsetDelta: 10},
			{BatchStartOffsetDelta: 10, BatchLastOffsetDelta: 20},
		},
	}

	size := estimateEntrySize(entry)
	assert.Greater(t, size, int64(200)) // Base size plus strings
}

func TestDefaultIndexCacheConfig(t *testing.T) {
	config := DefaultIndexCacheConfig()
	assert.Equal(t, int64(64*1024*1024), config.MaxMemoryBytes)
	assert.Equal(t, 1000, config.MaxEntriesPerStream)
}

func TestIndexCacheConfigDefaults(t *testing.T) {
	store := metadata.NewMockStore()

	// Zero config should use defaults
	cache := NewIndexCache(store, IndexCacheConfig{})
	defer cache.Close()

	stats := cache.Stats()
	assert.Equal(t, int64(64*1024*1024), stats.MaxSizeBytes)
}

func TestExtractOffsetIndexKeyInfo(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		wantStream  string
		wantOffset  int64
		wantSize    int64
		expectError bool
	}{
		{
			name:        "valid key",
			key:         "/dray/v1/streams/test-stream/offset-index/00000000000000000100/00000000000000001024",
			wantStream:  "test-stream",
			wantOffset:  100,
			wantSize:    1024,
			expectError: false,
		},
		{
			name:        "not an offset-index key",
			key:         "/dray/v1/streams/test-stream/hwm",
			expectError: true,
		},
		{
			name:        "wrong prefix",
			key:         "/other/prefix/key",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamID, endOffset, cumulativeSize, err := extractOffsetIndexKeyInfo(tt.key)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantStream, streamID)
				assert.Equal(t, tt.wantOffset, endOffset)
				assert.Equal(t, tt.wantSize, cumulativeSize)
			}
		})
	}
}

func TestIndexCacheGetOrFetch(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()

	sm := NewStreamManager(store)

	// Create a stream
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	require.NoError(t, err)

	// Append an index entry
	result, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 1024,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          "wal-1",
		WalPath:        "/wal/path",
		ChunkOffset:    0,
		ChunkLength:    1024,
	})
	require.NoError(t, err)

	// First call should fetch from store
	entry, version, err := cache.GetOrFetch(ctx, streamID, result.IndexKey, sm)
	require.NoError(t, err)
	assert.Equal(t, int64(0), entry.StartOffset)
	assert.Equal(t, int64(100), entry.EndOffset)
	assert.Greater(t, version, metadata.Version(0))

	// Second call should hit cache
	entry2, version2, err := cache.GetOrFetch(ctx, streamID, result.IndexKey, sm)
	require.NoError(t, err)
	assert.Equal(t, entry.EndOffset, entry2.EndOffset)
	assert.Equal(t, version, version2)

	// Verify it's cached
	stats := cache.Stats()
	assert.Equal(t, 1, stats.TotalEntries)
}
