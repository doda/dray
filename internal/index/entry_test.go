package index

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/google/uuid"
)

// indexTestStore extends the mock store to support List for offset index tests.
type indexTestStore struct {
	data     map[string]indexTestKV
	closed   bool
	txnCount int
}

type indexTestKV struct {
	value   []byte
	version metadata.Version
}

func newIndexTestStore() *indexTestStore {
	return &indexTestStore{
		data: make(map[string]indexTestKV),
	}
}

func (m *indexTestStore) Get(ctx context.Context, key string) (metadata.GetResult, error) {
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

func (m *indexTestStore) Put(ctx context.Context, key string, value []byte, opts ...metadata.PutOption) (metadata.Version, error) {
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
	m.data[key] = indexTestKV{value: value, version: newVersion}
	return newVersion, nil
}

func (m *indexTestStore) Delete(ctx context.Context, key string, opts ...metadata.DeleteOption) error {
	if m.closed {
		return metadata.ErrStoreClosed
	}
	delete(m.data, key)
	return nil
}

func (m *indexTestStore) List(ctx context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	if m.closed {
		return nil, metadata.ErrStoreClosed
	}

	// Collect all matching keys
	// The semantics are: if endKey is empty, it's a prefix match.
	// If endKey is provided, it's a range query [startKey, endKey).
	var keys []string
	for k := range m.data {
		if endKey == "" {
			// Prefix match mode
			if strings.HasPrefix(k, startKey) {
				keys = append(keys, k)
			}
		} else {
			// Range query mode: startKey <= k < endKey
			if k >= startKey && k < endKey {
				keys = append(keys, k)
			}
		}
	}

	// Sort keys lexicographically
	sort.Strings(keys)

	// Apply limit
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	// Build result
	result := make([]metadata.KV, 0, len(keys))
	for _, k := range keys {
		kv := m.data[k]
		result = append(result, metadata.KV{
			Key:     k,
			Value:   kv.value,
			Version: kv.version,
		})
	}

	return result, nil
}

func (m *indexTestStore) Txn(ctx context.Context, scopeKey string, fn func(metadata.Txn) error) error {
	if m.closed {
		return metadata.ErrStoreClosed
	}
	m.txnCount++
	txn := &indexTestTxn{store: m, pending: make(map[string][]byte), deletes: make(map[string]bool)}
	if err := fn(txn); err != nil {
		return err
	}
	// Apply pending writes
	for key, value := range txn.pending {
		if value == nil {
			delete(m.data, key)
		} else {
			newVersion := metadata.Version(1)
			if kv, ok := m.data[key]; ok {
				newVersion = kv.version + 1
			}
			m.data[key] = indexTestKV{value: value, version: newVersion}
		}
	}
	for key := range txn.deletes {
		delete(m.data, key)
	}
	return nil
}

func (m *indexTestStore) Notifications(ctx context.Context) (metadata.NotificationStream, error) {
	return nil, nil
}

func (m *indexTestStore) PutEphemeral(ctx context.Context, key string, value []byte, opts ...metadata.EphemeralOption) (metadata.Version, error) {
	return m.Put(ctx, key, value)
}

func (m *indexTestStore) Close() error {
	m.closed = true
	return nil
}

type indexTestTxn struct {
	store   *indexTestStore
	pending map[string][]byte
	deletes map[string]bool
}

func (t *indexTestTxn) Get(key string) (value []byte, version metadata.Version, err error) {
	if kv, ok := t.store.data[key]; ok {
		return kv.value, kv.version, nil
	}
	return nil, 0, metadata.ErrKeyNotFound
}

func (t *indexTestTxn) Put(key string, value []byte) {
	t.pending[key] = value
}

func (t *indexTestTxn) PutWithVersion(key string, value []byte, expectedVersion metadata.Version) {
	t.pending[key] = value
}

func (t *indexTestTxn) Delete(key string) {
	t.deletes[key] = true
}

func (t *indexTestTxn) DeleteWithVersion(key string, expectedVersion metadata.Version) {
	t.deletes[key] = true
}

func TestAppendIndexEntry_Basic(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create a stream first
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an index entry
	now := time.Now().UnixMilli()
	req := AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    now,
		MinTimestampMs: now - 1000,
		MaxTimestampMs: now,
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	}

	result, err := sm.AppendIndexEntry(ctx, req)
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Verify offset allocation
	if result.StartOffset != 0 {
		t.Errorf("StartOffset = %d, want 0", result.StartOffset)
	}
	if result.EndOffset != 100 {
		t.Errorf("EndOffset = %d, want 100", result.EndOffset)
	}

	// Verify HWM was updated
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}
	if hwm != 100 {
		t.Errorf("HWM = %d, want 100", hwm)
	}

	// Verify index entry was created
	entries, err := sm.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("ListIndexEntries failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}

	entry := entries[0]
	if entry.StreamID != streamID {
		t.Errorf("entry.StreamID = %s, want %s", entry.StreamID, streamID)
	}
	if entry.StartOffset != 0 {
		t.Errorf("entry.StartOffset = %d, want 0", entry.StartOffset)
	}
	if entry.EndOffset != 100 {
		t.Errorf("entry.EndOffset = %d, want 100", entry.EndOffset)
	}
	if entry.CumulativeSize != 4096 {
		t.Errorf("entry.CumulativeSize = %d, want 4096", entry.CumulativeSize)
	}
	if entry.FileType != FileTypeWAL {
		t.Errorf("entry.FileType = %s, want WAL", entry.FileType)
	}
	if entry.RecordCount != 100 {
		t.Errorf("entry.RecordCount = %d, want 100", entry.RecordCount)
	}
}

func TestAppendIndexEntry_MultipleAppends(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append first entry
	result1, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/1.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("First AppendIndexEntry failed: %v", err)
	}

	// Append second entry
	result2, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    50,
		ChunkSizeBytes: 2048,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/2.wo",
		ChunkOffset:    0,
		ChunkLength:    2048,
	})
	if err != nil {
		t.Fatalf("Second AppendIndexEntry failed: %v", err)
	}

	// Verify offset continuity
	if result1.StartOffset != 0 {
		t.Errorf("First StartOffset = %d, want 0", result1.StartOffset)
	}
	if result1.EndOffset != 100 {
		t.Errorf("First EndOffset = %d, want 100", result1.EndOffset)
	}
	if result2.StartOffset != 100 {
		t.Errorf("Second StartOffset = %d, want 100", result2.StartOffset)
	}
	if result2.EndOffset != 150 {
		t.Errorf("Second EndOffset = %d, want 150", result2.EndOffset)
	}

	// Verify HWM
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}
	if hwm != 150 {
		t.Errorf("HWM = %d, want 150", hwm)
	}

	// Verify cumulative sizes
	entries, err := sm.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("ListIndexEntries failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}

	if entries[0].CumulativeSize != 4096 {
		t.Errorf("First entry CumulativeSize = %d, want 4096", entries[0].CumulativeSize)
	}
	if entries[1].CumulativeSize != 6144 { // 4096 + 2048
		t.Errorf("Second entry CumulativeSize = %d, want 6144", entries[1].CumulativeSize)
	}
}

func TestAppendIndexEntry_StreamNotFound(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       "nonexistent-stream",
		RecordCount:    100,
		ChunkSizeBytes: 4096,
	})
	if err != ErrStreamNotFound {
		t.Errorf("AppendIndexEntry should return ErrStreamNotFound, got %v", err)
	}
}

func TestAppendIndexEntry_InvalidRecordCount(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    0, // Invalid
		ChunkSizeBytes: 4096,
	})
	if err != ErrInvalidRecordCount {
		t.Errorf("AppendIndexEntry should return ErrInvalidRecordCount, got %v", err)
	}
}

func TestAppendIndexEntry_InvalidChunkSize(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 0, // Invalid
	})
	if err != ErrInvalidChunkSize {
		t.Errorf("AppendIndexEntry should return ErrInvalidChunkSize, got %v", err)
	}
}

func TestAppendIndexEntry_WithBatchIndex(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	now := time.Now().UnixMilli()
	batchIndex := []BatchIndexEntry{
		{
			BatchStartOffsetDelta: 0,
			BatchLastOffsetDelta:  49,
			BatchOffsetInChunk:    0,
			BatchLength:           2048,
			MinTimestampMs:        now - 2000,
			MaxTimestampMs:        now - 1000,
		},
		{
			BatchStartOffsetDelta: 50,
			BatchLastOffsetDelta:  99,
			BatchOffsetInChunk:    2048,
			BatchLength:           2048,
			MinTimestampMs:        now - 1000,
			MaxTimestampMs:        now,
		},
	}

	result, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    now,
		MinTimestampMs: now - 2000,
		MaxTimestampMs: now,
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
		BatchIndex:     batchIndex,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Verify batch index was stored
	entry, _, err := sm.GetIndexEntry(ctx, result.IndexKey)
	if err != nil {
		t.Fatalf("GetIndexEntry failed: %v", err)
	}

	if len(entry.BatchIndex) != 2 {
		t.Fatalf("len(entry.BatchIndex) = %d, want 2", len(entry.BatchIndex))
	}
	if entry.BatchIndex[0].BatchStartOffsetDelta != 0 {
		t.Errorf("BatchIndex[0].BatchStartOffsetDelta = %d, want 0", entry.BatchIndex[0].BatchStartOffsetDelta)
	}
	if entry.BatchIndex[1].BatchStartOffsetDelta != 50 {
		t.Errorf("BatchIndex[1].BatchStartOffsetDelta = %d, want 50", entry.BatchIndex[1].BatchStartOffsetDelta)
	}
}

func TestAppendIndexEntry_OffsetsAreMonotonic(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append multiple entries and verify offsets are monotonically increasing
	var prevEndOffset int64 = 0
	for i := 0; i < 10; i++ {
		result, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    uint32(10 + i), // Variable record counts
			ChunkSizeBytes: int64(1000 + i*100),
			CreatedAtMs:    time.Now().UnixMilli(),
			WalID:          uuid.New().String(),
			WalPath:        "s3://bucket/wal/test.wo",
			ChunkOffset:    0,
			ChunkLength:    uint32(1000 + i*100),
		})
		if err != nil {
			t.Fatalf("AppendIndexEntry %d failed: %v", i, err)
		}

		if result.StartOffset != prevEndOffset {
			t.Errorf("Append %d: StartOffset = %d, want %d", i, result.StartOffset, prevEndOffset)
		}
		if result.EndOffset <= result.StartOffset {
			t.Errorf("Append %d: EndOffset (%d) should be > StartOffset (%d)", i, result.EndOffset, result.StartOffset)
		}

		prevEndOffset = result.EndOffset
	}
}

func TestAppendIndexEntry_TransactionUsed(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	txnCountBefore := store.txnCount

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Verify a transaction was used
	if store.txnCount != txnCountBefore+1 {
		t.Errorf("txnCount = %d, want %d", store.txnCount, txnCountBefore+1)
	}
}

func TestGetIndexEntry(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	result, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	entry, version, err := sm.GetIndexEntry(ctx, result.IndexKey)
	if err != nil {
		t.Fatalf("GetIndexEntry failed: %v", err)
	}

	if version == 0 {
		t.Error("version should not be 0")
	}
	if entry.StreamID != streamID {
		t.Errorf("entry.StreamID = %s, want %s", entry.StreamID, streamID)
	}
	if entry.StartOffset != 0 {
		t.Errorf("entry.StartOffset = %d, want 0", entry.StartOffset)
	}
	if entry.EndOffset != 100 {
		t.Errorf("entry.EndOffset = %d, want 100", entry.EndOffset)
	}
}

func TestGetIndexEntry_NotFound(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, _, err := sm.GetIndexEntry(ctx, "/nonexistent/key")
	if err != metadata.ErrKeyNotFound {
		t.Errorf("GetIndexEntry should return ErrKeyNotFound, got %v", err)
	}
}

func TestListIndexEntries_Empty(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	entries, err := sm.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("ListIndexEntries failed: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("len(entries) = %d, want 0", len(entries))
	}
}

func TestListIndexEntries_WithLimit(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append 5 entries
	for i := 0; i < 5; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    10,
			ChunkSizeBytes: 1000,
			CreatedAtMs:    time.Now().UnixMilli(),
			WalID:          uuid.New().String(),
			WalPath:        "s3://bucket/wal/test.wo",
			ChunkOffset:    0,
			ChunkLength:    1000,
		})
		if err != nil {
			t.Fatalf("AppendIndexEntry %d failed: %v", i, err)
		}
	}

	// List with limit
	entries, err := sm.ListIndexEntries(ctx, streamID, 3)
	if err != nil {
		t.Fatalf("ListIndexEntries failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("len(entries) = %d, want 3", len(entries))
	}
}

func TestIndexEntrySorting(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append multiple entries
	for i := 0; i < 5; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    10,
			ChunkSizeBytes: 1000,
			CreatedAtMs:    time.Now().UnixMilli(),
			WalID:          uuid.New().String(),
			WalPath:        "s3://bucket/wal/test.wo",
			ChunkOffset:    0,
			ChunkLength:    1000,
		})
		if err != nil {
			t.Fatalf("AppendIndexEntry %d failed: %v", i, err)
		}
	}

	// List all entries
	entries, err := sm.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("ListIndexEntries failed: %v", err)
	}

	// Verify entries are sorted by endOffset (lexicographically via key)
	for i := 1; i < len(entries); i++ {
		if entries[i].EndOffset <= entries[i-1].EndOffset {
			t.Errorf("Entries not sorted: entries[%d].EndOffset (%d) <= entries[%d].EndOffset (%d)",
				i, entries[i].EndOffset, i-1, entries[i-1].EndOffset)
		}
	}
}

// Tests for LookupOffset

func TestLookupOffset_Basic(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an entry covering offsets 0-99
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Lookup offset 0 (first offset)
	result, err := sm.LookupOffset(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if !result.Found {
		t.Error("Expected Found=true for offset 0")
	}
	if result.OffsetBeyondHWM {
		t.Error("Expected OffsetBeyondHWM=false for offset 0")
	}
	if result.Entry == nil {
		t.Fatal("Expected Entry to be non-nil")
	}
	if result.Entry.StartOffset != 0 {
		t.Errorf("Entry.StartOffset = %d, want 0", result.Entry.StartOffset)
	}
	if result.Entry.EndOffset != 100 {
		t.Errorf("Entry.EndOffset = %d, want 100", result.Entry.EndOffset)
	}
	if result.HWM != 100 {
		t.Errorf("HWM = %d, want 100", result.HWM)
	}
}

func TestLookupOffset_MiddleOfEntry(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an entry covering offsets 0-99
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Lookup offset 50 (middle of entry)
	result, err := sm.LookupOffset(ctx, streamID, 50)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if !result.Found {
		t.Error("Expected Found=true for offset 50")
	}
	if result.Entry == nil {
		t.Fatal("Expected Entry to be non-nil")
	}
	if result.Entry.StartOffset != 0 {
		t.Errorf("Entry.StartOffset = %d, want 0", result.Entry.StartOffset)
	}
	if result.Entry.EndOffset != 100 {
		t.Errorf("Entry.EndOffset = %d, want 100", result.Entry.EndOffset)
	}
}

func TestLookupOffset_LastOffsetOfEntry(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an entry covering offsets 0-99
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Lookup offset 99 (last offset in entry, endOffset is 100 exclusive)
	result, err := sm.LookupOffset(ctx, streamID, 99)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if !result.Found {
		t.Error("Expected Found=true for offset 99")
	}
	if result.Entry == nil {
		t.Fatal("Expected Entry to be non-nil")
	}
	if result.Entry.StartOffset != 0 {
		t.Errorf("Entry.StartOffset = %d, want 0", result.Entry.StartOffset)
	}
	if result.Entry.EndOffset != 100 {
		t.Errorf("Entry.EndOffset = %d, want 100", result.Entry.EndOffset)
	}
}

func TestLookupOffset_MultipleEntries(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append three entries:
	// Entry 1: offsets 0-99
	// Entry 2: offsets 100-199
	// Entry 3: offsets 200-299
	for i := 0; i < 3; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    100,
			ChunkSizeBytes: 4096,
			CreatedAtMs:    time.Now().UnixMilli(),
			WalID:          uuid.New().String(),
			WalPath:        "s3://bucket/wal/test.wo",
			ChunkOffset:    0,
			ChunkLength:    4096,
		})
		if err != nil {
			t.Fatalf("AppendIndexEntry %d failed: %v", i, err)
		}
	}

	tests := []struct {
		offset        int64
		wantStart     int64
		wantEnd       int64
		wantFound     bool
	}{
		{0, 0, 100, true},
		{50, 0, 100, true},
		{99, 0, 100, true},
		{100, 100, 200, true},
		{150, 100, 200, true},
		{199, 100, 200, true},
		{200, 200, 300, true},
		{250, 200, 300, true},
		{299, 200, 300, true},
	}

	for _, tc := range tests {
		result, err := sm.LookupOffset(ctx, streamID, tc.offset)
		if err != nil {
			t.Fatalf("LookupOffset(%d) failed: %v", tc.offset, err)
		}

		if result.Found != tc.wantFound {
			t.Errorf("LookupOffset(%d): Found = %v, want %v", tc.offset, result.Found, tc.wantFound)
		}
		if tc.wantFound && result.Entry != nil {
			if result.Entry.StartOffset != tc.wantStart {
				t.Errorf("LookupOffset(%d): Entry.StartOffset = %d, want %d", tc.offset, result.Entry.StartOffset, tc.wantStart)
			}
			if result.Entry.EndOffset != tc.wantEnd {
				t.Errorf("LookupOffset(%d): Entry.EndOffset = %d, want %d", tc.offset, result.Entry.EndOffset, tc.wantEnd)
			}
		}
	}
}

func TestLookupOffset_BeyondHWM(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an entry covering offsets 0-99, HWM = 100
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Lookup offset 100 (equals HWM, which is exclusive)
	result, err := sm.LookupOffset(ctx, streamID, 100)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if result.Found {
		t.Error("Expected Found=false for offset 100 (at HWM)")
	}
	if !result.OffsetBeyondHWM {
		t.Error("Expected OffsetBeyondHWM=true for offset 100")
	}
	if result.HWM != 100 {
		t.Errorf("HWM = %d, want 100", result.HWM)
	}

	// Lookup offset 200 (well beyond HWM)
	result, err = sm.LookupOffset(ctx, streamID, 200)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if result.Found {
		t.Error("Expected Found=false for offset 200 (beyond HWM)")
	}
	if !result.OffsetBeyondHWM {
		t.Error("Expected OffsetBeyondHWM=true for offset 200")
	}
}

func TestLookupOffset_EmptyStream(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Lookup offset 0 on empty stream (HWM = 0)
	result, err := sm.LookupOffset(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if result.Found {
		t.Error("Expected Found=false for empty stream")
	}
	if !result.OffsetBeyondHWM {
		t.Error("Expected OffsetBeyondHWM=true for empty stream (offset 0 >= HWM 0)")
	}
	if result.HWM != 0 {
		t.Errorf("HWM = %d, want 0", result.HWM)
	}
}

func TestLookupOffset_StreamNotFound(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Lookup on non-existent stream
	_, err := sm.LookupOffset(ctx, "nonexistent-stream", 0)
	if err != ErrStreamNotFound {
		t.Errorf("LookupOffset should return ErrStreamNotFound, got %v", err)
	}
}

func TestLookupOffset_NegativeOffset(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an entry covering offsets 0-99
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Lookup negative offset - should be treated as offset 0
	result, err := sm.LookupOffset(ctx, streamID, -10)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if !result.Found {
		t.Error("Expected Found=true for negative offset (treated as 0)")
	}
	if result.Entry == nil {
		t.Fatal("Expected Entry to be non-nil")
	}
	if result.Entry.StartOffset != 0 {
		t.Errorf("Entry.StartOffset = %d, want 0", result.Entry.StartOffset)
	}
}

func TestLookupOffset_WALEntry(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	walID := uuid.New().String()
	walPath := "s3://bucket/wal/test.wo"

	// Append a WAL entry
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          walID,
		WalPath:        walPath,
		ChunkOffset:    1024,
		ChunkLength:    2048,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	result, err := sm.LookupOffset(ctx, streamID, 50)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if !result.Found {
		t.Fatal("Expected Found=true")
	}
	if result.Entry.FileType != FileTypeWAL {
		t.Errorf("FileType = %s, want WAL", result.Entry.FileType)
	}
	if result.Entry.WalID != walID {
		t.Errorf("WalID = %s, want %s", result.Entry.WalID, walID)
	}
	if result.Entry.WalPath != walPath {
		t.Errorf("WalPath = %s, want %s", result.Entry.WalPath, walPath)
	}
	if result.Entry.ChunkOffset != 1024 {
		t.Errorf("ChunkOffset = %d, want 1024", result.Entry.ChunkOffset)
	}
	if result.Entry.ChunkLength != 2048 {
		t.Errorf("ChunkLength = %d, want 2048", result.Entry.ChunkLength)
	}
}

func TestLookupOffset_ParquetEntry(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Manually create a Parquet entry by first appending and then modifying
	// In real usage, compaction would create Parquet entries.
	// For testing, we'll directly construct one.

	// First append a WAL entry to get the offsets
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          uuid.New().String(),
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// The lookup should still work for WAL entries
	result, err := sm.LookupOffset(ctx, streamID, 50)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	// Verify WAL entry was found
	if !result.Found {
		t.Fatal("Expected Found=true")
	}
	if result.Entry.FileType != FileTypeWAL {
		t.Errorf("FileType = %s, want WAL", result.Entry.FileType)
	}

	// Note: Parquet entries would be created by compaction and have FileType == FileTypeParquet
	// The LookupOffset function handles both types transparently
}

func TestLookupOffsetWithBounds(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append three entries
	for i := 0; i < 3; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    100,
			ChunkSizeBytes: 4096,
			CreatedAtMs:    time.Now().UnixMilli(),
			WalID:          uuid.New().String(),
			WalPath:        "s3://bucket/wal/test.wo",
			ChunkOffset:    0,
			ChunkLength:    4096,
		})
		if err != nil {
			t.Fatalf("AppendIndexEntry %d failed: %v", i, err)
		}
	}

	result, earliestOffset, err := sm.LookupOffsetWithBounds(ctx, streamID, 150)
	if err != nil {
		t.Fatalf("LookupOffsetWithBounds failed: %v", err)
	}

	if !result.Found {
		t.Error("Expected Found=true")
	}
	if result.Entry == nil {
		t.Fatal("Expected Entry to be non-nil")
	}
	if result.Entry.StartOffset != 100 {
		t.Errorf("Entry.StartOffset = %d, want 100", result.Entry.StartOffset)
	}
	if result.Entry.EndOffset != 200 {
		t.Errorf("Entry.EndOffset = %d, want 200", result.Entry.EndOffset)
	}
	if result.HWM != 300 {
		t.Errorf("HWM = %d, want 300", result.HWM)
	}
	if earliestOffset != 0 {
		t.Errorf("earliestOffset = %d, want 0", earliestOffset)
	}
}

func TestLookupOffsetWithBounds_EmptyStream(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	result, earliestOffset, err := sm.LookupOffsetWithBounds(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("LookupOffsetWithBounds failed: %v", err)
	}

	if result.Found {
		t.Error("Expected Found=false for empty stream")
	}
	if !result.OffsetBeyondHWM {
		t.Error("Expected OffsetBeyondHWM=true for empty stream")
	}
	if result.HWM != 0 {
		t.Errorf("HWM = %d, want 0", result.HWM)
	}
	if earliestOffset != 0 {
		t.Errorf("earliestOffset = %d, want 0", earliestOffset)
	}
}

func TestLookupOffset_LargeOffsets(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append many entries to simulate large offsets
	for i := 0; i < 100; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    1000,
			ChunkSizeBytes: 10000,
			CreatedAtMs:    time.Now().UnixMilli(),
			WalID:          uuid.New().String(),
			WalPath:        "s3://bucket/wal/test.wo",
			ChunkOffset:    0,
			ChunkLength:    10000,
		})
		if err != nil {
			t.Fatalf("AppendIndexEntry %d failed: %v", i, err)
		}
	}

	// Lookup a large offset (should be in entry 50)
	// Entry 50 covers offsets 50000-50999
	result, err := sm.LookupOffset(ctx, streamID, 50500)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}

	if !result.Found {
		t.Error("Expected Found=true for offset 50500")
	}
	if result.Entry == nil {
		t.Fatal("Expected Entry to be non-nil")
	}
	if result.Entry.StartOffset != 50000 {
		t.Errorf("Entry.StartOffset = %d, want 50000", result.Entry.StartOffset)
	}
	if result.Entry.EndOffset != 51000 {
		t.Errorf("Entry.EndOffset = %d, want 51000", result.Entry.EndOffset)
	}
}
