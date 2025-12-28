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
	var keys []string
	for k := range m.data {
		if strings.HasPrefix(k, startKey) {
			if endKey == "" || k < endKey {
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
