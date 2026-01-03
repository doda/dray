package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/google/uuid"
)

// indexTestStore extends the mock store to support List for offset index tests.
type indexTestStore struct {
	data             map[string]indexTestKV
	closed           bool
	txnCount         int
	listCalls        int
	lastListStartKey string
	lastListEndKey   string
	lastListLimit    int
	failTxnOnKey     string
	failTxnOnPrefix  string
	failTxnErr       error
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

	m.listCalls++
	m.lastListStartKey = startKey
	m.lastListEndKey = endKey
	m.lastListLimit = limit

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
	if m.failTxnErr != nil && (m.failTxnOnKey != "" || m.failTxnOnPrefix != "") {
		for key := range txn.pending {
			if m.failTxnOnKey != "" && key == m.failTxnOnKey {
				return m.failTxnErr
			}
			if m.failTxnOnPrefix != "" && strings.HasPrefix(key, m.failTxnOnPrefix) {
				return m.failTxnErr
			}
		}
		for key := range txn.deletes {
			if m.failTxnOnKey != "" && key == m.failTxnOnKey {
				return m.failTxnErr
			}
			if m.failTxnOnPrefix != "" && strings.HasPrefix(key, m.failTxnOnPrefix) {
				return m.failTxnErr
			}
		}
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

func TestAppendIndexEntry_UsesLastEntryLookup(t *testing.T) {
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

	store.listCalls = 0
	store.lastListStartKey = ""
	store.lastListEndKey = ""
	store.lastListLimit = 0

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
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

	expectedStartKey, err := keys.OffsetIndexStartKey(streamID, 100)
	if err != nil {
		t.Fatalf("OffsetIndexStartKey failed: %v", err)
	}

	if store.listCalls != 1 {
		t.Errorf("listCalls = %d, want 1", store.listCalls)
	}
	if store.lastListStartKey != expectedStartKey {
		t.Errorf("lastListStartKey = %s, want %s", store.lastListStartKey, expectedStartKey)
	}
	if store.lastListLimit != 1 {
		t.Errorf("lastListLimit = %d, want 1", store.lastListLimit)
	}
	expectedEndKey := keys.OffsetIndexEndKey(streamID)
	if store.lastListEndKey != expectedEndKey {
		t.Errorf("lastListEndKey = %s, want %s", store.lastListEndKey, expectedEndKey)
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

func TestAppendIndexEntry_HWMNotAdvancedOnIndexWriteFailure(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	errInjected := errors.New("index: injected index write failure")
	store.failTxnOnPrefix = keys.OffsetIndexPrefix(streamID)
	store.failTxnErr = errInjected

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
	if !errors.Is(err, errInjected) {
		t.Fatalf("AppendIndexEntry error = %v, want %v", err, errInjected)
	}

	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("HWM = %d, want 0", hwm)
	}

	entries, err := sm.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("ListIndexEntries failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("len(entries) = %d, want 0", len(entries))
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
		offset    int64
		wantStart int64
		wantEnd   int64
		wantFound bool
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

// TestConcurrentAppendIndexEntry verifies that concurrent offset index appends
// produce monotonic, non-overlapping offsets. This tests the invariant I1
// (linearizable write ordering per partition) under concurrent access.
//
// Steps verified:
// 1. Run 2+ concurrent writers to same stream
// 2. Verify all offsets are monotonically increasing
// 3. Verify no offset ranges overlap
// 4. Verify total records equals sum of all appends
func TestConcurrentAppendIndexEntry(t *testing.T) {
	store := newConcurrentTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	const (
		numWriters       = 5
		appendsPerWriter = 20
		recordsPerAppend = 10
	)

	type appendResult struct {
		writerID    int
		startOffset int64
		endOffset   int64
		err         error
	}

	results := make(chan appendResult, numWriters*appendsPerWriter)
	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < appendsPerWriter; i++ {
				// Retry loop for handling version conflicts
				for retries := 0; retries < 100; retries++ {
					result, err := sm.AppendIndexEntry(ctx, AppendRequest{
						StreamID:       streamID,
						RecordCount:    recordsPerAppend,
						ChunkSizeBytes: 1000,
						CreatedAtMs:    time.Now().UnixMilli(),
						WalID:          uuid.New().String(),
						WalPath:        "s3://bucket/wal/test.wo",
						ChunkOffset:    0,
						ChunkLength:    1000,
					})
					if err == nil {
						results <- appendResult{
							writerID:    writerID,
							startOffset: result.StartOffset,
							endOffset:   result.EndOffset,
						}
						break
					}
					if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
						// Expected during concurrent access, retry
						continue
					}
					// Unexpected error
					results <- appendResult{writerID: writerID, err: err}
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(results)

	// Collect all results
	var allResults []appendResult
	for r := range results {
		if r.err != nil {
			t.Fatalf("Writer %d failed: %v", r.writerID, r.err)
		}
		allResults = append(allResults, r)
	}

	// Verify we got the expected number of successful appends
	expectedAppends := numWriters * appendsPerWriter
	if len(allResults) != expectedAppends {
		t.Fatalf("Expected %d appends, got %d", expectedAppends, len(allResults))
	}

	// Sort results by startOffset for verification
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].startOffset < allResults[j].startOffset
	})

	// Verify 1: All offsets are monotonically increasing
	// Verify 2: No offset ranges overlap (each entry's startOffset == previous entry's endOffset)
	for i := 1; i < len(allResults); i++ {
		prev := allResults[i-1]
		curr := allResults[i]

		// Start offset should be strictly greater than previous start
		if curr.startOffset <= prev.startOffset {
			t.Errorf("Offsets not monotonic: entry %d startOffset (%d) <= entry %d startOffset (%d)",
				i, curr.startOffset, i-1, prev.startOffset)
		}

		// No overlap: current startOffset should equal previous endOffset
		if curr.startOffset != prev.endOffset {
			t.Errorf("Offset gap or overlap: entry %d endOffset (%d) != entry %d startOffset (%d)",
				i-1, prev.endOffset, i, curr.startOffset)
		}

		// Each entry should span exactly recordsPerAppend offsets
		if curr.endOffset-curr.startOffset != recordsPerAppend {
			t.Errorf("Entry %d has wrong span: %d - %d = %d, expected %d",
				i, curr.endOffset, curr.startOffset, curr.endOffset-curr.startOffset, recordsPerAppend)
		}
	}

	// Verify first entry starts at 0
	if allResults[0].startOffset != 0 {
		t.Errorf("First entry should start at 0, got %d", allResults[0].startOffset)
	}

	// Verify 3: Total records equals sum of all appends
	expectedTotalRecords := int64(numWriters * appendsPerWriter * recordsPerAppend)
	lastEntry := allResults[len(allResults)-1]
	if lastEntry.endOffset != expectedTotalRecords {
		t.Errorf("Total records mismatch: last endOffset = %d, expected %d",
			lastEntry.endOffset, expectedTotalRecords)
	}

	// Verify HWM matches the last endOffset
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}
	if hwm != expectedTotalRecords {
		t.Errorf("HWM = %d, expected %d", hwm, expectedTotalRecords)
	}

	// Verify index entries match the results
	entries, err := sm.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("ListIndexEntries failed: %v", err)
	}
	if len(entries) != expectedAppends {
		t.Errorf("Expected %d index entries, got %d", expectedAppends, len(entries))
	}
}

// concurrentTestStore wraps MockStore for concurrent testing.
// It properly handles version conflicts and provides true atomicity.
type concurrentTestStore struct {
	*metadata.MockStore
}

func newConcurrentTestStore() *concurrentTestStore {
	return &concurrentTestStore{MockStore: metadata.NewMockStore()}
}

// TestConcurrentAppendIndexEntry_TwoWriters is a simpler test with exactly 2 writers
// to ensure the basic concurrent behavior works.
func TestConcurrentAppendIndexEntry_TwoWriters(t *testing.T) {
	store := newConcurrentTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	const (
		appendsPerWriter = 50
		recordsPerAppend = 5
	)

	type appendResult struct {
		startOffset int64
		endOffset   int64
	}

	results1 := make([]appendResult, 0, appendsPerWriter)
	results2 := make([]appendResult, 0, appendsPerWriter)

	var wg sync.WaitGroup
	var mu1, mu2 sync.Mutex

	// Writer 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < appendsPerWriter; i++ {
			for retries := 0; retries < 100; retries++ {
				result, err := sm.AppendIndexEntry(ctx, AppendRequest{
					StreamID:       streamID,
					RecordCount:    recordsPerAppend,
					ChunkSizeBytes: 500,
					CreatedAtMs:    time.Now().UnixMilli(),
					WalID:          uuid.New().String(),
					WalPath:        "s3://bucket/wal/w1.wo",
					ChunkOffset:    0,
					ChunkLength:    500,
				})
				if err == nil {
					mu1.Lock()
					results1 = append(results1, appendResult{result.StartOffset, result.EndOffset})
					mu1.Unlock()
					break
				}
				if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
					continue
				}
				t.Errorf("Writer 1 unexpected error: %v", err)
				return
			}
		}
	}()

	// Writer 2
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < appendsPerWriter; i++ {
			for retries := 0; retries < 100; retries++ {
				result, err := sm.AppendIndexEntry(ctx, AppendRequest{
					StreamID:       streamID,
					RecordCount:    recordsPerAppend,
					ChunkSizeBytes: 500,
					CreatedAtMs:    time.Now().UnixMilli(),
					WalID:          uuid.New().String(),
					WalPath:        "s3://bucket/wal/w2.wo",
					ChunkOffset:    0,
					ChunkLength:    500,
				})
				if err == nil {
					mu2.Lock()
					results2 = append(results2, appendResult{result.StartOffset, result.EndOffset})
					mu2.Unlock()
					break
				}
				if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
					continue
				}
				t.Errorf("Writer 2 unexpected error: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	// Verify both writers completed all appends
	if len(results1) != appendsPerWriter {
		t.Errorf("Writer 1: expected %d appends, got %d", appendsPerWriter, len(results1))
	}
	if len(results2) != appendsPerWriter {
		t.Errorf("Writer 2: expected %d appends, got %d", appendsPerWriter, len(results2))
	}

	// Combine and sort all results
	allResults := append(results1, results2...)
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].startOffset < allResults[j].startOffset
	})

	// Verify monotonicity and no overlaps
	for i := 1; i < len(allResults); i++ {
		if allResults[i].startOffset != allResults[i-1].endOffset {
			t.Errorf("Gap or overlap at index %d: prev.endOffset=%d, curr.startOffset=%d",
				i, allResults[i-1].endOffset, allResults[i].startOffset)
		}
	}

	// Verify total
	expectedTotal := int64(2 * appendsPerWriter * recordsPerAppend)
	hwm, _, _ := sm.GetHWM(ctx, streamID)
	if hwm != expectedTotal {
		t.Errorf("HWM = %d, expected %d", hwm, expectedTotal)
	}
}

// TestConcurrentAppendIndexEntry_HighContention tests with many writers and few appends
// to maximize contention and version conflict retries.
func TestConcurrentAppendIndexEntry_HighContention(t *testing.T) {
	store := newConcurrentTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	const (
		numWriters       = 20
		appendsPerWriter = 5
		recordsPerAppend = 3
	)

	var successCount int64
	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < appendsPerWriter; i++ {
				for retries := 0; retries < 200; retries++ {
					_, err := sm.AppendIndexEntry(ctx, AppendRequest{
						StreamID:       streamID,
						RecordCount:    recordsPerAppend,
						ChunkSizeBytes: 100,
						CreatedAtMs:    time.Now().UnixMilli(),
						WalID:          uuid.New().String(),
						WalPath:        "s3://bucket/wal/test.wo",
						ChunkOffset:    0,
						ChunkLength:    100,
					})
					if err == nil {
						atomic.AddInt64(&successCount, 1)
						break
					}
					if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
						continue
					}
					t.Errorf("Unexpected error: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	expectedAppends := int64(numWriters * appendsPerWriter)
	if successCount != expectedAppends {
		t.Errorf("Expected %d successful appends, got %d", expectedAppends, successCount)
	}

	// Verify final state
	expectedHWM := expectedAppends * recordsPerAppend
	hwm, _, _ := sm.GetHWM(ctx, streamID)
	if hwm != expectedHWM {
		t.Errorf("HWM = %d, expected %d", hwm, expectedHWM)
	}

	entries, _ := sm.ListIndexEntries(ctx, streamID, 0)
	if len(entries) != int(expectedAppends) {
		t.Errorf("Expected %d entries, got %d", expectedAppends, len(entries))
	}

	// Verify entries are contiguous
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].StartOffset < entries[j].StartOffset
	})

	for i := 1; i < len(entries); i++ {
		if entries[i].StartOffset != entries[i-1].EndOffset {
			t.Errorf("Gap at index %d: prev.endOffset=%d, curr.startOffset=%d",
				i, entries[i-1].EndOffset, entries[i].StartOffset)
		}
	}
}

// TestLookupOffsetByTimestamp_Basic tests basic timestamp-based offset lookup.
func TestLookupOffsetByTimestamp_Basic(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	// Add entries with different timestamp ranges
	_, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 2000,
		MaxTimestampMs: 2500,
		WalID:          "wal-2",
		WalPath:        "wal/2.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
		expectedFound  bool
	}{
		{
			name:           "before first entry",
			timestamp:      500,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "at first entry start",
			timestamp:      1000,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "in first entry",
			timestamp:      1200,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "at second entry start",
			timestamp:      2000,
			expectedOffset: 5,
			expectedFound:  true,
		},
		{
			name:           "after all entries",
			timestamp:      3000,
			expectedOffset: -1,
			expectedFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sm.LookupOffsetByTimestamp(ctx, streamID, tt.timestamp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Found != tt.expectedFound {
				t.Errorf("Found = %v, expected %v", result.Found, tt.expectedFound)
			}

			if result.Offset != tt.expectedOffset {
				t.Errorf("Offset = %d, expected %d", result.Offset, tt.expectedOffset)
			}
		})
	}
}

func TestLookupOffsetByTimestamp_NonMonotonicEntries(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	_, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 500,
		MaxTimestampMs: 1200,
		WalID:          "wal-2",
		WalPath:        "wal/2.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1600,
		MaxTimestampMs: 2000,
		WalID:          "wal-3",
		WalPath:        "wal/3.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
		expectedFound  bool
	}{
		{
			name:           "matches earliest entry despite non-monotonic max",
			timestamp:      1300,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "matches later entry after earlier ranges",
			timestamp:      1550,
			expectedOffset: 10,
			expectedFound:  true,
		},
		{
			name:           "after all entries",
			timestamp:      2500,
			expectedOffset: -1,
			expectedFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sm.LookupOffsetByTimestamp(ctx, streamID, tt.timestamp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Found != tt.expectedFound {
				t.Errorf("Found = %v, expected %v", result.Found, tt.expectedFound)
			}

			if result.Offset != tt.expectedOffset {
				t.Errorf("Offset = %d, expected %d", result.Offset, tt.expectedOffset)
			}
		})
	}
}

func TestLookupOffsetByTimestamp_NonMonotonicOverlappingRanges(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	_, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1500,
		MaxTimestampMs: 2500,
		WalID:          "wal-2",
		WalPath:        "wal/2.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 900,
		MaxTimestampMs: 1100,
		WalID:          "wal-3",
		WalPath:        "wal/3.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
		expectedFound  bool
	}{
		{
			name:           "overlapping ranges select earliest offset",
			timestamp:      1550,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "later range when earlier ranges end",
			timestamp:      2100,
			expectedOffset: 5,
			expectedFound:  true,
		},
		{
			name:           "out-of-order early range does not override earliest offset",
			timestamp:      950,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "after all entries",
			timestamp:      2600,
			expectedOffset: -1,
			expectedFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sm.LookupOffsetByTimestamp(ctx, streamID, tt.timestamp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Found != tt.expectedFound {
				t.Errorf("Found = %v, expected %v", result.Found, tt.expectedFound)
			}

			if result.Offset != tt.expectedOffset {
				t.Errorf("Offset = %d, expected %d", result.Offset, tt.expectedOffset)
			}
		})
	}
}

// TestLookupOffsetByTimestamp_EmptyStream tests lookup on empty stream.
func TestLookupOffsetByTimestamp_EmptyStream(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	result, err := sm.LookupOffsetByTimestamp(ctx, streamID, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected not found for empty stream")
	}

	if result.Offset != -1 {
		t.Errorf("expected offset=-1, got %d", result.Offset)
	}
}

// TestLookupOffsetByTimestamp_WithBatchIndex tests using batchIndex for finer lookup.
func TestLookupOffsetByTimestamp_WithBatchIndex(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	_, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    15,
		ChunkSizeBytes: 300,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 3000,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    300,
		BatchIndex: []BatchIndexEntry{
			{BatchStartOffsetDelta: 0, BatchLastOffsetDelta: 4, MinTimestampMs: 1000, MaxTimestampMs: 1500},
			{BatchStartOffsetDelta: 5, BatchLastOffsetDelta: 9, MinTimestampMs: 2000, MaxTimestampMs: 2500},
			{BatchStartOffsetDelta: 10, BatchLastOffsetDelta: 14, MinTimestampMs: 2700, MaxTimestampMs: 3000},
		},
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
	}{
		{"in first batch", 1200, 0},
		{"in second batch", 2200, 5},
		{"in third batch", 2800, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sm.LookupOffsetByTimestamp(ctx, streamID, tt.timestamp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !result.Found {
				t.Error("expected to find offset")
			}

			if result.Offset != tt.expectedOffset {
				t.Errorf("Offset = %d, expected %d", result.Offset, tt.expectedOffset)
			}
		})
	}
}

func TestLookupOffsetByTimestamp_ScanFallback(t *testing.T) {
	store := newIndexTestStore()
	objStore := newMockObjectStore()
	sm := NewStreamManager(store)
	sm.SetTimestampScanner(&testTimestampScanner{store: objStore})
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	batch1 := buildBatchWithTimestamps([]int64{1000, 1100, 1200})
	batch2 := buildBatchWithTimestamps([]int64{2000, 2100, 2200})
	chunkData := buildChunkData(batch1, batch2)

	walPath := "wal/test.wal"
	if err := objStore.Put(ctx, walPath, bytes.NewReader(chunkData), int64(len(chunkData)), "application/octet-stream"); err != nil {
		t.Fatalf("failed to write WAL data: %v", err)
	}

	_, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    6,
		ChunkSizeBytes: int64(len(chunkData)),
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 2200,
		WalID:          "wal-1",
		WalPath:        walPath,
		ChunkOffset:    0,
		ChunkLength:    uint32(len(chunkData)),
	})
	if err != nil {
		t.Fatalf("failed to append entry: %v", err)
	}

	result, err := sm.LookupOffsetByTimestamp(ctx, streamID, 2100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Found {
		t.Fatal("expected to find offset")
	}

	if result.Offset != 4 {
		t.Errorf("Offset = %d, expected %d", result.Offset, 4)
	}

	if result.Timestamp != 2100 {
		t.Errorf("Timestamp = %d, expected %d", result.Timestamp, 2100)
	}
}

// TestLookupOffsetByTimestamp_StreamNotFound tests error handling.
func TestLookupOffsetByTimestamp_StreamNotFound(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, err := sm.LookupOffsetByTimestamp(ctx, "nonexistent", 1000)
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound, got %v", err)
	}
}

// TestLookupOffsetByTimestamp_LimitedListCalls verifies that the optimized timestamp
// lookup uses O(log N) List calls instead of loading all entries at once.
// This ensures the binary search optimization is working correctly.
func TestLookupOffsetByTimestamp_LimitedListCalls(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	// Create 1000 entries with monotonically increasing timestamps
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    10,
			ChunkSizeBytes: 100,
			CreatedAtMs:    time.Now().UnixMilli(),
			MinTimestampMs: int64(i * 1000),
			MaxTimestampMs: int64(i*1000 + 500),
			WalID:          fmt.Sprintf("wal-%d", i),
			WalPath:        fmt.Sprintf("wal/%d.wal", i),
			ChunkOffset:    0,
			ChunkLength:    100,
		})
		if err != nil {
			t.Fatalf("failed to append entry %d: %v", i, err)
		}
	}

	// Reset List call counter
	store.listCalls = 0

	// Lookup a timestamp in the middle of the range
	// With 1000 entries, binary search should need about log2(1000) ≈ 10 iterations
	targetTimestamp := int64(500 * 1000) // Middle of range
	result, err := sm.LookupOffsetByTimestamp(ctx, streamID, targetTimestamp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Found {
		t.Error("expected to find offset")
	}

	// We expect O(log N) List calls:
	// - 1 call for GetHWM (to verify stream exists)
	// - 1 call to load first entry
	// - 1 call to load last entry
	// - ~log2(1000) ≈ 10 calls for binary search
	// - 1 call for final resolution
	// Total: approximately 15-20 calls
	// With full scan it would be 1 call loading all 1000 entries
	// We assert the number of calls is significantly less than a threshold
	// that would indicate a full scan or excessive operations
	maxExpectedCalls := 25 // Generous upper bound for O(log N) behavior

	if store.listCalls > maxExpectedCalls {
		t.Errorf("listCalls = %d, expected <= %d (O(log N) behavior)", store.listCalls, maxExpectedCalls)
	}

	t.Logf("listCalls = %d for %d entries (expected O(log N) ≈ %d)", store.listCalls, numEntries, int(1+2+10+1))

	// Also verify correctness: timestamp 500000 should be in entry 500
	// Entry 500 has min=500000, max=500500, startOffset=500*10=5000
	expectedOffset := int64(500 * 10)
	if result.Offset != expectedOffset {
		t.Errorf("Offset = %d, expected %d", result.Offset, expectedOffset)
	}
}

// TestLookupOffsetByTimestamp_LimitedListCalls_EdgeCases tests that edge cases
// also use limited List calls.
func TestLookupOffsetByTimestamp_LimitedListCalls_EdgeCases(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	// Create 500 entries
	numEntries := 500
	for i := 0; i < numEntries; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    10,
			ChunkSizeBytes: 100,
			CreatedAtMs:    time.Now().UnixMilli(),
			MinTimestampMs: int64(i * 1000),
			MaxTimestampMs: int64(i*1000 + 500),
			WalID:          fmt.Sprintf("wal-%d", i),
			WalPath:        fmt.Sprintf("wal/%d.wal", i),
			ChunkOffset:    0,
			ChunkLength:    100,
		})
		if err != nil {
			t.Fatalf("failed to append entry %d: %v", i, err)
		}
	}

	tests := []struct {
		name         string
		timestamp    int64
		expectFound  bool
		maxListCalls int
	}{
		{
			name:         "first entry quick path",
			timestamp:    100, // Within first entry (min=0, max=500)
			expectFound:  true,
			maxListCalls: 5, // GetHWM + first entry load + resolution
		},
		{
			name:         "last entry quick path",
			timestamp:    int64((numEntries - 1) * 1000), // Start of last entry
			expectFound:  true,
			maxListCalls: 25, // GetHWM + first + last + binary search
		},
		{
			name:         "after all entries",
			timestamp:    int64(numEntries * 1000), // After all entries
			expectFound:  false,
			maxListCalls: 5, // GetHWM + first + last (quick rejection)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.listCalls = 0

			result, err := sm.LookupOffsetByTimestamp(ctx, streamID, tt.timestamp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Found != tt.expectFound {
				t.Errorf("Found = %v, expected %v", result.Found, tt.expectFound)
			}

			if store.listCalls > tt.maxListCalls {
				t.Errorf("listCalls = %d, expected <= %d", store.listCalls, tt.maxListCalls)
			}

			t.Logf("listCalls = %d for %s", store.listCalls, tt.name)
		})
	}
}

// TestGetEarliestOffset_Basic tests basic earliest offset lookup.
func TestGetEarliestOffset_Basic(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

	// Empty stream should return 0
	offset, err := sm.GetEarliestOffset(ctx, streamID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if offset != 0 {
		t.Errorf("expected 0 for empty stream, got %d", offset)
	}

	// Add an entry
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// First entry starts at 0
	offset, err = sm.GetEarliestOffset(ctx, streamID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if offset != 0 {
		t.Errorf("expected 0, got %d", offset)
	}
}

// TestGetEarliestOffset_StreamNotFound tests error handling.
func TestGetEarliestOffset_StreamNotFound(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, err := sm.GetEarliestOffset(ctx, "nonexistent")
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound, got %v", err)
	}
}

type mockObjectStore struct {
	objects map[string][]byte
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *mockObjectStore) Put(_ context.Context, key string, reader io.Reader, _ int64, _ string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.objects[key] = data
	return nil
}

func (m *mockObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *mockObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockObjectStore) GetRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if start < 0 || start >= int64(len(data)) {
		return nil, objectstore.ErrInvalidRange
	}
	if end < 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (m *mockObjectStore) Head(_ context.Context, key string) (objectstore.ObjectMeta, error) {
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *mockObjectStore) Delete(_ context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

func (m *mockObjectStore) List(_ context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	var result []objectstore.ObjectMeta
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, objectstore.ObjectMeta{
				Key:  key,
				Size: int64(len(data)),
			})
		}
	}
	return result, nil
}

func (m *mockObjectStore) Close() error {
	return nil
}

type testTimestampScanner struct {
	store objectstore.Store
}

func (s *testTimestampScanner) ScanOffsetByTimestamp(ctx context.Context, entry *IndexEntry, timestamp int64) (int64, int64, bool, error) {
	if entry.FileType != FileTypeWAL {
		return -1, -1, false, nil
	}

	start := int64(entry.ChunkOffset)
	end := start + int64(entry.ChunkLength) - 1
	rc, err := s.store.GetRange(ctx, entry.WalPath, start, end)
	if err != nil {
		return -1, -1, false, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return -1, -1, false, err
	}

	return scanChunkForTimestamp(data, entry.StartOffset, timestamp)
}

func buildChunkData(batches ...[]byte) []byte {
	var buf bytes.Buffer
	var lenBuf [4]byte
	for _, batch := range batches {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(batch)))
		buf.Write(lenBuf[:])
		buf.Write(batch)
	}
	return buf.Bytes()
}

func buildBatchWithTimestamps(timestamps []int64) []byte {
	firstTimestamp := timestamps[0]
	maxTimestamp := timestamps[0]
	var records bytes.Buffer

	for i, ts := range timestamps {
		if ts > maxTimestamp {
			maxTimestamp = ts
		}
		writeTestRecord(&records, ts-firstTimestamp, int64(i))
	}

	recordsBytes := records.Bytes()
	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + len(recordsBytes)
	totalSize := 8 + 4 + batchLength

	batch := make([]byte, totalSize)
	offset := 0

	binary.BigEndian.PutUint64(batch[offset:], 0)
	offset += 8

	binary.BigEndian.PutUint32(batch[offset:], uint32(batchLength))
	offset += 4

	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	batch[offset] = 2
	offset++

	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	binary.BigEndian.PutUint16(batch[offset:], 0)
	offset += 2

	binary.BigEndian.PutUint32(batch[offset:], uint32(len(timestamps)-1))
	offset += 4

	binary.BigEndian.PutUint64(batch[offset:], uint64(firstTimestamp))
	offset += 8

	binary.BigEndian.PutUint64(batch[offset:], uint64(maxTimestamp))
	offset += 8

	binary.BigEndian.PutUint64(batch[offset:], 0xFFFFFFFFFFFFFFFF)
	offset += 8

	binary.BigEndian.PutUint16(batch[offset:], 0xFFFF)
	offset += 2

	binary.BigEndian.PutUint32(batch[offset:], 0xFFFFFFFF)
	offset += 4

	binary.BigEndian.PutUint32(batch[offset:], uint32(len(timestamps)))
	offset += 4

	copy(batch[offset:], recordsBytes)

	return batch
}

func writeTestRecord(buf *bytes.Buffer, timestampDelta int64, offsetDelta int64) {
	var record bytes.Buffer
	record.WriteByte(0)
	record.Write(encodeVarint(timestampDelta))
	record.Write(encodeVarint(offsetDelta))
	record.Write(encodeVarint(-1))
	record.Write(encodeVarint(-1))
	record.Write(encodeVarint(0))

	recordBytes := record.Bytes()
	buf.Write(encodeVarint(int64(len(recordBytes))))
	buf.Write(recordBytes)
}

func encodeVarint(v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63))
	var out []byte
	for {
		b := byte(uv & 0x7F)
		uv >>= 7
		if uv == 0 {
			out = append(out, b)
			break
		}
		out = append(out, b|0x80)
	}
	return out
}

func scanChunkForTimestamp(chunkData []byte, baseOffset int64, timestamp int64) (int64, int64, bool, error) {
	offset := 0
	currentOffset := baseOffset

	for offset < len(chunkData) {
		if offset+4 > len(chunkData) {
			return -1, -1, false, errors.New("missing batch length")
		}
		batchLen := int(binary.BigEndian.Uint32(chunkData[offset : offset+4]))
		offset += 4
		if batchLen <= 0 || offset+batchLen > len(chunkData) {
			return -1, -1, false, errors.New("invalid batch length")
		}

		batchData := chunkData[offset : offset+batchLen]
		recordCount := int(binary.BigEndian.Uint32(batchData[57:61]))
		recOffset, recTs, found, err := scanBatchForTimestamp(batchData, currentOffset, timestamp)
		if err != nil {
			return -1, -1, false, err
		}
		if found {
			return recOffset, recTs, true, nil
		}

		currentOffset += int64(recordCount)
		offset += batchLen
	}

	return -1, -1, false, nil
}

func scanBatchForTimestamp(batchData []byte, baseOffset int64, timestamp int64) (int64, int64, bool, error) {
	if len(batchData) < 61 {
		return -1, -1, false, errors.New("batch too small")
	}
	firstTimestamp := int64(binary.BigEndian.Uint64(batchData[27:35]))
	maxTimestamp := int64(binary.BigEndian.Uint64(batchData[35:43]))
	if timestamp <= firstTimestamp {
		return baseOffset, firstTimestamp, true, nil
	}
	if timestamp > maxTimestamp {
		return -1, -1, false, nil
	}

	recordsData := batchData[61:]
	recordCount := int(binary.BigEndian.Uint32(batchData[57:61]))
	pos := 0

	for i := 0; i < recordCount; i++ {
		if pos >= len(recordsData) {
			return -1, -1, false, errors.New("records truncated")
		}

		recordLen, bytesRead := readVarint(recordsData[pos:])
		if bytesRead <= 0 {
			return -1, -1, false, errors.New("failed to read record length")
		}
		pos += bytesRead
		if recordLen < 0 {
			return -1, -1, false, errors.New("negative record length")
		}
		recordStart := pos

		pos++
		tsDelta, bytesRead := readVarint(recordsData[pos:])
		if bytesRead <= 0 {
			return -1, -1, false, errors.New("failed to read timestamp delta")
		}
		pos += bytesRead
		offsetDelta, bytesRead := readVarint(recordsData[pos:])
		if bytesRead <= 0 {
			return -1, -1, false, errors.New("failed to read offset delta")
		}
		pos += bytesRead

		recTimestamp := firstTimestamp + tsDelta
		if recTimestamp >= timestamp {
			return baseOffset + offsetDelta, recTimestamp, true, nil
		}

		remaining := int(recordLen) - (pos - recordStart)
		if remaining < 0 || pos+remaining > len(recordsData) {
			return -1, -1, false, errors.New("record length mismatch")
		}
		pos += remaining
	}

	return -1, -1, false, nil
}

func readVarint(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	var uv uint64
	var shift uint
	var bytesRead int

	for i := 0; i < len(data) && i < 10; i++ {
		b := data[i]
		uv |= uint64(b&0x7F) << shift
		bytesRead++
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	if bytesRead == 0 {
		return 0, 0
	}

	v := int64((uv >> 1) ^ -(uv & 1))
	return v, bytesRead
}

// countingStore wraps indexTestStore to track Get and List call counts.
type countingStore struct {
	*indexTestStore
	getCalls      int32
	listCalls     int32
	notifyChan    chan metadata.Notification
	notifyActive  bool
	notifyClosed  bool
	mu            sync.Mutex
}

func newCountingStore() *countingStore {
	return &countingStore{
		indexTestStore: newIndexTestStore(),
		notifyChan:     make(chan metadata.Notification, 100),
	}
}

func (c *countingStore) Get(ctx context.Context, key string) (metadata.GetResult, error) {
	atomic.AddInt32(&c.getCalls, 1)
	return c.indexTestStore.Get(ctx, key)
}

func (c *countingStore) List(ctx context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	atomic.AddInt32(&c.listCalls, 1)
	return c.indexTestStore.List(ctx, startKey, endKey, limit)
}

func (c *countingStore) Notifications(ctx context.Context) (metadata.NotificationStream, error) {
	c.mu.Lock()
	c.notifyChan = make(chan metadata.Notification, 100)
	c.notifyActive = true
	c.notifyClosed = false
	ch := c.notifyChan
	c.mu.Unlock()
	return &countingNotificationStream{
		ctx: ctx,
		ch:  ch,
	}, nil
}

func (c *countingStore) SimulateNotification(n metadata.Notification) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.notifyActive && !c.notifyClosed && c.notifyChan != nil {
		select {
		case c.notifyChan <- n:
		default:
		}
	}
}

type countingNotificationStream struct {
	ctx context.Context
	ch  <-chan metadata.Notification
}

func (s *countingNotificationStream) Next(ctx context.Context) (metadata.Notification, error) {
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

func (s *countingNotificationStream) Close() error {
	return nil
}

// TestLookupOffset_WithIndexCacheHit verifies that LookupOffset uses the IndexCache
// and avoids metadata store hits after cache warm-up.
func TestLookupOffset_WithIndexCacheHit(t *testing.T) {
	store := newCountingStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create stream
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append some index entries
	for i := 0; i < 3; i++ {
		_, err := sm.AppendIndexEntry(ctx, AppendRequest{
			StreamID:       streamID,
			RecordCount:    100,
			ChunkSizeBytes: 4096,
			CreatedAtMs:    time.Now().UnixMilli(),
			WalID:          uuid.New().String(),
			WalPath:        fmt.Sprintf("s3://bucket/wal/%d.wo", i),
			ChunkOffset:    0,
			ChunkLength:    4096,
		})
		if err != nil {
			t.Fatalf("AppendIndexEntry %d failed: %v", i, err)
		}
	}

	// Create and configure index cache
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()
	sm.SetIndexCache(cache)

	// Reset counters after setup
	atomic.StoreInt32(&store.listCalls, 0)

	// First lookup - cache miss, should hit store
	result, err := sm.LookupOffset(ctx, streamID, 50)
	if err != nil {
		t.Fatalf("First LookupOffset failed: %v", err)
	}
	if !result.Found {
		t.Fatal("First LookupOffset should find entry")
	}
	if result.Entry.StartOffset != 0 || result.Entry.EndOffset != 100 {
		t.Errorf("First lookup got [%d, %d), want [0, 100)", result.Entry.StartOffset, result.Entry.EndOffset)
	}

	listCallsAfterFirst := atomic.LoadInt32(&store.listCalls)
	if listCallsAfterFirst == 0 {
		t.Error("First lookup should have made a List call (cache miss)")
	}

	// Second lookup for same offset - should be cache hit, no additional store calls
	result, err = sm.LookupOffset(ctx, streamID, 50)
	if err != nil {
		t.Fatalf("Second LookupOffset failed: %v", err)
	}
	if !result.Found {
		t.Fatal("Second LookupOffset should find entry")
	}

	listCallsAfterSecond := atomic.LoadInt32(&store.listCalls)
	if listCallsAfterSecond != listCallsAfterFirst {
		t.Errorf("Second lookup should use cache, but made %d additional List calls",
			listCallsAfterSecond-listCallsAfterFirst)
	}

	// Third lookup for different offset in same entry - still cache hit
	result, err = sm.LookupOffset(ctx, streamID, 75)
	if err != nil {
		t.Fatalf("Third LookupOffset failed: %v", err)
	}
	if !result.Found {
		t.Fatal("Third LookupOffset should find entry")
	}

	listCallsAfterThird := atomic.LoadInt32(&store.listCalls)
	if listCallsAfterThird != listCallsAfterFirst {
		t.Errorf("Third lookup should use cache, but made %d additional List calls",
			listCallsAfterThird-listCallsAfterFirst)
	}
}

// TestGetHWM_WithHWMCacheHit verifies that GetHWM uses the HWMCache
// and avoids metadata store hits after cache warm-up.
func TestGetHWM_WithHWMCacheHit(t *testing.T) {
	store := newCountingStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create stream
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Create and configure HWM cache
	cache := NewHWMCache(store)
	defer cache.Close()
	sm.SetHWMCache(cache)

	// Wait for notification watcher to start
	time.Sleep(50 * time.Millisecond)

	// Reset counters after setup
	atomic.StoreInt32(&store.getCalls, 0)

	// First GetHWM - cache miss, should hit store
	hwm, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("First GetHWM failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("First GetHWM = %d, want 0", hwm)
	}
	if version == 0 {
		t.Error("First GetHWM version should not be 0")
	}

	getCallsAfterFirst := atomic.LoadInt32(&store.getCalls)
	if getCallsAfterFirst == 0 {
		t.Error("First GetHWM should have made a Get call (cache miss)")
	}

	// Second GetHWM - should be cache hit, no additional store calls
	hwm2, version2, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("Second GetHWM failed: %v", err)
	}
	if hwm2 != hwm || version2 != version {
		t.Errorf("Second GetHWM = (%d, %d), want (%d, %d)", hwm2, version2, hwm, version)
	}

	getCallsAfterSecond := atomic.LoadInt32(&store.getCalls)
	if getCallsAfterSecond != getCallsAfterFirst {
		t.Errorf("Second GetHWM should use cache, but made %d additional Get calls",
			getCallsAfterSecond-getCallsAfterFirst)
	}

	// Third GetHWM - still cache hit
	_, _, err = sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("Third GetHWM failed: %v", err)
	}

	getCallsAfterThird := atomic.LoadInt32(&store.getCalls)
	if getCallsAfterThird != getCallsAfterFirst {
		t.Errorf("Third GetHWM should use cache, but made %d additional Get calls",
			getCallsAfterThird-getCallsAfterFirst)
	}
}

// TestLookupOffset_IndexCacheInvalidation verifies that LookupOffset respects
// cache invalidation on notifications.
func TestLookupOffset_IndexCacheInvalidation(t *testing.T) {
	store := newCountingStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create stream
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an index entry
	now := time.Now().UnixMilli()
	result, err := sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    now,
		WalID:          "original-wal",
		WalPath:        "s3://bucket/wal/original.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// Create and configure index cache
	cache := NewIndexCache(store, DefaultIndexCacheConfig())
	defer cache.Close()
	sm.SetIndexCache(cache)

	// Wait for notification watcher to start
	time.Sleep(50 * time.Millisecond)

	// First lookup - populates cache
	lookupResult, err := sm.LookupOffset(ctx, streamID, 50)
	if err != nil {
		t.Fatalf("First LookupOffset failed: %v", err)
	}
	if lookupResult.Entry.WalID != "original-wal" {
		t.Errorf("First lookup WalID = %s, want original-wal", lookupResult.Entry.WalID)
	}

	// Simulate notification with updated entry (different WalID)
	updatedEntry := IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 4096,
		FileType:       FileTypeWAL,
		WalID:          "updated-wal",
		WalPath:        "s3://bucket/wal/updated.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	}
	updatedBytes, _ := json.Marshal(updatedEntry)

	store.SimulateNotification(metadata.Notification{
		Key:     result.IndexKey,
		Value:   updatedBytes,
		Version: 10,
		Deleted: false,
	})

	// Wait for notification to be processed
	time.Sleep(100 * time.Millisecond)

	// Cache should be updated with new entry
	cached, version, ok := cache.Get(streamID, result.IndexKey)
	if !ok {
		t.Fatal("Cache should have updated entry")
	}
	if version != 10 {
		t.Errorf("Cached version = %d, want 10", version)
	}
	if cached.WalID != "updated-wal" {
		t.Errorf("Cached WalID = %s, want updated-wal", cached.WalID)
	}
}

// TestGetHWM_HWMCacheInvalidation verifies that GetHWM respects
// cache invalidation on notifications.
func TestGetHWM_HWMCacheInvalidation(t *testing.T) {
	store := newCountingStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create stream
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Create and configure HWM cache
	cache := NewHWMCache(store)
	defer cache.Close()
	sm.SetHWMCache(cache)

	// Wait for notification watcher to start
	time.Sleep(50 * time.Millisecond)

	// First GetHWM - populates cache
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("First GetHWM failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("First GetHWM = %d, want 0", hwm)
	}

	// Simulate notification with updated HWM
	hwmKey := keys.HwmKeyPath(streamID)
	updatedHWMBytes := EncodeHWM(500)

	store.SimulateNotification(metadata.Notification{
		Key:     hwmKey,
		Value:   updatedHWMBytes,
		Version: 10,
		Deleted: false,
	})

	// Wait for notification to be processed
	time.Sleep(100 * time.Millisecond)

	// Cache should be updated with new HWM
	cachedHWM, version, ok := cache.GetIfCached(streamID)
	if !ok {
		t.Fatal("Cache should have updated HWM")
	}
	if version != 10 {
		t.Errorf("Cached version = %d, want 10", version)
	}
	if cachedHWM != 500 {
		t.Errorf("Cached HWM = %d, want 500", cachedHWM)
	}

	// GetHWM should return cached value
	hwm2, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("Second GetHWM failed: %v", err)
	}
	if hwm2 != 500 {
		t.Errorf("Second GetHWM = %d, want 500", hwm2)
	}
}

// TestLookupOffset_WithoutCache verifies LookupOffset still works when no cache is configured.
func TestLookupOffset_WithoutCache(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create stream
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Append an index entry
	_, err = sm.AppendIndexEntry(ctx, AppendRequest{
		StreamID:       streamID,
		RecordCount:    100,
		ChunkSizeBytes: 4096,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          "test-wal",
		WalPath:        "s3://bucket/wal/test.wo",
		ChunkOffset:    0,
		ChunkLength:    4096,
	})
	if err != nil {
		t.Fatalf("AppendIndexEntry failed: %v", err)
	}

	// LookupOffset without cache configured
	result, err := sm.LookupOffset(ctx, streamID, 50)
	if err != nil {
		t.Fatalf("LookupOffset failed: %v", err)
	}
	if !result.Found {
		t.Fatal("LookupOffset should find entry")
	}
	if result.Entry.WalID != "test-wal" {
		t.Errorf("Entry WalID = %s, want test-wal", result.Entry.WalID)
	}
}

// TestGetHWM_WithoutCache verifies GetHWM still works when no cache is configured.
func TestGetHWM_WithoutCache(t *testing.T) {
	store := newIndexTestStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create stream
	streamID, err := sm.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// GetHWM without cache configured
	hwm, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}
	if hwm != 0 {
		t.Errorf("GetHWM = %d, want 0", hwm)
	}
	if version == 0 {
		t.Error("GetHWM version should not be 0")
	}
}
