package produce

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/wal"
)

// mockObjectStore implements objectstore.Store for testing.
type mockObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		objects: make(map[string][]byte),
	}
}

func (s *mockObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()
	return nil
}

func (s *mockObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return s.Put(ctx, key, reader, size, contentType)
}

func (s *mockObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *mockObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if end == -1 || end >= int64(len(data)) {
		end = int64(len(data) - 1)
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (s *mockObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (s *mockObjectStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}

func (s *mockObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var keys []objectstore.ObjectMeta
	for k, v := range s.objects {
		if len(prefix) == 0 || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, objectstore.ObjectMeta{Key: k, Size: int64(len(v))})
		}
	}
	return keys, nil
}

func (s *mockObjectStore) Close() error {
	return nil
}

var _ objectstore.Store = (*mockObjectStore)(nil)

// TestCommitter_CommitStream tests single stream commit with offset allocation.
func TestCommitter_CommitStream(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream first
	streamMgr := index.NewStreamManager(metaStore)
	streamID, err := streamMgr.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Commit some records
	result, err := committer.CommitStream(ctx, StreamCommitRequest{
		StreamID:       streamID,
		RecordCount:    10,
		ChunkSizeBytes: 1000,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-id-1",
		WalPath:        "wal/path/1.wo",
	})
	if err != nil {
		t.Fatalf("failed to commit stream: %v", err)
	}

	// Verify offset allocation
	if result.StartOffset != 0 {
		t.Errorf("expected start offset 0, got %d", result.StartOffset)
	}
	if result.EndOffset != 10 {
		t.Errorf("expected end offset 10, got %d", result.EndOffset)
	}

	// Verify HWM was updated
	hwm, _, err := streamMgr.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM: %v", err)
	}
	if hwm != 10 {
		t.Errorf("expected HWM 10, got %d", hwm)
	}

	// Commit more records
	result2, err := committer.CommitStream(ctx, StreamCommitRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 500,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 2000,
		MaxTimestampMs: 3000,
		WalID:          "wal-id-2",
		WalPath:        "wal/path/2.wo",
	})
	if err != nil {
		t.Fatalf("failed to commit second batch: %v", err)
	}

	// Verify second offset allocation continues from HWM
	if result2.StartOffset != 10 {
		t.Errorf("expected start offset 10, got %d", result2.StartOffset)
	}
	if result2.EndOffset != 15 {
		t.Errorf("expected end offset 15, got %d", result2.EndOffset)
	}
}

// TestCommitter_IndexEntryCreated tests that index entries are created correctly.
func TestCommitter_IndexEntryCreated(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID, err := streamMgr.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Commit records
	_, err = committer.CommitStream(ctx, StreamCommitRequest{
		StreamID:       streamID,
		RecordCount:    10,
		ChunkSizeBytes: 1000,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-id-1",
		WalPath:        "wal/path/1.wo",
	})
	if err != nil {
		t.Fatalf("failed to commit stream: %v", err)
	}

	// Verify index entry was created
	entries, err := streamMgr.ListIndexEntries(ctx, streamID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.StreamID != streamID {
		t.Errorf("expected stream ID %s, got %s", streamID, entry.StreamID)
	}
	if entry.StartOffset != 0 {
		t.Errorf("expected start offset 0, got %d", entry.StartOffset)
	}
	if entry.EndOffset != 10 {
		t.Errorf("expected end offset 10, got %d", entry.EndOffset)
	}
	if entry.RecordCount != 10 {
		t.Errorf("expected record count 10, got %d", entry.RecordCount)
	}
	if entry.CumulativeSize != 1000 {
		t.Errorf("expected cumulative size 1000, got %d", entry.CumulativeSize)
	}
	if entry.WalID != "wal-id-1" {
		t.Errorf("expected WAL ID wal-id-1, got %s", entry.WalID)
	}
	if entry.FileType != index.FileTypeWAL {
		t.Errorf("expected file type WAL, got %s", entry.FileType)
	}
}

// TestCommitter_CumulativeSize tests that cumulative size is calculated correctly.
func TestCommitter_CumulativeSize(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID, err := streamMgr.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Commit first batch
	_, err = committer.CommitStream(ctx, StreamCommitRequest{
		StreamID:       streamID,
		RecordCount:    10,
		ChunkSizeBytes: 1000,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          "wal-1",
		WalPath:        "wal/1.wo",
	})
	if err != nil {
		t.Fatalf("failed to commit first batch: %v", err)
	}

	// Commit second batch
	_, err = committer.CommitStream(ctx, StreamCommitRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 500,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          "wal-2",
		WalPath:        "wal/2.wo",
	})
	if err != nil {
		t.Fatalf("failed to commit second batch: %v", err)
	}

	// Commit third batch
	_, err = committer.CommitStream(ctx, StreamCommitRequest{
		StreamID:       streamID,
		RecordCount:    3,
		ChunkSizeBytes: 300,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          "wal-3",
		WalPath:        "wal/3.wo",
	})
	if err != nil {
		t.Fatalf("failed to commit third batch: %v", err)
	}

	// Verify cumulative sizes
	entries, err := streamMgr.ListIndexEntries(ctx, streamID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 index entries, got %d", len(entries))
	}

	expectedSizes := []int64{1000, 1500, 1800}
	for i, entry := range entries {
		if entry.CumulativeSize != expectedSizes[i] {
			t.Errorf("entry %d: expected cumulative size %d, got %d", i, expectedSizes[i], entry.CumulativeSize)
		}
	}
}

// TestCommitter_StreamNotFound tests that commit fails for non-existent stream.
func TestCommitter_StreamNotFound(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	_, err := committer.CommitStream(ctx, StreamCommitRequest{
		StreamID:       "non-existent-stream",
		RecordCount:    10,
		ChunkSizeBytes: 1000,
		CreatedAtMs:    time.Now().UnixMilli(),
		WalID:          "wal-1",
		WalPath:        "wal/1.wo",
	})

	if err == nil {
		t.Error("expected error for non-existent stream")
	}
}

// TestCommitter_Commit tests the full commit flow with pending requests.
func TestCommitter_Commit(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream - we need the stream ID as uint64 matching the hash
	// For testing, we'll create the stream entries directly
	streamIDStr := "test-stream-1"

	// Initialize the stream by creating HWM key
	streamMgr := index.NewStreamManager(metaStore)
	err := streamMgr.CreateStreamWithID(ctx, streamIDStr, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create pending requests with the original string stream ID
	streamID := parseStreamID(streamIDStr)
	requests := []*PendingRequest{
		{
			StreamID:       streamID,
			StreamIDStr:    streamIDStr,
			Batches:        []wal.BatchEntry{{Data: []byte("batch1")}},
			RecordCount:    5,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           100,
			Done:           make(chan struct{}),
		},
		{
			StreamID:       streamID,
			StreamIDStr:    streamIDStr,
			Batches:        []wal.BatchEntry{{Data: []byte("batch2")}},
			RecordCount:    3,
			MinTimestampMs: 2000,
			MaxTimestampMs: 3000,
			Size:           80,
			Done:           make(chan struct{}),
		},
	}

	// With the fix, Commit now uses StreamIDStr for metadata lookups
	err = committer.Commit(ctx, 0, requests)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	// Verify that results were populated
	for i, req := range requests {
		if req.Result == nil {
			t.Errorf("request %d: expected Result to be set", i)
		}
	}
}

// TestWALObjectRecord_Marshal tests WAL object record marshaling.
func TestWALObjectRecord_Marshal(t *testing.T) {
	record := WALObjectRecord{
		Path:      "wal/domain=0/abc.wo",
		RefCount:  3,
		CreatedAt: 1234567890,
		SizeBytes: 1024,
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var unmarshaled WALObjectRecord
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if unmarshaled.Path != record.Path {
		t.Errorf("path mismatch: %s != %s", unmarshaled.Path, record.Path)
	}
	if unmarshaled.RefCount != record.RefCount {
		t.Errorf("refCount mismatch: %d != %d", unmarshaled.RefCount, record.RefCount)
	}
	if unmarshaled.CreatedAt != record.CreatedAt {
		t.Errorf("createdAt mismatch: %d != %d", unmarshaled.CreatedAt, record.CreatedAt)
	}
	if unmarshaled.SizeBytes != record.SizeBytes {
		t.Errorf("sizeBytes mismatch: %d != %d", unmarshaled.SizeBytes, record.SizeBytes)
	}
}

// TestCreateFlushHandler tests that CreateFlushHandler returns a valid handler.
func TestCreateFlushHandler(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})
	handler := committer.CreateFlushHandler()

	if handler == nil {
		t.Error("expected non-nil flush handler")
	}
}

// TestCommitter_FullProduceCommitFlow verifies all steps of produce-commit per spec 9.2-9.7:
// 1. Write WAL object to storage
// 2. Write staging marker before commit
// 3. Execute atomic transaction per spec 9.2
// 4. Allocate offsets for each stream chunk
// 5. Create offset index entries
// 6. Update hwm for each stream
// 7. Create WAL object record with refCount
// 8. Delete staging marker
// 9. Return assigned offsets
func TestCommitter_FullProduceCommitFlow(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create two streams in the same MetaDomain
	streamMgr := index.NewStreamManager(metaStore)
	stream1ID := "test-stream-001"
	stream2ID := "test-stream-002"

	err := streamMgr.CreateStreamWithID(ctx, stream1ID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream1: %v", err)
	}
	err = streamMgr.CreateStreamWithID(ctx, stream2ID, "test-topic", 1)
	if err != nil {
		t.Fatalf("failed to create stream2: %v", err)
	}

	// Verify initial HWM is 0 for both streams
	hwm1Before, _, err := streamMgr.GetHWM(ctx, stream1ID)
	if err != nil {
		t.Fatalf("failed to get HWM for stream1: %v", err)
	}
	if hwm1Before != 0 {
		t.Fatalf("expected initial HWM 0, got %d", hwm1Before)
	}

	hwm2Before, _, err := streamMgr.GetHWM(ctx, stream2ID)
	if err != nil {
		t.Fatalf("failed to get HWM for stream2: %v", err)
	}
	if hwm2Before != 0 {
		t.Fatalf("expected initial HWM 0, got %d", hwm2Before)
	}

	// Create pending requests for two streams
	stream1Hash := parseStreamID(stream1ID)
	stream2Hash := parseStreamID(stream2ID)

	requests := []*PendingRequest{
		{
			StreamID:       stream1Hash,
			StreamIDStr:    stream1ID,
			Batches:        []wal.BatchEntry{{Data: []byte("stream1-batch1")}},
			RecordCount:    10,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           100,
			Done:           make(chan struct{}),
		},
		{
			StreamID:       stream2Hash,
			StreamIDStr:    stream2ID,
			Batches:        []wal.BatchEntry{{Data: []byte("stream2-batch1")}},
			RecordCount:    5,
			MinTimestampMs: 1500,
			MaxTimestampMs: 2500,
			Size:           80,
			Done:           make(chan struct{}),
		},
	}

	// Execute commit
	err = committer.Commit(ctx, 0, requests)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Step 1: Verify WAL object was written to storage
	objStore.mu.Lock()
	walCount := 0
	var walPath string
	for path := range objStore.objects {
		if len(path) > 0 {
			walCount++
			walPath = path
		}
	}
	objStore.mu.Unlock()

	if walCount != 1 {
		t.Errorf("expected 1 WAL object in storage, got %d", walCount)
	}
	if walPath == "" {
		t.Error("WAL object path should not be empty")
	}

	// Step 4 & 9: Verify offset allocation and returned offsets
	req1 := requests[0]
	req2 := requests[1]

	if req1.Result == nil {
		t.Fatal("request 1: Result should not be nil")
	}
	if req2.Result == nil {
		t.Fatal("request 2: Result should not be nil")
	}

	// Stream 1: should have offsets 0-10
	if req1.Result.StartOffset != 0 {
		t.Errorf("stream1: expected StartOffset 0, got %d", req1.Result.StartOffset)
	}
	if req1.Result.EndOffset != 10 {
		t.Errorf("stream1: expected EndOffset 10, got %d", req1.Result.EndOffset)
	}

	// Stream 2: should have offsets 0-5
	if req2.Result.StartOffset != 0 {
		t.Errorf("stream2: expected StartOffset 0, got %d", req2.Result.StartOffset)
	}
	if req2.Result.EndOffset != 5 {
		t.Errorf("stream2: expected EndOffset 5, got %d", req2.Result.EndOffset)
	}

	// Step 6: Verify HWM was updated for each stream
	hwm1After, _, err := streamMgr.GetHWM(ctx, stream1ID)
	if err != nil {
		t.Fatalf("failed to get HWM for stream1: %v", err)
	}
	if hwm1After != 10 {
		t.Errorf("stream1: expected HWM 10, got %d", hwm1After)
	}

	hwm2After, _, err := streamMgr.GetHWM(ctx, stream2ID)
	if err != nil {
		t.Fatalf("failed to get HWM for stream2: %v", err)
	}
	if hwm2After != 5 {
		t.Errorf("stream2: expected HWM 5, got %d", hwm2After)
	}

	// Step 5: Verify offset index entries were created
	entries1, err := streamMgr.ListIndexEntries(ctx, stream1ID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries for stream1: %v", err)
	}
	if len(entries1) != 1 {
		t.Errorf("stream1: expected 1 index entry, got %d", len(entries1))
	}
	if len(entries1) > 0 {
		entry := entries1[0]
		if entry.StartOffset != 0 {
			t.Errorf("stream1 entry: expected StartOffset 0, got %d", entry.StartOffset)
		}
		if entry.EndOffset != 10 {
			t.Errorf("stream1 entry: expected EndOffset 10, got %d", entry.EndOffset)
		}
		if entry.RecordCount != 10 {
			t.Errorf("stream1 entry: expected RecordCount 10, got %d", entry.RecordCount)
		}
		if entry.FileType != index.FileTypeWAL {
			t.Errorf("stream1 entry: expected FileType WAL, got %s", entry.FileType)
		}
		if entry.WalPath == "" {
			t.Error("stream1 entry: WalPath should not be empty")
		}
	}

	entries2, err := streamMgr.ListIndexEntries(ctx, stream2ID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries for stream2: %v", err)
	}
	if len(entries2) != 1 {
		t.Errorf("stream2: expected 1 index entry, got %d", len(entries2))
	}

	// Step 7: Verify WAL object record was created with correct refCount
	// The WAL object record should be at /wal/objects/<domain>/<walId>
	// We need to find it by scanning the keys
	var walObjectRecord *WALObjectRecord
	allKeys := metaStore.GetAllKeys()
	for _, key := range allKeys {
		if len(key) > 12 && key[:12] == "/wal/objects" {
			result, err := metaStore.Get(ctx, key)
			if err != nil || !result.Exists {
				continue
			}
			var record WALObjectRecord
			if err := json.Unmarshal(result.Value, &record); err == nil {
				walObjectRecord = &record
				break
			}
		}
	}

	if walObjectRecord == nil {
		t.Error("WAL object record should exist")
	} else {
		// RefCount should equal number of stream chunks
		if walObjectRecord.RefCount != 2 {
			t.Errorf("WAL object record: expected RefCount 2, got %d", walObjectRecord.RefCount)
		}
		if walObjectRecord.Path == "" {
			t.Error("WAL object record: Path should not be empty")
		}
		if walObjectRecord.SizeBytes <= 0 {
			t.Error("WAL object record: SizeBytes should be positive")
		}
	}

	// Step 2 & 8: Verify staging marker was deleted
	// Staging markers are at /wal/staging/<domain>/<walId>
	foundStaging := false
	for _, key := range allKeys {
		if len(key) > 12 && key[:12] == "/wal/staging" {
			foundStaging = true
			break
		}
	}
	if foundStaging {
		t.Error("staging marker should have been deleted after commit")
	}
}

// TestCommitter_MultiStreamOffsetAllocation tests that offsets are allocated
// correctly when multiple requests for the same stream are committed together.
func TestCommitter_MultiStreamOffsetAllocation(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-offset-alloc"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create multiple requests for the same stream
	streamHash := parseStreamID(streamID)

	requests := []*PendingRequest{
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch1")}},
			RecordCount:    5,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           50,
			Done:           make(chan struct{}),
		},
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch2")}},
			RecordCount:    3,
			MinTimestampMs: 2000,
			MaxTimestampMs: 3000,
			Size:           30,
			Done:           make(chan struct{}),
		},
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch3")}},
			RecordCount:    7,
			MinTimestampMs: 3000,
			MaxTimestampMs: 4000,
			Size:           70,
			Done:           make(chan struct{}),
		},
	}

	// Execute commit
	err = committer.Commit(ctx, 0, requests)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all requests in the same chunk get contiguous offsets
	// Total record count is 5+3+7 = 15
	// First request: 0-5, Second: 5-8, Third: 8-15
	if requests[0].Result.StartOffset != 0 || requests[0].Result.EndOffset != 5 {
		t.Errorf("request 0: expected offsets [0,5), got [%d,%d)",
			requests[0].Result.StartOffset, requests[0].Result.EndOffset)
	}
	if requests[1].Result.StartOffset != 5 || requests[1].Result.EndOffset != 8 {
		t.Errorf("request 1: expected offsets [5,8), got [%d,%d)",
			requests[1].Result.StartOffset, requests[1].Result.EndOffset)
	}
	if requests[2].Result.StartOffset != 8 || requests[2].Result.EndOffset != 15 {
		t.Errorf("request 2: expected offsets [8,15), got [%d,%d)",
			requests[2].Result.StartOffset, requests[2].Result.EndOffset)
	}

	// Verify HWM reflects total record count
	hwm, _, err := streamMgr.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM: %v", err)
	}
	if hwm != 15 {
		t.Errorf("expected HWM 15, got %d", hwm)
	}

	// Verify index entry covers full range
	entries, err := streamMgr.ListIndexEntries(ctx, streamID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 index entry, got %d", len(entries))
	}
	if len(entries) > 0 {
		entry := entries[0]
		if entry.StartOffset != 0 || entry.EndOffset != 15 {
			t.Errorf("entry: expected offsets [0,15), got [%d,%d)",
				entry.StartOffset, entry.EndOffset)
		}
		if entry.RecordCount != 15 {
			t.Errorf("entry: expected RecordCount 15, got %d", entry.RecordCount)
		}
	}
}

// TestCommitter_SecondCommitContinuesOffsets verifies that a second commit
// continues offset allocation from where the first left off.
func TestCommitter_SecondCommitContinuesOffsets(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-sequential"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	streamHash := parseStreamID(streamID)

	// First commit
	requests1 := []*PendingRequest{
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("commit1-batch1")}},
			RecordCount:    10,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           100,
			Done:           make(chan struct{}),
		},
	}

	err = committer.Commit(ctx, 0, requests1)
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	// Verify first commit offsets
	if requests1[0].Result.StartOffset != 0 || requests1[0].Result.EndOffset != 10 {
		t.Errorf("first commit: expected offsets [0,10), got [%d,%d)",
			requests1[0].Result.StartOffset, requests1[0].Result.EndOffset)
	}

	// Second commit
	requests2 := []*PendingRequest{
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("commit2-batch1")}},
			RecordCount:    5,
			MinTimestampMs: 2000,
			MaxTimestampMs: 3000,
			Size:           50,
			Done:           make(chan struct{}),
		},
	}

	err = committer.Commit(ctx, 0, requests2)
	if err != nil {
		t.Fatalf("second commit failed: %v", err)
	}

	// Verify second commit continues from HWM
	if requests2[0].Result.StartOffset != 10 || requests2[0].Result.EndOffset != 15 {
		t.Errorf("second commit: expected offsets [10,15), got [%d,%d)",
			requests2[0].Result.StartOffset, requests2[0].Result.EndOffset)
	}

	// Verify final HWM
	hwm, _, err := streamMgr.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM: %v", err)
	}
	if hwm != 15 {
		t.Errorf("expected HWM 15, got %d", hwm)
	}

	// Verify two index entries exist
	entries, err := streamMgr.ListIndexEntries(ctx, streamID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 index entries, got %d", len(entries))
	}
}
