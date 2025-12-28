package produce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
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

// --- WAL Write Failure Tests (produce-failure-wal-write task) ---

// failingObjectStore is a mock that fails on Put to simulate WAL write failures.
type failingObjectStore struct {
	*mockObjectStore
	failPut bool
	putErr  error
}

func newFailingObjectStore(failPut bool) *failingObjectStore {
	return &failingObjectStore{
		mockObjectStore: newMockObjectStore(),
		failPut:         failPut,
		putErr:          errors.New("simulated S3 put failure"),
	}
}

func (s *failingObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	if s.failPut {
		return s.putErr
	}
	return s.mockObjectStore.Put(ctx, key, reader, size, contentType)
}

func (s *failingObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	if s.failPut {
		return s.putErr
	}
	return s.mockObjectStore.PutWithOptions(ctx, key, reader, size, contentType, opts)
}

var _ objectstore.Store = (*failingObjectStore)(nil)

// TestCommitter_WALWriteFailure_NoMetadataCommit verifies that when WAL write fails,
// no metadata commit occurs (no HWM update, no index entries, no WAL object record).
func TestCommitter_WALWriteFailure_NoMetadataCommit(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newFailingObjectStore(true)
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-wal-fail"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Record initial state
	hwmBefore, _, err := streamMgr.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get initial HWM: %v", err)
	}

	entriesBefore, err := streamMgr.ListIndexEntries(ctx, streamID, 10)
	if err != nil {
		t.Fatalf("failed to list initial index entries: %v", err)
	}

	// Count initial WAL object records
	allKeysBefore := metaStore.GetAllKeys()
	walRecordCountBefore := 0
	for _, key := range allKeysBefore {
		if len(key) > 12 && key[:12] == "/wal/objects" {
			walRecordCountBefore++
		}
	}

	// Create pending requests
	streamHash := parseStreamID(streamID)
	requests := []*PendingRequest{
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch1")}},
			RecordCount:    10,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           100,
			Done:           make(chan struct{}),
		},
	}

	// Execute commit - should fail due to WAL write failure
	err = committer.Commit(ctx, 0, requests)
	if err == nil {
		t.Fatal("expected commit to fail due to WAL write failure")
	}

	// Verify error message indicates WAL failure
	if !strings.Contains(err.Error(), "write to object store failed") &&
		!strings.Contains(err.Error(), "flush WAL") {
		t.Errorf("expected error to mention WAL/object store failure, got: %v", err)
	}

	// Verify no metadata changes occurred:

	// 1. HWM should not have changed
	hwmAfter, _, err := streamMgr.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM after failure: %v", err)
	}
	if hwmAfter != hwmBefore {
		t.Errorf("HWM should not have changed on WAL failure: was %d, now %d", hwmBefore, hwmAfter)
	}

	// 2. No new index entries should have been created
	entriesAfter, err := streamMgr.ListIndexEntries(ctx, streamID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries after failure: %v", err)
	}
	if len(entriesAfter) != len(entriesBefore) {
		t.Errorf("index entries should not have changed on WAL failure: was %d, now %d",
			len(entriesBefore), len(entriesAfter))
	}

	// 3. No new WAL object record should have been created
	allKeysAfter := metaStore.GetAllKeys()
	walRecordCountAfter := 0
	for _, key := range allKeysAfter {
		if len(key) > 12 && key[:12] == "/wal/objects" {
			walRecordCountAfter++
		}
	}
	if walRecordCountAfter != walRecordCountBefore {
		t.Errorf("WAL object records should not have changed on WAL failure: was %d, now %d",
			walRecordCountBefore, walRecordCountAfter)
	}

	// 4. Verify result was not populated
	if requests[0].Result != nil {
		t.Error("request Result should be nil when WAL write fails")
	}
}

// TestCommitter_WALWriteFailure_StagingMarkerRemains verifies that when WAL write fails,
// the staging marker remains for orphan GC to clean up.
func TestCommitter_WALWriteFailure_StagingMarkerRemains(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newFailingObjectStore(true)
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-staging-remains"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create pending requests
	streamHash := parseStreamID(streamID)
	requests := []*PendingRequest{
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch1")}},
			RecordCount:    10,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           100,
			Done:           make(chan struct{}),
		},
	}

	// Execute commit - should fail due to WAL write failure
	err = committer.Commit(ctx, 0, requests)
	if err == nil {
		t.Fatal("expected commit to fail due to WAL write failure")
	}

	// Verify staging marker exists (it was written before the WAL object write failed)
	allKeys := metaStore.GetAllKeys()
	foundStaging := false
	for _, key := range allKeys {
		if len(key) > 12 && key[:12] == "/wal/staging" {
			foundStaging = true
			break
		}
	}

	if !foundStaging {
		t.Error("staging marker should remain after WAL write failure (for orphan GC)")
	}
}

// TestCommitter_WALWriteFailure_ErrorReturned verifies that WAL write failure
// returns an appropriate error to the caller.
func TestCommitter_WALWriteFailure_ErrorReturned(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newFailingObjectStore(true)
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-error-return"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create pending requests
	streamHash := parseStreamID(streamID)
	requests := []*PendingRequest{
		{
			StreamID:       streamHash,
			StreamIDStr:    streamID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch1")}},
			RecordCount:    10,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           100,
			Done:           make(chan struct{}),
		},
	}

	// Execute commit
	err = committer.Commit(ctx, 0, requests)

	// Must return an error
	if err == nil {
		t.Fatal("expected error to be returned when WAL write fails")
	}

	// The error should be wrapped appropriately
	// It should mention the produce/WAL context
	errStr := err.Error()
	if !strings.Contains(errStr, "WAL") && !strings.Contains(errStr, "flush") && !strings.Contains(errStr, "produce") {
		t.Errorf("error should indicate WAL/produce failure context, got: %v", err)
	}
}

// TestBuffer_WALWriteFailure_PropagatesError verifies that when WAL write fails,
// the buffer correctly propagates the error to all pending requests.
func TestBuffer_WALWriteFailure_PropagatesError(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newFailingObjectStore(true)
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-buffer-error"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create buffer with the failing committer
	buffer := NewBuffer(BufferConfig{
		MaxBufferBytes: 10000,
		FlushSizeBytes: 50, // Low threshold to trigger immediate flush
		LingerMs:       0,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	// Add a request (should trigger flush due to low threshold)
	req, err := buffer.Add(ctx, streamID, []wal.BatchEntry{{Data: make([]byte, 100)}}, 10, 1000, 2000)
	if err != nil {
		t.Fatalf("failed to add to buffer: %v", err)
	}

	// Wait for the request to complete (with timeout)
	select {
	case <-req.Done:
		// Request completed
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for request completion")
	}

	// Verify error was propagated to the request
	if req.Err == nil {
		t.Error("expected error to be propagated to request when WAL write fails")
	}

	// Verify result was not set
	if req.Result != nil {
		t.Error("expected Result to be nil when WAL write fails")
	}
}

// TestBuffer_WALWriteFailure_DiscardsRequests verifies that the buffer discards
// requests after a flush failure (does not retry automatically).
func TestBuffer_WALWriteFailure_DiscardsRequests(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newFailingObjectStore(true)
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-buffer-discard"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create buffer with the failing committer
	buffer := NewBuffer(BufferConfig{
		MaxBufferBytes: 10000,
		FlushSizeBytes: 50, // Low threshold to trigger immediate flush
		LingerMs:       0,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	// Add a request (should trigger flush due to low threshold)
	req, err := buffer.Add(ctx, streamID, []wal.BatchEntry{{Data: make([]byte, 100)}}, 10, 1000, 2000)
	if err != nil {
		t.Fatalf("failed to add to buffer: %v", err)
	}

	// Wait for the request to complete
	select {
	case <-req.Done:
		// Request completed
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for request completion")
	}

	// Verify error was propagated
	if req.Err == nil {
		t.Fatal("expected error to be propagated")
	}

	// Allow a brief moment for buffer cleanup
	time.Sleep(50 * time.Millisecond)

	// Verify the buffer is now empty (requests were discarded, not retained for retry)
	stats := buffer.Stats()
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending requests after failed flush, got %d", stats.PendingCount)
	}
	if stats.TotalBytes != 0 {
		t.Errorf("expected 0 total bytes after failed flush, got %d", stats.TotalBytes)
	}
}

// TestBuffer_WALWriteFailure_SubsequentRequestsSucceed verifies that after a
// WAL write failure, subsequent requests can succeed if the failure is transient.
func TestBuffer_WALWriteFailure_SubsequentRequestsSucceed(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newFailingObjectStore(true)
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create a stream
	streamMgr := index.NewStreamManager(metaStore)
	streamID := "test-stream-subsequent-success"

	err := streamMgr.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create buffer
	buffer := NewBuffer(BufferConfig{
		MaxBufferBytes: 10000,
		FlushSizeBytes: 50,
		LingerMs:       0,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	// First request - should fail
	req1, err := buffer.Add(ctx, streamID, []wal.BatchEntry{{Data: make([]byte, 100)}}, 10, 1000, 2000)
	if err != nil {
		t.Fatalf("failed to add first request: %v", err)
	}

	select {
	case <-req1.Done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first request")
	}

	if req1.Err == nil {
		t.Fatal("expected first request to fail")
	}

	// Now fix the object store (simulate transient failure resolved)
	objStore.failPut = false

	// Second request - should succeed
	req2, err := buffer.Add(ctx, streamID, []wal.BatchEntry{{Data: make([]byte, 100)}}, 5, 2000, 3000)
	if err != nil {
		t.Fatalf("failed to add second request: %v", err)
	}

	select {
	case <-req2.Done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for second request")
	}

	if req2.Err != nil {
		t.Fatalf("expected second request to succeed, got error: %v", req2.Err)
	}

	if req2.Result == nil {
		t.Error("expected second request to have Result set")
	}

	// Verify HWM was updated only for the successful request
	hwm, _, err := streamMgr.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM: %v", err)
	}
	// Only the second request's 5 records should be committed
	if hwm != 5 {
		t.Errorf("expected HWM 5 (only second request committed), got %d", hwm)
	}
}

// TestCommitter_WALWriteFailure_MultipleStreams verifies that when WAL write
// fails, no metadata is committed for any stream in the batch.
func TestCommitter_WALWriteFailure_MultipleStreams(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newFailingObjectStore(true)
	ctx := context.Background()

	committer := NewCommitter(objStore, metaStore, CommitterConfig{NumDomains: 4})

	// Create two streams
	streamMgr := index.NewStreamManager(metaStore)
	stream1ID := "test-stream-multi-1"
	stream2ID := "test-stream-multi-2"

	err := streamMgr.CreateStreamWithID(ctx, stream1ID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream1: %v", err)
	}
	err = streamMgr.CreateStreamWithID(ctx, stream2ID, "test-topic", 1)
	if err != nil {
		t.Fatalf("failed to create stream2: %v", err)
	}

	// Record initial state
	hwm1Before, _, _ := streamMgr.GetHWM(ctx, stream1ID)
	hwm2Before, _, _ := streamMgr.GetHWM(ctx, stream2ID)

	// Create pending requests for both streams
	stream1Hash := parseStreamID(stream1ID)
	stream2Hash := parseStreamID(stream2ID)

	requests := []*PendingRequest{
		{
			StreamID:       stream1Hash,
			StreamIDStr:    stream1ID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch1")}},
			RecordCount:    10,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Size:           100,
			Done:           make(chan struct{}),
		},
		{
			StreamID:       stream2Hash,
			StreamIDStr:    stream2ID,
			Batches:        []wal.BatchEntry{{Data: []byte("batch2")}},
			RecordCount:    5,
			MinTimestampMs: 1500,
			MaxTimestampMs: 2500,
			Size:           80,
			Done:           make(chan struct{}),
		},
	}

	// Execute commit - should fail
	err = committer.Commit(ctx, 0, requests)
	if err == nil {
		t.Fatal("expected commit to fail")
	}

	// Verify no HWM changes for either stream
	hwm1After, _, _ := streamMgr.GetHWM(ctx, stream1ID)
	hwm2After, _, _ := streamMgr.GetHWM(ctx, stream2ID)

	if hwm1After != hwm1Before {
		t.Errorf("stream1 HWM should not change on WAL failure: was %d, now %d", hwm1Before, hwm1After)
	}
	if hwm2After != hwm2Before {
		t.Errorf("stream2 HWM should not change on WAL failure: was %d, now %d", hwm2Before, hwm2After)
	}

	// Verify no results set for either request
	if requests[0].Result != nil {
		t.Error("request 0 Result should be nil on WAL failure")
	}
	if requests[1].Result != nil {
		t.Error("request 1 Result should be nil on WAL failure")
	}
}
