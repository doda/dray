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
