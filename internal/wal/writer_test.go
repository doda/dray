package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/objectstore"
	"github.com/google/uuid"
)

// mockStore implements objectstore.Store for testing.
type mockStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	putErr  error
}

func newMockStore() *mockStore {
	return &mockStore{
		objects: make(map[string][]byte),
	}
}

func (s *mockStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	if s.putErr != nil {
		return s.putErr
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()
	return nil
}

func (s *mockStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return s.Put(ctx, key, reader, size, contentType)
}

func (s *mockStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *mockStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if end == -1 {
		end = int64(len(data) - 1)
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (s *mockStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
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

func (s *mockStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}

func (s *mockStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []objectstore.ObjectMeta
	for key, data := range s.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, objectstore.ObjectMeta{
				Key:  key,
				Size: int64(len(data)),
			})
		}
	}
	return result, nil
}

func (s *mockStore) Close() error {
	return nil
}

func TestWriterAddChunk(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	chunk := Chunk{
		StreamID:       1,
		RecordCount:    10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		Batches: []BatchEntry{
			{Data: []byte("batch1")},
		},
	}

	err := w.AddChunk(chunk, 0)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	if w.ChunkCount() != 1 {
		t.Errorf("ChunkCount = %d, want 1", w.ChunkCount())
	}

	if w.MetaDomain() == nil || *w.MetaDomain() != 0 {
		t.Errorf("MetaDomain = %v, want 0", w.MetaDomain())
	}
}

func TestWriterMetaDomainMismatch(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	chunk1 := Chunk{
		StreamID:    1,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("data1")}},
	}
	chunk2 := Chunk{
		StreamID:    2,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("data2")}},
	}

	err := w.AddChunk(chunk1, 0)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	err = w.AddChunk(chunk2, 1)
	if !errors.Is(err, ErrMetaDomainMismatch) {
		t.Errorf("expected ErrMetaDomainMismatch, got %v", err)
	}

	// Same MetaDomain should work
	err = w.AddChunk(chunk2, 0)
	if err != nil {
		t.Errorf("AddChunk with same MetaDomain failed: %v", err)
	}
}

func TestWriterFlush(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	chunk1 := Chunk{
		StreamID:       100,
		RecordCount:    5,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		Batches: []BatchEntry{
			{Data: []byte("batch1")},
		},
	}
	chunk2 := Chunk{
		StreamID:       50, // Lower streamId to test sorting
		RecordCount:    10,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2500,
		Batches: []BatchEntry{
			{Data: []byte("batch2a")},
			{Data: []byte("batch2b")},
		},
	}

	w.AddChunk(chunk1, 5)
	w.AddChunk(chunk2, 5)

	ctx := context.Background()
	result, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if result.WalID == uuid.Nil {
		t.Error("WalID should not be nil")
	}
	if result.MetaDomain != 5 {
		t.Errorf("MetaDomain = %d, want 5", result.MetaDomain)
	}
	if result.Size <= 0 {
		t.Error("Size should be positive")
	}
	if len(result.ChunkOffsets) != 2 {
		t.Errorf("ChunkOffsets len = %d, want 2", len(result.ChunkOffsets))
	}
	if result.CreatedAtUnixMs <= 0 {
		t.Error("CreatedAtUnixMs should be positive")
	}

	// Verify object was written to store
	if len(store.objects) != 1 {
		t.Errorf("Expected 1 object in store, got %d", len(store.objects))
	}

	data := store.objects[result.Path]
	if len(data) != int(result.Size) {
		t.Errorf("Stored size = %d, result.Size = %d", len(data), result.Size)
	}

	// Verify the WAL can be decoded
	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to decode written WAL: %v", err)
	}

	if decoded.MetaDomain != 5 {
		t.Errorf("Decoded MetaDomain = %d, want 5", decoded.MetaDomain)
	}
	if len(decoded.Chunks) != 2 {
		t.Fatalf("Decoded Chunks len = %d, want 2", len(decoded.Chunks))
	}

	// Verify chunks are sorted by StreamID
	if decoded.Chunks[0].StreamID != 50 || decoded.Chunks[1].StreamID != 100 {
		t.Errorf("Chunks not sorted: got StreamIDs %d, %d; want 50, 100",
			decoded.Chunks[0].StreamID, decoded.Chunks[1].StreamID)
	}

	// Verify writer is reset after flush
	if w.ChunkCount() != 0 {
		t.Error("ChunkCount should be 0 after flush")
	}
	if w.MetaDomain() != nil {
		t.Error("MetaDomain should be nil after flush")
	}
}

func TestWriterFlushEmpty(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	ctx := context.Background()
	_, err := w.Flush(ctx)
	if !errors.Is(err, ErrEmptyWAL) {
		t.Errorf("expected ErrEmptyWAL, got %v", err)
	}
}

func TestWriterFlushError(t *testing.T) {
	store := newMockStore()
	store.putErr = errors.New("storage unavailable")

	w := NewWriter(store, nil)
	defer w.Close()

	chunk := Chunk{
		StreamID:    1,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("data")}},
	}
	w.AddChunk(chunk, 0)

	ctx := context.Background()
	_, err := w.Flush(ctx)
	if err == nil {
		t.Error("expected error from Flush")
	}
}

func TestWriterFlushDuplicateStreamID(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	chunk1 := Chunk{
		StreamID:    7,
		RecordCount: 3,
		Batches:     []BatchEntry{{Data: []byte("first")}},
	}
	chunk2 := Chunk{
		StreamID:    7,
		RecordCount: 2,
		Batches:     []BatchEntry{{Data: []byte("second")}},
	}

	w.AddChunk(chunk1, 0)
	w.AddChunk(chunk2, 0)

	_, err := w.Flush(context.Background())
	if !errors.Is(err, ErrDuplicateStreamID) {
		t.Fatalf("expected ErrDuplicateStreamID, got %v", err)
	}
	if len(store.objects) != 0 {
		t.Errorf("expected no objects written, got %d", len(store.objects))
	}
}

func TestWriterClosed(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)

	chunk := Chunk{
		StreamID:    1,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("data")}},
	}

	w.Close()

	err := w.AddChunk(chunk, 0)
	if !errors.Is(err, ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed from AddChunk, got %v", err)
	}

	_, err = w.Flush(context.Background())
	if !errors.Is(err, ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed from Flush, got %v", err)
	}
}

func TestWriterReset(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	chunk := Chunk{
		StreamID:    1,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("data")}},
	}
	w.AddChunk(chunk, 0)

	if w.ChunkCount() != 1 {
		t.Fatal("expected 1 chunk before reset")
	}

	w.Reset()

	if w.ChunkCount() != 0 {
		t.Error("ChunkCount should be 0 after reset")
	}
	if w.MetaDomain() != nil {
		t.Error("MetaDomain should be nil after reset")
	}

	// Should be able to add chunks with different MetaDomain after reset
	err := w.AddChunk(chunk, 99)
	if err != nil {
		t.Errorf("AddChunk after reset failed: %v", err)
	}
	if *w.MetaDomain() != 99 {
		t.Errorf("MetaDomain after reset = %d, want 99", *w.MetaDomain())
	}
}

func TestWriterCustomPathFormatter(t *testing.T) {
	store := newMockStore()

	customFormatter := &DefaultPathFormatter{Prefix: "custom/prefix"}
	w := NewWriter(store, &WriterConfig{PathFormatter: customFormatter, ZoneID: "test-zone"})
	defer w.Close()

	chunk := Chunk{
		StreamID:    1,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("data")}},
	}
	w.AddChunk(chunk, 42)

	result, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	createdAt := time.UnixMilli(result.CreatedAtUnixMs).UTC()
	expectedPrefix := fmt.Sprintf(
		"custom/prefix/wal/v1/zone=test-zone/domain=42/date=%04d/%02d/%02d/",
		createdAt.Year(),
		createdAt.Month(),
		createdAt.Day(),
	)
	if len(result.Path) < len(expectedPrefix) || result.Path[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("Path = %s, expected prefix %s", result.Path, expectedPrefix)
	}
}

func TestWriterMultipleFlushes(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	ctx := context.Background()

	// First flush with MetaDomain 0
	chunk1 := Chunk{
		StreamID:    1,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("first")}},
	}
	w.AddChunk(chunk1, 0)
	result1, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("First Flush failed: %v", err)
	}

	// Second flush with different MetaDomain (should work after reset)
	chunk2 := Chunk{
		StreamID:    2,
		RecordCount: 10,
		Batches:     []BatchEntry{{Data: []byte("second")}},
	}
	w.AddChunk(chunk2, 7)
	result2, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Second Flush failed: %v", err)
	}

	if result1.WalID == result2.WalID {
		t.Error("WalIDs should be different")
	}
	if result1.MetaDomain != 0 || result2.MetaDomain != 7 {
		t.Errorf("MetaDomains = %d, %d; want 0, 7", result1.MetaDomain, result2.MetaDomain)
	}
	if len(store.objects) != 2 {
		t.Errorf("Expected 2 objects in store, got %d", len(store.objects))
	}
}

func TestWriterChunkOffsetsPreserved(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	chunks := []Chunk{
		{
			StreamID:       300,
			RecordCount:    15,
			MinTimestampMs: 3000,
			MaxTimestampMs: 3500,
			Batches: []BatchEntry{
				{Data: []byte("batch3a")},
				{Data: []byte("batch3b")},
				{Data: []byte("batch3c")},
			},
		},
		{
			StreamID:       100,
			RecordCount:    5,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1500,
			Batches:        []BatchEntry{{Data: []byte("batch1")}},
		},
		{
			StreamID:       200,
			RecordCount:    10,
			MinTimestampMs: 2000,
			MaxTimestampMs: 2500,
			Batches: []BatchEntry{
				{Data: []byte("batch2a")},
				{Data: []byte("batch2b")},
			},
		},
	}

	for _, chunk := range chunks {
		w.AddChunk(chunk, 1)
	}

	result, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// ChunkOffsets should be in the order added (not sorted)
	if len(result.ChunkOffsets) != 3 {
		t.Fatalf("ChunkOffsets len = %d, want 3", len(result.ChunkOffsets))
	}

	for i, chunk := range chunks {
		offset := result.ChunkOffsets[i]
		if offset.StreamID != chunk.StreamID {
			t.Errorf("ChunkOffsets[%d].StreamID = %d, want %d", i, offset.StreamID, chunk.StreamID)
		}
		if offset.RecordCount != chunk.RecordCount {
			t.Errorf("ChunkOffsets[%d].RecordCount = %d, want %d", i, offset.RecordCount, chunk.RecordCount)
		}
		if offset.BatchCount != uint32(len(chunk.Batches)) {
			t.Errorf("ChunkOffsets[%d].BatchCount = %d, want %d", i, offset.BatchCount, len(chunk.Batches))
		}
		if offset.MinTimestampMs != chunk.MinTimestampMs {
			t.Errorf("ChunkOffsets[%d].MinTimestampMs = %d, want %d", i, offset.MinTimestampMs, chunk.MinTimestampMs)
		}
		if offset.MaxTimestampMs != chunk.MaxTimestampMs {
			t.Errorf("ChunkOffsets[%d].MaxTimestampMs = %d, want %d", i, offset.MaxTimestampMs, chunk.MaxTimestampMs)
		}
	}

	chunk100Len := uint32(4 + len("batch1"))
	chunk200Len := uint32(4 + len("batch2a") + 4 + len("batch2b"))
	chunk300Len := uint32(4 + len("batch3a") + 4 + len("batch3b") + 4 + len("batch3c"))
	offset100 := uint64(HeaderSize)
	offset200 := offset100 + uint64(chunk100Len)
	offset300 := offset200 + uint64(chunk200Len)

	expected := map[uint64]struct {
		offset uint64
		length uint32
	}{
		100: {offset: offset100, length: chunk100Len},
		200: {offset: offset200, length: chunk200Len},
		300: {offset: offset300, length: chunk300Len},
	}

	for i, chunk := range chunks {
		offset := result.ChunkOffsets[i]
		want, ok := expected[chunk.StreamID]
		if !ok {
			t.Fatalf("missing expected offsets for stream %d", chunk.StreamID)
		}
		if offset.ByteOffset != want.offset {
			t.Errorf("ChunkOffsets[%d].ByteOffset = %d, want %d", i, offset.ByteOffset, want.offset)
		}
		if offset.ByteLength != want.length {
			t.Errorf("ChunkOffsets[%d].ByteLength = %d, want %d", i, offset.ByteLength, want.length)
		}
	}
}

func TestWriterSortsByStreamId(t *testing.T) {
	store := newMockStore()
	w := NewWriter(store, nil)
	defer w.Close()

	// Add chunks in reverse order by StreamID
	streamIDs := []uint64{500, 100, 300, 200, 400}
	for _, id := range streamIDs {
		chunk := Chunk{
			StreamID:    id,
			RecordCount: 1,
			Batches:     []BatchEntry{{Data: []byte{byte(id)}}},
		}
		w.AddChunk(chunk, 0)
	}

	result, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Decode and verify chunks are sorted
	data := store.objects[result.Path]
	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	// Verify chunks are sorted by StreamID in the encoded WAL
	sortedIDs := make([]uint64, len(streamIDs))
	copy(sortedIDs, streamIDs)
	sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })

	for i, chunk := range decoded.Chunks {
		if chunk.StreamID != sortedIDs[i] {
			t.Errorf("Decoded chunk %d has StreamID %d, want %d", i, chunk.StreamID, sortedIDs[i])
		}
	}
}

func TestDefaultPathFormatter(t *testing.T) {
	createdAt := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name       string
		prefix     string
		metaDomain uint32
		zoneID     string
		walID      uuid.UUID
		want       string
	}{
		{
			name:       "no prefix",
			prefix:     "",
			metaDomain: 0,
			zoneID:     "zone-a",
			walID:      uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
			want:       "wal/v1/zone=zone-a/domain=0/date=2025/01/02/12345678-1234-1234-1234-123456789abc.wo",
		},
		{
			name:       "with prefix",
			prefix:     "dray/v1",
			metaDomain: 42,
			zoneID:     "zone-b",
			walID:      uuid.MustParse("abcdef00-1234-5678-90ab-cdef12345678"),
			want:       "dray/v1/wal/v1/zone=zone-b/domain=42/date=2025/01/02/abcdef00-1234-5678-90ab-cdef12345678.wo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &DefaultPathFormatter{Prefix: tt.prefix}
			got := f.FormatPath(tt.metaDomain, tt.walID, createdAt, tt.zoneID)
			if got != tt.want {
				t.Errorf("FormatPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestMetaDomainIsolation verifies WAL writer enforces MetaDomain isolation:
// - Different domains never mix in same WAL
// - Attempts to add streams from different MetaDomains are rejected
// - Single-domain WAL writes succeed
// multipartMockStore extends mockStore with multipart upload support for testing StreamingWriter.
type multipartMockStore struct {
	*mockStore
	uploads          map[string]*multipartUploadMock
	uploadIDCounter  int
	MultipartTracker *objectstore.MultipartTracker
	mu               sync.Mutex
}

type multipartUploadMock struct {
	store       *multipartMockStore
	uploadID    string
	key         string
	contentType string
	parts       map[int][]byte
	mu          sync.Mutex
	completed   bool
	aborted     bool
}

func newMultipartMockStore() *multipartMockStore {
	return &multipartMockStore{
		mockStore:        newMockStore(),
		uploads:          make(map[string]*multipartUploadMock),
		MultipartTracker: &objectstore.MultipartTracker{},
	}
}

func (s *multipartMockStore) CreateMultipartUpload(ctx context.Context, key string, contentType string) (objectstore.MultipartUpload, error) {
	return s.CreateMultipartUploadWithOptions(ctx, key, contentType, objectstore.PutOptions{})
}

func (s *multipartMockStore) CreateMultipartUploadWithOptions(ctx context.Context, key string, contentType string, opts objectstore.PutOptions) (objectstore.MultipartUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.uploadIDCounter++
	uploadID := fmt.Sprintf("upload-%d", s.uploadIDCounter)
	upload := &multipartUploadMock{
		store:       s,
		uploadID:    uploadID,
		key:         key,
		contentType: contentType,
		parts:       make(map[int][]byte),
	}
	s.uploads[uploadID] = upload
	return upload, nil
}

func (m *multipartUploadMock) UploadID() string {
	return m.uploadID
}

func (m *multipartUploadMock) UploadPart(ctx context.Context, partNum int, reader io.Reader, size int64) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.completed || m.aborted {
		return "", errors.New("upload already finished")
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	m.parts[partNum] = data

	if m.store.MultipartTracker != nil {
		m.store.MultipartTracker.RecordPart(int64(len(data)))
	}

	return fmt.Sprintf("etag-part-%d", partNum), nil
}

func (m *multipartUploadMock) Complete(ctx context.Context, etags []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.completed || m.aborted {
		return errors.New("upload already finished")
	}

	// Assemble parts
	var totalData []byte
	for i := 1; i <= len(m.parts); i++ {
		part, ok := m.parts[i]
		if !ok {
			return fmt.Errorf("missing part %d", i)
		}
		totalData = append(totalData, part...)
	}

	m.store.mockStore.mu.Lock()
	m.store.mockStore.objects[m.key] = totalData
	m.store.mockStore.mu.Unlock()

	m.store.mu.Lock()
	delete(m.store.uploads, m.uploadID)
	m.store.mu.Unlock()

	m.completed = true

	if m.store.MultipartTracker != nil {
		m.store.MultipartTracker.RecordUpload(m.key)
	}

	return nil
}

func (m *multipartUploadMock) Abort(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.completed && !m.aborted {
		m.store.mu.Lock()
		delete(m.store.uploads, m.uploadID)
		m.store.mu.Unlock()
	}

	m.aborted = true
	return nil
}

func TestStreamingWriterSmallWAL(t *testing.T) {
	store := newMultipartMockStore()
	w := NewStreamingWriter(store, &StreamingWriterConfig{
		MultipartThreshold: 1024 * 1024, // 1MB threshold
		PartSize:           512 * 1024,  // 512KB parts
	})
	defer w.Close()

	// Add a small chunk
	chunk := Chunk{
		StreamID:       1,
		RecordCount:    10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		Batches:        []BatchEntry{{Data: []byte("small batch data")}},
	}

	err := w.AddChunk(chunk, 0)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	ctx := context.Background()
	result, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify multipart was NOT used (small WAL)
	if store.MultipartTracker.UploadCount() != 0 {
		t.Errorf("Expected no multipart uploads for small WAL, got %d", store.MultipartTracker.UploadCount())
	}

	// Verify object was written via regular Put
	if len(store.mockStore.objects) != 1 {
		t.Errorf("Expected 1 object in store, got %d", len(store.mockStore.objects))
	}

	// Verify WAL can be decoded
	data := store.mockStore.objects[result.Path]
	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to decode WAL: %v", err)
	}
	if len(decoded.Chunks) != 1 {
		t.Errorf("Expected 1 chunk, got %d", len(decoded.Chunks))
	}
}

func TestStreamingWriterLargeWALUsesMultipart(t *testing.T) {
	store := newMultipartMockStore()
	// Use small thresholds for testing
	w := NewStreamingWriter(store, &StreamingWriterConfig{
		MultipartThreshold: 1024, // 1KB threshold
		PartSize:           512,  // 512 byte parts
	})
	defer w.Close()

	// Create a chunk large enough to exceed the threshold
	largeData := make([]byte, 2048) // 2KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	chunk := Chunk{
		StreamID:       1,
		RecordCount:    100,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		Batches:        []BatchEntry{{Data: largeData}},
	}

	err := w.AddChunk(chunk, 0)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	ctx := context.Background()
	result, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify multipart WAS used
	if store.MultipartTracker.UploadCount() != 1 {
		t.Errorf("Expected 1 multipart upload, got %d", store.MultipartTracker.UploadCount())
	}

	// With 2KB data + headers, we expect multiple parts at 512 byte part size
	if store.MultipartTracker.PartCount() < 2 {
		t.Errorf("Expected at least 2 parts for large WAL, got %d", store.MultipartTracker.PartCount())
	}

	// Verify object was written
	if len(store.mockStore.objects) != 1 {
		t.Errorf("Expected 1 object in store, got %d", len(store.mockStore.objects))
	}

	// Verify WAL can be decoded
	data := store.mockStore.objects[result.Path]
	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to decode WAL: %v", err)
	}
	if len(decoded.Chunks) != 1 {
		t.Errorf("Expected 1 chunk, got %d", len(decoded.Chunks))
	}
	if !bytes.Equal(decoded.Chunks[0].Batches[0].Data, largeData) {
		t.Error("Decoded data doesn't match original")
	}

	// Verify total bytes tracked
	if store.MultipartTracker.TotalBytes() != result.Size {
		t.Errorf("TotalBytes = %d, want %d", store.MultipartTracker.TotalBytes(), result.Size)
	}
}

func TestStreamingWriterVeryLargeWAL(t *testing.T) {
	store := newMultipartMockStore()
	// Use small thresholds for testing
	w := NewStreamingWriter(store, &StreamingWriterConfig{
		MultipartThreshold: 1024,       // 1KB threshold
		PartSize:           10 * 1024,  // 10KB parts
	})
	defer w.Close()

	// Create a 100KB chunk
	largeData := make([]byte, 100*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	chunk := Chunk{
		StreamID:       1,
		RecordCount:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		Batches:        []BatchEntry{{Data: largeData}},
	}

	err := w.AddChunk(chunk, 0)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	ctx := context.Background()
	result, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify multipart was used
	if store.MultipartTracker.UploadCount() != 1 {
		t.Errorf("Expected 1 multipart upload, got %d", store.MultipartTracker.UploadCount())
	}

	// With 100KB data at 10KB part size, expect ~11 parts (data + header/index/footer)
	expectedParts := (100*1024 + HeaderSize + ChunkIndexEntrySize + FooterSize + 10*1024 - 1) / (10 * 1024)
	if store.MultipartTracker.PartCount() < expectedParts-2 {
		t.Errorf("Expected at least %d parts, got %d", expectedParts-2, store.MultipartTracker.PartCount())
	}

	// Verify object was written and can be decoded
	data := store.mockStore.objects[result.Path]
	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to decode WAL: %v", err)
	}
	if !bytes.Equal(decoded.Chunks[0].Batches[0].Data, largeData) {
		t.Error("Decoded data doesn't match original")
	}
}

func TestStreamingWriterMultipleChunksLarge(t *testing.T) {
	store := newMultipartMockStore()
	w := NewStreamingWriter(store, &StreamingWriterConfig{
		MultipartThreshold: 1024, // 1KB threshold
		PartSize:           1024, // 1KB parts
	})
	defer w.Close()

	// Create multiple chunks
	chunks := make([]Chunk, 5)
	for i := 0; i < 5; i++ {
		data := make([]byte, 500)
		for j := range data {
			data[j] = byte(i*100 + j%256)
		}
		chunks[i] = Chunk{
			StreamID:       uint64(100 + i),
			RecordCount:    uint32(10 + i),
			MinTimestampMs: int64(1000 * (i + 1)),
			MaxTimestampMs: int64(2000 * (i + 1)),
			Batches:        []BatchEntry{{Data: data}},
		}
		err := w.AddChunk(chunks[i], 0)
		if err != nil {
			t.Fatalf("AddChunk failed: %v", err)
		}
	}

	ctx := context.Background()
	result, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify multipart was used (5 x 500 bytes > 1KB threshold)
	if store.MultipartTracker.UploadCount() != 1 {
		t.Errorf("Expected 1 multipart upload, got %d", store.MultipartTracker.UploadCount())
	}

	// Verify object was written
	data := store.mockStore.objects[result.Path]
	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to decode WAL: %v", err)
	}
	if len(decoded.Chunks) != 5 {
		t.Errorf("Expected 5 chunks, got %d", len(decoded.Chunks))
	}
}

func TestStreamingWriterNoMultipartSupport(t *testing.T) {
	// Use regular mockStore which doesn't implement MultipartStore
	store := newMockStore()
	w := NewStreamingWriter(store, &StreamingWriterConfig{
		MultipartThreshold: 100, // Very low threshold
		PartSize:           50,
	})
	defer w.Close()

	// Create a large chunk that would trigger multipart if available
	largeData := make([]byte, 500)
	for i := range largeData {
		largeData[i] = byte(i)
	}

	chunk := Chunk{
		StreamID:       1,
		RecordCount:    10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		Batches:        []BatchEntry{{Data: largeData}},
	}

	err := w.AddChunk(chunk, 0)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	ctx := context.Background()
	result, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify object was written using regular Put
	if len(store.objects) != 1 {
		t.Errorf("Expected 1 object in store, got %d", len(store.objects))
	}

	// Verify WAL can be decoded
	data := store.objects[result.Path]
	decoded, err := DecodeFromBytes(data)
	if err != nil {
		t.Fatalf("Failed to decode WAL: %v", err)
	}
	if !bytes.Equal(decoded.Chunks[0].Batches[0].Data, largeData) {
		t.Error("Decoded data doesn't match original")
	}
}

func TestStreamingWriterChunkOffsetsCorrect(t *testing.T) {
	store := newMultipartMockStore()
	w := NewStreamingWriter(store, &StreamingWriterConfig{
		MultipartThreshold: 100, // Low threshold to trigger multipart
		PartSize:           100,
	})
	defer w.Close()

	chunks := []Chunk{
		{
			StreamID:       300,
			RecordCount:    15,
			MinTimestampMs: 3000,
			MaxTimestampMs: 3500,
			Batches:        []BatchEntry{{Data: make([]byte, 50)}},
		},
		{
			StreamID:       100,
			RecordCount:    5,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1500,
			Batches:        []BatchEntry{{Data: make([]byte, 30)}},
		},
		{
			StreamID:       200,
			RecordCount:    10,
			MinTimestampMs: 2000,
			MaxTimestampMs: 2500,
			Batches:        []BatchEntry{{Data: make([]byte, 40)}},
		},
	}

	for _, chunk := range chunks {
		w.AddChunk(chunk, 1)
	}

	result, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify chunk offsets are returned in insertion order
	if len(result.ChunkOffsets) != 3 {
		t.Fatalf("Expected 3 chunk offsets, got %d", len(result.ChunkOffsets))
	}

	for i, chunk := range chunks {
		offset := result.ChunkOffsets[i]
		if offset.StreamID != chunk.StreamID {
			t.Errorf("ChunkOffsets[%d].StreamID = %d, want %d", i, offset.StreamID, chunk.StreamID)
		}
		if offset.RecordCount != chunk.RecordCount {
			t.Errorf("ChunkOffsets[%d].RecordCount = %d, want %d", i, offset.RecordCount, chunk.RecordCount)
		}
	}
}

func TestMetaDomainIsolation(t *testing.T) {
	t.Run("reject_mixed_domains_at_second_chunk", func(t *testing.T) {
		store := newMockStore()
		w := NewWriter(store, nil)
		defer w.Close()

		chunk1 := Chunk{
			StreamID:       100,
			RecordCount:    10,
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			Batches:        []BatchEntry{{Data: []byte("domain-0-data")}},
		}
		chunk2 := Chunk{
			StreamID:       200,
			RecordCount:    5,
			MinTimestampMs: 3000,
			MaxTimestampMs: 4000,
			Batches:        []BatchEntry{{Data: []byte("domain-1-data")}},
		}

		// First chunk establishes MetaDomain 0
		err := w.AddChunk(chunk1, 0)
		if err != nil {
			t.Fatalf("First AddChunk failed: %v", err)
		}

		// Second chunk with different MetaDomain must be rejected
		err = w.AddChunk(chunk2, 1)
		if !errors.Is(err, ErrMetaDomainMismatch) {
			t.Errorf("Expected ErrMetaDomainMismatch, got: %v", err)
		}

		// Writer should still have only 1 chunk (the rejected one was not added)
		if w.ChunkCount() != 1 {
			t.Errorf("ChunkCount = %d, want 1 (rejected chunk should not be added)", w.ChunkCount())
		}
	})

	t.Run("reject_mixed_domains_at_third_chunk", func(t *testing.T) {
		store := newMockStore()
		w := NewWriter(store, nil)
		defer w.Close()

		// Add two chunks with same MetaDomain
		for i := 0; i < 2; i++ {
			chunk := Chunk{
				StreamID:       uint64(100 + i),
				RecordCount:    uint32(5 + i),
				MinTimestampMs: int64(1000 * (i + 1)),
				MaxTimestampMs: int64(2000 * (i + 1)),
				Batches:        []BatchEntry{{Data: []byte("same-domain")}},
			}
			err := w.AddChunk(chunk, 5)
			if err != nil {
				t.Fatalf("AddChunk %d failed: %v", i, err)
			}
		}

		// Third chunk with different MetaDomain must be rejected
		chunk3 := Chunk{
			StreamID:    300,
			RecordCount: 15,
			Batches:     []BatchEntry{{Data: []byte("different-domain")}},
		}
		err := w.AddChunk(chunk3, 99)
		if !errors.Is(err, ErrMetaDomainMismatch) {
			t.Errorf("Expected ErrMetaDomainMismatch, got: %v", err)
		}

		// Should still have 2 chunks
		if w.ChunkCount() != 2 {
			t.Errorf("ChunkCount = %d, want 2", w.ChunkCount())
		}
	})

	t.Run("single_domain_write_succeeds", func(t *testing.T) {
		store := newMockStore()
		w := NewWriter(store, nil)
		defer w.Close()

		// Add multiple chunks all from MetaDomain 42
		chunks := []Chunk{
			{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("chunk1")}}},
			{StreamID: 2, RecordCount: 10, Batches: []BatchEntry{{Data: []byte("chunk2")}}},
			{StreamID: 3, RecordCount: 15, Batches: []BatchEntry{{Data: []byte("chunk3")}}},
		}

		for i, chunk := range chunks {
			err := w.AddChunk(chunk, 42)
			if err != nil {
				t.Fatalf("AddChunk %d failed: %v", i, err)
			}
		}

		// Flush should succeed
		result, err := w.Flush(context.Background())
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		// Verify WAL was written with correct MetaDomain
		if result.MetaDomain != 42 {
			t.Errorf("MetaDomain = %d, want 42", result.MetaDomain)
		}
		if len(result.ChunkOffsets) != 3 {
			t.Errorf("ChunkOffsets len = %d, want 3", len(result.ChunkOffsets))
		}

		// Verify WAL was written to storage
		if len(store.objects) != 1 {
			t.Errorf("Expected 1 object in store, got %d", len(store.objects))
		}

		// Verify the written WAL can be decoded and has correct domain
		data := store.objects[result.Path]
		decoded, err := DecodeFromBytes(data)
		if err != nil {
			t.Fatalf("Failed to decode WAL: %v", err)
		}
		if decoded.MetaDomain != 42 {
			t.Errorf("Decoded MetaDomain = %d, want 42", decoded.MetaDomain)
		}
		if len(decoded.Chunks) != 3 {
			t.Errorf("Decoded chunks = %d, want 3", len(decoded.Chunks))
		}
	})

	t.Run("multiple_domains_different_writers", func(t *testing.T) {
		store := newMockStore()

		// Each writer handles a different MetaDomain
		writers := make([]*Writer, 3)
		for i := range writers {
			writers[i] = NewWriter(store, nil)
			defer writers[i].Close()
		}

		// Add chunks with different domains to different writers
		for i, w := range writers {
			domain := uint32(i * 10) // Domains: 0, 10, 20
			chunk := Chunk{
				StreamID:    uint64(100 + i),
				RecordCount: uint32(5 + i),
				Batches:     []BatchEntry{{Data: []byte("writer-specific-data")}},
			}
			err := w.AddChunk(chunk, domain)
			if err != nil {
				t.Fatalf("Writer %d AddChunk failed: %v", i, err)
			}
		}

		// Flush all writers - each should succeed with its own domain
		for i, w := range writers {
			result, err := w.Flush(context.Background())
			if err != nil {
				t.Fatalf("Writer %d Flush failed: %v", i, err)
			}
			expectedDomain := uint32(i * 10)
			if result.MetaDomain != expectedDomain {
				t.Errorf("Writer %d MetaDomain = %d, want %d", i, result.MetaDomain, expectedDomain)
			}
		}

		// Should have 3 separate WAL objects
		if len(store.objects) != 3 {
			t.Errorf("Expected 3 objects in store, got %d", len(store.objects))
		}
	})

	t.Run("same_stream_different_domains_rejected", func(t *testing.T) {
		store := newMockStore()
		w := NewWriter(store, nil)
		defer w.Close()

		// Same streamID but different MetaDomain
		chunk := Chunk{
			StreamID:    12345,
			RecordCount: 10,
			Batches:     []BatchEntry{{Data: []byte("data")}},
		}

		err := w.AddChunk(chunk, 0)
		if err != nil {
			t.Fatalf("First AddChunk failed: %v", err)
		}

		// Same streamID but different domain must be rejected
		err = w.AddChunk(chunk, 1)
		if !errors.Is(err, ErrMetaDomainMismatch) {
			t.Errorf("Expected ErrMetaDomainMismatch, got: %v", err)
		}
	})
}
