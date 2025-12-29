package fetch

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// mockStore implements objectstore.Store for testing.
type mockStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	getErr  error

	getCalls      int
	getRangeCalls int
	getRangeBytes int64
}

func newMockStore() *mockStore {
	return &mockStore{
		objects: make(map[string][]byte),
	}
}

func (s *mockStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
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
	if s.getErr != nil {
		return nil, s.getErr
	}
	s.mu.Lock()
	s.getCalls++
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *mockStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if end == -1 {
		end = int64(len(data) - 1)
	}
	if start < 0 || end >= int64(len(data)) || start > end {
		return nil, objectstore.ErrInvalidRange
	}
	s.mu.Lock()
	s.getRangeCalls++
	s.getRangeBytes += end - start + 1
	s.mu.Unlock()
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

const (
	walTestZone = "zone-a"
	walTestDate = "2025/01/02"
)

func testWALPath(walID string) string {
	return fmt.Sprintf("wal/v1/zone=%s/domain=0/date=%s/%s.wo", walTestZone, walTestDate, walID)
}

// makeMinimalBatch creates a minimal valid Kafka record batch for testing.
// Record batch format (minimum 61 bytes):
//   - baseOffset: int64 (8 bytes) - offset 0
//   - batchLength: int32 (4 bytes) - offset 8 (length from here to end)
//   - partitionLeaderEpoch: int32 (4 bytes) - offset 12
//   - magic: int8 (1 byte) - offset 16, must be 2
//   - crc: uint32 (4 bytes) - offset 17
//   - attributes: int16 (2 bytes) - offset 21
//   - lastOffsetDelta: int32 (4 bytes) - offset 23
//   - firstTimestamp: int64 (8 bytes) - offset 27
//   - maxTimestamp: int64 (8 bytes) - offset 35
//   - producerId: int64 (8 bytes) - offset 43
//   - producerEpoch: int16 (2 bytes) - offset 51
//   - baseSequence: int32 (4 bytes) - offset 53
//   - recordCount: int32 (4 bytes) - offset 57
func makeMinimalBatch(baseOffset int64, recordCount int32) []byte {
	batch := make([]byte, 61)

	// baseOffset (8 bytes)
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))

	// batchLength (4 bytes) - size of everything after this field
	binary.BigEndian.PutUint32(batch[8:12], 49) // 61 - 12 = 49

	// partitionLeaderEpoch (4 bytes)
	binary.BigEndian.PutUint32(batch[12:16], 0)

	// magic (1 byte)
	batch[16] = 2

	// crc (4 bytes) - not validated in our tests
	binary.BigEndian.PutUint32(batch[17:21], 0)

	// attributes (2 bytes)
	binary.BigEndian.PutUint16(batch[21:23], 0)

	// lastOffsetDelta (4 bytes)
	binary.BigEndian.PutUint32(batch[23:27], uint32(recordCount-1))

	// firstTimestamp (8 bytes)
	binary.BigEndian.PutUint64(batch[27:35], 1000)

	// maxTimestamp (8 bytes)
	binary.BigEndian.PutUint64(batch[35:43], 2000)

	// producerId (8 bytes)
	binary.BigEndian.PutUint64(batch[43:51], 0xFFFFFFFFFFFFFFFF) // -1

	// producerEpoch (2 bytes)
	binary.BigEndian.PutUint16(batch[51:53], 0xFFFF) // -1

	// baseSequence (4 bytes)
	binary.BigEndian.PutUint32(batch[53:57], 0xFFFFFFFF) // -1

	// recordCount (4 bytes)
	binary.BigEndian.PutUint32(batch[57:61], uint32(recordCount))

	return batch
}

// makeChunkData creates WAL chunk data with multiple batches.
// Format: [batchLen (4 bytes), batchData (batchLen bytes)] repeated
func makeChunkData(batches [][]byte) []byte {
	var result []byte
	for _, batch := range batches {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(batch)))
		result = append(result, lenBuf...)
		result = append(result, batch...)
	}
	return result
}

func TestWALReader_ReadBatches_SingleBatch(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create a single batch with 5 records
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch})

	// Store the chunk data
	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 100,
		EndOffset:   105,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()
	result, err := reader.ReadBatches(ctx, entry, 100, 0)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	if len(result.Batches) != 1 {
		t.Errorf("expected 1 batch, got %d", len(result.Batches))
	}
	if result.StartOffset != 100 {
		t.Errorf("expected StartOffset 100, got %d", result.StartOffset)
	}
	if result.EndOffset != 105 {
		t.Errorf("expected EndOffset 105, got %d", result.EndOffset)
	}
	if result.TotalBytes != int64(len(batch)) {
		t.Errorf("expected TotalBytes %d, got %d", len(batch), result.TotalBytes)
	}
}

func TestWALReader_ReadBatches_MultipleBatches(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create 3 batches: 3 records, 5 records, 2 records = 10 total
	batch1 := makeMinimalBatch(0, 3)
	batch2 := makeMinimalBatch(0, 5)
	batch3 := makeMinimalBatch(0, 2)
	chunkData := makeChunkData([][]byte{batch1, batch2, batch3})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   10,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()
	result, err := reader.ReadBatches(ctx, entry, 0, 0)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	if len(result.Batches) != 3 {
		t.Errorf("expected 3 batches, got %d", len(result.Batches))
	}
	if result.StartOffset != 0 {
		t.Errorf("expected StartOffset 0, got %d", result.StartOffset)
	}
	if result.EndOffset != 10 {
		t.Errorf("expected EndOffset 10, got %d", result.EndOffset)
	}
}

func TestWALReader_ReadBatches_OffsetFiltering(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create 3 batches with 3, 5, 2 records = offsets [0,3), [3,8), [8,10)
	batch1 := makeMinimalBatch(0, 3)
	batch2 := makeMinimalBatch(0, 5)
	batch3 := makeMinimalBatch(0, 2)
	chunkData := makeChunkData([][]byte{batch1, batch2, batch3})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   10,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	tests := []struct {
		name             string
		fetchOffset      int64
		expectedBatches  int
		expectedStartOff int64
	}{
		{"fetch from 0", 0, 3, 0},
		{"fetch from 3 (batch 2 start)", 3, 2, 3},
		{"fetch from 5 (middle of batch 2)", 5, 2, 3},
		{"fetch from 8 (batch 3 start)", 8, 1, 8},
		{"fetch from 9 (middle of batch 3)", 9, 1, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			result, err := reader.ReadBatches(ctx, entry, tt.fetchOffset, 0)
			if err != nil {
				t.Fatalf("ReadBatches failed: %v", err)
			}

			if len(result.Batches) != tt.expectedBatches {
				t.Errorf("expected %d batches, got %d", tt.expectedBatches, len(result.Batches))
			}
			if result.StartOffset != tt.expectedStartOff {
				t.Errorf("expected StartOffset %d, got %d", tt.expectedStartOff, result.StartOffset)
			}
		})
	}
}

func TestWALReader_ReadBatches_MaxBytesLimit(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create 3 identical batches
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch, batch, batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   15, // 3 batches * 5 records each
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()

	// Limit to 1 batch worth of bytes
	maxBytes := int64(len(batch) + 1)
	result, err := reader.ReadBatches(ctx, entry, 0, maxBytes)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	if len(result.Batches) != 1 {
		t.Errorf("expected 1 batch due to maxBytes limit, got %d", len(result.Batches))
	}

	// With no limit, should get all 3
	result, err = reader.ReadBatches(ctx, entry, 0, 0)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	if len(result.Batches) != 3 {
		t.Errorf("expected 3 batches with no limit, got %d", len(result.Batches))
	}
}

func TestWALReader_ReadBatches_WithBatchIndex(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create 3 batches with specific record counts
	batch1 := makeMinimalBatch(0, 3)
	batch2 := makeMinimalBatch(0, 5)
	batch3 := makeMinimalBatch(0, 2)
	chunkData := makeChunkData([][]byte{batch1, batch2, batch3})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	// Calculate batch positions in chunk
	batch1Len := uint32(len(batch1))
	batch2Start := 4 + batch1Len + 4 // skip length + batch1 + next length
	batch2Len := uint32(len(batch2))
	batch3Start := batch2Start + batch2Len + 4
	batch3Len := uint32(len(batch3))

	// Build batch index
	batchIndex := []index.BatchIndexEntry{
		{
			BatchStartOffsetDelta: 0,
			BatchLastOffsetDelta:  2, // records 0,1,2
			BatchOffsetInChunk:    4, // skip the 4-byte length prefix
			BatchLength:           batch1Len,
			MinTimestampMs:        1000,
			MaxTimestampMs:        2000,
		},
		{
			BatchStartOffsetDelta: 3,
			BatchLastOffsetDelta:  7, // records 3,4,5,6,7
			BatchOffsetInChunk:    batch2Start,
			BatchLength:           batch2Len,
			MinTimestampMs:        2000,
			MaxTimestampMs:        3000,
		},
		{
			BatchStartOffsetDelta: 8,
			BatchLastOffsetDelta:  9, // records 8,9
			BatchOffsetInChunk:    batch3Start,
			BatchLength:           batch3Len,
			MinTimestampMs:        3000,
			MaxTimestampMs:        4000,
		},
	}

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 100,
		EndOffset:   110,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
		BatchIndex:  batchIndex,
	}

	tests := []struct {
		name             string
		fetchOffset      int64
		expectedBatches  int
		expectedStartOff int64
	}{
		{"fetch from 100 (start)", 100, 3, 100},
		{"fetch from 103 (batch 2)", 103, 2, 103},
		{"fetch from 107 (end of batch 2)", 107, 2, 103},
		{"fetch from 108 (batch 3)", 108, 1, 108},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			result, err := reader.ReadBatches(ctx, entry, tt.fetchOffset, 0)
			if err != nil {
				t.Fatalf("ReadBatches failed: %v", err)
			}

			if len(result.Batches) != tt.expectedBatches {
				t.Errorf("expected %d batches, got %d", tt.expectedBatches, len(result.Batches))
			}
			if result.StartOffset != tt.expectedStartOff {
				t.Errorf("expected StartOffset %d, got %d", tt.expectedStartOff, result.StartOffset)
			}
		})
	}
}

func TestWALReader_ReadBatches_LargeBatchIndex(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	const batchCount = 1000
	batches := make([][]byte, batchCount)
	batchIndex := make([]index.BatchIndexEntry, 0, batchCount)
	var offsetDelta uint32
	var chunkOffset uint32

	for i := 0; i < batchCount; i++ {
		batch := makeMinimalBatch(0, 1)
		batches[i] = batch

		batchIndex = append(batchIndex, index.BatchIndexEntry{
			BatchStartOffsetDelta: offsetDelta,
			BatchLastOffsetDelta:  offsetDelta,
			BatchOffsetInChunk:    chunkOffset + 4,
			BatchLength:           uint32(len(batch)),
			MinTimestampMs:        1000,
			MaxTimestampMs:        2000,
		})

		offsetDelta++
		chunkOffset += 4 + uint32(len(batch))
	}

	chunkData := makeChunkData(batches)
	walPath := testWALPath("large")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 100,
		EndOffset:   1100,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
		BatchIndex:  batchIndex,
	}

	fetchOffset := int64(800)
	result, err := reader.ReadBatches(context.Background(), entry, fetchOffset, 0)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	expectedStart := fetchOffset
	if result.StartOffset != expectedStart {
		t.Errorf("expected StartOffset %d, got %d", expectedStart, result.StartOffset)
	}

	expectedBatches := batchCount - int(fetchOffset-entry.StartOffset)
	if len(result.Batches) != expectedBatches {
		t.Errorf("expected %d batches, got %d", expectedBatches, len(result.Batches))
	}
}

func TestWALReader_ReadBatches_ChunkOffset(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create chunk data
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch})

	// Create a larger object with prefix padding
	padding := make([]byte, 100)
	for i := range padding {
		padding[i] = 0xFF
	}
	fullObject := append(padding, chunkData...)

	walPath := testWALPath("test")
	store.objects[walPath] = fullObject

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   5,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 100, // Chunk starts after 100 bytes of padding
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()
	result, err := reader.ReadBatches(ctx, entry, 0, 0)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	if len(result.Batches) != 1 {
		t.Errorf("expected 1 batch, got %d", len(result.Batches))
	}
}

func TestWALReader_ReadBatches_ObjectNotFound(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   5,
		FileType:    index.FileTypeWAL,
		WalPath:     testWALPath("nonexistent"),
		ChunkOffset: 0,
		ChunkLength: 100,
	}

	ctx := context.Background()
	_, err := reader.ReadBatches(ctx, entry, 0, 0)
	if !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("expected ErrChunkNotFound, got %v", err)
	}
}

func TestWALReader_ReadBatches_InvalidFileType(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	entry := &index.IndexEntry{
		StreamID: "stream1",
		FileType: index.FileTypeParquet, // Wrong type
	}

	ctx := context.Background()
	_, err := reader.ReadBatches(ctx, entry, 0, 0)
	if err == nil {
		t.Error("expected error for Parquet file type")
	}
}

func TestWALReader_ReadBatches_TruncatedChunk(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create chunk data but claim it's longer
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   5,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData) + 100), // Claim it's longer
	}

	ctx := context.Background()
	_, err := reader.ReadBatches(ctx, entry, 0, 0)
	// The GetRange operation will fail when the requested range extends beyond the object
	if err == nil {
		t.Error("expected error for truncated chunk")
	}
}

func TestWALReader_ReadBatches_OffsetNotInChunk(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create chunk with records for offsets 0-4
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   5,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()

	// Request offset beyond what's in the chunk
	_, err := reader.ReadBatches(ctx, entry, 100, 0)
	if !errors.Is(err, ErrOffsetNotInChunk) {
		t.Errorf("expected ErrOffsetNotInChunk, got %v", err)
	}
}

func TestWALReader_ReadBatches_RawBytesForPatching(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create batch with known content
	batch := makeMinimalBatch(0, 5)
	originalBaseOffset := GetBaseOffset(batch)
	chunkData := makeChunkData([][]byte{batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 100, // Will be patched to this
		EndOffset:   105,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()
	result, err := reader.ReadBatches(ctx, entry, 100, 0)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	// Verify the returned batch is a copy and can be patched
	if len(result.Batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result.Batches))
	}

	returnedBatch := result.Batches[0]

	// Verify the batch still has original baseOffset (not yet patched)
	returnedBaseOffset := GetBaseOffset(returnedBatch)
	if returnedBaseOffset != originalBaseOffset {
		t.Errorf("expected original baseOffset %d, got %d", originalBaseOffset, returnedBaseOffset)
	}

	// Verify we can patch it
	err = PatchBaseOffset(returnedBatch, 100)
	if err != nil {
		t.Fatalf("PatchBaseOffset failed: %v", err)
	}

	newBaseOffset := GetBaseOffset(returnedBatch)
	if newBaseOffset != 100 {
		t.Errorf("expected patched baseOffset 100, got %d", newBaseOffset)
	}

	// Verify original store data is unchanged (batch was copied)
	storedData := store.objects[walPath]
	storedBatchOffset := 4 // skip the 4-byte length prefix
	storedBaseOffset := int64(binary.BigEndian.Uint64(storedData[storedBatchOffset : storedBatchOffset+8]))
	if storedBaseOffset != originalBaseOffset {
		t.Errorf("store data was modified, expected %d, got %d", originalBaseOffset, storedBaseOffset)
	}
}

func TestWALReader_GetRecordCount(t *testing.T) {
	reader := &WALReader{}

	tests := []struct {
		name        string
		recordCount int64
	}{
		{"1 record", 1},
		{"5 records", 5},
		{"100 records", 100},
		{"max records", 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := makeMinimalBatch(0, int32(tt.recordCount))
			got := reader.getRecordCount(batch)
			if got != tt.recordCount {
				t.Errorf("getRecordCount() = %d, want %d", got, tt.recordCount)
			}
		})
	}
}

func TestWALReader_GetRecordCount_TooSmall(t *testing.T) {
	reader := &WALReader{}

	// Batch that's too small to contain record count
	smallBatch := make([]byte, 60) // record count is at offset 57-61
	got := reader.getRecordCount(smallBatch)
	if got != 0 {
		t.Errorf("getRecordCount() for small batch = %d, want 0", got)
	}
}

func TestWALReaderWithCache_CacheHit(t *testing.T) {
	store := newMockStore()
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()
	reader := NewWALReaderWithCache(store, cache)

	// Create a single batch
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 100,
		EndOffset:   105,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()

	// First read - should populate cache
	result1, err := reader.ReadBatches(ctx, entry, 100, 0)
	if err != nil {
		t.Fatalf("First ReadBatches failed: %v", err)
	}

	// Verify cache was populated
	stats := cache.Stats()
	if stats.RangeCount != 1 {
		t.Errorf("Expected 1 cached range after first read, got %d", stats.RangeCount)
	}

	// Delete the object from store to ensure second read must use cache
	delete(store.objects, walPath)

	// Second read - should hit cache
	result2, err := reader.ReadBatches(ctx, entry, 100, 0)
	if err != nil {
		t.Fatalf("Second ReadBatches failed: %v", err)
	}

	// Results should be identical
	if len(result1.Batches) != len(result2.Batches) {
		t.Errorf("Batch counts differ: %d vs %d", len(result1.Batches), len(result2.Batches))
	}
	if result1.StartOffset != result2.StartOffset {
		t.Errorf("StartOffsets differ: %d vs %d", result1.StartOffset, result2.StartOffset)
	}
}

func TestWALReaderWithCache_CacheInvalidation(t *testing.T) {
	store := newMockStore()
	cache := NewObjectRangeCache(nil, DefaultRangeCacheConfig())
	defer cache.Close()
	reader := NewWALReaderWithCache(store, cache)

	// Create a single batch
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 100,
		EndOffset:   105,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()

	// First read - populates cache
	_, err := reader.ReadBatches(ctx, entry, 100, 0)
	if err != nil {
		t.Fatalf("First ReadBatches failed: %v", err)
	}

	// Invalidate the WAL
	cache.InvalidateWAL(walPath)

	// Verify cache is empty
	stats := cache.Stats()
	if stats.RangeCount != 0 {
		t.Errorf("Expected 0 cached ranges after invalidation, got %d", stats.RangeCount)
	}

	// Third read should work (re-fetches from store)
	_, err = reader.ReadBatches(ctx, entry, 100, 0)
	if err != nil {
		t.Fatalf("Third ReadBatches failed: %v", err)
	}

	// Cache should be repopulated
	stats = cache.Stats()
	if stats.RangeCount != 1 {
		t.Errorf("Expected 1 cached range after re-read, got %d", stats.RangeCount)
	}
}

func TestWALReaderWithCache_NilCache(t *testing.T) {
	store := newMockStore()
	reader := NewWALReaderWithCache(store, nil) // nil cache

	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 100,
		EndOffset:   105,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
	}

	ctx := context.Background()

	// Should work without cache
	result, err := reader.ReadBatches(ctx, entry, 100, 0)
	if err != nil {
		t.Fatalf("ReadBatches with nil cache failed: %v", err)
	}
	if len(result.Batches) != 1 {
		t.Errorf("Expected 1 batch, got %d", len(result.Batches))
	}
}

func TestWALReader_ReadBatches_BatchIndexMaxBytes(t *testing.T) {
	store := newMockStore()
	reader := NewWALReader(store)

	// Create 3 batches
	batch := makeMinimalBatch(0, 5)
	chunkData := makeChunkData([][]byte{batch, batch, batch})

	walPath := testWALPath("test")
	store.objects[walPath] = chunkData

	batchLen := uint32(len(batch))

	// Build batch index
	batchIndex := []index.BatchIndexEntry{
		{
			BatchStartOffsetDelta: 0,
			BatchLastOffsetDelta:  4,
			BatchOffsetInChunk:    4,
			BatchLength:           batchLen,
		},
		{
			BatchStartOffsetDelta: 5,
			BatchLastOffsetDelta:  9,
			BatchOffsetInChunk:    4 + batchLen + 4,
			BatchLength:           batchLen,
		},
		{
			BatchStartOffsetDelta: 10,
			BatchLastOffsetDelta:  14,
			BatchOffsetInChunk:    4 + batchLen + 4 + batchLen + 4,
			BatchLength:           batchLen,
		},
	}

	entry := &index.IndexEntry{
		StreamID:    "stream1",
		StartOffset: 0,
		EndOffset:   15,
		FileType:    index.FileTypeWAL,
		WalPath:     walPath,
		ChunkOffset: 0,
		ChunkLength: uint32(len(chunkData)),
		BatchIndex:  batchIndex,
	}

	ctx := context.Background()

	// Limit to 1 batch
	maxBytes := int64(batchLen + 1)
	result, err := reader.ReadBatches(ctx, entry, 0, maxBytes)
	if err != nil {
		t.Fatalf("ReadBatches failed: %v", err)
	}

	if len(result.Batches) != 1 {
		t.Errorf("expected 1 batch with maxBytes limit, got %d", len(result.Batches))
	}
}
