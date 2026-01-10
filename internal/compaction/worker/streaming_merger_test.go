package worker

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// mockStreamingStore is a mock object store for testing streaming merges.
type mockStreamingStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMockStreamingStore() *mockStreamingStore {
	return &mockStreamingStore{
		objects: make(map[string][]byte),
	}
}

func (m *mockStreamingStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return nil
}

func (m *mockStreamingStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *mockStreamingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockStreamingStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if start >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (m *mockStreamingStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{Size: int64(len(data))}, nil
}

func (m *mockStreamingStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func (m *mockStreamingStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	return nil, nil
}

func (m *mockStreamingStore) Close() error {
	return nil
}

// createParquetFile creates a parquet file with records having sequential offsets.
func createParquetFile(startOffset, count int) ([]byte, error) {
	records := make([]Record, count)
	for i := 0; i < count; i++ {
		records[i] = Record{
			Partition: 0,
			Offset:    int64(startOffset + i),
			Timestamp: int64(1000000 + startOffset + i),
			Key:       []byte("key"),
			Value:     []byte("value"),
		}
	}
	data, _, err := WriteToBuffer(BuildParquetSchema(nil), records)
	return data, err
}

// createParquetFileWithOffsets creates a parquet file with specific offsets.
func createParquetFileWithOffsets(offsets []int64) ([]byte, error) {
	records := make([]Record, len(offsets))
	for i, offset := range offsets {
		records[i] = Record{
			Partition: 0,
			Offset:    offset,
			Timestamp: int64(1000000 + offset),
			Key:       []byte("key"),
			Value:     []byte("value"),
		}
	}
	data, _, err := WriteToBuffer(BuildParquetSchema(nil), records)
	return data, err
}

func TestStreamingMerger_MergeTwoFiles(t *testing.T) {
	ctx := context.Background()
	store := newMockStreamingStore()

	// Create first Parquet file with records 0-99
	data1, err := createParquetFile(0, 100)
	if err != nil {
		t.Fatalf("createParquetFile() error = %v", err)
	}
	store.objects["parquet/file1.parquet"] = data1

	// Create second Parquet file with records 100-199
	data2, err := createParquetFile(100, 100)
	if err != nil {
		t.Fatalf("createParquetFile() error = %v", err)
	}
	store.objects["parquet/file2.parquet"] = data2

	// Create merger and merge
	merger := NewStreamingMerger(store)
	entries := []*index.IndexEntry{
		{
			StreamID:         "stream-1",
			StartOffset:      0,
			EndOffset:        100,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "parquet/file1.parquet",
			ParquetSizeBytes: uint64(len(data1)),
		},
		{
			StreamID:         "stream-1",
			StartOffset:      100,
			EndOffset:        200,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "parquet/file2.parquet",
			ParquetSizeBytes: uint64(len(data2)),
		},
	}

	result, err := merger.Merge(ctx, entries, DefaultStreamingMergeConfig())
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Verify result
	if result.RecordCount != 200 {
		t.Errorf("RecordCount = %d, want 200", result.RecordCount)
	}

	// Read merged Parquet and verify records
	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer reader.Close()

	mergedRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(mergedRecords) != 200 {
		t.Fatalf("len(mergedRecords) = %d, want 200", len(mergedRecords))
	}

	// Verify offsets are in order
	for i, rec := range mergedRecords {
		if rec.Offset != int64(i) {
			t.Errorf("Record %d: Offset = %d, want %d", i, rec.Offset, i)
		}
	}
}

func TestStreamingMerger_MergeManyFiles(t *testing.T) {
	ctx := context.Background()
	store := newMockStreamingStore()

	// Create 10 parquet files with 1000 records each
	numFiles := 10
	recordsPerFile := 1000
	totalRecords := numFiles * recordsPerFile

	entries := make([]*index.IndexEntry, numFiles)
	for i := 0; i < numFiles; i++ {
		startOffset := i * recordsPerFile
		data, err := createParquetFile(startOffset, recordsPerFile)
		if err != nil {
			t.Fatalf("createParquetFile(%d) error = %v", i, err)
		}
		path := "parquet/file" + string(rune('0'+i)) + ".parquet"
		store.objects[path] = data
		entries[i] = &index.IndexEntry{
			StreamID:         "stream-1",
			StartOffset:      int64(startOffset),
			EndOffset:        int64(startOffset + recordsPerFile),
			FileType:         index.FileTypeParquet,
			ParquetPath:      path,
			ParquetSizeBytes: uint64(len(data)),
		}
	}

	// Merge with small buffer sizes to test batching
	merger := NewStreamingMerger(store)
	cfg := StreamingMergeConfig{
		IteratorBufSize: 64,   // Small buffer to test refilling
		WriteBatchSize:  500,  // Small batch to test multiple flushes
	}

	result, err := merger.Merge(ctx, entries, cfg)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Verify result
	if result.RecordCount != int64(totalRecords) {
		t.Errorf("RecordCount = %d, want %d", result.RecordCount, totalRecords)
	}

	// Read merged Parquet and verify records
	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer reader.Close()

	mergedRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(mergedRecords) != totalRecords {
		t.Fatalf("len(mergedRecords) = %d, want %d", len(mergedRecords), totalRecords)
	}

	// Verify offsets are in order
	for i, rec := range mergedRecords {
		if rec.Offset != int64(i) {
			t.Errorf("Record %d: Offset = %d, want %d", i, rec.Offset, i)
		}
	}
}

func TestStreamingMerger_InterleavedOffsets(t *testing.T) {
	ctx := context.Background()
	store := newMockStreamingStore()

	// Create files with interleaved offsets to test k-way merge ordering
	// File 1: 0, 3, 6, 9
	// File 2: 1, 4, 7, 10
	// File 3: 2, 5, 8, 11
	// Expected merge: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

	data1, err := createParquetFileWithOffsets([]int64{0, 3, 6, 9})
	if err != nil {
		t.Fatalf("createParquetFileWithOffsets() error = %v", err)
	}
	store.objects["parquet/file1.parquet"] = data1

	data2, err := createParquetFileWithOffsets([]int64{1, 4, 7, 10})
	if err != nil {
		t.Fatalf("createParquetFileWithOffsets() error = %v", err)
	}
	store.objects["parquet/file2.parquet"] = data2

	data3, err := createParquetFileWithOffsets([]int64{2, 5, 8, 11})
	if err != nil {
		t.Fatalf("createParquetFileWithOffsets() error = %v", err)
	}
	store.objects["parquet/file3.parquet"] = data3

	entries := []*index.IndexEntry{
		{StreamID: "stream-1", FileType: index.FileTypeParquet, ParquetPath: "parquet/file1.parquet"},
		{StreamID: "stream-1", FileType: index.FileTypeParquet, ParquetPath: "parquet/file2.parquet"},
		{StreamID: "stream-1", FileType: index.FileTypeParquet, ParquetPath: "parquet/file3.parquet"},
	}

	merger := NewStreamingMerger(store)
	result, err := merger.Merge(ctx, entries, DefaultStreamingMergeConfig())
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	if result.RecordCount != 12 {
		t.Errorf("RecordCount = %d, want 12", result.RecordCount)
	}

	// Read and verify order
	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer reader.Close()

	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	// Verify sorted order
	for i, rec := range records {
		if rec.Offset != int64(i) {
			t.Errorf("Record %d: Offset = %d, want %d", i, rec.Offset, i)
		}
	}
}

func TestStreamingMerger_EmptyEntries(t *testing.T) {
	store := newMockStreamingStore()
	merger := NewStreamingMerger(store)

	_, err := merger.Merge(context.Background(), []*index.IndexEntry{}, DefaultStreamingMergeConfig())
	if err == nil {
		t.Error("Merge(empty) should return error")
	}
}

func TestStreamingMerger_WrongFileType(t *testing.T) {
	store := newMockStreamingStore()
	merger := NewStreamingMerger(store)

	entries := []*index.IndexEntry{
		{
			StreamID: "stream-1",
			FileType: index.FileTypeWAL, // Wrong type
		},
	}

	_, err := merger.Merge(context.Background(), entries, DefaultStreamingMergeConfig())
	if err == nil {
		t.Error("Merge(WAL entries) should return error")
	}
}

func TestStreamingMerger_DifferentStreams(t *testing.T) {
	store := newMockStreamingStore()
	merger := NewStreamingMerger(store)

	entries := []*index.IndexEntry{
		{StreamID: "stream-1", FileType: index.FileTypeParquet},
		{StreamID: "stream-2", FileType: index.FileTypeParquet}, // Different stream
	}

	_, err := merger.Merge(context.Background(), entries, DefaultStreamingMergeConfig())
	if err == nil {
		t.Error("Merge(different streams) should return error")
	}
}

func TestStreamingMerger_SingleFile(t *testing.T) {
	ctx := context.Background()
	store := newMockStreamingStore()

	// Create single file
	data, err := createParquetFile(0, 50)
	if err != nil {
		t.Fatalf("createParquetFile() error = %v", err)
	}
	store.objects["parquet/single.parquet"] = data

	entries := []*index.IndexEntry{
		{StreamID: "stream-1", FileType: index.FileTypeParquet, ParquetPath: "parquet/single.parquet"},
	}

	merger := NewStreamingMerger(store)
	result, err := merger.Merge(ctx, entries, DefaultStreamingMergeConfig())
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	if result.RecordCount != 50 {
		t.Errorf("RecordCount = %d, want 50", result.RecordCount)
	}
}

func TestParquetRecordIterator_Basic(t *testing.T) {
	ctx := context.Background()
	store := newMockStreamingStore()

	// Create parquet file with 100 records
	data, err := createParquetFile(0, 100)
	if err != nil {
		t.Fatalf("createParquetFile() error = %v", err)
	}
	store.objects["parquet/test.parquet"] = data

	entry := &index.IndexEntry{
		StreamID:    "stream-1",
		FileType:    index.FileTypeParquet,
		ParquetPath: "parquet/test.parquet",
	}

	// Create iterator with small buffer
	it, err := NewParquetRecordIterator(ctx, store, entry, 10)
	if err != nil {
		t.Fatalf("NewParquetRecordIterator() error = %v", err)
	}
	defer it.Close()

	// Read all records
	var count int
	var lastOffset int64 = -1
	for {
		rec, ok := it.Peek()
		if !ok {
			break
		}
		if rec.Offset <= lastOffset {
			t.Errorf("Offset %d not greater than last %d", rec.Offset, lastOffset)
		}
		lastOffset = rec.Offset
		count++
		if !it.Next() {
			break
		}
	}

	// We've already read the first record in NewParquetRecordIterator
	// so count should be 100
	if count != 100 {
		t.Errorf("Read %d records, want 100", count)
	}

	if it.Err() != nil {
		t.Errorf("Iterator error: %v", it.Err())
	}
}

// BenchmarkStreamingMerge benchmarks the streaming merge with many files.
func BenchmarkStreamingMerge(b *testing.B) {
	ctx := context.Background()
	store := newMockStreamingStore()

	// Create 20 files with 500 records each
	numFiles := 20
	recordsPerFile := 500

	entries := make([]*index.IndexEntry, numFiles)
	for i := 0; i < numFiles; i++ {
		startOffset := i * recordsPerFile
		data, err := createParquetFile(startOffset, recordsPerFile)
		if err != nil {
			b.Fatalf("createParquetFile(%d) error = %v", i, err)
		}
		path := "parquet/file" + string(rune('a'+i)) + ".parquet"
		store.objects[path] = data
		entries[i] = &index.IndexEntry{
			StreamID:    "stream-1",
			FileType:    index.FileTypeParquet,
			ParquetPath: path,
		}
	}

	merger := NewStreamingMerger(store)
	cfg := DefaultStreamingMergeConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := merger.Merge(ctx, entries, cfg)
		if err != nil {
			b.Fatalf("Merge() error = %v", err)
		}
	}
}
