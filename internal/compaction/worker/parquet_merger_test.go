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

// mockMergerStore is a mock object store for testing merges.
type mockMergerStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMockMergerStore() *mockMergerStore {
	return &mockMergerStore{
		objects: make(map[string][]byte),
	}
}

func (m *mockMergerStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return nil
}

func (m *mockMergerStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *mockMergerStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockMergerStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
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

func (m *mockMergerStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{Size: int64(len(data))}, nil
}

func (m *mockMergerStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func (m *mockMergerStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	return nil, nil
}

func (m *mockMergerStore) Close() error {
	return nil
}

func TestParquetMerger_MergeMultipleFiles(t *testing.T) {
	ctx := context.Background()
	store := newMockMergerStore()

	// Create first Parquet file with records 0-99
	records1 := make([]Record, 100)
	for i := 0; i < 100; i++ {
		records1[i] = Record{
			Partition: 0,
			Offset:    int64(i),
			Timestamp: int64(1000 + i),
			Key:       []byte("key"),
			Value:     []byte("value1"),
		}
	}
	data1, stats1, err := WriteToBuffer(records1)
	if err != nil {
		t.Fatalf("WriteToBuffer() error = %v", err)
	}
	store.objects["parquet/file1.parquet"] = data1

	// Create second Parquet file with records 100-199
	records2 := make([]Record, 100)
	for i := 0; i < 100; i++ {
		records2[i] = Record{
			Partition: 0,
			Offset:    int64(100 + i),
			Timestamp: int64(2000 + i),
			Key:       []byte("key"),
			Value:     []byte("value2"),
		}
	}
	data2, stats2, err := WriteToBuffer(records2)
	if err != nil {
		t.Fatalf("WriteToBuffer() error = %v", err)
	}
	store.objects["parquet/file2.parquet"] = data2

	// Create merger and merge the files
	merger := NewParquetMerger(store)
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

	result, err := merger.Merge(ctx, entries)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Verify result
	if result.RecordCount != 200 {
		t.Errorf("RecordCount = %d, want 200", result.RecordCount)
	}
	if result.Stats.MinOffset != 0 {
		t.Errorf("MinOffset = %d, want 0", result.Stats.MinOffset)
	}
	if result.Stats.MaxOffset != 199 {
		t.Errorf("MaxOffset = %d, want 199", result.Stats.MaxOffset)
	}
	if result.Stats.MinTimestamp != stats1.MinTimestamp {
		t.Errorf("MinTimestamp = %d, want %d", result.Stats.MinTimestamp, stats1.MinTimestamp)
	}
	if result.Stats.MaxTimestamp != stats2.MaxTimestamp {
		t.Errorf("MaxTimestamp = %d, want %d", result.Stats.MaxTimestamp, stats2.MaxTimestamp)
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

func TestParquetMerger_EmptyEntries(t *testing.T) {
	store := newMockMergerStore()
	merger := NewParquetMerger(store)

	_, err := merger.Merge(context.Background(), []*index.IndexEntry{})
	if err == nil {
		t.Error("Merge(empty) should return error")
	}
}

func TestParquetMerger_WrongFileType(t *testing.T) {
	store := newMockMergerStore()
	merger := NewParquetMerger(store)

	entries := []*index.IndexEntry{
		{
			StreamID:    "stream-1",
			StartOffset: 0,
			EndOffset:   100,
			FileType:    index.FileTypeWAL, // Wrong type
		},
	}

	_, err := merger.Merge(context.Background(), entries)
	if err == nil {
		t.Error("Merge(WAL entries) should return error")
	}
}

func TestParquetMerger_DifferentStreams(t *testing.T) {
	store := newMockMergerStore()
	merger := NewParquetMerger(store)

	entries := []*index.IndexEntry{
		{
			StreamID:    "stream-1",
			StartOffset: 0,
			EndOffset:   100,
			FileType:    index.FileTypeParquet,
		},
		{
			StreamID:    "stream-2", // Different stream
			StartOffset: 100,
			EndOffset:   200,
			FileType:    index.FileTypeParquet,
		},
	}

	_, err := merger.Merge(context.Background(), entries)
	if err == nil {
		t.Error("Merge(different streams) should return error")
	}
}

func TestParquetMerger_WriteParquetToStorage(t *testing.T) {
	ctx := context.Background()
	store := newMockMergerStore()
	merger := NewParquetMerger(store)

	// Create test Parquet data
	records := []Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Value: []byte("test")},
	}
	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer() error = %v", err)
	}

	// Write to storage
	path := "output/merged.parquet"
	err = merger.WriteParquetToStorage(ctx, path, data)
	if err != nil {
		t.Fatalf("WriteParquetToStorage() error = %v", err)
	}

	// Verify it was written
	stored, ok := store.objects[path]
	if !ok {
		t.Fatal("Parquet file not written to storage")
	}
	if !bytes.Equal(stored, data) {
		t.Error("Stored data doesn't match")
	}
}
