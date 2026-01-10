// Package worker converts WAL entries to Parquet and performs index swap.
package worker

import (
	"context"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// ParquetMerger reads multiple Parquet files and merges them into one.
type ParquetMerger struct {
	store objectstore.Store
}

// NewParquetMerger creates a new Parquet merger.
func NewParquetMerger(store objectstore.Store) *ParquetMerger {
	return &ParquetMerger{store: store}
}

// MergeResult contains the result of a Parquet merge operation.
type MergeResult struct {
	// ParquetData is the merged Parquet file bytes.
	ParquetData []byte
	// Stats contains file-level statistics.
	Stats FileStats
	// RecordCount is the total number of records merged.
	RecordCount int64
}

// Merge reads records from multiple Parquet files and merges them into one.
// All entries must be Parquet type and belong to the same stream.
// Entries are expected to be in offset order.
//
// This implementation uses streaming k-way merge to keep memory usage bounded.
// Instead of loading all records into memory, it uses iterators with buffering
// and a min-heap to merge sorted streams efficiently.
func (m *ParquetMerger) Merge(ctx context.Context, entries []*index.IndexEntry) (*MergeResult, error) {
	// Delegate to streaming merger for memory-efficient merging
	streamingMerger := NewStreamingMerger(m.store)
	return streamingMerger.Merge(ctx, entries, DefaultStreamingMergeConfig())
}

// MergeWithConfig reads records from multiple Parquet files and merges them into one
// using the provided configuration.
func (m *ParquetMerger) MergeWithConfig(ctx context.Context, entries []*index.IndexEntry, cfg StreamingMergeConfig) (*MergeResult, error) {
	streamingMerger := NewStreamingMerger(m.store)
	return streamingMerger.Merge(ctx, entries, cfg)
}

// readRecordsFromParquet reads all records from a single Parquet file.
func (m *ParquetMerger) readRecordsFromParquet(ctx context.Context, entry *index.IndexEntry) ([]Record, error) {
	paths := entry.ParquetPaths
	if len(paths) == 0 {
		if entry.ParquetPath == "" {
			return nil, fmt.Errorf("merger: parquet entry missing path for stream %s", entry.StreamID)
		}
		paths = []string{entry.ParquetPath}
	}

	var all []Record
	for _, path := range paths {
		key := objectstore.NormalizeKey(path)
		// Get file metadata for size
		meta, err := m.store.Head(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("getting file metadata: %w", err)
		}

		// Create a reader at for range reads
		readerAt := newObjectStoreReaderAt(ctx, m.store, key, meta.Size)

		// Open Parquet file
		parquetFile, err := parquet.OpenFile(readerAt, meta.Size,
			parquet.SkipPageIndex(true),
			parquet.SkipBloomFilters(true))
		if err != nil {
			return nil, fmt.Errorf("opening parquet file: %w", err)
		}

		// Read all records using generic reader
		reader := parquet.NewGenericReader[Record](parquetFile)

		numRows := reader.NumRows()
		if numRows == 0 {
			continue
		}

		records := make([]Record, numRows)
		n, err := reader.Read(records)
		if err != nil && err != io.EOF {
			reader.Close()
			return nil, fmt.Errorf("reading records: %w", err)
		}
		if err := reader.Close(); err != nil {
			return nil, fmt.Errorf("closing parquet reader: %w", err)
		}
		for i := 0; i < n; i++ {
			records[i].Timestamp = normalizeParquetTimestamp(records[i].Timestamp)
		}
		all = append(all, records[:n]...)
	}

	return all, nil
}

// WriteParquetToStorage writes Parquet data to object storage.
func (m *ParquetMerger) WriteParquetToStorage(ctx context.Context, path string, data []byte) error {
	key := objectstore.NormalizeKey(path)
	return m.store.Put(ctx, key, newBytesReader(data), int64(len(data)), "application/x-parquet")
}

// objectStoreReaderAt implements parquet.File for reading from object storage via range requests.
type objectStoreReaderAt struct {
	ctx   context.Context
	store objectstore.Store
	key   string
	size  int64
}

func newObjectStoreReaderAt(ctx context.Context, store objectstore.Store, key string, size int64) *objectStoreReaderAt {
	return &objectStoreReaderAt{
		ctx:   ctx,
		store: store,
		key:   key,
		size:  size,
	}
}

func (r *objectStoreReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	rc, err := r.store.GetRange(r.ctx, r.key, off, end)
	if err != nil {
		return 0, fmt.Errorf("range read: %w", err)
	}
	defer rc.Close()

	return io.ReadFull(rc, p[:end-off+1])
}

func (r *objectStoreReaderAt) Size() int64 {
	return r.size
}

// bytesReader wraps a byte slice to implement io.Reader.
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
