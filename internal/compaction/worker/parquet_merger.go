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
func (m *ParquetMerger) Merge(ctx context.Context, entries []*index.IndexEntry) (*MergeResult, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("merger: no entries to merge")
	}

	// Validate all entries are Parquet type and belong to same stream
	streamID := entries[0].StreamID
	for _, e := range entries {
		if e.FileType != index.FileTypeParquet {
			return nil, fmt.Errorf("merger: expected Parquet entry, got %s", e.FileType)
		}
		if e.StreamID != streamID {
			return nil, fmt.Errorf("merger: entries must belong to same stream")
		}
	}

	// Collect all records from all Parquet files
	var allRecords []Record

	for _, entry := range entries {
		records, err := m.readRecordsFromParquet(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("merger: reading parquet %s: %w", entry.ParquetPath, err)
		}
		allRecords = append(allRecords, records...)
	}

	if len(allRecords) == 0 {
		return nil, fmt.Errorf("merger: no records extracted from Parquet files")
	}

	// Write merged records to new Parquet file
	parquetData, stats, err := WriteToBuffer(BuildParquetSchema(nil), allRecords)
	if err != nil {
		return nil, fmt.Errorf("merger: writing merged parquet: %w", err)
	}

	return &MergeResult{
		ParquetData: parquetData,
		Stats:       stats,
		RecordCount: int64(len(allRecords)),
	}, nil
}

// readRecordsFromParquet reads all records from a single Parquet file.
func (m *ParquetMerger) readRecordsFromParquet(ctx context.Context, entry *index.IndexEntry) ([]Record, error) {
	// Get file metadata for size
	meta, err := m.store.Head(ctx, entry.ParquetPath)
	if err != nil {
		return nil, fmt.Errorf("getting file metadata: %w", err)
	}

	// Create a reader at for range reads
	readerAt := newObjectStoreReaderAt(ctx, m.store, entry.ParquetPath, meta.Size)

	// Open Parquet file
	parquetFile, err := parquet.OpenFile(readerAt, meta.Size,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}

	// Read all records using generic reader
	reader := parquet.NewGenericReader[Record](parquetFile)
	defer reader.Close()

	numRows := reader.NumRows()
	if numRows == 0 {
		return nil, nil
	}

	records := make([]Record, numRows)
	n, err := reader.Read(records)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading records: %w", err)
	}

	return records[:n], nil
}

// WriteParquetToStorage writes Parquet data to object storage.
func (m *ParquetMerger) WriteParquetToStorage(ctx context.Context, path string, data []byte) error {
	return m.store.Put(ctx, path, newBytesReader(data), int64(len(data)), "application/x-parquet")
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
