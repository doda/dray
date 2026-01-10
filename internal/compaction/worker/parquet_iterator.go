// Package worker provides compaction workers for converting and merging data.
package worker

import (
	"context"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// DefaultIteratorBufSize is the default number of records to buffer per iterator.
const DefaultIteratorBufSize = 256

// ParquetRecordIterator reads records from parquet files with buffering.
// It reads in small batches to avoid loading entire files into memory.
type ParquetRecordIterator struct {
	ctx     context.Context
	store   objectstore.Store
	entry   *index.IndexEntry
	paths   []string
	pathIdx int

	// Current file state
	readerAt *objectStoreReaderAt
	file     *parquet.File
	reader   *parquet.GenericReader[Record]

	// Buffer state
	buf     []Record
	bufSize int
	pos     int

	// Current record (valid after first Next() call)
	current Record
	done    bool
	err     error
}

// NewParquetRecordIterator creates a new iterator for reading records from parquet files.
// The iterator reads records in batches of bufSize to minimize memory usage.
func NewParquetRecordIterator(ctx context.Context, store objectstore.Store, entry *index.IndexEntry, bufSize int) (*ParquetRecordIterator, error) {
	if bufSize <= 0 {
		bufSize = DefaultIteratorBufSize
	}

	paths := entry.ParquetPaths
	if len(paths) == 0 {
		if entry.ParquetPath == "" {
			return nil, fmt.Errorf("iterator: parquet entry missing path for stream %s", entry.StreamID)
		}
		paths = []string{entry.ParquetPath}
	}

	it := &ParquetRecordIterator{
		ctx:     ctx,
		store:   store,
		entry:   entry,
		paths:   paths,
		pathIdx: -1, // Will be incremented to 0 on first openNextFile
		buf:     make([]Record, 0, bufSize), // Length 0 so first Next() triggers fillBuffer
		bufSize: bufSize,
		pos:     0,
	}

	// Open first file and read first record
	if err := it.openNextFile(); err != nil {
		return nil, err
	}

	// Advance to first record
	if !it.Next() {
		if it.err != nil {
			return nil, it.err
		}
		// No records - mark as done
		it.done = true
	}

	return it, nil
}

// openNextFile opens the next parquet file in the list.
func (it *ParquetRecordIterator) openNextFile() error {
	// Close current reader if open
	if it.reader != nil {
		it.reader.Close()
		it.reader = nil
	}

	it.pathIdx++
	if it.pathIdx >= len(it.paths) {
		it.done = true
		return nil
	}

	path := it.paths[it.pathIdx]
	key := objectstore.NormalizeKey(path)

	// Get file metadata for size
	meta, err := it.store.Head(it.ctx, key)
	if err != nil {
		return fmt.Errorf("iterator: getting file metadata for %s: %w", path, err)
	}

	// Create reader at for range reads
	it.readerAt = newObjectStoreReaderAt(it.ctx, it.store, key, meta.Size)

	// Open parquet file
	it.file, err = parquet.OpenFile(it.readerAt, meta.Size,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true))
	if err != nil {
		return fmt.Errorf("iterator: opening parquet file %s: %w", path, err)
	}

	// Create generic reader
	it.reader = parquet.NewGenericReader[Record](it.file)
	it.pos = 0

	return nil
}

// fillBuffer reads the next batch of records into the buffer.
func (it *ParquetRecordIterator) fillBuffer() error {
	if it.reader == nil {
		return io.EOF
	}

	n, err := it.reader.Read(it.buf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("iterator: reading records: %w", err)
	}

	// Normalize timestamps
	for i := 0; i < n; i++ {
		it.buf[i].Timestamp = normalizeParquetTimestamp(it.buf[i].Timestamp)
	}

	it.pos = 0
	it.buf = it.buf[:n]

	if n == 0 {
		// No records read - try next file
		if err := it.openNextFile(); err != nil {
			return err
		}
		if it.done {
			return io.EOF
		}
		// Recursively fill from new file
		return it.fillBuffer()
	}

	// Got records - return them. If err == io.EOF, we'll discover there's
	// nothing more on the next fillBuffer() call when Read returns n=0.
	return nil
}

// Peek returns the current record without advancing the iterator.
// Returns false if the iterator is exhausted.
func (it *ParquetRecordIterator) Peek() (Record, bool) {
	if it.done {
		return Record{}, false
	}
	return it.current, true
}

// Next advances the iterator to the next record.
// Returns true if there is a next record, false if exhausted or error.
func (it *ParquetRecordIterator) Next() bool {
	if it.done {
		return false
	}

	// Check if we need to refill buffer
	if it.pos >= len(it.buf) {
		// Reset buffer capacity
		it.buf = it.buf[:cap(it.buf)]
		if err := it.fillBuffer(); err != nil {
			if err == io.EOF {
				it.done = true
				return false
			}
			it.err = err
			it.done = true
			return false
		}
	}

	if len(it.buf) == 0 {
		it.done = true
		return false
	}

	it.current = it.buf[it.pos]
	it.pos++
	return true
}

// Err returns any error that occurred during iteration.
func (it *ParquetRecordIterator) Err() error {
	return it.err
}

// Close closes the iterator and releases resources.
func (it *ParquetRecordIterator) Close() error {
	if it.reader != nil {
		it.reader.Close()
		it.reader = nil
	}
	it.done = true
	return nil
}

// Current returns the current record. Only valid after Next() returns true.
func (it *ParquetRecordIterator) Current() Record {
	return it.current
}
