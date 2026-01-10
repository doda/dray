// Package worker provides compaction workers for converting and merging data.
package worker

import (
	"container/heap"
	"context"
	"fmt"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// DefaultMergeBatchSize is the default number of records to buffer before writing.
const DefaultMergeBatchSize = 10000

// StreamingMergeConfig configures the streaming merge behavior.
type StreamingMergeConfig struct {
	// IteratorBufSize is the number of records to buffer per input file.
	// Default is 256.
	IteratorBufSize int

	// WriteBatchSize is the number of records to accumulate before writing to output.
	// Default is 10000.
	WriteBatchSize int
}

// DefaultStreamingMergeConfig returns the default configuration.
func DefaultStreamingMergeConfig() StreamingMergeConfig {
	return StreamingMergeConfig{
		IteratorBufSize: DefaultIteratorBufSize,
		WriteBatchSize:  DefaultMergeBatchSize,
	}
}

// StreamingMerger merges multiple sorted parquet files using k-way merge.
// It uses a min-heap to efficiently merge sorted streams while keeping
// memory usage bounded.
type StreamingMerger struct {
	store objectstore.Store
}

// NewStreamingMerger creates a new streaming merger.
func NewStreamingMerger(store objectstore.Store) *StreamingMerger {
	return &StreamingMerger{store: store}
}

// iteratorHeap is a min-heap of iterators ordered by current record offset.
type iteratorHeap []*ParquetRecordIterator

func (h iteratorHeap) Len() int { return len(h) }

func (h iteratorHeap) Less(i, j int) bool {
	// Order by offset for correct merge order
	return h[i].current.Offset < h[j].current.Offset
}

func (h iteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iteratorHeap) Push(x any) {
	*h = append(*h, x.(*ParquetRecordIterator))
}

func (h *iteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}

// Merge reads records from multiple parquet files and merges them into one.
// It uses a k-way merge with a min-heap to maintain sort order while
// keeping memory usage bounded.
func (m *StreamingMerger) Merge(ctx context.Context, entries []*index.IndexEntry, cfg StreamingMergeConfig) (*MergeResult, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("streaming merger: no entries to merge")
	}

	// Apply defaults
	if cfg.IteratorBufSize <= 0 {
		cfg.IteratorBufSize = DefaultIteratorBufSize
	}
	if cfg.WriteBatchSize <= 0 {
		cfg.WriteBatchSize = DefaultMergeBatchSize
	}

	// Validate all entries are Parquet type and belong to same stream
	streamID := entries[0].StreamID
	for _, e := range entries {
		if e.FileType != index.FileTypeParquet {
			return nil, fmt.Errorf("streaming merger: expected Parquet entry, got %s", e.FileType)
		}
		if e.StreamID != streamID {
			return nil, fmt.Errorf("streaming merger: entries must belong to same stream")
		}
	}

	// Create iterators for each entry
	iterators := make([]*ParquetRecordIterator, 0, len(entries))
	defer func() {
		// Clean up all iterators on exit
		for _, it := range iterators {
			it.Close()
		}
	}()

	for _, entry := range entries {
		it, err := NewParquetRecordIterator(ctx, m.store, entry, cfg.IteratorBufSize)
		if err != nil {
			return nil, fmt.Errorf("streaming merger: creating iterator for %s: %w", entry.ParquetPath, err)
		}
		// Only add non-empty iterators
		if _, ok := it.Peek(); ok {
			iterators = append(iterators, it)
		} else {
			it.Close()
		}
	}

	if len(iterators) == 0 {
		return nil, fmt.Errorf("streaming merger: no records found in any parquet files")
	}

	// Build min-heap from iterators
	h := make(iteratorHeap, len(iterators))
	copy(h, iterators)
	heap.Init(&h)

	// Create parquet writer
	schema := BuildParquetSchema(nil)
	writer := NewWriter(schema)

	// Batch buffer for writing
	batch := make([]Record, 0, cfg.WriteBatchSize)
	var recordCount int64

	// K-way merge loop
	for h.Len() > 0 {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Get iterator with smallest current record
		it := h[0]
		rec, _ := it.Peek()

		// Add to batch
		batch = append(batch, rec)
		recordCount++

		// Flush batch if full
		if len(batch) >= cfg.WriteBatchSize {
			if err := writer.WriteRecords(batch); err != nil {
				return nil, fmt.Errorf("streaming merger: writing batch: %w", err)
			}
			batch = batch[:0]
		}

		// Advance the iterator
		if it.Next() {
			// Iterator has more records, re-heapify
			heap.Fix(&h, 0)
		} else {
			// Iterator exhausted, remove from heap
			if err := it.Err(); err != nil {
				return nil, fmt.Errorf("streaming merger: iterator error: %w", err)
			}
			heap.Pop(&h)
		}
	}

	// Flush remaining records
	if len(batch) > 0 {
		if err := writer.WriteRecords(batch); err != nil {
			return nil, fmt.Errorf("streaming merger: writing final batch: %w", err)
		}
	}

	// Close writer and get output
	parquetData, stats, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("streaming merger: closing writer: %w", err)
	}

	return &MergeResult{
		ParquetData: parquetData,
		Stats:       stats,
		RecordCount: recordCount,
	}, nil
}
