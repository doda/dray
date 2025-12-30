package fetch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// Common errors for WAL reading.
var (
	// ErrChunkNotFound is returned when the chunk cannot be located in the WAL.
	ErrChunkNotFound = errors.New("fetch: chunk not found in WAL")

	// ErrInvalidChunk is returned when chunk data is corrupt or invalid.
	ErrInvalidChunk = errors.New("fetch: invalid chunk data")

	// ErrOffsetNotInChunk is returned when the requested offset is not in the chunk.
	ErrOffsetNotInChunk = errors.New("fetch: offset not in chunk")
)

// WALReader reads record batches from WAL objects.
type WALReader struct {
	store objectstore.Store
	cache *ObjectRangeCache
}

// NewWALReader creates a new WAL reader.
func NewWALReader(store objectstore.Store) *WALReader {
	return &WALReader{store: store}
}

// NewWALReaderWithCache creates a new WAL reader with an optional range cache.
// If cache is non-nil, WAL chunk reads will be cached for faster subsequent access.
func NewWALReaderWithCache(store objectstore.Store, cache *ObjectRangeCache) *WALReader {
	return &WALReader{store: store, cache: cache}
}

// FetchResult contains the result of a fetch operation.
type FetchResult struct {
	// Batches contains the raw Kafka record batch bytes.
	// These are ready to be patched with correct base offsets.
	Batches [][]byte

	// TotalBytes is the total size of all batches.
	TotalBytes int64

	// StartOffset is the first offset in the returned batches.
	StartOffset int64

	// EndOffset is one past the last offset (exclusive).
	EndOffset int64
}

// ReadBatches reads record batches from a WAL object for the given index entry.
// It uses the batchIndex for efficient offset seeking when available.
// If a cache is configured, reads are served from cache when available.
//
// Parameters:
//   - ctx: Context for cancellation
//   - entry: The index entry pointing to the WAL location
//   - fetchOffset: The offset to start reading from
//   - maxBytes: Maximum bytes to return (0 = unlimited)
//
// Returns the raw Kafka record batch bytes ready for offset patching.
func (r *WALReader) ReadBatches(ctx context.Context, entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (*FetchResult, error) {
	if entry.FileType != index.FileTypeWAL {
		return nil, fmt.Errorf("fetch: expected WAL entry, got %s", entry.FileType)
	}

	if len(entry.BatchIndex) > 0 {
		return r.readBatchesWithIndex(ctx, entry, fetchOffset, maxBytes)
	}

	// Range-read the chunk data from object storage
	// ChunkOffset and ChunkLength tell us exactly where the chunk is
	startByte := int64(entry.ChunkOffset)
	endByte := startByte + int64(entry.ChunkLength) - 1

	chunkData, err := r.readChunk(ctx, entry.WalPath, startByte, endByte, int(entry.ChunkLength))
	if err != nil {
		return nil, err
	}

	// Parse batches from chunk data and find the ones we need
	return r.extractBatches(chunkData, entry, fetchOffset, maxBytes)
}

// readChunk reads a WAL chunk, using the cache if available.
func (r *WALReader) readChunk(ctx context.Context, walPath string, startByte, endByte int64, expectedLen int) ([]byte, error) {
	// Try cache first if available
	if r.cache != nil {
		if data, ok := r.cache.Get(walPath, startByte, endByte); ok {
			if len(data) == expectedLen {
				return data, nil
			}
			// Cache data size mismatch - invalidate and fetch fresh
			r.cache.InvalidateWAL(walPath)
		}
	}

	// Fetch from object store
	rc, err := r.store.GetRange(ctx, walPath, startByte, endByte)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrChunkNotFound, walPath)
		}
		return nil, fmt.Errorf("fetch: reading WAL chunk: %w", err)
	}
	defer rc.Close()

	chunkData, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("fetch: reading chunk data: %w", err)
	}

	if len(chunkData) != expectedLen {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", ErrInvalidChunk, expectedLen, len(chunkData))
	}

	// Store in cache if available
	if r.cache != nil {
		r.cache.Put(walPath, startByte, endByte, chunkData)
	}

	return chunkData, nil
}

func (r *WALReader) readBatchesWithIndex(ctx context.Context, entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (*FetchResult, error) {
	startBatchIdx, endBatchIdx, err := r.batchRangeForFetch(entry, fetchOffset, maxBytes)
	if err != nil {
		return nil, err
	}

	rangeStart := int64(entry.BatchIndex[startBatchIdx].BatchOffsetInChunk)
	rangeEnd := int64(entry.BatchIndex[endBatchIdx].BatchOffsetInChunk) + int64(entry.BatchIndex[endBatchIdx].BatchLength) - 1
	if rangeEnd < rangeStart {
		return nil, fmt.Errorf("%w: invalid batch range", ErrInvalidChunk)
	}

	startByte := int64(entry.ChunkOffset) + rangeStart
	endByte := int64(entry.ChunkOffset) + rangeEnd
	chunkData, err := r.readChunk(ctx, entry.WalPath, startByte, endByte, int(rangeEnd-rangeStart+1))
	if err != nil {
		return nil, err
	}

	return r.extractBatchesWithIndexRange(chunkData, entry, startBatchIdx, endBatchIdx, rangeStart)
}

func (r *WALReader) batchRangeForFetch(entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (int, int, error) {
	if len(entry.BatchIndex) == 0 {
		return -1, -1, ErrOffsetNotInChunk
	}

	// Find the first batch where lastOffset >= fetchOffset.
	startBatchIdx := -1
	targetDelta := fetchOffset - entry.StartOffset
	lo, hi := 0, len(entry.BatchIndex)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		batch := entry.BatchIndex[mid]
		lastDelta := int64(batch.BatchLastOffsetDelta)

		if lastDelta >= targetDelta {
			startBatchIdx = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	if startBatchIdx < 0 {
		return -1, -1, ErrOffsetNotInChunk
	}

	endBatchIdx := startBatchIdx
	var totalBytes int64
	for i := startBatchIdx; i < len(entry.BatchIndex); i++ {
		bi := entry.BatchIndex[i]

		if maxBytes > 0 && totalBytes+int64(bi.BatchLength) > maxBytes && i > startBatchIdx {
			break
		}

		totalBytes += int64(bi.BatchLength)
		endBatchIdx = i
	}

	return startBatchIdx, endBatchIdx, nil
}

// extractBatches parses the chunk data and extracts batches starting from fetchOffset.
// The chunk format is: [batchLen (4 bytes), batchData (batchLen bytes)] repeated.
func (r *WALReader) extractBatches(chunkData []byte, entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (*FetchResult, error) {
	result := &FetchResult{
		Batches:     make([][]byte, 0),
		StartOffset: -1,
	}

	// Without batchIndex, we need to scan through all batches
	offset := 0
	currentOffset := entry.StartOffset
	var totalBytes int64

	for offset < len(chunkData) {
		// Read batch length prefix (4 bytes)
		if offset+4 > len(chunkData) {
			break
		}
		batchLen := int(binary.BigEndian.Uint32(chunkData[offset : offset+4]))
		offset += 4

		if offset+batchLen > len(chunkData) {
			return nil, fmt.Errorf("%w: batch truncated at offset %d", ErrInvalidChunk, offset)
		}

		batchData := chunkData[offset : offset+batchLen]

		// Get record count from batch header
		recordCount := r.getRecordCount(batchData)
		batchEndOffset := currentOffset + int64(recordCount)

		// Check if this batch contains data we want
		if batchEndOffset > fetchOffset {
			// Check maxBytes limit
			if maxBytes > 0 && totalBytes+int64(batchLen) > maxBytes && len(result.Batches) > 0 {
				break
			}

			// Include this batch
			batchCopy := make([]byte, batchLen)
			copy(batchCopy, batchData)
			result.Batches = append(result.Batches, batchCopy)
			totalBytes += int64(batchLen)

			if result.StartOffset < 0 {
				result.StartOffset = currentOffset
			}
			result.EndOffset = batchEndOffset
		}

		currentOffset = batchEndOffset
		offset += batchLen
	}

	if len(result.Batches) == 0 {
		return nil, ErrOffsetNotInChunk
	}

	result.TotalBytes = totalBytes
	return result, nil
}

func (r *WALReader) extractBatchesWithIndexRange(chunkData []byte, entry *index.IndexEntry, startBatchIdx int, endBatchIdx int, rangeStart int64) (*FetchResult, error) {
	result := &FetchResult{
		Batches:     make([][]byte, 0),
		StartOffset: -1,
	}

	if startBatchIdx < 0 || endBatchIdx < startBatchIdx {
		return nil, ErrOffsetNotInChunk
	}

	var totalBytes int64

	for i := startBatchIdx; i <= endBatchIdx; i++ {
		bi := entry.BatchIndex[i]

		// Read the batch data using the batchIndex offsets.
		// BatchOffsetInChunk is the offset to the batch data (after the length prefix).
		actualStart := int64(bi.BatchOffsetInChunk) - rangeStart
		if actualStart < 0 {
			return nil, fmt.Errorf("%w: batch %d starts before range", ErrInvalidChunk, i)
		}
		actualEnd := actualStart + int64(bi.BatchLength)
		if actualEnd > int64(len(chunkData)) {
			return nil, fmt.Errorf("%w: batch %d overflows chunk", ErrInvalidChunk, i)
		}

		batchData := make([]byte, bi.BatchLength)
		copy(batchData, chunkData[int(actualStart):int(actualEnd)])
		result.Batches = append(result.Batches, batchData)
		totalBytes += int64(bi.BatchLength)

		batchStartOffset := entry.StartOffset + int64(bi.BatchStartOffsetDelta)
		batchEndOffset := entry.StartOffset + int64(bi.BatchLastOffsetDelta) + 1

		if result.StartOffset < 0 {
			result.StartOffset = batchStartOffset
		}
		result.EndOffset = batchEndOffset
	}

	if len(result.Batches) == 0 {
		return nil, ErrOffsetNotInChunk
	}

	result.TotalBytes = totalBytes
	return result, nil
}

// getRecordCount extracts the record count from a Kafka record batch.
// The record count is at offset 57 (4 bytes) in the batch header.
func (r *WALReader) getRecordCount(batchData []byte) int64 {
	if len(batchData) < 61 {
		return 0
	}
	// Kafka record batch format:
	// baseOffset (8) + batchLength (4) + partitionLeaderEpoch (4) + magic (1) +
	// crc (4) + attributes (2) + lastOffsetDelta (4) + firstTimestamp (8) +
	// maxTimestamp (8) + producerId (8) + producerEpoch (2) + firstSequence (4) +
	// recordCount (4)
	// Total offset to recordCount: 8+4+4+1+4+2+4+8+8+8+2+4 = 57
	recordCount := binary.BigEndian.Uint32(batchData[57:61])
	return int64(recordCount)
}
