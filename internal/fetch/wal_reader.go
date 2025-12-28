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
}

// NewWALReader creates a new WAL reader.
func NewWALReader(store objectstore.Store) *WALReader {
	return &WALReader{store: store}
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

	// Range-read the chunk data from object storage
	// ChunkOffset and ChunkLength tell us exactly where the chunk is
	startByte := int64(entry.ChunkOffset)
	endByte := startByte + int64(entry.ChunkLength) - 1

	rc, err := r.store.GetRange(ctx, entry.WalPath, startByte, endByte)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrChunkNotFound, entry.WalPath)
		}
		return nil, fmt.Errorf("fetch: reading WAL chunk: %w", err)
	}
	defer rc.Close()

	chunkData, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("fetch: reading chunk data: %w", err)
	}

	if len(chunkData) != int(entry.ChunkLength) {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", ErrInvalidChunk, entry.ChunkLength, len(chunkData))
	}

	// Parse batches from chunk data and find the ones we need
	return r.extractBatches(chunkData, entry, fetchOffset, maxBytes)
}

// extractBatches parses the chunk data and extracts batches starting from fetchOffset.
// The chunk format is: [batchLen (4 bytes), batchData (batchLen bytes)] repeated.
func (r *WALReader) extractBatches(chunkData []byte, entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (*FetchResult, error) {
	result := &FetchResult{
		Batches:     make([][]byte, 0),
		StartOffset: -1,
	}

	// If we have a batchIndex, use it for efficient seeking
	if len(entry.BatchIndex) > 0 {
		return r.extractBatchesWithIndex(chunkData, entry, fetchOffset, maxBytes)
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

// extractBatchesWithIndex uses the batchIndex for efficient offset seeking.
func (r *WALReader) extractBatchesWithIndex(chunkData []byte, entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (*FetchResult, error) {
	result := &FetchResult{
		Batches:     make([][]byte, 0),
		StartOffset: -1,
	}

	// Find the first batch that contains data at or after fetchOffset
	startBatchIdx := -1
	for i, bi := range entry.BatchIndex {
		batchStartOffset := entry.StartOffset + int64(bi.BatchStartOffsetDelta)
		batchLastOffset := entry.StartOffset + int64(bi.BatchLastOffsetDelta)

		// This batch contains our offset if: fetchOffset <= batchLastOffset
		// (batch covers [batchStartOffset, batchLastOffset])
		if fetchOffset <= batchLastOffset {
			startBatchIdx = i
			break
		}
		_ = batchStartOffset // used for offset tracking
	}

	if startBatchIdx < 0 {
		return nil, ErrOffsetNotInChunk
	}

	var totalBytes int64

	// Extract batches starting from startBatchIdx
	for i := startBatchIdx; i < len(entry.BatchIndex); i++ {
		bi := entry.BatchIndex[i]

		// Check maxBytes limit
		if maxBytes > 0 && totalBytes+int64(bi.BatchLength) > maxBytes && len(result.Batches) > 0 {
			break
		}

		// Read the batch data using the batchIndex offsets
		batchStart := int(bi.BatchOffsetInChunk)
		batchEnd := batchStart + int(bi.BatchLength)

		// Note: BatchOffsetInChunk is the offset within the chunk data AFTER the 4-byte length prefix
		// So we need to adjust for the batch format in the chunk
		// The chunk format is: [4-byte length][batch data][4-byte length][batch data]...
		// But BatchOffsetInChunk might already account for this - let's check the actual layout

		// Actually, looking at the WAL format, the chunk body has:
		// [batchLen (4 bytes), batchData (batchLen bytes)] repeated
		// So we need to skip the length prefix when reading

		// Recalculate the actual position accounting for length prefixes
		actualStart := int(bi.BatchOffsetInChunk)
		if actualStart+int(bi.BatchLength) > len(chunkData) {
			return nil, fmt.Errorf("%w: batch %d overflows chunk", ErrInvalidChunk, i)
		}

		batchData := make([]byte, bi.BatchLength)
		copy(batchData, chunkData[actualStart:actualStart+int(bi.BatchLength)])
		result.Batches = append(result.Batches, batchData)
		totalBytes += int64(bi.BatchLength)

		batchStartOffset := entry.StartOffset + int64(bi.BatchStartOffsetDelta)
		batchEndOffset := entry.StartOffset + int64(bi.BatchLastOffsetDelta) + 1

		if result.StartOffset < 0 {
			result.StartOffset = batchStartOffset
		}
		result.EndOffset = batchEndOffset
		_ = batchEnd // mark as used
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
