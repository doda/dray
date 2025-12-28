package fetch

import (
	"encoding/binary"
	"errors"
)

// Kafka record batch header offsets
const (
	// baseOffsetOffset is the offset of the baseOffset field (8 bytes at position 0).
	baseOffsetOffset = 0
	// baseOffsetSize is the size of the baseOffset field.
	baseOffsetSize = 8

	// minBatchSize is the minimum valid Kafka record batch size.
	// Must include at least: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) +
	// magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) + firstTimestamp(8) +
	// maxTimestamp(8) + producerId(8) + producerEpoch(2) + firstSequence(4) + recordCount(4)
	minBatchSize = 61
)

// ErrBatchTooSmall is returned when a batch is too small to patch.
var ErrBatchTooSmall = errors.New("fetch: batch too small to patch")

// PatchBaseOffset patches the baseOffset field in a Kafka record batch.
// Per spec section 9.5, the baseOffset field is the first 8 bytes of the batch.
// This patching does NOT affect the CRC because the CRC only covers bytes
// starting from attributes (offset 21) per the Kafka protocol spec.
//
// The Kafka v2 record batch format:
//   - baseOffset: int64 (8 bytes) - PATCHED HERE
//   - batchLength: int32 (4 bytes)
//   - partitionLeaderEpoch: int32 (4 bytes)
//   - magic: int8 (1 byte) - must be 2
//   - crc: uint32 (4 bytes) - CRC of everything from attributes onwards
//   - attributes: int16 (2 bytes) - CRC starts here
//   - lastOffsetDelta: int32 (4 bytes)
//   - firstTimestamp: int64 (8 bytes)
//   - maxTimestamp: int64 (8 bytes)
//   - producerId: int64 (8 bytes)
//   - producerEpoch: int16 (2 bytes)
//   - baseSequence: int32 (4 bytes)
//   - recordCount: int32 (4 bytes)
//   - records: [...]
//
// Since the CRC only covers bytes from attributes (offset 21) onwards,
// patching baseOffset does not invalidate the CRC.
func PatchBaseOffset(batch []byte, newBaseOffset int64) error {
	if len(batch) < minBatchSize {
		return ErrBatchTooSmall
	}

	// Write the new baseOffset (big-endian int64)
	binary.BigEndian.PutUint64(batch[baseOffsetOffset:baseOffsetOffset+baseOffsetSize], uint64(newBaseOffset))

	return nil
}

// PatchBatches patches the baseOffset in all batches to reflect assigned offsets.
// Each batch's baseOffset is set based on the startOffset and cumulative record counts.
//
// Parameters:
//   - batches: The raw Kafka record batch bytes to patch (modified in place)
//   - startOffset: The first logical offset assigned to these batches
//
// Returns the total number of records across all batches.
func PatchBatches(batches [][]byte, startOffset int64) (int64, error) {
	currentOffset := startOffset
	var totalRecords int64

	for i, batch := range batches {
		if err := PatchBaseOffset(batch, currentOffset); err != nil {
			return 0, err
		}

		recordCount := GetRecordCount(batch)
		if recordCount == 0 {
			return 0, errors.New("fetch: batch has zero record count")
		}

		currentOffset += int64(recordCount)
		totalRecords += int64(recordCount)
		_ = i // used for error context if needed
	}

	return totalRecords, nil
}

// GetRecordCount extracts the record count from a Kafka record batch.
// The record count is at offset 57 (4 bytes) in the batch header.
func GetRecordCount(batch []byte) int32 {
	if len(batch) < minBatchSize {
		return 0
	}
	return int32(binary.BigEndian.Uint32(batch[57:61]))
}

// GetLastOffsetDelta extracts the lastOffsetDelta from a Kafka record batch.
// This is the offset of the last record relative to baseOffset.
// Located at offset 23 (4 bytes).
func GetLastOffsetDelta(batch []byte) int32 {
	if len(batch) < minBatchSize {
		return 0
	}
	return int32(binary.BigEndian.Uint32(batch[23:27]))
}

// GetBaseOffset extracts the baseOffset from a Kafka record batch.
func GetBaseOffset(batch []byte) int64 {
	if len(batch) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(batch[0:8]))
}
