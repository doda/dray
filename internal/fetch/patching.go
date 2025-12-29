package fetch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka record batch header offsets
const (
	// baseOffsetOffset is the offset of the baseOffset field (8 bytes at position 0).
	baseOffsetOffset = 0
	// baseOffsetSize is the size of the baseOffset field.
	baseOffsetSize = 8

	// crcOffset is the offset of the CRC field (4 bytes at position 17).
	crcOffset = 17
	// crcSize is the size of the CRC field.
	crcSize = 4

	// crcDataOffset is the start of the data covered by the CRC (offset 21, attributes).
	// The CRC covers everything from attributes (offset 21) to the end of the batch.
	crcDataOffset = 21

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

// GetCRC extracts the CRC from a Kafka record batch.
// The CRC is at offset 17 (4 bytes) in the batch header.
func GetCRC(batch []byte) uint32 {
	if len(batch) < crcOffset+crcSize {
		return 0
	}
	return binary.BigEndian.Uint32(batch[crcOffset : crcOffset+crcSize])
}

// CalculateCRC calculates the expected CRC for a Kafka record batch.
// The CRC is calculated over bytes from offset 21 (attributes) to end of batch.
// Uses CRC32C (Castagnoli polynomial) as per Kafka spec.
func CalculateCRC(batch []byte) uint32 {
	if len(batch) < crcDataOffset {
		return 0
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(batch[crcDataOffset:], table)
}

// VerifyCRC verifies that the CRC in the batch matches the calculated CRC.
// Returns nil if valid, error if invalid.
func VerifyCRC(batch []byte) error {
	if len(batch) < minBatchSize {
		return ErrBatchTooSmall
	}

	storedCRC := GetCRC(batch)
	calculatedCRC := CalculateCRC(batch)

	if storedCRC != calculatedCRC {
		return fmt.Errorf("fetch: CRC mismatch: stored=0x%08x, calculated=0x%08x", storedCRC, calculatedCRC)
	}

	return nil
}

// ErrInvalidOffsetDelta is returned when record offset deltas are not sequential.
var ErrInvalidOffsetDelta = errors.New("fetch: invalid offset delta sequence")

// ErrInvalidCompression is returned when the compression type is invalid.
var ErrInvalidCompression = errors.New("fetch: invalid compression type")

// Kafka compression types (bits 0-2 of attributes)
const (
	CompressionNone   = 0
	CompressionGzip   = 1
	CompressionSnappy = 2
	CompressionLz4    = 3
	CompressionZstd   = 4
)

// attributesOffset is the offset of the attributes field (2 bytes at position 21).
const attributesOffset = 21

// GetAttributes extracts the attributes field from a Kafka record batch.
// The attributes field is at offset 21 (2 bytes) in the batch header.
func GetAttributes(batch []byte) int16 {
	if len(batch) < attributesOffset+2 {
		return 0
	}
	return int16(binary.BigEndian.Uint16(batch[attributesOffset : attributesOffset+2]))
}

// GetCompressionType extracts the compression type from a Kafka record batch.
// Compression type is stored in bits 0-2 of the attributes field.
func GetCompressionType(batch []byte) int {
	attrs := GetAttributes(batch)
	return int(attrs & 0x07) // bits 0-2
}

// ValidateCompression validates that the compression type in the batch is valid.
// Per spec 9.5, we must validate compression correctness.
// Valid compression types: 0 (none), 1 (gzip), 2 (snappy), 3 (lz4), 4 (zstd)
func ValidateCompression(batch []byte) error {
	recordBatch, err := parseRecordBatch(batch)
	if err != nil {
		return err
	}

	compression := kgo.CompressionCodecType(recordBatch.Attributes & 0x0007)
	if compression < kgo.CodecNone || compression > kgo.CodecZstd {
		return fmt.Errorf("%w: type=%d", ErrInvalidCompression, compression)
	}

	if compression == kgo.CodecNone {
		return nil
	}

	if _, err := batchDecompressor.Decompress(recordBatch.Records, compression); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCompression, err)
	}

	return nil
}

// ValidateOffsetDeltas validates that record offset deltas are sequential (0, 1, 2, ..., n-1).
// This is a requirement per spec section 9.5.
//
// For a batch with n records:
//   - lastOffsetDelta should be n-1
//   - recordCount should be n
//
// This means the records have offsets: baseOffset+0, baseOffset+1, ..., baseOffset+(n-1)
func ValidateOffsetDeltas(batch []byte) error {
	recordBatch, err := parseRecordBatch(batch)
	if err != nil {
		return err
	}

	rawRecords, err := decompressBatchRecords(recordBatch)
	if err != nil {
		return err
	}

	recordCount := recordBatch.NumRecords
	if recordCount < 0 {
		return fmt.Errorf("fetch: invalid record count %d", recordCount)
	}

	if _, err := parseBatchRecords(rawRecords, recordCount); err != nil {
		return err
	}

	lastOffsetDelta := recordBatch.LastOffsetDelta
	expectedLastDelta := recordCount - 1
	if lastOffsetDelta != expectedLastDelta {
		return fmt.Errorf("%w: lastOffsetDelta=%d, expected=%d (recordCount=%d)",
			ErrInvalidOffsetDelta, lastOffsetDelta, expectedLastDelta, recordCount)
	}

	return nil
}

// PatchAndValidate patches the baseOffset and validates the batch integrity.
// This combines patching with CRC verification, compression validation, and
// offset delta validation per spec section 9.5.
func PatchAndValidate(batch []byte, newBaseOffset int64) error {
	// First validate the batch integrity
	if err := VerifyCRC(batch); err != nil {
		return fmt.Errorf("pre-patch validation: %w", err)
	}

	// Validate compression type is valid (spec 9.5 requirement)
	if err := ValidateCompression(batch); err != nil {
		return fmt.Errorf("compression validation: %w", err)
	}

	if err := ValidateOffsetDeltas(batch); err != nil {
		return fmt.Errorf("offset delta validation: %w", err)
	}

	// Patch the baseOffset
	if err := PatchBaseOffset(batch, newBaseOffset); err != nil {
		return err
	}

	// CRC should still be valid after patching baseOffset
	// (baseOffset is outside the CRC region)
	if err := VerifyCRC(batch); err != nil {
		return fmt.Errorf("post-patch validation: %w", err)
	}

	return nil
}

// PatchBatchesWithValidation patches all batches and validates their integrity.
// Unlike PatchBatches, this also validates CRC, compression, and offset deltas per spec 9.5.
func PatchBatchesWithValidation(batches [][]byte, startOffset int64) (int64, error) {
	currentOffset := startOffset
	var totalRecords int64

	for i, batch := range batches {
		if err := PatchAndValidate(batch, currentOffset); err != nil {
			return 0, fmt.Errorf("batch %d: %w", i, err)
		}

		recordCount := GetRecordCount(batch)
		if recordCount == 0 {
			return 0, fmt.Errorf("batch %d: zero record count", i)
		}

		currentOffset += int64(recordCount)
		totalRecords += int64(recordCount)
	}

	return totalRecords, nil
}

var batchDecompressor = kgo.DefaultDecompressor()

func parseRecordBatch(batch []byte) (*kmsg.RecordBatch, error) {
	if len(batch) < minBatchSize {
		return nil, ErrBatchTooSmall
	}

	var recordBatch kmsg.RecordBatch
	if err := recordBatch.ReadFrom(batch); err != nil {
		return nil, fmt.Errorf("fetch: invalid record batch: %w", err)
	}

	if recordBatch.Magic != 2 {
		return nil, fmt.Errorf("fetch: unsupported record batch magic %d", recordBatch.Magic)
	}

	expectedLength := int32(len(batch) - 12)
	if expectedLength != recordBatch.Length {
		return nil, fmt.Errorf("fetch: batch length mismatch: header=%d actual=%d", recordBatch.Length, expectedLength)
	}

	return &recordBatch, nil
}

func decompressBatchRecords(recordBatch *kmsg.RecordBatch) ([]byte, error) {
	compression := kgo.CompressionCodecType(recordBatch.Attributes & 0x0007)
	if compression < kgo.CodecNone || compression > kgo.CodecZstd {
		return nil, fmt.Errorf("%w: type=%d", ErrInvalidCompression, compression)
	}

	if compression == kgo.CodecNone {
		return recordBatch.Records, nil
	}

	rawRecords, err := batchDecompressor.Decompress(recordBatch.Records, compression)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCompression, err)
	}

	return rawRecords, nil
}

func parseBatchRecords(raw []byte, recordCount int32) ([]kmsg.Record, error) {
	if recordCount == 0 {
		if len(raw) != 0 {
			return nil, fmt.Errorf("fetch: unexpected record bytes for empty batch")
		}
		return nil, nil
	}

	records := make([]kmsg.Record, 0, recordCount)
	remaining := raw
	for i := int32(0); i < recordCount; i++ {
		length, used := readVarint32(remaining)
		if used <= 0 {
			return nil, fmt.Errorf("fetch: truncated record length")
		}
		if length < 0 {
			return nil, fmt.Errorf("fetch: negative record length %d", length)
		}
		total := used + int(length)
		if len(remaining) < total {
			return nil, fmt.Errorf("fetch: truncated record data")
		}

		var rec kmsg.Record
		if err := rec.ReadFrom(remaining[:total]); err != nil {
			return nil, fmt.Errorf("fetch: record parse error: %w", err)
		}
		if rec.OffsetDelta != i {
			return nil, fmt.Errorf("%w: record=%d offsetDelta=%d", ErrInvalidOffsetDelta, i, rec.OffsetDelta)
		}
		records = append(records, rec)
		remaining = remaining[total:]
	}

	if len(remaining) != 0 {
		return nil, fmt.Errorf("fetch: extra record data")
	}

	return records, nil
}

func readVarint32(in []byte) (int32, int) {
	uv, n := binary.Uvarint(in)
	if n <= 0 {
		return 0, n
	}
	if uv > uint64(^uint32(0)) {
		return 0, -1
	}
	return int32((uv >> 1) ^ uint64(-(int64(uv & 1)))), n
}
