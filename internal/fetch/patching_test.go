package fetch

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestPatchBaseOffset tests patching the baseOffset field.
func TestPatchBaseOffset(t *testing.T) {
	tests := []struct {
		name          string
		baseOffset    int64
		newBaseOffset int64
	}{
		{
			name:          "patch offset 0 to 100",
			baseOffset:    0,
			newBaseOffset: 100,
		},
		{
			name:          "patch offset 100 to 0",
			baseOffset:    100,
			newBaseOffset: 0,
		},
		{
			name:          "patch to large offset",
			baseOffset:    0,
			newBaseOffset: 9223372036854775807, // max int64
		},
		{
			name:          "patch from large offset",
			baseOffset:    9223372036854775807,
			newBaseOffset: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := buildTestBatch(1, tt.baseOffset)

			err := PatchBaseOffset(batch, tt.newBaseOffset)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := GetBaseOffset(batch)
			if got != tt.newBaseOffset {
				t.Errorf("PatchBaseOffset() got baseOffset=%d, want %d", got, tt.newBaseOffset)
			}
		})
	}
}

// TestPatchBaseOffset_TooSmall tests that small batches are rejected.
func TestPatchBaseOffset_TooSmall(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "empty", size: 0, wantErr: true},
		{name: "7 bytes", size: 7, wantErr: true},
		{name: "8 bytes", size: 8, wantErr: true},
		{name: "60 bytes", size: 60, wantErr: true},
		{name: "61 bytes (min)", size: 61, wantErr: false},
		{name: "100 bytes", size: 100, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := make([]byte, tt.size)
			if tt.size >= 61 {
				// Make it look like a valid batch header
				batch[16] = 2 // magic byte
			}

			err := PatchBaseOffset(batch, 42)
			if (err != nil) != tt.wantErr {
				t.Errorf("PatchBaseOffset() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestPatchBatches tests patching multiple batches.
func TestPatchBatches(t *testing.T) {
	tests := []struct {
		name         string
		recordCounts []int
		startOffset  int64
	}{
		{
			name:         "single batch",
			recordCounts: []int{5},
			startOffset:  0,
		},
		{
			name:         "multiple batches",
			recordCounts: []int{5, 3, 2},
			startOffset:  100,
		},
		{
			name:         "large offsets",
			recordCounts: []int{10, 20, 30},
			startOffset:  9223372036854775700, // near max int64
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batches := make([][]byte, len(tt.recordCounts))
			for i, count := range tt.recordCounts {
				batches[i] = buildTestBatch(count, 0) // All start with offset 0
			}

			totalRecords, err := PatchBatches(batches, tt.startOffset)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify total record count
			var expectedTotal int64
			for _, count := range tt.recordCounts {
				expectedTotal += int64(count)
			}
			if totalRecords != expectedTotal {
				t.Errorf("PatchBatches() totalRecords = %d, want %d", totalRecords, expectedTotal)
			}

			// Verify each batch has correct baseOffset
			currentOffset := tt.startOffset
			for i, batch := range batches {
				got := GetBaseOffset(batch)
				if got != currentOffset {
					t.Errorf("batch %d: baseOffset = %d, want %d", i, got, currentOffset)
				}
				currentOffset += int64(tt.recordCounts[i])
			}
		})
	}
}

// TestGetRecordCount tests extracting record count from batch.
func TestGetRecordCount(t *testing.T) {
	tests := []struct {
		name        string
		recordCount int
	}{
		{name: "1 record", recordCount: 1},
		{name: "10 records", recordCount: 10},
		{name: "100 records", recordCount: 100},
		{name: "max records", recordCount: 2147483647}, // max int32
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := buildTestBatch(tt.recordCount, 0)

			got := GetRecordCount(batch)
			if got != int32(tt.recordCount) {
				t.Errorf("GetRecordCount() = %d, want %d", got, tt.recordCount)
			}
		})
	}
}

// TestGetRecordCount_TooSmall tests that small batches return 0.
func TestGetRecordCount_TooSmall(t *testing.T) {
	small := make([]byte, 50)
	if GetRecordCount(small) != 0 {
		t.Error("expected 0 for small batch")
	}
}

// TestGetLastOffsetDelta tests extracting lastOffsetDelta from batch.
func TestGetLastOffsetDelta(t *testing.T) {
	tests := []struct {
		name        string
		recordCount int
		wantDelta   int32
	}{
		{name: "1 record", recordCount: 1, wantDelta: 0},    // last record is at offset 0
		{name: "5 records", recordCount: 5, wantDelta: 4},   // last record is at offset 4
		{name: "100 records", recordCount: 100, wantDelta: 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := buildTestBatch(tt.recordCount, 0)

			got := GetLastOffsetDelta(batch)
			if got != tt.wantDelta {
				t.Errorf("GetLastOffsetDelta() = %d, want %d", got, tt.wantDelta)
			}
		})
	}
}

// buildTestBatch creates a minimal valid Kafka record batch for testing.
// This version does NOT set a valid CRC (legacy behavior for basic tests).
func buildTestBatch(recordCount int, baseOffset int64) []byte {
	batch := make([]byte, 80)

	// baseOffset (8 bytes)
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))

	// batchLength (4 bytes) - minimum 49 for header fields after batchLength
	binary.BigEndian.PutUint32(batch[8:12], 49)

	// partitionLeaderEpoch (4 bytes)
	binary.BigEndian.PutUint32(batch[12:16], 0)

	// magic (1 byte) = 2
	batch[16] = 2

	// crc (4 bytes) - placeholder
	binary.BigEndian.PutUint32(batch[17:21], 0)

	// attributes (2 bytes)
	binary.BigEndian.PutUint16(batch[21:23], 0)

	// lastOffsetDelta (4 bytes) = recordCount - 1
	binary.BigEndian.PutUint32(batch[23:27], uint32(recordCount-1))

	// firstTimestamp (8 bytes)
	binary.BigEndian.PutUint64(batch[27:35], 0)

	// maxTimestamp (8 bytes)
	binary.BigEndian.PutUint64(batch[35:43], 0)

	// producerId (8 bytes) = -1
	binary.BigEndian.PutUint64(batch[43:51], 0xFFFFFFFFFFFFFFFF)

	// producerEpoch (2 bytes) = -1
	binary.BigEndian.PutUint16(batch[51:53], 0xFFFF)

	// firstSequence (4 bytes) = -1
	binary.BigEndian.PutUint32(batch[53:57], 0xFFFFFFFF)

	// recordCount (4 bytes)
	binary.BigEndian.PutUint32(batch[57:61], uint32(recordCount))

	return batch
}

// buildTestBatchWithValidCRC creates a Kafka record batch with a valid CRC.
// This is needed for tests that verify CRC behavior.
func buildTestBatchWithValidCRC(recordCount int, baseOffset int64) []byte {
	return buildTestBatchWithRecords(recordCount, baseOffset, CompressionNone, nil, true)
}

// buildTestBatchWithCompression creates a Kafka record batch with a specific
// compression type and valid CRC.
func buildTestBatchWithCompression(recordCount int, baseOffset int64, compressionType int) []byte {
	return buildTestBatchWithRecords(recordCount, baseOffset, compressionType, nil, true)
}

func buildTestBatchWithRecords(recordCount int, baseOffset int64, compressionType int, offsetDeltas []int32, setCRC bool) []byte {
	recordsData := buildTestRecords(recordCount, offsetDeltas)
	if compressionType > 0 && compressionType <= CompressionZstd {
		compressed, err := compressRecords(recordsData, compressionType)
		if err != nil {
			panic(err)
		}
		recordsData = compressed
	}

	lastOffsetDelta := int32(-1)
	if recordCount > 0 {
		lastOffsetDelta = int32(recordCount - 1)
	}

	recordBatch := kmsg.RecordBatch{
		FirstOffset:          baseOffset,
		Length:               int32(49 + len(recordsData)),
		PartitionLeaderEpoch: 0,
		Magic:                2,
		CRC:                  0,
		Attributes:           int16(compressionType & 0x07),
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       0,
		MaxTimestamp:         0,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(recordCount),
		Records:              recordsData,
	}

	batch := recordBatch.AppendTo(nil)
	if setCRC {
		crcValue := CalculateCRC(batch)
		binary.BigEndian.PutUint32(batch[17:21], crcValue)
	}

	return batch
}

func buildTestRecords(recordCount int, offsetDeltas []int32) []byte {
	if recordCount == 0 {
		return nil
	}

	var buf bytes.Buffer
	for i := 0; i < recordCount; i++ {
		offsetDelta := int64(i)
		if offsetDeltas != nil {
			offsetDelta = int64(offsetDeltas[i])
		}
		writeTestRecord(&buf, offsetDelta)
	}
	return buf.Bytes()
}

func compressRecords(data []byte, compressionType int) ([]byte, error) {
	var codec kgo.CompressionCodec
	switch compressionType {
	case CompressionGzip:
		codec = kgo.GzipCompression()
	case CompressionSnappy:
		codec = kgo.SnappyCompression()
	case CompressionLz4:
		codec = kgo.Lz4Compression()
	case CompressionZstd:
		codec = kgo.ZstdCompression()
	default:
		return data, nil
	}

	compressor, err := kgo.DefaultCompressor(codec)
	if err != nil {
		return nil, err
	}
	if compressor == nil {
		return data, nil
	}

	var buf bytes.Buffer
	compressed, _ := compressor.Compress(&buf, data)
	return compressed, nil
}

// writeTestRecord writes a single Kafka v2 record.
func writeTestRecord(w *bytes.Buffer, offsetDelta int64) {
	var body bytes.Buffer

	body.WriteByte(0)
	writeVarint(&body, 0)
	writeVarint(&body, offsetDelta)
	writeVarint(&body, -1)
	writeVarint(&body, -1)
	writeVarint(&body, 0)

	bodyBytes := body.Bytes()
	writeVarint(w, int64(len(bodyBytes)))
	w.Write(bodyBytes)
}

// writeVarint writes a zigzag-encoded signed varint.
func writeVarint(w *bytes.Buffer, v int64) {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		w.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	w.WriteByte(byte(uv))
}

func buildFranzBatchWithCompression(t *testing.T, recordCount int, baseOffset int64, compressionType int) []byte {
	t.Helper()

	records := buildFranzRecords(recordCount)
	compressed := records

	if compressionType != CompressionNone {
		codec, err := compressionCodecForType(compressionType)
		if err != nil {
			t.Fatalf("compression codec error: %v", err)
		}
		compressor, err := kgo.DefaultCompressor(codec)
		if err != nil {
			t.Fatalf("failed to create compressor: %v", err)
		}
		var buf bytes.Buffer
		compressed, _ = compressor.Compress(&buf, records)
	}

	lastOffsetDelta := int32(-1)
	if recordCount > 0 {
		lastOffsetDelta = int32(recordCount - 1)
	}

	ts := int64(1234567890)
	recordBatch := kmsg.RecordBatch{
		FirstOffset:          baseOffset,
		Length:               int32(49 + len(compressed)),
		PartitionLeaderEpoch: 0,
		Magic:                2,
		CRC:                  0,
		Attributes:           int16(compressionType & 0x07),
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       ts,
		MaxTimestamp:         ts,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(recordCount),
		Records:              compressed,
	}

	batch := recordBatch.AppendTo(nil)
	crcValue := CalculateCRC(batch)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

func buildFranzRecords(recordCount int) []byte {
	var records []byte
	for i := 0; i < recordCount; i++ {
		var body []byte
		body = kbin.AppendInt8(body, 0)
		body = kbin.AppendVarlong(body, 0)
		body = kbin.AppendVarint(body, int32(i))
		body = kbin.AppendVarintBytes(body, []byte("k"))
		body = kbin.AppendVarintBytes(body, []byte("v"))
		body = kbin.AppendVarint(body, 0)
		records = kbin.AppendVarint(records, int32(len(body)))
		records = append(records, body...)
	}
	return records
}

func compressionCodecForType(compressionType int) (kgo.CompressionCodec, error) {
	switch compressionType {
	case CompressionGzip:
		return kgo.GzipCompression(), nil
	case CompressionSnappy:
		return kgo.SnappyCompression(), nil
	case CompressionLz4:
		return kgo.Lz4Compression(), nil
	case CompressionZstd:
		return kgo.ZstdCompression(), nil
	default:
		return kgo.NoCompression(), fmt.Errorf("unsupported compression type %d", compressionType)
	}
}

// TestCRCVerification tests that CRC verification works correctly.
func TestCRCVerification(t *testing.T) {
	t.Run("valid CRC passes verification", func(t *testing.T) {
		batch := buildTestBatchWithValidCRC(5, 0)

		err := VerifyCRC(batch)
		if err != nil {
			t.Errorf("VerifyCRC() unexpected error: %v", err)
		}
	})

	t.Run("invalid CRC fails verification", func(t *testing.T) {
		batch := buildTestBatch(5, 0) // No valid CRC

		err := VerifyCRC(batch)
		if err == nil {
			t.Error("VerifyCRC() expected error for invalid CRC")
		}
	})

	t.Run("small batch fails verification", func(t *testing.T) {
		batch := make([]byte, 20)

		err := VerifyCRC(batch)
		if err == nil {
			t.Error("VerifyCRC() expected error for small batch")
		}
	})
}

// TestPatchingDoesNotAffectCRC verifies that patching baseOffset doesn't change CRC.
// Per spec 9.5, baseOffset is outside the CRC region (which starts at offset 21).
func TestPatchingDoesNotAffectCRC(t *testing.T) {
	tests := []struct {
		name          string
		recordCount   int
		initialOffset int64
		newOffset     int64
	}{
		{
			name:          "patch from 0 to 100",
			recordCount:   5,
			initialOffset: 0,
			newOffset:     100,
		},
		{
			name:          "patch from 100 to 0",
			recordCount:   10,
			initialOffset: 100,
			newOffset:     0,
		},
		{
			name:          "patch to max offset",
			recordCount:   1,
			initialOffset: 0,
			newOffset:     9223372036854775807,
		},
		{
			name:          "patch from max offset",
			recordCount:   3,
			initialOffset: 9223372036854775807,
			newOffset:     42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := buildTestBatchWithValidCRC(tt.recordCount, tt.initialOffset)

			// Verify CRC is valid before patching
			if err := VerifyCRC(batch); err != nil {
				t.Fatalf("pre-patch CRC verification failed: %v", err)
			}

			// Get CRC before patching
			crcBefore := GetCRC(batch)

			// Patch the baseOffset
			if err := PatchBaseOffset(batch, tt.newOffset); err != nil {
				t.Fatalf("PatchBaseOffset() failed: %v", err)
			}

			// Verify baseOffset was patched
			if got := GetBaseOffset(batch); got != tt.newOffset {
				t.Errorf("baseOffset = %d, want %d", got, tt.newOffset)
			}

			// Get CRC after patching
			crcAfter := GetCRC(batch)

			// CRC should be unchanged
			if crcBefore != crcAfter {
				t.Errorf("CRC changed after patching: before=0x%08x, after=0x%08x",
					crcBefore, crcAfter)
			}

			// CRC should still be valid
			if err := VerifyCRC(batch); err != nil {
				t.Errorf("post-patch CRC verification failed: %v", err)
			}
		})
	}
}

// TestValidateOffsetDeltas tests offset delta validation.
func TestValidateOffsetDeltas(t *testing.T) {
	t.Run("valid deltas pass", func(t *testing.T) {
		tests := []struct {
			name        string
			recordCount int
		}{
			{"1 record", 1},
			{"5 records", 5},
			{"100 records", 100},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				batch := buildTestBatchWithValidCRC(tt.recordCount, 0)

				err := ValidateOffsetDeltas(batch)
				if err != nil {
					t.Errorf("ValidateOffsetDeltas() unexpected error: %v", err)
				}
			})
		}
	})

	t.Run("invalid deltas fail", func(t *testing.T) {
		// Create a batch with mismatched lastOffsetDelta
		batch := buildTestBatchWithValidCRC(5, 0)

		// Corrupt the lastOffsetDelta (should be 4 for 5 records)
		binary.BigEndian.PutUint32(batch[23:27], 10) // Set to 10 instead of 4

		err := ValidateOffsetDeltas(batch)
		if err == nil {
			t.Error("ValidateOffsetDeltas() expected error for invalid deltas")
		}
	})

	t.Run("invalid record delta sequence fails", func(t *testing.T) {
		batch := buildTestBatchWithRecords(3, 0, CompressionNone, []int32{0, 2, 2}, true)

		err := ValidateOffsetDeltas(batch)
		if err == nil {
			t.Error("ValidateOffsetDeltas() expected error for invalid record delta sequence")
		}
	})

	t.Run("small batch fails", func(t *testing.T) {
		batch := make([]byte, 20)

		err := ValidateOffsetDeltas(batch)
		if err == nil {
			t.Error("ValidateOffsetDeltas() expected error for small batch")
		}
	})
}

// TestValidateCompression tests compression type validation.
func TestValidateCompression(t *testing.T) {
	t.Run("valid compression types pass", func(t *testing.T) {
		compressionTypes := []struct {
			name string
			typ  int
		}{
			{"none", CompressionNone},
			{"gzip", CompressionGzip},
			{"snappy", CompressionSnappy},
			{"lz4", CompressionLz4},
			{"zstd", CompressionZstd},
		}

		for _, tc := range compressionTypes {
			t.Run(tc.name, func(t *testing.T) {
				batch := buildTestBatchWithCompression(5, 0, tc.typ)

				err := ValidateCompression(batch)
				if err != nil {
					t.Errorf("ValidateCompression() unexpected error for %s: %v", tc.name, err)
				}
			})
		}
	})

	t.Run("invalid compression types fail", func(t *testing.T) {
		invalidTypes := []int{5, 6, 7} // Values above zstd (4) are invalid

		for _, typ := range invalidTypes {
			t.Run(fmt.Sprintf("type_%d", typ), func(t *testing.T) {
				batch := buildTestBatchWithCompression(5, 0, typ)

				err := ValidateCompression(batch)
				if err == nil {
					t.Errorf("ValidateCompression() expected error for compression type %d", typ)
				}
			})
		}
	})

	t.Run("small batch fails", func(t *testing.T) {
		batch := make([]byte, 20)

		err := ValidateCompression(batch)
		if err == nil {
			t.Error("ValidateCompression() expected error for small batch")
		}
	})
}

// TestPatchAndValidate_FranzCompressedBatches verifies real franz-go compressed batches validate.
func TestPatchAndValidate_FranzCompressedBatches(t *testing.T) {
	compressionTypes := []struct {
		name string
		typ  int
	}{
		{"gzip", CompressionGzip},
		{"snappy", CompressionSnappy},
		{"lz4", CompressionLz4},
		{"zstd", CompressionZstd},
	}

	for _, tc := range compressionTypes {
		t.Run(tc.name, func(t *testing.T) {
			batch := buildFranzBatchWithCompression(t, 3, 0, tc.typ)

			if err := PatchAndValidate(batch, 100); err != nil {
				t.Fatalf("PatchAndValidate() unexpected error for %s: %v", tc.name, err)
			}
		})
	}

	t.Run("corrupted compressed payload fails", func(t *testing.T) {
		for _, tc := range compressionTypes {
			t.Run(tc.name, func(t *testing.T) {
				batch := buildFranzBatchWithCompression(t, 3, 0, tc.typ)
				recordsOffset := 61
				if len(batch) <= recordsOffset {
					t.Fatalf("expected records payload, got batch length %d", len(batch))
				}
				batch[recordsOffset] ^= 0xFF
				binary.BigEndian.PutUint32(batch[17:21], CalculateCRC(batch))

				err := PatchAndValidate(batch, 100)
				if err == nil {
					t.Fatalf("expected error for corrupted %s payload", tc.name)
				}
				if !errors.Is(err, ErrInvalidCompression) {
					t.Fatalf("expected ErrInvalidCompression, got %v", err)
				}
			})
		}
	})
}

// TestGetCompressionType tests extracting compression type from batch.
func TestGetCompressionType(t *testing.T) {
	tests := []struct {
		name            string
		compressionType int
	}{
		{"none", CompressionNone},
		{"gzip", CompressionGzip},
		{"snappy", CompressionSnappy},
		{"lz4", CompressionLz4},
		{"zstd", CompressionZstd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := buildTestBatchWithCompression(5, 0, tt.compressionType)

			got := GetCompressionType(batch)
			if got != tt.compressionType {
				t.Errorf("GetCompressionType() = %d, want %d", got, tt.compressionType)
			}
		})
	}
}

// TestPatchAndValidate tests the combined patch and validation function.
func TestPatchAndValidate(t *testing.T) {
	t.Run("valid batch patches successfully", func(t *testing.T) {
		batch := buildTestBatchWithValidCRC(5, 0)

		err := PatchAndValidate(batch, 100)
		if err != nil {
			t.Errorf("PatchAndValidate() unexpected error: %v", err)
		}

		if got := GetBaseOffset(batch); got != 100 {
			t.Errorf("baseOffset = %d, want 100", got)
		}
	})

	t.Run("invalid CRC fails", func(t *testing.T) {
		batch := buildTestBatch(5, 0) // No valid CRC

		err := PatchAndValidate(batch, 100)
		if err == nil {
			t.Error("PatchAndValidate() expected error for invalid CRC")
		}
	})

	t.Run("invalid compression type fails", func(t *testing.T) {
		batch := buildTestBatchWithCompression(5, 0, 7) // Invalid compression type

		err := PatchAndValidate(batch, 100)
		if err == nil {
			t.Error("PatchAndValidate() expected error for invalid compression type")
		}
	})

	t.Run("corrupted compressed payload fails", func(t *testing.T) {
		batch := buildTestBatchWithCompression(3, 0, CompressionGzip)

		recordsOffset := 61
		if len(batch) <= recordsOffset {
			t.Fatalf("expected records payload, got batch length %d", len(batch))
		}
		batch[recordsOffset] ^= 0xFF
		binary.BigEndian.PutUint32(batch[17:21], CalculateCRC(batch))

		err := PatchAndValidate(batch, 100)
		if err == nil {
			t.Error("PatchAndValidate() expected error for corrupted compressed payload")
		}
	})

	t.Run("invalid offset deltas fail", func(t *testing.T) {
		batch := buildTestBatchWithValidCRC(5, 0)
		// Corrupt the lastOffsetDelta
		binary.BigEndian.PutUint32(batch[23:27], 10)
		// Recalculate CRC to avoid CRC error
		table := crc32.MakeTable(crc32.Castagnoli)
		crcValue := crc32.Checksum(batch[21:], table)
		binary.BigEndian.PutUint32(batch[17:21], crcValue)

		err := PatchAndValidate(batch, 100)
		if err == nil {
			t.Error("PatchAndValidate() expected error for invalid offset deltas")
		}
	})
}

// TestPatchBatchesWithValidation tests batch-level validation and patching.
func TestPatchBatchesWithValidation(t *testing.T) {
	t.Run("valid batches patch successfully", func(t *testing.T) {
		batches := [][]byte{
			buildTestBatchWithValidCRC(5, 0),
			buildTestBatchWithValidCRC(3, 0),
			buildTestBatchWithValidCRC(2, 0),
		}

		totalRecords, err := PatchBatchesWithValidation(batches, 100)
		if err != nil {
			t.Fatalf("PatchBatchesWithValidation() unexpected error: %v", err)
		}

		if totalRecords != 10 {
			t.Errorf("totalRecords = %d, want 10", totalRecords)
		}

		// Verify offsets
		expectedOffsets := []int64{100, 105, 108}
		for i, batch := range batches {
			if got := GetBaseOffset(batch); got != expectedOffsets[i] {
				t.Errorf("batch %d: baseOffset = %d, want %d", i, got, expectedOffsets[i])
			}
		}
	})

	t.Run("invalid batch fails", func(t *testing.T) {
		batches := [][]byte{
			buildTestBatchWithValidCRC(5, 0),
			buildTestBatch(3, 0), // No valid CRC
		}

		_, err := PatchBatchesWithValidation(batches, 100)
		if err == nil {
			t.Error("PatchBatchesWithValidation() expected error for invalid batch")
		}
	})
}

// TestRoundTripOffsets tests that offsets are correctly assigned in a round-trip scenario.
// This simulates: produce (offset=0) -> WAL -> fetch (patched to assigned offset)
func TestRoundTripOffsets(t *testing.T) {
	t.Run("single batch round-trip", func(t *testing.T) {
		// Simulate producing a batch with placeholder offset 0
		batch := buildTestBatchWithValidCRC(5, 0)

		// Index assigns offsets starting at 100
		assignedStartOffset := int64(100)

		// Fetch path patches the offset
		err := PatchAndValidate(batch, assignedStartOffset)
		if err != nil {
			t.Fatalf("PatchAndValidate() failed: %v", err)
		}

		// Verify the batch now has correct offsets
		if got := GetBaseOffset(batch); got != assignedStartOffset {
			t.Errorf("baseOffset = %d, want %d", got, assignedStartOffset)
		}

		// Records should have offsets 100, 101, 102, 103, 104
		recordCount := GetRecordCount(batch)
		lastOffsetDelta := GetLastOffsetDelta(batch)

		expectedLastOffset := assignedStartOffset + int64(lastOffsetDelta)
		if expectedLastOffset != 104 {
			t.Errorf("last offset = %d, want 104", expectedLastOffset)
		}

		if recordCount != 5 {
			t.Errorf("recordCount = %d, want 5", recordCount)
		}
	})

	t.Run("multiple batches round-trip", func(t *testing.T) {
		// Simulate producing multiple batches
		batches := [][]byte{
			buildTestBatchWithValidCRC(5, 0),  // 5 records
			buildTestBatchWithValidCRC(3, 0),  // 3 records
			buildTestBatchWithValidCRC(2, 0),  // 2 records
		}

		// Index assigns offsets starting at 1000
		assignedStartOffset := int64(1000)

		// Fetch path patches all batches
		totalRecords, err := PatchBatchesWithValidation(batches, assignedStartOffset)
		if err != nil {
			t.Fatalf("PatchBatchesWithValidation() failed: %v", err)
		}

		if totalRecords != 10 {
			t.Errorf("totalRecords = %d, want 10", totalRecords)
		}

		// Verify offset assignments:
		// Batch 0: baseOffset=1000, records at 1000-1004
		// Batch 1: baseOffset=1005, records at 1005-1007
		// Batch 2: baseOffset=1008, records at 1008-1009
		expectedBaseOffsets := []int64{1000, 1005, 1008}
		expectedLastOffsets := []int64{1004, 1007, 1009}

		for i, batch := range batches {
			baseOffset := GetBaseOffset(batch)
			if baseOffset != expectedBaseOffsets[i] {
				t.Errorf("batch %d: baseOffset = %d, want %d",
					i, baseOffset, expectedBaseOffsets[i])
			}

			lastOffsetDelta := GetLastOffsetDelta(batch)
			lastOffset := baseOffset + int64(lastOffsetDelta)
			if lastOffset != expectedLastOffsets[i] {
				t.Errorf("batch %d: lastOffset = %d, want %d",
					i, lastOffset, expectedLastOffsets[i])
			}
		}
	})

	t.Run("offsets are monotonically increasing", func(t *testing.T) {
		// Create batches with various sizes
		recordCounts := []int{1, 5, 3, 10, 2}
		batches := make([][]byte, len(recordCounts))
		for i, count := range recordCounts {
			batches[i] = buildTestBatchWithValidCRC(count, 0)
		}

		assignedStartOffset := int64(0)
		_, err := PatchBatchesWithValidation(batches, assignedStartOffset)
		if err != nil {
			t.Fatalf("PatchBatchesWithValidation() failed: %v", err)
		}

		// Verify all offsets are monotonically increasing
		var prevEnd int64 = -1
		for i, batch := range batches {
			baseOffset := GetBaseOffset(batch)
			recordCount := int64(GetRecordCount(batch))
			endOffset := baseOffset + recordCount - 1

			if baseOffset <= prevEnd {
				t.Errorf("batch %d: baseOffset %d not greater than previous end %d",
					i, baseOffset, prevEnd)
			}

			prevEnd = endOffset
		}
	})
}
