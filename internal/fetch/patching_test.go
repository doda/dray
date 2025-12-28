package fetch

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
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
	batch := buildTestBatch(recordCount, baseOffset)

	// Calculate and set the CRC over bytes from offset 21 onwards
	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[21:], table)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

// buildTestBatchWithCompression creates a Kafka record batch with a specific
// compression type and valid CRC.
func buildTestBatchWithCompression(recordCount int, baseOffset int64, compressionType int) []byte {
	batch := buildTestBatch(recordCount, baseOffset)

	// Set compression type in attributes field (bits 0-2)
	attrs := int16(compressionType & 0x07)
	binary.BigEndian.PutUint16(batch[21:23], uint16(attrs))

	// Calculate and set the CRC over bytes from offset 21 onwards
	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[21:], table)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
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
