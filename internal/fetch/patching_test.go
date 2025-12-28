package fetch

import (
	"encoding/binary"
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
