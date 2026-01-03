package catalog

import (
	"encoding/binary"
	"testing"
)

// TestDataFileStatsIntegration verifies that DataFile stats can be populated
// using the helper functions, matching real compaction workflow.
func TestDataFileStatsIntegration(t *testing.T) {
	// Simulate a compaction workflow:
	// 1. Parquet writer produces FileStats with min/max offset/timestamp
	// 2. BuildDataFileFromStats converts to DataFile with Iceberg-compatible bounds
	// 3. DataFile is ready for Iceberg commit

	path := "s3://bucket/compaction/v1/topic=test/partition=0/date=2024/01/15/abc123.parquet"
	var partition int32 = 0
	var minOffset int64 = 1000
	var maxOffset int64 = 1999
	var minTs int64 = 1700000000000
	var maxTs int64 = 1700001000000
	var recordCount int64 = 1000
	var fileSizeBytes int64 = 128 * 1024

	// Step 1: Create stats using helper
	stats := DefaultDataFileStats(partition, minOffset, maxOffset, minTs, maxTs, recordCount)

	// Step 2: Build DataFile with stats
	df := BuildDataFileFromStats(path, partition, recordCount, fileSizeBytes, stats)

	// Step 3: Verify all fields are correctly populated
	if df.Path != path {
		t.Errorf("Path mismatch: expected %s, got %s", path, df.Path)
	}
	if df.RecordCount != recordCount {
		t.Errorf("RecordCount mismatch: expected %d, got %d", recordCount, df.RecordCount)
	}
	if df.FileSizeBytes != fileSizeBytes {
		t.Errorf("FileSizeBytes mismatch: expected %d, got %d", fileSizeBytes, df.FileSizeBytes)
	}
	if df.PartitionValue != partition {
		t.Errorf("PartitionValue mismatch: expected %d, got %d", partition, df.PartitionValue)
	}

	// Verify bounds are set for the right field IDs
	boundFields := []int{FieldIDPartition, FieldIDOffset, FieldIDTimestampMs}
	for _, fieldID := range boundFields {
		if df.LowerBounds[fieldID] == nil {
			t.Errorf("LowerBounds[%d] is nil", fieldID)
		}
		if df.UpperBounds[fieldID] == nil {
			t.Errorf("UpperBounds[%d] is nil", fieldID)
		}
	}

	// Verify offset bounds decode correctly
	gotMinOffset := int64(binary.BigEndian.Uint64(df.LowerBounds[FieldIDOffset]))
	gotMaxOffset := int64(binary.BigEndian.Uint64(df.UpperBounds[FieldIDOffset]))
	if gotMinOffset != minOffset || gotMaxOffset != maxOffset {
		t.Errorf("Offset bounds mismatch: expected [%d,%d], got [%d,%d]",
			minOffset, maxOffset, gotMinOffset, gotMaxOffset)
	}

	// Verify timestamp bounds decode correctly
	gotMinTs := int64(binary.BigEndian.Uint64(df.LowerBounds[FieldIDTimestampMs]))
	gotMaxTs := int64(binary.BigEndian.Uint64(df.UpperBounds[FieldIDTimestampMs]))
	if gotMinTs != minTs || gotMaxTs != maxTs {
		t.Errorf("Timestamp bounds mismatch: expected [%d,%d], got [%d,%d]",
			minTs, maxTs, gotMinTs, gotMaxTs)
	}

	// Verify value counts
	for _, fieldID := range boundFields {
		if df.ValueCounts[fieldID] != recordCount {
			t.Errorf("ValueCounts[%d] mismatch: expected %d, got %d",
				fieldID, recordCount, df.ValueCounts[fieldID])
		}
	}
}

func TestBuildDataFileFromStats_Basic(t *testing.T) {
	path := "s3://bucket/test.parquet"
	var partition int32 = 5
	var recordCount int64 = 100
	var fileSizeBytes int64 = 4096

	df := BuildDataFileFromStats(path, partition, recordCount, fileSizeBytes, nil)

	if df.Path != path {
		t.Errorf("expected Path=%s, got %s", path, df.Path)
	}
	if df.RecordCount != recordCount {
		t.Errorf("expected RecordCount=%d, got %d", recordCount, df.RecordCount)
	}
	if df.FileSizeBytes != fileSizeBytes {
		t.Errorf("expected FileSizeBytes=%d, got %d", fileSizeBytes, df.FileSizeBytes)
	}
	if df.PartitionValue != partition {
		t.Errorf("expected PartitionValue=%d, got %d", partition, df.PartitionValue)
	}
}

func TestBuildDataFileFromStats_WithStats(t *testing.T) {
	path := "s3://bucket/test.parquet"
	var partition int32 = 0
	var minOffset int64 = 100
	var maxOffset int64 = 199
	var minTs int64 = 1700000000000
	var maxTs int64 = 1700000001000
	var recordCount int64 = 100
	var fileSizeBytes int64 = 8192

	stats := DefaultDataFileStats(partition, minOffset, maxOffset, minTs, maxTs, recordCount)

	df := BuildDataFileFromStats(path, partition, recordCount, fileSizeBytes, stats)

	if df.Path != path {
		t.Errorf("expected Path=%s, got %s", path, df.Path)
	}
	if df.RecordCount != recordCount {
		t.Errorf("expected RecordCount=%d, got %d", recordCount, df.RecordCount)
	}
	if df.FileSizeBytes != fileSizeBytes {
		t.Errorf("expected FileSizeBytes=%d, got %d", fileSizeBytes, df.FileSizeBytes)
	}
	if df.PartitionValue != partition {
		t.Errorf("expected PartitionValue=%d, got %d", partition, df.PartitionValue)
	}

	// Verify LowerBounds
	if df.LowerBounds == nil {
		t.Fatal("expected LowerBounds to be non-nil")
	}
	if len(df.LowerBounds) != 3 {
		t.Errorf("expected 3 LowerBounds entries, got %d", len(df.LowerBounds))
	}
	// Check offset lower bound
	offsetLower := df.LowerBounds[FieldIDOffset]
	if offsetLower == nil {
		t.Fatal("expected offset lower bound")
	}
	gotMinOffset := int64(binary.BigEndian.Uint64(offsetLower))
	if gotMinOffset != minOffset {
		t.Errorf("expected minOffset=%d in LowerBounds, got %d", minOffset, gotMinOffset)
	}

	// Verify UpperBounds
	if df.UpperBounds == nil {
		t.Fatal("expected UpperBounds to be non-nil")
	}
	if len(df.UpperBounds) != 3 {
		t.Errorf("expected 3 UpperBounds entries, got %d", len(df.UpperBounds))
	}
	// Check offset upper bound
	offsetUpper := df.UpperBounds[FieldIDOffset]
	if offsetUpper == nil {
		t.Fatal("expected offset upper bound")
	}
	gotMaxOffset := int64(binary.BigEndian.Uint64(offsetUpper))
	if gotMaxOffset != maxOffset {
		t.Errorf("expected maxOffset=%d in UpperBounds, got %d", maxOffset, gotMaxOffset)
	}

	// Check timestamp lower bound
	tsLower := df.LowerBounds[FieldIDTimestampMs]
	if tsLower == nil {
		t.Fatal("expected timestamp lower bound")
	}
	gotMinTs := int64(binary.BigEndian.Uint64(tsLower))
	if gotMinTs != minTs {
		t.Errorf("expected minTs=%d in LowerBounds, got %d", minTs, gotMinTs)
	}

	// Check timestamp upper bound
	tsUpper := df.UpperBounds[FieldIDTimestampMs]
	if tsUpper == nil {
		t.Fatal("expected timestamp upper bound")
	}
	gotMaxTs := int64(binary.BigEndian.Uint64(tsUpper))
	if gotMaxTs != maxTs {
		t.Errorf("expected maxTs=%d in UpperBounds, got %d", maxTs, gotMaxTs)
	}

	// Verify ValueCounts
	if df.ValueCounts == nil {
		t.Fatal("expected ValueCounts to be non-nil")
	}
	if len(df.ValueCounts) != 3 {
		t.Errorf("expected 3 ValueCounts entries, got %d", len(df.ValueCounts))
	}
	if df.ValueCounts[FieldIDOffset] != recordCount {
		t.Errorf("expected ValueCounts[offset]=%d, got %d", recordCount, df.ValueCounts[FieldIDOffset])
	}
	if df.ValueCounts[FieldIDTimestampMs] != recordCount {
		t.Errorf("expected ValueCounts[timestamp]=%d, got %d", recordCount, df.ValueCounts[FieldIDTimestampMs])
	}
	if df.ValueCounts[FieldIDPartition] != recordCount {
		t.Errorf("expected ValueCounts[partition]=%d, got %d", recordCount, df.ValueCounts[FieldIDPartition])
	}
}

func TestNewOffsetBounds(t *testing.T) {
	minOffset := int64(100)
	maxOffset := int64(200)

	lower, upper := NewOffsetBounds(minOffset, maxOffset)

	gotMin := int64(binary.BigEndian.Uint64(lower))
	gotMax := int64(binary.BigEndian.Uint64(upper))

	if gotMin != minOffset {
		t.Errorf("expected lower bound %d, got %d", minOffset, gotMin)
	}
	if gotMax != maxOffset {
		t.Errorf("expected upper bound %d, got %d", maxOffset, gotMax)
	}
}

func TestNewTimestampBounds(t *testing.T) {
	minTs := int64(1700000000000)
	maxTs := int64(1700000001000)

	lower, upper := NewTimestampBounds(minTs, maxTs)

	gotMin := int64(binary.BigEndian.Uint64(lower))
	gotMax := int64(binary.BigEndian.Uint64(upper))

	if gotMin != minTs {
		t.Errorf("expected lower bound %d, got %d", minTs, gotMin)
	}
	if gotMax != maxTs {
		t.Errorf("expected upper bound %d, got %d", maxTs, gotMax)
	}
}

func TestNewPartitionBound(t *testing.T) {
	partition := int32(42)

	bound := NewPartitionBound(partition)

	got := int32(binary.BigEndian.Uint32(bound))
	if got != partition {
		t.Errorf("expected partition bound %d, got %d", partition, got)
	}
}

func TestDefaultDataFileStats(t *testing.T) {
	var partition int32 = 3
	var minOffset int64 = 0
	var maxOffset int64 = 99
	var minTs int64 = 1700000000000
	var maxTs int64 = 1700000099000
	var recordCount int64 = 100

	stats := DefaultDataFileStats(partition, minOffset, maxOffset, minTs, maxTs, recordCount)

	// Check bounds are set correctly
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if len(stats.LowerBounds) != 3 {
		t.Errorf("expected 3 LowerBounds, got %d", len(stats.LowerBounds))
	}
	if len(stats.UpperBounds) != 3 {
		t.Errorf("expected 3 UpperBounds, got %d", len(stats.UpperBounds))
	}
	if len(stats.ValueCounts) != 3 {
		t.Errorf("expected 3 ValueCounts, got %d", len(stats.ValueCounts))
	}

	// Verify offset bounds
	offsetLower := stats.LowerBounds[FieldIDOffset]
	gotMinOffset := int64(binary.BigEndian.Uint64(offsetLower))
	if gotMinOffset != minOffset {
		t.Errorf("expected minOffset=%d, got %d", minOffset, gotMinOffset)
	}

	// Verify timestamp bounds
	tsUpper := stats.UpperBounds[FieldIDTimestampMs]
	gotMaxTs := int64(binary.BigEndian.Uint64(tsUpper))
	if gotMaxTs != maxTs {
		t.Errorf("expected maxTs=%d, got %d", maxTs, gotMaxTs)
	}

	// Verify partition bound (both lower and upper should be same for identity partition)
	partLower := stats.LowerBounds[FieldIDPartition]
	gotPartition := int32(binary.BigEndian.Uint32(partLower))
	if gotPartition != partition {
		t.Errorf("expected partition=%d, got %d", partition, gotPartition)
	}

	// Verify value counts
	if stats.ValueCounts[FieldIDOffset] != recordCount {
		t.Errorf("expected offset ValueCount=%d, got %d", recordCount, stats.ValueCounts[FieldIDOffset])
	}
}

func TestBuildDataFileFromStats_EmptyStats(t *testing.T) {
	path := "s3://bucket/test.parquet"
	var partition int32 = 0
	var recordCount int64 = 50
	var fileSizeBytes int64 = 2048

	// Pass empty stats (not nil, but empty maps)
	stats := &DataFileStats{
		LowerBounds: map[int32][]byte{},
		UpperBounds: map[int32][]byte{},
	}

	df := BuildDataFileFromStats(path, partition, recordCount, fileSizeBytes, stats)

	if df.Path != path {
		t.Errorf("expected Path=%s, got %s", path, df.Path)
	}
	if df.RecordCount != recordCount {
		t.Errorf("expected RecordCount=%d, got %d", recordCount, df.RecordCount)
	}
	// Empty maps should still be created (not nil)
	if df.LowerBounds == nil {
		t.Error("expected LowerBounds to be non-nil (empty map)")
	}
	if len(df.LowerBounds) != 0 {
		t.Errorf("expected 0 LowerBounds entries, got %d", len(df.LowerBounds))
	}
}
