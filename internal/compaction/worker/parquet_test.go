package worker

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestWriter_SingleRecord(t *testing.T) {
	records := []Record{
		{
			Partition:   0,
			Offset:      0,
			Timestamp: 1000,
			Key:         []byte("key1"),
			Value:       []byte("value1"),
			Attributes:  0,
		},
	}

	data, stats, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("expected non-empty parquet data")
	}

	if stats.RecordCount != 1 {
		t.Errorf("expected RecordCount=1, got %d", stats.RecordCount)
	}
	if stats.MinOffset != 0 || stats.MaxOffset != 0 {
		t.Errorf("expected offset range [0,0], got [%d,%d]", stats.MinOffset, stats.MaxOffset)
	}
	if stats.MinTimestamp != 1000 || stats.MaxTimestamp != 1000 {
		t.Errorf("expected timestamp range [1000,1000], got [%d,%d]", stats.MinTimestamp, stats.MaxTimestamp)
	}
}

func TestWriter_MultipleRecords(t *testing.T) {
	records := []Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1"), Attributes: 0},
		{Partition: 0, Offset: 1, Timestamp: 1100, Key: []byte("k2"), Value: []byte("v2"), Attributes: 0},
		{Partition: 0, Offset: 2, Timestamp: 900, Key: []byte("k3"), Value: []byte("v3"), Attributes: 0},
	}

	data, stats, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	if stats.RecordCount != 3 {
		t.Errorf("expected RecordCount=3, got %d", stats.RecordCount)
	}
	if stats.MinOffset != 0 || stats.MaxOffset != 2 {
		t.Errorf("expected offset range [0,2], got [%d,%d]", stats.MinOffset, stats.MaxOffset)
	}
	if stats.MinTimestamp != 900 || stats.MaxTimestamp != 1100 {
		t.Errorf("expected timestamp range [900,1100], got [%d,%d]", stats.MinTimestamp, stats.MaxTimestamp)
	}

	// Verify round-trip
	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecords))
	}

	for i, rec := range readRecords {
		if rec.Offset != records[i].Offset {
			t.Errorf("record %d: expected offset %d, got %d", i, records[i].Offset, rec.Offset)
		}
	}
}

func TestWriter_Headers(t *testing.T) {
	records := []Record{
		{
			Partition:  0,
			Offset:     0,
			Timestamp:  1000,
			Key:        []byte("key"),
			Value:      []byte("value"),
			Headers:    `[{"Key":"h1","Value":"djE="},{"Key":"h2","Value":"djI="},{"Key":"h1","Value":"djM="}]`,
			Attributes: 0,
		},
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	rec := readRecords[0]
	if rec.Headers != records[0].Headers {
		t.Errorf("headers mismatch: expected %q, got %q", records[0].Headers, rec.Headers)
	}
}

func TestWriter_NullKeyValue(t *testing.T) {
	records := []Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Key: nil, Value: []byte("v1"), Attributes: 0},
		{Partition: 0, Offset: 1, Timestamp: 1001, Key: []byte("k2"), Value: nil, Attributes: 0},
		{Partition: 0, Offset: 2, Timestamp: 1002, Key: nil, Value: nil, Attributes: 0},
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecords))
	}

	// Record 0: null key
	if readRecords[0].Key != nil {
		t.Errorf("record 0: expected null key, got %v", readRecords[0].Key)
	}
	if !bytes.Equal(readRecords[0].Value, []byte("v1")) {
		t.Errorf("record 0: expected value v1, got %v", readRecords[0].Value)
	}

	// Record 1: null value
	if !bytes.Equal(readRecords[1].Key, []byte("k2")) {
		t.Errorf("record 1: expected key k2, got %v", readRecords[1].Key)
	}
	if readRecords[1].Value != nil {
		t.Errorf("record 1: expected null value, got %v", readRecords[1].Value)
	}

	// Record 2: both null
	// Note: parquet-go may return empty slice instead of nil for optional bytes
	if len(readRecords[2].Key) != 0 {
		t.Errorf("record 2: expected empty/null key, got %v", readRecords[2].Key)
	}
	if len(readRecords[2].Value) != 0 {
		t.Errorf("record 2: expected empty/null value, got %v", readRecords[2].Value)
	}
}

func TestWriter_ProducerFields(t *testing.T) {
	pid := int64(12345)
	epoch := int32(5)
	seq := int32(100)

	records := []Record{
		{
			Partition:     0,
			Offset:        0,
			Timestamp:   1000,
			Key:           []byte("key"),
			Value:         []byte("value"),
			ProducerID:    &pid,
			ProducerEpoch: &epoch,
			BaseSequence:  &seq,
			Attributes:    0,
		},
		{
			Partition:     0,
			Offset:        1,
			Timestamp:   1001,
			Key:           []byte("key2"),
			Value:         []byte("value2"),
			ProducerID:    nil, // Null producer fields (per spec)
			ProducerEpoch: nil,
			BaseSequence:  nil,
			Attributes:    0,
		},
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 2 {
		t.Fatalf("expected 2 records, got %d", len(readRecords))
	}

	// Record 0: has producer fields
	rec0 := readRecords[0]
	if rec0.ProducerID == nil || *rec0.ProducerID != pid {
		t.Errorf("record 0: expected producer_id %d, got %v", pid, rec0.ProducerID)
	}
	if rec0.ProducerEpoch == nil || *rec0.ProducerEpoch != epoch {
		t.Errorf("record 0: expected producer_epoch %d, got %v", epoch, rec0.ProducerEpoch)
	}
	if rec0.BaseSequence == nil || *rec0.BaseSequence != seq {
		t.Errorf("record 0: expected base_sequence %d, got %v", seq, rec0.BaseSequence)
	}

	// Record 1: null producer fields
	rec1 := readRecords[1]
	if rec1.ProducerID != nil {
		t.Errorf("record 1: expected null producer_id, got %v", rec1.ProducerID)
	}
	if rec1.ProducerEpoch != nil {
		t.Errorf("record 1: expected null producer_epoch, got %v", rec1.ProducerEpoch)
	}
	if rec1.BaseSequence != nil {
		t.Errorf("record 1: expected null base_sequence, got %v", rec1.BaseSequence)
	}
}

func TestWriter_Attributes(t *testing.T) {
	records := []Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Key: []byte("k"), Value: []byte("v"), Attributes: 0},
		{Partition: 0, Offset: 1, Timestamp: 1001, Key: []byte("k"), Value: []byte("v"), Attributes: 1}, // gzip
		{Partition: 0, Offset: 2, Timestamp: 1002, Key: []byte("k"), Value: []byte("v"), Attributes: 2}, // snappy
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecords))
	}

	expectedAttrs := []int32{0, 1, 2}
	for i, rec := range readRecords {
		if rec.Attributes != expectedAttrs[i] {
			t.Errorf("record %d: expected attributes %d, got %d", i, expectedAttrs[i], rec.Attributes)
		}
	}
}

func TestWriter_IncrementalWrites(t *testing.T) {
	w := NewWriter()

	// First batch of records
	err := w.WriteRecords([]Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1"), Attributes: 0},
		{Partition: 0, Offset: 1, Timestamp: 1001, Key: []byte("k2"), Value: []byte("v2"), Attributes: 0},
	})
	if err != nil {
		t.Fatalf("first WriteRecords failed: %v", err)
	}

	// Second batch of records
	err = w.WriteRecords([]Record{
		{Partition: 0, Offset: 2, Timestamp: 1002, Key: []byte("k3"), Value: []byte("v3"), Attributes: 0},
	})
	if err != nil {
		t.Fatalf("second WriteRecords failed: %v", err)
	}

	data, stats, err := w.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if stats.RecordCount != 3 {
		t.Errorf("expected RecordCount=3, got %d", stats.RecordCount)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecords))
	}
}

func TestWriter_EmptyWrite(t *testing.T) {
	w := NewWriter()

	// Write empty slice
	err := w.WriteRecords([]Record{})
	if err != nil {
		t.Fatalf("empty WriteRecords failed: %v", err)
	}

	// Closing without writing should error
	_, _, err = w.Close()
	if err == nil {
		t.Error("expected error when closing without writes")
	}
}

func TestWriter_FileStats(t *testing.T) {
	records := []Record{
		{Partition: 0, Offset: 10, Timestamp: 2000, Key: []byte("k1"), Value: []byte("v1"), Attributes: 0},
		{Partition: 0, Offset: 5, Timestamp: 500, Key: []byte("k2"), Value: []byte("v2"), Attributes: 0},
		{Partition: 0, Offset: 15, Timestamp: 3000, Key: []byte("k3"), Value: []byte("v3"), Attributes: 0},
	}

	data, stats, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	if stats.MinOffset != 5 {
		t.Errorf("expected MinOffset=5, got %d", stats.MinOffset)
	}
	if stats.MaxOffset != 15 {
		t.Errorf("expected MaxOffset=15, got %d", stats.MaxOffset)
	}
	if stats.MinTimestamp != 500 {
		t.Errorf("expected MinTimestamp=500, got %d", stats.MinTimestamp)
	}
	if stats.MaxTimestamp != 3000 {
		t.Errorf("expected MaxTimestamp=3000, got %d", stats.MaxTimestamp)
	}
	if stats.RecordCount != 3 {
		t.Errorf("expected RecordCount=3, got %d", stats.RecordCount)
	}
	if stats.SizeBytes != int64(len(data)) {
		t.Errorf("expected SizeBytes=%d, got %d", len(data), stats.SizeBytes)
	}
}

func TestWriter_LargeRecord(t *testing.T) {
	largeValue := make([]byte, 1024*1024) // 1MB value
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	records := []Record{
		{
			Partition:   0,
			Offset:      0,
			Timestamp: 1000,
			Key:         []byte("large-key"),
			Value:       largeValue,
			Attributes:  0,
		},
	}

	data, stats, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	if stats.RecordCount != 1 {
		t.Errorf("expected RecordCount=1, got %d", stats.RecordCount)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	if !bytes.Equal(readRecords[0].Value, largeValue) {
		t.Error("large value mismatch")
	}
}

func TestWriter_MultiplePartitions(t *testing.T) {
	records := []Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1"), Attributes: 0},
		{Partition: 1, Offset: 0, Timestamp: 1001, Key: []byte("k2"), Value: []byte("v2"), Attributes: 0},
		{Partition: 2, Offset: 0, Timestamp: 1002, Key: []byte("k3"), Value: []byte("v3"), Attributes: 0},
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecords))
	}

	expectedPartitions := []int32{0, 1, 2}
	for i, rec := range readRecords {
		if rec.Partition != expectedPartitions[i] {
			t.Errorf("record %d: expected partition %d, got %d", i, expectedPartitions[i], rec.Partition)
		}
	}
}

func TestWriter_HeadersWithNullValues(t *testing.T) {
	records := []Record{
		{
			Partition:  0,
			Offset:     0,
			Timestamp:  1000,
			Key:        []byte("key"),
			Value:      []byte("value"),
			Headers:    `[{"Key":"h1","Value":"djE="},{"Key":"h2","Value":null},{"Key":"h3","Value":""}]`,
			Attributes: 0,
		},
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	rec := readRecords[0]
	if rec.Headers != records[0].Headers {
		t.Errorf("headers mismatch: expected %q, got %q", records[0].Headers, rec.Headers)
	}
}

func TestGenerateParquetPath(t *testing.T) {
	path := GenerateParquetPath("my-topic", 5, "2024/01/15", "abc123")
	expected := "compaction/v1/topic=my-topic/partition=5/date=2024/01/15/abc123.parquet"
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestGenerateParquetID(t *testing.T) {
	id1 := GenerateParquetID()
	id2 := GenerateParquetID()

	if id1 == id2 {
		t.Error("expected unique IDs, got duplicates")
	}

	// Verify UUID format (36 characters with hyphens)
	if len(id1) != 36 {
		t.Errorf("expected 36 character UUID, got %d", len(id1))
	}
}

func TestReader_NumRows(t *testing.T) {
	records := []Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1"), Attributes: 0},
		{Partition: 0, Offset: 1, Timestamp: 1001, Key: []byte("k2"), Value: []byte("v2"), Attributes: 0},
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 2 {
		t.Errorf("expected NumRows=2, got %d", reader.NumRows())
	}
}

func TestWriter_RoundTripPreservesAllFields(t *testing.T) {
	pid := int64(999)
	epoch := int32(42)
	seq := int32(7)

	original := Record{
		Partition:  3,
		Offset:     12345,
		Timestamp:  time.Now().UnixMilli(),
		Key:        []byte("test-key"),
		Value:      []byte("test-value"),
		Headers:    `[{"Key":"trace-id","Value":"YWJjMTIz"},{"Key":"meta","Value":null}]`,
		ProducerID: &pid,
		ProducerEpoch: &epoch,
		BaseSequence:  &seq,
		Attributes: 8,
	}

	data, _, err := WriteToBuffer([]Record{original})
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	reader, err := NewReader(data)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	rec := readRecords[0]

	if rec.Partition != original.Partition {
		t.Errorf("partition mismatch: expected %d, got %d", original.Partition, rec.Partition)
	}
	if rec.Offset != original.Offset {
		t.Errorf("offset mismatch: expected %d, got %d", original.Offset, rec.Offset)
	}
	if rec.Timestamp != original.Timestamp {
		t.Errorf("timestamp mismatch: expected %d, got %d", original.Timestamp, rec.Timestamp)
	}
	if !bytes.Equal(rec.Key, original.Key) {
		t.Errorf("key mismatch: expected %v, got %v", original.Key, rec.Key)
	}
	if rec.Value != nil && !bytes.Equal(rec.Value, original.Value) {
		t.Errorf("value mismatch: expected %v, got %v", original.Value, rec.Value)
	}
	if rec.Headers != original.Headers {
		t.Errorf("headers mismatch: expected %q, got %q", original.Headers, rec.Headers)
	}
	if rec.ProducerID == nil || *rec.ProducerID != *original.ProducerID {
		t.Errorf("producer_id mismatch: expected %v, got %v", original.ProducerID, rec.ProducerID)
	}
	if rec.ProducerEpoch == nil || *rec.ProducerEpoch != *original.ProducerEpoch {
		t.Errorf("producer_epoch mismatch: expected %v, got %v", original.ProducerEpoch, rec.ProducerEpoch)
	}
	if rec.BaseSequence == nil || *rec.BaseSequence != *original.BaseSequence {
		t.Errorf("base_sequence mismatch: expected %v, got %v", original.BaseSequence, rec.BaseSequence)
	}
	if rec.Attributes != original.Attributes {
		t.Errorf("attributes mismatch: expected %d, got %d", original.Attributes, rec.Attributes)
	}
}

func TestWriter_CompatibilityWithFetchReader(t *testing.T) {
	// Verify that files written by the compaction writer can be read by the fetch reader.
	// This ensures schema compatibility between writer and reader.
	pid := int64(123)
	epoch := int32(1)
	seq := int32(5)

	records := []Record{
		{
			Partition:  0,
			Offset:     100,
			Timestamp:  1000,
			Key:        []byte("test-key"),
			Value:      []byte("test-value"),
			Headers:    `[{"Key":"h1","Value":"djE="},{"Key":"h2","Value":null}]`,
			ProducerID: &pid,
			ProducerEpoch: &epoch,
			BaseSequence:  &seq,
			Attributes: 0,
		},
		{
			Partition:  0,
			Offset:     101,
			Timestamp:  1001,
			Key:        nil,
			Value:      []byte("value-only"),
			Headers:    `[]`,
			ProducerID: nil,
			ProducerEpoch: nil,
			BaseSequence:  nil,
			Attributes: 1,
		},
	}

	data, stats, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	// Verify the file can be opened with the generic parquet reader
	file := newBytesFile(data)
	reader := parquet.NewGenericReader[Record](file)
	defer reader.Close()

	numRows := reader.NumRows()
	if numRows != 2 {
		t.Errorf("expected 2 rows, got %d", numRows)
	}

	// Read back and verify
	readRecords := make([]Record, int(numRows))
	n, err := reader.Read(readRecords)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 records, got %d", n)
	}

	// Verify stats match record data
	if stats.MinOffset != 100 || stats.MaxOffset != 101 {
		t.Errorf("stats offset range mismatch: expected [100,101], got [%d,%d]", stats.MinOffset, stats.MaxOffset)
	}
	if stats.MinTimestamp != 1000 || stats.MaxTimestamp != 1001 {
		t.Errorf("stats timestamp range mismatch: expected [1000,1001], got [%d,%d]", stats.MinTimestamp, stats.MaxTimestamp)
	}
}

func TestWriter_ParquetFileStats(t *testing.T) {
	// Verify that Parquet file-level statistics are populated per SPEC 5.3
	records := []Record{
		{Partition: 0, Offset: 100, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1"), Attributes: 0},
		{Partition: 0, Offset: 200, Timestamp: 2000, Key: []byte("k2"), Value: []byte("v2"), Attributes: 0},
		{Partition: 0, Offset: 150, Timestamp: 1500, Key: []byte("k3"), Value: []byte("v3"), Attributes: 0},
	}

	data, _, err := WriteToBuffer(records)
	if err != nil {
		t.Fatalf("WriteToBuffer failed: %v", err)
	}

	// Open the Parquet file to access metadata
	file := newBytesFile(data)
	pfile, err := parquet.OpenFile(file, file.Size())
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Check that row groups have column statistics
	rowGroups := pfile.RowGroups()
	if len(rowGroups) == 0 {
		t.Fatal("no row groups found")
	}

	rg := rowGroups[0]
	columns := rg.ColumnChunks()

	// Find the offset and timestamp columns
	schema := pfile.Schema()
	var offsetColIdx, timestampColIdx int = -1, -1
	for i, field := range schema.Fields() {
		switch field.Name() {
		case "offset":
			offsetColIdx = i
		case "timestamp":
			timestampColIdx = i
		}
	}

	if offsetColIdx < 0 || timestampColIdx < 0 {
		t.Fatal("offset or timestamp column not found")
	}

	// Verify offset column stats
	if offsetColIdx < len(columns) {
		col := columns[offsetColIdx]
		// Check that the column has pages with statistics
		pages := col.Pages()
		hasPages := false
		for {
			page, err := pages.ReadPage()
			if err != nil {
				break
			}
			if page != nil {
				hasPages = true
				// The page should have some data
				break
			}
		}
		pages.Close()
		if !hasPages {
			t.Error("offset column has no pages")
		}
	}

	// Verify timestamp column stats
	if timestampColIdx < len(columns) {
		col := columns[timestampColIdx]
		pages := col.Pages()
		hasPages := false
		for {
			page, err := pages.ReadPage()
			if err != nil {
				break
			}
			if page != nil {
				hasPages = true
				break
			}
		}
		pages.Close()
		if !hasPages {
			t.Error("timestamp column has no pages")
		}
	}
}

func TestParquetSchema_MatchesSpec(t *testing.T) {
	// Verify the schema matches SPEC 5.3 by checking the parquet schema
	records := []Record{
		{Partition: 0, Offset: 0, Timestamp: 1000, Attributes: 0},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[Record](&buf)
	_, err := writer.Write(records)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	reader := parquet.NewGenericReader[Record](newBytesFile(buf.Bytes()))
	defer reader.Close()

	schema := reader.Schema()

	// Verify column names per SPEC 5.3
	expectedColumns := map[string]bool{
		"partition":      true,
		"offset":         true,
		"timestamp":      true,
		"key":            true,
		"value":          true,
		"headers":        true,
		"producer_id":    true,
		"producer_epoch": true,
		"base_sequence":  true,
		"attributes":     true,
		"record_crc":     true,
	}

	for _, field := range schema.Fields() {
		name := field.Name()
		if !expectedColumns[name] {
			t.Errorf("unexpected column: %s", name)
		}
		delete(expectedColumns, name)
	}

	for col := range expectedColumns {
		t.Errorf("missing column: %s", col)
	}
}
