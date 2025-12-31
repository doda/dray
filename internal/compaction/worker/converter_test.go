package worker

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/dray-io/dray/internal/wal"
	"github.com/google/uuid"
)

// buildTestRecordBatch creates a minimal Kafka v2 record batch for testing.
// This builds an uncompressed batch with the specified records.
func buildTestRecordBatch(records []testRecord, firstTimestamp int64, baseOffset int64) []byte {
	// Build the records section
	var recordsData bytes.Buffer
	for i, rec := range records {
		writeTestRecord(&recordsData, rec, int64(i), firstTimestamp)
	}

	// Build the batch header
	recordsBytes := recordsData.Bytes()
	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + len(recordsBytes)
	totalSize := 8 + 4 + batchLength

	batch := make([]byte, totalSize)
	offset := 0

	// baseOffset (8)
	binary.BigEndian.PutUint64(batch[offset:], uint64(baseOffset))
	offset += 8

	// batchLength (4)
	binary.BigEndian.PutUint32(batch[offset:], uint32(batchLength))
	offset += 4

	// partitionLeaderEpoch (4)
	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	// magic (1)
	batch[offset] = 2
	offset++

	// crc placeholder (4)
	crcOffset := offset
	offset += 4

	// Start of CRC region
	crcStart := offset

	// attributes (2) - no compression
	binary.BigEndian.PutUint16(batch[offset:], 0)
	offset += 2

	// lastOffsetDelta (4)
	binary.BigEndian.PutUint32(batch[offset:], uint32(len(records)-1))
	offset += 4

	// firstTimestamp (8)
	binary.BigEndian.PutUint64(batch[offset:], uint64(firstTimestamp))
	offset += 8

	// maxTimestamp (8) - same as first for simplicity
	maxTs := firstTimestamp
	for _, rec := range records {
		if firstTimestamp+rec.timestampDelta > maxTs {
			maxTs = firstTimestamp + rec.timestampDelta
		}
	}
	binary.BigEndian.PutUint64(batch[offset:], uint64(maxTs))
	offset += 8

	// producerId (8) - -1 for non-idempotent
	binary.BigEndian.PutUint64(batch[offset:], 0xFFFFFFFFFFFFFFFF)
	offset += 8

	// producerEpoch (2)
	binary.BigEndian.PutUint16(batch[offset:], 0xFFFF)
	offset += 2

	// firstSequence (4)
	binary.BigEndian.PutUint32(batch[offset:], 0xFFFFFFFF)
	offset += 4

	// recordCount (4)
	binary.BigEndian.PutUint32(batch[offset:], uint32(len(records)))
	offset += 4

	// Copy records
	copy(batch[offset:], recordsBytes)

	// Calculate CRC
	crc := crc32.Checksum(batch[crcStart:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcOffset:], crc)

	return batch
}

// buildCompressedTestRecordBatch creates a compressed Kafka v2 record batch.
func buildCompressedTestRecordBatch(records []testRecord, firstTimestamp int64, baseOffset int64, compressionType int) []byte {
	// Build the records section
	var recordsData bytes.Buffer
	for i, rec := range records {
		writeTestRecord(&recordsData, rec, int64(i), firstTimestamp)
	}

	// Compress the records
	recordsBytes := recordsData.Bytes()
	compressedRecords := compressData(recordsBytes, compressionType)

	// Build the batch header
	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + len(compressedRecords)
	totalSize := 8 + 4 + batchLength

	batch := make([]byte, totalSize)
	offset := 0

	// baseOffset (8)
	binary.BigEndian.PutUint64(batch[offset:], uint64(baseOffset))
	offset += 8

	// batchLength (4)
	binary.BigEndian.PutUint32(batch[offset:], uint32(batchLength))
	offset += 4

	// partitionLeaderEpoch (4)
	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	// magic (1)
	batch[offset] = 2
	offset++

	// crc placeholder (4)
	crcOffset := offset
	offset += 4

	// Start of CRC region
	crcStart := offset

	// attributes (2) - with compression
	binary.BigEndian.PutUint16(batch[offset:], uint16(compressionType))
	offset += 2

	// lastOffsetDelta (4)
	binary.BigEndian.PutUint32(batch[offset:], uint32(len(records)-1))
	offset += 4

	// firstTimestamp (8)
	binary.BigEndian.PutUint64(batch[offset:], uint64(firstTimestamp))
	offset += 8

	// maxTimestamp (8)
	maxTs := firstTimestamp
	for _, rec := range records {
		if firstTimestamp+rec.timestampDelta > maxTs {
			maxTs = firstTimestamp + rec.timestampDelta
		}
	}
	binary.BigEndian.PutUint64(batch[offset:], uint64(maxTs))
	offset += 8

	// producerId (8)
	binary.BigEndian.PutUint64(batch[offset:], 0xFFFFFFFFFFFFFFFF)
	offset += 8

	// producerEpoch (2)
	binary.BigEndian.PutUint16(batch[offset:], 0xFFFF)
	offset += 2

	// firstSequence (4)
	binary.BigEndian.PutUint32(batch[offset:], 0xFFFFFFFF)
	offset += 4

	// recordCount (4)
	binary.BigEndian.PutUint32(batch[offset:], uint32(len(records)))
	offset += 4

	// Copy compressed records
	copy(batch[offset:], compressedRecords)

	// Calculate CRC
	crc := crc32.Checksum(batch[crcStart:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcOffset:], crc)

	return batch
}

// compressData compresses data using the specified compression type.
func compressData(data []byte, compressionType int) []byte {
	switch compressionType {
	case compressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		w.Write(data)
		w.Close()
		return buf.Bytes()

	case compressionSnappy:
		return snappy.Encode(nil, data)

	case compressionLz4:
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		w.Write(data)
		w.Close()
		return buf.Bytes()

	case compressionZstd:
		enc, _ := zstd.NewWriter(nil)
		return enc.EncodeAll(data, nil)

	default:
		return data
	}
}

type testRecord struct {
	key            []byte
	value          []byte
	timestampDelta int64
	headers        []testHeader
}

type testHeader struct {
	key   string
	value []byte
}

// writeTestRecord writes a single Kafka v2 record.
func writeTestRecord(w *bytes.Buffer, rec testRecord, offsetDelta int64, firstTimestamp int64) {
	var body bytes.Buffer

	// attributes (1 byte)
	body.WriteByte(0)

	// timestampDelta (varint)
	writeVarint(&body, rec.timestampDelta)

	// offsetDelta (varint)
	writeVarint(&body, offsetDelta)

	// keyLength (varint)
	if rec.key == nil {
		writeVarint(&body, -1)
	} else {
		writeVarint(&body, int64(len(rec.key)))
		body.Write(rec.key)
	}

	// valueLength (varint)
	if rec.value == nil {
		writeVarint(&body, -1)
	} else {
		writeVarint(&body, int64(len(rec.value)))
		body.Write(rec.value)
	}

	// headerCount (varint)
	writeVarint(&body, int64(len(rec.headers)))

	// headers
	for _, h := range rec.headers {
		writeVarint(&body, int64(len(h.key)))
		body.WriteString(h.key)
		if h.value == nil {
			writeVarint(&body, -1)
		} else {
			writeVarint(&body, int64(len(h.value)))
			body.Write(h.value)
		}
	}

	// Write length prefix and body
	bodyBytes := body.Bytes()
	writeVarint(w, int64(len(bodyBytes)))
	w.Write(bodyBytes)
}

// writeVarint writes a zigzag-encoded signed varint.
func writeVarint(w *bytes.Buffer, v int64) {
	// Zigzag encode
	uv := uint64((v << 1) ^ (v >> 63))
	// Write as unsigned varint
	for uv >= 0x80 {
		w.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	w.WriteByte(byte(uv))
}

// buildTestWAL creates a WAL with the given batches.
func buildTestWAL(batches [][]byte, streamID uint64) []byte {
	walObj := wal.NewWAL(uuid.New(), 0, 1000)

	batchEntries := make([]wal.BatchEntry, len(batches))
	for i, b := range batches {
		batchEntries[i] = wal.BatchEntry{Data: b}
	}

	walObj.AddChunk(wal.Chunk{
		StreamID:       streamID,
		Batches:        batchEntries,
		RecordCount:    100, // placeholder
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
	})

	data, _ := wal.EncodeToBytes(walObj)
	return data
}

func TestConvertWALToParquet_SimpleRecord(t *testing.T) {
	// Create a simple test batch with one record
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	if result.RecordCount != 1 {
		t.Errorf("expected RecordCount=1, got %d", result.RecordCount)
	}

	if result.Stats.MinOffset != 0 || result.Stats.MaxOffset != 0 {
		t.Errorf("expected offset range [0,0], got [%d,%d]", result.Stats.MinOffset, result.Stats.MaxOffset)
	}

	// Verify the Parquet data can be read back
	reader, err := NewReader(result.ParquetData)
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

	if !bytes.Equal(readRecords[0].Key, []byte("key1")) {
		t.Errorf("key mismatch: expected 'key1', got %s", string(readRecords[0].Key))
	}
	if !bytes.Equal(readRecords[0].Value, []byte("value1")) {
		t.Errorf("value mismatch: expected 'value1', got %s", string(readRecords[0].Value))
	}
}

func TestConvertWALToParquet_MultipleRecords(t *testing.T) {
	records := []testRecord{
		{key: []byte("k1"), value: []byte("v1"), timestampDelta: 0},
		{key: []byte("k2"), value: []byte("v2"), timestampDelta: 100},
		{key: []byte("k3"), value: []byte("v3"), timestampDelta: 200},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 5, 100)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	if result.RecordCount != 3 {
		t.Errorf("expected RecordCount=3, got %d", result.RecordCount)
	}

	// Verify offsets are assigned correctly starting from 100
	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	expectedOffsets := []int64{100, 101, 102}
	for i, rec := range readRecords {
		if rec.Offset != expectedOffsets[i] {
			t.Errorf("record %d: expected offset %d, got %d", i, expectedOffsets[i], rec.Offset)
		}
	}

	// Verify timestamps are calculated correctly
	expectedTimestamps := []int64{1000, 1100, 1200}
	for i, rec := range readRecords {
		if rec.Timestamp != expectedTimestamps[i] {
			t.Errorf("record %d: expected timestamp %d, got %d", i, expectedTimestamps[i], rec.Timestamp)
		}
	}
}

func TestConvertWALToParquet_NullKeyValue(t *testing.T) {
	records := []testRecord{
		{key: nil, value: []byte("v1"), timestampDelta: 0},
		{key: []byte("k2"), value: nil, timestampDelta: 0},
		{key: nil, value: nil, timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// Record 0: null key
	if readRecords[0].Key != nil {
		t.Errorf("record 0: expected null key, got %v", readRecords[0].Key)
	}

	// Record 1: null value
	if readRecords[1].Value != nil {
		t.Errorf("record 1: expected null value, got %v", readRecords[1].Value)
	}
}

func TestConvertWALToParquet_WithHeaders(t *testing.T) {
	records := []testRecord{
		{
			key:            []byte("key"),
			value:          []byte("value"),
			timestampDelta: 0,
			headers: []testHeader{
				{key: "h1", value: []byte("v1")},
				{key: "h2", value: []byte("v2")},
				{key: "h1", value: []byte("v3")}, // Duplicate key
			},
		},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if readRecords[0].Headers == "" {
		t.Fatal("expected non-empty headers JSON")
	}

	var readHeaders []Header
	if err := json.Unmarshal([]byte(readRecords[0].Headers), &readHeaders); err != nil {
		t.Fatalf("failed to unmarshal headers: %v", err)
	}

	if len(readHeaders) != 3 {
		t.Fatalf("expected 3 headers, got %d", len(readHeaders))
	}

	// Verify header order is preserved
	if readHeaders[0].Key != "h1" || !bytes.Equal(readHeaders[0].Value, []byte("v1")) {
		t.Errorf("header 0 mismatch")
	}
	if readHeaders[1].Key != "h2" || !bytes.Equal(readHeaders[1].Value, []byte("v2")) {
		t.Errorf("header 1 mismatch")
	}
	if readHeaders[2].Key != "h1" || !bytes.Equal(readHeaders[2].Value, []byte("v3")) {
		t.Errorf("header 2 mismatch")
	}
}

func TestConvertWALToParquet_GzipCompression(t *testing.T) {
	records := []testRecord{
		{key: []byte("k1"), value: []byte("v1"), timestampDelta: 0},
		{key: []byte("k2"), value: []byte("v2"), timestampDelta: 10},
	}
	batch := buildCompressedTestRecordBatch(records, 1000, 0, compressionGzip)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet with gzip failed: %v", err)
	}

	if result.RecordCount != 2 {
		t.Errorf("expected RecordCount=2, got %d", result.RecordCount)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(readRecords[0].Key, []byte("k1")) {
		t.Errorf("key mismatch after gzip decompression")
	}
}

func TestConvertWALToParquet_SnappyCompression(t *testing.T) {
	records := []testRecord{
		{key: []byte("snappy-key"), value: []byte("snappy-value"), timestampDelta: 0},
	}
	batch := buildCompressedTestRecordBatch(records, 1000, 0, compressionSnappy)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet with snappy failed: %v", err)
	}

	if result.RecordCount != 1 {
		t.Errorf("expected RecordCount=1, got %d", result.RecordCount)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(readRecords[0].Key, []byte("snappy-key")) {
		t.Errorf("key mismatch after snappy decompression")
	}
}

func TestConvertWALToParquet_Lz4Compression(t *testing.T) {
	records := []testRecord{
		{key: []byte("lz4-key"), value: []byte("lz4-value"), timestampDelta: 0},
	}
	batch := buildCompressedTestRecordBatch(records, 1000, 0, compressionLz4)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet with lz4 failed: %v", err)
	}

	if result.RecordCount != 1 {
		t.Errorf("expected RecordCount=1, got %d", result.RecordCount)
	}
}

func TestConvertWALToParquet_ZstdCompression(t *testing.T) {
	records := []testRecord{
		{key: []byte("zstd-key"), value: []byte("zstd-value"), timestampDelta: 0},
	}
	batch := buildCompressedTestRecordBatch(records, 1000, 0, compressionZstd)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet with zstd failed: %v", err)
	}

	if result.RecordCount != 1 {
		t.Errorf("expected RecordCount=1, got %d", result.RecordCount)
	}
}

func TestConvertWALToParquet_MultipleBatches(t *testing.T) {
	// Create two batches
	records1 := []testRecord{
		{key: []byte("batch1-k1"), value: []byte("batch1-v1"), timestampDelta: 0},
		{key: []byte("batch1-k2"), value: []byte("batch1-v2"), timestampDelta: 10},
	}
	batch1 := buildTestRecordBatch(records1, 1000, 0)

	records2 := []testRecord{
		{key: []byte("batch2-k1"), value: []byte("batch2-v1"), timestampDelta: 0},
		{key: []byte("batch2-k2"), value: []byte("batch2-v2"), timestampDelta: 10},
		{key: []byte("batch2-k3"), value: []byte("batch2-v3"), timestampDelta: 20},
	}
	batch2 := buildTestRecordBatch(records2, 2000, 2)

	walData := buildTestWAL([][]byte{batch1, batch2}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	if result.RecordCount != 5 {
		t.Errorf("expected RecordCount=5, got %d", result.RecordCount)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// Verify offsets are sequential
	for i, rec := range readRecords {
		if rec.Offset != int64(i) {
			t.Errorf("record %d: expected offset %d, got %d", i, i, rec.Offset)
		}
	}
}

func TestConvertWALToParquet_Partition(t *testing.T) {
	records := []testRecord{
		{key: []byte("key"), value: []byte("value"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	// Convert with partition 42
	result, err := ConvertWALToParquet(walData, 42, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if readRecords[0].Partition != 42 {
		t.Errorf("expected partition 42, got %d", readRecords[0].Partition)
	}
}

func TestConvertWALToParquet_LargeValue(t *testing.T) {
	// Create a large value (1MB)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	records := []testRecord{
		{key: []byte("large"), value: largeValue, timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(readRecords[0].Value, largeValue) {
		t.Errorf("large value mismatch")
	}
}

func TestReadVarint(t *testing.T) {
	tests := []struct {
		input    []byte
		expected int64
		bytes    int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x01}, -1, 1},
		{[]byte{0x02}, 1, 1},
		{[]byte{0x03}, -2, 1},
		{[]byte{0x04}, 2, 1},
		{[]byte{0xFE, 0x01}, 127, 2},
		{[]byte{0xFF, 0x01}, -128, 2},
	}

	for _, tc := range tests {
		got, n := readVarint(tc.input)
		if got != tc.expected || n != tc.bytes {
			t.Errorf("readVarint(%v): expected (%d, %d), got (%d, %d)", tc.input, tc.expected, tc.bytes, got, n)
		}
	}
}

func TestExtractRecordsFromBatch_InvalidBatch(t *testing.T) {
	// Batch too small
	_, err := extractRecordsFromBatch(make([]byte, 10), 0, 0)
	if err == nil {
		t.Error("expected error for small batch")
	}

	// Zero record count
	batch := make([]byte, 61)
	batch[16] = 2 // magic
	// recordCount at 57:61 is already 0
	_, err = extractRecordsFromBatch(batch, 0, 0)
	if err == nil {
		t.Error("expected error for zero record count")
	}
}

func TestParseBatchesFromChunk(t *testing.T) {
	// Build a chunk with two batches
	records1 := []testRecord{{key: []byte("k1"), value: []byte("v1")}}
	batch1 := buildTestRecordBatch(records1, 1000, 0)

	records2 := []testRecord{{key: []byte("k2"), value: []byte("v2")}}
	batch2 := buildTestRecordBatch(records2, 1000, 1)

	// Build chunk: [len1][batch1][len2][batch2]
	var chunk bytes.Buffer
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(batch1)))
	chunk.Write(lenBuf)
	chunk.Write(batch1)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(batch2)))
	chunk.Write(lenBuf)
	chunk.Write(batch2)

	batches, err := parseBatchesFromChunk(chunk.Bytes())
	if err != nil {
		t.Fatalf("parseBatchesFromChunk failed: %v", err)
	}

	if len(batches) != 2 {
		t.Errorf("expected 2 batches, got %d", len(batches))
	}

	if !bytes.Equal(batches[0], batch1) {
		t.Errorf("batch 0 mismatch")
	}
	if !bytes.Equal(batches[1], batch2) {
		t.Errorf("batch 1 mismatch")
	}
}

func TestParseBatchesFromChunk_Truncated(t *testing.T) {
	// Truncated length prefix
	_, err := parseBatchesFromChunk([]byte{0x00, 0x00})
	if err == nil {
		t.Error("expected error for truncated length")
	}

	// Truncated batch data
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data, 100) // claims 100 bytes but only 4 more
	_, err = parseBatchesFromChunk(data)
	if err == nil {
		t.Error("expected error for truncated batch")
	}
}

func TestDecompressRecords(t *testing.T) {
	original := []byte("test data for compression")

	// Test each compression type
	types := []int{compressionGzip, compressionSnappy, compressionLz4, compressionZstd}
	for _, ct := range types {
		compressed := compressData(original, ct)
		decompressed, err := decompressRecords(compressed, ct)
		if err != nil {
			t.Errorf("decompression type %d failed: %v", ct, err)
			continue
		}
		if !bytes.Equal(decompressed, original) {
			t.Errorf("decompression type %d: data mismatch", ct)
		}
	}
}

func TestDecompressRecords_UnsupportedType(t *testing.T) {
	_, err := decompressRecords([]byte("data"), 99)
	if err == nil {
		t.Error("expected error for unsupported compression type")
	}
}

func TestConvertWALToParquet_EmptyWAL(t *testing.T) {
	// Create WAL with no chunks
	walObj := wal.NewWAL(uuid.New(), 0, 1000)
	data, _ := wal.EncodeToBytes(walObj)

	_, err := ConvertWALToParquet(data, 0, 0)
	if err == nil {
		t.Error("expected error for empty WAL")
	}
}

func TestConvertWALToParquet_HeaderWithNullValue(t *testing.T) {
	records := []testRecord{
		{
			key:   []byte("key"),
			value: []byte("value"),
			headers: []testHeader{
				{key: "h1", value: nil}, // null value
			},
		},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	var readHeaders []Header
	if err := json.Unmarshal([]byte(readRecords[0].Headers), &readHeaders); err != nil {
		t.Fatalf("failed to unmarshal headers: %v", err)
	}

	if len(readHeaders) != 1 {
		t.Fatalf("expected 1 header, got %d", len(readHeaders))
	}

	if readHeaders[0].Value != nil {
		t.Errorf("expected null header value, got %v", readHeaders[0].Value)
	}
}

func TestFileStats(t *testing.T) {
	records := []testRecord{
		{key: []byte("k1"), value: []byte("v1"), timestampDelta: 0},
		{key: []byte("k2"), value: []byte("v2"), timestampDelta: 500},
		{key: []byte("k3"), value: []byte("v3"), timestampDelta: 100},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 50)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	// Check stats
	if result.Stats.MinOffset != 50 {
		t.Errorf("expected MinOffset=50, got %d", result.Stats.MinOffset)
	}
	if result.Stats.MaxOffset != 52 {
		t.Errorf("expected MaxOffset=52, got %d", result.Stats.MaxOffset)
	}
	if result.Stats.MinTimestamp != 1000 {
		t.Errorf("expected MinTimestamp=1000, got %d", result.Stats.MinTimestamp)
	}
	if result.Stats.MaxTimestamp != 1500 {
		t.Errorf("expected MaxTimestamp=1500, got %d", result.Stats.MaxTimestamp)
	}
	if result.Stats.RecordCount != 3 {
		t.Errorf("expected RecordCount=3, got %d", result.Stats.RecordCount)
	}
}
