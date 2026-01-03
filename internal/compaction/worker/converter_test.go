package worker

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
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

	if len(readRecords[0].Headers) != 3 {
		t.Fatalf("expected 3 headers, got %d", len(readRecords[0].Headers))
	}

	// Verify header order is preserved
	if readRecords[0].Headers[0].Key != "h1" || !bytes.Equal(readRecords[0].Headers[0].Value, []byte("v1")) {
		t.Errorf("header 0 mismatch")
	}
	if readRecords[0].Headers[1].Key != "h2" || !bytes.Equal(readRecords[0].Headers[1].Value, []byte("v2")) {
		t.Errorf("header 1 mismatch")
	}
	if readRecords[0].Headers[2].Key != "h1" || !bytes.Equal(readRecords[0].Headers[2].Value, []byte("v3")) {
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

	if len(readRecords[0].Headers) != 1 {
		t.Fatalf("expected 1 header, got %d", len(readRecords[0].Headers))
	}

	if readRecords[0].Headers[0].Value != nil {
		t.Errorf("expected null header value, got %v", readRecords[0].Headers[0].Value)
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

// --- Streaming Converter Tests ---

func TestStreamingConvertWALFromBytes_Simple(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	cfg := DefaultStreamingConvertConfig()
	result, err := StreamingConvertWALFromBytes(walData, 0, 0, cfg)
	if err != nil {
		t.Fatalf("StreamingConvertWALFromBytes failed: %v", err)
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

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	if !bytes.Equal(readRecords[0].Key, []byte("key1")) {
		t.Errorf("key mismatch")
	}
	if !bytes.Equal(readRecords[0].Value, []byte("value1")) {
		t.Errorf("value mismatch")
	}
}

func TestStreamingConvertWALFromBytes_MatchesNonStreaming(t *testing.T) {
	records := []testRecord{
		{key: []byte("k1"), value: []byte("v1"), timestampDelta: 0},
		{key: []byte("k2"), value: []byte("v2"), timestampDelta: 100},
		{key: []byte("k3"), value: []byte("v3"), timestampDelta: 200},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	// Non-streaming result
	nonStreamingResult, err := ConvertWALToParquet(walData, 5, 100)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	// Streaming result
	cfg := DefaultStreamingConvertConfig()
	streamingResult, err := StreamingConvertWALFromBytes(walData, 5, 100, cfg)
	if err != nil {
		t.Fatalf("StreamingConvertWALFromBytes failed: %v", err)
	}

	// Compare record counts
	if nonStreamingResult.RecordCount != streamingResult.RecordCount {
		t.Errorf("RecordCount mismatch: non-streaming=%d, streaming=%d",
			nonStreamingResult.RecordCount, streamingResult.RecordCount)
	}

	// Compare stats
	if nonStreamingResult.Stats.MinOffset != streamingResult.Stats.MinOffset {
		t.Errorf("MinOffset mismatch: %d vs %d",
			nonStreamingResult.Stats.MinOffset, streamingResult.Stats.MinOffset)
	}
	if nonStreamingResult.Stats.MaxOffset != streamingResult.Stats.MaxOffset {
		t.Errorf("MaxOffset mismatch: %d vs %d",
			nonStreamingResult.Stats.MaxOffset, streamingResult.Stats.MaxOffset)
	}
	if nonStreamingResult.Stats.MinTimestamp != streamingResult.Stats.MinTimestamp {
		t.Errorf("MinTimestamp mismatch: %d vs %d",
			nonStreamingResult.Stats.MinTimestamp, streamingResult.Stats.MinTimestamp)
	}
	if nonStreamingResult.Stats.MaxTimestamp != streamingResult.Stats.MaxTimestamp {
		t.Errorf("MaxTimestamp mismatch: %d vs %d",
			nonStreamingResult.Stats.MaxTimestamp, streamingResult.Stats.MaxTimestamp)
	}

	// Read both Parquet files and compare records
	nsReader, err := NewReader(nonStreamingResult.ParquetData)
	if err != nil {
		t.Fatalf("NewReader (non-streaming) failed: %v", err)
	}
	defer nsReader.Close()

	sReader, err := NewReader(streamingResult.ParquetData)
	if err != nil {
		t.Fatalf("NewReader (streaming) failed: %v", err)
	}
	defer sReader.Close()

	nsRecords, err := nsReader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll (non-streaming) failed: %v", err)
	}

	sRecords, err := sReader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll (streaming) failed: %v", err)
	}

	if len(nsRecords) != len(sRecords) {
		t.Fatalf("Record count mismatch: %d vs %d", len(nsRecords), len(sRecords))
	}

	for i := range nsRecords {
		if nsRecords[i].Offset != sRecords[i].Offset {
			t.Errorf("Record %d offset mismatch: %d vs %d", i, nsRecords[i].Offset, sRecords[i].Offset)
		}
		if nsRecords[i].Timestamp != sRecords[i].Timestamp {
			t.Errorf("Record %d timestamp mismatch: %d vs %d", i, nsRecords[i].Timestamp, sRecords[i].Timestamp)
		}
		if !bytes.Equal(nsRecords[i].Key, sRecords[i].Key) {
			t.Errorf("Record %d key mismatch", i)
		}
		if !bytes.Equal(nsRecords[i].Value, sRecords[i].Value) {
			t.Errorf("Record %d value mismatch", i)
		}
	}
}

func TestStreamingConvertWALFromBytes_MultipleBatches(t *testing.T) {
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

	cfg := DefaultStreamingConvertConfig()
	result, err := StreamingConvertWALFromBytes(walData, 0, 0, cfg)
	if err != nil {
		t.Fatalf("StreamingConvertWALFromBytes failed: %v", err)
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

	for i, rec := range readRecords {
		if rec.Offset != int64(i) {
			t.Errorf("record %d: expected offset %d, got %d", i, i, rec.Offset)
		}
	}
}

func TestStreamingConvertWALFromBytes_SmallBatchSize(t *testing.T) {
	// Create many records to test batch flushing
	var records []testRecord
	for i := 0; i < 50; i++ {
		records = append(records, testRecord{
			key:            []byte("key" + string(rune(i))),
			value:          []byte("value" + string(rune(i))),
			timestampDelta: int64(i * 10),
		})
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	// Use small batch size to force multiple flushes
	cfg := StreamingConvertConfig{BatchSize: 5}
	result, err := StreamingConvertWALFromBytes(walData, 0, 0, cfg)
	if err != nil {
		t.Fatalf("StreamingConvertWALFromBytes failed: %v", err)
	}

	if result.RecordCount != 50 {
		t.Errorf("expected RecordCount=50, got %d", result.RecordCount)
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

	if len(readRecords) != 50 {
		t.Fatalf("expected 50 records, got %d", len(readRecords))
	}
}

func TestStreamingConvertWALFromBytes_EmptyWAL(t *testing.T) {
	walObj := wal.NewWAL(uuid.New(), 0, 1000)
	data, _ := wal.EncodeToBytes(walObj)

	cfg := DefaultStreamingConvertConfig()
	_, err := StreamingConvertWALFromBytes(data, 0, 0, cfg)
	if err == nil {
		t.Error("expected error for empty WAL")
	}
}

func TestStreamingConvertWALFromBytes_Compression(t *testing.T) {
	records := []testRecord{
		{key: []byte("gzip-key"), value: []byte("gzip-value"), timestampDelta: 0},
	}
	batch := buildCompressedTestRecordBatch(records, 1000, 0, compressionGzip)
	walData := buildTestWAL([][]byte{batch}, 123)

	cfg := DefaultStreamingConvertConfig()
	result, err := StreamingConvertWALFromBytes(walData, 0, 0, cfg)
	if err != nil {
		t.Fatalf("StreamingConvertWALFromBytes with gzip failed: %v", err)
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

	if !bytes.Equal(readRecords[0].Key, []byte("gzip-key")) {
		t.Errorf("key mismatch after gzip decompression")
	}
}

func TestStreamingConvertWALFromBytes_LargeWAL_BoundedMemory(t *testing.T) {
	// Create a large WAL with many records to verify streaming behavior
	// Each record has a 1KB value to make the WAL reasonably large
	const numBatches = 20
	const recordsPerBatch = 100
	const valueSize = 1024

	var allBatches [][]byte
	baseOffset := int64(0)
	for b := 0; b < numBatches; b++ {
		var records []testRecord
		for i := 0; i < recordsPerBatch; i++ {
			value := make([]byte, valueSize)
			for j := range value {
				value[j] = byte((b*recordsPerBatch + i + j) % 256)
			}
			records = append(records, testRecord{
				key:            []byte("key"),
				value:          value,
				timestampDelta: int64(i),
			})
		}
		batch := buildTestRecordBatch(records, int64(1000+b*1000), baseOffset)
		allBatches = append(allBatches, batch)
		baseOffset += int64(recordsPerBatch)
	}

	walData := buildTestWAL(allBatches, 123)
	t.Logf("Created WAL with %d bytes, %d batches, %d total records",
		len(walData), numBatches, numBatches*recordsPerBatch)

	// Use a small batch size to ensure records are flushed frequently
	cfg := StreamingConvertConfig{BatchSize: 50}
	result, err := StreamingConvertWALFromBytes(walData, 0, 0, cfg)
	if err != nil {
		t.Fatalf("StreamingConvertWALFromBytes failed: %v", err)
	}

	expectedRecords := int64(numBatches * recordsPerBatch)
	if result.RecordCount != expectedRecords {
		t.Errorf("expected RecordCount=%d, got %d", expectedRecords, result.RecordCount)
	}

	// Verify stats
	if result.Stats.MinOffset != 0 {
		t.Errorf("expected MinOffset=0, got %d", result.Stats.MinOffset)
	}
	if result.Stats.MaxOffset != expectedRecords-1 {
		t.Errorf("expected MaxOffset=%d, got %d", expectedRecords-1, result.Stats.MaxOffset)
	}

	// Verify we can read back all records
	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != expectedRecords {
		t.Errorf("expected %d rows in Parquet, got %d", expectedRecords, reader.NumRows())
	}

	t.Logf("Streaming conversion successful: %d records, Parquet size: %d bytes",
		result.RecordCount, len(result.ParquetData))
}

func TestStreamingConvertWALFromBytes_WithHeaders(t *testing.T) {
	records := []testRecord{
		{
			key:            []byte("key"),
			value:          []byte("value"),
			timestampDelta: 0,
			headers: []testHeader{
				{key: "h1", value: []byte("v1")},
				{key: "h2", value: []byte("v2")},
			},
		},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	cfg := DefaultStreamingConvertConfig()
	result, err := StreamingConvertWALFromBytes(walData, 0, 0, cfg)
	if err != nil {
		t.Fatalf("StreamingConvertWALFromBytes failed: %v", err)
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

	if len(readRecords[0].Headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(readRecords[0].Headers))
	}
	if readRecords[0].Headers[0].Key != "h1" {
		t.Errorf("header 0 key mismatch")
	}
}

func TestDefaultStreamingConvertConfig(t *testing.T) {
	cfg := DefaultStreamingConvertConfig()
	if cfg.BatchSize != 1000 {
		t.Errorf("expected default BatchSize=1000, got %d", cfg.BatchSize)
	}
}

// --- CRC Validation Tests ---

func TestValidateBatchCRC_ValidBatch(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	err := validateBatchCRC(batch)
	if err != nil {
		t.Errorf("validateBatchCRC failed for valid batch: %v", err)
	}
}

func TestValidateBatchCRC_CorruptedCRC(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Corrupt the CRC field (bytes 17-20)
	batch[17] ^= 0xFF
	batch[18] ^= 0xFF

	err := validateBatchCRC(batch)
	if err == nil {
		t.Error("expected CRC validation error for corrupted CRC")
	}
	if !errors.Is(err, ErrBatchCRCMismatch) {
		t.Errorf("expected ErrBatchCRCMismatch, got: %v", err)
	}
}

func TestValidateBatchCRC_CorruptedData(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Corrupt the data section (after the header)
	if len(batch) > 65 {
		batch[65] ^= 0xFF // Corrupt a data byte
	}

	err := validateBatchCRC(batch)
	if err == nil {
		t.Error("expected CRC validation error for corrupted data")
	}
	if !errors.Is(err, ErrBatchCRCMismatch) {
		t.Errorf("expected ErrBatchCRCMismatch, got: %v", err)
	}
}

func TestValidateBatchCRC_BatchTooSmall(t *testing.T) {
	// Batch smaller than minimum size for CRC validation
	batch := make([]byte, 20)
	err := validateBatchCRC(batch)
	if err == nil {
		t.Error("expected error for batch too small")
	}
}

func TestValidateBatchCRC_InvalidMagic(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Change magic byte to invalid value
	batch[16] = 1 // Change from 2 to 1

	err := validateBatchCRC(batch)
	if err == nil {
		t.Error("expected error for invalid magic byte")
	}
	if !strings.Contains(err.Error(), "unsupported magic byte") {
		t.Errorf("expected 'unsupported magic byte' error, got: %v", err)
	}
}

func TestExtractRecordsFromBatch_CRCValidation(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Valid batch should work
	extractedRecords, err := extractRecordsFromBatch(batch, 0, 0)
	if err != nil {
		t.Fatalf("extractRecordsFromBatch failed for valid batch: %v", err)
	}
	if len(extractedRecords) != 1 {
		t.Errorf("expected 1 record, got %d", len(extractedRecords))
	}
}

func TestExtractRecordsFromBatch_CorruptedBatchFailsExtraction(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Corrupt the CRC
	batch[17] ^= 0xFF

	_, err := extractRecordsFromBatch(batch, 0, 0)
	if err == nil {
		t.Error("expected error for corrupted batch")
	}
	if !strings.Contains(err.Error(), "CRC validation failed") {
		t.Errorf("expected 'CRC validation failed' error, got: %v", err)
	}
}

func TestConvertWALToParquet_CorruptedBatchFailsCompaction(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Corrupt the batch data after the CRC field
	if len(batch) > 30 {
		batch[30] ^= 0xFF
	}

	walData := buildTestWAL([][]byte{batch}, 123)

	_, err := ConvertWALToParquet(walData, 0, 0)
	if err == nil {
		t.Error("expected compaction to fail with corrupted batch")
	}
	if !strings.Contains(err.Error(), "CRC") {
		t.Errorf("expected error message to mention CRC, got: %v", err)
	}
}

func TestConvertWALToParquet_CorruptedCRCFieldFailsCompaction(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Corrupt the CRC field directly
	batch[17] = 0x00
	batch[18] = 0x00
	batch[19] = 0x00
	batch[20] = 0x00

	walData := buildTestWAL([][]byte{batch}, 123)

	_, err := ConvertWALToParquet(walData, 0, 0)
	if err == nil {
		t.Error("expected compaction to fail with corrupted CRC field")
	}
	if !strings.Contains(err.Error(), "CRC") {
		t.Errorf("expected error message to mention CRC, got: %v", err)
	}
}

func TestStreamingConvertWALFromBytes_CorruptedBatchFailsCompaction(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}
	batch := buildTestRecordBatch(records, 1000, 0)

	// Corrupt the batch
	batch[25] ^= 0xFF

	walData := buildTestWAL([][]byte{batch}, 123)

	cfg := DefaultStreamingConvertConfig()
	_, err := StreamingConvertWALFromBytes(walData, 0, 0, cfg)
	if err == nil {
		t.Error("expected streaming compaction to fail with corrupted batch")
	}
	if !strings.Contains(err.Error(), "CRC") {
		t.Errorf("expected error message to mention CRC, got: %v", err)
	}
}

func TestValidateBatchCRC_CompressedBatch(t *testing.T) {
	// Verify CRC validation works with compressed batches
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
		{key: []byte("key2"), value: []byte("value2"), timestampDelta: 100},
	}

	compressionTypes := []int{compressionGzip, compressionSnappy, compressionLz4, compressionZstd}
	for _, ct := range compressionTypes {
		batch := buildCompressedTestRecordBatch(records, 1000, 0, ct)

		// Valid compressed batch should pass CRC validation
		err := validateBatchCRC(batch)
		if err != nil {
			t.Errorf("CRC validation failed for compression type %d: %v", ct, err)
		}

		// Corrupted compressed batch should fail
		corruptedBatch := make([]byte, len(batch))
		copy(corruptedBatch, batch)
		if len(corruptedBatch) > 30 {
			corruptedBatch[30] ^= 0xFF
		}

		err = validateBatchCRC(corruptedBatch)
		if err == nil {
			t.Errorf("expected CRC error for corrupted compression type %d batch", ct)
		}
	}
}

func TestConvertWALToParquet_CorruptedCompressedBatchFailsCompaction(t *testing.T) {
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
	}

	compressionTypes := []int{compressionGzip, compressionSnappy, compressionLz4, compressionZstd}
	for _, ct := range compressionTypes {
		batch := buildCompressedTestRecordBatch(records, 1000, 0, ct)

		// Corrupt the compressed data
		if len(batch) > 65 {
			batch[65] ^= 0xFF
		}

		walData := buildTestWAL([][]byte{batch}, 123)

		_, err := ConvertWALToParquet(walData, 0, 0)
		if err == nil {
			t.Errorf("expected compaction to fail with corrupted compressed batch (type %d)", ct)
		}
	}
}

func TestConvertWALToParquet_MultipleBatches_OneCorrupted(t *testing.T) {
	// Create two valid batches
	records1 := []testRecord{
		{key: []byte("batch1-k1"), value: []byte("batch1-v1"), timestampDelta: 0},
	}
	batch1 := buildTestRecordBatch(records1, 1000, 0)

	records2 := []testRecord{
		{key: []byte("batch2-k1"), value: []byte("batch2-v1"), timestampDelta: 0},
	}
	batch2 := buildTestRecordBatch(records2, 2000, 1)

	// Corrupt the second batch
	batch2[25] ^= 0xFF

	walData := buildTestWAL([][]byte{batch1, batch2}, 123)

	_, err := ConvertWALToParquet(walData, 0, 0)
	if err == nil {
		t.Error("expected compaction to fail when one batch is corrupted")
	}
	if !strings.Contains(err.Error(), "CRC") {
		t.Errorf("expected error message to mention CRC, got: %v", err)
	}
}

func TestConvertWALToParquet_RecordCRCPopulated(t *testing.T) {
	// Create a batch with known content
	records := []testRecord{
		{key: []byte("key1"), value: []byte("value1"), timestampDelta: 0},
		{key: []byte("key2"), value: []byte("value2"), timestampDelta: 100},
	}
	batch := buildTestRecordBatch(records, 1000, 0)
	walData := buildTestWAL([][]byte{batch}, 123)

	result, err := ConvertWALToParquet(walData, 0, 0)
	if err != nil {
		t.Fatalf("ConvertWALToParquet failed: %v", err)
	}

	// Read back the Parquet records
	reader, err := NewReader(result.ParquetData)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	parquetRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(parquetRecords) != 2 {
		t.Fatalf("expected 2 records, got %d", len(parquetRecords))
	}

	// Verify each record has a non-nil RecordCRC
	for i, rec := range parquetRecords {
		if rec.RecordCRC == nil {
			t.Errorf("record %d: expected non-nil RecordCRC", i)
			continue
		}

		// Verify the CRC matches what we compute
		expectedCRC := computeRecordCRC(rec.Key, rec.Value, rec.Headers)
		if *rec.RecordCRC != expectedCRC {
			t.Errorf("record %d: CRC mismatch - got %d, expected %d", i, *rec.RecordCRC, expectedCRC)
		}
	}
}

func TestComputeRecordCRC_Consistency(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	headers := []Header{
		{Key: "h1", Value: []byte("v1")},
		{Key: "h2", Value: nil},
	}

	// Same inputs should produce same CRC
	crc1 := computeRecordCRC(key, value, headers)
	crc2 := computeRecordCRC(key, value, headers)
	if crc1 != crc2 {
		t.Errorf("CRC not consistent: %d != %d", crc1, crc2)
	}

	// Different inputs should produce different CRCs
	crc3 := computeRecordCRC([]byte("different-key"), value, headers)
	if crc1 == crc3 {
		t.Error("CRC should differ for different keys")
	}

	crc4 := computeRecordCRC(key, []byte("different-value"), headers)
	if crc1 == crc4 {
		t.Error("CRC should differ for different values")
	}

	crc5 := computeRecordCRC(key, value, nil)
	if crc1 == crc5 {
		t.Error("CRC should differ with no headers")
	}
}

func TestComputeRecordCRC_NilKeyValue(t *testing.T) {
	// Nil key and value should not panic
	crc := computeRecordCRC(nil, nil, nil)
	if crc != 0 {
		// CRC32C of empty input is 0
		t.Logf("CRC for nil inputs: %d", crc)
	}

	// Empty key/value vs nil should produce different CRCs
	crcEmpty := computeRecordCRC([]byte{}, []byte{}, nil)
	crcNil := computeRecordCRC(nil, nil, nil)
	// Empty byte slices add no bytes, so should equal nil case
	if crcEmpty != crcNil {
		t.Errorf("empty vs nil CRC mismatch: %d vs %d", crcEmpty, crcNil)
	}
}

func TestConvertWALToParquet_RecordCRCWithHeaders(t *testing.T) {
	// Create a batch with headers (testRecord already supports headers)
	records := []testRecord{
		{
			key:            []byte("key1"),
			value:          []byte("value1"),
			timestampDelta: 0,
			headers: []testHeader{
				{key: "trace-id", value: []byte("abc123")},
				{key: "source", value: []byte("test")},
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

	parquetRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(parquetRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(parquetRecords))
	}

	rec := parquetRecords[0]
	if rec.RecordCRC == nil {
		t.Fatal("expected non-nil RecordCRC")
	}

	// Verify headers are present
	if len(rec.Headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(rec.Headers))
	}

	// Verify CRC includes headers
	expectedCRC := computeRecordCRC(rec.Key, rec.Value, rec.Headers)
	if *rec.RecordCRC != expectedCRC {
		t.Errorf("CRC mismatch with headers - got %d, expected %d", *rec.RecordCRC, expectedCRC)
	}
}
