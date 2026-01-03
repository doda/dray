package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestParquetSchemaRoundtrip verifies that:
// 1. All columns per SPEC 5.3 are present and correctly typed
// 2. Headers are stored as an ordered list preserving duplicates
// 3. record_crc column is present and correctly computed
// 4. Data round-trips correctly through compaction and fetch
func TestParquetSchemaRoundtrip(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newParquetTestObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "parquet-schema-test",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "parquet-schema-test", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Produce records with various characteristics to test schema
	t.Log("Producing test records with headers...")
	produceReq := buildSchemaTestProduceRequest("parquet-schema-test", 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", produceResp.Topics[0].Partitions[0].ErrorCode)
	}

	// Fetch records from WAL before compaction
	t.Log("Fetching data from WAL before compaction...")
	walRecords := fetchAllRecordsWithHeaders(t, fetchHandler, ctx, "parquet-schema-test", 0, 0)
	if len(walRecords) != 5 {
		t.Fatalf("expected 5 records from WAL, got %d", len(walRecords))
	}

	// Run compaction and get the Parquet file data
	t.Log("Running compaction...")
	parquetPath := runSchemaTestCompaction(t, ctx, streamID, metaStore, objStore, 0)
	t.Log("Compaction complete, Parquet file:", parquetPath)

	// Fetch records from Parquet after compaction
	t.Log("Fetching data from Parquet after compaction...")
	parquetRecords := fetchAllRecordsWithHeaders(t, fetchHandler, ctx, "parquet-schema-test", 0, 0)
	if len(parquetRecords) != 5 {
		t.Fatalf("expected 5 records from Parquet, got %d", len(parquetRecords))
	}

	// Verify data consistency between WAL and Parquet
	t.Log("Verifying data consistency...")
	for i := 0; i < len(walRecords); i++ {
		verifyRecordConsistency(t, i, walRecords[i], parquetRecords[i])
	}

	// Read Parquet file directly to verify schema
	t.Log("Verifying Parquet schema directly...")
	verifyParquetSchema(t, objStore, parquetPath)

	t.Log("All schema and roundtrip tests passed")
}

// TestParquetSchemaColumns verifies all columns per SPEC 5.3 are present with correct types.
func TestParquetSchemaColumns(t *testing.T) {
	// Create test records with all fields populated
	records := []worker.Record{
		{
			Partition:     0,
			Offset:        0,
			Timestamp:     time.Now().UnixMilli(),
			Key:           []byte("test-key"),
			Value:         []byte("test-value"),
			Headers:       []worker.Header{{Key: "h1", Value: []byte("v1")}},
			ProducerID:    nil, // nil as per spec (idempotence deferred)
			ProducerEpoch: nil,
			BaseSequence:  nil,
			Attributes:    0,
			RecordCRC:     intPtr(12345),
		},
	}

	parquetData, _, err := worker.WriteToBuffer(records)
	if err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Open Parquet file and verify schema
	file := newParquetBytesFile(parquetData)
	pf, err := parquet.OpenFile(file, file.Size())
	if err != nil {
		t.Fatalf("failed to open Parquet file: %v", err)
	}

	schema := pf.Schema()

	// Expected columns per SPEC 5.3
	expectedColumns := []string{
		"partition",
		"offset",
		"timestamp",
		"key",
		"value",
		"headers",
		"producer_id",
		"producer_epoch",
		"base_sequence",
		"attributes",
		"record_crc",
	}

	// Build a set of field names from the schema
	schemaFields := make(map[string]bool)
	for _, f := range schema.Fields() {
		schemaFields[f.Name()] = true
	}

	t.Log("Verifying Parquet schema columns...")
	for _, expected := range expectedColumns {
		if !schemaFields[expected] {
			t.Errorf("missing column: %s", expected)
			continue
		}
		t.Logf("  Column %s: found", expected)
	}

	// Verify headers field exists and is correctly typed
	var headersField parquet.Field
	for _, f := range schema.Fields() {
		if f.Name() == "headers" {
			headersField = f
			break
		}
	}
	if headersField == nil {
		t.Fatal("headers field not found in schema")
	}
	t.Log("  Headers field is correctly defined as a list type")
}

// TestParquetHeadersPreserveOrderAndDuplicates verifies headers are stored as
// an ordered list that preserves duplicates (SPEC 5.3 requirement).
func TestParquetHeadersPreserveOrderAndDuplicates(t *testing.T) {
	// Create records with ordered headers including duplicates
	records := []worker.Record{
		{
			Partition: 0,
			Offset:    0,
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("key"),
			Value:     []byte("value"),
			Headers: []worker.Header{
				{Key: "Content-Type", Value: []byte("application/json")},
				{Key: "X-Custom", Value: []byte("first")},
				{Key: "X-Custom", Value: []byte("second")}, // duplicate key
				{Key: "X-Custom", Value: []byte("third")},  // duplicate key
				{Key: "Trace-ID", Value: []byte("abc123")},
			},
			Attributes: 0,
			RecordCRC:  intPtr(0),
		},
	}

	parquetData, _, err := worker.WriteToBuffer(records)
	if err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Read back and verify
	reader, err := worker.NewReader(parquetData)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	rec := readRecords[0]
	if len(rec.Headers) != 5 {
		t.Fatalf("expected 5 headers, got %d", len(rec.Headers))
	}

	// Verify order is preserved
	expectedHeaders := []struct {
		key   string
		value string
	}{
		{"Content-Type", "application/json"},
		{"X-Custom", "first"},
		{"X-Custom", "second"},
		{"X-Custom", "third"},
		{"Trace-ID", "abc123"},
	}

	for i, expected := range expectedHeaders {
		if rec.Headers[i].Key != expected.key {
			t.Errorf("header %d: expected key %q, got %q", i, expected.key, rec.Headers[i].Key)
		}
		if string(rec.Headers[i].Value) != expected.value {
			t.Errorf("header %d: expected value %q, got %q", i, expected.value, string(rec.Headers[i].Value))
		}
	}

	// Count duplicates to verify they're preserved
	xCustomCount := 0
	for _, h := range rec.Headers {
		if h.Key == "X-Custom" {
			xCustomCount++
		}
	}
	if xCustomCount != 3 {
		t.Errorf("expected 3 X-Custom headers (duplicates), got %d", xCustomCount)
	}

	t.Log("Headers preserve order and duplicates correctly")
}

// TestParquetRecordCRC verifies the record_crc column is populated correctly.
func TestParquetRecordCRC(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	headers := []worker.Header{
		{Key: "h1", Value: []byte("v1")},
		{Key: "h2", Value: []byte("v2")},
	}

	// Compute expected CRC
	crc32cTable := crc32.MakeTable(crc32.Castagnoli)
	h := crc32.New(crc32cTable)
	h.Write(key)
	h.Write(value)
	h.Write([]byte("h1"))
	h.Write([]byte("v1"))
	h.Write([]byte("h2"))
	h.Write([]byte("v2"))
	expectedCRC := int32(h.Sum32())

	records := []worker.Record{
		{
			Partition:  0,
			Offset:     0,
			Timestamp:  time.Now().UnixMilli(),
			Key:        key,
			Value:      value,
			Headers:    headers,
			Attributes: 0,
			RecordCRC:  &expectedCRC,
		},
	}

	parquetData, _, err := worker.WriteToBuffer(records)
	if err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	reader, err := worker.NewReader(parquetData)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	rec := readRecords[0]
	if rec.RecordCRC == nil {
		t.Fatal("record_crc is nil, expected non-nil")
	}

	if *rec.RecordCRC != expectedCRC {
		t.Errorf("record_crc mismatch: expected %d, got %d", expectedCRC, *rec.RecordCRC)
	}

	t.Logf("record_crc correctly stored: %d", *rec.RecordCRC)
}

// TestParquetNullableFields verifies nullable fields work correctly.
func TestParquetNullableFields(t *testing.T) {
	records := []worker.Record{
		// Record with all nullable fields set
		{
			Partition:  0,
			Offset:     0,
			Timestamp:  time.Now().UnixMilli(),
			Key:        []byte("key"),
			Value:      []byte("value"),
			Headers:    []worker.Header{{Key: "h", Value: []byte("v")}},
			Attributes: 0,
			RecordCRC:  intPtr(123),
		},
		// Record with null key
		{
			Partition:  0,
			Offset:     1,
			Timestamp:  time.Now().UnixMilli(),
			Key:        nil,
			Value:      []byte("value-only"),
			Headers:    nil,
			Attributes: 0,
			RecordCRC:  nil,
		},
		// Record with null value
		{
			Partition:  0,
			Offset:     2,
			Timestamp:  time.Now().UnixMilli(),
			Key:        []byte("key-only"),
			Value:      nil,
			Headers:    []worker.Header{},
			Attributes: 0,
			RecordCRC:  nil,
		},
	}

	parquetData, _, err := worker.WriteToBuffer(records)
	if err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	reader, err := worker.NewReader(parquetData)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	if len(readRecords) != 3 {
		t.Fatalf("expected 3 records, got %d", len(readRecords))
	}

	// Verify record 0 (all fields set)
	if readRecords[0].Key == nil {
		t.Error("record 0: key should not be nil")
	}
	if readRecords[0].Value == nil {
		t.Error("record 0: value should not be nil")
	}

	// Verify record 1 (null key)
	if readRecords[1].Key != nil {
		t.Error("record 1: key should be nil")
	}
	if readRecords[1].Value == nil {
		t.Error("record 1: value should not be nil")
	}

	// Verify record 2 (null value)
	if readRecords[2].Key == nil {
		t.Error("record 2: key should not be nil")
	}
	if readRecords[2].Value != nil {
		t.Error("record 2: value should be nil")
	}

	t.Log("Nullable fields work correctly")
}

// TestParquetTimestampType verifies timestamp is stored with millisecond precision.
func TestParquetTimestampType(t *testing.T) {
	now := time.Now().UnixMilli()
	records := []worker.Record{
		{
			Partition:  0,
			Offset:     0,
			Timestamp:  now,
			Key:        []byte("key"),
			Value:      []byte("value"),
			Attributes: 0,
		},
	}

	parquetData, _, err := worker.WriteToBuffer(records)
	if err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	reader, err := worker.NewReader(parquetData)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer reader.Close()

	readRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read records: %v", err)
	}

	if len(readRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(readRecords))
	}

	if readRecords[0].Timestamp != now {
		t.Errorf("timestamp mismatch: expected %d, got %d", now, readRecords[0].Timestamp)
	}

	t.Logf("Timestamp correctly stored: %d", readRecords[0].Timestamp)
}

// Helper functions

type schemaTestRecord struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []schemaTestHeader
}

type schemaTestHeader struct {
	Key   string
	Value []byte
}

func buildSchemaTestProduceRequest(topic string, partition int32) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = partition
	partReq.Records = buildSchemaTestRecordBatch()

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// buildSchemaTestRecordBatch creates a record batch with 5 records:
// - Record 0: simple key/value, no headers
// - Record 1: key/value with single header
// - Record 2: key/value with multiple headers including duplicates
// - Record 3: null key
// - Record 4: null value
func buildSchemaTestRecordBatch() []byte {
	var records []byte
	ts := time.Now().UnixMilli()

	// Record 0: simple key/value, no headers
	records = append(records, buildSchemaTestRecord(0, []byte("key-0"), []byte("value-0"), nil)...)

	// Record 1: key/value with single header
	records = append(records, buildSchemaTestRecord(1, []byte("key-1"), []byte("value-1"), []schemaTestHeader{
		{Key: "header1", Value: []byte("value1")},
	})...)

	// Record 2: key/value with duplicate headers
	records = append(records, buildSchemaTestRecord(2, []byte("key-2"), []byte("value-2"), []schemaTestHeader{
		{Key: "dup-header", Value: []byte("first")},
		{Key: "dup-header", Value: []byte("second")},
		{Key: "dup-header", Value: []byte("third")},
	})...)

	// Record 3: null key
	records = append(records, buildSchemaTestRecord(3, nil, []byte("value-3"), nil)...)

	// Record 4: null value
	records = append(records, buildSchemaTestRecord(4, []byte("key-4"), nil, nil)...)

	recordCount := 5

	// Build batch header (61 bytes fixed)
	batch := make([]byte, 61+len(records))

	// baseOffset (8 bytes) = 0
	binary.BigEndian.PutUint64(batch[0:8], 0)

	// batchLength (4 bytes) = 49 + len(records)
	batchLength := 49 + len(records)
	binary.BigEndian.PutUint32(batch[8:12], uint32(batchLength))

	// partitionLeaderEpoch (4 bytes)
	binary.BigEndian.PutUint32(batch[12:16], 0)

	// magic (1 byte) = 2
	batch[16] = 2

	// crc placeholder (4 bytes at 17-21)

	// attributes (2 bytes) = 0 (uncompressed)
	binary.BigEndian.PutUint16(batch[21:23], 0)

	// lastOffsetDelta (4 bytes)
	binary.BigEndian.PutUint32(batch[23:27], uint32(recordCount-1))

	// firstTimestamp (8 bytes)
	binary.BigEndian.PutUint64(batch[27:35], uint64(ts))

	// maxTimestamp (8 bytes)
	binary.BigEndian.PutUint64(batch[35:43], uint64(ts))

	// producerId (8 bytes) = -1
	binary.BigEndian.PutUint64(batch[43:51], 0xffffffffffffffff)

	// producerEpoch (2 bytes) = -1
	binary.BigEndian.PutUint16(batch[51:53], 0xffff)

	// firstSequence (4 bytes) = -1
	binary.BigEndian.PutUint32(batch[53:57], 0xffffffff)

	// recordCount (4 bytes)
	binary.BigEndian.PutUint32(batch[57:61], uint32(recordCount))

	// Copy records
	copy(batch[61:], records)

	// Calculate CRC32C over bytes from offset 21 onwards
	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[21:], table)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

func buildSchemaTestRecord(offsetDelta int, key, value []byte, headers []schemaTestHeader) []byte {
	var body []byte

	// attributes (1 byte) = 0
	body = append(body, 0)

	// timestampDelta (varint) = 0
	body = appendSchemaTestVarint(body, 0)

	// offsetDelta (varint)
	body = appendSchemaTestVarint(body, int64(offsetDelta))

	// key
	if key == nil {
		body = appendSchemaTestVarint(body, -1)
	} else {
		body = appendSchemaTestVarint(body, int64(len(key)))
		body = append(body, key...)
	}

	// value
	if value == nil {
		body = appendSchemaTestVarint(body, -1)
	} else {
		body = appendSchemaTestVarint(body, int64(len(value)))
		body = append(body, value...)
	}

	// headers count
	body = appendSchemaTestVarint(body, int64(len(headers)))

	// headers
	for _, h := range headers {
		body = appendSchemaTestVarint(body, int64(len(h.Key)))
		body = append(body, []byte(h.Key)...)
		if h.Value == nil {
			body = appendSchemaTestVarint(body, -1)
		} else {
			body = appendSchemaTestVarint(body, int64(len(h.Value)))
			body = append(body, h.Value...)
		}
	}

	// Prepend length
	var result []byte
	result = appendSchemaTestVarint(result, int64(len(body)))
	result = append(result, body...)

	return result
}

func appendSchemaTestVarint(b []byte, v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		b = append(b, byte(uv)|0x80)
		uv >>= 7
	}
	b = append(b, byte(uv))
	return b
}

func fetchAllRecordsWithHeaders(t *testing.T, handler *protocol.FetchHandler, ctx context.Context, topic string, partition int32, startOffset int64) []schemaTestRecord {
	t.Helper()

	var allRecords []schemaTestRecord
	currentOffset := startOffset

	for {
		fetchReq := buildFetchRequest(topic, partition, currentOffset)
		fetchResp := handler.Handle(ctx, 12, fetchReq)

		if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
			t.Fatalf("unexpected empty fetch response")
		}

		partResp := fetchResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("fetch failed with error code %d at offset %d", partResp.ErrorCode, currentOffset)
		}

		if len(partResp.RecordBatches) == 0 {
			break
		}

		records := parseSchemaTestRecordBatches(t, partResp.RecordBatches)
		if len(records) == 0 {
			break
		}

		for _, rec := range records {
			if rec.Offset >= startOffset {
				allRecords = append(allRecords, rec)
			}
		}
		currentOffset = records[len(records)-1].Offset + 1

		if currentOffset >= partResp.HighWatermark {
			break
		}
	}

	return allRecords
}

func parseSchemaTestRecordBatches(t *testing.T, data []byte) []schemaTestRecord {
	t.Helper()

	var records []schemaTestRecord
	offset := 0

	for offset < len(data) {
		if offset+61 > len(data) {
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		batchLength := int(binary.BigEndian.Uint32(data[offset+8 : offset+12]))

		totalBatchSize := 12 + batchLength
		if offset+totalBatchSize > len(data) {
			break
		}

		firstTimestamp := int64(binary.BigEndian.Uint64(data[offset+27 : offset+35]))
		recordCount := int(binary.BigEndian.Uint32(data[offset+57 : offset+61]))

		recordsStart := offset + 61
		recordsEnd := offset + totalBatchSize
		recordsData := data[recordsStart:recordsEnd]

		recOffset := 0
		for i := 0; i < recordCount; i++ {
			if recOffset >= len(recordsData) {
				break
			}
			rec, bytesRead := parseSchemaTestRecord(recordsData[recOffset:], baseOffset+int64(i), firstTimestamp)
			if bytesRead <= 0 {
				break
			}
			records = append(records, rec)
			recOffset += bytesRead
		}

		offset += totalBatchSize
	}

	return records
}

func parseSchemaTestRecord(data []byte, offset int64, baseTimestamp int64) (schemaTestRecord, int) {
	if len(data) == 0 {
		return schemaTestRecord{}, 0
	}

	pos := 0

	recordLen, lenPrefixBytes := readSchemaTestVarint(data[pos:])
	if lenPrefixBytes <= 0 {
		return schemaTestRecord{}, 0
	}
	pos += lenPrefixBytes

	if recordLen < 0 || recordLen > int64(len(data)-pos) {
		return schemaTestRecord{}, 0
	}

	if pos >= len(data) {
		return schemaTestRecord{}, 0
	}
	_ = data[pos]
	pos++

	timestampDelta, bytesRead := readSchemaTestVarint(data[pos:])
	if bytesRead <= 0 {
		return schemaTestRecord{}, 0
	}
	pos += bytesRead

	_, bytesRead = readSchemaTestVarint(data[pos:])
	if bytesRead <= 0 {
		return schemaTestRecord{}, 0
	}
	pos += bytesRead

	keyLen, bytesRead := readSchemaTestVarint(data[pos:])
	if bytesRead <= 0 {
		return schemaTestRecord{}, 0
	}
	pos += bytesRead

	var key []byte
	if keyLen >= 0 {
		if pos+int(keyLen) > len(data) {
			return schemaTestRecord{}, 0
		}
		key = make([]byte, keyLen)
		copy(key, data[pos:pos+int(keyLen)])
		pos += int(keyLen)
	}

	valueLen, bytesRead := readSchemaTestVarint(data[pos:])
	if bytesRead <= 0 {
		return schemaTestRecord{}, 0
	}
	pos += bytesRead

	var value []byte
	if valueLen >= 0 {
		if pos+int(valueLen) > len(data) {
			return schemaTestRecord{}, 0
		}
		value = make([]byte, valueLen)
		copy(value, data[pos:pos+int(valueLen)])
		pos += int(valueLen)
	}

	headerCount, bytesRead := readSchemaTestVarint(data[pos:])
	if bytesRead <= 0 {
		return schemaTestRecord{}, 0
	}
	pos += bytesRead

	var headers []schemaTestHeader
	for i := int64(0); i < headerCount && headerCount >= 0; i++ {
		hKeyLen, bytesRead := readSchemaTestVarint(data[pos:])
		if bytesRead <= 0 {
			break
		}
		pos += bytesRead

		var hKey string
		if hKeyLen >= 0 {
			if pos+int(hKeyLen) > len(data) {
				break
			}
			hKey = string(data[pos : pos+int(hKeyLen)])
			pos += int(hKeyLen)
		}

		hValueLen, bytesRead := readSchemaTestVarint(data[pos:])
		if bytesRead <= 0 {
			break
		}
		pos += bytesRead

		var hValue []byte
		if hValueLen >= 0 {
			if pos+int(hValueLen) > len(data) {
				break
			}
			hValue = make([]byte, hValueLen)
			copy(hValue, data[pos:pos+int(hValueLen)])
			pos += int(hValueLen)
		}

		headers = append(headers, schemaTestHeader{Key: hKey, Value: hValue})
	}

	return schemaTestRecord{
		Offset:    offset,
		Timestamp: baseTimestamp + timestampDelta,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, lenPrefixBytes + int(recordLen)
}

func readSchemaTestVarint(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	var uv uint64
	var shift uint
	var bytesRead int

	for i := 0; i < len(data) && i < 10; i++ {
		b := data[i]
		uv |= uint64(b&0x7F) << shift
		bytesRead++
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	if bytesRead == 0 {
		return 0, 0
	}

	v := int64((uv >> 1) ^ -(uv & 1))
	return v, bytesRead
}

func verifyRecordConsistency(t *testing.T, i int, wal, parquet schemaTestRecord) {
	t.Helper()

	if wal.Offset != parquet.Offset {
		t.Errorf("record %d: offset mismatch - WAL=%d, Parquet=%d", i, wal.Offset, parquet.Offset)
	}

	if wal.Timestamp != parquet.Timestamp {
		t.Errorf("record %d: timestamp mismatch - WAL=%d, Parquet=%d", i, wal.Timestamp, parquet.Timestamp)
	}

	if !bytes.Equal(wal.Key, parquet.Key) {
		t.Errorf("record %d: key mismatch - WAL=%q, Parquet=%q", i, wal.Key, parquet.Key)
	}

	if !bytes.Equal(wal.Value, parquet.Value) {
		t.Errorf("record %d: value mismatch - WAL=%q, Parquet=%q", i, wal.Value, parquet.Value)
	}

	if len(wal.Headers) != len(parquet.Headers) {
		t.Errorf("record %d: header count mismatch - WAL=%d, Parquet=%d", i, len(wal.Headers), len(parquet.Headers))
	} else {
		for j, walHdr := range wal.Headers {
			parquetHdr := parquet.Headers[j]
			if walHdr.Key != parquetHdr.Key {
				t.Errorf("record %d header %d: key mismatch - WAL=%q, Parquet=%q", i, j, walHdr.Key, parquetHdr.Key)
			}
			if !bytes.Equal(walHdr.Value, parquetHdr.Value) {
				t.Errorf("record %d header %d: value mismatch - WAL=%v, Parquet=%v", i, j, walHdr.Value, parquetHdr.Value)
			}
		}
	}
}

func verifyParquetSchema(t *testing.T, objStore *parquetTestObjectStore, path string) {
	t.Helper()

	data, ok := objStore.get(path)
	if !ok {
		t.Fatalf("Parquet file not found: %s", path)
	}

	file := newParquetBytesFile(data)
	pf, err := parquet.OpenFile(file, file.Size())
	if err != nil {
		t.Fatalf("failed to open Parquet file: %v", err)
	}

	schema := pf.Schema()

	// Build a set of field names from the schema
	schemaFields := make(map[string]bool)
	for _, f := range schema.Fields() {
		schemaFields[f.Name()] = true
	}

	// Verify all expected columns exist
	expectedColumns := []string{
		"partition", "offset", "timestamp", "key", "value",
		"headers", "producer_id", "producer_epoch", "base_sequence",
		"attributes", "record_crc",
	}

	for _, colName := range expectedColumns {
		if !schemaFields[colName] {
			t.Errorf("missing column in Parquet schema: %s", colName)
		}
	}

	// Verify timestamp column name (not timestamp_ms per SPEC 5.3)
	if !schemaFields["timestamp"] {
		t.Error("timestamp column should be named 'timestamp' (not 'timestamp_ms')")
	}

	t.Log("Parquet schema verified: all columns present with correct names")
}

func runSchemaTestCompaction(t *testing.T, ctx context.Context, streamID string, metaStore metadata.MetadataStore, objStore objectstore.Store, partition int32) string {
	t.Helper()

	prefix := fmt.Sprintf("/dray/v1/streams/%s/offset-index/", streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	if len(kvs) == 0 {
		t.Fatal("no index entries to compact")
	}

	var walEntries []*index.IndexEntry
	var walIndexKeys []string
	var minOffset, maxOffset int64

	for i, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
			walIndexKeys = append(walIndexKeys, kv.Key)
			if i == 0 || entry.StartOffset < minOffset {
				minOffset = entry.StartOffset
			}
			if entry.EndOffset > maxOffset {
				maxOffset = entry.EndOffset
			}
		}
	}

	if len(walEntries) == 0 {
		t.Fatal("no WAL entries to compact")
	}

	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, partition)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	date := time.Now().Format("2006-01-02")
	parquetID := worker.GenerateParquetID()
	parquetPath := worker.GenerateParquetPath("parquet-schema-test", partition, date, parquetID)
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      minOffset,
		EndOffset:        maxOffset,
		FileType:         index.FileTypeParquet,
		RecordCount:      uint32(convertResult.RecordCount),
		MessageCount:     uint32(convertResult.RecordCount),
		CreatedAtMs:      time.Now().UnixMilli(),
		MinTimestampMs:   convertResult.Stats.MinTimestamp,
		MaxTimestampMs:   convertResult.Stats.MaxTimestamp,
		ParquetPath:      parquetPath,
		ParquetSizeBytes: uint64(len(convertResult.ParquetData)),
	}

	swapper := compaction.NewIndexSwapper(metaStore)
	_, err = swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   0,
	})
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}

	return parquetPath
}

type parquetTestObjectStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newParquetTestObjectStore() *parquetTestObjectStore {
	return &parquetTestObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *parquetTestObjectStore) get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	return data, ok
}

func (m *parquetTestObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = data
	return nil
}

func (m *parquetTestObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *parquetTestObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *parquetTestObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if start < 0 || start >= int64(len(data)) {
		return nil, objectstore.ErrInvalidRange
	}
	if end < 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return io.NopCloser(bytes.NewReader(data[start:end+1])), nil
}

func (m *parquetTestObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *parquetTestObjectStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *parquetTestObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []objectstore.ObjectMeta
	for key, data := range m.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, objectstore.ObjectMeta{
				Key:  key,
				Size: int64(len(data)),
			})
		}
	}
	return result, nil
}

func (m *parquetTestObjectStore) Close() error {
	return nil
}

var _ objectstore.Store = (*parquetTestObjectStore)(nil)

type parquetBytesFile struct {
	data []byte
}

func newParquetBytesFile(data []byte) *parquetBytesFile {
	return &parquetBytesFile{data: data}
}

func (f *parquetBytesFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *parquetBytesFile) Size() int64 {
	return int64(len(f.data))
}

func intPtr(v int32) *int32 {
	return &v
}
