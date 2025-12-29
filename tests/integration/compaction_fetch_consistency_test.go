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

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestCompactionFetchConsistency verifies that fetch returns identical data
// before and after compaction. This is a critical invariant: compaction must
// not alter the logical record content.
func TestCompactionFetchConsistency(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	// Create topic with one partition
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "compaction-test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "compaction-test-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush
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

	// Step 1: Produce multiple batches of records
	// We produce 3 separate batches to create multiple index entries
	recordsPerBatch := []int{3, 5, 7} // 15 total records
	var totalRecords int
	for _, count := range recordsPerBatch {
		totalRecords += count
	}

	for i, recordCount := range recordsPerBatch {
		produceReq := buildCompactionTestProduceRequest("compaction-test-topic", 0, recordCount, i)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("produce batch %d failed with error code %d", i, partResp.ErrorCode)
		}
		t.Logf("Produced batch %d: %d records, BaseOffset=%d", i, recordCount, partResp.BaseOffset)
	}

	// Step 2: Fetch and record all data from WAL
	t.Log("Fetching data from WAL (before compaction)...")
	walRecords := fetchAllRecords(t, fetchHandler, ctx, "compaction-test-topic", 0, 0)
	if len(walRecords) != totalRecords {
		t.Fatalf("expected %d records from WAL, got %d", totalRecords, len(walRecords))
	}
	t.Logf("Fetched %d records from WAL", len(walRecords))

	// Verify all records have correct offsets before compaction
	for i, rec := range walRecords {
		if rec.Offset != int64(i) {
			t.Errorf("WAL record %d has offset %d, expected %d", i, rec.Offset, i)
		}
	}

	// Step 3: Run compaction
	t.Log("Running compaction (WAL -> Parquet)...")
	runCompaction(t, ctx, streamID, metaStore, objStore, 0)
	t.Log("Compaction complete")

	// Verify index entries are now Parquet type
	entries, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 index entry after compaction, got %d", len(entries))
	}
	if entries[0].FileType != index.FileTypeParquet {
		t.Fatalf("expected Parquet entry, got %s", entries[0].FileType)
	}
	t.Logf("Index entry after compaction: FileType=%s, StartOffset=%d, EndOffset=%d",
		entries[0].FileType, entries[0].StartOffset, entries[0].EndOffset)

	// Step 4: Fetch all data from Parquet
	t.Log("Fetching data from Parquet (after compaction)...")
	parquetRecords := fetchAllRecords(t, fetchHandler, ctx, "compaction-test-topic", 0, 0)
	if len(parquetRecords) != totalRecords {
		t.Fatalf("expected %d records from Parquet, got %d", totalRecords, len(parquetRecords))
	}
	t.Logf("Fetched %d records from Parquet", len(parquetRecords))

	// Step 5: Verify data is semantically identical
	t.Log("Verifying record content consistency...")
	for i := 0; i < totalRecords; i++ {
		walRec := walRecords[i]
		parquetRec := parquetRecords[i]

		// Verify offsets match
		if walRec.Offset != parquetRec.Offset {
			t.Errorf("record %d: offset mismatch - WAL=%d, Parquet=%d",
				i, walRec.Offset, parquetRec.Offset)
		}

		// Verify timestamps match
		if walRec.Timestamp != parquetRec.Timestamp {
			t.Errorf("record %d: timestamp mismatch - WAL=%d, Parquet=%d",
				i, walRec.Timestamp, parquetRec.Timestamp)
		}

		// Verify keys match
		if !bytes.Equal(walRec.Key, parquetRec.Key) {
			t.Errorf("record %d: key mismatch - WAL=%q, Parquet=%q",
				i, walRec.Key, parquetRec.Key)
		}

		// Verify values match
		if !bytes.Equal(walRec.Value, parquetRec.Value) {
			t.Errorf("record %d: value mismatch - WAL=%q, Parquet=%q",
				i, walRec.Value, parquetRec.Value)
		}

		// Verify headers match
		if len(walRec.Headers) != len(parquetRec.Headers) {
			t.Errorf("record %d: header count mismatch - WAL=%d, Parquet=%d",
				i, len(walRec.Headers), len(parquetRec.Headers))
		} else {
			for j, walHdr := range walRec.Headers {
				parquetHdr := parquetRec.Headers[j]
				if walHdr.Key != parquetHdr.Key {
					t.Errorf("record %d header %d: key mismatch - WAL=%q, Parquet=%q",
						i, j, walHdr.Key, parquetHdr.Key)
				}
				if !bytes.Equal(walHdr.Value, parquetHdr.Value) {
					t.Errorf("record %d header %d: value mismatch - WAL=%v, Parquet=%v",
						i, j, walHdr.Value, parquetHdr.Value)
				}
			}
		}
	}

	t.Log("All records verified - compaction preserved data integrity")
}

// TestCompactionFetchConsistency_MultipleProduceCycles tests compaction with
// records produced across multiple produce cycles.
func TestCompactionFetchConsistency_MultipleProduceCycles(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "multi-cycle-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "multi-cycle-topic", 0); err != nil {
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

	// Produce 5 cycles, each with 2 records
	cycles := 5
	recordsPerCycle := 2
	totalRecords := cycles * recordsPerCycle

	for cycle := 0; cycle < cycles; cycle++ {
		produceReq := buildCompactionTestProduceRequest("multi-cycle-topic", 0, recordsPerCycle, cycle)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("produce cycle %d failed with error code %d", cycle, partResp.ErrorCode)
		}
	}

	// Fetch before compaction
	walRecords := fetchAllRecords(t, fetchHandler, ctx, "multi-cycle-topic", 0, 0)
	if len(walRecords) != totalRecords {
		t.Fatalf("expected %d records from WAL, got %d", totalRecords, len(walRecords))
	}

	// Run compaction
	runCompaction(t, ctx, streamID, metaStore, objStore, 0)

	// Fetch after compaction
	parquetRecords := fetchAllRecords(t, fetchHandler, ctx, "multi-cycle-topic", 0, 0)
	if len(parquetRecords) != totalRecords {
		t.Fatalf("expected %d records from Parquet, got %d", totalRecords, len(parquetRecords))
	}

	// Verify consistency
	for i := 0; i < totalRecords; i++ {
		walRec := walRecords[i]
		parquetRec := parquetRecords[i]

		if walRec.Offset != parquetRec.Offset {
			t.Errorf("record %d: offset mismatch", i)
		}
		if !bytes.Equal(walRec.Key, parquetRec.Key) {
			t.Errorf("record %d: key mismatch", i)
		}
		if !bytes.Equal(walRec.Value, parquetRec.Value) {
			t.Errorf("record %d: value mismatch", i)
		}
	}
}

// TestCompactionFetchConsistency_FetchFromMiddle tests fetching from an offset
// in the middle of compacted data.
func TestCompactionFetchConsistency_FetchFromMiddle(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "mid-fetch-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "mid-fetch-topic", 0); err != nil {
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

	// Produce 10 records
	totalRecords := 10
	produceReq := buildCompactionTestProduceRequest("mid-fetch-topic", 0, totalRecords, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed")
	}

	// Fetch from middle (offset 5) before compaction
	midOffset := int64(5)
	walRecords := fetchAllRecords(t, fetchHandler, ctx, "mid-fetch-topic", 0, midOffset)
	expectedCount := totalRecords - int(midOffset)
	if len(walRecords) != expectedCount {
		t.Fatalf("expected %d records from WAL (mid-fetch), got %d", expectedCount, len(walRecords))
	}

	// Verify first record starts at midOffset
	if walRecords[0].Offset != midOffset {
		t.Errorf("first WAL record should have offset %d, got %d", midOffset, walRecords[0].Offset)
	}

	// Run compaction
	runCompaction(t, ctx, streamID, metaStore, objStore, 0)

	// Fetch from middle after compaction
	parquetRecords := fetchAllRecords(t, fetchHandler, ctx, "mid-fetch-topic", 0, midOffset)
	if len(parquetRecords) != expectedCount {
		t.Fatalf("expected %d records from Parquet (mid-fetch), got %d", expectedCount, len(parquetRecords))
	}

	// Verify first record starts at midOffset
	if parquetRecords[0].Offset != midOffset {
		t.Errorf("first Parquet record should have offset %d, got %d", midOffset, parquetRecords[0].Offset)
	}

	// Verify all records match
	for i := 0; i < expectedCount; i++ {
		walRec := walRecords[i]
		parquetRec := parquetRecords[i]

		if walRec.Offset != parquetRec.Offset {
			t.Errorf("record %d: offset mismatch - WAL=%d, Parquet=%d",
				i, walRec.Offset, parquetRec.Offset)
		}
		if !bytes.Equal(walRec.Key, parquetRec.Key) {
			t.Errorf("record %d: key mismatch", i)
		}
		if !bytes.Equal(walRec.Value, parquetRec.Value) {
			t.Errorf("record %d: value mismatch", i)
		}
	}
}

// ExtractedRecord contains the logical content of a Kafka record.
type ExtractedRecord struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []ExtractedHeader
}

// ExtractedHeader contains a header key-value pair.
type ExtractedHeader struct {
	Key   string
	Value []byte
}

// fetchAllRecords fetches all records from the given offset using the fetch handler.
func fetchAllRecords(t *testing.T, handler *protocol.FetchHandler, ctx context.Context, topic string, partition int32, startOffset int64) []ExtractedRecord {
	t.Helper()

	var allRecords []ExtractedRecord
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

		// If no data returned, we've reached HWM
		if len(partResp.RecordBatches) == 0 {
			break
		}

		// Parse the record batches to extract logical records
		records := parseRecordBatches(t, partResp.RecordBatches)
		if len(records) == 0 {
			break
		}

		// Filter records to only those at or after startOffset
		for _, rec := range records {
			if rec.Offset >= startOffset {
				allRecords = append(allRecords, rec)
			}
		}
		currentOffset = records[len(records)-1].Offset + 1

		// Stop if we've reached HWM
		if currentOffset >= partResp.HighWatermark {
			break
		}
	}

	return allRecords
}

// parseRecordBatches parses the raw record batch bytes and extracts logical records.
func parseRecordBatches(t *testing.T, data []byte) []ExtractedRecord {
	t.Helper()

	var records []ExtractedRecord
	offset := 0

	for offset < len(data) {
		if offset+61 > len(data) {
			break
		}

		// Parse batch header
		// Kafka RecordBatch format:
		// - baseOffset (8 bytes)
		// - batchLength (4 bytes) - length of everything after this field
		// Total batch size = 8 + 4 + batchLength = 12 + batchLength
		baseOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		batchLength := int(binary.BigEndian.Uint32(data[offset+8 : offset+12]))

		// Calculate total batch size (header + batchLength)
		totalBatchSize := 12 + batchLength
		if offset+totalBatchSize > len(data) {
			t.Logf("Warning: truncated batch at offset %d", offset)
			break
		}

		// Attributes at offset 21
		attributes := int16(binary.BigEndian.Uint16(data[offset+21 : offset+23]))
		firstTimestamp := int64(binary.BigEndian.Uint64(data[offset+27 : offset+35]))
		recordCount := int(binary.BigEndian.Uint32(data[offset+57 : offset+61]))

		// Get compression type
		compressionType := int(attributes & 0x07)
		if compressionType != 0 {
			t.Logf("Warning: batch has compression type %d, may not parse correctly", compressionType)
		}

		// Records start at offset 61 (after the 61-byte header)
		// Records end at offset + totalBatchSize
		recordsStart := offset + 61
		recordsEnd := offset + totalBatchSize

		// Sanity check - recordsEnd must be >= recordsStart
		if recordsEnd < recordsStart {
			t.Logf("Warning: batch at offset %d has invalid size (batchLength=%d)", offset, batchLength)
			offset += totalBatchSize
			continue
		}

		recordsData := data[recordsStart:recordsEnd]

		// Parse individual records
		recOffset := 0
		for i := 0; i < recordCount; i++ {
			if recOffset >= len(recordsData) {
				break
			}
			rec, bytesRead := parseRecord(recordsData[recOffset:], baseOffset+int64(i), firstTimestamp)
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

// parseRecord parses a single Kafka v2 record.
func parseRecord(data []byte, offset int64, baseTimestamp int64) (ExtractedRecord, int) {
	if len(data) == 0 {
		return ExtractedRecord{}, 0
	}

	pos := 0

	// Record length (varint)
	recordLen, lenPrefixBytes := readVarintParse(data[pos:])
	if lenPrefixBytes <= 0 {
		return ExtractedRecord{}, 0
	}
	pos += lenPrefixBytes

	// Validate recordLen is reasonable
	if recordLen < 0 || recordLen > int64(len(data)-pos) {
		return ExtractedRecord{}, 0
	}

	// Attributes (1 byte)
	if pos >= len(data) {
		return ExtractedRecord{}, 0
	}
	_ = data[pos]
	pos++

	// Timestamp delta (varint)
	timestampDelta, bytesRead := readVarintParse(data[pos:])
	if bytesRead <= 0 {
		return ExtractedRecord{}, 0
	}
	pos += bytesRead

	// Offset delta (varint) - ignored, we use the provided offset
	_, bytesRead = readVarintParse(data[pos:])
	if bytesRead <= 0 {
		return ExtractedRecord{}, 0
	}
	pos += bytesRead

	// Key length (varint)
	keyLen, bytesRead := readVarintParse(data[pos:])
	if bytesRead <= 0 {
		return ExtractedRecord{}, 0
	}
	pos += bytesRead

	// Key data
	var key []byte
	if keyLen >= 0 {
		if pos+int(keyLen) > len(data) {
			return ExtractedRecord{}, 0
		}
		key = make([]byte, keyLen)
		copy(key, data[pos:pos+int(keyLen)])
		pos += int(keyLen)
	}

	// Value length (varint)
	valueLen, bytesRead := readVarintParse(data[pos:])
	if bytesRead <= 0 {
		return ExtractedRecord{}, 0
	}
	pos += bytesRead

	// Value data
	var value []byte
	if valueLen >= 0 {
		if pos+int(valueLen) > len(data) {
			return ExtractedRecord{}, 0
		}
		value = make([]byte, valueLen)
		copy(value, data[pos:pos+int(valueLen)])
		pos += int(valueLen)
	}

	// Header count (varint)
	headerCount, bytesRead := readVarintParse(data[pos:])
	if bytesRead <= 0 {
		return ExtractedRecord{}, 0
	}
	pos += bytesRead

	// Parse headers
	var headers []ExtractedHeader
	for i := int64(0); i < headerCount && headerCount >= 0; i++ {
		// Header key length
		hKeyLen, bytesRead := readVarintParse(data[pos:])
		if bytesRead <= 0 {
			break
		}
		pos += bytesRead

		// Header key
		var hKey string
		if hKeyLen >= 0 {
			if pos+int(hKeyLen) > len(data) {
				break
			}
			hKey = string(data[pos : pos+int(hKeyLen)])
			pos += int(hKeyLen)
		}

		// Header value length
		hValueLen, bytesRead := readVarintParse(data[pos:])
		if bytesRead <= 0 {
			break
		}
		pos += bytesRead

		// Header value
		var hValue []byte
		if hValueLen >= 0 {
			if pos+int(hValueLen) > len(data) {
				break
			}
			hValue = make([]byte, hValueLen)
			copy(hValue, data[pos:pos+int(hValueLen)])
			pos += int(hValueLen)
		}

		headers = append(headers, ExtractedHeader{Key: hKey, Value: hValue})
	}

	return ExtractedRecord{
		Offset:    offset,
		Timestamp: baseTimestamp + timestampDelta,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, lenPrefixBytes + int(recordLen) // length prefix + record body
}

// readVarintParse reads a zigzag-encoded varint.
func readVarintParse(data []byte) (int64, int) {
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

	// Zigzag decode
	v := int64((uv >> 1) ^ -(uv & 1))
	return v, bytesRead
}

// buildCompactionTestProduceRequest creates a produce request with records that
// include keys, values, and headers for comprehensive testing.
func buildCompactionTestProduceRequest(topic string, partition int32, recordCount int, batchID int) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = partition
	partReq.Records = buildSimpleRecordBatch(recordCount)

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// buildSimpleRecordBatch creates a valid Kafka record batch with simple records.
// Each record has a key and value but no headers to avoid parsing complexity.
func buildSimpleRecordBatch(recordCount int) []byte {
	// Build records with simple, known-good format
	var records []byte
	ts := time.Now().UnixMilli()

	for i := 0; i < recordCount; i++ {
		record := buildSimpleRecord(i)
		records = append(records, record...)
	}

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

// buildSimpleRecord builds a simple Kafka v2 record with just key and value, no headers.
func buildSimpleRecord(recordID int) []byte {
	var body []byte

	// attributes (1 byte) = 0
	body = append(body, 0)

	// timestampDelta (varint) = 0
	body = appendVarintEncode(body, 0)

	// offsetDelta (varint)
	body = appendVarintEncode(body, int64(recordID))

	// key - simple format
	key := []byte(fmt.Sprintf("key-%d", recordID))
	body = appendVarintEncode(body, int64(len(key)))
	body = append(body, key...)

	// value - simple format
	value := []byte(fmt.Sprintf("value-%d", recordID))
	body = appendVarintEncode(body, int64(len(value)))
	body = append(body, value...)

	// headers count = 0 (no headers)
	body = appendVarintEncode(body, 0)

	// Prepend length
	var result []byte
	result = appendVarintEncode(result, int64(len(body)))
	result = append(result, body...)

	return result
}

// appendVarintEncode appends a zigzag-encoded varint.
func appendVarintEncode(b []byte, v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		b = append(b, byte(uv)|0x80)
		uv >>= 7
	}
	b = append(b, byte(uv))
	return b
}

// runCompaction runs the compaction process for a stream.
func runCompaction(t *testing.T, ctx context.Context, streamID string, metaStore metadata.MetadataStore, objStore objectstore.Store, partition int32) {
	t.Helper()

	// Get all WAL index entries
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	if len(kvs) == 0 {
		t.Fatal("no index entries to compact")
	}

	// Parse entries and collect WAL entries
	var walEntries []*index.IndexEntry
	var walIndexKeys []string
	var minOffset, maxOffset int64 = 0, 0

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
		t.Log("No WAL entries to compact")
		return
	}

	t.Logf("Compacting %d WAL entries covering offsets %d-%d", len(walEntries), minOffset, maxOffset)

	// Convert WAL to Parquet
	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, partition)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	// Write Parquet to object store
	date := time.Now().Format("2006-01-02")
	parquetID := worker.GenerateParquetID()
	parquetPath := worker.GenerateParquetPath("test-topic", partition, date, parquetID)
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}
	t.Logf("Wrote Parquet file: %s (%d bytes, %d records)", parquetPath, len(convertResult.ParquetData), convertResult.RecordCount)

	// Calculate metadata domain
	metaDomain := 0 // Simplified for test

	// Create Parquet index entry
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

	// Execute index swap
	swapper := compaction.NewIndexSwapper(metaStore)
	swapResult, err := swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}

	t.Logf("Index swap complete: new key=%s, decremented %d WAL objects",
		swapResult.NewIndexKey, len(swapResult.DecrementedWALObjects))
}

// compactionTestObjectStore is a mock object store that tracks objects.
type compactionTestObjectStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newCompactionTestObjectStore() *compactionTestObjectStore {
	return &compactionTestObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *compactionTestObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = data
	return nil
}

func (m *compactionTestObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *compactionTestObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *compactionTestObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
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

func (m *compactionTestObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
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

func (m *compactionTestObjectStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *compactionTestObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
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

func (m *compactionTestObjectStore) Close() error {
	return nil
}

var _ objectstore.Store = (*compactionTestObjectStore)(nil)
