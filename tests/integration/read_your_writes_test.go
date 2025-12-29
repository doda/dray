package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
	"github.com/dray-io/dray/internal/wal"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestReadYourWrites_ImmediateVisibility tests the read-your-writes invariant I2:
// After a produce request is acknowledged, the produced records must be
// immediately visible to subsequent fetch requests.
func TestReadYourWrites_ImmediateVisibility(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a topic with one partition
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create committer for the produce buffer
	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	// Create produce buffer with the committer's flush handler
	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush for tests
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	// Create produce handler
	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Create fetch handler
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Step 1: Produce records and wait for ack
	recordCount := 5
	produceReq := buildProduceRequest("test-topic", 0, recordCount)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)

	// Verify produce succeeded
	if len(produceResp.Topics) != 1 || len(produceResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected produce response structure")
	}

	producePartResp := produceResp.Topics[0].Partitions[0]
	if producePartResp.ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", producePartResp.ErrorCode)
	}

	ackedBaseOffset := producePartResp.BaseOffset
	t.Logf("Produce acked with BaseOffset=%d", ackedBaseOffset)

	// Step 2: Immediately fetch from same connection (same handlers)
	fetchReq := buildFetchRequest("test-topic", 0, ackedBaseOffset)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	// Step 3: Verify fetched records include just-produced data
	if len(fetchResp.Topics) != 1 || len(fetchResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected fetch response structure")
	}

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Errorf("fetch failed with error code %d", fetchPartResp.ErrorCode)
	}

	// Step 4: Verify HWM reflects the produced records
	expectedHWM := ackedBaseOffset + int64(recordCount)
	if fetchPartResp.HighWatermark != expectedHWM {
		t.Errorf("expected HWM=%d (base %d + count %d), got %d",
			expectedHWM, ackedBaseOffset, recordCount, fetchPartResp.HighWatermark)
	}

	// Verify we got record batches back
	if len(fetchPartResp.RecordBatches) == 0 {
		t.Error("expected record batches in fetch response, but got none")
	}

	t.Logf("Fetch returned HWM=%d with %d bytes of record batches",
		fetchPartResp.HighWatermark, len(fetchPartResp.RecordBatches))
}

// TestReadYourWrites_OffsetMatchesAck verifies that the offsets returned in the
// fetch response exactly match the offsets acked by the produce response.
func TestReadYourWrites_OffsetMatchesAck(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0); err != nil {
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

	// Produce first batch of 3 records
	produceReq1 := buildProduceRequest("test-topic", 0, 3)
	produceResp1 := produceHandler.Handle(ctx, 9, produceReq1)

	partResp1 := produceResp1.Topics[0].Partitions[0]
	if partResp1.ErrorCode != 0 {
		t.Fatalf("first produce failed with error code %d", partResp1.ErrorCode)
	}
	baseOffset1 := partResp1.BaseOffset
	t.Logf("First produce: BaseOffset=%d", baseOffset1)

	// Produce second batch of 5 records
	produceReq2 := buildProduceRequest("test-topic", 0, 5)
	produceResp2 := produceHandler.Handle(ctx, 9, produceReq2)

	partResp2 := produceResp2.Topics[0].Partitions[0]
	if partResp2.ErrorCode != 0 {
		t.Fatalf("second produce failed with error code %d", partResp2.ErrorCode)
	}
	baseOffset2 := partResp2.BaseOffset
	t.Logf("Second produce: BaseOffset=%d", baseOffset2)

	// Verify second offset starts where first ended
	if baseOffset2 != baseOffset1+3 {
		t.Errorf("expected second baseOffset=%d, got %d", baseOffset1+3, baseOffset2)
	}

	// Fetch from offset 0 to get all records
	fetchReq := buildFetchRequest("test-topic", 0, 0)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Errorf("fetch failed with error code %d", fetchPartResp.ErrorCode)
	}

	// Verify HWM matches total records produced
	expectedHWM := baseOffset1 + 3 + 5 // 0 + 3 + 5 = 8
	if fetchPartResp.HighWatermark != expectedHWM {
		t.Errorf("expected HWM=%d, got %d", expectedHWM, fetchPartResp.HighWatermark)
	}

	t.Logf("Final HWM=%d matches expected total offsets", fetchPartResp.HighWatermark)
}

// TestReadYourWrites_FetchFromMidOffset verifies that fetching from an offset
// in the middle of produced records returns the correct subset.
func TestReadYourWrites_FetchFromMidOffset(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0); err != nil {
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
	produceReq := buildProduceRequest("test-topic", 0, 10)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)

	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", partResp.ErrorCode)
	}

	baseOffset := partResp.BaseOffset
	t.Logf("Produce: BaseOffset=%d (10 records)", baseOffset)

	// Fetch from middle offset (offset 5)
	midOffset := baseOffset + 5
	fetchReq := buildFetchRequest("test-topic", 0, midOffset)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Errorf("fetch failed with error code %d", fetchPartResp.ErrorCode)
	}

	// HWM should still be total records
	expectedHWM := baseOffset + 10
	if fetchPartResp.HighWatermark != expectedHWM {
		t.Errorf("expected HWM=%d, got %d", expectedHWM, fetchPartResp.HighWatermark)
	}

	// Should have record batches (the batch containing offset 5+)
	if len(fetchPartResp.RecordBatches) == 0 {
		t.Error("expected record batches when fetching from mid-offset")
	}

	t.Logf("Fetch from offset %d returned HWM=%d with %d bytes",
		midOffset, fetchPartResp.HighWatermark, len(fetchPartResp.RecordBatches))
}

// TestReadYourWrites_FetchAtHWM verifies that fetching at HWM returns empty
// (consumer is at end of log).
func TestReadYourWrites_FetchAtHWM(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0); err != nil {
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

	// Produce 5 records
	produceReq := buildProduceRequest("test-topic", 0, 5)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)

	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", partResp.ErrorCode)
	}

	hwm := partResp.BaseOffset + 5 // HWM should be base + count

	// Fetch at HWM (consumer caught up)
	fetchReq := buildFetchRequest("test-topic", 0, hwm)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Errorf("fetch at HWM should succeed, got error code %d", fetchPartResp.ErrorCode)
	}

	// HWM in response should match what we expect
	if fetchPartResp.HighWatermark != hwm {
		t.Errorf("expected HWM=%d, got %d", hwm, fetchPartResp.HighWatermark)
	}

	// Should have no record batches (at end of log)
	if len(fetchPartResp.RecordBatches) != 0 {
		t.Errorf("expected no record batches at HWM, got %d bytes", len(fetchPartResp.RecordBatches))
	}

	t.Logf("Fetch at HWM=%d correctly returned empty response", hwm)
}

// TestReadYourWrites_MultiplePartitions verifies read-your-writes across
// multiple partitions in parallel.
func TestReadYourWrites_MultiplePartitions(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create topic with 3 partitions
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create streams for all partitions
	for _, p := range result.Partitions {
		if err := streamManager.CreateStreamWithID(ctx, p.StreamID, "test-topic", p.Partition); err != nil {
			t.Fatalf("failed to create stream for partition %d: %v", p.Partition, err)
		}
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

	// Produce to all 3 partitions with different record counts
	recordCounts := []int{3, 5, 7}
	baseOffsets := make([]int64, 3)

	for partition, count := range recordCounts {
		produceReq := buildProduceRequest("test-topic", int32(partition), count)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("produce to partition %d failed with error code %d", partition, partResp.ErrorCode)
		}
		baseOffsets[partition] = partResp.BaseOffset
		t.Logf("Partition %d: produced %d records, BaseOffset=%d", partition, count, baseOffsets[partition])
	}

	// Fetch from all partitions and verify
	for partition, count := range recordCounts {
		fetchReq := buildFetchRequest("test-topic", int32(partition), baseOffsets[partition])
		fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

		fetchPartResp := fetchResp.Topics[0].Partitions[0]
		if fetchPartResp.ErrorCode != 0 {
			t.Errorf("fetch from partition %d failed with error code %d", partition, fetchPartResp.ErrorCode)
		}

		expectedHWM := baseOffsets[partition] + int64(count)
		if fetchPartResp.HighWatermark != expectedHWM {
			t.Errorf("partition %d: expected HWM=%d, got %d", partition, expectedHWM, fetchPartResp.HighWatermark)
		}

		if len(fetchPartResp.RecordBatches) == 0 {
			t.Errorf("partition %d: expected record batches, got none", partition)
		}

		t.Logf("Partition %d: fetch verified HWM=%d", partition, fetchPartResp.HighWatermark)
	}
}

// TestReadYourWrites_SequentialProduces verifies that multiple sequential
// produces maintain correct offset allocation visible to fetches.
func TestReadYourWrites_SequentialProduces(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0); err != nil {
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

	// Perform 5 sequential produce-then-fetch cycles
	expectedNextOffset := int64(0)

	for i := 0; i < 5; i++ {
		recordCount := i + 1 // 1, 2, 3, 4, 5 records

		// Produce
		produceReq := buildProduceRequest("test-topic", 0, recordCount)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("cycle %d: produce failed with error code %d", i, partResp.ErrorCode)
		}

		// Verify base offset matches expected
		if partResp.BaseOffset != expectedNextOffset {
			t.Errorf("cycle %d: expected BaseOffset=%d, got %d", i, expectedNextOffset, partResp.BaseOffset)
		}

		// Immediately fetch
		fetchReq := buildFetchRequest("test-topic", 0, partResp.BaseOffset)
		fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

		fetchPartResp := fetchResp.Topics[0].Partitions[0]
		if fetchPartResp.ErrorCode != 0 {
			t.Errorf("cycle %d: fetch failed with error code %d", i, fetchPartResp.ErrorCode)
		}

		// Verify HWM reflects all produced records so far
		expectedHWM := expectedNextOffset + int64(recordCount)
		if fetchPartResp.HighWatermark != expectedHWM {
			t.Errorf("cycle %d: expected HWM=%d, got %d", i, expectedHWM, fetchPartResp.HighWatermark)
		}

		// Update expected offset for next cycle
		expectedNextOffset = expectedHWM

		t.Logf("Cycle %d: produced %d records at offset %d, HWM=%d",
			i, recordCount, partResp.BaseOffset, fetchPartResp.HighWatermark)
	}

	// Final verification: fetch from 0 should see all 1+2+3+4+5=15 records
	finalFetchReq := buildFetchRequest("test-topic", 0, 0)
	finalFetchResp := fetchHandler.Handle(ctx, 12, finalFetchReq)

	finalPartResp := finalFetchResp.Topics[0].Partitions[0]
	if finalPartResp.HighWatermark != 15 {
		t.Errorf("final fetch: expected HWM=15, got %d", finalPartResp.HighWatermark)
	}

	if len(finalPartResp.RecordBatches) == 0 {
		t.Error("final fetch: expected record batches")
	}

	t.Logf("Final verification: HWM=%d with %d bytes of records",
		finalPartResp.HighWatermark, len(finalPartResp.RecordBatches))
}

// buildProduceRequest creates a produce request with the specified number of records.
func buildProduceRequest(topic string, partition int32, recordCount int) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = partition
	partReq.Records = buildRecordBatch(recordCount)

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// buildFetchRequest creates a fetch request for the specified topic/partition/offset.
func buildFetchRequest(topic string, partition int32, fetchOffset int64) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(12)
	req.MaxBytes = 1024 * 1024

	topicReq := kmsg.NewFetchRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewFetchRequestTopicPartition()
	partReq.Partition = partition
	partReq.FetchOffset = fetchOffset
	partReq.PartitionMaxBytes = 1024 * 1024

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// buildRecordBatch creates a valid Kafka record batch with the specified record count.
func buildRecordBatch(recordCount int) []byte {
	var records []byte
	for i := 0; i < recordCount; i++ {
		recordBody := []byte{0}
		recordBody = appendVarint(recordBody, 0)
		recordBody = appendVarint(recordBody, int64(i))
		recordBody = appendVarint(recordBody, -1)
		recordBody = appendVarint(recordBody, -1)
		recordBody = appendVarint(recordBody, 0)

		var record []byte
		record = appendVarint(record, int64(len(recordBody)))
		record = append(record, recordBody...)
		records = append(records, record...)
	}

	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + len(records)
	totalSize := 8 + 4 + batchLength
	batch := make([]byte, totalSize)

	offset := 0
	binary.BigEndian.PutUint64(batch[offset:], 0)
	offset += 8

	binary.BigEndian.PutUint32(batch[offset:], uint32(batchLength))
	offset += 4

	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	batch[offset] = 2
	offset++

	crcOffset := offset
	offset += 4
	crcStart := offset

	binary.BigEndian.PutUint16(batch[offset:], 0)
	offset += 2

	lastOffsetDelta := int32(0)
	if recordCount > 0 {
		lastOffsetDelta = int32(recordCount - 1)
	}
	binary.BigEndian.PutUint32(batch[offset:], uint32(lastOffsetDelta))
	offset += 4

	ts := time.Now().UnixMilli()
	binary.BigEndian.PutUint64(batch[offset:], uint64(ts))
	offset += 8

	binary.BigEndian.PutUint64(batch[offset:], uint64(ts))
	offset += 8

	binary.BigEndian.PutUint64(batch[offset:], 0xFFFFFFFFFFFFFFFF)
	offset += 8

	binary.BigEndian.PutUint16(batch[offset:], 0xFFFF)
	offset += 2

	binary.BigEndian.PutUint32(batch[offset:], 0xFFFFFFFF)
	offset += 4

	binary.BigEndian.PutUint32(batch[offset:], uint32(recordCount))
	offset += 4

	copy(batch[offset:], records)

	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[crcStart:], table)
	binary.BigEndian.PutUint32(batch[crcOffset:], crcValue)

	return batch
}

// appendVarint appends a signed varint to the byte slice using zigzag encoding.
func appendVarint(b []byte, v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		b = append(b, byte(uv)|0x80)
		uv >>= 7
	}
	b = append(b, byte(uv))
	return b
}

// mockObjectStore implements objectstore.Store for testing.
type mockObjectStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *mockObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return nil
}

func (m *mockObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *mockObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	data, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.RLock()
	data, ok := m.objects[key]
	m.mu.RUnlock()
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

func (m *mockObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	m.mu.RLock()
	data, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *mockObjectStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func (m *mockObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
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

func (m *mockObjectStore) Close() error {
	return nil
}

// parseStreamIDToUint64 parses a UUID string to uint64 for WAL encoding.
func parseStreamIDToUint64(streamID string) uint64 {
	u, err := uuid.Parse(streamID)
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(u[:8])
}

// Ensure mockObjectStore satisfies the interface.
var _ objectstore.Store = (*mockObjectStore)(nil)

// Ensure wal is used (for WAL encoding in tests).
var _ = wal.HeaderSize
