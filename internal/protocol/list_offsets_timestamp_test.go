package protocol

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// buildRecordBatchWithTimestamps creates a Kafka record batch with specific timestamps.
// This allows testing timestamp-based offset lookups with known values.
func buildRecordBatchWithTimestamps(recordCount int, firstTimestamp, maxTimestamp int64) []byte {
	batch := make([]byte, 0, 80)

	// baseOffset (8 bytes) = 0
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// batchLength (4 bytes)
	batch = append(batch, 0, 0, 0, 49)

	// partitionLeaderEpoch (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// magic (1 byte) = 2
	batch = append(batch, 2)

	// crc (4 bytes) - placeholder, will be set below
	batch = append(batch, 0, 0, 0, 0)

	// attributes (2 bytes)
	batch = append(batch, 0, 0)

	// lastOffsetDelta (4 bytes)
	batch = append(batch, 0, 0, 0, byte(recordCount-1))

	// firstTimestamp (8 bytes)
	batch = append(batch,
		byte(firstTimestamp>>56), byte(firstTimestamp>>48), byte(firstTimestamp>>40), byte(firstTimestamp>>32),
		byte(firstTimestamp>>24), byte(firstTimestamp>>16), byte(firstTimestamp>>8), byte(firstTimestamp))

	// maxTimestamp (8 bytes)
	batch = append(batch,
		byte(maxTimestamp>>56), byte(maxTimestamp>>48), byte(maxTimestamp>>40), byte(maxTimestamp>>32),
		byte(maxTimestamp>>24), byte(maxTimestamp>>16), byte(maxTimestamp>>8), byte(maxTimestamp))

	// producerId (8 bytes) = -1 (non-idempotent)
	batch = append(batch, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)

	// producerEpoch (2 bytes) = -1
	batch = append(batch, 0xff, 0xff)

	// firstSequence (4 bytes) = -1
	batch = append(batch, 0xff, 0xff, 0xff, 0xff)

	// recordCount (4 bytes)
	batch = append(batch, 0, 0, 0, byte(recordCount))

	// Calculate and set CRC over bytes from offset 21 onwards (attributes to end)
	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[21:], table)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

// TestListOffsetsTimestamp_ProduceThenQuery simulates producing messages with known timestamps
// and then queries ListOffsets with various timestamp values to verify correct offset lookup.
// This tests the end-to-end behavior per task requirements:
// 1. Produce messages with known timestamps
// 2. Query ListOffsets for timestamp in middle
// 3. Verify returned offset is first record >= timestamp
// 4. Test edge cases: before first, after last
func TestListOffsetsTimestamp_ProduceThenQuery(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	// Create a test topic with 1 partition
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "timestamp-test-topic",
		PartitionCount: 1,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID

	// Create the stream
	err = streamManager.CreateStreamWithID(ctx, streamID, "timestamp-test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Simulate producing 3 batches with known timestamps:
	// Batch 1: offsets 0-4, timestamps 1000-1500
	// Batch 2: offsets 5-9, timestamps 2000-2500
	// Batch 3: offsets 10-14, timestamps 3000-3500
	//
	// This simulates what the produce handler would do after commit.

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		WalID:          "wal-batch-1",
		WalPath:        "wal/batch-1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
		BatchIndex: []index.BatchIndexEntry{
			{
				BatchStartOffsetDelta: 0,
				BatchLastOffsetDelta:  4,
				BatchOffsetInChunk:    0,
				BatchLength:           100,
				MinTimestampMs:        1000,
				MaxTimestampMs:        1500,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to append batch 1: %v", err)
	}

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    2000,
		MinTimestampMs: 2000,
		MaxTimestampMs: 2500,
		WalID:          "wal-batch-2",
		WalPath:        "wal/batch-2.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
		BatchIndex: []index.BatchIndexEntry{
			{
				BatchStartOffsetDelta: 0,
				BatchLastOffsetDelta:  4,
				BatchOffsetInChunk:    0,
				BatchLength:           100,
				MinTimestampMs:        2000,
				MaxTimestampMs:        2500,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to append batch 2: %v", err)
	}

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    3000,
		MinTimestampMs: 3000,
		MaxTimestampMs: 3500,
		WalID:          "wal-batch-3",
		WalPath:        "wal/batch-3.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
		BatchIndex: []index.BatchIndexEntry{
			{
				BatchStartOffsetDelta: 0,
				BatchLastOffsetDelta:  4,
				BatchOffsetInChunk:    0,
				BatchLength:           100,
				MinTimestampMs:        3000,
				MaxTimestampMs:        3500,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to append batch 3: %v", err)
	}

	// Now create the ListOffsets handler and run timestamp queries
	handler := NewListOffsetsHandler(topicStore, streamManager)

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
		expectFound    bool
		description    string
	}{
		{
			name:           "timestamp_before_first_record",
			timestamp:      500,
			expectedOffset: 0,
			expectFound:    true,
			description:    "timestamp before all records should return first offset (0)",
		},
		{
			name:           "timestamp_at_exact_start",
			timestamp:      1000,
			expectedOffset: 0,
			expectFound:    true,
			description:    "timestamp exactly at first batch start should return offset 0",
		},
		{
			name:           "timestamp_in_first_batch",
			timestamp:      1200,
			expectedOffset: 0,
			expectFound:    true,
			description:    "timestamp in first batch should return offset 0 (batch start)",
		},
		{
			name:           "timestamp_between_batches",
			timestamp:      1700,
			expectedOffset: 5,
			expectFound:    true,
			description:    "timestamp between batch 1 (max 1500) and batch 2 (min 2000) should return batch 2 start (offset 5)",
		},
		{
			name:           "timestamp_at_second_batch_start",
			timestamp:      2000,
			expectedOffset: 5,
			expectFound:    true,
			description:    "timestamp exactly at second batch start should return offset 5",
		},
		{
			name:           "timestamp_in_middle_of_second_batch",
			timestamp:      2200,
			expectedOffset: 5,
			expectFound:    true,
			description:    "timestamp in middle of second batch should return batch start (offset 5)",
		},
		{
			name:           "timestamp_in_third_batch",
			timestamp:      3200,
			expectedOffset: 10,
			expectFound:    true,
			description:    "timestamp in third batch should return batch start (offset 10)",
		},
		{
			name:           "timestamp_at_last_batch_end",
			timestamp:      3500,
			expectedOffset: 10,
			expectFound:    true,
			description:    "timestamp at last batch maxTimestamp should still return batch start (offset 10)",
		},
		{
			name:           "timestamp_after_all_records",
			timestamp:      5000,
			expectedOffset: -1,
			expectFound:    false,
			description:    "timestamp after all records should return -1 (not found)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildListOffsetsRequest("timestamp-test-topic", 0, tt.timestamp)
			resp := handler.Handle(ctx, 5, req)

			if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
				t.Fatalf("unexpected response structure")
			}

			partResp := resp.Topics[0].Partitions[0]

			if partResp.ErrorCode != errNoError {
				t.Errorf("expected no error, got error code %d", partResp.ErrorCode)
			}

			if partResp.Offset != tt.expectedOffset {
				t.Errorf("%s: expected offset=%d, got %d",
					tt.description, tt.expectedOffset, partResp.Offset)
			}

			// For not-found case, verify timestamp is also -1
			if !tt.expectFound && partResp.Timestamp != -1 {
				t.Errorf("for not-found case, expected timestamp=-1, got %d", partResp.Timestamp)
			}
		})
	}
}

// TestListOffsetsTimestamp_EmptyPartition tests timestamp lookup on an empty partition.
func TestListOffsetsTimestamp_EmptyPartition(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "empty-topic",
		PartitionCount: 1,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "empty-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)
	req := buildListOffsetsRequest("empty-topic", 0, 1000)
	resp := handler.Handle(ctx, 5, req)

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != errNoError {
		t.Errorf("expected no error, got error code %d", partResp.ErrorCode)
	}

	// Empty partition should return -1 (not found)
	if partResp.Offset != -1 {
		t.Errorf("expected offset=-1 for empty partition, got %d", partResp.Offset)
	}

	if partResp.Timestamp != -1 {
		t.Errorf("expected timestamp=-1 for empty partition, got %d", partResp.Timestamp)
	}
}

// TestListOffsetsTimestamp_FirstRecordGteSemantics verifies the "first record >= timestamp" semantics.
// Per Kafka protocol, ListOffsets TIMESTAMP should return the offset of the first record whose
// timestamp is >= the requested timestamp.
func TestListOffsetsTimestamp_FirstRecordGteSemantics(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "gte-test-topic",
		PartitionCount: 1,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "gte-test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create a single batch with 10 records, timestamps 1000-2000
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    10,
		ChunkSizeBytes: 200,
		CreatedAtMs:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    200,
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	// Test: timestamp 1500 is within the batch's range [1000, 2000]
	// Since we can't determine exact record-level timestamps from batch-level min/max,
	// we return the batch's start offset as the "first record >= timestamp" candidate.
	req := buildListOffsetsRequest("gte-test-topic", 0, 1500)
	resp := handler.Handle(ctx, 5, req)

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != errNoError {
		t.Errorf("expected no error, got error code %d", partResp.ErrorCode)
	}

	// Should return offset 0 (start of the batch containing timestamps in the range)
	if partResp.Offset != 0 {
		t.Errorf("expected offset=0, got %d", partResp.Offset)
	}
}

// TestListOffsetsTimestamp_MultiplePartitions tests timestamp lookup across multiple partitions.
func TestListOffsetsTimestamp_MultiplePartitions(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "multi-partition-topic",
		PartitionCount: 3,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create streams and add different data to each partition
	for i, p := range result.Partitions {
		err := streamManager.CreateStreamWithID(ctx, p.StreamID, "multi-partition-topic", p.Partition)
		if err != nil {
			t.Fatalf("failed to create stream for partition %d: %v", i, err)
		}

		// Add data with different timestamp ranges per partition
		// Partition 0: timestamps 1000-1500
		// Partition 1: timestamps 2000-2500
		// Partition 2: empty
		if i < 2 {
			baseTs := int64((i + 1) * 1000)
			_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       p.StreamID,
				RecordCount:    5,
				ChunkSizeBytes: 100,
				CreatedAtMs:    baseTs,
				MinTimestampMs: baseTs,
				MaxTimestampMs: baseTs + 500,
				WalID:          "wal-" + p.StreamID,
				WalPath:        "wal/" + p.StreamID + ".wal",
				ChunkOffset:    0,
				ChunkLength:    100,
			})
			if err != nil {
				t.Fatalf("failed to append index entry for partition %d: %v", i, err)
			}
		}
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	// Build request for all 3 partitions with timestamp 1200
	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(5)
	req.ReplicaID = -1

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = "multi-partition-topic"

	for i := int32(0); i < 3; i++ {
		partReq := kmsg.NewListOffsetsRequestTopicPartition()
		partReq.Partition = i
		partReq.Timestamp = 1200 // Falls in partition 0's range, before partition 1's range
		topicReq.Partitions = append(topicReq.Partitions, partReq)
	}

	req.Topics = append(req.Topics, topicReq)
	resp := handler.Handle(ctx, 5, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 3 {
		t.Fatalf("expected 3 partition responses, got %d", len(resp.Topics[0].Partitions))
	}

	// Partition 0: timestamp 1200 is within [1000, 1500], should return offset 0
	if resp.Topics[0].Partitions[0].Offset != 0 {
		t.Errorf("partition 0: expected offset=0, got %d", resp.Topics[0].Partitions[0].Offset)
	}

	// Partition 1: timestamp 1200 is before [2000, 2500], should return offset 0 (first record >= 1200)
	if resp.Topics[0].Partitions[1].Offset != 0 {
		t.Errorf("partition 1: expected offset=0, got %d", resp.Topics[0].Partitions[1].Offset)
	}

	// Partition 2: empty, should return -1
	if resp.Topics[0].Partitions[2].Offset != -1 {
		t.Errorf("partition 2: expected offset=-1, got %d", resp.Topics[0].Partitions[2].Offset)
	}
}

// TestListOffsetsTimestamp_BatchIndexGranularity tests that batchIndex provides finer-grained
// timestamp lookup within a single index entry containing multiple batches.
func TestListOffsetsTimestamp_BatchIndexGranularity(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "batch-index-topic",
		PartitionCount: 1,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "batch-index-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create a single index entry containing multiple batches with distinct timestamp ranges
	// Batch 1: offsets 0-4, timestamps 1000-1500
	// Batch 2: offsets 5-9, timestamps 2000-2500
	// Batch 3: offsets 10-14, timestamps 3000-3500
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    15,
		ChunkSizeBytes: 300,
		CreatedAtMs:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 3500,
		WalID:          "wal-multi-batch",
		WalPath:        "wal/multi-batch.wal",
		ChunkOffset:    0,
		ChunkLength:    300,
		BatchIndex: []index.BatchIndexEntry{
			{
				BatchStartOffsetDelta: 0,
				BatchLastOffsetDelta:  4,
				BatchOffsetInChunk:    0,
				BatchLength:           100,
				MinTimestampMs:        1000,
				MaxTimestampMs:        1500,
			},
			{
				BatchStartOffsetDelta: 5,
				BatchLastOffsetDelta:  9,
				BatchOffsetInChunk:    100,
				BatchLength:           100,
				MinTimestampMs:        2000,
				MaxTimestampMs:        2500,
			},
			{
				BatchStartOffsetDelta: 10,
				BatchLastOffsetDelta:  14,
				BatchOffsetInChunk:    200,
				BatchLength:           100,
				MinTimestampMs:        3000,
				MaxTimestampMs:        3500,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
	}{
		{"in_first_batch", 1200, 0},
		{"between_first_and_second", 1700, 5},
		{"in_second_batch", 2200, 5},
		{"between_second_and_third", 2700, 10},
		{"in_third_batch", 3200, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildListOffsetsRequest("batch-index-topic", 0, tt.timestamp)
			resp := handler.Handle(ctx, 5, req)

			partResp := resp.Topics[0].Partitions[0]

			if partResp.ErrorCode != errNoError {
				t.Errorf("expected no error, got error code %d", partResp.ErrorCode)
			}

			if partResp.Offset != tt.expectedOffset {
				t.Errorf("timestamp %d: expected offset=%d, got %d",
					tt.timestamp, tt.expectedOffset, partResp.Offset)
			}
		})
	}
}

// TestListOffsetsTimestamp_EdgeCaseExactBoundaries tests exact boundary timestamp values.
func TestListOffsetsTimestamp_EdgeCaseExactBoundaries(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "boundary-topic",
		PartitionCount: 1,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "boundary-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create two batches with contiguous timestamp ranges
	// Batch 1: timestamps 1000-2000
	// Batch 2: timestamps 2000-3000 (starts exactly where batch 1 ends)
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append batch 1: %v", err)
	}

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    2000,
		MinTimestampMs: 2000,
		MaxTimestampMs: 3000,
		WalID:          "wal-2",
		WalPath:        "wal/2.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append batch 2: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
		description    string
	}{
		{
			name:           "at_first_batch_min",
			timestamp:      1000,
			expectedOffset: 0,
			description:    "exactly at first batch min timestamp",
		},
		{
			name:           "at_first_batch_max",
			timestamp:      2000,
			expectedOffset: 0,
			description:    "exactly at first batch max timestamp (also second batch min)",
		},
		{
			name:           "one_after_first_batch_max",
			timestamp:      2001,
			expectedOffset: 5,
			description:    "just after first batch max should return second batch",
		},
		{
			name:           "at_second_batch_max",
			timestamp:      3000,
			expectedOffset: 5,
			description:    "exactly at second batch max timestamp",
		},
		{
			name:           "one_after_last_timestamp",
			timestamp:      3001,
			expectedOffset: -1,
			description:    "just after last batch max should return -1 (not found)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildListOffsetsRequest("boundary-topic", 0, tt.timestamp)
			resp := handler.Handle(ctx, 5, req)

			partResp := resp.Topics[0].Partitions[0]

			if partResp.ErrorCode != errNoError {
				t.Errorf("expected no error, got error code %d", partResp.ErrorCode)
			}

			if partResp.Offset != tt.expectedOffset {
				t.Errorf("%s: expected offset=%d, got %d",
					tt.description, tt.expectedOffset, partResp.Offset)
			}
		})
	}
}

// TestListOffsetsTimestamp_Version0Response tests version 0 response format with timestamps.
func TestListOffsetsTimestamp_Version0Response(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "v0-topic",
		PartitionCount: 1,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "v0-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	// Query with version 0
	req := buildListOffsetsRequest("v0-topic", 0, 1200)
	resp := handler.Handle(ctx, 0, req)

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != errNoError {
		t.Errorf("expected no error, got error code %d", partResp.ErrorCode)
	}

	// Version 0 uses OldStyleOffsets array
	if len(partResp.OldStyleOffsets) != 1 {
		t.Errorf("expected 1 old-style offset, got %d", len(partResp.OldStyleOffsets))
	}

	if partResp.OldStyleOffsets[0] != 0 {
		t.Errorf("expected old-style offset=0, got %d", partResp.OldStyleOffsets[0])
	}
}

// TestListOffsetsTimestamp_NegativeTimestamp tests behavior with negative timestamp values.
// Note: Negative timestamps other than -1, -2, -3, -4 should be treated as regular timestamps.
func TestListOffsetsTimestamp_NegativeTimestamp(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "negative-ts-topic",
		PartitionCount: 1,
		NowMs:          1000000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "negative-ts-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// All records have positive timestamps
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    1000,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	// Query with timestamp -5 (not a special value, treated as a regular negative timestamp)
	// Since all records have positive timestamps >= 1000, any negative timestamp should return offset 0
	req := buildListOffsetsRequest("negative-ts-topic", 0, -5)
	resp := handler.Handle(ctx, 5, req)

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != errNoError {
		t.Errorf("expected no error, got error code %d", partResp.ErrorCode)
	}

	// Negative timestamp -5 is before all records (which start at 1000)
	// So it should return offset 0 (first record >= -5)
	if partResp.Offset != 0 {
		t.Errorf("expected offset=0, got %d", partResp.Offset)
	}
}
