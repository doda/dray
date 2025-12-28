package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestListOffsetsHandler_Latest tests that LATEST returns the HWM (LEO).
func TestListOffsetsHandler_Latest(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
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
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Set HWM to 100
	_, version, _ := streamManager.GetHWM(ctx, streamID)
	_, _, err = streamManager.IncrementHWM(ctx, streamID, 100, version)
	if err != nil {
		t.Fatalf("failed to set HWM: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)
	req := buildListOffsetsRequest("test-topic", 0, TimestampLatest)
	resp := handler.Handle(ctx, 5, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}

	if partResp.Offset != 100 {
		t.Errorf("expected offset=100 (HWM), got %d", partResp.Offset)
	}
}

// TestListOffsetsHandler_Earliest tests that EARLIEST returns the smallest available offset.
func TestListOffsetsHandler_Earliest(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
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
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	t.Run("empty stream returns 0", func(t *testing.T) {
		req := buildListOffsetsRequest("test-topic", 0, TimestampEarliest)
		resp := handler.Handle(ctx, 5, req)

		partResp := resp.Topics[0].Partitions[0]

		if partResp.ErrorCode != 0 {
			t.Errorf("expected success, got error code %d", partResp.ErrorCode)
		}

		if partResp.Offset != 0 {
			t.Errorf("expected offset=0 for empty stream, got %d", partResp.Offset)
		}
	})

	t.Run("with data returns first offset", func(t *testing.T) {
		// Add an index entry starting at offset 10
		_, err := streamManager.AppendIndexEntry(ctx, index.AppendRequest{
			StreamID:       streamID,
			RecordCount:    5,
			ChunkSizeBytes: 100,
			CreatedAtMs:    time.Now().UnixMilli(),
			MinTimestampMs: 1000,
			MaxTimestampMs: 2000,
			WalID:          "test-wal",
			WalPath:        "wal/test.wal",
			ChunkOffset:    0,
			ChunkLength:    100,
		})
		if err != nil {
			t.Fatalf("failed to append index entry: %v", err)
		}

		req := buildListOffsetsRequest("test-topic", 0, TimestampEarliest)
		resp := handler.Handle(ctx, 5, req)

		partResp := resp.Topics[0].Partitions[0]

		if partResp.ErrorCode != 0 {
			t.Errorf("expected success, got error code %d", partResp.ErrorCode)
		}

		// First entry starts at offset 0 (initial HWM was 0)
		if partResp.Offset != 0 {
			t.Errorf("expected offset=0, got %d", partResp.Offset)
		}
	})
}

// TestListOffsetsHandler_Timestamp tests timestamp-based lookup.
func TestListOffsetsHandler_Timestamp(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
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
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Add index entries with different timestamps
	// Entry 1: offsets 0-4, timestamps 1000-1500
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
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

	// Entry 2: offsets 5-9, timestamps 2000-2500
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 2000,
		MaxTimestampMs: 2500,
		WalID:          "wal-2",
		WalPath:        "wal/2.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	// Entry 3: offsets 10-14, timestamps 3000-3500
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 3000,
		MaxTimestampMs: 3500,
		WalID:          "wal-3",
		WalPath:        "wal/3.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	tests := []struct {
		name           string
		timestamp      int64
		expectedOffset int64
		expectedFound  bool
	}{
		{
			name:           "timestamp before first entry",
			timestamp:      500,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "timestamp at start of first entry",
			timestamp:      1000,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			name:           "timestamp in middle of second entry",
			timestamp:      2200,
			expectedOffset: 5,
			expectedFound:  true,
		},
		{
			name:           "timestamp at start of third entry",
			timestamp:      3000,
			expectedOffset: 10,
			expectedFound:  true,
		},
		{
			name:           "timestamp after all entries",
			timestamp:      5000,
			expectedOffset: -1,
			expectedFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildListOffsetsRequest("test-topic", 0, tt.timestamp)
			resp := handler.Handle(ctx, 5, req)

			partResp := resp.Topics[0].Partitions[0]

			if partResp.ErrorCode != 0 {
				t.Errorf("expected success, got error code %d", partResp.ErrorCode)
			}

			if partResp.Offset != tt.expectedOffset {
				t.Errorf("expected offset=%d, got %d", tt.expectedOffset, partResp.Offset)
			}
		})
	}
}

// TestListOffsetsHandler_TimestampWithBatchIndex tests timestamp lookup using batchIndex.
func TestListOffsetsHandler_TimestampWithBatchIndex(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
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
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Add index entry with batchIndex for finer-grained lookup
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    15,
		ChunkSizeBytes: 300,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 1000,
		MaxTimestampMs: 3000,
		WalID:          "wal-1",
		WalPath:        "wal/1.wal",
		ChunkOffset:    0,
		ChunkLength:    300,
		BatchIndex: []index.BatchIndexEntry{
			{BatchStartOffsetDelta: 0, BatchLastOffsetDelta: 4, BatchOffsetInChunk: 0, BatchLength: 100, MinTimestampMs: 1000, MaxTimestampMs: 1500},
			{BatchStartOffsetDelta: 5, BatchLastOffsetDelta: 9, BatchOffsetInChunk: 100, BatchLength: 100, MinTimestampMs: 2000, MaxTimestampMs: 2500},
			{BatchStartOffsetDelta: 10, BatchLastOffsetDelta: 14, BatchOffsetInChunk: 200, BatchLength: 100, MinTimestampMs: 2700, MaxTimestampMs: 3000},
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
		{
			name:           "timestamp in first batch",
			timestamp:      1200,
			expectedOffset: 0,
		},
		{
			name:           "timestamp in second batch",
			timestamp:      2200,
			expectedOffset: 5,
		},
		{
			name:           "timestamp in third batch",
			timestamp:      2800,
			expectedOffset: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildListOffsetsRequest("test-topic", 0, tt.timestamp)
			resp := handler.Handle(ctx, 5, req)

			partResp := resp.Topics[0].Partitions[0]

			if partResp.ErrorCode != 0 {
				t.Errorf("expected success, got error code %d", partResp.ErrorCode)
			}

			if partResp.Offset != tt.expectedOffset {
				t.Errorf("expected offset=%d, got %d", tt.expectedOffset, partResp.Offset)
			}
		})
	}
}

// TestListOffsetsHandler_UnknownTopic tests error handling for unknown topics.
func TestListOffsetsHandler_UnknownTopic(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	handler := NewListOffsetsHandler(topicStore, streamManager)
	req := buildListOffsetsRequest("nonexistent-topic", 0, TimestampLatest)
	resp := handler.Handle(ctx, 5, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != errUnknownTopicOrPartitionErr {
		t.Errorf("expected UNKNOWN_TOPIC_OR_PARTITION error, got %d", partResp.ErrorCode)
	}
}

// TestListOffsetsHandler_UnknownPartition tests error handling for unknown partitions.
func TestListOffsetsHandler_UnknownPartition(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)
	req := buildListOffsetsRequest("test-topic", 10, TimestampLatest)
	resp := handler.Handle(ctx, 5, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != errUnknownTopicOrPartitionErr {
		t.Errorf("expected UNKNOWN_TOPIC_OR_PARTITION error, got %d", partResp.ErrorCode)
	}
}

// TestListOffsetsHandler_MultiplePartitions tests list offsets for multiple partitions.
func TestListOffsetsHandler_MultiplePartitions(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create streams for all partitions with different HWMs
	for i, p := range result.Partitions {
		err := streamManager.CreateStreamWithID(ctx, p.StreamID, "test-topic", p.Partition)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
		_, version, _ := streamManager.GetHWM(ctx, p.StreamID)
		_, _, _ = streamManager.IncrementHWM(ctx, p.StreamID, int64((i+1)*10), version)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)

	// Build request for multiple partitions
	req := kmsg.NewPtrListOffsetsRequest()
	req.SetVersion(5)
	req.ReplicaID = -1

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = "test-topic"

	for i := int32(0); i < 3; i++ {
		partReq := kmsg.NewListOffsetsRequestTopicPartition()
		partReq.Partition = i
		partReq.Timestamp = TimestampLatest
		topicReq.Partitions = append(topicReq.Partitions, partReq)
	}

	req.Topics = append(req.Topics, topicReq)
	resp := handler.Handle(ctx, 5, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if len(topicResp.Partitions) != 3 {
		t.Fatalf("expected 3 partition responses, got %d", len(topicResp.Partitions))
	}

	expectedHWMs := []int64{10, 20, 30}
	for i, partResp := range topicResp.Partitions {
		if partResp.ErrorCode != 0 {
			t.Errorf("partition %d: expected success, got error code %d", i, partResp.ErrorCode)
		}
		if partResp.Offset != expectedHWMs[i] {
			t.Errorf("partition %d: expected offset=%d, got %d", i, expectedHWMs[i], partResp.Offset)
		}
	}
}

// TestListOffsetsHandler_MaxTimestamp tests the max timestamp (-3) lookup (KIP-734).
func TestListOffsetsHandler_MaxTimestamp(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
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
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Add entries with different max timestamps
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
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

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: 2000,
		MaxTimestampMs: 5000, // Highest max timestamp
		WalID:          "wal-2",
		WalPath:        "wal/2.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)
	req := buildListOffsetsRequest("test-topic", 0, TimestampMaxTimestamp)
	resp := handler.Handle(ctx, 7, req)

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}

	// Should return offset 9 (last offset in second entry) with timestamp 5000
	if partResp.Offset != 9 {
		t.Errorf("expected offset=9, got %d", partResp.Offset)
	}

	if partResp.Timestamp != 5000 {
		t.Errorf("expected timestamp=5000, got %d", partResp.Timestamp)
	}
}

// TestListOffsetsHandler_EmptyStreamMaxTimestamp tests max timestamp on empty stream.
func TestListOffsetsHandler_EmptyStreamMaxTimestamp(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
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
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)
	req := buildListOffsetsRequest("test-topic", 0, TimestampMaxTimestamp)
	resp := handler.Handle(ctx, 7, req)

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}

	if partResp.Offset != -1 {
		t.Errorf("expected offset=-1 for empty stream, got %d", partResp.Offset)
	}

	if partResp.Timestamp != -1 {
		t.Errorf("expected timestamp=-1 for empty stream, got %d", partResp.Timestamp)
	}
}

// TestListOffsetsHandler_Version0 tests version 0 response format (OldStyleOffsets).
func TestListOffsetsHandler_Version0(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
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
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	_, version, _ := streamManager.GetHWM(ctx, streamID)
	_, _, err = streamManager.IncrementHWM(ctx, streamID, 50, version)
	if err != nil {
		t.Fatalf("failed to set HWM: %v", err)
	}

	handler := NewListOffsetsHandler(topicStore, streamManager)
	req := buildListOffsetsRequest("test-topic", 0, TimestampLatest)
	resp := handler.Handle(ctx, 0, req)

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}

	// Version 0 uses OldStyleOffsets array
	if len(partResp.OldStyleOffsets) != 1 {
		t.Errorf("expected 1 old-style offset, got %d", len(partResp.OldStyleOffsets))
	}

	if partResp.OldStyleOffsets[0] != 50 {
		t.Errorf("expected old-style offset=50, got %d", partResp.OldStyleOffsets[0])
	}
}

// buildListOffsetsRequest creates a minimal list offsets request for testing.
func buildListOffsetsRequest(topic string, partition int32, timestamp int64) *kmsg.ListOffsetsRequest {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewListOffsetsRequestTopicPartition()
	partReq.Partition = partition
	partReq.Timestamp = timestamp

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}
