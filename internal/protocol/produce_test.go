package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestProduceHandler_ValidateTopicAndPartition tests that produce handler validates topic and partition existence.
func TestProduceHandler_ValidateTopicAndPartition(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx := context.Background()

	// Create a test topic with 3 partitions
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create buffer with immediate flush for tests
	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush for tests
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
			// Complete all requests with a mock result
			for _, req := range requests {
				req.Result = &produce.RequestResult{
					StartOffset: 0,
					EndOffset:   int64(req.RecordCount),
				}
			}
			return nil
		},
	})
	defer buffer.Close()

	handler := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer)

	tests := []struct {
		name           string
		topic          string
		partition      int32
		wantTopicErr   bool
		wantPartErr    bool
	}{
		{
			name:      "valid topic and partition",
			topic:     "test-topic",
			partition: 0,
		},
		{
			name:      "valid topic and last partition",
			topic:     "test-topic",
			partition: 2,
		},
		{
			name:         "unknown topic",
			topic:        "nonexistent-topic",
			partition:    0,
			wantTopicErr: true,
		},
		{
			name:        "unknown partition",
			topic:       "test-topic",
			partition:   5,
			wantPartErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build a minimal produce request with record batches
			req := buildProduceRequest(tt.topic, tt.partition)

			resp := handler.Handle(ctx, 9, req)

			if len(resp.Topics) != 1 {
				t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
			}

			topicResp := resp.Topics[0]
			if len(topicResp.Partitions) != 1 {
				t.Fatalf("expected 1 partition response, got %d", len(topicResp.Partitions))
			}

			partResp := topicResp.Partitions[0]

			if tt.wantTopicErr || tt.wantPartErr {
				if partResp.ErrorCode == 0 {
					t.Errorf("expected error, got success")
				}
				if partResp.ErrorCode != errUnknownTopicOrPartitionErr {
					t.Errorf("expected UNKNOWN_TOPIC_OR_PARTITION error (%d), got %d", errUnknownTopicOrPartitionErr, partResp.ErrorCode)
				}
			} else {
				if partResp.ErrorCode != 0 {
					t.Errorf("expected success, got error code %d", partResp.ErrorCode)
				}
			}
		})
	}
}

// TestProduceHandler_RejectIdempotent tests that idempotent requests are rejected.
func TestProduceHandler_RejectIdempotent(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx := context.Background()

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
			for _, req := range requests {
				req.Result = &produce.RequestResult{StartOffset: 0, EndOffset: int64(req.RecordCount)}
			}
			return nil
		},
	})
	defer buffer.Close()

	handler := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer)

	// Build a produce request with idempotent producer ID in record batch
	req := buildIdempotentProduceRequest("test-topic", 0)

	resp := handler.Handle(ctx, 9, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]
	if partResp.ErrorCode != errInvalidProducerIDMapping {
		t.Errorf("expected INVALID_PRODUCER_ID_MAPPING error (%d), got %d", errInvalidProducerIDMapping, partResp.ErrorCode)
	}
}

// TestProduceHandler_RejectTransactional tests that transactional requests are rejected.
func TestProduceHandler_RejectTransactional(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx := context.Background()

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
			for _, req := range requests {
				req.Result = &produce.RequestResult{StartOffset: 0, EndOffset: int64(req.RecordCount)}
			}
			return nil
		},
	})
	defer buffer.Close()

	handler := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer)

	// Build a transactional produce request
	req := buildProduceRequest("test-topic", 0)
	txnID := "my-transaction"
	req.TransactionID = &txnID

	resp := handler.Handle(ctx, 9, req)

	// All partitions should have TRANSACTIONAL_ID_NOT_FOUND error
	for _, topicResp := range resp.Topics {
		for _, partResp := range topicResp.Partitions {
			if partResp.ErrorCode != errTransactionalIDNotFound {
				t.Errorf("expected TRANSACTIONAL_ID_NOT_FOUND error (%d), got %d", errTransactionalIDNotFound, partResp.ErrorCode)
			}
		}
	}
}

// TestProduceHandler_BufferAndFlush tests that records are buffered and flushed correctly.
func TestProduceHandler_BufferAndFlush(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx := context.Background()

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	var flushedRequests []*produce.PendingRequest
	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Trigger immediate flush
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
			flushedRequests = append(flushedRequests, requests...)
			baseOffset := int64(100) // Simulate offset assignment
			for _, req := range requests {
				req.Result = &produce.RequestResult{
					StartOffset: baseOffset,
					EndOffset:   baseOffset + int64(req.RecordCount),
				}
				baseOffset += int64(req.RecordCount)
			}
			return nil
		},
	})
	defer buffer.Close()

	handler := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer)

	req := buildProduceRequest("test-topic", 0)
	resp := handler.Handle(ctx, 9, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}

	if partResp.BaseOffset != 100 {
		t.Errorf("expected base offset 100, got %d", partResp.BaseOffset)
	}

	if len(flushedRequests) == 0 {
		t.Error("expected flush to be called")
	}
}

// TestProduceHandler_AcksZero tests that acks=0 doesn't wait for commit.
func TestProduceHandler_AcksZero(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx := context.Background()

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	flushCalled := make(chan struct{}, 1)
	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 512 * 1024,
		LingerMs:       100,
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
			select {
			case flushCalled <- struct{}{}:
			default:
			}
			for _, req := range requests {
				req.Result = &produce.RequestResult{StartOffset: 0, EndOffset: int64(req.RecordCount)}
			}
			return nil
		},
	})
	defer buffer.Close()

	handler := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer)

	req := buildProduceRequest("test-topic", 0)
	req.Acks = 0

	resp := handler.Handle(ctx, 9, req)

	// With acks=0, response should come back immediately
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}
}

// TestProduceHandler_PerPartitionErrors tests that errors are set per-partition.
func TestProduceHandler_PerPartitionErrors(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx := context.Background()

	// Create topic with 2 partitions
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
			for _, req := range requests {
				req.Result = &produce.RequestResult{StartOffset: 0, EndOffset: int64(req.RecordCount)}
			}
			return nil
		},
	})
	defer buffer.Close()

	handler := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer)

	// Build request with one valid partition and one invalid
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = "test-topic"

	// Valid partition
	part0 := kmsg.NewProduceRequestTopicPartition()
	part0.Partition = 0
	part0.Records = buildRecordBatch(1)
	topicReq.Partitions = append(topicReq.Partitions, part0)

	// Invalid partition
	part10 := kmsg.NewProduceRequestTopicPartition()
	part10.Partition = 10
	part10.Records = buildRecordBatch(1)
	topicReq.Partitions = append(topicReq.Partitions, part10)

	req.Topics = append(req.Topics, topicReq)

	resp := handler.Handle(ctx, 9, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if len(topicResp.Partitions) != 2 {
		t.Fatalf("expected 2 partition responses, got %d", len(topicResp.Partitions))
	}

	// Check partition 0 (valid) - should succeed
	var part0Resp, part10Resp *kmsg.ProduceResponseTopicPartition
	for i := range topicResp.Partitions {
		if topicResp.Partitions[i].Partition == 0 {
			part0Resp = &topicResp.Partitions[i]
		}
		if topicResp.Partitions[i].Partition == 10 {
			part10Resp = &topicResp.Partitions[i]
		}
	}

	if part0Resp == nil || part10Resp == nil {
		t.Fatal("missing expected partition responses")
	}

	if part0Resp.ErrorCode != 0 {
		t.Errorf("partition 0: expected success, got error code %d", part0Resp.ErrorCode)
	}

	if part10Resp.ErrorCode != errUnknownTopicOrPartitionErr {
		t.Errorf("partition 10: expected UNKNOWN_TOPIC_OR_PARTITION, got error code %d", part10Resp.ErrorCode)
	}
}

// buildProduceRequest creates a minimal produce request for testing.
func buildProduceRequest(topic string, partition int32) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = partition
	partReq.Records = buildRecordBatch(1)

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// buildIdempotentProduceRequest creates a produce request with idempotent producer flags.
func buildIdempotentProduceRequest(topic string, partition int32) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = partition
	partReq.Records = buildIdempotentRecordBatch(1)

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// buildRecordBatch creates a minimal Kafka record batch for testing.
// This creates a valid record batch structure that can be parsed.
func buildRecordBatch(recordCount int) []byte {
	// Minimal Kafka record batch format:
	// baseOffset: 8 bytes (0)
	// batchLength: 4 bytes
	// partitionLeaderEpoch: 4 bytes
	// magic: 1 byte (2)
	// crc: 4 bytes
	// attributes: 2 bytes
	// lastOffsetDelta: 4 bytes
	// firstTimestamp: 8 bytes
	// maxTimestamp: 8 bytes
	// producerId: 8 bytes (-1 for non-idempotent)
	// producerEpoch: 2 bytes (-1)
	// firstSequence: 4 bytes (-1)
	// recordCount: 4 bytes
	// ...records

	batch := make([]byte, 0, 80)

	// baseOffset (8 bytes) = 0
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// batchLength (4 bytes) - will be set later
	batch = append(batch, 0, 0, 0, 49) // 49 + 12 = 61 bytes total

	// partitionLeaderEpoch (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// magic (1 byte) = 2
	batch = append(batch, 2)

	// crc (4 bytes) - placeholder
	batch = append(batch, 0, 0, 0, 0)

	// attributes (2 bytes)
	batch = append(batch, 0, 0)

	// lastOffsetDelta (4 bytes)
	batch = append(batch, 0, 0, 0, byte(recordCount-1))

	// firstTimestamp (8 bytes)
	ts := time.Now().UnixMilli()
	batch = append(batch,
		byte(ts>>56), byte(ts>>48), byte(ts>>40), byte(ts>>32),
		byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts))

	// maxTimestamp (8 bytes)
	batch = append(batch,
		byte(ts>>56), byte(ts>>48), byte(ts>>40), byte(ts>>32),
		byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts))

	// producerId (8 bytes) = -1 (non-idempotent)
	batch = append(batch, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)

	// producerEpoch (2 bytes) = -1
	batch = append(batch, 0xff, 0xff)

	// firstSequence (4 bytes) = -1
	batch = append(batch, 0xff, 0xff, 0xff, 0xff)

	// recordCount (4 bytes)
	batch = append(batch, 0, 0, 0, byte(recordCount))

	return batch
}

// buildIdempotentRecordBatch creates a record batch with idempotent producer flags.
func buildIdempotentRecordBatch(recordCount int) []byte {
	batch := make([]byte, 0, 80)

	// baseOffset (8 bytes)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// batchLength (4 bytes)
	batch = append(batch, 0, 0, 0, 49)

	// partitionLeaderEpoch (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// magic (1 byte) = 2
	batch = append(batch, 2)

	// crc (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// attributes (2 bytes)
	batch = append(batch, 0, 0)

	// lastOffsetDelta (4 bytes)
	batch = append(batch, 0, 0, 0, byte(recordCount-1))

	// firstTimestamp (8 bytes)
	ts := time.Now().UnixMilli()
	batch = append(batch,
		byte(ts>>56), byte(ts>>48), byte(ts>>40), byte(ts>>32),
		byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts))

	// maxTimestamp (8 bytes)
	batch = append(batch,
		byte(ts>>56), byte(ts>>48), byte(ts>>40), byte(ts>>32),
		byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts))

	// producerId (8 bytes) = 1234 (idempotent)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0x04, 0xd2)

	// producerEpoch (2 bytes) = 0
	batch = append(batch, 0, 0)

	// firstSequence (4 bytes) = 0
	batch = append(batch, 0, 0, 0, 0)

	// recordCount (4 bytes)
	batch = append(batch, 0, 0, 0, byte(recordCount))

	return batch
}
