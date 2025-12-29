package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/topics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kgo"
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
	if partResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected INVALID_REQUEST error (%d), got %d", errInvalidRequest, partResp.ErrorCode)
	}
}

func TestProduceHandler_NonOwnerServeAnyway(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx, buf := newLogContext()

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
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
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

	handler := NewProduceHandler(ProduceHandlerConfig{
		LocalNodeID:    1,
		EnforceOwner:   false,
		LeaderSelector: staticLeaderSelector{leader: 2},
	}, topicStore, buffer)

	resp := handler.Handle(ctx, 9, buildProduceRequest("test-topic", 0))
	if resp.Topics[0].Partitions[0].ErrorCode != errNoError {
		t.Fatalf("expected success, got error code %d", resp.Topics[0].Partitions[0].ErrorCode)
	}

	assertLogContains(t, buf, "affinity violation: produce request handled by non-owner broker")
}

func TestProduceHandler_NonOwnerEnforced(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx, buf := newLogContext()

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
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
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

	handler := NewProduceHandler(ProduceHandlerConfig{
		LocalNodeID:    1,
		EnforceOwner:   true,
		LeaderSelector: staticLeaderSelector{leader: 2},
	}, topicStore, buffer)

	resp := handler.Handle(ctx, 9, buildProduceRequest("test-topic", 0))
	partResp := resp.Topics[0].Partitions[0]
	if partResp.ErrorCode != errNotLeaderOrFollower {
		t.Fatalf("expected not leader error %d, got %d", errNotLeaderOrFollower, partResp.ErrorCode)
	}
	if partResp.BaseOffset != -1 {
		t.Fatalf("expected BaseOffset -1, got %d", partResp.BaseOffset)
	}

	assertLogContains(t, buf, "affinity violation: produce request handled by non-owner broker")
}

// TestProduceHandler_RejectIdempotentVariousProducerIds tests rejection with various producer IDs.
func TestProduceHandler_RejectIdempotentVariousProducerIds(t *testing.T) {
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

	tests := []struct {
		name       string
		producerId int64
		wantReject bool
	}{
		{
			name:       "producerId=0 (idempotent)",
			producerId: 0,
			wantReject: true,
		},
		{
			name:       "producerId=1 (idempotent)",
			producerId: 1,
			wantReject: true,
		},
		{
			name:       "producerId=1234 (idempotent)",
			producerId: 1234,
			wantReject: true,
		},
		{
			name:       "producerId=max_int64 (idempotent)",
			producerId: 9223372036854775807,
			wantReject: true,
		},
		{
			name:       "producerId=-1 (non-idempotent)",
			producerId: -1,
			wantReject: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := kmsg.NewPtrProduceRequest()
			req.Acks = -1
			req.SetVersion(9)

			topicReq := kmsg.NewProduceRequestTopic()
			topicReq.Topic = "test-topic"

			partReq := kmsg.NewProduceRequestTopicPartition()
			partReq.Partition = 0
			partReq.Records = buildRecordBatchWithProducerId(1, tt.producerId)

			topicReq.Partitions = append(topicReq.Partitions, partReq)
			req.Topics = append(req.Topics, topicReq)

			resp := handler.Handle(ctx, 9, req)

			if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
				t.Fatalf("unexpected response structure")
			}

			partResp := resp.Topics[0].Partitions[0]
			if tt.wantReject {
				if partResp.ErrorCode != errInvalidRequest {
					t.Errorf("expected INVALID_REQUEST error (%d), got %d", errInvalidRequest, partResp.ErrorCode)
				}
				if partResp.BaseOffset != -1 {
					t.Errorf("expected BaseOffset=-1 on rejection, got %d", partResp.BaseOffset)
				}
			} else {
				if partResp.ErrorCode != 0 {
					t.Errorf("expected success, got error code %d", partResp.ErrorCode)
				}
			}
		})
	}
}

// TestProduceHandler_RejectIdempotentMultiplePartitions tests that idempotent rejection is per-partition.
func TestProduceHandler_RejectIdempotentMultiplePartitions(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	ctx := context.Background()

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

	// Build request with partition 0 idempotent and partition 1 non-idempotent
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = "test-topic"

	// Partition 0: idempotent (should reject)
	part0 := kmsg.NewProduceRequestTopicPartition()
	part0.Partition = 0
	part0.Records = buildRecordBatchWithProducerId(1, 1234)
	topicReq.Partitions = append(topicReq.Partitions, part0)

	// Partition 1: non-idempotent (should succeed)
	part1 := kmsg.NewProduceRequestTopicPartition()
	part1.Partition = 1
	part1.Records = buildRecordBatchWithProducerId(1, -1)
	topicReq.Partitions = append(topicReq.Partitions, part1)

	req.Topics = append(req.Topics, topicReq)

	resp := handler.Handle(ctx, 9, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if len(topicResp.Partitions) != 2 {
		t.Fatalf("expected 2 partition responses, got %d", len(topicResp.Partitions))
	}

	var part0Resp, part1Resp *kmsg.ProduceResponseTopicPartition
	for i := range topicResp.Partitions {
		if topicResp.Partitions[i].Partition == 0 {
			part0Resp = &topicResp.Partitions[i]
		}
		if topicResp.Partitions[i].Partition == 1 {
			part1Resp = &topicResp.Partitions[i]
		}
	}

	if part0Resp == nil || part1Resp == nil {
		t.Fatal("missing expected partition responses")
	}

	// Partition 0 should be rejected (idempotent)
	if part0Resp.ErrorCode != errInvalidRequest {
		t.Errorf("partition 0: expected INVALID_REQUEST error (%d), got %d",
			errInvalidRequest, part0Resp.ErrorCode)
	}

	// Partition 1 should succeed (non-idempotent)
	if part1Resp.ErrorCode != 0 {
		t.Errorf("partition 1: expected success, got error code %d", part1Resp.ErrorCode)
	}
}

// TestExtractProducerId tests the extractProducerId function directly.
func TestExtractProducerId(t *testing.T) {
	tests := []struct {
		name       string
		data       []byte
		wantId     int64
	}{
		{
			name:   "idempotent producer (id=1234)",
			data:   buildRecordBatchWithProducerId(1, 1234),
			wantId: 1234,
		},
		{
			name:   "idempotent producer (id=0)",
			data:   buildRecordBatchWithProducerId(1, 0),
			wantId: 0,
		},
		{
			name:   "idempotent producer (id=max_int64)",
			data:   buildRecordBatchWithProducerId(1, 9223372036854775807),
			wantId: 9223372036854775807,
		},
		{
			name:   "non-idempotent producer (id=-1)",
			data:   buildRecordBatchWithProducerId(1, -1),
			wantId: -1,
		},
		{
			name:   "empty data",
			data:   []byte{},
			wantId: -1,
		},
		{
			name:   "too short data",
			data:   make([]byte, 10),
			wantId: -1,
		},
		{
			name: "truncated batch",
			data: func() []byte {
				batch := buildRecordBatchWithProducerId(1, 1234)
				return batch[:40] // Truncate to less than 53 bytes
			}(),
			wantId: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractProducerId(tt.data)
			if got != tt.wantId {
				t.Errorf("extractProducerId() = %d, want %d", got, tt.wantId)
			}
		})
	}
}

// TestProduceHandler_IdempotentRejectionClientHandling verifies that the error code returned
// is consistent with the spec behavior for deferred idempotent producer support.
func TestProduceHandler_IdempotentRejectionClientHandling(t *testing.T) {
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

	// Test multiple idempotent requests to ensure consistent rejection behavior
	for i := 0; i < 3; i++ {
		req := buildIdempotentProduceRequest("test-topic", 0)
		resp := handler.Handle(ctx, 9, req)

		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("iteration %d: unexpected response structure", i)
		}

		partResp := resp.Topics[0].Partitions[0]

		// Verify consistent error code per spec for deferred idempotence.
		if partResp.ErrorCode != errInvalidRequest {
			t.Errorf("iteration %d: expected INVALID_REQUEST error (%d), got %d",
				i, errInvalidRequest, partResp.ErrorCode)
		}

		// Kafka clients expect BaseOffset=-1 on error
		if partResp.BaseOffset != -1 {
			t.Errorf("iteration %d: expected BaseOffset=-1, got %d", i, partResp.BaseOffset)
		}
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

	// All partitions should have UNSUPPORTED_VERSION error per spec 14.3
	for _, topicResp := range resp.Topics {
		for _, partResp := range topicResp.Partitions {
			if partResp.ErrorCode != errUnsupportedVersion {
				t.Errorf("expected UNSUPPORTED_VERSION error (%d), got %d", errUnsupportedVersion, partResp.ErrorCode)
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

// buildIdempotentRecordBatch creates a record batch with idempotent producer flags.
func buildIdempotentRecordBatch(recordCount int) []byte {
	return buildRecordBatchWithProducerId(recordCount, 1234)
}

// buildRecordBatchWithProducerId creates a record batch with the specified producer ID.
// Use producerId >= 0 for idempotent producers, -1 for non-idempotent.
func buildRecordBatchWithProducerId(recordCount int, producerId int64) []byte {
	batch := make([]byte, 0, 80)

	// baseOffset (8 bytes)
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
	ts := time.Now().UnixMilli()
	batch = append(batch,
		byte(ts>>56), byte(ts>>48), byte(ts>>40), byte(ts>>32),
		byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts))

	// maxTimestamp (8 bytes)
	batch = append(batch,
		byte(ts>>56), byte(ts>>48), byte(ts>>40), byte(ts>>32),
		byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts))

	// producerId (8 bytes)
	batch = append(batch,
		byte(producerId>>56), byte(producerId>>48), byte(producerId>>40), byte(producerId>>32),
		byte(producerId>>24), byte(producerId>>16), byte(producerId>>8), byte(producerId))

	// producerEpoch (2 bytes)
	if producerId >= 0 {
		batch = append(batch, 0, 0) // epoch 0 for idempotent
	} else {
		batch = append(batch, 0xff, 0xff) // -1 for non-idempotent
	}

	// firstSequence (4 bytes)
	if producerId >= 0 {
		batch = append(batch, 0, 0, 0, 0) // sequence 0 for idempotent
	} else {
		batch = append(batch, 0xff, 0xff, 0xff, 0xff) // -1 for non-idempotent
	}

	// recordCount (4 bytes)
	batch = append(batch, 0, 0, 0, byte(recordCount))

	// Calculate and set CRC over bytes from offset 21 onwards (attributes to end)
	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[21:], table)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

// TestProduceHandler_InvalidMagicByte tests that record batches with invalid magic byte are rejected.
func TestProduceHandler_InvalidMagicByte(t *testing.T) {
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

	// Build request with invalid magic byte
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = "test-topic"

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = 0
	partReq.Records = buildRecordBatchWithMagic(1, 0) // Invalid magic byte 0

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	resp := handler.Handle(ctx, 9, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]
	if partResp.ErrorCode != errUnsupportedForMessageFormat {
		t.Errorf("expected UNSUPPORTED_FOR_MESSAGE_FORMAT error (%d), got %d", errUnsupportedForMessageFormat, partResp.ErrorCode)
	}
}

// TestProduceHandler_TruncatedBatch tests that truncated record batches are rejected.
func TestProduceHandler_TruncatedBatch(t *testing.T) {
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

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "too short for header",
			data: []byte{0, 0, 0, 0, 0, 0, 0, 0}, // Only 8 bytes, need 12
		},
		{
			name: "length mismatch",
			data: func() []byte {
				batch := make([]byte, 20)
				// batchLength claims 100 bytes but only 20 provided
				batch[8], batch[9], batch[10], batch[11] = 0, 0, 0, 100
				return batch
			}(),
		},
		{
			name: "too short for record batch",
			data: func() []byte {
				batch := make([]byte, 40)
				// Set batchLength to 28 (40 - 12 = 28, but need at least 49 for minimal batch)
				batch[8], batch[9], batch[10], batch[11] = 0, 0, 0, 28
				return batch
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := kmsg.NewPtrProduceRequest()
			req.Acks = -1
			req.SetVersion(9)

			topicReq := kmsg.NewProduceRequestTopic()
			topicReq.Topic = "test-topic"

			partReq := kmsg.NewProduceRequestTopicPartition()
			partReq.Partition = 0
			partReq.Records = tt.data

			topicReq.Partitions = append(topicReq.Partitions, partReq)
			req.Topics = append(req.Topics, topicReq)

			resp := handler.Handle(ctx, 9, req)

			if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
				t.Fatalf("unexpected response structure")
			}

			partResp := resp.Topics[0].Partitions[0]
			if partResp.ErrorCode != errUnsupportedForMessageFormat {
				t.Errorf("expected UNSUPPORTED_FOR_MESSAGE_FORMAT error (%d), got %d", errUnsupportedForMessageFormat, partResp.ErrorCode)
			}
		})
	}
}

// TestProduceHandler_InvalidCRC tests that record batches with invalid CRC are rejected.
func TestProduceHandler_InvalidCRC(t *testing.T) {
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

	batch := buildRecordBatch(1)
	corrupted := append([]byte(nil), batch...)
	corrupted[17] ^= 0x01

	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = "test-topic"

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = 0
	partReq.Records = corrupted

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	resp := handler.Handle(ctx, 9, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]
	if partResp.ErrorCode != errUnsupportedForMessageFormat {
		t.Errorf("expected UNSUPPORTED_FOR_MESSAGE_FORMAT error (%d), got %d", errUnsupportedForMessageFormat, partResp.ErrorCode)
	}
}

// TestProduceHandler_InvalidCompressedPayload tests that invalid compressed record payloads are rejected.
func TestProduceHandler_InvalidCompressedPayload(t *testing.T) {
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

	batch := buildFranzCompressedBatch(t, 3, compressionGzip, time.Now().UnixMilli())
	corrupted := append([]byte(nil), batch...)

	var recordBatch kmsg.RecordBatch
	if err := recordBatch.ReadFrom(corrupted); err != nil {
		t.Fatalf("failed to parse record batch: %v", err)
	}
	if len(recordBatch.Records) < 2 {
		t.Fatalf("unexpected compressed record payload size %d", len(recordBatch.Records))
	}

	recordsStart := len(corrupted) - len(recordBatch.Records)
	corrupted[recordsStart] = 0x00
	corrupted[recordsStart+1] = 0x00

	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(corrupted[21:], table)
	binary.BigEndian.PutUint32(corrupted[17:21], crcValue)

	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = "test-topic"

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = 0
	partReq.Records = corrupted

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	resp := handler.Handle(ctx, 9, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]
	if partResp.ErrorCode != errUnsupportedForMessageFormat {
		t.Errorf("expected UNSUPPORTED_FOR_MESSAGE_FORMAT error (%d), got %d", errUnsupportedForMessageFormat, partResp.ErrorCode)
	}
}

func TestParseRecordBatches_Compressed(t *testing.T) {
	compressionTypes := []struct {
		name string
		typ  int
	}{
		{"gzip", compressionGzip},
		{"snappy", compressionSnappy},
		{"lz4", compressionLz4},
		{"zstd", compressionZstd},
	}

	ts := int64(1234567890)
	for _, tc := range compressionTypes {
		t.Run(tc.name, func(t *testing.T) {
			batch := buildFranzCompressedBatch(t, 3, tc.typ, ts)

			batches, recordCount, minTs, maxTs, err := parseRecordBatches(batch)
			if err != nil {
				t.Fatalf("parseRecordBatches() error: %v", err)
			}
			if len(batches) != 1 {
				t.Fatalf("expected 1 batch, got %d", len(batches))
			}
			if recordCount != 3 {
				t.Fatalf("expected recordCount=3, got %d", recordCount)
			}
			if minTs != ts || maxTs != ts {
				t.Fatalf("expected timestamps %d, got min=%d max=%d", ts, minTs, maxTs)
			}
		})
	}
}

// buildRecordBatchWithMagic creates a record batch with a specified magic byte.
func buildRecordBatchWithMagic(recordCount int, magic byte) []byte {
	batch := make([]byte, 0, 80)

	// baseOffset (8 bytes)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// batchLength (4 bytes)
	batch = append(batch, 0, 0, 0, 49)

	// partitionLeaderEpoch (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// magic (1 byte) - using parameter
	batch = append(batch, magic)

	// crc (4 bytes) - placeholder, will be set below
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

	// producerId (8 bytes) = -1
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

const (
	compressionNone   = 0
	compressionGzip   = 1
	compressionSnappy = 2
	compressionLz4    = 3
	compressionZstd   = 4
)

func buildFranzCompressedBatch(t *testing.T, recordCount int, compressionType int, ts int64) []byte {
	t.Helper()

	records := buildFranzRecords(recordCount)
	compressed := records
	if compressionType != compressionNone {
		codec, err := compressionCodecForType(compressionType)
		if err != nil {
			t.Fatalf("compression codec error: %v", err)
		}
		compressor, err := kgo.DefaultCompressor(codec)
		if err != nil {
			t.Fatalf("failed to create compressor: %v", err)
		}
		var buf bytes.Buffer
		compressed, _ = compressor.Compress(&buf, records)
	}

	lastOffsetDelta := int32(-1)
	if recordCount > 0 {
		lastOffsetDelta = int32(recordCount - 1)
	}

	recordBatch := kmsg.RecordBatch{
		FirstOffset:          0,
		Length:               int32(49 + len(compressed)),
		PartitionLeaderEpoch: 0,
		Magic:                2,
		CRC:                  0,
		Attributes:           int16(compressionType & 0x07),
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       ts,
		MaxTimestamp:         ts,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(recordCount),
		Records:              compressed,
	}

	batch := recordBatch.AppendTo(nil)
	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[21:], table)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

func buildFranzRecords(recordCount int) []byte {
	var records []byte
	for i := 0; i < recordCount; i++ {
		var body []byte
		body = kbin.AppendInt8(body, 0)
		body = kbin.AppendVarlong(body, 0)
		body = kbin.AppendVarint(body, int32(i))
		body = kbin.AppendVarintBytes(body, []byte("k"))
		body = kbin.AppendVarintBytes(body, []byte("v"))
		body = kbin.AppendVarint(body, 0)
		records = kbin.AppendVarint(records, int32(len(body)))
		records = append(records, body...)
	}
	return records
}

func compressionCodecForType(compressionType int) (kgo.CompressionCodec, error) {
	switch compressionType {
	case compressionGzip:
		return kgo.GzipCompression(), nil
	case compressionSnappy:
		return kgo.SnappyCompression(), nil
	case compressionLz4:
		return kgo.Lz4Compression(), nil
	case compressionZstd:
		return kgo.ZstdCompression(), nil
	default:
		return kgo.NoCompression(), fmt.Errorf("unsupported compression type %d", compressionType)
	}
}

// TestProduceHandler_MetricsRecording verifies that produce latency metrics are recorded correctly.
func TestProduceHandler_MetricsRecording(t *testing.T) {
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
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*produce.PendingRequest) error {
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

	// Create metrics with custom registry
	reg := prometheus.NewRegistry()
	m := metrics.NewProduceMetricsWithRegistry(reg)

	handler := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer).WithMetrics(m)

	t.Run("successful produce records success metric", func(t *testing.T) {
		req := kmsg.NewPtrProduceRequest()
		req.Acks = -1
		req.SetVersion(9)

		topicReq := kmsg.NewProduceRequestTopic()
		topicReq.Topic = "test-topic"

		partReq := kmsg.NewProduceRequestTopicPartition()
		partReq.Partition = 0
		partReq.Records = buildRecordBatchWithMagic(1, 2)

		topicReq.Partitions = append(topicReq.Partitions, partReq)
		req.Topics = append(req.Topics, topicReq)

		resp := handler.Handle(ctx, 9, req)

		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected response structure")
		}
		if resp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("expected success, got error code %d", resp.Topics[0].Partitions[0].ErrorCode)
		}

		// Verify success metric was recorded
		successCounter := m.RequestsTotal.WithLabelValues(metrics.StatusSuccess)
		successMetric := &dto.Metric{}
		if err := successCounter.Write(successMetric); err != nil {
			t.Fatalf("failed to read success counter: %v", err)
		}
		if successMetric.Counter.GetValue() < 1 {
			t.Error("expected at least 1 success counter increment")
		}

		// Verify histogram was populated
		successHist := m.LatencyHistogram.WithLabelValues(metrics.StatusSuccess)
		histMetric := &dto.Metric{}
		if err := successHist.(prometheus.Metric).Write(histMetric); err != nil {
			t.Fatalf("failed to read histogram: %v", err)
		}
		if histMetric.Histogram.GetSampleCount() < 1 {
			t.Error("expected at least 1 histogram sample")
		}
	})

	t.Run("failed produce records failure metric", func(t *testing.T) {
		req := kmsg.NewPtrProduceRequest()
		req.Acks = -1
		req.SetVersion(9)

		topicReq := kmsg.NewProduceRequestTopic()
		topicReq.Topic = "nonexistent-topic"

		partReq := kmsg.NewProduceRequestTopicPartition()
		partReq.Partition = 0
		partReq.Records = buildRecordBatchWithMagic(1, 2)

		topicReq.Partitions = append(topicReq.Partitions, partReq)
		req.Topics = append(req.Topics, topicReq)

		resp := handler.Handle(ctx, 9, req)

		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected response structure")
		}
		if resp.Topics[0].Partitions[0].ErrorCode == 0 {
			t.Fatal("expected error, got success")
		}

		// Verify failure metric was recorded
		failureCounter := m.RequestsTotal.WithLabelValues(metrics.StatusFailure)
		failureMetric := &dto.Metric{}
		if err := failureCounter.Write(failureMetric); err != nil {
			t.Fatalf("failed to read failure counter: %v", err)
		}
		if failureMetric.Counter.GetValue() < 1 {
			t.Error("expected at least 1 failure counter increment")
		}

		// Verify histogram was populated for failure
		failureHist := m.LatencyHistogram.WithLabelValues(metrics.StatusFailure)
		histMetric := &dto.Metric{}
		if err := failureHist.(prometheus.Metric).Write(histMetric); err != nil {
			t.Fatalf("failed to read histogram: %v", err)
		}
		if histMetric.Histogram.GetSampleCount() < 1 {
			t.Error("expected at least 1 histogram sample for failure")
		}
	})

	t.Run("transactional rejection records failure", func(t *testing.T) {
		// Create new registry for isolation
		reg2 := prometheus.NewRegistry()
		m2 := metrics.NewProduceMetricsWithRegistry(reg2)
		handler2 := NewProduceHandler(ProduceHandlerConfig{}, topicStore, buffer).WithMetrics(m2)

		req := kmsg.NewPtrProduceRequest()
		req.Acks = -1
		req.SetVersion(9)
		txnId := "my-txn"
		req.TransactionID = &txnId

		topicReq := kmsg.NewProduceRequestTopic()
		topicReq.Topic = "test-topic"

		partReq := kmsg.NewProduceRequestTopicPartition()
		partReq.Partition = 0
		partReq.Records = buildRecordBatchWithMagic(1, 2)

		topicReq.Partitions = append(topicReq.Partitions, partReq)
		req.Topics = append(req.Topics, topicReq)

		_ = handler2.Handle(ctx, 9, req)

		// Verify failure metric was recorded for transactional rejection
		failureCounter := m2.RequestsTotal.WithLabelValues(metrics.StatusFailure)
		failureMetric := &dto.Metric{}
		if err := failureCounter.Write(failureMetric); err != nil {
			t.Fatalf("failed to read failure counter: %v", err)
		}
		if failureMetric.Counter.GetValue() != 1 {
			t.Errorf("expected exactly 1 failure counter, got %f", failureMetric.Counter.GetValue())
		}
	})
}
