package protocol

import (
	"context"
	"errors"
	"time"

	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/topics"
	"github.com/dray-io/dray/internal/wal"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for Produce responses (reusing errLeaderNotAvailable from metadata.go).
const (
	errNoError                     int16 = 0
	errUnknownTopicOrPartitionErr  int16 = 3
	errInvalidRequiredAcks         int16 = 21
	errClusterAuthorizationFailed  int16 = 31
	errUnsupportedForMessageFormat int16 = 43
	errInvalidProducerEpoch        int16 = 47
	errInvalidProducerIDMapping    int16 = 49
	errInvalidTransactionalState   int16 = 73
	errTransactionalIDNotFound     int16 = 90
)

// ProduceHandlerConfig configures the produce handler.
type ProduceHandlerConfig struct {
	// RequiredAcks is the required acks setting for this broker.
	// Dray only supports acks=-1 (all replicas).
	RequiredAcks int16

	// TimeoutMs is the default timeout for produce requests.
	TimeoutMs int32
}

// ProduceHandler handles Produce (key 0) requests.
type ProduceHandler struct {
	cfg        ProduceHandlerConfig
	topicStore *topics.Store
	buffer     *produce.Buffer
}

// NewProduceHandler creates a new Produce handler.
func NewProduceHandler(cfg ProduceHandlerConfig, topicStore *topics.Store, buffer *produce.Buffer) *ProduceHandler {
	return &ProduceHandler{
		cfg:        cfg,
		topicStore: topicStore,
		buffer:     buffer,
	}
}

// Handle processes a Produce request.
// Per spec section 9.3, it:
// 1. Validates topic and partition existence
// 2. Rejects idempotent/transactional requests with correct errors
// 3. Buffers incoming record batches by MetaDomain
// 4. Waits for flush completion
// 5. Returns offsets only after WAL + metadata commit
func (h *ProduceHandler) Handle(ctx context.Context, version int16, req *kmsg.ProduceRequest) *kmsg.ProduceResponse {
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(version)

	// Validate acks - Dray only supports acks=-1 (all)
	// Per Kafka protocol, acks can be 0, 1, or -1
	// For acks=0, no response is expected (fire and forget)
	// For acks=1 or -1, we need to wait for commit
	if req.Acks != 0 && req.Acks != 1 && req.Acks != -1 {
		// Invalid acks value
		return h.buildErrorResponse(version, req, errInvalidRequiredAcks)
	}

	// Reject transactional requests per spec 14.3
	// Use UNSUPPORTED_FOR_MESSAGE_FORMAT to indicate transactions are not supported
	if req.TransactionID != nil && *req.TransactionID != "" {
		return h.buildErrorResponse(version, req, errUnsupportedForMessageFormat)
	}

	// Process each topic
	for _, topicData := range req.Topics {
		topicResp := kmsg.NewProduceResponseTopic()
		topicResp.Topic = topicData.Topic

		// Get topic metadata
		topicMeta, err := h.topicStore.GetTopic(ctx, topicData.Topic)
		if err != nil {
			// Topic not found - add error for all partitions
			for _, partData := range topicData.Partitions {
				partResp := h.buildPartitionError(version, partData.Partition, errUnknownTopicOrPartitionErr)
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}

		// Process each partition
		for _, partData := range topicData.Partitions {
			partResp := h.processPartition(ctx, version, topicMeta, &partData, req.Acks)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	// Set throttle time for version >= 1
	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	return resp
}

// processPartition handles a single partition's produce request.
func (h *ProduceHandler) processPartition(ctx context.Context, version int16, topicMeta *topics.TopicMeta, partData *kmsg.ProduceRequestTopicPartition, acks int16) kmsg.ProduceResponseTopicPartition {
	resp := kmsg.NewProduceResponseTopicPartition()
	resp.Partition = partData.Partition

	// Validate partition exists
	partMeta, err := h.topicStore.GetPartition(ctx, topicMeta.Name, partData.Partition)
	if err != nil {
		if errors.Is(err, topics.ErrPartitionNotFound) {
			resp.ErrorCode = errUnknownTopicOrPartitionErr
		} else {
			resp.ErrorCode = errUnknownTopicOrPartitionErr
		}
		resp.BaseOffset = -1
		return resp
	}

	// Check partition state
	if partMeta.State != "active" {
		resp.ErrorCode = errLeaderNotAvailable
		resp.BaseOffset = -1
		return resp
	}

	// Parse record batches from the partition data
	records := partData.Records
	if len(records) == 0 {
		resp.ErrorCode = errNoError
		resp.BaseOffset = 0
		return resp
	}

	// Extract batch information from Kafka record batch format
	batches, recordCount, minTs, maxTs, err := parseRecordBatches(records)
	if err != nil {
		resp.ErrorCode = errUnsupportedForMessageFormat
		resp.BaseOffset = -1
		return resp
	}

	// Check for idempotent producer flags in record batches
	if isIdempotentRequest(records) {
		resp.ErrorCode = errInvalidProducerIDMapping
		resp.BaseOffset = -1
		return resp
	}

	// For acks=0, we don't wait for commit
	if acks == 0 {
		// Fire and forget - add to buffer but don't wait
		_, _ = h.buffer.Add(ctx, partMeta.StreamID, batches, recordCount, minTs, maxTs)
		resp.ErrorCode = errNoError
		resp.BaseOffset = 0
		return resp
	}

	// Add to produce buffer and wait for completion
	pending, err := h.buffer.Add(ctx, partMeta.StreamID, batches, recordCount, minTs, maxTs)
	if err != nil {
		if errors.Is(err, produce.ErrBufferFull) {
			resp.ErrorCode = errClusterAuthorizationFailed // Use appropriate error
		} else {
			resp.ErrorCode = errUnknownTopicOrPartitionErr
		}
		resp.BaseOffset = -1
		return resp
	}

	// Wait for flush completion
	select {
	case <-pending.Done:
		if pending.Err != nil {
			resp.ErrorCode = errUnknownTopicOrPartitionErr
			resp.BaseOffset = -1
			return resp
		}
		if pending.Result != nil {
			resp.BaseOffset = pending.Result.StartOffset
			resp.LogAppendTime = time.Now().UnixMilli()
		} else {
			resp.BaseOffset = 0
		}
	case <-ctx.Done():
		resp.ErrorCode = errUnknownTopicOrPartitionErr
		resp.BaseOffset = -1
		return resp
	}

	resp.ErrorCode = errNoError
	return resp
}

// buildErrorResponse builds a response with errors for all partitions.
func (h *ProduceHandler) buildErrorResponse(version int16, req *kmsg.ProduceRequest, errorCode int16) *kmsg.ProduceResponse {
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(version)

	for _, topicData := range req.Topics {
		topicResp := kmsg.NewProduceResponseTopic()
		topicResp.Topic = topicData.Topic

		for _, partData := range topicData.Partitions {
			partResp := h.buildPartitionError(version, partData.Partition, errorCode)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	return resp
}

// buildPartitionError builds a partition response with an error.
func (h *ProduceHandler) buildPartitionError(version int16, partition int32, errorCode int16) kmsg.ProduceResponseTopicPartition {
	resp := kmsg.NewProduceResponseTopicPartition()
	resp.Partition = partition
	resp.ErrorCode = errorCode
	resp.BaseOffset = -1
	return resp
}

// errInvalidRecordBatch indicates the record batch format is invalid.
var errInvalidRecordBatch = errors.New("invalid record batch format")

// kafkaBatchMagicV2 is the magic byte for Kafka record batch format v2.
const kafkaBatchMagicV2 = 2

// parseRecordBatches extracts batch entries from raw Kafka record batch data.
// Returns the batches, total record count, min timestamp, and max timestamp.
// Validates that record batches have the correct magic byte (v2 format).
func parseRecordBatches(data []byte) ([]wal.BatchEntry, uint32, int64, int64, error) {
	if len(data) == 0 {
		return nil, 0, 0, 0, nil
	}

	var batches []wal.BatchEntry
	var totalRecords uint32
	var minTs, maxTs int64 = 0, 0
	firstBatch := true

	offset := 0
	for offset < len(data) {
		// Kafka record batch format:
		// baseOffset: 8 bytes
		// batchLength: 4 bytes
		// ...rest of batch
		if offset+12 > len(data) {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		batchLength := int(beUint32(data[offset+8:offset+12])) + 12 // +12 for baseOffset and batchLength
		if offset+batchLength > len(data) {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		// Validate minimum batch size: must have at least up to recordCount field
		// Kafka record batch format (after batchLength):
		// partitionLeaderEpoch: 4 bytes (offset 12)
		// magic: 1 byte (offset 16)
		// crc: 4 bytes (offset 17)
		// attributes: 2 bytes (offset 21)
		// lastOffsetDelta: 4 bytes (offset 23)
		// firstTimestamp: 8 bytes (offset 27)
		// maxTimestamp: 8 bytes (offset 35)
		// producerId: 8 bytes (offset 43)
		// producerEpoch: 2 bytes (offset 51)
		// firstSequence: 4 bytes (offset 53)
		// recordCount: 4 bytes (offset 57)
		if batchLength < 61 {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		// Validate magic byte (must be 2 for v2 record batch format)
		magic := data[offset+16]
		if magic != kafkaBatchMagicV2 {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		batchData := make([]byte, batchLength)
		copy(batchData, data[offset:offset+batchLength])
		batches = append(batches, wal.BatchEntry{Data: batchData})

		recordCount := beUint32(data[offset+57 : offset+61])
		totalRecords += recordCount

		batchFirstTs := beInt64(data[offset+27 : offset+35])
		batchMaxTs := beInt64(data[offset+35 : offset+43])

		if firstBatch {
			minTs = batchFirstTs
			maxTs = batchMaxTs
			firstBatch = false
		} else {
			if batchFirstTs < minTs {
				minTs = batchFirstTs
			}
			if batchMaxTs > maxTs {
				maxTs = batchMaxTs
			}
		}

		offset += batchLength
	}

	return batches, totalRecords, minTs, maxTs, nil
}

// isIdempotentRequest checks if the record batches indicate idempotent producer.
// Idempotent producers have producerId >= 0 and producerEpoch >= 0.
func isIdempotentRequest(data []byte) bool {
	if len(data) < 53 {
		return false
	}

	offset := 0
	for offset < len(data) {
		if offset+12 > len(data) {
			break
		}

		batchLength := int(beUint32(data[offset+8:offset+12])) + 12
		if offset+batchLength > len(data) || batchLength < 53 {
			break
		}

		// Check producerId at offset 43 (8 bytes)
		producerId := beInt64(data[offset+43 : offset+51])
		if producerId >= 0 {
			// Non-negative producerId indicates idempotent producer
			return true
		}

		offset += batchLength
	}

	return false
}

// beUint32 reads a big-endian uint32.
func beUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// beInt64 reads a big-endian int64.
func beInt64(b []byte) int64 {
	return int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
		int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
}
