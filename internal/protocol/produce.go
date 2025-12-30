package protocol

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/server"
	"github.com/dray-io/dray/internal/topics"
	"github.com/dray-io/dray/internal/wal"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for Produce responses (reusing errLeaderNotAvailable from metadata.go).
const (
	errNoError                     int16 = 0
	errUnknownTopicOrPartitionErr  int16 = 3
	errInvalidRequiredAcks         int16 = 21
	errTopicAuthorizationFailed    int16 = 29
	errClusterAuthorizationFailed  int16 = 31
	errUnsupportedForMessageFormat int16 = 43
	errInvalidProducerEpoch        int16 = 47
	errInvalidProducerIDMapping    int16 = 49
	errInvalidTransactionalState   int16 = 73
	errTransactionalIDNotFound     int16 = 90
	errNotLeaderOrFollower         int16 = 6
	errRequestTimedOut             int16 = 7
)

// ProduceHandlerConfig configures the produce handler.
type ProduceHandlerConfig struct {
	// RequiredAcks is the required acks setting for this broker.
	// Dray only supports acks=-1 (all replicas).
	RequiredAcks int16

	// TimeoutMs is the default timeout for produce requests.
	TimeoutMs int32

	// LocalNodeID is the node ID of this broker.
	LocalNodeID int32

	// EnforceOwner rejects produce requests routed to a non-affinity broker.
	EnforceOwner bool

	// LeaderSelector provides affinity routing for ownership checks.
	LeaderSelector PartitionLeaderSelector
}

// ProduceHandler handles Produce (key 0) requests.
type ProduceHandler struct {
	cfg        ProduceHandlerConfig
	topicStore *topics.Store
	buffer     *produce.Buffer
	enforcer   *auth.Enforcer
	metrics    *metrics.ProduceMetrics
}

// NewProduceHandler creates a new Produce handler.
func NewProduceHandler(cfg ProduceHandlerConfig, topicStore *topics.Store, buffer *produce.Buffer) *ProduceHandler {
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 15000
	}
	return &ProduceHandler{
		cfg:        cfg,
		topicStore: topicStore,
		buffer:     buffer,
	}
}

// WithEnforcer sets the ACL enforcer for this handler.
func (h *ProduceHandler) WithEnforcer(enforcer *auth.Enforcer) *ProduceHandler {
	h.enforcer = enforcer
	return h
}

// WithMetrics sets the produce metrics collector for this handler.
func (h *ProduceHandler) WithMetrics(m *metrics.ProduceMetrics) *ProduceHandler {
	h.metrics = m
	return h
}

// Handle processes a Produce request.
// Per spec section 9.3, it:
// 1. Validates topic and partition existence
// 2. Rejects idempotent/transactional requests with correct errors
// 3. Buffers incoming record batches by MetaDomain
// 4. Waits for flush completion
// 5. Returns offsets only after WAL + metadata commit
func (h *ProduceHandler) Handle(ctx context.Context, version int16, req *kmsg.ProduceRequest) (resp *kmsg.ProduceResponse) {
	startTime := time.Now()
	reqCtx, cancel := h.requestContext(ctx, req.TimeoutMillis)
	if cancel != nil {
		defer cancel()
	}
	ctx = reqCtx

	resp = kmsg.NewPtrProduceResponse()
	resp.SetVersion(version)

	// Record metrics on exit
	defer func() {
		if h.metrics != nil {
			duration := time.Since(startTime).Seconds()
			hasError := h.responseHasError(resp)
			h.metrics.RecordLatency(duration, !hasError)
		}
	}()

	// Validate acks - Dray only supports acks=-1 (all)
	// Per Kafka protocol, acks can be 0, 1, or -1
	// For acks=0, no response is expected (fire and forget)
	// For acks=1 or -1, we need to wait for commit
	if req.Acks != 0 && req.Acks != 1 && req.Acks != -1 {
		// Invalid acks value
		resp = h.buildErrorResponse(version, req, errInvalidRequiredAcks)
		return resp
	}

	// Reject transactional requests per spec 14.3
	if req.TransactionID != nil && *req.TransactionID != "" {
		logging.FromCtx(ctx).Warnf("rejecting transactional produce request: transactions are explicitly deferred per spec 2.2/14.3", map[string]any{
			"transactionId": *req.TransactionID,
		})
		resp = h.buildErrorResponse(version, req, errUnsupportedVersion)
		return resp
	}

	// Process each topic
	for _, topicData := range req.Topics {
		topicResp := kmsg.NewProduceResponseTopic()
		topicResp.Topic = topicData.Topic

		// Check ACL before processing - need WRITE permission to produce
		if h.enforcer != nil {
			if errCode := h.enforcer.AuthorizeTopicFromCtx(ctx, topicData.Topic, auth.OperationWrite); errCode != nil {
				// Authorization failed - add error for all partitions
				for _, partData := range topicData.Partitions {
					partResp := h.buildPartitionError(version, partData.Partition, *errCode)
					topicResp.Partitions = append(topicResp.Partitions, partResp)
				}
				resp.Topics = append(resp.Topics, topicResp)
				continue
			}
		}

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

	if h.cfg.LeaderSelector != nil {
		zoneID := server.ZoneIDFromContext(ctx)
		leader, err := h.cfg.LeaderSelector.GetPartitionLeader(ctx, zoneID, partMeta.StreamID)
		if err == nil && leader != -1 && leader != h.cfg.LocalNodeID {
			logging.FromCtx(ctx).Warnf("affinity violation: produce request handled by non-owner broker", map[string]any{
				"topic":       topicMeta.Name,
				"partition":   partData.Partition,
				"streamId":    partMeta.StreamID,
				"leaderNode":  leader,
				"localNodeId": h.cfg.LocalNodeID,
				"zoneId":      zoneID,
			})
			if h.cfg.EnforceOwner {
				resp.ErrorCode = errNotLeaderOrFollower
				resp.BaseOffset = -1
				return resp
			}
		}
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
	// Reject idempotent producers per spec 2.2/14.3
	if producerId := extractProducerId(records); producerId >= 0 {
		logging.FromCtx(ctx).Warnf("rejecting idempotent produce request: idempotent producers are explicitly deferred per spec 2.2/14.3", map[string]any{
			"topic":      topicMeta.Name,
			"partition":  partData.Partition,
			"producerId": producerId,
		})
		resp.ErrorCode = errInvalidRequest
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
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			resp.ErrorCode = errRequestTimedOut
		} else if errors.Is(err, produce.ErrBufferFull) {
			resp.ErrorCode = errClusterAuthorizationFailed
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
			if errors.Is(pending.Err, context.DeadlineExceeded) || errors.Is(pending.Err, context.Canceled) {
				resp.ErrorCode = errRequestTimedOut
			} else {
				resp.ErrorCode = errUnknownTopicOrPartitionErr
			}
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
		resp.ErrorCode = errRequestTimedOut
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

// responseHasError checks if any partition in the response has an error code.
func (h *ProduceHandler) responseHasError(resp *kmsg.ProduceResponse) bool {
	for _, topic := range resp.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != errNoError {
				return true
			}
		}
	}
	return false
}

func (h *ProduceHandler) requestContext(ctx context.Context, timeoutMillis int32) (context.Context, context.CancelFunc) {
	timeoutMs := timeoutMillis
	if timeoutMs <= 0 {
		timeoutMs = h.cfg.TimeoutMs
	}
	if timeoutMs <= 0 {
		return ctx, nil
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, nil
	}

	return context.WithTimeout(ctx, timeout)
}

// errInvalidRecordBatch indicates the record batch format is invalid.
var errInvalidRecordBatch = errors.New("invalid record batch format")
var errRecordBatchTooLarge = errors.New("record batch decompressed size exceeds limit")
var errRecordBatchRequestTooLarge = errors.New("produce request decompressed size exceeds limit")

// kafkaBatchMagicV2 is the magic byte for Kafka record batch format v2.
const kafkaBatchMagicV2 = 2
const kafkaBatchMinSize = 61
const (
	maxDecompressedBatchBytes   int64 = 8 * 1024 * 1024
	maxDecompressedRequestBytes int64 = 32 * 1024 * 1024
)

var (
	recordBatchCRCTable = crc32.MakeTable(crc32.Castagnoli)
	xerialSnappyPrefix  = []byte{130, 83, 78, 65, 80, 80, 89, 0}
)

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
	var totalDecompressed int64

	offset := 0
	for offset < len(data) {
		// Kafka record batch format:
		// baseOffset: 8 bytes
		// batchLength: 4 bytes
		// ...rest of batch
		if offset+12 > len(data) {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		rawLength := int32(beUint32(data[offset+8 : offset+12]))
		if rawLength < 0 {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}
		batchLength := int(rawLength) + 12 // +12 for baseOffset and batchLength
		if offset+batchLength > len(data) {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		if batchLength < kafkaBatchMinSize {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		batchData := data[offset : offset+batchLength]
		var recordBatch kmsg.RecordBatch
		if err := recordBatch.ReadFrom(batchData); err != nil {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}
		if recordBatch.Magic != kafkaBatchMagicV2 {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}
		if recordBatch.Length != int32(len(batchData)-12) {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		calculatedCRC := crc32.Checksum(batchData[21:], recordBatchCRCTable)
		if int32(calculatedCRC) != recordBatch.CRC {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		compression := kgo.CompressionCodecType(recordBatch.Attributes & 0x0007)
		if compression < kgo.CodecNone || compression > kgo.CodecZstd {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		decompressedBytes, err := decompressedRecordsSize(recordBatch.Records, compression, maxDecompressedBatchBytes)
		if err != nil {
			if errors.Is(err, errRecordBatchTooLarge) {
				return nil, 0, 0, 0, err
			}
			return nil, 0, 0, 0, errInvalidRecordBatch
		}
		totalDecompressed += decompressedBytes
		if totalDecompressed > maxDecompressedRequestBytes {
			return nil, 0, 0, 0, errRecordBatchRequestTooLarge
		}

		if recordBatch.NumRecords < 0 {
			return nil, 0, 0, 0, errInvalidRecordBatch
		}

		batchCopy := make([]byte, batchLength)
		copy(batchCopy, batchData)
		batches = append(batches, wal.BatchEntry{Data: batchCopy})

		recordCount := uint32(recordBatch.NumRecords)
		totalRecords += recordCount

		batchFirstTs := recordBatch.FirstTimestamp
		batchMaxTs := recordBatch.MaxTimestamp

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

func decompressedRecordsSize(records []byte, compression kgo.CompressionCodecType, limit int64) (int64, error) {
	if compression == kgo.CodecNone {
		size := int64(len(records))
		if size > limit {
			return 0, errRecordBatchTooLarge
		}
		return size, nil
	}

	switch compression {
	case kgo.CodecGzip:
		reader, err := gzip.NewReader(bytes.NewReader(records))
		if err != nil {
			return 0, err
		}
		defer reader.Close()
		return readDecompressedSize(reader, limit)
	case kgo.CodecLz4:
		reader := lz4.NewReader(bytes.NewReader(records))
		return readDecompressedSize(reader, limit)
	case kgo.CodecZstd:
		reader, err := zstd.NewReader(bytes.NewReader(records),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderConcurrency(1),
		)
		if err != nil {
			return 0, err
		}
		defer reader.Close()
		return readDecompressedSize(reader, limit)
	case kgo.CodecSnappy:
		return snappyDecompressedSize(records, limit)
	default:
		return 0, errInvalidRecordBatch
	}
}

func readDecompressedSize(reader io.Reader, limit int64) (int64, error) {
	if limit <= 0 {
		return 0, errRecordBatchTooLarge
	}
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			total += int64(n)
			if total > limit {
				return 0, errRecordBatchTooLarge
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

func snappyDecompressedSize(records []byte, limit int64) (int64, error) {
	if len(records) > 16 && bytes.HasPrefix(records, xerialSnappyPrefix) {
		return xerialDecodedLen(records, limit)
	}

	decodedLen, err := s2.DecodedLen(records)
	if err != nil {
		return 0, err
	}
	if int64(decodedLen) > limit {
		return 0, errRecordBatchTooLarge
	}
	return int64(decodedLen), nil
}

func xerialDecodedLen(records []byte, limit int64) (int64, error) {
	src := records[16:]
	var total int64
	for len(src) > 0 {
		if len(src) < 4 {
			return 0, errInvalidRecordBatch
		}
		size := int(binary.BigEndian.Uint32(src))
		src = src[4:]
		if size <= 0 || len(src) < size {
			return 0, errInvalidRecordBatch
		}
		decodedLen, err := s2.DecodedLen(src[:size])
		if err != nil {
			return 0, err
		}
		total += int64(decodedLen)
		if total > limit {
			return 0, errRecordBatchTooLarge
		}
		src = src[size:]
	}
	return total, nil
}

// extractProducerId extracts the producer ID from record batch data.
// Returns the first non-negative producer ID found, or -1 if none found.
// Idempotent producers have producerId >= 0.
func extractProducerId(data []byte) int64 {
	if len(data) < 53 {
		return -1
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
			return producerId
		}

		offset += batchLength
	}

	return -1
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
