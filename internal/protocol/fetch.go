package protocol

import (
	"bytes"
	"context"
	"errors"

	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for Fetch responses.
const (
	errOffsetOutOfRange int16 = 1
	// errUnknownTopicOrPartition is already defined in produce.go as errUnknownTopicOrPartitionErr
)

// FetchHandlerConfig configures the fetch handler.
type FetchHandlerConfig struct {
	// MaxBytes is the default maximum bytes to return per partition.
	MaxBytes int32
}

// FetchHandler handles Fetch (key 1) requests.
type FetchHandler struct {
	cfg           FetchHandlerConfig
	topicStore    *topics.Store
	fetcher       *fetch.Fetcher
	streamManager *index.StreamManager
}

// NewFetchHandler creates a new Fetch handler.
func NewFetchHandler(cfg FetchHandlerConfig, topicStore *topics.Store, fetcher *fetch.Fetcher, streamManager *index.StreamManager) *FetchHandler {
	return &FetchHandler{
		cfg:           cfg,
		topicStore:    topicStore,
		fetcher:       fetcher,
		streamManager: streamManager,
	}
}

// Handle processes a Fetch request.
// Per the task requirements, it:
//  1. Resolves streamId from topic/partition
//  2. Checks fetchOffset against hwm
//  3. Finds index entry for requested offset
//  4. Reads data from WAL or Parquet
//  5. Applies record batch offset patching
//  6. Returns batches up to maxBytes
//  7. Sets high watermark in response
func (h *FetchHandler) Handle(ctx context.Context, version int16, req *kmsg.FetchRequest) *kmsg.FetchResponse {
	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(version)

	// Set throttle time (version >= 1)
	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	// Process each topic
	for _, topicReq := range req.Topics {
		topicResp := kmsg.NewFetchResponseTopic()
		topicResp.Topic = topicReq.Topic

		// Get topic metadata to validate topic exists
		topicMeta, err := h.topicStore.GetTopic(ctx, topicReq.Topic)
		if err != nil {
			// Topic not found - add error for all partitions
			for _, partReq := range topicReq.Partitions {
				partResp := h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0)
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
			continue
		}

		// For version >= 13, we need to set TopicID
		if version >= 13 {
			topicResp.TopicID = parseTopicID(topicMeta.TopicID)
		}

		// Process each partition
		for _, partReq := range topicReq.Partitions {
			partResp := h.processPartition(ctx, version, topicMeta.Name, &partReq)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// processPartition handles a single partition's fetch request.
func (h *FetchHandler) processPartition(ctx context.Context, version int16, topicName string, partReq *kmsg.FetchRequestTopicPartition) kmsg.FetchResponseTopicPartition {
	partResp := kmsg.NewFetchResponseTopicPartition()
	partResp.Partition = partReq.Partition

	// Get partition metadata to get streamId
	partMeta, err := h.topicStore.GetPartition(ctx, topicName, partReq.Partition)
	if err != nil {
		if errors.Is(err, topics.ErrPartitionNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0)
		}
		return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0)
	}

	// Check partition state
	if partMeta.State != "active" {
		return h.buildPartitionError(version, partReq.Partition, errLeaderNotAvailable, 0)
	}

	streamID := partMeta.StreamID

	// Get HWM for the response
	hwm, _, err := h.streamManager.GetHWM(ctx, streamID)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0)
		}
		return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0)
	}

	fetchOffset := partReq.FetchOffset

	// Calculate maxBytes for this partition
	maxBytes := int64(partReq.PartitionMaxBytes)
	if maxBytes <= 0 {
		maxBytes = int64(h.cfg.MaxBytes)
	}

	// Fetch records
	fetchResp, err := h.fetcher.Fetch(ctx, &fetch.FetchRequest{
		StreamID:    streamID,
		FetchOffset: fetchOffset,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, hwm)
		}
		if errors.Is(err, fetch.ErrOffsetNotInChunk) {
			return h.buildPartitionError(version, partReq.Partition, errOffsetOutOfRange, hwm)
		}
		return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, hwm)
	}

	// Build successful response
	partResp.ErrorCode = errNoError
	partResp.HighWatermark = fetchResp.HighWatermark
	partResp.LastStableOffset = fetchResp.HighWatermark // No transactions, so LSO = HWM
	partResp.LogStartOffset = 0                         // TODO: Track earliest offset after compaction

	// Set preferred read replica (version >= 11)
	if version >= 11 {
		partResp.PreferredReadReplica = -1 // No preferred replica, we're the only one
	}

	// Combine batches into RecordBatches
	if len(fetchResp.Batches) > 0 {
		var buf bytes.Buffer
		for _, batch := range fetchResp.Batches {
			buf.Write(batch)
		}
		partResp.RecordBatches = buf.Bytes()
	}

	return partResp
}

// buildPartitionError builds a partition response with an error code.
func (h *FetchHandler) buildPartitionError(version int16, partition int32, errorCode int16, hwm int64) kmsg.FetchResponseTopicPartition {
	partResp := kmsg.NewFetchResponseTopicPartition()
	partResp.Partition = partition
	partResp.ErrorCode = errorCode
	partResp.HighWatermark = hwm
	partResp.LastStableOffset = hwm
	partResp.LogStartOffset = 0

	if version >= 11 {
		partResp.PreferredReadReplica = -1
	}

	return partResp
}

// NOTE: parseTopicID, parseHexByte, hexVal are defined in metadata.go
