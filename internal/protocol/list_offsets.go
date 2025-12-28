package protocol

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListOffsets special timestamp values.
const (
	// TimestampLatest returns the log end offset (HWM, end-exclusive).
	TimestampLatest int64 = -1
	// TimestampEarliest returns the earliest available offset.
	TimestampEarliest int64 = -2
	// TimestampMaxTimestamp returns the offset with the largest timestamp (Kafka 3.0+).
	TimestampMaxTimestamp int64 = -3
	// TimestampLocalLogStart returns the local log start offset (Kafka 3.4+, tiered storage).
	TimestampLocalLogStart int64 = -4
)

// ListOffsetsHandler handles ListOffsets (key 2) requests.
// Per SPEC section 10.4, this is an early-implement API because many clients
// call it at startup to determine earliest/latest offsets.
type ListOffsetsHandler struct {
	topicStore    *topics.Store
	streamManager *index.StreamManager
}

// NewListOffsetsHandler creates a new ListOffsets handler.
func NewListOffsetsHandler(topicStore *topics.Store, streamManager *index.StreamManager) *ListOffsetsHandler {
	return &ListOffsetsHandler{
		topicStore:    topicStore,
		streamManager: streamManager,
	}
}

// Handle processes a ListOffsets request.
// Per SPEC section 10.4:
//   - LATEST: return hwm (LEO, end-exclusive)
//   - EARLIEST: return smallest available offset (typically 0 unless retention deleted earlier)
//   - TIMESTAMP: binary search using index entry timestamps
func (h *ListOffsetsHandler) Handle(ctx context.Context, version int16, req *kmsg.ListOffsetsRequest) *kmsg.ListOffsetsResponse {
	resp := kmsg.NewPtrListOffsetsResponse()
	resp.SetVersion(version)

	// Set throttle time (version >= 2)
	if version >= 2 {
		resp.ThrottleMillis = 0
	}

	// Process each topic
	for _, topicReq := range req.Topics {
		topicResp := kmsg.NewListOffsetsResponseTopic()
		topicResp.Topic = topicReq.Topic

		// Get topic metadata
		topicMeta, err := h.topicStore.GetTopic(ctx, topicReq.Topic)
		if err != nil {
			// Topic not found - add error for all partitions
			for _, partReq := range topicReq.Partitions {
				partResp := h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr)
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
			continue
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

// processPartition handles a single partition's list offsets request.
func (h *ListOffsetsHandler) processPartition(ctx context.Context, version int16, topicName string, partReq *kmsg.ListOffsetsRequestTopicPartition) kmsg.ListOffsetsResponseTopicPartition {
	// Get partition metadata to get streamId
	partMeta, err := h.topicStore.GetPartition(ctx, topicName, partReq.Partition)
	if err != nil {
		if errors.Is(err, topics.ErrPartitionNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr)
		}
		return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr)
	}

	// Check partition state
	if partMeta.State != "active" {
		return h.buildPartitionError(version, partReq.Partition, errLeaderNotAvailable)
	}

	streamID := partMeta.StreamID
	timestamp := partReq.Timestamp

	switch timestamp {
	case TimestampLatest:
		return h.handleLatest(ctx, version, partReq.Partition, streamID)
	case TimestampEarliest:
		return h.handleEarliest(ctx, version, partReq.Partition, streamID)
	case TimestampMaxTimestamp:
		return h.handleMaxTimestamp(ctx, version, partReq.Partition, streamID)
	case TimestampLocalLogStart:
		// For tiered storage - same as earliest for Dray since no local/remote distinction
		return h.handleEarliest(ctx, version, partReq.Partition, streamID)
	default:
		// Timestamp-based lookup
		return h.handleTimestamp(ctx, version, partReq.Partition, streamID, timestamp)
	}
}

// handleLatest returns the HWM (LEO, log end offset) for the partition.
func (h *ListOffsetsHandler) handleLatest(ctx context.Context, version int16, partition int32, streamID string) kmsg.ListOffsetsResponseTopicPartition {
	hwm, _, err := h.streamManager.GetHWM(ctx, streamID)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
		}
		return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
	}

	return h.buildPartitionSuccess(version, partition, hwm, TimestampLatest)
}

// handleEarliest returns the earliest available offset for the partition.
func (h *ListOffsetsHandler) handleEarliest(ctx context.Context, version int16, partition int32, streamID string) kmsg.ListOffsetsResponseTopicPartition {
	earliestOffset, err := h.streamManager.GetEarliestOffset(ctx, streamID)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
		}
		return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
	}

	return h.buildPartitionSuccess(version, partition, earliestOffset, TimestampEarliest)
}

// handleMaxTimestamp returns the offset with the maximum timestamp (Kafka 3.0+, KIP-734).
func (h *ListOffsetsHandler) handleMaxTimestamp(ctx context.Context, version int16, partition int32, streamID string) kmsg.ListOffsetsResponseTopicPartition {
	// Get the HWM first to verify stream exists
	hwm, _, err := h.streamManager.GetHWM(ctx, streamID)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
		}
		return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
	}

	// If stream is empty, return -1 for both
	if hwm == 0 {
		return h.buildPartitionSuccess(version, partition, -1, -1)
	}

	// List all entries to find max timestamp
	entries, err := h.streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
	}

	if len(entries) == 0 {
		return h.buildPartitionSuccess(version, partition, -1, -1)
	}

	// Find the entry with the maximum timestamp
	var maxTs int64 = -1
	var maxTsOffset int64 = -1
	for _, entry := range entries {
		if entry.MaxTimestampMs > maxTs {
			maxTs = entry.MaxTimestampMs
			// The offset corresponding to max timestamp is the last offset in this entry
			maxTsOffset = entry.EndOffset - 1
		}
	}

	return h.buildPartitionSuccess(version, partition, maxTsOffset, maxTs)
}

// handleTimestamp returns the first offset with timestamp >= requested timestamp.
func (h *ListOffsetsHandler) handleTimestamp(ctx context.Context, version int16, partition int32, streamID string, timestamp int64) kmsg.ListOffsetsResponseTopicPartition {
	result, err := h.streamManager.LookupOffsetByTimestamp(ctx, streamID, timestamp)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
		}
		return h.buildPartitionError(version, partition, errUnknownTopicOrPartitionErr)
	}

	if !result.Found {
		// No offset found for the timestamp - return -1 for both per Kafka protocol
		return h.buildPartitionSuccess(version, partition, -1, -1)
	}

	return h.buildPartitionSuccess(version, partition, result.Offset, result.Timestamp)
}

// buildPartitionSuccess builds a successful partition response.
func (h *ListOffsetsHandler) buildPartitionSuccess(version int16, partition int32, offset int64, timestamp int64) kmsg.ListOffsetsResponseTopicPartition {
	partResp := kmsg.NewListOffsetsResponseTopicPartition()
	partResp.Partition = partition
	partResp.ErrorCode = errNoError

	if version >= 1 {
		partResp.Timestamp = timestamp
		partResp.Offset = offset
	} else {
		// Version 0 uses OldStyleOffsets array
		partResp.OldStyleOffsets = []int64{offset}
	}

	if version >= 4 {
		partResp.LeaderEpoch = -1 // No leader epoch in Dray
	}

	return partResp
}

// buildPartitionError builds a partition response with an error code.
func (h *ListOffsetsHandler) buildPartitionError(version int16, partition int32, errorCode int16) kmsg.ListOffsetsResponseTopicPartition {
	partResp := kmsg.NewListOffsetsResponseTopicPartition()
	partResp.Partition = partition
	partResp.ErrorCode = errorCode

	if version >= 1 {
		partResp.Timestamp = -1
		partResp.Offset = -1
	}

	if version >= 4 {
		partResp.LeaderEpoch = -1
	}

	return partResp
}
