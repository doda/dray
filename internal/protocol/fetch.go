package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/dray-io/dray/internal/server"
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

	// LocalNodeID is the node ID of this broker.
	LocalNodeID int32

	// EnforceOwner rejects fetch requests routed to a non-affinity broker.
	EnforceOwner bool

	// LeaderSelector provides affinity routing for ownership checks.
	LeaderSelector PartitionLeaderSelector
}

// FetchHandler handles Fetch (key 1) requests.
type FetchHandler struct {
	cfg           FetchHandlerConfig
	topicStore    *topics.Store
	fetcher       *fetch.Fetcher
	streamManager *index.StreamManager
	hwmWatcher    *fetch.HWMWatcher
	enforcer      *auth.Enforcer
	metrics       *metrics.FetchMetrics
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

// NewFetchHandlerWithWatcher creates a new Fetch handler with long-poll support.
func NewFetchHandlerWithWatcher(cfg FetchHandlerConfig, topicStore *topics.Store, fetcher *fetch.Fetcher, streamManager *index.StreamManager, hwmWatcher *fetch.HWMWatcher) *FetchHandler {
	return &FetchHandler{
		cfg:           cfg,
		topicStore:    topicStore,
		fetcher:       fetcher,
		streamManager: streamManager,
		hwmWatcher:    hwmWatcher,
	}
}

// WithEnforcer sets the ACL enforcer for this handler.
func (h *FetchHandler) WithEnforcer(enforcer *auth.Enforcer) *FetchHandler {
	h.enforcer = enforcer
	return h
}

// WithMetrics sets the fetch metrics collector for this handler.
func (h *FetchHandler) WithMetrics(m *metrics.FetchMetrics) *FetchHandler {
	h.metrics = m
	return h
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
//  8. If fetchOffset >= hwm and maxWaitMs > 0, waits for new data via long-poll
func (h *FetchHandler) Handle(ctx context.Context, version int16, req *kmsg.FetchRequest) (resp *kmsg.FetchResponse) {
	startTime := time.Now()

	resp = kmsg.NewPtrFetchResponse()
	resp.SetVersion(version)

	// Record metrics on exit
	defer func() {
		if h.metrics != nil {
			duration := time.Since(startTime).Seconds()
			hasError := h.responseHasError(resp)
			h.metrics.RecordLatency(duration, !hasError)
		}
	}()

	// Set throttle time (version >= 1)
	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	// Get maxWaitMs from request (used for long-poll)
	maxWaitMs := req.MaxWaitMillis

	// Process each topic
	for _, topicReq := range req.Topics {
		topicResp := kmsg.NewFetchResponseTopic()
		topicResp.Topic = topicReq.Topic

		// For v13+, the client sends TopicID instead of Topic name
		// We need to resolve the topic name from the TopicID
		topicName := topicReq.Topic
		if version >= 13 && topicName == "" {
			// Look up topic by TopicID - convert bytes to UUID string with dashes
			id := topicReq.TopicID
			topicIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
			meta, err := h.topicStore.GetTopicByID(ctx, topicIDStr)
			if err == nil {
				topicName = meta.Name
			}
		}

		// Check ACL before processing - need READ permission to fetch
		if h.enforcer != nil {
			if errCode := h.enforcer.AuthorizeTopicFromCtx(ctx, topicName, auth.OperationRead); errCode != nil {
				// Authorization failed - add error for all partitions
				for _, partReq := range topicReq.Partitions {
					partResp := h.buildPartitionError(version, partReq.Partition, *errCode, 0, 0)
					topicResp.Partitions = append(topicResp.Partitions, partResp)
				}
				resp.Topics = append(resp.Topics, topicResp)
				continue
			}
		}

		// Get topic metadata to validate topic exists
		topicMeta, err := h.topicStore.GetTopic(ctx, topicName)
		if err != nil {
			// Topic not found - add error for all partitions
			for _, partReq := range topicReq.Partitions {
				partResp := h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0, 0)
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
			partResp := h.processPartition(ctx, version, topicMeta.Name, &partReq, maxWaitMs)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// processPartition handles a single partition's fetch request.
// It implements long-poll waiting when fetchOffset >= hwm and maxWaitMs > 0.
func (h *FetchHandler) processPartition(ctx context.Context, version int16, topicName string, partReq *kmsg.FetchRequestTopicPartition, maxWaitMs int32) kmsg.FetchResponseTopicPartition {
	partResp := kmsg.NewFetchResponseTopicPartition()
	partResp.Partition = partReq.Partition

	// Get partition metadata to get streamId
	partMeta, err := h.topicStore.GetPartition(ctx, topicName, partReq.Partition)
	if err != nil {
		if errors.Is(err, topics.ErrPartitionNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0, 0)
		}
		return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0, 0)
	}

	// Check partition state
	if partMeta.State != "active" {
		return h.buildPartitionError(version, partReq.Partition, errLeaderNotAvailable, 0, 0)
	}

	if h.cfg.LeaderSelector != nil {
		zoneID := server.ZoneIDFromContext(ctx)
		leader, err := h.cfg.LeaderSelector.GetPartitionLeader(ctx, zoneID, partMeta.StreamID)
		if err == nil && leader != -1 && leader != h.cfg.LocalNodeID {
			logging.FromCtx(ctx).Warnf("affinity violation: fetch request handled by non-owner broker", map[string]any{
				"topic":       topicName,
				"partition":   partReq.Partition,
				"streamId":    partMeta.StreamID,
				"leaderNode":  leader,
				"localNodeId": h.cfg.LocalNodeID,
				"zoneId":      zoneID,
			})
			if h.cfg.EnforceOwner {
				return h.buildPartitionError(version, partReq.Partition, errNotLeaderOrFollower, 0, 0)
			}
		}
	}

	streamID := partMeta.StreamID

	// Get HWM for the response
	hwm, _, err := h.streamManager.GetHWM(ctx, streamID)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0, 0)
		}
		return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, 0, 0)
	}

	earliestOffset, err := h.streamManager.GetEarliestOffset(ctx, streamID)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, hwm, 0)
		}
		logging.FromCtx(ctx).Warnf("failed to load earliest offset for fetch", map[string]any{
			"topic":     topicName,
			"partition": partReq.Partition,
			"streamId":  streamID,
			"error":     err,
		})
		earliestOffset = 0
	}

	fetchOffset := partReq.FetchOffset

	// Long-poll waiting: If fetchOffset >= hwm and maxWaitMs > 0, wait for new data
	// Per Kafka protocol, this implements long-polling for consumers waiting at the end.
	if fetchOffset >= hwm && maxWaitMs > 0 && h.hwmWatcher != nil {
		maxWait := time.Duration(maxWaitMs) * time.Millisecond
		waitResult, err := h.hwmWatcher.WaitForHWM(ctx, streamID, fetchOffset, maxWait)
		if err != nil {
			// On error, return current state with HWM
			h.recordSourceLatency(time.Time{}, fetch.SourceNone) // empty fetch
			return h.buildEmptyPartitionResponse(version, partReq.Partition, hwm, earliestOffset)
		}

		// Update HWM from wait result
		hwm = waitResult.CurrentHWM

		// If still no new data after waiting, return empty response
		if !waitResult.NewDataAvailable {
			h.recordSourceLatency(time.Time{}, fetch.SourceNone) // empty fetch
			return h.buildEmptyPartitionResponse(version, partReq.Partition, hwm, earliestOffset)
		}
	}

	// Calculate maxBytes for this partition
	maxBytes := int64(partReq.PartitionMaxBytes)
	if maxBytes <= 0 {
		maxBytes = int64(h.cfg.MaxBytes)
	}

	// Fetch records - track latency by source
	fetchStart := time.Now()
	fetchResp, err := h.fetcher.Fetch(ctx, &fetch.FetchRequest{
		StreamID:    streamID,
		FetchOffset: fetchOffset,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, hwm, earliestOffset)
		}
		if errors.Is(err, fetch.ErrOffsetNotInChunk) {
			return h.buildPartitionError(version, partReq.Partition, errOffsetOutOfRange, hwm, earliestOffset)
		}
		return h.buildPartitionError(version, partReq.Partition, errUnknownTopicOrPartitionErr, hwm, earliestOffset)
	}

	// Record source-specific latency
	h.recordSourceLatency(fetchStart, fetchResp.Source)

	// If offset is beyond HWM and no watcher configured (or maxWaitMs=0),
	// just return empty response with current HWM
	if fetchResp.OffsetBeyondHWM {
		return h.buildEmptyPartitionResponse(version, partReq.Partition, fetchResp.HighWatermark, earliestOffset)
	}

	// Build successful response
	partResp.ErrorCode = errNoError
	partResp.HighWatermark = fetchResp.HighWatermark
	partResp.LastStableOffset = fetchResp.HighWatermark // No transactions, so LSO = HWM
	partResp.LogStartOffset = earliestOffset

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

// recordSourceLatency records source-specific latency metrics.
// If start is zero, records a minimal latency for empty/none fetches.
func (h *FetchHandler) recordSourceLatency(start time.Time, source string) {
	if h.metrics == nil {
		return
	}
	var duration float64
	if start.IsZero() {
		duration = 0
	} else {
		duration = time.Since(start).Seconds()
	}
	h.metrics.RecordSourceLatency(duration, source)
}

// buildEmptyPartitionResponse builds a successful response with no record batches.
// Used when fetch offset is at or beyond HWM (consumer is at end of log).
func (h *FetchHandler) buildEmptyPartitionResponse(version int16, partition int32, hwm int64, logStartOffset int64) kmsg.FetchResponseTopicPartition {
	partResp := kmsg.NewFetchResponseTopicPartition()
	partResp.Partition = partition
	partResp.ErrorCode = errNoError
	partResp.HighWatermark = hwm
	partResp.LastStableOffset = hwm
	partResp.LogStartOffset = logStartOffset

	if version >= 11 {
		partResp.PreferredReadReplica = -1
	}

	return partResp
}

// buildPartitionError builds a partition response with an error code.
func (h *FetchHandler) buildPartitionError(version int16, partition int32, errorCode int16, hwm int64, logStartOffset int64) kmsg.FetchResponseTopicPartition {
	partResp := kmsg.NewFetchResponseTopicPartition()
	partResp.Partition = partition
	partResp.ErrorCode = errorCode
	partResp.HighWatermark = hwm
	partResp.LastStableOffset = hwm
	partResp.LogStartOffset = logStartOffset

	if version >= 11 {
		partResp.PreferredReadReplica = -1
	}

	return partResp
}

// responseHasError checks if any partition in the response has an error code.
func (h *FetchHandler) responseHasError(resp *kmsg.FetchResponse) bool {
	for _, topic := range resp.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != errNoError {
				return true
			}
		}
	}
	return false
}

// NOTE: parseTopicID, parseHexByte, hexVal are defined in metadata.go
