package protocol

import (
	"context"
	"errors"
	"time"

	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for CreateTopics.
const (
	errTopicAlreadyExists       int16 = 36
	errInvalidPartitions        int16 = 37
	errInvalidReplicationFactor int16 = 38
	errInvalidTopicException    int16 = 17
)

// CreateTopicsHandlerConfig configures the CreateTopics handler.
type CreateTopicsHandlerConfig struct {
	// DefaultPartitions is the default partition count when -1 is specified.
	DefaultPartitions int32
	// DefaultReplicationFactor is the default replication factor when -1 is specified.
	DefaultReplicationFactor int16
	// IcebergEnabled enables Iceberg table creation for topics.
	IcebergEnabled bool
	// ClusterID is the Dray cluster identifier stored in Iceberg table properties.
	ClusterID string
	// IcebergNamespace is the Iceberg namespace for tables.
	// Defaults to ["dray"] if not specified.
	IcebergNamespace []string
}

// CreateTopicsHandler handles CreateTopics (key 19) requests.
type CreateTopicsHandler struct {
	cfg           CreateTopicsHandlerConfig
	topicStore    *topics.Store
	streamManager *index.StreamManager
	tableCreator  *catalog.TableCreator
}

// NewCreateTopicsHandler creates a new CreateTopics handler.
func NewCreateTopicsHandler(
	cfg CreateTopicsHandlerConfig,
	topicStore *topics.Store,
	streamManager *index.StreamManager,
	icebergCatalog catalog.Catalog,
) *CreateTopicsHandler {
	var tableCreator *catalog.TableCreator
	if cfg.IcebergEnabled && icebergCatalog != nil {
		tableCreator = catalog.NewTableCreator(catalog.TableCreatorConfig{
			Catalog:   icebergCatalog,
			Namespace: cfg.IcebergNamespace,
			ClusterID: cfg.ClusterID,
		})
	}

	return &CreateTopicsHandler{
		cfg:          cfg,
		topicStore:   topicStore,
		streamManager: streamManager,
		tableCreator: tableCreator,
	}
}

// Handle processes a CreateTopics request.
func (h *CreateTopicsHandler) Handle(ctx context.Context, version int16, req *kmsg.CreateTopicsRequest) *kmsg.CreateTopicsResponse {
	resp := kmsg.NewPtrCreateTopicsResponse()
	resp.SetVersion(version)

	if version >= 2 {
		resp.ThrottleMillis = 0
	}

	for _, topicReq := range req.Topics {
		topicResp := h.handleTopic(ctx, version, topicReq, req.ValidateOnly)
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// handleTopic handles creation of a single topic.
func (h *CreateTopicsHandler) handleTopic(ctx context.Context, version int16, topicReq kmsg.CreateTopicsRequestTopic, validateOnly bool) kmsg.CreateTopicsResponseTopic {
	resp := kmsg.NewCreateTopicsResponseTopic()
	resp.Topic = topicReq.Topic

	// Validate topic name
	if topicReq.Topic == "" {
		resp.ErrorCode = errInvalidTopicException
		if version >= 1 {
			errMsg := "Topic name is empty"
			resp.ErrorMessage = &errMsg
		}
		return resp
	}

	// Determine partition count
	numPartitions := topicReq.NumPartitions
	if numPartitions == -1 {
		// Use default from config (v4+ feature)
		numPartitions = h.cfg.DefaultPartitions
	}
	if numPartitions <= 0 {
		resp.ErrorCode = errInvalidPartitions
		if version >= 1 {
			errMsg := "Number of partitions must be positive"
			resp.ErrorMessage = &errMsg
		}
		return resp
	}

	// Determine replication factor (Dray ignores this but validates it)
	replicationFactor := topicReq.ReplicationFactor
	if replicationFactor == -1 {
		replicationFactor = h.cfg.DefaultReplicationFactor
	}
	if replicationFactor <= 0 {
		resp.ErrorCode = errInvalidReplicationFactor
		if version >= 1 {
			errMsg := "Replication factor must be positive"
			resp.ErrorMessage = &errMsg
		}
		return resp
	}

	// Build config map from request
	config := make(map[string]string)
	for _, cfg := range topicReq.Configs {
		if cfg.Value != nil {
			config[cfg.Name] = *cfg.Value
		}
	}

	// If validate only, skip actual creation
	if validateOnly {
		resp.ErrorCode = errNone
		if version >= 5 {
			resp.NumPartitions = numPartitions
			resp.ReplicationFactor = replicationFactor
		}
		return resp
	}

	// Create the topic in the topic store
	nowMs := time.Now().UnixMilli()
	result, err := h.topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicReq.Topic,
		PartitionCount: numPartitions,
		Config:         config,
		NowMs:          nowMs,
	})
	if err != nil {
		if errors.Is(err, topics.ErrTopicExists) {
			resp.ErrorCode = errTopicAlreadyExists
			if version >= 1 {
				errMsg := "Topic already exists"
				resp.ErrorMessage = &errMsg
			}
		} else if errors.Is(err, topics.ErrInvalidTopicName) {
			resp.ErrorCode = errInvalidTopicException
			if version >= 1 {
				errMsg := "Invalid topic name"
				resp.ErrorMessage = &errMsg
			}
		} else if errors.Is(err, topics.ErrInvalidPartitions) {
			resp.ErrorCode = errInvalidPartitions
			if version >= 1 {
				errMsg := "Invalid partition count"
				resp.ErrorMessage = &errMsg
			}
		} else {
			// Generic error
			resp.ErrorCode = errUnknownTopicOrPartition
			if version >= 1 {
				errMsg := err.Error()
				resp.ErrorMessage = &errMsg
			}
		}
		return resp
	}

	// Create stream entries for each partition (initializes HWM to 0)
	for _, partition := range result.Partitions {
		err := h.streamManager.CreateStreamWithID(ctx, partition.StreamID, topicReq.Topic, partition.Partition)
		if err != nil {
			// Stream creation failed - this shouldn't happen in normal operation
			// since we just created the partition with a fresh UUID.
			// Log this as a warning but don't fail the topic creation since
			// the topic metadata already exists.
			if !errors.Is(err, index.ErrStreamExists) {
				// Stream doesn't exist yet, but creation failed for another reason
				resp.ErrorCode = errUnknownTopicOrPartition
				if version >= 1 {
					errMsg := "Failed to initialize partition stream: " + err.Error()
					resp.ErrorMessage = &errMsg
				}
				return resp
			}
			// Stream already exists - this is fine, continue
		}
	}

	// Create Iceberg table if duality mode is enabled for this topic.
	// Check per-topic override (table.iceberg.enabled) with global default.
	topicIcebergEnabled := topics.GetIcebergEnabled(config, h.cfg.IcebergEnabled)
	if h.tableCreator != nil && topicIcebergEnabled {
		_, err := h.tableCreator.CreateTableForTopic(ctx, topicReq.Topic)
		if err != nil {
			// Iceberg table creation failure should not fail topic creation
			// per spec - produce/fetch should remain available when Iceberg is down.
			// We just log the error and continue.
			// TODO: Log this error when logging is wired in
		}
	}

	// Success
	resp.ErrorCode = errNone
	if version >= 5 {
		resp.NumPartitions = numPartitions
		resp.ReplicationFactor = replicationFactor
		// Configs are optionally returned in v5+
		for name, value := range config {
			cfgResp := kmsg.NewCreateTopicsResponseTopicConfig()
			cfgResp.Name = name
			valueCopy := value
			cfgResp.Value = &valueCopy
			resp.Configs = append(resp.Configs, cfgResp)
		}
	}
	if version >= 7 {
		resp.TopicID = parseTopicID(result.Topic.TopicID)
	}

	return resp
}
