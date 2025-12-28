package protocol

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes used in Metadata responses.
// See: https://kafka.apache.org/protocol#protocol_error_codes
const (
	errNone                    int16 = 0
	errUnknownTopicOrPartition int16 = 3
	errLeaderNotAvailable      int16 = 5
)

// BrokerInfo holds broker information for metadata responses.
type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

// MetadataHandlerConfig configures the metadata handler.
type MetadataHandlerConfig struct {
	ClusterID           string
	ControllerID        int32
	AutoCreateTopics    bool
	DefaultPartitions   int32
	DefaultReplication  int16
	LocalBroker         BrokerInfo
	BrokerLister        BrokerLister
}

// BrokerLister provides access to registered brokers.
type BrokerLister interface {
	// ListBrokers returns all registered brokers. If zoneID is non-empty,
	// it may filter to return only brokers in the specified zone.
	ListBrokers(ctx context.Context, zoneID string) ([]BrokerInfo, error)
}

// MetadataHandler handles Metadata (key 3) requests.
type MetadataHandler struct {
	cfg        MetadataHandlerConfig
	topicStore *topics.Store
}

// NewMetadataHandler creates a new Metadata handler.
func NewMetadataHandler(cfg MetadataHandlerConfig, topicStore *topics.Store) *MetadataHandler {
	return &MetadataHandler{
		cfg:        cfg,
		topicStore: topicStore,
	}
}

// Handle processes a Metadata request.
func (h *MetadataHandler) Handle(ctx context.Context, version int16, req *kmsg.MetadataRequest, zoneID string) *kmsg.MetadataResponse {
	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(version)

	// Get brokers - optionally filtered by zone
	brokers := h.getBrokers(ctx, zoneID)
	for _, b := range brokers {
		broker := kmsg.NewMetadataResponseBroker()
		broker.NodeID = b.NodeID
		broker.Host = b.Host
		broker.Port = b.Port
		if version >= 1 && b.Rack != "" {
			broker.Rack = &b.Rack
		}
		resp.Brokers = append(resp.Brokers, broker)
	}

	// Set cluster metadata
	if version >= 1 {
		resp.ThrottleMillis = 0
	}
	if version >= 2 {
		resp.ClusterID = &h.cfg.ClusterID
	}
	if version >= 1 {
		resp.ControllerID = h.cfg.ControllerID
	}

	// Process topic requests
	topics := h.getRequestedTopics(ctx, version, req, zoneID)
	resp.Topics = topics

	return resp
}

// getBrokers retrieves broker list, optionally filtered by zone.
func (h *MetadataHandler) getBrokers(ctx context.Context, zoneID string) []BrokerInfo {
	if h.cfg.BrokerLister == nil {
		// Fallback to local broker only
		return []BrokerInfo{h.cfg.LocalBroker}
	}

	brokers, err := h.cfg.BrokerLister.ListBrokers(ctx, zoneID)
	if err != nil || len(brokers) == 0 {
		// Fallback to all brokers if zone filtering returns nothing
		brokers, _ = h.cfg.BrokerLister.ListBrokers(ctx, "")
	}
	if len(brokers) == 0 {
		return []BrokerInfo{h.cfg.LocalBroker}
	}
	return brokers
}

// getRequestedTopics processes the topic request and returns topic metadata.
func (h *MetadataHandler) getRequestedTopics(ctx context.Context, version int16, req *kmsg.MetadataRequest, zoneID string) []kmsg.MetadataResponseTopic {
	var result []kmsg.MetadataResponseTopic

	// Determine which topics to return
	var topicNames []string
	requestAllTopics := false
	allowAutoCreate := h.cfg.AutoCreateTopics
	if version >= 4 && !req.AllowAutoTopicCreation {
		allowAutoCreate = false
	}

	if len(req.Topics) == 0 {
		// Empty array means request all topics
		requestAllTopics = true
	} else {
		for _, t := range req.Topics {
			if t.Topic == nil {
				// null topic in array means request all topics
				requestAllTopics = true
				break
			}
			topicNames = append(topicNames, *t.Topic)
		}
	}

	if requestAllTopics {
		// Return metadata for all topics
		allTopics, err := h.topicStore.ListTopics(ctx)
		if err == nil {
			for _, t := range allTopics {
				topic := h.buildTopicMetadata(ctx, version, t.Name, zoneID, allowAutoCreate)
				result = append(result, topic)
			}
		}
	} else {
		// Return metadata for requested topics
		for _, name := range topicNames {
			topic := h.buildTopicMetadata(ctx, version, name, zoneID, allowAutoCreate)
			result = append(result, topic)
		}
	}

	return result
}

// buildTopicMetadata builds metadata for a single topic.
func (h *MetadataHandler) buildTopicMetadata(ctx context.Context, version int16, topicName string, zoneID string, allowAutoCreate bool) kmsg.MetadataResponseTopic {
	topic := kmsg.NewMetadataResponseTopic()
	topic.Topic = &topicName

	// Try to get topic metadata
	topicMeta, err := h.topicStore.GetTopic(ctx, topicName)
	if err != nil {
		if !errors.Is(err, topics.ErrTopicNotFound) {
			topic.ErrorCode = errUnknownTopicOrPartition
			return topic
		}
		// Topic not found - check auto-creation
		if allowAutoCreate {
			// Auto-create the topic
			result, createErr := h.topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
				Name:           topicName,
				PartitionCount: h.cfg.DefaultPartitions,
				NowMs:          time.Now().UnixMilli(),
			})
			if createErr == nil {
				topicMeta = &result.Topic
			} else {
				topic.ErrorCode = errUnknownTopicOrPartition
				return topic
			}
		} else {
			topic.ErrorCode = errUnknownTopicOrPartition
			return topic
		}
	}

	topic.ErrorCode = errNone
	if version >= 1 {
		topic.IsInternal = false
	}
	if version >= 10 {
		topic.TopicID = parseTopicID(topicMeta.TopicID)
	}

	// Get partitions
	partitions, err := h.topicStore.ListPartitions(ctx, topicName)
	if err != nil {
		topic.ErrorCode = errUnknownTopicOrPartition
		return topic
	}

	// Build partition metadata
	brokers := h.getBrokers(ctx, zoneID)
	for _, p := range partitions {
		partition := h.buildPartitionMetadata(version, p, brokers)
		topic.Partitions = append(topic.Partitions, partition)
	}

	return topic
}

// buildPartitionMetadata builds metadata for a single partition.
func (h *MetadataHandler) buildPartitionMetadata(version int16, p topics.PartitionMeta, brokers []BrokerInfo) kmsg.MetadataResponseTopicPartition {
	partition := kmsg.NewMetadataResponseTopicPartition()
	partition.Partition = p.Partition

	if p.State != "active" {
		partition.ErrorCode = errLeaderNotAvailable
	} else {
		partition.ErrorCode = errNone
	}

	// Assign leader from available brokers (simple round-robin based on partition)
	if len(brokers) > 0 {
		leaderIdx := int(p.Partition) % len(brokers)
		partition.Leader = brokers[leaderIdx].NodeID

		// Build replica list (all brokers since Dray is leaderless)
		for _, b := range brokers {
			partition.Replicas = append(partition.Replicas, b.NodeID)
		}
		partition.ISR = partition.Replicas // All replicas are in-sync
	} else {
		partition.Leader = -1
	}

	if version >= 5 {
		partition.OfflineReplicas = nil
	}
	if version >= 7 {
		partition.LeaderEpoch = 0
	}

	return partition
}

// parseTopicID parses a topic ID string into a UUID.
func parseTopicID(id string) [16]byte {
	var result [16]byte
	// Simple hex parse - topic IDs are UUIDs
	id = strings.ReplaceAll(id, "-", "")
	if len(id) != 32 {
		return result
	}
	for i := 0; i < 16; i++ {
		b := parseHexByte(id[i*2], id[i*2+1])
		result[i] = b
	}
	return result
}

func parseHexByte(hi, lo byte) byte {
	return (hexVal(hi) << 4) | hexVal(lo)
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 0
	}
}

// ParseZoneID extracts zone_id from a Kafka client.id string.
// The expected format is "zone_id=<zone>,key1=value1,key2=value2".
// Returns empty string if zone_id is not found or invalid.
func ParseZoneID(clientID string) string {
	if clientID == "" {
		return ""
	}

	// Split by comma and look for zone_id
	pairs := strings.Split(clientID, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if strings.HasPrefix(pair, "zone_id=") {
			return strings.TrimPrefix(pair, "zone_id=")
		}
	}
	return ""
}
