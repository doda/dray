package protocol

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka resource types for DescribeConfigs.
const (
	ResourceTypeUnknown = 0
	ResourceTypeTopic   = 2
	ResourceTypeBroker  = 4
)

// Additional Kafka error codes for DescribeConfigs.
const (
	errInvalidRequest    int16 = 42
	errUnknownServerError int16 = -1
)

// DescribeConfigsHandler handles DescribeConfigs (key 32) requests.
type DescribeConfigsHandler struct {
	topicStore *topics.Store
	brokerID   int32
}

// NewDescribeConfigsHandler creates a new DescribeConfigs handler.
func NewDescribeConfigsHandler(topicStore *topics.Store, brokerID int32) *DescribeConfigsHandler {
	return &DescribeConfigsHandler{
		topicStore: topicStore,
		brokerID:   brokerID,
	}
}

// Handle processes a DescribeConfigs request.
func (h *DescribeConfigsHandler) Handle(ctx context.Context, version int16, req *kmsg.DescribeConfigsRequest) *kmsg.DescribeConfigsResponse {
	resp := kmsg.NewPtrDescribeConfigsResponse()
	resp.SetVersion(version)

	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	for _, resource := range req.Resources {
		resourceResp := h.handleResource(ctx, version, resource, req.IncludeSynonyms, req.IncludeDocumentation)
		resp.Resources = append(resp.Resources, resourceResp)
	}

	return resp
}

// handleResource handles a single resource in the DescribeConfigs request.
func (h *DescribeConfigsHandler) handleResource(ctx context.Context, version int16, resource kmsg.DescribeConfigsRequestResource, includeSynonyms, includeDocumentation bool) kmsg.DescribeConfigsResponseResource {
	resp := kmsg.NewDescribeConfigsResponseResource()
	resp.ResourceType = resource.ResourceType
	resp.ResourceName = resource.ResourceName

	switch resource.ResourceType {
	case ResourceTypeTopic:
		h.describeTopic(ctx, version, resource, &resp, includeSynonyms, includeDocumentation)
	case ResourceTypeBroker:
		h.describeBroker(ctx, version, resource, &resp, includeSynonyms, includeDocumentation)
	default:
		resp.ErrorCode = errInvalidRequest
		errMsg := "Unsupported resource type"
		resp.ErrorMessage = &errMsg
	}

	return resp
}

// describeTopic returns config for a topic resource.
func (h *DescribeConfigsHandler) describeTopic(ctx context.Context, version int16, resource kmsg.DescribeConfigsRequestResource, resp *kmsg.DescribeConfigsResponseResource, includeSynonyms, includeDocumentation bool) {
	// Get the topic metadata to fetch stored config
	topicMeta, err := h.topicStore.GetTopic(ctx, resource.ResourceName)
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			resp.ErrorCode = errUnknownTopicOrPartition
			errMsg := "Topic not found: " + resource.ResourceName
			resp.ErrorMessage = &errMsg
		} else {
			resp.ErrorCode = errUnknownServerError
			errMsg := err.Error()
			resp.ErrorMessage = &errMsg
		}
		return
	}

	// Build map of stored config values
	storedConfig := topicMeta.Config
	if storedConfig == nil {
		storedConfig = make(map[string]string)
	}

	// Get defaults to merge with stored values
	defaults := topics.DefaultConfigs()

	// Determine which config keys to return
	requestedKeys := resource.ConfigNames
	if len(requestedKeys) == 0 {
		// If no specific keys requested, return all supported configs
		requestedKeys = topics.SupportedConfigs()
	}

	for _, key := range requestedKeys {
		config := h.buildTopicConfigEntry(key, storedConfig, defaults, version, includeSynonyms, includeDocumentation)
		resp.Configs = append(resp.Configs, config)
	}
}

// buildTopicConfigEntry builds a config entry for a topic config key.
func (h *DescribeConfigsHandler) buildTopicConfigEntry(key string, storedConfig, defaults map[string]string, version int16, includeSynonyms, includeDocumentation bool) kmsg.DescribeConfigsResponseResourceConfig {
	config := kmsg.NewDescribeConfigsResponseResourceConfig()
	config.Name = key

	// Determine value and source
	var value string
	var source kmsg.ConfigSource = kmsg.ConfigSourceDefaultConfig

	if storedVal, ok := storedConfig[key]; ok {
		value = storedVal
		source = kmsg.ConfigSourceDynamicTopicConfig
	} else if defaultVal, ok := defaults[key]; ok {
		value = defaultVal
		source = kmsg.ConfigSourceDefaultConfig
	}

	config.Value = &value

	// Set read-only flag based on config type
	config.ReadOnly = isReadOnlyConfig(key)

	// Set whether it's sensitive (password-like)
	config.IsSensitive = false

	// Set the source (v1+)
	if version >= 1 {
		config.Source = source
	}

	// Set config type (v3+)
	if version >= 3 {
		config.ConfigType = getConfigType(key)
	}

	// Add synonyms if requested (v1+)
	if includeSynonyms && version >= 1 {
		synonym := kmsg.NewDescribeConfigsResponseResourceConfigConfigSynonym()
		synonym.Name = key
		synonym.Value = &value
		synonym.Source = source
		config.ConfigSynonyms = append(config.ConfigSynonyms, synonym)
	}

	// Add documentation if requested (v3+)
	if includeDocumentation && version >= 3 {
		doc := getConfigDocumentation(key)
		config.Documentation = &doc
	}

	return config
}

// describeBroker returns config for a broker resource.
func (h *DescribeConfigsHandler) describeBroker(ctx context.Context, version int16, resource kmsg.DescribeConfigsRequestResource, resp *kmsg.DescribeConfigsResponseResource, includeSynonyms, includeDocumentation bool) {
	// Dray is a leaderless broker architecture, so broker configs are minimal.
	// We return a basic set of broker configs that make sense for Dray.

	// Determine which config keys to return
	requestedKeys := resource.ConfigNames
	if len(requestedKeys) == 0 {
		// If no specific keys requested, return all broker configs
		requestedKeys = supportedBrokerConfigs()
	}

	for _, key := range requestedKeys {
		config := h.buildBrokerConfigEntry(key, version, includeSynonyms, includeDocumentation)
		resp.Configs = append(resp.Configs, config)
	}
}

// buildBrokerConfigEntry builds a config entry for a broker config key.
func (h *DescribeConfigsHandler) buildBrokerConfigEntry(key string, version int16, includeSynonyms, includeDocumentation bool) kmsg.DescribeConfigsResponseResourceConfig {
	config := kmsg.NewDescribeConfigsResponseResourceConfig()
	config.Name = key

	value := getBrokerConfigValue(key)
	config.Value = &value

	// All broker configs in Dray are read-only since we're stateless
	config.ReadOnly = true
	config.IsSensitive = false
	config.Source = kmsg.ConfigSourceStaticBrokerConfig

	if version >= 3 {
		config.ConfigType = getConfigType(key)
	}

	if includeSynonyms && version >= 1 {
		synonym := kmsg.NewDescribeConfigsResponseResourceConfigConfigSynonym()
		synonym.Name = key
		synonym.Value = &value
		synonym.Source = kmsg.ConfigSourceStaticBrokerConfig
		config.ConfigSynonyms = append(config.ConfigSynonyms, synonym)
	}

	if includeDocumentation && version >= 3 {
		doc := getConfigDocumentation(key)
		config.Documentation = &doc
	}

	return config
}

// isReadOnlyConfig returns true if the config key is immutable.
func isReadOnlyConfig(key string) bool {
	// In Dray, most configs can be modified via AlterConfigs.
	// min.insync.replicas and replication.factor are accepted but ignored,
	// so they could be considered read-only in practice.
	switch key {
	case topics.ConfigMinInsyncReplicas, topics.ConfigReplicationFactor:
		// These are accepted but ignored, so mark as read-only
		return true
	default:
		return false
	}
}

// getConfigType returns the Kafka config type for a config key.
func getConfigType(key string) kmsg.ConfigType {
	switch key {
	case topics.ConfigRetentionMs, topics.ConfigRetentionBytes:
		return kmsg.ConfigTypeLong
	case topics.ConfigCleanupPolicy:
		return kmsg.ConfigTypeString
	case topics.ConfigMinInsyncReplicas, topics.ConfigReplicationFactor:
		return kmsg.ConfigTypeInt
	case "log.segment.bytes", "log.retention.bytes":
		return kmsg.ConfigTypeLong
	case "log.retention.ms":
		return kmsg.ConfigTypeLong
	case "num.partitions":
		return kmsg.ConfigTypeInt
	case "default.replication.factor":
		return kmsg.ConfigTypeInt
	default:
		return kmsg.ConfigTypeString
	}
}

// getConfigDocumentation returns documentation for a config key.
func getConfigDocumentation(key string) string {
	switch key {
	case topics.ConfigRetentionMs:
		return "The maximum time to retain a log before deleting it. If set to -1, no time limit is applied."
	case topics.ConfigRetentionBytes:
		return "The maximum size of the log before deleting it. If set to -1, no size limit is applied."
	case topics.ConfigCleanupPolicy:
		return "The cleanup policy for segments. Valid values are 'delete' and 'compact'."
	case topics.ConfigMinInsyncReplicas:
		return "The minimum number of replicas that must acknowledge a write (accepted but ignored in Dray)."
	case topics.ConfigReplicationFactor:
		return "The replication factor for the topic (accepted but ignored in Dray)."
	case "log.segment.bytes":
		return "The maximum size of a single log segment file."
	case "log.retention.bytes":
		return "The maximum size of the log before deleting old segments."
	case "log.retention.ms":
		return "The maximum time to retain the log before deleting old segments."
	case "num.partitions":
		return "The default number of partitions for auto-created topics."
	case "default.replication.factor":
		return "The default replication factor for auto-created topics."
	default:
		return ""
	}
}

// supportedBrokerConfigs returns the list of broker config keys that Dray supports.
func supportedBrokerConfigs() []string {
	return []string{
		"log.segment.bytes",
		"log.retention.bytes",
		"log.retention.ms",
		"num.partitions",
		"default.replication.factor",
	}
}

// getBrokerConfigValue returns the current value for a broker config key.
func getBrokerConfigValue(key string) string {
	// Dray defaults - these would typically come from the config system
	switch key {
	case "log.segment.bytes":
		return "1073741824" // 1GB
	case "log.retention.bytes":
		return "-1" // unlimited
	case "log.retention.ms":
		return "604800000" // 7 days
	case "num.partitions":
		return "1"
	case "default.replication.factor":
		return "1"
	default:
		return ""
	}
}
