package protocol

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// IncrementalAlterConfigsHandler handles IncrementalAlterConfigs (key 44) requests.
type IncrementalAlterConfigsHandler struct {
	topicStore *topics.Store
}

// NewIncrementalAlterConfigsHandler creates a new IncrementalAlterConfigs handler.
func NewIncrementalAlterConfigsHandler(topicStore *topics.Store) *IncrementalAlterConfigsHandler {
	return &IncrementalAlterConfigsHandler{
		topicStore: topicStore,
	}
}

// Handle processes an IncrementalAlterConfigs request.
func (h *IncrementalAlterConfigsHandler) Handle(ctx context.Context, version int16, req *kmsg.IncrementalAlterConfigsRequest) *kmsg.IncrementalAlterConfigsResponse {
	resp := kmsg.NewPtrIncrementalAlterConfigsResponse()
	resp.SetVersion(version)
	resp.ThrottleMillis = 0

	for _, resource := range req.Resources {
		resourceResp := h.handleResource(ctx, resource, req.ValidateOnly)
		resp.Resources = append(resp.Resources, resourceResp)
	}

	return resp
}

// handleResource handles a single resource in the IncrementalAlterConfigs request.
func (h *IncrementalAlterConfigsHandler) handleResource(ctx context.Context, resource kmsg.IncrementalAlterConfigsRequestResource, validateOnly bool) kmsg.IncrementalAlterConfigsResponseResource {
	resp := kmsg.NewIncrementalAlterConfigsResponseResource()
	resp.ResourceType = resource.ResourceType
	resp.ResourceName = resource.ResourceName

	switch resource.ResourceType {
	case kmsg.ConfigResourceTypeTopic:
		h.alterTopicConfigs(ctx, resource, &resp, validateOnly)
	case kmsg.ConfigResourceTypeBroker:
		h.alterBrokerConfigs(ctx, resource, &resp, validateOnly)
	default:
		resp.ErrorCode = errInvalidRequest
		errMsg := "Unsupported resource type"
		resp.ErrorMessage = &errMsg
	}

	return resp
}

// alterTopicConfigs handles IncrementalAlterConfigs for a topic resource.
func (h *IncrementalAlterConfigsHandler) alterTopicConfigs(ctx context.Context, resource kmsg.IncrementalAlterConfigsRequestResource, resp *kmsg.IncrementalAlterConfigsResponseResource, validateOnly bool) {
	topicName := resource.ResourceName

	// First, get the existing topic to verify it exists and get current config
	topicMeta, err := h.topicStore.GetTopic(ctx, topicName)
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			resp.ErrorCode = errUnknownTopicOrPartition
			errMsg := "Topic not found: " + topicName
			resp.ErrorMessage = &errMsg
		} else {
			resp.ErrorCode = errUnknownServerError
			errMsg := err.Error()
			resp.ErrorMessage = &errMsg
		}
		return
	}

	// Get current config
	currentConfig := topicMeta.Config
	if currentConfig == nil {
		currentConfig = make(map[string]string)
	}

	// Create a copy of the current config to apply changes
	newConfig := make(map[string]string, len(currentConfig))
	for k, v := range currentConfig {
		newConfig[k] = v
	}

	// Apply each config operation
	for _, cfg := range resource.Configs {
		if err := h.applyConfigOp(cfg, newConfig); err != nil {
			resp.ErrorCode = errInvalidRequest
			errMsg := err.Error()
			resp.ErrorMessage = &errMsg
			return
		}
	}

	// Validate the resulting config
	if err := topics.ValidateConfigs(newConfig); err != nil {
		resp.ErrorCode = errInvalidRequest
		errMsg := err.Error()
		resp.ErrorMessage = &errMsg
		return
	}

	// If validate only, don't apply changes
	if validateOnly {
		return
	}

	// Apply the new config atomically
	err = h.topicStore.UpdateTopicConfig(ctx, topics.UpdateTopicConfigRequest{
		TopicName: topicName,
		Configs:   newConfig,
	})
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			resp.ErrorCode = errUnknownTopicOrPartition
			errMsg := "Topic not found: " + topicName
			resp.ErrorMessage = &errMsg
		} else {
			resp.ErrorCode = errUnknownServerError
			errMsg := err.Error()
			resp.ErrorMessage = &errMsg
		}
		return
	}
}

// alterBrokerConfigs handles IncrementalAlterConfigs for a broker resource.
// Dray is a leaderless broker architecture, so broker configs are read-only.
func (h *IncrementalAlterConfigsHandler) alterBrokerConfigs(_ context.Context, _ kmsg.IncrementalAlterConfigsRequestResource, resp *kmsg.IncrementalAlterConfigsResponseResource, _ bool) {
	// Dray broker configs are read-only since we're stateless
	resp.ErrorCode = errInvalidRequest
	errMsg := "Broker configs are read-only in Dray"
	resp.ErrorMessage = &errMsg
}

// applyConfigOp applies a single config operation to the config map.
func (h *IncrementalAlterConfigsHandler) applyConfigOp(cfg kmsg.IncrementalAlterConfigsRequestResourceConfig, config map[string]string) error {
	configName := cfg.Name

	// Check if the config key is supported
	if !topics.IsSupportedConfig(configName) {
		return &topics.ConfigValidationError{
			Key:     configName,
			Value:   "",
			Message: "unknown config key",
		}
	}

	switch cfg.Op {
	case kmsg.IncrementalAlterConfigOpSet:
		if cfg.Value == nil {
			return &topics.ConfigValidationError{
				Key:     configName,
				Value:   "",
				Message: "SET operation requires a value",
			}
		}
		// Validate the new value
		if err := topics.ValidateConfig(configName, *cfg.Value); err != nil {
			return err
		}
		config[configName] = *cfg.Value

	case kmsg.IncrementalAlterConfigOpDelete:
		// Delete removes the config, reverting to default
		delete(config, configName)

	case kmsg.IncrementalAlterConfigOpAppend:
		// APPEND is for list-type configs. Dray's supported configs are not lists,
		// so we reject this operation.
		return &topics.ConfigValidationError{
			Key:     configName,
			Value:   "",
			Message: "APPEND operation is not supported for this config",
		}

	case kmsg.IncrementalAlterConfigOpSubtract:
		// SUBTRACT is for list-type configs. Dray's supported configs are not lists,
		// so we reject this operation.
		return &topics.ConfigValidationError{
			Key:     configName,
			Value:   "",
			Message: "SUBTRACT operation is not supported for this config",
		}

	default:
		return &topics.ConfigValidationError{
			Key:     configName,
			Value:   "",
			Message: "unknown config operation",
		}
	}

	return nil
}
