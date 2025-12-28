package protocol

import (
	"context"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDescribeConfigsHandler_TopicConfigs(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic with custom config
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		Config: map[string]string{
			"retention.ms":    "86400000",
			"cleanup.policy":  "compact",
		},
		NowMs: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "test-topic"
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d: %v", resourceResp.ErrorCode, resourceResp.ErrorMessage)
	}
	if resourceResp.ResourceType != ResourceTypeTopic {
		t.Errorf("expected resource type %d, got %d", ResourceTypeTopic, resourceResp.ResourceType)
	}
	if resourceResp.ResourceName != "test-topic" {
		t.Errorf("expected resource name 'test-topic', got '%s'", resourceResp.ResourceName)
	}

	// Should return all supported configs
	if len(resourceResp.Configs) != len(topics.SupportedConfigs()) {
		t.Errorf("expected %d configs, got %d", len(topics.SupportedConfigs()), len(resourceResp.Configs))
	}

	// Verify specific config values
	configMap := make(map[string]kmsg.DescribeConfigsResponseResourceConfig)
	for _, c := range resourceResp.Configs {
		configMap[c.Name] = c
	}

	// Check retention.ms has custom value
	if cfg, ok := configMap["retention.ms"]; ok {
		if *cfg.Value != "86400000" {
			t.Errorf("expected retention.ms='86400000', got '%s'", *cfg.Value)
		}
		if cfg.Source != kmsg.ConfigSourceDynamicTopicConfig {
			t.Errorf("expected source %d for custom config, got %d", kmsg.ConfigSourceDynamicTopicConfig, cfg.Source)
		}
	} else {
		t.Error("retention.ms not found in response")
	}

	// Check cleanup.policy has custom value
	if cfg, ok := configMap["cleanup.policy"]; ok {
		if *cfg.Value != "compact" {
			t.Errorf("expected cleanup.policy='compact', got '%s'", *cfg.Value)
		}
	} else {
		t.Error("cleanup.policy not found in response")
	}

	// Check retention.bytes has default value
	if cfg, ok := configMap["retention.bytes"]; ok {
		if *cfg.Value != "-1" {
			t.Errorf("expected retention.bytes='-1' (default), got '%s'", *cfg.Value)
		}
		if cfg.Source != kmsg.ConfigSourceDefaultConfig {
			t.Errorf("expected source %d for default config, got %d", kmsg.ConfigSourceDefaultConfig, cfg.Source)
		}
	} else {
		t.Error("retention.bytes not found in response")
	}
}

func TestDescribeConfigsHandler_SpecificConfigKeys(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "topic-with-configs",
		PartitionCount: 1,
		Config: map[string]string{
			"retention.ms": "172800000",
		},
		NowMs: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "topic-with-configs"
	resource.ConfigNames = []string{"retention.ms", "cleanup.policy"}
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resourceResp.ErrorCode)
	}

	// Should only return the requested configs
	if len(resourceResp.Configs) != 2 {
		t.Errorf("expected 2 configs, got %d", len(resourceResp.Configs))
	}

	configNames := make(map[string]bool)
	for _, c := range resourceResp.Configs {
		configNames[c.Name] = true
	}

	if !configNames["retention.ms"] {
		t.Error("retention.ms not found in response")
	}
	if !configNames["cleanup.policy"] {
		t.Error("cleanup.policy not found in response")
	}
}

func TestDescribeConfigsHandler_UnknownTopic(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "nonexistent-topic"
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errUnknownTopicOrPartition {
		t.Errorf("expected error code %d (UNKNOWN_TOPIC_OR_PARTITION), got %d", errUnknownTopicOrPartition, resourceResp.ErrorCode)
	}
	if resourceResp.ErrorMessage == nil {
		t.Error("expected error message to be set")
	}
}

func TestDescribeConfigsHandler_BrokerConfigs(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeBroker
	resource.ResourceName = "1"
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d: %v", resourceResp.ErrorCode, resourceResp.ErrorMessage)
	}
	if resourceResp.ResourceType != ResourceTypeBroker {
		t.Errorf("expected resource type %d, got %d", ResourceTypeBroker, resourceResp.ResourceType)
	}

	// Should return all supported broker configs
	if len(resourceResp.Configs) != len(supportedBrokerConfigs()) {
		t.Errorf("expected %d configs, got %d", len(supportedBrokerConfigs()), len(resourceResp.Configs))
	}

	// Verify all broker configs are read-only
	for _, cfg := range resourceResp.Configs {
		if !cfg.ReadOnly {
			t.Errorf("expected broker config '%s' to be read-only", cfg.Name)
		}
	}
}

func TestDescribeConfigsHandler_ReadOnlyFlag(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "readonly-test",
		PartitionCount: 1,
		Config: map[string]string{
			"min.insync.replicas": "2",
			"replication.factor":  "3",
		},
		NowMs: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "readonly-test"
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resourceResp.ErrorCode)
	}

	configMap := make(map[string]kmsg.DescribeConfigsResponseResourceConfig)
	for _, c := range resourceResp.Configs {
		configMap[c.Name] = c
	}

	// min.insync.replicas should be read-only (accepted but ignored)
	if cfg, ok := configMap["min.insync.replicas"]; ok {
		if !cfg.ReadOnly {
			t.Error("expected min.insync.replicas to be read-only")
		}
	}

	// replication.factor should be read-only (accepted but ignored)
	if cfg, ok := configMap["replication.factor"]; ok {
		if !cfg.ReadOnly {
			t.Error("expected replication.factor to be read-only")
		}
	}

	// retention.ms should NOT be read-only
	if cfg, ok := configMap["retention.ms"]; ok {
		if cfg.ReadOnly {
			t.Error("expected retention.ms to NOT be read-only")
		}
	}
}

func TestDescribeConfigsHandler_MultipleResources(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "topic-1",
		PartitionCount: 1,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic-1: %v", err)
	}

	_, err = topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "topic-2",
		PartitionCount: 1,
		Config:         map[string]string{"retention.ms": "1000000"},
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic-2: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()

	// Add topic-1
	resource1 := kmsg.NewDescribeConfigsRequestResource()
	resource1.ResourceType = ResourceTypeTopic
	resource1.ResourceName = "topic-1"
	req.Resources = append(req.Resources, resource1)

	// Add topic-2
	resource2 := kmsg.NewDescribeConfigsRequestResource()
	resource2.ResourceType = ResourceTypeTopic
	resource2.ResourceName = "topic-2"
	req.Resources = append(req.Resources, resource2)

	// Add nonexistent topic
	resource3 := kmsg.NewDescribeConfigsRequestResource()
	resource3.ResourceType = ResourceTypeTopic
	resource3.ResourceName = "nonexistent"
	req.Resources = append(req.Resources, resource3)

	// Add broker
	resource4 := kmsg.NewDescribeConfigsRequestResource()
	resource4.ResourceType = ResourceTypeBroker
	resource4.ResourceName = "1"
	req.Resources = append(req.Resources, resource4)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 4 {
		t.Fatalf("expected 4 resource responses, got %d", len(resp.Resources))
	}

	// topic-1 should succeed
	if resp.Resources[0].ErrorCode != 0 {
		t.Errorf("topic-1 should succeed, got error %d", resp.Resources[0].ErrorCode)
	}

	// topic-2 should succeed
	if resp.Resources[1].ErrorCode != 0 {
		t.Errorf("topic-2 should succeed, got error %d", resp.Resources[1].ErrorCode)
	}

	// nonexistent topic should fail
	if resp.Resources[2].ErrorCode != errUnknownTopicOrPartition {
		t.Errorf("nonexistent topic should fail with UNKNOWN_TOPIC_OR_PARTITION, got %d", resp.Resources[2].ErrorCode)
	}

	// broker should succeed
	if resp.Resources[3].ErrorCode != 0 {
		t.Errorf("broker should succeed, got error %d", resp.Resources[3].ErrorCode)
	}
}

func TestDescribeConfigsHandler_UnsupportedResourceType(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = 99 // Unknown resource type
	resource.ResourceName = "something"
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST), got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestDescribeConfigsHandler_IncludeSynonyms(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "synonym-test",
		PartitionCount: 1,
		Config: map[string]string{
			"retention.ms": "86400000",
		},
		NowMs: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	req.IncludeSynonyms = true
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "synonym-test"
	resource.ConfigNames = []string{"retention.ms"}
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resourceResp.ErrorCode)
	}

	if len(resourceResp.Configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(resourceResp.Configs))
	}

	cfg := resourceResp.Configs[0]
	if len(cfg.ConfigSynonyms) == 0 {
		t.Error("expected config synonyms to be populated")
	} else {
		syn := cfg.ConfigSynonyms[0]
		if syn.Name != "retention.ms" {
			t.Errorf("expected synonym name 'retention.ms', got '%s'", syn.Name)
		}
		if *syn.Value != "86400000" {
			t.Errorf("expected synonym value '86400000', got '%s'", *syn.Value)
		}
	}
}

func TestDescribeConfigsHandler_IncludeDocumentation(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "doc-test",
		PartitionCount: 1,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	req.IncludeDocumentation = true
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "doc-test"
	resource.ConfigNames = []string{"retention.ms"}
	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resourceResp.ErrorCode)
	}

	if len(resourceResp.Configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(resourceResp.Configs))
	}

	cfg := resourceResp.Configs[0]
	if cfg.Documentation == nil || *cfg.Documentation == "" {
		t.Error("expected documentation to be populated for v4")
	}
}

func TestDescribeConfigsHandler_ConfigTypes(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "type-test",
		PartitionCount: 1,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "type-test"
	req.Resources = append(req.Resources, resource)

	// Use version 4 which includes config types
	resp := handler.Handle(ctx, 4, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	configMap := make(map[string]kmsg.DescribeConfigsResponseResourceConfig)
	for _, c := range resourceResp.Configs {
		configMap[c.Name] = c
	}

	// retention.ms should be LONG type
	if cfg, ok := configMap["retention.ms"]; ok {
		if cfg.ConfigType != kmsg.ConfigTypeLong {
			t.Errorf("expected retention.ms to be LONG type (%d), got %d", kmsg.ConfigTypeLong, cfg.ConfigType)
		}
	}

	// cleanup.policy should be STRING type
	if cfg, ok := configMap["cleanup.policy"]; ok {
		if cfg.ConfigType != kmsg.ConfigTypeString {
			t.Errorf("expected cleanup.policy to be STRING type (%d), got %d", kmsg.ConfigTypeString, cfg.ConfigType)
		}
	}

	// min.insync.replicas should be INT type
	if cfg, ok := configMap["min.insync.replicas"]; ok {
		if cfg.ConfigType != kmsg.ConfigTypeInt {
			t.Errorf("expected min.insync.replicas to be INT type (%d), got %d", kmsg.ConfigTypeInt, cfg.ConfigType)
		}
	}
}

func TestDescribeConfigsHandler_V0Response(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "v0-test",
		PartitionCount: 1,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDescribeConfigsHandler(topicStore, 1)

	req := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType = ResourceTypeTopic
	resource.ResourceName = "v0-test"
	resource.ConfigNames = []string{"retention.ms"}
	req.Resources = append(req.Resources, resource)

	// Use version 0
	resp := handler.Handle(ctx, 0, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resourceResp.ErrorCode)
	}

	if len(resourceResp.Configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(resourceResp.Configs))
	}

	// v0 should not have synonyms or config type
	cfg := resourceResp.Configs[0]
	if len(cfg.ConfigSynonyms) != 0 {
		t.Errorf("expected no synonyms for v0, got %d", len(cfg.ConfigSynonyms))
	}
	// ConfigType is 0 (unknown) for v0
	if cfg.ConfigType != kmsg.ConfigTypeUnknown {
		t.Errorf("expected ConfigType=UNKNOWN for v0, got %d", cfg.ConfigType)
	}
}
