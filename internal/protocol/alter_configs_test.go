package protocol

import (
	"context"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIncrementalAlterConfigsHandler_SetConfig(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	// SET retention.ms to a new value
	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	newValue := "86400000"
	configEntry.Value = &newValue
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d: %v", resourceResp.ErrorCode, resourceResp.ErrorMessage)
	}

	// Verify the config was set
	topicMeta, err := topicStore.GetTopic(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topicMeta.Config["retention.ms"] != "86400000" {
		t.Errorf("expected retention.ms='86400000', got '%s'", topicMeta.Config["retention.ms"])
	}
}

func TestIncrementalAlterConfigsHandler_DeleteConfig(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic with custom config
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		Config: map[string]string{
			"retention.ms": "86400000",
		},
		NowMs: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	// DELETE retention.ms
	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpDelete
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d: %v", resourceResp.ErrorCode, resourceResp.ErrorMessage)
	}

	// Verify the config was deleted (reverts to default)
	topicMeta, err := topicStore.GetTopic(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if _, ok := topicMeta.Config["retention.ms"]; ok {
		t.Error("expected retention.ms to be deleted from config")
	}
}

func TestIncrementalAlterConfigsHandler_MultipleConfigs(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	// SET retention.ms
	configEntry1 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry1.Name = "retention.ms"
	configEntry1.Op = kmsg.IncrementalAlterConfigOpSet
	value1 := "86400000"
	configEntry1.Value = &value1
	resource.Configs = append(resource.Configs, configEntry1)

	// SET cleanup.policy
	configEntry2 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry2.Name = "cleanup.policy"
	configEntry2.Op = kmsg.IncrementalAlterConfigOpSet
	value2 := "compact"
	configEntry2.Value = &value2
	resource.Configs = append(resource.Configs, configEntry2)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d: %v", resourceResp.ErrorCode, resourceResp.ErrorMessage)
	}

	// Verify both configs were set
	topicMeta, err := topicStore.GetTopic(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topicMeta.Config["retention.ms"] != "86400000" {
		t.Errorf("expected retention.ms='86400000', got '%s'", topicMeta.Config["retention.ms"])
	}
	if topicMeta.Config["cleanup.policy"] != "compact" {
		t.Errorf("expected cleanup.policy='compact', got '%s'", topicMeta.Config["cleanup.policy"])
	}
}

func TestIncrementalAlterConfigsHandler_InvalidConfigValue(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	// SET retention.ms to invalid value
	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	invalidValue := "not-a-number"
	configEntry.Value = &invalidValue
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST), got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
	if resourceResp.ErrorMessage == nil {
		t.Error("expected error message to be set")
	}
}

func TestIncrementalAlterConfigsHandler_InvalidCleanupPolicy(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	// SET cleanup.policy to invalid value
	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "cleanup.policy"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	invalidValue := "invalid-policy"
	configEntry.Value = &invalidValue
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST), got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_UnknownConfigKey(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	// SET unknown config key
	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "unknown.config.key"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	value := "some-value"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST), got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_UnknownTopic(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "nonexistent-topic"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	value := "86400000"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

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

func TestIncrementalAlterConfigsHandler_BrokerConfigsReadOnly(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeBroker
	resource.ResourceName = "1"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "log.retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	value := "86400000"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST) for read-only broker configs, got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_ValidateOnly(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.ValidateOnly = true

	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	value := "86400000"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error for validate-only, got error code %d", resourceResp.ErrorCode)
	}

	// Verify the config was NOT actually changed
	topicMeta, err := topicStore.GetTopic(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if _, ok := topicMeta.Config["retention.ms"]; ok {
		t.Error("config should not have been changed with ValidateOnly=true")
	}
}

func TestIncrementalAlterConfigsHandler_ValidateOnlyWithInvalidConfig(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.ValidateOnly = true

	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	invalidValue := "invalid"
	configEntry.Value = &invalidValue
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST) for invalid config, got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_UnsupportedResourceType(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = 99 // Unknown resource type
	resource.ResourceName = "something"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "some.config"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	value := "value"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST), got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_AppendNotSupported(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpAppend
	value := "86400000"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST) for APPEND operation, got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_SubtractNotSupported(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSubtract
	value := "86400000"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST) for SUBTRACT operation, got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_SetWithoutValue(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	configEntry.Value = nil // No value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != errInvalidRequest {
		t.Errorf("expected error code %d (INVALID_REQUEST) for SET without value, got %d", errInvalidRequest, resourceResp.ErrorCode)
	}
}

func TestIncrementalAlterConfigsHandler_MultipleResources(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create two topics
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
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic-2: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()

	// Add topic-1
	resource1 := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource1.ResourceType = kmsg.ConfigResourceTypeTopic
	resource1.ResourceName = "topic-1"
	configEntry1 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry1.Name = "retention.ms"
	configEntry1.Op = kmsg.IncrementalAlterConfigOpSet
	value1 := "100000"
	configEntry1.Value = &value1
	resource1.Configs = append(resource1.Configs, configEntry1)
	req.Resources = append(req.Resources, resource1)

	// Add topic-2
	resource2 := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource2.ResourceType = kmsg.ConfigResourceTypeTopic
	resource2.ResourceName = "topic-2"
	configEntry2 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry2.Name = "cleanup.policy"
	configEntry2.Op = kmsg.IncrementalAlterConfigOpSet
	value2 := "compact"
	configEntry2.Value = &value2
	resource2.Configs = append(resource2.Configs, configEntry2)
	req.Resources = append(req.Resources, resource2)

	// Add nonexistent topic
	resource3 := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource3.ResourceType = kmsg.ConfigResourceTypeTopic
	resource3.ResourceName = "nonexistent"
	configEntry3 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry3.Name = "retention.ms"
	configEntry3.Op = kmsg.IncrementalAlterConfigOpSet
	value3 := "100000"
	configEntry3.Value = &value3
	resource3.Configs = append(resource3.Configs, configEntry3)
	req.Resources = append(req.Resources, resource3)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 3 {
		t.Fatalf("expected 3 resource responses, got %d", len(resp.Resources))
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

	// Verify configs were actually set for existing topics
	topic1, _ := topicStore.GetTopic(ctx, "topic-1")
	if topic1.Config["retention.ms"] != "100000" {
		t.Errorf("topic-1 retention.ms not set correctly")
	}

	topic2, _ := topicStore.GetTopic(ctx, "topic-2")
	if topic2.Config["cleanup.policy"] != "compact" {
		t.Errorf("topic-2 cleanup.policy not set correctly")
	}
}

func TestIncrementalAlterConfigsHandler_AtomicUpdate(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic with existing config
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		Config: map[string]string{
			"retention.ms":   "604800000",
			"cleanup.policy": "delete",
		},
		NowMs: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	// SET retention.ms to new value
	configEntry1 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry1.Name = "retention.ms"
	configEntry1.Op = kmsg.IncrementalAlterConfigOpSet
	value1 := "86400000"
	configEntry1.Value = &value1
	resource.Configs = append(resource.Configs, configEntry1)

	// DELETE cleanup.policy (revert to default)
	configEntry2 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry2.Name = "cleanup.policy"
	configEntry2.Op = kmsg.IncrementalAlterConfigOpDelete
	resource.Configs = append(resource.Configs, configEntry2)

	req.Resources = append(req.Resources, resource)

	resp := handler.Handle(ctx, 1, req)

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d: %v", resourceResp.ErrorCode, resourceResp.ErrorMessage)
	}

	// Verify the config was updated atomically
	topicMeta, err := topicStore.GetTopic(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topicMeta.Config["retention.ms"] != "86400000" {
		t.Errorf("expected retention.ms='86400000', got '%s'", topicMeta.Config["retention.ms"])
	}
	if _, ok := topicMeta.Config["cleanup.policy"]; ok {
		t.Error("expected cleanup.policy to be deleted")
	}
}

func TestIncrementalAlterConfigsHandler_V0Response(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          1000,
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewIncrementalAlterConfigsHandler(topicStore)

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = "test-topic"

	configEntry := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	configEntry.Name = "retention.ms"
	configEntry.Op = kmsg.IncrementalAlterConfigOpSet
	value := "86400000"
	configEntry.Value = &value
	resource.Configs = append(resource.Configs, configEntry)

	req.Resources = append(req.Resources, resource)

	// Use version 0
	resp := handler.Handle(ctx, 0, req)

	if resp.GetVersion() != 0 {
		t.Errorf("expected response version 0, got %d", resp.GetVersion())
	}

	if len(resp.Resources) != 1 {
		t.Fatalf("expected 1 resource response, got %d", len(resp.Resources))
	}

	resourceResp := resp.Resources[0]
	if resourceResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resourceResp.ErrorCode)
	}
}
