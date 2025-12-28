package topics

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

func TestStore_CreateTopic(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		Config: map[string]string{
			"retention.ms": "604800000",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if result.Topic.Name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got %s", result.Topic.Name)
	}
	if result.Topic.PartitionCount != 3 {
		t.Errorf("expected 3 partitions, got %d", result.Topic.PartitionCount)
	}
	if result.Topic.TopicID == "" {
		t.Error("expected topic ID to be set")
	}
	if len(result.Partitions) != 3 {
		t.Errorf("expected 3 partition entries, got %d", len(result.Partitions))
	}

	// Verify partition details
	for i, p := range result.Partitions {
		if p.Partition != int32(i) {
			t.Errorf("partition %d: expected partition number %d, got %d", i, i, p.Partition)
		}
		if p.StreamID == "" {
			t.Errorf("partition %d: expected stream ID to be set", i)
		}
		if p.State != "active" {
			t.Errorf("partition %d: expected state 'active', got %s", i, p.State)
		}
	}

	// Verify config was stored
	if result.Topic.Config["retention.ms"] != "604800000" {
		t.Errorf("expected retention.ms config, got %v", result.Topic.Config)
	}
}

func TestStore_CreateTopicDuplicate(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "dup-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}

	_, err = store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "dup-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != ErrTopicExists {
		t.Errorf("expected ErrTopicExists, got %v", err)
	}
}

func TestStore_GetTopic(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "get-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	topic, err := store.GetTopic(ctx, "get-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}

	if topic.Name != "get-topic" {
		t.Errorf("expected name 'get-topic', got %s", topic.Name)
	}
	if topic.PartitionCount != 2 {
		t.Errorf("expected 2 partitions, got %d", topic.PartitionCount)
	}
}

func TestStore_GetTopicNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.GetTopic(ctx, "nonexistent")
	if err != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestStore_GetPartition(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "partition-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	p, err := store.GetPartition(ctx, "partition-topic", 1)
	if err != nil {
		t.Fatalf("failed to get partition: %v", err)
	}

	if p.Partition != 1 {
		t.Errorf("expected partition 1, got %d", p.Partition)
	}
	if p.StreamID != result.Partitions[1].StreamID {
		t.Errorf("stream ID mismatch: expected %s, got %s", result.Partitions[1].StreamID, p.StreamID)
	}
}

func TestStore_GetPartitionNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "partition-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	_, err = store.GetPartition(ctx, "partition-topic", 5)
	if err != ErrPartitionNotFound {
		t.Errorf("expected ErrPartitionNotFound, got %v", err)
	}

	_, err = store.GetPartition(ctx, "nonexistent-topic", 0)
	if err != ErrPartitionNotFound {
		t.Errorf("expected ErrPartitionNotFound for nonexistent topic, got %v", err)
	}
}

func TestStore_ListPartitions(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "list-topic",
		PartitionCount: 5,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	partitions, err := store.ListPartitions(ctx, "list-topic")
	if err != nil {
		t.Fatalf("failed to list partitions: %v", err)
	}

	if len(partitions) != 5 {
		t.Errorf("expected 5 partitions, got %d", len(partitions))
	}

	// Verify all partitions are present
	seen := make(map[int32]bool)
	for _, p := range partitions {
		seen[p.Partition] = true
	}
	for i := int32(0); i < 5; i++ {
		if !seen[i] {
			t.Errorf("missing partition %d", i)
		}
	}
}

func TestStore_ListTopics(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create multiple topics
	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err := store.CreateTopic(ctx, CreateTopicRequest{
			Name:           name,
			PartitionCount: 1,
			NowMs:          time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
	}

	topics, err := store.ListTopics(ctx)
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}

	if len(topics) != 3 {
		t.Errorf("expected 3 topics, got %d", len(topics))
	}

	names := make(map[string]bool)
	for _, topic := range topics {
		names[topic.Name] = true
	}
	for _, expected := range []string{"alpha", "beta", "gamma"} {
		if !names[expected] {
			t.Errorf("missing topic %s", expected)
		}
	}
}

func TestStore_TopicExists(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	exists, err := store.TopicExists(ctx, "exists-topic")
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if exists {
		t.Error("topic should not exist yet")
	}

	_, err = store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "exists-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	exists, err = store.TopicExists(ctx, "exists-topic")
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if !exists {
		t.Error("topic should exist")
	}
}

func TestStore_InvalidInputs(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Empty name
	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != ErrInvalidTopicName {
		t.Errorf("expected ErrInvalidTopicName for empty name, got %v", err)
	}

	// Zero partitions
	_, err = store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "valid-name",
		PartitionCount: 0,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != ErrInvalidPartitions {
		t.Errorf("expected ErrInvalidPartitions for zero partitions, got %v", err)
	}

	// Negative partitions
	_, err = store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "valid-name",
		PartitionCount: -1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != ErrInvalidPartitions {
		t.Errorf("expected ErrInvalidPartitions for negative partitions, got %v", err)
	}
}

func TestStore_DeleteTopic(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create a topic
	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "delete-topic",
		PartitionCount: 3,
		Config: map[string]string{
			"retention.ms": "604800000",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Verify topic exists
	_, err = store.GetTopic(ctx, "delete-topic")
	if err != nil {
		t.Fatalf("topic should exist: %v", err)
	}

	// Delete the topic
	deleteResult, err := store.DeleteTopic(ctx, "delete-topic")
	if err != nil {
		t.Fatalf("failed to delete topic: %v", err)
	}

	// Verify returned data matches created data
	if deleteResult.Topic.Name != "delete-topic" {
		t.Errorf("expected topic name 'delete-topic', got %s", deleteResult.Topic.Name)
	}
	if deleteResult.Topic.TopicID != result.Topic.TopicID {
		t.Errorf("expected topic ID %s, got %s", result.Topic.TopicID, deleteResult.Topic.TopicID)
	}
	if len(deleteResult.Partitions) != 3 {
		t.Errorf("expected 3 partitions returned, got %d", len(deleteResult.Partitions))
	}

	// Verify topic no longer exists
	_, err = store.GetTopic(ctx, "delete-topic")
	if err != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}

	// Verify partitions no longer exist
	partitions, err := store.ListPartitions(ctx, "delete-topic")
	if err != nil {
		t.Fatalf("failed to list partitions: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("expected 0 partitions after delete, got %d", len(partitions))
	}
}

func TestStore_DeleteTopicNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.DeleteTopic(ctx, "nonexistent")
	if err != ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestStore_DeleteTopicEmptyName(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.DeleteTopic(ctx, "")
	if err != ErrInvalidTopicName {
		t.Errorf("expected ErrInvalidTopicName, got %v", err)
	}
}

func TestStore_DeleteTopicThenRecreate(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create topic
	_, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "recreate-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Delete it
	_, err = store.DeleteTopic(ctx, "recreate-topic")
	if err != nil {
		t.Fatalf("failed to delete topic: %v", err)
	}

	// Recreate with different partition count
	result, err := store.CreateTopic(ctx, CreateTopicRequest{
		Name:           "recreate-topic",
		PartitionCount: 5,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to recreate topic: %v", err)
	}

	if result.Topic.PartitionCount != 5 {
		t.Errorf("expected 5 partitions, got %d", result.Topic.PartitionCount)
	}

	// Verify the topic exists with new partition count
	topic, err := store.GetTopic(ctx, "recreate-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topic.PartitionCount != 5 {
		t.Errorf("expected 5 partitions in stored topic, got %d", topic.PartitionCount)
	}
}
