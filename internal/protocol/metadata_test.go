package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// mockBrokerLister implements BrokerLister for testing.
type mockBrokerLister struct {
	brokers      []BrokerInfo
	zoneBrokers  map[string][]BrokerInfo
}

func (m *mockBrokerLister) ListBrokers(_ context.Context, zoneID string) ([]BrokerInfo, error) {
	if zoneID != "" && m.zoneBrokers != nil {
		if brokers, ok := m.zoneBrokers[zoneID]; ok {
			return brokers, nil
		}
		return nil, nil // Empty list for unknown zone
	}
	return m.brokers, nil
}

func TestMetadataHandler_BasicMetadata(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a test topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	}, topicStore)

	// Request metadata for specific topic
	req := kmsg.NewPtrMetadataRequest()
	topicName := "test-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	resp := handler.Handle(ctx, 9, req, "")

	// Verify brokers
	if len(resp.Brokers) != 1 {
		t.Errorf("expected 1 broker, got %d", len(resp.Brokers))
	}
	if resp.Brokers[0].NodeID != 1 {
		t.Errorf("expected broker NodeID 1, got %d", resp.Brokers[0].NodeID)
	}

	// Verify cluster metadata
	if resp.ControllerID != 1 {
		t.Errorf("expected controller ID 1, got %d", resp.ControllerID)
	}
	if resp.ClusterID == nil || *resp.ClusterID != "test-cluster" {
		t.Errorf("expected cluster ID 'test-cluster', got %v", resp.ClusterID)
	}

	// Verify topic metadata
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	topic := resp.Topics[0]
	if topic.Topic == nil || *topic.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %v", topic.Topic)
	}
	if topic.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", topic.ErrorCode)
	}
	if len(topic.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(topic.Partitions))
	}
}

func TestMetadataHandler_TopicNotFound(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:        "test-cluster",
		ControllerID:     1,
		AutoCreateTopics: false,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	}, topicStore)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "nonexistent-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	resp := handler.Handle(ctx, 9, req, "")

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != errUnknownTopicOrPartition {
		t.Errorf("expected UNKNOWN_TOPIC_OR_PARTITION error (3), got %d", resp.Topics[0].ErrorCode)
	}
}

func TestMetadataHandler_AutoCreateTopic(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:         "test-cluster",
		ControllerID:      1,
		AutoCreateTopics:  true,
		DefaultPartitions: 4,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	}, topicStore)

	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = true
	topicName := "auto-created-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	resp := handler.Handle(ctx, 9, req, "")

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	topic := resp.Topics[0]
	if topic.ErrorCode != 0 {
		t.Errorf("expected no error after auto-creation, got %d", topic.ErrorCode)
	}
	if len(topic.Partitions) != 4 {
		t.Errorf("expected 4 partitions from auto-creation, got %d", len(topic.Partitions))
	}

	// Verify topic was actually created
	exists, err := topicStore.TopicExists(ctx, "auto-created-topic")
	if err != nil {
		t.Fatalf("failed to check topic existence: %v", err)
	}
	if !exists {
		t.Error("topic should exist after auto-creation")
	}
}

func TestMetadataHandler_ZoneFiltering(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a test topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "us-east-1a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "us-east-1b"},
			{NodeID: 3, Host: "broker3", Port: 9092, Rack: "us-east-1c"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"us-east-1a": {{NodeID: 1, Host: "broker1", Port: 9092, Rack: "us-east-1a"}},
			"us-east-1b": {{NodeID: 2, Host: "broker2", Port: 9092, Rack: "us-east-1b"}},
		},
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
		BrokerLister: lister,
	}, topicStore)

	// Test with zone filtering
	req := kmsg.NewPtrMetadataRequest()
	resp := handler.Handle(ctx, 9, req, "us-east-1a")

	if len(resp.Brokers) != 1 {
		t.Errorf("expected 1 broker for zone us-east-1a, got %d", len(resp.Brokers))
	}
	if resp.Brokers[0].NodeID != 1 {
		t.Errorf("expected broker 1 for zone us-east-1a, got %d", resp.Brokers[0].NodeID)
	}

	// Test without zone filtering
	resp = handler.Handle(ctx, 9, req, "")
	if len(resp.Brokers) != 3 {
		t.Errorf("expected 3 brokers without zone filter, got %d", len(resp.Brokers))
	}

	// Test with unknown zone (should fallback to all brokers)
	resp = handler.Handle(ctx, 9, req, "unknown-zone")
	if len(resp.Brokers) != 3 {
		t.Errorf("expected 3 brokers for unknown zone fallback, got %d", len(resp.Brokers))
	}
}

func TestMetadataHandler_ListAllTopics(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create multiple topics
	for _, name := range []string{"topic-a", "topic-b", "topic-c"} {
		_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
			Name:           name,
			PartitionCount: 1,
			NowMs:          time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	}, topicStore)

	// Request all topics (empty Topics array)
	req := kmsg.NewPtrMetadataRequest()
	resp := handler.Handle(ctx, 9, req, "")

	if len(resp.Topics) != 3 {
		t.Errorf("expected 3 topics, got %d", len(resp.Topics))
	}

	topicNames := make(map[string]bool)
	for _, topic := range resp.Topics {
		if topic.Topic != nil {
			topicNames[*topic.Topic] = true
		}
	}
	for _, expected := range []string{"topic-a", "topic-b", "topic-c"} {
		if !topicNames[expected] {
			t.Errorf("expected topic %s in response", expected)
		}
	}
}

func TestParseZoneID(t *testing.T) {
	tests := []struct {
		clientID string
		expected string
	}{
		{"zone_id=us-east-1a", "us-east-1a"},
		{"zone_id=us-east-1a,app=myapp", "us-east-1a"},
		{"app=myapp,zone_id=us-east-1b,env=prod", "us-east-1b"},
		{"app=myapp", ""},
		{"", ""},
		{"zone_id=", ""},
		{"zone_id=us-west-2a, key=value", "us-west-2a"},
		{" zone_id=trimmed ", "trimmed"},
	}

	for _, tt := range tests {
		t.Run(tt.clientID, func(t *testing.T) {
			got := ParseZoneID(tt.clientID)
			if got != tt.expected {
				t.Errorf("ParseZoneID(%q) = %q, want %q", tt.clientID, got, tt.expected)
			}
		})
	}
}

func TestMetadataHandler_PartitionLeaderAssignment(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	// Create a topic with multiple partitions
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "multi-partition",
		PartitionCount: 6,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092},
			{NodeID: 2, Host: "broker2", Port: 9092},
			{NodeID: 3, Host: "broker3", Port: 9092},
		},
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker:  BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092},
		BrokerLister: lister,
	}, topicStore)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "multi-partition"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	resp := handler.Handle(ctx, 9, req, "")

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}

	topic := resp.Topics[0]
	if len(topic.Partitions) != 6 {
		t.Fatalf("expected 6 partitions, got %d", len(topic.Partitions))
	}

	// Verify leaders are distributed across brokers
	leaderCounts := make(map[int32]int)
	for _, p := range topic.Partitions {
		leaderCounts[p.Leader]++
		// Verify all brokers are in replica set
		if len(p.Replicas) != 3 {
			t.Errorf("partition %d: expected 3 replicas, got %d", p.Partition, len(p.Replicas))
		}
		// Verify ISR matches replicas
		if len(p.ISR) != len(p.Replicas) {
			t.Errorf("partition %d: ISR count %d doesn't match replica count %d",
				p.Partition, len(p.ISR), len(p.Replicas))
		}
	}

	// Each broker should be leader for 2 partitions
	for nodeID, count := range leaderCounts {
		if count != 2 {
			t.Errorf("broker %d is leader for %d partitions, expected 2", nodeID, count)
		}
	}
}

func TestMetadataHandler_VersionedFields(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
			Rack:   "rack1",
		},
	}, topicStore)

	req := kmsg.NewPtrMetadataRequest()

	// Version 0 - no cluster ID, no controller, no rack
	resp := handler.Handle(ctx, 0, req, "")
	if resp.ClusterID != nil {
		t.Error("v0 should not have cluster ID")
	}

	// Version 2 - should have cluster ID
	resp = handler.Handle(ctx, 2, req, "")
	if resp.ClusterID == nil || *resp.ClusterID != "test-cluster" {
		t.Error("v2 should have cluster ID")
	}

	// Version 1+ - should have rack
	resp = handler.Handle(ctx, 1, req, "")
	if len(resp.Brokers) > 0 && resp.Brokers[0].Rack == nil {
		t.Error("v1+ should have rack")
	}
}
