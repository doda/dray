package protocol

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
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
	streamManager := index.NewStreamManager(store)

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
	}, topicStore, streamManager)

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
	streamManager := index.NewStreamManager(store)

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:        "test-cluster",
		ControllerID:     1,
		AutoCreateTopics: false,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	}, topicStore, streamManager)

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
	streamManager := index.NewStreamManager(store)

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
	}, topicStore, streamManager)

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

	partitions, err := topicStore.ListPartitions(ctx, "auto-created-topic")
	if err != nil {
		t.Fatalf("failed to list partitions: %v", err)
	}
	for _, partition := range partitions {
		hwm, _, err := streamManager.GetHWM(ctx, partition.StreamID)
		if err != nil {
			t.Fatalf("expected hwm for stream %s: %v", partition.StreamID, err)
		}
		if hwm != 0 {
			t.Fatalf("expected hwm=0 for stream %s, got %d", partition.StreamID, hwm)
		}
		meta, err := streamManager.GetStreamMeta(ctx, partition.StreamID)
		if err != nil {
			t.Fatalf("expected stream meta for %s: %v", partition.StreamID, err)
		}
		if meta.TopicName != "auto-created-topic" || meta.Partition != partition.Partition {
			t.Fatalf("unexpected stream meta for %s: %+v", partition.StreamID, meta)
		}
	}
}

func TestMetadataHandler_ZoneFiltering(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

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
	}, topicStore, streamManager)

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
	streamManager := index.NewStreamManager(store)

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
	}, topicStore, streamManager)

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
	streamManager := index.NewStreamManager(store)

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
	}, topicStore, streamManager)

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
	streamManager := index.NewStreamManager(store)

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
	}, topicStore, streamManager)

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

func TestMetadataHandler_ZoneFilteredPartitionLeaders(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	// Create a topic with 6 partitions
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "zone-test-topic",
		PartitionCount: 6,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Set up brokers in 3 zones
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "us-east-1a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "us-east-1a"},
			{NodeID: 3, Host: "broker3", Port: 9092, Rack: "us-east-1b"},
			{NodeID: 4, Host: "broker4", Port: 9092, Rack: "us-east-1b"},
			{NodeID: 5, Host: "broker5", Port: 9092, Rack: "us-east-1c"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"us-east-1a": {
				{NodeID: 1, Host: "broker1", Port: 9092, Rack: "us-east-1a"},
				{NodeID: 2, Host: "broker2", Port: 9092, Rack: "us-east-1a"},
			},
			"us-east-1b": {
				{NodeID: 3, Host: "broker3", Port: 9092, Rack: "us-east-1b"},
				{NodeID: 4, Host: "broker4", Port: 9092, Rack: "us-east-1b"},
			},
			"us-east-1c": {
				{NodeID: 5, Host: "broker5", Port: 9092, Rack: "us-east-1c"},
			},
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
	}, topicStore, streamManager)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "zone-test-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	// Test 1: Request with zone us-east-1a - should get only zone-1a brokers
	resp := handler.Handle(ctx, 9, req, "us-east-1a")

	// Verify only us-east-1a brokers are returned
	if len(resp.Brokers) != 2 {
		t.Errorf("expected 2 brokers for zone us-east-1a, got %d", len(resp.Brokers))
	}
	for _, b := range resp.Brokers {
		if *b.Rack != "us-east-1a" {
			t.Errorf("expected rack us-east-1a, got %s", *b.Rack)
		}
	}

	// Verify partition leaders are from same zone
	topic := resp.Topics[0]
	for _, p := range topic.Partitions {
		if p.Leader != 1 && p.Leader != 2 {
			t.Errorf("partition %d leader %d should be from zone us-east-1a (1 or 2)",
				p.Partition, p.Leader)
		}
		// Replicas should also be only same-zone brokers
		for _, r := range p.Replicas {
			if r != 1 && r != 2 {
				t.Errorf("partition %d replica %d should be from zone us-east-1a",
					p.Partition, r)
			}
		}
	}

	// Test 2: Request with zone us-east-1c - should get only the single broker in that zone
	resp = handler.Handle(ctx, 9, req, "us-east-1c")

	if len(resp.Brokers) != 1 {
		t.Errorf("expected 1 broker for zone us-east-1c, got %d", len(resp.Brokers))
	}
	if len(resp.Brokers) > 0 && *resp.Brokers[0].Rack != "us-east-1c" {
		t.Errorf("expected rack us-east-1c, got %s", *resp.Brokers[0].Rack)
	}

	// All partition leaders should be broker 5
	topic = resp.Topics[0]
	for _, p := range topic.Partitions {
		if p.Leader != 5 {
			t.Errorf("partition %d leader %d should be 5 (only broker in zone)",
				p.Partition, p.Leader)
		}
	}
}

func TestMetadataHandler_ZoneFallbackToAllBrokers(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "fallback-test",
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
	}, topicStore, streamManager)

	req := kmsg.NewPtrMetadataRequest()

	// Request with non-existent zone - should fallback to all brokers
	resp := handler.Handle(ctx, 9, req, "nonexistent-zone")

	if len(resp.Brokers) != 2 {
		t.Errorf("expected fallback to 2 brokers, got %d", len(resp.Brokers))
	}

	// Verify brokers from different zones are included
	zones := make(map[string]bool)
	for _, b := range resp.Brokers {
		if b.Rack != nil {
			zones[*b.Rack] = true
		}
	}
	if !zones["us-east-1a"] || !zones["us-east-1b"] {
		t.Errorf("expected brokers from both zones in fallback, got zones: %v", zones)
	}
}

func TestMetadataHandler_EmptyZoneReturnsAllBrokers(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "no-zone-test",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"},
			{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-c"},
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
	}, topicStore, streamManager)

	req := kmsg.NewPtrMetadataRequest()

	// Request with empty zone - should return all brokers
	resp := handler.Handle(ctx, 9, req, "")

	if len(resp.Brokers) != 3 {
		t.Errorf("expected 3 brokers when no zone specified, got %d", len(resp.Brokers))
	}
}

func TestMetadataHandler_ZoneFilteredLeadersDistribution(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	// Create topic with 10 partitions
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "distribution-test",
		PartitionCount: 10,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Set up 2 brokers in the same zone
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-a"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"zone-a": {
				{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
				{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-a"},
			},
		},
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker:  BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092},
		BrokerLister: lister,
	}, topicStore, streamManager)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "distribution-test"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	resp := handler.Handle(ctx, 9, req, "zone-a")

	topic := resp.Topics[0]
	if len(topic.Partitions) != 10 {
		t.Fatalf("expected 10 partitions, got %d", len(topic.Partitions))
	}

	// Count leader distribution
	leaderCounts := make(map[int32]int)
	for _, p := range topic.Partitions {
		leaderCounts[p.Leader]++
	}

	// With 10 partitions and 2 brokers, each should have 5 partitions
	if leaderCounts[1] != 5 || leaderCounts[2] != 5 {
		t.Errorf("expected even leader distribution (5, 5), got broker1=%d, broker2=%d",
			leaderCounts[1], leaderCounts[2])
	}

	// All leaders should be from zone-a (node 1 or 2)
	for _, p := range topic.Partitions {
		if p.Leader != 1 && p.Leader != 2 {
			t.Errorf("partition %d has leader %d not from zone-a", p.Partition, p.Leader)
		}
	}
}

// mockLeaderSelector implements PartitionLeaderSelector for testing.
type mockLeaderSelector struct {
	leaders map[string]int32 // key: "topic-partition"
}

func (m *mockLeaderSelector) GetPartitionLeader(_ context.Context, zoneID, topic string, partition int32) (int32, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)
	if leader, ok := m.leaders[key]; ok {
		return leader, nil
	}
	return -1, nil
}

func TestMetadataHandler_WithLeaderSelector(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	// Create a topic with 3 partitions
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "selector-test",
		PartitionCount: 3,
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

	// Create a custom leader selector that assigns specific leaders
	selector := &mockLeaderSelector{
		leaders: map[string]int32{
			"selector-test-0": 3, // partition 0 -> broker 3
			"selector-test-1": 1, // partition 1 -> broker 1
			"selector-test-2": 2, // partition 2 -> broker 2
		},
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:      "test-cluster",
		ControllerID:   1,
		LocalBroker:    BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092},
		BrokerLister:   lister,
		LeaderSelector: selector,
	}, topicStore, streamManager)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "selector-test"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	resp := handler.Handle(ctx, 9, req, "")

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}

	topic := resp.Topics[0]
	if len(topic.Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(topic.Partitions))
	}

	// Verify leaders match what the selector returned
	expectedLeaders := map[int32]int32{
		0: 3,
		1: 1,
		2: 2,
	}

	for _, p := range topic.Partitions {
		expected := expectedLeaders[p.Partition]
		if p.Leader != expected {
			t.Errorf("partition %d: expected leader %d, got %d", p.Partition, expected, p.Leader)
		}
	}
}

func TestMetadataHandler_LeaderSelectorDeterministic(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "deterministic-test",
		PartitionCount: 5,
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

	// Use a deterministic selector (same key always returns same result)
	selector := &mockLeaderSelector{
		leaders: map[string]int32{
			"deterministic-test-0": 1,
			"deterministic-test-1": 2,
			"deterministic-test-2": 3,
			"deterministic-test-3": 1,
			"deterministic-test-4": 2,
		},
	}

	handler := NewMetadataHandler(MetadataHandlerConfig{
		ClusterID:      "test-cluster",
		ControllerID:   1,
		LocalBroker:    BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092},
		BrokerLister:   lister,
		LeaderSelector: selector,
	}, topicStore, streamManager)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "deterministic-test"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	// Make two requests and verify leaders are identical
	resp1 := handler.Handle(ctx, 9, req, "")
	resp2 := handler.Handle(ctx, 9, req, "")

	topic1 := resp1.Topics[0]
	topic2 := resp2.Topics[0]

	for i := 0; i < len(topic1.Partitions); i++ {
		if topic1.Partitions[i].Leader != topic2.Partitions[i].Leader {
			t.Errorf("partition %d: leaders differ between requests (%d vs %d)",
				i, topic1.Partitions[i].Leader, topic2.Partitions[i].Leader)
		}
	}
}
