package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/routing"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestZoneFiltering_MetadataReturnsOnlySameZoneBrokers verifies that when a client
// connects with a zone_id, the Metadata response only contains brokers from that zone.
func TestZoneFiltering_MetadataReturnsOnlySameZoneBrokers(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)

	// Step 1: Register brokers in multiple zones
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1a", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1b", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "us-east-1b", AdvertisedListeners: []string{"broker4:9092"}},
		{BrokerID: "broker-5", NodeID: 5, ZoneID: "us-east-1c", AdvertisedListeners: []string{"broker5:9092"}},
	}

	clusterID := "test-cluster"
	for _, broker := range brokers {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatalf("failed to marshal broker info: %v", err)
		}
		key := keys.BrokerKeyPath(clusterID, broker.BrokerID)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to register broker %s: %v", broker.BrokerID, err)
		}
	}

	// Create a test topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create registry and adapter for the metadata handler
	registry := routing.NewRegistry(metaStore, routing.RegistryConfig{
		ClusterID: clusterID,
		BrokerID:  "local-broker",
		NodeID:    0,
		ZoneID:    "us-east-1a",
	})
	adapter := routing.NewAffinityListerAdapter(registry)

	localBroker := protocol.BrokerInfo{NodeID: 1, Host: "broker1", Port: 9092, Rack: "us-east-1a"}
	metadataHandler := protocol.NewMetadataHandler(protocol.MetadataHandlerConfig{
		ClusterID:      clusterID,
		ControllerID:   1,
		LocalBroker:    localBroker,
		BrokerLister:   adapter,
		LeaderSelector: adapter,
	}, topicStore)

	// Step 2 & 3: Request with zone_id=us-east-1a - should return only zone-1a brokers
	req := kmsg.NewPtrMetadataRequest()
	topicName := "test-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	t.Run("client in zone us-east-1a gets only us-east-1a brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "us-east-1a")

		// Verify only us-east-1a brokers are returned
		if len(resp.Brokers) != 2 {
			t.Errorf("expected 2 brokers for zone us-east-1a, got %d", len(resp.Brokers))
		}
		for _, b := range resp.Brokers {
			if b.NodeID != 1 && b.NodeID != 2 {
				t.Errorf("expected broker from us-east-1a (NodeID 1 or 2), got %d", b.NodeID)
			}
			if b.Rack == nil || *b.Rack != "us-east-1a" {
				t.Errorf("expected rack us-east-1a, got %v", b.Rack)
			}
		}

		// Verify partition leaders are from same zone
		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
		}
		topic := resp.Topics[0]
		for _, p := range topic.Partitions {
			if p.Leader != 1 && p.Leader != 2 {
				t.Errorf("partition %d leader %d should be from us-east-1a (1 or 2)", p.Partition, p.Leader)
			}
			for _, r := range p.Replicas {
				if r != 1 && r != 2 {
					t.Errorf("partition %d replica %d should be from us-east-1a", p.Partition, r)
				}
			}
		}
	})

	t.Run("client in zone us-east-1b gets only us-east-1b brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "us-east-1b")

		// Verify only us-east-1b brokers are returned
		if len(resp.Brokers) != 2 {
			t.Errorf("expected 2 brokers for zone us-east-1b, got %d", len(resp.Brokers))
		}
		for _, b := range resp.Brokers {
			if b.NodeID != 3 && b.NodeID != 4 {
				t.Errorf("expected broker from us-east-1b (NodeID 3 or 4), got %d", b.NodeID)
			}
			if b.Rack == nil || *b.Rack != "us-east-1b" {
				t.Errorf("expected rack us-east-1b, got %v", b.Rack)
			}
		}

		// Verify partition leaders are from same zone
		topic := resp.Topics[0]
		for _, p := range topic.Partitions {
			if p.Leader != 3 && p.Leader != 4 {
				t.Errorf("partition %d leader %d should be from us-east-1b (3 or 4)", p.Partition, p.Leader)
			}
		}
	})

	t.Run("client in zone us-east-1c gets only us-east-1c broker", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "us-east-1c")

		// Verify only us-east-1c broker is returned (just one broker in this zone)
		if len(resp.Brokers) != 1 {
			t.Errorf("expected 1 broker for zone us-east-1c, got %d", len(resp.Brokers))
		}
		if len(resp.Brokers) > 0 {
			if resp.Brokers[0].NodeID != 5 {
				t.Errorf("expected broker 5 for zone us-east-1c, got %d", resp.Brokers[0].NodeID)
			}
			if resp.Brokers[0].Rack == nil || *resp.Brokers[0].Rack != "us-east-1c" {
				t.Errorf("expected rack us-east-1c, got %v", resp.Brokers[0].Rack)
			}
		}

		// All partition leaders should be broker 5
		topic := resp.Topics[0]
		for _, p := range topic.Partitions {
			if p.Leader != 5 {
				t.Errorf("partition %d leader %d should be 5 (only broker in zone)", p.Partition, p.Leader)
			}
		}
	})

	t.Run("client with no zone gets all brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "")

		// Should return all 5 brokers
		if len(resp.Brokers) != 5 {
			t.Errorf("expected 5 brokers when no zone specified, got %d", len(resp.Brokers))
		}

		zones := make(map[string]int)
		for _, b := range resp.Brokers {
			if b.Rack != nil {
				zones[*b.Rack]++
			}
		}
		if zones["us-east-1a"] != 2 || zones["us-east-1b"] != 2 || zones["us-east-1c"] != 1 {
			t.Errorf("unexpected zone distribution: %v", zones)
		}
	})

	t.Run("client with unknown zone falls back to all brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "us-west-2a")

		// Should fallback to all 5 brokers
		if len(resp.Brokers) != 5 {
			t.Errorf("expected 5 brokers for fallback, got %d", len(resp.Brokers))
		}
	})
}

// TestZoneFiltering_FindCoordinatorReturnsSameZoneBroker verifies that when a client
// connects with a zone_id, the FindCoordinator response returns a broker from that zone.
func TestZoneFiltering_FindCoordinatorReturnsSameZoneBroker(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()

	// Register brokers in multiple zones
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1a", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1b", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "us-east-1c", AdvertisedListeners: []string{"broker4:9092"}},
	}

	clusterID := "test-cluster"
	for _, broker := range brokers {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatalf("failed to marshal broker info: %v", err)
		}
		key := keys.BrokerKeyPath(clusterID, broker.BrokerID)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to register broker %s: %v", broker.BrokerID, err)
		}
	}

	registry := routing.NewRegistry(metaStore, routing.RegistryConfig{
		ClusterID: clusterID,
		BrokerID:  "local-broker",
		NodeID:    0,
		ZoneID:    "us-east-1a",
	})
	adapter := routing.NewRegistryListerAdapter(registry)

	localBroker := protocol.BrokerInfo{NodeID: 1, Host: "broker1", Port: 9092, Rack: "us-east-1a"}
	handler := protocol.NewFindCoordinatorHandler(protocol.FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: adapter,
	})

	t.Run("client in zone us-east-1a gets coordinator from us-east-1a", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "test-group"
		req.CoordinatorType = 0 // Group coordinator

		resp := handler.Handle(ctx, 3, req, "us-east-1a")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return broker 1 or 2 (both in us-east-1a)
		if resp.NodeID != 1 && resp.NodeID != 2 {
			t.Errorf("expected coordinator from us-east-1a (1 or 2), got %d", resp.NodeID)
		}
		if resp.Host != "broker1" && resp.Host != "broker2" {
			t.Errorf("expected host broker1 or broker2, got %s", resp.Host)
		}
	})

	t.Run("client in zone us-east-1b gets coordinator from us-east-1b", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "test-group"
		req.CoordinatorType = 0

		resp := handler.Handle(ctx, 3, req, "us-east-1b")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return broker 3 (only broker in us-east-1b)
		if resp.NodeID != 3 {
			t.Errorf("expected coordinator from us-east-1b (3), got %d", resp.NodeID)
		}
		if resp.Host != "broker3" {
			t.Errorf("expected host broker3, got %s", resp.Host)
		}
	})

	t.Run("client in zone us-east-1c gets coordinator from us-east-1c", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "another-group"
		req.CoordinatorType = 0

		resp := handler.Handle(ctx, 3, req, "us-east-1c")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return broker 4 (only broker in us-east-1c)
		if resp.NodeID != 4 {
			t.Errorf("expected coordinator from us-east-1c (4), got %d", resp.NodeID)
		}
		if resp.Host != "broker4" {
			t.Errorf("expected host broker4, got %s", resp.Host)
		}
	})

	t.Run("client with unknown zone falls back to any broker", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "fallback-group"
		req.CoordinatorType = 0

		resp := handler.Handle(ctx, 3, req, "unknown-zone")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return any broker from the cluster
		if resp.NodeID < 1 || resp.NodeID > 4 {
			t.Errorf("expected coordinator from any zone (1-4), got %d", resp.NodeID)
		}
	})

	t.Run("deterministic coordinator selection within zone", func(t *testing.T) {
		// With multiple brokers in zone, the same group should always map to the same broker
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "consistent-group"
		req.CoordinatorType = 0

		firstResp := handler.Handle(ctx, 3, req, "us-east-1a")
		expectedNodeID := firstResp.NodeID

		for i := 0; i < 10; i++ {
			resp := handler.Handle(ctx, 3, req, "us-east-1a")
			if resp.NodeID != expectedNodeID {
				t.Errorf("iteration %d: expected consistent coordinator %d, got %d", i, expectedNodeID, resp.NodeID)
			}
		}
	})
}

// TestZoneFiltering_BatchedFindCoordinator verifies zone filtering with v4+ batched requests.
func TestZoneFiltering_BatchedFindCoordinator(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()

	// Register brokers in multiple zones
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "zone-a", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "zone-a", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "zone-b", AdvertisedListeners: []string{"broker3:9092"}},
	}

	clusterID := "test-cluster"
	for _, broker := range brokers {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatalf("failed to marshal broker info: %v", err)
		}
		key := keys.BrokerKeyPath(clusterID, broker.BrokerID)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to register broker %s: %v", broker.BrokerID, err)
		}
	}

	registry := routing.NewRegistry(metaStore, routing.RegistryConfig{
		ClusterID: clusterID,
		BrokerID:  "local-broker",
		NodeID:    0,
		ZoneID:    "zone-a",
	})
	adapter := routing.NewRegistryListerAdapter(registry)

	localBroker := protocol.BrokerInfo{NodeID: 1, Host: "broker1", Port: 9092}
	handler := protocol.NewFindCoordinatorHandler(protocol.FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: adapter,
	})

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(4)
	req.CoordinatorType = 0
	req.CoordinatorKeys = []string{"group-1", "group-2", "group-3"}

	resp := handler.Handle(ctx, 4, req, "zone-a")

	if len(resp.Coordinators) != 3 {
		t.Fatalf("expected 3 coordinators, got %d", len(resp.Coordinators))
	}

	for i, coord := range resp.Coordinators {
		if coord.ErrorCode != 0 {
			t.Errorf("coordinator %d: expected no error, got %d", i, coord.ErrorCode)
		}
		// All coordinators should be from zone-a (broker 1 or 2)
		if coord.NodeID != 1 && coord.NodeID != 2 {
			t.Errorf("coordinator %d for key %s: expected node from zone-a (1 or 2), got %d",
				i, coord.Key, coord.NodeID)
		}
	}
}

// TestZoneFiltering_ParseZoneIDFromClientID tests zone_id parsing from client.id.
func TestZoneFiltering_ParseZoneIDFromClientID(t *testing.T) {
	tests := []struct {
		clientID string
		expected string
	}{
		{"zone_id=us-east-1a", "us-east-1a"},
		{"zone_id=us-east-1a,app=myapp", "us-east-1a"},
		{"app=myapp,zone_id=us-west-2b,env=prod", "us-west-2b"},
		{"zone_id=eu-west-1c, key=value", "eu-west-1c"},
		{" zone_id=trimmed ", "trimmed"},
		{"app=myapp", ""},
		{"", ""},
		{"zone_id=", ""},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("clientID=%q", tt.clientID), func(t *testing.T) {
			got := protocol.ParseZoneID(tt.clientID)
			if got != tt.expected {
				t.Errorf("ParseZoneID(%q) = %q, want %q", tt.clientID, got, tt.expected)
			}

			// Also test the routing package version
			gotRouting := routing.ParseZoneID(tt.clientID)
			if gotRouting != tt.expected {
				t.Errorf("routing.ParseZoneID(%q) = %q, want %q", tt.clientID, gotRouting, tt.expected)
			}
		})
	}
}

// TestZoneFiltering_PartitionLeadersFromZone verifies that partition leader assignment
// uses only brokers from the client's zone.
func TestZoneFiltering_PartitionLeadersFromZone(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)

	// Register brokers in multiple zones
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "zone-a", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "zone-a", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "zone-a", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "zone-b", AdvertisedListeners: []string{"broker4:9092"}},
		{BrokerID: "broker-5", NodeID: 5, ZoneID: "zone-b", AdvertisedListeners: []string{"broker5:9092"}},
	}

	clusterID := "test-cluster"
	for _, broker := range brokers {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatalf("failed to marshal broker info: %v", err)
		}
		key := keys.BrokerKeyPath(clusterID, broker.BrokerID)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to register broker %s: %v", broker.BrokerID, err)
		}
	}

	// Create topic with 10 partitions
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "multi-partition-topic",
		PartitionCount: 10,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	registry := routing.NewRegistry(metaStore, routing.RegistryConfig{
		ClusterID: clusterID,
		BrokerID:  "local-broker",
		NodeID:    0,
	})
	adapter := routing.NewAffinityListerAdapter(registry)

	localBroker := protocol.BrokerInfo{NodeID: 1, Host: "broker1", Port: 9092}
	metadataHandler := protocol.NewMetadataHandler(protocol.MetadataHandlerConfig{
		ClusterID:      clusterID,
		ControllerID:   1,
		LocalBroker:    localBroker,
		BrokerLister:   adapter,
		LeaderSelector: adapter,
	}, topicStore)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "multi-partition-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	t.Run("zone-a client: all leaders from zone-a brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "zone-a")

		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
		}

		topic := resp.Topics[0]
		if len(topic.Partitions) != 10 {
			t.Fatalf("expected 10 partitions, got %d", len(topic.Partitions))
		}

		// Count leader distribution within zone-a
		leaderCounts := make(map[int32]int)
		for _, p := range topic.Partitions {
			// Leader must be from zone-a (brokers 1, 2, or 3)
			if p.Leader < 1 || p.Leader > 3 {
				t.Errorf("partition %d: leader %d not from zone-a (expected 1, 2, or 3)", p.Partition, p.Leader)
			}
			leaderCounts[p.Leader]++
		}

		// Verify all leaders are from zone-a (the key test)
		// Note: distribution depends on hash function and may not be perfectly even
		t.Logf("zone-a leader distribution: %v", leaderCounts)
	})

	t.Run("zone-b client: all leaders from zone-b brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "zone-b")

		topic := resp.Topics[0]

		// Count leader distribution within zone-b
		leaderCounts := make(map[int32]int)
		for _, p := range topic.Partitions {
			// Leader must be from zone-b (brokers 4 or 5)
			if p.Leader != 4 && p.Leader != 5 {
				t.Errorf("partition %d: leader %d not from zone-b (expected 4 or 5)", p.Partition, p.Leader)
			}
			leaderCounts[p.Leader]++
		}

		// Verify all leaders are from zone-b (the key test)
		// Note: distribution depends on hash function and may not be perfectly even
		t.Logf("zone-b leader distribution: %v", leaderCounts)
	})
}

// TestZoneFiltering_ReplicasOnlyFromZone verifies that replica lists only contain
// brokers from the client's zone.
func TestZoneFiltering_ReplicasOnlyFromZone(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)

	// Register brokers in multiple zones
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "zone-x", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "zone-x", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "zone-y", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "zone-y", AdvertisedListeners: []string{"broker4:9092"}},
	}

	clusterID := "test-cluster"
	for _, broker := range brokers {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatalf("failed to marshal broker info: %v", err)
		}
		key := keys.BrokerKeyPath(clusterID, broker.BrokerID)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to register broker %s: %v", broker.BrokerID, err)
		}
	}

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "replica-test-topic",
		PartitionCount: 4,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	registry := routing.NewRegistry(metaStore, routing.RegistryConfig{
		ClusterID: clusterID,
		BrokerID:  "local-broker",
		NodeID:    0,
	})
	adapter := routing.NewAffinityListerAdapter(registry)

	localBroker := protocol.BrokerInfo{NodeID: 1, Host: "broker1", Port: 9092}
	metadataHandler := protocol.NewMetadataHandler(protocol.MetadataHandlerConfig{
		ClusterID:      clusterID,
		ControllerID:   1,
		LocalBroker:    localBroker,
		BrokerLister:   adapter,
		LeaderSelector: adapter,
	}, topicStore)

	req := kmsg.NewPtrMetadataRequest()
	topicName := "replica-test-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	resp := metadataHandler.Handle(ctx, 9, req, "zone-x")

	topic := resp.Topics[0]
	for _, p := range topic.Partitions {
		// All replicas should be from zone-x (brokers 1 or 2)
		for _, r := range p.Replicas {
			if r != 1 && r != 2 {
				t.Errorf("partition %d: replica %d should be from zone-x (1 or 2)", p.Partition, r)
			}
		}
		// ISR should match replicas
		if len(p.ISR) != len(p.Replicas) {
			t.Errorf("partition %d: ISR length %d != replica length %d", p.Partition, len(p.ISR), len(p.Replicas))
		}
		for _, isr := range p.ISR {
			if isr != 1 && isr != 2 {
				t.Errorf("partition %d: ISR member %d should be from zone-x (1 or 2)", p.Partition, isr)
			}
		}
	}
}
