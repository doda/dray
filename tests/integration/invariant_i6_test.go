package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/routing"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestInvariantI6_ZoneAwareMetadata tests invariant I6:
// Zone-aware metadata returns only zone brokers when zone_id is set.
//
// This invariant ensures that when a Kafka client specifies its availability
// zone via the client.id field (format: zone_id=<zone>,...), the Metadata
// response contains only brokers from that zone. This enables zone-aware
// routing for multi-AZ deployments to minimize cross-zone traffic costs.
func TestInvariantI6_ZoneAwareMetadata(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)

	clusterID := "test-cluster-i6"

	// Step 1: Register brokers in multiple zones
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1a", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1b", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "us-east-1b", AdvertisedListeners: []string{"broker4:9092"}},
		{BrokerID: "broker-5", NodeID: 5, ZoneID: "us-east-1c", AdvertisedListeners: []string{"broker5:9092"}},
	}

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

	// Create test topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i6-topic",
		PartitionCount: 6,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

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

	// Step 2: Send Metadata with zone_id in client.id
	req := kmsg.NewPtrMetadataRequest()
	topicName := "invariant-i6-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	t.Run("zone_id=us-east-1a returns only us-east-1a brokers", func(t *testing.T) {
		// Step 3: Verify only same-zone brokers returned
		resp := metadataHandler.Handle(ctx, 9, req, "us-east-1a")

		// Should return exactly 2 brokers (broker-1 and broker-2)
		if len(resp.Brokers) != 2 {
			t.Fatalf("expected 2 brokers for zone us-east-1a, got %d", len(resp.Brokers))
		}

		// Verify all returned brokers are from the correct zone
		for _, b := range resp.Brokers {
			if b.NodeID != 1 && b.NodeID != 2 {
				t.Errorf("unexpected broker NodeID %d (expected 1 or 2 from us-east-1a)", b.NodeID)
			}
			if b.Rack == nil || *b.Rack != "us-east-1a" {
				t.Errorf("broker %d: expected rack us-east-1a, got %v", b.NodeID, b.Rack)
			}
		}

		// Verify partition leaders/replicas are also zone-aware
		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic in response, got %d", len(resp.Topics))
		}
		topic := resp.Topics[0]
		for _, p := range topic.Partitions {
			if p.Leader != 1 && p.Leader != 2 {
				t.Errorf("partition %d: leader %d should be from us-east-1a (1 or 2)", p.Partition, p.Leader)
			}
			for _, r := range p.Replicas {
				if r != 1 && r != 2 {
					t.Errorf("partition %d: replica %d should be from us-east-1a (1 or 2)", p.Partition, r)
				}
			}
		}
	})

	t.Run("zone_id=us-east-1b returns only us-east-1b brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "us-east-1b")

		// Should return exactly 2 brokers (broker-3 and broker-4)
		if len(resp.Brokers) != 2 {
			t.Fatalf("expected 2 brokers for zone us-east-1b, got %d", len(resp.Brokers))
		}

		for _, b := range resp.Brokers {
			if b.NodeID != 3 && b.NodeID != 4 {
				t.Errorf("unexpected broker NodeID %d (expected 3 or 4 from us-east-1b)", b.NodeID)
			}
			if b.Rack == nil || *b.Rack != "us-east-1b" {
				t.Errorf("broker %d: expected rack us-east-1b, got %v", b.NodeID, b.Rack)
			}
		}

		// Verify leaders are from zone-b
		topic := resp.Topics[0]
		for _, p := range topic.Partitions {
			if p.Leader != 3 && p.Leader != 4 {
				t.Errorf("partition %d: leader %d should be from us-east-1b (3 or 4)", p.Partition, p.Leader)
			}
		}
	})

	t.Run("zone_id=us-east-1c returns only us-east-1c broker", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "us-east-1c")

		// Should return exactly 1 broker (broker-5)
		if len(resp.Brokers) != 1 {
			t.Fatalf("expected 1 broker for zone us-east-1c, got %d", len(resp.Brokers))
		}

		if resp.Brokers[0].NodeID != 5 {
			t.Errorf("expected broker 5 from us-east-1c, got %d", resp.Brokers[0].NodeID)
		}
		if resp.Brokers[0].Rack == nil || *resp.Brokers[0].Rack != "us-east-1c" {
			t.Errorf("expected rack us-east-1c, got %v", resp.Brokers[0].Rack)
		}

		// All leaders must be broker 5
		topic := resp.Topics[0]
		for _, p := range topic.Partitions {
			if p.Leader != 5 {
				t.Errorf("partition %d: leader %d should be 5 (only broker in zone)", p.Partition, p.Leader)
			}
		}
	})

	t.Run("empty zone_id returns all brokers", func(t *testing.T) {
		// Step 4: Verify fallback to all when no zone_id
		resp := metadataHandler.Handle(ctx, 9, req, "")

		// Should return all 5 brokers
		if len(resp.Brokers) != 5 {
			t.Fatalf("expected 5 brokers when no zone specified, got %d", len(resp.Brokers))
		}

		// Verify zone distribution
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

	t.Run("unknown zone_id falls back to all brokers", func(t *testing.T) {
		// Step 4: Verify fallback to all when no zone match
		resp := metadataHandler.Handle(ctx, 9, req, "unknown-zone")

		// Should fall back to all 5 brokers
		if len(resp.Brokers) != 5 {
			t.Fatalf("expected 5 brokers for unknown zone fallback, got %d", len(resp.Brokers))
		}

		// Verify all zones are represented
		zones := make(map[string]int)
		for _, b := range resp.Brokers {
			if b.Rack != nil {
				zones[*b.Rack]++
			}
		}
		if zones["us-east-1a"] != 2 || zones["us-east-1b"] != 2 || zones["us-east-1c"] != 1 {
			t.Errorf("unexpected zone distribution in fallback: %v", zones)
		}
	})
}

// TestInvariantI6_FindCoordinatorZoneAware tests that FindCoordinator also
// respects zone_id and returns coordinators from the client's zone.
func TestInvariantI6_FindCoordinatorZoneAware(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()

	clusterID := "test-cluster-i6-coordinator"

	// Register brokers in multiple zones
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "zone-x", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "zone-x", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "zone-y", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "zone-z", AdvertisedListeners: []string{"broker4:9092"}},
	}

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
		ZoneID:    "zone-x",
	})
	adapter := routing.NewRegistryListerAdapter(registry)

	localBroker := protocol.BrokerInfo{NodeID: 1, Host: "broker1", Port: 9092}
	handler := protocol.NewFindCoordinatorHandler(protocol.FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: adapter,
	})

	t.Run("zone-x client gets coordinator from zone-x", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "test-group"
		req.CoordinatorType = 0 // Group coordinator

		resp := handler.Handle(ctx, 3, req, "zone-x")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return broker 1 or 2 (both in zone-x)
		if resp.NodeID != 1 && resp.NodeID != 2 {
			t.Errorf("expected coordinator from zone-x (1 or 2), got %d", resp.NodeID)
		}
	})

	t.Run("zone-y client gets coordinator from zone-y", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "another-group"
		req.CoordinatorType = 0

		resp := handler.Handle(ctx, 3, req, "zone-y")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return broker 3 (only broker in zone-y)
		if resp.NodeID != 3 {
			t.Errorf("expected coordinator from zone-y (3), got %d", resp.NodeID)
		}
	})

	t.Run("zone-z client gets coordinator from zone-z", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "group-in-zone-z"
		req.CoordinatorType = 0

		resp := handler.Handle(ctx, 3, req, "zone-z")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return broker 4 (only broker in zone-z)
		if resp.NodeID != 4 {
			t.Errorf("expected coordinator from zone-z (4), got %d", resp.NodeID)
		}
	})

	t.Run("unknown zone falls back to any broker", func(t *testing.T) {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "fallback-group"
		req.CoordinatorType = 0

		resp := handler.Handle(ctx, 3, req, "unknown-zone")

		if resp.ErrorCode != 0 {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
		// Should return any valid broker
		if resp.NodeID < 1 || resp.NodeID > 4 {
			t.Errorf("expected coordinator from any zone (1-4), got %d", resp.NodeID)
		}
	})

	t.Run("deterministic coordinator selection", func(t *testing.T) {
		// Same group key should always map to same coordinator in the zone
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(3)
		req.CoordinatorKey = "consistent-group"
		req.CoordinatorType = 0

		firstResp := handler.Handle(ctx, 3, req, "zone-x")
		expectedNodeID := firstResp.NodeID

		for i := 0; i < 10; i++ {
			resp := handler.Handle(ctx, 3, req, "zone-x")
			if resp.NodeID != expectedNodeID {
				t.Errorf("iteration %d: expected consistent coordinator %d, got %d",
					i, expectedNodeID, resp.NodeID)
			}
		}
	})
}

// TestInvariantI6_PartitionLeadersZoneAware tests that partition leaders
// are assigned from the client's zone brokers.
func TestInvariantI6_PartitionLeadersZoneAware(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)

	clusterID := "test-cluster-i6-leaders"

	// Register brokers in 2 zones with different counts
	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "zone-a", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "zone-a", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "zone-a", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "zone-b", AdvertisedListeners: []string{"broker4:9092"}},
		{BrokerID: "broker-5", NodeID: 5, ZoneID: "zone-b", AdvertisedListeners: []string{"broker5:9092"}},
	}

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
		Name:           "invariant-i6-leaders-topic",
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
	topicName := "invariant-i6-leaders-topic"
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topicName
	req.Topics = append(req.Topics, reqTopic)

	t.Run("zone-a client: all leaders from zone-a brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "zone-a")

		topic := resp.Topics[0]
		if len(topic.Partitions) != 10 {
			t.Fatalf("expected 10 partitions, got %d", len(topic.Partitions))
		}

		leaderCounts := make(map[int32]int)
		for _, p := range topic.Partitions {
			// Leader must be from zone-a (brokers 1, 2, or 3)
			if p.Leader < 1 || p.Leader > 3 {
				t.Errorf("partition %d: leader %d not from zone-a (expected 1, 2, or 3)", p.Partition, p.Leader)
			}
			leaderCounts[p.Leader]++
		}
		t.Logf("zone-a leader distribution: %v", leaderCounts)
	})

	t.Run("zone-b client: all leaders from zone-b brokers", func(t *testing.T) {
		resp := metadataHandler.Handle(ctx, 9, req, "zone-b")

		topic := resp.Topics[0]
		leaderCounts := make(map[int32]int)
		for _, p := range topic.Partitions {
			// Leader must be from zone-b (brokers 4 or 5)
			if p.Leader != 4 && p.Leader != 5 {
				t.Errorf("partition %d: leader %d not from zone-b (expected 4 or 5)", p.Partition, p.Leader)
			}
			leaderCounts[p.Leader]++
		}
		t.Logf("zone-b leader distribution: %v", leaderCounts)
	})
}

// TestInvariantI6_ReplicasZoneAware tests that replicas are only assigned
// from brokers in the client's zone.
func TestInvariantI6_ReplicasZoneAware(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)

	clusterID := "test-cluster-i6-replicas"

	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "zone-x", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "zone-x", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "zone-y", AdvertisedListeners: []string{"broker3:9092"}},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "zone-y", AdvertisedListeners: []string{"broker4:9092"}},
	}

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
		Name:           "invariant-i6-replicas-topic",
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
	topicName := "invariant-i6-replicas-topic"
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
		for _, isr := range p.ISR {
			if isr != 1 && isr != 2 {
				t.Errorf("partition %d: ISR member %d should be from zone-x (1 or 2)", p.Partition, isr)
			}
		}
	}
}

// TestInvariantI6_ParseZoneIDFromClientID tests the zone_id parsing logic
// that extracts zone from client.id field per spec 7.1.
func TestInvariantI6_ParseZoneIDFromClientID(t *testing.T) {
	testCases := []struct {
		clientID     string
		expectedZone string
	}{
		// Standard format per spec 7.1
		{"zone_id=us-east-1a", "us-east-1a"},
		{"zone_id=us-east-1a,app=myapp", "us-east-1a"},
		{"app=myapp,zone_id=us-west-2b,env=prod", "us-west-2b"},

		// Edge cases
		{"zone_id=eu-west-1c, key=value", "eu-west-1c"},    // space after comma
		{" zone_id=trimmed ", "trimmed"},                    // leading/trailing spaces
		{"zone_id=zone-with-dashes-123", "zone-with-dashes-123"},

		// No zone_id cases - should return empty
		{"app=myapp", ""},
		{"", ""},
		{"zone_id=", ""},        // empty zone value
		{"other_zone_id=x", ""}, // not exact match
	}

	for _, tc := range testCases {
		t.Run("clientID="+tc.clientID, func(t *testing.T) {
			// Test both protocol and routing package implementations
			protocolResult := protocol.ParseZoneID(tc.clientID)
			routingResult := routing.ParseZoneID(tc.clientID)

			if protocolResult != tc.expectedZone {
				t.Errorf("protocol.ParseZoneID(%q) = %q, expected %q",
					tc.clientID, protocolResult, tc.expectedZone)
			}
			if routingResult != tc.expectedZone {
				t.Errorf("routing.ParseZoneID(%q) = %q, expected %q",
					tc.clientID, routingResult, tc.expectedZone)
			}
			if protocolResult != routingResult {
				t.Errorf("protocol and routing implementations differ: %q vs %q",
					protocolResult, routingResult)
			}
		})
	}
}

// TestInvariantI6_BatchedFindCoordinator tests zone filtering with v4+ batched requests.
func TestInvariantI6_BatchedFindCoordinator(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()

	clusterID := "test-cluster-i6-batched"

	brokers := []routing.BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "zone-a", AdvertisedListeners: []string{"broker1:9092"}},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "zone-a", AdvertisedListeners: []string{"broker2:9092"}},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "zone-b", AdvertisedListeners: []string{"broker3:9092"}},
	}

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
