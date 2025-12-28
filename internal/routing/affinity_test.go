package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
)

func TestRendezvousHash_EmptyBrokers(t *testing.T) {
	result := RendezvousHash(nil, "stream-1")
	if result != -1 {
		t.Errorf("expected -1 for empty brokers, got %d", result)
	}

	result = RendezvousHash([]BrokerInfo{}, "stream-1")
	if result != -1 {
		t.Errorf("expected -1 for empty broker slice, got %d", result)
	}
}

func TestRendezvousHash_SingleBroker(t *testing.T) {
	brokers := []BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a"},
	}

	result := RendezvousHash(brokers, "stream-1")
	if result != 1 {
		t.Errorf("expected NodeID 1 for single broker, got %d", result)
	}

	// Should return same broker for any stream
	result = RendezvousHash(brokers, "stream-2")
	if result != 1 {
		t.Errorf("expected NodeID 1 for single broker with different stream, got %d", result)
	}
}

func TestRendezvousHash_Deterministic(t *testing.T) {
	brokers := []BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1b"},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1c"},
	}

	// Same input should always produce same output
	for i := 0; i < 100; i++ {
		result1 := RendezvousHash(brokers, "stream-123")
		result2 := RendezvousHash(brokers, "stream-123")
		if result1 != result2 {
			t.Errorf("non-deterministic result: %d vs %d", result1, result2)
		}
	}
}

func TestRendezvousHash_OrderIndependent(t *testing.T) {
	brokers1 := []BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1b"},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1c"},
	}

	brokers2 := []BrokerInfo{
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1c"},
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1b"},
	}

	// Order of brokers should not affect the result
	result1 := RendezvousHash(brokers1, "stream-123")
	result2 := RendezvousHash(brokers2, "stream-123")
	if result1 != result2 {
		t.Errorf("order affected result: %d vs %d", result1, result2)
	}
}

func TestRendezvousHash_Distribution(t *testing.T) {
	// Use UUIDs for realistic broker IDs to get better hash distribution
	brokers := []BrokerInfo{
		{BrokerID: "a1b2c3d4-e5f6-7890-abcd-ef0123456789", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "b2c3d4e5-f6a7-8901-bcde-f01234567890", NodeID: 2, ZoneID: "us-east-1b"},
		{BrokerID: "c3d4e5f6-a7b8-9012-cdef-012345678901", NodeID: 3, ZoneID: "us-east-1c"},
	}

	counts := make(map[int32]int)
	numStreams := 10000

	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		nodeID := RendezvousHash(brokers, streamID)
		counts[nodeID]++
	}

	// Verify all brokers get some streams
	for _, b := range brokers {
		if counts[b.NodeID] == 0 {
			t.Errorf("broker %d got no streams", b.NodeID)
		}
	}

	// Check distribution - each broker should get at least 20% of traffic
	// (with 3 brokers, perfect is 33%, so 20% is a reasonable lower bound)
	minExpected := numStreams / 5 // 20%

	for nodeID, count := range counts {
		if count < minExpected {
			t.Errorf("broker %d has too few streams: %d (expected at least %d)", nodeID, count, minExpected)
		}
	}
}

func TestRendezvousHash_MinimalDisruption(t *testing.T) {
	// Use UUIDs for realistic broker IDs
	brokers := []BrokerInfo{
		{BrokerID: "a1b2c3d4-e5f6-7890-abcd-ef0123456789", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "b2c3d4e5-f6a7-8901-bcde-f01234567890", NodeID: 2, ZoneID: "us-east-1b"},
		{BrokerID: "c3d4e5f6-a7b8-9012-cdef-012345678901", NodeID: 3, ZoneID: "us-east-1c"},
	}

	// Record assignments with 3 brokers
	assignments := make(map[string]int32)
	numStreams := 1000
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		assignments[streamID] = RendezvousHash(brokers, streamID)
	}

	// Remove one broker (broker-2)
	brokersReduced := []BrokerInfo{
		{BrokerID: "a1b2c3d4-e5f6-7890-abcd-ef0123456789", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "c3d4e5f6-a7b8-9012-cdef-012345678901", NodeID: 3, ZoneID: "us-east-1c"},
	}

	// Count how many streams changed assignment
	changed := 0
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		newNodeID := RendezvousHash(brokersReduced, streamID)
		if assignments[streamID] != newNodeID {
			changed++
		}
	}

	// Only streams previously assigned to broker-2 should change
	// Verify that streams NOT on broker-2 remained unchanged (rendezvous hash property)
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		oldNodeID := assignments[streamID]
		newNodeID := RendezvousHash(brokersReduced, streamID)
		// If stream was on broker-1 or broker-3, it should remain there
		if oldNodeID != 2 && oldNodeID != newNodeID {
			t.Errorf("stream %s on broker %d should not change (got %d)", streamID, oldNodeID, newNodeID)
		}
	}

	// Some streams should have moved (the ones that were on broker-2)
	if changed == 0 {
		t.Error("expected some streams to change when broker removed")
	}
}

func TestRendezvousHash_AddBroker(t *testing.T) {
	// Use UUIDs for realistic broker IDs
	brokers := []BrokerInfo{
		{BrokerID: "a1b2c3d4-e5f6-7890-abcd-ef0123456789", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "b2c3d4e5-f6a7-8901-bcde-f01234567890", NodeID: 2, ZoneID: "us-east-1b"},
	}

	// Record assignments with 2 brokers
	assignments := make(map[string]int32)
	numStreams := 1000
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		assignments[streamID] = RendezvousHash(brokers, streamID)
	}

	// Add a third broker
	brokersExpanded := []BrokerInfo{
		{BrokerID: "a1b2c3d4-e5f6-7890-abcd-ef0123456789", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "b2c3d4e5-f6a7-8901-bcde-f01234567890", NodeID: 2, ZoneID: "us-east-1b"},
		{BrokerID: "c3d4e5f6-a7b8-9012-cdef-012345678901", NodeID: 3, ZoneID: "us-east-1c"},
	}

	// Count how many streams changed assignment
	changed := 0
	movedToNew := 0
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		oldNodeID := assignments[streamID]
		newNodeID := RendezvousHash(brokersExpanded, streamID)
		if oldNodeID != newNodeID {
			changed++
			// Streams that changed should only move to the new broker
			if newNodeID != 3 {
				t.Errorf("stream %s moved from %d to %d (expected to stay or move to 3)", streamID, oldNodeID, newNodeID)
			}
			movedToNew++
		}
	}

	// Some streams should move to the new broker
	if movedToNew == 0 {
		t.Error("expected some streams to move to new broker")
	}

	// Not all should move
	if movedToNew == numStreams {
		t.Error("expected most streams to stay with original brokers")
	}
}

func TestGetAffinityBrokerFromList(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		result := GetAffinityBrokerFromList(nil, "stream-1")
		if result != -1 {
			t.Errorf("expected -1, got %d", result)
		}
	})

	t.Run("single broker", func(t *testing.T) {
		brokers := []BrokerInfo{
			{BrokerID: "broker-1", NodeID: 1},
		}
		result := GetAffinityBrokerFromList(brokers, "stream-1")
		if result != 1 {
			t.Errorf("expected 1, got %d", result)
		}
	})

	t.Run("multiple brokers", func(t *testing.T) {
		brokers := []BrokerInfo{
			{BrokerID: "broker-1", NodeID: 1},
			{BrokerID: "broker-2", NodeID: 2},
		}
		result := GetAffinityBrokerFromList(brokers, "stream-123")
		if result != 1 && result != 2 {
			t.Errorf("expected 1 or 2, got %d", result)
		}
	})
}

func TestStaticAffinityMapper(t *testing.T) {
	brokers := []BrokerInfo{
		{BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1a"},
		{BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1b"},
		{BrokerID: "broker-4", NodeID: 4, ZoneID: "us-east-1b"},
	}

	mapper := NewStaticAffinityMapper(brokers)
	ctx := context.Background()

	t.Run("all brokers when zone is empty", func(t *testing.T) {
		nodeID, err := mapper.SelectLeader(ctx, "", "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nodeID < 1 || nodeID > 4 {
			t.Errorf("unexpected nodeID: %d", nodeID)
		}
	})

	t.Run("zone-specific brokers", func(t *testing.T) {
		nodeID, err := mapper.SelectLeader(ctx, "us-east-1a", "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nodeID != 1 && nodeID != 2 {
			t.Errorf("expected nodeID 1 or 2 for zone us-east-1a, got %d", nodeID)
		}
	})

	t.Run("fallback to all when zone has no brokers", func(t *testing.T) {
		nodeID, err := mapper.SelectLeader(ctx, "us-west-2a", "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nodeID < 1 || nodeID > 4 {
			t.Errorf("unexpected nodeID: %d", nodeID)
		}
	})

	t.Run("deterministic within zone", func(t *testing.T) {
		nodeID1, _ := mapper.SelectLeader(ctx, "us-east-1b", "stream-123")
		nodeID2, _ := mapper.SelectLeader(ctx, "us-east-1b", "stream-123")
		if nodeID1 != nodeID2 {
			t.Errorf("non-deterministic: %d vs %d", nodeID1, nodeID2)
		}
	})
}

func TestRendezvousHashUint64(t *testing.T) {
	// Use UUIDs for realistic broker IDs
	brokers := []BrokerInfo{
		{BrokerID: "a1b2c3d4-e5f6-7890-abcd-ef0123456789", NodeID: 1, ZoneID: "us-east-1a"},
		{BrokerID: "b2c3d4e5-f6a7-8901-bcde-f01234567890", NodeID: 2, ZoneID: "us-east-1b"},
		{BrokerID: "c3d4e5f6-a7b8-9012-cdef-012345678901", NodeID: 3, ZoneID: "us-east-1c"},
	}

	t.Run("empty brokers", func(t *testing.T) {
		result := RendezvousHashUint64(nil, 123)
		if result != -1 {
			t.Errorf("expected -1, got %d", result)
		}
	})

	t.Run("single broker", func(t *testing.T) {
		result := RendezvousHashUint64(brokers[:1], 123)
		if result != 1 {
			t.Errorf("expected 1, got %d", result)
		}
	})

	t.Run("deterministic", func(t *testing.T) {
		result1 := RendezvousHashUint64(brokers, 12345)
		result2 := RendezvousHashUint64(brokers, 12345)
		if result1 != result2 {
			t.Errorf("non-deterministic: %d vs %d", result1, result2)
		}
	})

	t.Run("distribution", func(t *testing.T) {
		counts := make(map[int32]int)
		numKeys := 3000

		// Use varied keys to test distribution
		// Small sequential integers have poor entropy (many leading zeros)
		for i := uint64(0); i < uint64(numKeys); i++ {
			// Add a large offset and multiply to spread the key space
			key := (i * 1000000007) + 0x1234567890abcdef
			nodeID := RendezvousHashUint64(brokers, key)
			counts[nodeID]++
		}

		// Verify all brokers get some keys
		for _, b := range brokers {
			if counts[b.NodeID] == 0 {
				t.Errorf("broker %d got no keys", b.NodeID)
			}
		}
	})
}

func TestAffinityMapper_WithRegistry(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()
	ctx := context.Background()

	// Register brokers
	configs := []RegistryConfig{
		{ClusterID: "test-cluster", BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"localhost:9092"}},
		{ClusterID: "test-cluster", BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1a", AdvertisedListeners: []string{"localhost:9093"}},
		{ClusterID: "test-cluster", BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1b", AdvertisedListeners: []string{"localhost:9094"}},
	}

	var registry *Registry
	for _, cfg := range configs {
		r := NewRegistry(store, cfg)
		if err := r.Register(ctx); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
		if registry == nil {
			registry = r
		}
	}

	mapper := NewAffinityMapper(registry)

	t.Run("returns broker from zone", func(t *testing.T) {
		nodeID, err := mapper.GetAffinityBroker(ctx, "us-east-1a", "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nodeID != 1 && nodeID != 2 {
			t.Errorf("expected nodeID 1 or 2 for zone us-east-1a, got %d", nodeID)
		}
	})

	t.Run("fallback when zone empty", func(t *testing.T) {
		nodeID, err := mapper.GetAffinityBroker(ctx, "us-west-2a", "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nodeID < 1 || nodeID > 3 {
			t.Errorf("expected nodeID 1-3, got %d", nodeID)
		}
	})

	t.Run("all brokers when zone is empty string", func(t *testing.T) {
		nodeID, err := mapper.GetAffinityBroker(ctx, "", "stream-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if nodeID < 1 || nodeID > 3 {
			t.Errorf("expected nodeID 1-3, got %d", nodeID)
		}
	})

	t.Run("deterministic results", func(t *testing.T) {
		nodeID1, _ := mapper.GetAffinityBroker(ctx, "us-east-1a", "stream-123")
		nodeID2, _ := mapper.GetAffinityBroker(ctx, "us-east-1a", "stream-123")
		if nodeID1 != nodeID2 {
			t.Errorf("non-deterministic: %d vs %d", nodeID1, nodeID2)
		}
	})
}

func TestAffinityMapper_GetAffinityBrokersForPartitions(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()
	ctx := context.Background()

	// Register brokers
	configs := []RegistryConfig{
		{ClusterID: "test-cluster", BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"localhost:9092"}},
		{ClusterID: "test-cluster", BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1b", AdvertisedListeners: []string{"localhost:9093"}},
	}

	var registry *Registry
	for _, cfg := range configs {
		r := NewRegistry(store, cfg)
		if err := r.Register(ctx); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
		if registry == nil {
			registry = r
		}
	}

	mapper := NewAffinityMapper(registry)

	streamIDs := []string{"stream-1", "stream-2", "stream-3"}
	result, err := mapper.GetAffinityBrokersForPartitions(ctx, "", streamIDs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("expected 3 results, got %d", len(result))
	}

	for _, streamID := range streamIDs {
		nodeID, ok := result[streamID]
		if !ok {
			t.Errorf("missing result for %s", streamID)
		}
		if nodeID != 1 && nodeID != 2 {
			t.Errorf("unexpected nodeID for %s: %d", streamID, nodeID)
		}
	}
}

func TestAffinityMapper_CacheOperations(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()
	ctx := context.Background()

	cfg := RegistryConfig{
		ClusterID:           "test-cluster",
		BrokerID:            "broker-1",
		NodeID:              1,
		ZoneID:              "us-east-1a",
		AdvertisedListeners: []string{"localhost:9092"},
	}
	registry := NewRegistry(store, cfg)
	if err := registry.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	mapper := NewAffinityMapper(registry)

	// First call should populate cache
	nodeID1, err := mapper.GetAffinityBrokerCached(ctx, "us-east-1a", "stream-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second call should use cache
	nodeID2, err := mapper.GetAffinityBrokerCached(ctx, "us-east-1a", "stream-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if nodeID1 != nodeID2 {
		t.Errorf("cached result differs: %d vs %d", nodeID1, nodeID2)
	}

	// Clear cache
	mapper.ClearCache()

	// Call should still work after clear
	nodeID3, err := mapper.GetAffinityBrokerCached(ctx, "us-east-1a", "stream-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodeID1 != nodeID3 {
		t.Errorf("result differs after cache clear: %d vs %d", nodeID1, nodeID3)
	}
}

func TestAffinityMapper_NoBrokers(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()
	ctx := context.Background()

	cfg := RegistryConfig{
		ClusterID:           "test-cluster",
		BrokerID:            "test-broker",
		NodeID:              1,
		ZoneID:              "us-east-1a",
		AdvertisedListeners: []string{"localhost:9092"},
	}
	registry := NewRegistry(store, cfg)
	// Do NOT register - we want an empty broker list

	mapper := NewAffinityMapper(registry)

	nodeID, err := mapper.GetAffinityBroker(ctx, "", "stream-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodeID != -1 {
		t.Errorf("expected -1 for no brokers, got %d", nodeID)
	}
}

func TestAffinityMapper_UpdateOnBrokerMembershipChange(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()
	ctx := context.Background()

	// Start with 2 brokers
	configs := []RegistryConfig{
		{ClusterID: "test-cluster", BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"localhost:9092"}},
		{ClusterID: "test-cluster", BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1a", AdvertisedListeners: []string{"localhost:9093"}},
	}

	var registry *Registry
	for _, cfg := range configs {
		r := NewRegistry(store, cfg)
		if err := r.Register(ctx); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
		if registry == nil {
			registry = r
		}
	}

	mapper := NewAffinityMapper(registry)

	// Record assignments for multiple streams
	assignments := make(map[string]int32)
	numStreams := 1000
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		nodeID, _ := mapper.GetAffinityBroker(ctx, "", streamID)
		assignments[streamID] = nodeID
	}

	// Add a third broker
	cfg3 := RegistryConfig{ClusterID: "test-cluster", BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1a", AdvertisedListeners: []string{"localhost:9094"}}
	r3 := NewRegistry(store, cfg3)
	if err := r3.Register(ctx); err != nil {
		t.Fatalf("Register broker-3 failed: %v", err)
	}

	// Verify some streams moved to new broker (but not all)
	movedToNew := 0
	unchanged := 0
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		newNodeID, _ := mapper.GetAffinityBroker(ctx, "", streamID)
		if newNodeID == 3 {
			movedToNew++
		}
		if newNodeID == assignments[streamID] {
			unchanged++
		}
	}

	// With rendezvous hash, roughly 1/3 should move to new broker
	if movedToNew == 0 {
		t.Error("expected some streams to move to new broker")
	}
	if movedToNew == numStreams {
		t.Error("expected most streams to remain unchanged")
	}
	if unchanged < numStreams/2 {
		t.Errorf("too many streams changed: %d unchanged out of %d", unchanged, numStreams)
	}
}
