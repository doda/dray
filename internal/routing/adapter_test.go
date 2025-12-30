package routing

import (
	"context"
	"fmt"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
)

func TestRegistryListerAdapter_ListBrokers(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	ctx := context.Background()

	configs := []RegistryConfig{
		{
			ClusterID:           "cluster-1",
			BrokerID:            "broker-1",
			NodeID:              1,
			ZoneID:              "us-east-1a",
			AdvertisedListeners: []string{"broker-1.example.com:9092"},
		},
		{
			ClusterID:           "cluster-1",
			BrokerID:            "broker-2",
			NodeID:              2,
			ZoneID:              "us-east-1b",
			AdvertisedListeners: []string{"broker-2.example.com:9093"},
		},
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

	adapter := NewRegistryListerAdapter(registry)

	brokers, err := adapter.ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}

	if len(brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(brokers))
	}

	brokerMap := make(map[int32]struct {
		host string
		port int32
		rack string
	})
	for _, b := range brokers {
		brokerMap[b.NodeID] = struct {
			host string
			port int32
			rack string
		}{b.Host, b.Port, b.Rack}
	}

	if b, ok := brokerMap[1]; !ok {
		t.Error("broker 1 not found")
	} else {
		if b.host != "broker-1.example.com" {
			t.Errorf("broker 1 host mismatch: got %q", b.host)
		}
		if b.port != 9092 {
			t.Errorf("broker 1 port mismatch: got %d", b.port)
		}
		if b.rack != "us-east-1a" {
			t.Errorf("broker 1 rack mismatch: got %q", b.rack)
		}
	}

	if b, ok := brokerMap[2]; !ok {
		t.Error("broker 2 not found")
	} else {
		if b.host != "broker-2.example.com" {
			t.Errorf("broker 2 host mismatch: got %q", b.host)
		}
		if b.port != 9093 {
			t.Errorf("broker 2 port mismatch: got %d", b.port)
		}
		if b.rack != "us-east-1b" {
			t.Errorf("broker 2 rack mismatch: got %q", b.rack)
		}
	}
}

func TestRegistryListerAdapter_ListBrokersZoneFilter(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	ctx := context.Background()

	configs := []RegistryConfig{
		{ClusterID: "cluster-1", BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"b1:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1b", AdvertisedListeners: []string{"b2:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1a", AdvertisedListeners: []string{"b3:9092"}},
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

	adapter := NewRegistryListerAdapter(registry)

	brokers, err := adapter.ListBrokers(ctx, "us-east-1a")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}

	if len(brokers) != 2 {
		t.Errorf("expected 2 brokers in us-east-1a, got %d", len(brokers))
	}

	for _, b := range brokers {
		if b.Rack != "us-east-1a" {
			t.Errorf("expected rack us-east-1a, got %q", b.Rack)
		}
	}
}

func TestRegistryListerAdapter_EmptyRegistry(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	registry := NewRegistry(store, RegistryConfig{
		ClusterID: "cluster-1",
	})

	adapter := NewRegistryListerAdapter(registry)

	ctx := context.Background()
	brokers, err := adapter.ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}

	if len(brokers) != 0 {
		t.Errorf("expected 0 brokers, got %d", len(brokers))
	}
}

func TestRegistryListerAdapter_GetPartitionLeader(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	ctx := context.Background()

	configs := []RegistryConfig{
		{ClusterID: "cluster-1", BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"b1:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1a", AdvertisedListeners: []string{"b2:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1b", AdvertisedListeners: []string{"b3:9092"}},
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

	adapter := NewRegistryListerAdapter(registry)

	t.Run("deterministic leader selection", func(t *testing.T) {
		// Same stream ID should always return the same leader
		leader1, err := adapter.GetPartitionLeader(ctx, "", "stream-1")
		if err != nil {
			t.Fatalf("GetPartitionLeader failed: %v", err)
		}

		leader2, err := adapter.GetPartitionLeader(ctx, "", "stream-1")
		if err != nil {
			t.Fatalf("GetPartitionLeader failed: %v", err)
		}

		if leader1 != leader2 {
			t.Errorf("non-deterministic leader: %d vs %d", leader1, leader2)
		}
	})

	t.Run("different stream IDs return valid leaders", func(t *testing.T) {
		for i := int32(0); i < 100; i++ {
			streamID := fmt.Sprintf("stream-%d", i)
			leader, err := adapter.GetPartitionLeader(ctx, "", streamID)
			if err != nil {
				t.Fatalf("GetPartitionLeader failed: %v", err)
			}
			if leader < 1 || leader > 3 {
				t.Errorf("expected leader 1-3 for stream %q, got %d", streamID, leader)
			}
		}
	})

	t.Run("zone-filtered leader selection", func(t *testing.T) {
		// When filtering to us-east-1a, leader should be 1 or 2
		leader, err := adapter.GetPartitionLeader(ctx, "us-east-1a", "stream-1")
		if err != nil {
			t.Fatalf("GetPartitionLeader failed: %v", err)
		}

		if leader != 1 && leader != 2 {
			t.Errorf("expected leader 1 or 2 for zone us-east-1a, got %d", leader)
		}
	})

	t.Run("fallback when zone has no brokers", func(t *testing.T) {
		leader, err := adapter.GetPartitionLeader(ctx, "us-west-2a", "stream-1")
		if err != nil {
			t.Fatalf("GetPartitionLeader failed: %v", err)
		}

		// Should fall back to any broker
		if leader < 1 || leader > 3 {
			t.Errorf("expected valid leader, got %d", leader)
		}
	})

	t.Run("uses stream ID for hashing", func(t *testing.T) {
		streamID := "stream-xyz-123"
		leader, err := adapter.GetPartitionLeader(ctx, "", streamID)
		if err != nil {
			t.Fatalf("GetPartitionLeader failed: %v", err)
		}

		brokers, err := registry.ListBrokers(ctx, "")
		if err != nil {
			t.Fatalf("ListBrokers failed: %v", err)
		}

		expected := RendezvousHash(brokers, streamID)
		if leader != expected {
			t.Errorf("expected leader %d for streamID %q, got %d", expected, streamID, leader)
		}
	})
}

func TestRegistryListerAdapter_GetPartitionLeader_NoBrokers(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	registry := NewRegistry(store, RegistryConfig{
		ClusterID: "cluster-1",
	})

	adapter := NewRegistryListerAdapter(registry)

	ctx := context.Background()
	leader, err := adapter.GetPartitionLeader(ctx, "", "stream-1")
	if err != nil {
		t.Fatalf("GetPartitionLeader failed: %v", err)
	}

	if leader != -1 {
		t.Errorf("expected -1 for no brokers, got %d", leader)
	}
}

func TestAffinityListerAdapter(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	ctx := context.Background()

	configs := []RegistryConfig{
		{ClusterID: "cluster-1", BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"b1:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1b", AdvertisedListeners: []string{"b2:9092"}},
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

	adapter := NewAffinityListerAdapter(registry)

	// Test ListBrokers (inherited from RegistryListerAdapter)
	brokers, err := adapter.ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(brokers))
	}

	// Test GetPartitionLeader (inherited from RegistryListerAdapter)
	leader, err := adapter.GetPartitionLeader(ctx, "", "stream-1")
	if err != nil {
		t.Fatalf("GetPartitionLeader failed: %v", err)
	}
	if leader != 1 && leader != 2 {
		t.Errorf("expected leader 1 or 2, got %d", leader)
	}
}
