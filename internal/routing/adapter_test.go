package routing

import (
	"context"
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
