package routing

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata/oxia"
)

// Oxia requires a minimum session timeout of 5 seconds
const minSessionTimeout = 5 * time.Second

// TestBrokerRegistration_EphemeralKeyExpires tests that broker registration
// uses ephemeral keys that are deleted when the session ends.
func TestBrokerRegistration_EphemeralKeyExpires(t *testing.T) {
	server := oxia.StartTestServer(t)
	addr := server.Addr()

	cfg := oxia.Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	ctx := context.Background()

	store1, err := oxia.New(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create store1: %v", err)
	}

	regConfig := RegistryConfig{
		ClusterID:           "test-cluster",
		BrokerID:            "broker-1",
		NodeID:              1,
		ZoneID:              "us-east-1a",
		AdvertisedListeners: []string{"broker-1.test.local:9092"},
		BuildInfo: BuildInfo{
			Version:   "test",
			GitCommit: "abc123",
		},
	}

	registry := NewRegistry(store1, regConfig)

	if err := registry.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	brokers, err := registry.ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(brokers))
	}
	if brokers[0].BrokerID != "broker-1" {
		t.Errorf("broker ID mismatch: got %q, want %q", brokers[0].BrokerID, "broker-1")
	}

	store1.Close()

	store2, err := oxia.New(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create store2: %v", err)
	}
	defer store2.Close()

	time.Sleep(minSessionTimeout + 2*time.Second)

	registry2 := NewRegistry(store2, RegistryConfig{
		ClusterID: "test-cluster",
	})

	brokers, err = registry2.ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(brokers) != 0 {
		t.Errorf("expected 0 brokers after session expiry, got %d", len(brokers))
	}
}

// TestBrokerRegistration_MultipleBrokersDiscovery tests that multiple brokers
// can register and discover each other.
func TestBrokerRegistration_MultipleBrokersDiscovery(t *testing.T) {
	server := oxia.StartTestServer(t)
	addr := server.Addr()

	cfg := oxia.Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	ctx := context.Background()

	store, err := oxia.New(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	brokerConfigs := []RegistryConfig{
		{
			ClusterID:           "test-cluster",
			BrokerID:            "broker-1",
			NodeID:              1,
			ZoneID:              "us-east-1a",
			AdvertisedListeners: []string{"broker-1:9092"},
		},
		{
			ClusterID:           "test-cluster",
			BrokerID:            "broker-2",
			NodeID:              2,
			ZoneID:              "us-east-1b",
			AdvertisedListeners: []string{"broker-2:9092"},
		},
		{
			ClusterID:           "test-cluster",
			BrokerID:            "broker-3",
			NodeID:              3,
			ZoneID:              "us-east-1a",
			AdvertisedListeners: []string{"broker-3:9092"},
		},
	}

	var registries []*Registry
	for _, cfg := range brokerConfigs {
		reg := NewRegistry(store, cfg)
		if err := reg.Register(ctx); err != nil {
			t.Fatalf("Register %s failed: %v", cfg.BrokerID, err)
		}
		registries = append(registries, reg)
	}

	brokers, err := registries[0].ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(brokers) != 3 {
		t.Errorf("expected 3 brokers, got %d", len(brokers))
	}

	brokerIDs := make(map[string]bool)
	for _, b := range brokers {
		brokerIDs[b.BrokerID] = true
	}
	for _, cfg := range brokerConfigs {
		if !brokerIDs[cfg.BrokerID] {
			t.Errorf("broker %s not found in discovery", cfg.BrokerID)
		}
	}

	zone1Brokers, err := registries[0].ListBrokers(ctx, "us-east-1a")
	if err != nil {
		t.Fatalf("ListBrokers zone filter failed: %v", err)
	}
	if len(zone1Brokers) != 2 {
		t.Errorf("expected 2 brokers in us-east-1a, got %d", len(zone1Brokers))
	}
	for _, b := range zone1Brokers {
		if b.ZoneID != "us-east-1a" {
			t.Errorf("unexpected zone %s for broker %s", b.ZoneID, b.BrokerID)
		}
	}
}

// TestBrokerRegistration_UpdateRegistration tests that a broker can update
// its registration.
func TestBrokerRegistration_UpdateRegistration(t *testing.T) {
	server := oxia.StartTestServer(t)
	addr := server.Addr()

	cfg := oxia.Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	ctx := context.Background()

	store, err := oxia.New(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	regConfig := RegistryConfig{
		ClusterID:           "test-cluster",
		BrokerID:            "broker-1",
		NodeID:              1,
		ZoneID:              "us-east-1a",
		AdvertisedListeners: []string{"broker-1:9092"},
	}

	registry := NewRegistry(store, regConfig)
	if err := registry.Register(ctx); err != nil {
		t.Fatalf("first Register failed: %v", err)
	}

	regConfig.AdvertisedListeners = []string{"broker-1-new:9092"}
	registry2 := NewRegistry(store, regConfig)
	if err := registry2.Register(ctx); err != nil {
		t.Fatalf("second Register failed: %v", err)
	}

	broker, exists, err := registry2.GetBroker(ctx, "broker-1")
	if err != nil {
		t.Fatalf("GetBroker failed: %v", err)
	}
	if !exists {
		t.Fatal("broker should exist")
	}
	if len(broker.AdvertisedListeners) != 1 || broker.AdvertisedListeners[0] != "broker-1-new:9092" {
		t.Errorf("listeners not updated: got %v", broker.AdvertisedListeners)
	}
}
