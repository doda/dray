package routing

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

func TestRegistry_Register(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	cfg := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              1,
		ZoneID:              "us-east-1a",
		AdvertisedListeners: []string{"broker-1.example.com:9092"},
		BuildInfo: BuildInfo{
			Version:   "0.1.0",
			GitCommit: "abc123",
			BuildTime: "2024-01-01T00:00:00Z",
		},
	}

	registry := NewRegistry(store, cfg)

	ctx := context.Background()
	if err := registry.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if !registry.IsRegistered() {
		t.Error("registry should be registered")
	}

	key := keys.BrokerKeyPath("cluster-1", "broker-1")
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Fatal("broker key should exist after registration")
	}

	var info BrokerInfo
	if err := json.Unmarshal(result.Value, &info); err != nil {
		t.Fatalf("failed to unmarshal broker info: %v", err)
	}

	if info.BrokerID != "broker-1" {
		t.Errorf("BrokerID mismatch: got %q, want %q", info.BrokerID, "broker-1")
	}
	if info.NodeID != 1 {
		t.Errorf("NodeID mismatch: got %d, want %d", info.NodeID, 1)
	}
	if info.ZoneID != "us-east-1a" {
		t.Errorf("ZoneID mismatch: got %q, want %q", info.ZoneID, "us-east-1a")
	}
	if len(info.AdvertisedListeners) != 1 || info.AdvertisedListeners[0] != "broker-1.example.com:9092" {
		t.Errorf("AdvertisedListeners mismatch: got %v", info.AdvertisedListeners)
	}
	if info.StartedAt <= 0 {
		t.Error("StartedAt should be set")
	}
	if info.BuildInfo.Version != "0.1.0" {
		t.Errorf("Version mismatch: got %q, want %q", info.BuildInfo.Version, "0.1.0")
	}
}

func TestRegistry_RegisterDuplicateBrokerID(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	cfg1 := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              1,
		ZoneID:              "us-east-1a",
		AdvertisedListeners: []string{"broker-1.example.com:9092"},
	}
	cfg2 := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              2,
		ZoneID:              "us-east-1b",
		AdvertisedListeners: []string{"broker-1-new.example.com:9092"},
	}

	reg1 := NewRegistry(store, cfg1)
	reg2 := NewRegistry(store, cfg2)

	ctx := context.Background()
	if err := reg1.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if err := reg2.Register(ctx); err == nil {
		t.Fatal("expected duplicate registration to fail")
	} else if !errors.Is(err, ErrBrokerAlreadyRegistered) {
		t.Fatalf("expected ErrBrokerAlreadyRegistered, got %v", err)
	}

	key := keys.BrokerKeyPath("cluster-1", "broker-1")
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Fatal("broker key should exist after registration")
	}

	var info BrokerInfo
	if err := json.Unmarshal(result.Value, &info); err != nil {
		t.Fatalf("failed to unmarshal broker info: %v", err)
	}
	if info.NodeID != 1 {
		t.Errorf("NodeID mismatch: got %d, want %d", info.NodeID, 1)
	}
	if len(info.AdvertisedListeners) != 1 || info.AdvertisedListeners[0] != "broker-1.example.com:9092" {
		t.Errorf("AdvertisedListeners mismatch: got %v", info.AdvertisedListeners)
	}
}

func TestRegistry_RegisterMultipleBrokers(t *testing.T) {
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
			AdvertisedListeners: []string{"broker-2.example.com:9092"},
		},
		{
			ClusterID:           "cluster-1",
			BrokerID:            "broker-3",
			NodeID:              3,
			ZoneID:              "us-east-1a",
			AdvertisedListeners: []string{"broker-3.example.com:9092"},
		},
	}

	var registries []*Registry
	for _, cfg := range configs {
		registry := NewRegistry(store, cfg)
		if err := registry.Register(ctx); err != nil {
			t.Fatalf("Register for %s failed: %v", cfg.BrokerID, err)
		}
		registries = append(registries, registry)
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
	for _, cfg := range configs {
		if !brokerIDs[cfg.BrokerID] {
			t.Errorf("broker %s not found in list", cfg.BrokerID)
		}
	}
}

func TestRegistry_ListBrokersZoneFilter(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	ctx := context.Background()

	configs := []RegistryConfig{
		{ClusterID: "cluster-1", BrokerID: "broker-1", NodeID: 1, ZoneID: "us-east-1a", AdvertisedListeners: []string{"b1:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-2", NodeID: 2, ZoneID: "us-east-1b", AdvertisedListeners: []string{"b2:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-3", NodeID: 3, ZoneID: "us-east-1a", AdvertisedListeners: []string{"b3:9092"}},
		{ClusterID: "cluster-1", BrokerID: "broker-4", NodeID: 4, ZoneID: "us-east-1c", AdvertisedListeners: []string{"b4:9092"}},
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

	brokers, err := registry.ListBrokers(ctx, "us-east-1a")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(brokers) != 2 {
		t.Errorf("expected 2 brokers in us-east-1a, got %d", len(brokers))
	}
	for _, b := range brokers {
		if b.ZoneID != "us-east-1a" {
			t.Errorf("expected zone us-east-1a, got %s", b.ZoneID)
		}
	}

	brokers, err = registry.ListBrokers(ctx, "us-east-1b")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(brokers) != 1 {
		t.Errorf("expected 1 broker in us-east-1b, got %d", len(brokers))
	}

	brokers, err = registry.ListBrokers(ctx, "nonexistent-zone")
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(brokers) != 0 {
		t.Errorf("expected 0 brokers in nonexistent zone, got %d", len(brokers))
	}
}

func TestRegistry_Deregister(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	cfg := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              1,
		AdvertisedListeners: []string{"broker-1:9092"},
	}

	registry := NewRegistry(store, cfg)
	ctx := context.Background()

	if err := registry.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	key := keys.BrokerKeyPath("cluster-1", "broker-1")
	result, _ := store.Get(ctx, key)
	if !result.Exists {
		t.Fatal("broker should exist after registration")
	}

	if err := registry.Deregister(ctx); err != nil {
		t.Fatalf("Deregister failed: %v", err)
	}

	if registry.IsRegistered() {
		t.Error("registry should not be registered after deregistration")
	}

	result, _ = store.Get(ctx, key)
	if result.Exists {
		t.Error("broker key should not exist after deregistration")
	}
}

func TestRegistry_DeregisterIdempotent(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	cfg := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              1,
		AdvertisedListeners: []string{"broker-1:9092"},
	}

	registry := NewRegistry(store, cfg)
	ctx := context.Background()

	if err := registry.Deregister(ctx); err != nil {
		t.Fatalf("Deregister on unregistered should not fail: %v", err)
	}

	if err := registry.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if err := registry.Deregister(ctx); err != nil {
		t.Fatalf("First Deregister failed: %v", err)
	}

	if err := registry.Deregister(ctx); err != nil {
		t.Fatalf("Second Deregister should not fail: %v", err)
	}
}

func TestRegistry_GetBroker(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	cfg := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              1,
		ZoneID:              "us-east-1a",
		AdvertisedListeners: []string{"broker-1.example.com:9092"},
	}

	registry := NewRegistry(store, cfg)
	ctx := context.Background()

	info, exists, err := registry.GetBroker(ctx, "broker-1")
	if err != nil {
		t.Fatalf("GetBroker failed: %v", err)
	}
	if exists {
		t.Error("broker should not exist before registration")
	}

	if err := registry.Register(ctx); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	info, exists, err = registry.GetBroker(ctx, "broker-1")
	if err != nil {
		t.Fatalf("GetBroker failed: %v", err)
	}
	if !exists {
		t.Error("broker should exist after registration")
	}
	if info.BrokerID != "broker-1" {
		t.Errorf("BrokerID mismatch: got %q, want %q", info.BrokerID, "broker-1")
	}
	if info.ZoneID != "us-east-1a" {
		t.Errorf("ZoneID mismatch: got %q, want %q", info.ZoneID, "us-east-1a")
	}

	info, exists, err = registry.GetBroker(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetBroker failed: %v", err)
	}
	if exists {
		t.Error("nonexistent broker should not exist")
	}
}

func TestRegistry_BrokerInfoMethods(t *testing.T) {
	info := BrokerInfo{
		BrokerID:            "broker-1",
		NodeID:              1,
		AdvertisedListeners: []string{"broker-1.example.com:9092"},
	}

	if info.Host() != "broker-1.example.com" {
		t.Errorf("Host() mismatch: got %q, want %q", info.Host(), "broker-1.example.com")
	}
	if info.Port() != 9092 {
		t.Errorf("Port() mismatch: got %d, want %d", info.Port(), 9092)
	}

	infoEmpty := BrokerInfo{AdvertisedListeners: []string{}}
	if infoEmpty.Host() != "" {
		t.Errorf("Host() for empty listeners should be empty, got %q", infoEmpty.Host())
	}
	if infoEmpty.Port() != 0 {
		t.Errorf("Port() for empty listeners should be 0, got %d", infoEmpty.Port())
	}

	infoBadFormat := BrokerInfo{AdvertisedListeners: []string{"invalid-no-port"}}
	if infoBadFormat.Port() != 0 {
		t.Errorf("Port() for invalid format should be 0, got %d", infoBadFormat.Port())
	}
}

func TestRegistry_LocalBrokerInfo(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	cfg := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              42,
		ZoneID:              "us-west-2a",
		AdvertisedListeners: []string{"host1:9092", "host2:9093"},
		BuildInfo: BuildInfo{
			Version:   "1.0.0",
			GitCommit: "deadbeef",
			BuildTime: "2024-01-01T00:00:00Z",
		},
	}

	registry := NewRegistry(store, cfg)

	info := registry.BrokerInfo()
	if info.BrokerID != "broker-1" {
		t.Errorf("BrokerID mismatch: got %q, want %q", info.BrokerID, "broker-1")
	}
	if info.NodeID != 42 {
		t.Errorf("NodeID mismatch: got %d, want %d", info.NodeID, 42)
	}
	if info.ZoneID != "us-west-2a" {
		t.Errorf("ZoneID mismatch: got %q, want %q", info.ZoneID, "us-west-2a")
	}
	if len(info.AdvertisedListeners) != 2 {
		t.Errorf("AdvertisedListeners count mismatch: got %d, want %d", len(info.AdvertisedListeners), 2)
	}
	if info.BuildInfo.Version != "1.0.0" {
		t.Errorf("Version mismatch: got %q, want %q", info.BuildInfo.Version, "1.0.0")
	}
	if info.StartedAt <= 0 {
		t.Error("StartedAt should be set")
	}

	if registry.LocalBrokerID() != "broker-1" {
		t.Errorf("LocalBrokerID mismatch: got %q, want %q", registry.LocalBrokerID(), "broker-1")
	}
	if registry.ClusterID() != "cluster-1" {
		t.Errorf("ClusterID mismatch: got %q, want %q", registry.ClusterID(), "cluster-1")
	}
}

func TestParseZoneID(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		want     string
	}{
		{
			name:     "simple zone_id",
			clientID: "zone_id=us-east-1a",
			want:     "us-east-1a",
		},
		{
			name:     "zone_id with other fields",
			clientID: "app=myservice,zone_id=us-west-2b,version=1.0",
			want:     "us-west-2b",
		},
		{
			name:     "zone_id first",
			clientID: "zone_id=eu-west-1a,app=test",
			want:     "eu-west-1a",
		},
		{
			name:     "no zone_id",
			clientID: "app=myservice,version=1.0",
			want:     "",
		},
		{
			name:     "empty client_id",
			clientID: "",
			want:     "",
		},
		{
			name:     "zone_id with spaces (trimmed)",
			clientID: " zone_id=us-east-1c , app=test",
			want:     "us-east-1c",
		},
		{
			name:     "plain client_id without k=v format",
			clientID: "my-application",
			want:     "",
		},
		{
			name:     "zone_id empty value",
			clientID: "zone_id=",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseZoneID(tt.clientID)
			if got != tt.want {
				t.Errorf("ParseZoneID(%q) = %q, want %q", tt.clientID, got, tt.want)
			}
		})
	}
}

func TestRegistry_StartedAtIsSet(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	before := time.Now().UnixMilli()

	cfg := RegistryConfig{
		ClusterID:           "cluster-1",
		BrokerID:            "broker-1",
		NodeID:              1,
		AdvertisedListeners: []string{"broker-1:9092"},
	}

	registry := NewRegistry(store, cfg)

	after := time.Now().UnixMilli()

	info := registry.BrokerInfo()

	if info.StartedAt < before || info.StartedAt > after {
		t.Errorf("StartedAt %d should be between %d and %d", info.StartedAt, before, after)
	}
}

func TestRegistry_ClusterIsolation(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	ctx := context.Background()

	cfg1 := RegistryConfig{
		ClusterID:           "cluster-A",
		BrokerID:            "broker-1",
		NodeID:              1,
		AdvertisedListeners: []string{"broker-1:9092"},
	}
	cfg2 := RegistryConfig{
		ClusterID:           "cluster-B",
		BrokerID:            "broker-1",
		NodeID:              1,
		AdvertisedListeners: []string{"broker-1:9092"},
	}

	regA := NewRegistry(store, cfg1)
	regB := NewRegistry(store, cfg2)

	if err := regA.Register(ctx); err != nil {
		t.Fatalf("Register cluster-A failed: %v", err)
	}
	if err := regB.Register(ctx); err != nil {
		t.Fatalf("Register cluster-B failed: %v", err)
	}

	brokersA, err := regA.ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers cluster-A failed: %v", err)
	}
	if len(brokersA) != 1 {
		t.Errorf("cluster-A should have 1 broker, got %d", len(brokersA))
	}

	brokersB, err := regB.ListBrokers(ctx, "")
	if err != nil {
		t.Fatalf("ListBrokers cluster-B failed: %v", err)
	}
	if len(brokersB) != 1 {
		t.Errorf("cluster-B should have 1 broker, got %d", len(brokersB))
	}

	if brokersA[0].BrokerID == brokersB[0].BrokerID {
		keyA := keys.BrokerKeyPath("cluster-A", "broker-1")
		keyB := keys.BrokerKeyPath("cluster-B", "broker-1")
		if keyA == keyB {
			t.Error("cluster keys should be different")
		}
	}
}
