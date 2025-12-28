package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBrokerStartAndShutdown(t *testing.T) {
	// Create minimal config
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0" // Random port
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError) // Suppress logs in tests

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
		Version:   "test",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	// Start broker in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- broker.Start(ctx)
	}()

	// Wait for broker to start
	time.Sleep(200 * time.Millisecond)

	// Verify broker is listening
	if broker.tcpServer == nil || broker.tcpServer.Addr() == nil {
		t.Fatal("broker TCP server not running")
	}
	addr := broker.tcpServer.Addr().String()
	t.Logf("Broker listening on %s", addr)

	// Try to connect
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect to broker: %v", err)
	}
	conn.Close()

	// Verify health server
	if broker.healthServer == nil {
		t.Fatal("health server not running")
	}

	// Shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestBrokerRegistry(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker-123",
		NodeID:    42,
		ClusterID: "test-cluster",
		Version:   "1.0.0",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Verify broker is registered
	if broker.registry == nil {
		t.Fatal("broker registry not initialized")
	}

	if !broker.registry.IsRegistered() {
		t.Error("broker should be registered")
	}

	info := broker.registry.BrokerInfo()
	if info.BrokerID != "test-broker-123" {
		t.Errorf("expected broker ID 'test-broker-123', got %q", info.BrokerID)
	}
	if info.NodeID != 42 {
		t.Errorf("expected node ID 42, got %d", info.NodeID)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	broker.Shutdown(shutdownCtx)

	// After shutdown, broker should be deregistered
	if broker.registry.IsRegistered() {
		t.Error("broker should be deregistered after shutdown")
	}
}

func TestBrokerApiVersions(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	addr := broker.tcpServer.Addr().String()

	// Connect with franz-go client and verify ApiVersions
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Ping should succeed if ApiVersions works
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pingCancel()

	if err := client.Ping(pingCtx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	t.Log("Successfully connected to broker and performed API version negotiation")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	broker.Shutdown(shutdownCtx)
}

func TestBrokerGracefulShutdown(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	addr := broker.tcpServer.Addr().String()

	// Create a client connection
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// Verify connection works
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	if err := client.Ping(pingCtx); err != nil {
		pingCancel()
		client.Close()
		t.Fatalf("ping failed: %v", err)
	}
	pingCancel()

	// Trigger shutdown
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Shutdown should complete gracefully
	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Health check should fail after shutdown
	status := broker.healthServer.CheckHealth()
	if status.Status != "shutting_down" {
		t.Errorf("expected status 'shutting_down', got %q", status.Status)
	}

	client.Close()
}
