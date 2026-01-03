package main

import (
	"testing"
	"time"

	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/metadata/oxia"
)

// testConfigWithOxia creates a test configuration with an embedded Oxia server.
func testConfigWithOxia(t *testing.T) *config.Config {
	t.Helper()

	server := oxia.StartTestServer(t)

	cfg := config.Default()
	cfg.Metadata.OxiaEndpoint = server.Addr()
	cfg.Metadata.OxiaNamespace = "default" // Use default namespace from embedded Oxia server
	cfg.ObjectStore.Endpoint = "http://localhost:9000"
	cfg.ObjectStore.Bucket = "test-bucket"
	cfg.ObjectStore.AccessKey = "minioadmin"
	cfg.ObjectStore.SecretKey = "minioadmin"
	cfg.Iceberg.Enabled = false

	return cfg
}

// waitForBrokerStart waits for the broker to start listening.
func waitForBrokerStart(t *testing.T, b *Broker, errCh <-chan error) string {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("broker failed to start: %v", err)
		default:
		}

		if b.tcpServer != nil {
			addr := b.tcpServer.Addr()
			if addr != nil {
				return addr.String()
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timeout waiting for broker to start")
	return ""
}

// waitForHealthServer waits for the compactor's health server to be ready.
func waitForHealthServer(t *testing.T, c *Compactor, errCh <-chan error) string {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("compactor failed to start: %v", err)
		default:
		}

		if c.healthServer != nil {
			addr := c.healthServer.Addr()
			if addr != "" {
				return addr
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timeout waiting for health server to start")
	return ""
}
