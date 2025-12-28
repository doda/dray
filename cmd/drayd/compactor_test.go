package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/logging"
)

func TestCompactorStartAndShutdown(t *testing.T) {
	cfg := config.Default()
	cfg.Observability.MetricsAddr = "127.0.0.1:0"
	cfg.Compaction.Enabled = true

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := CompactorOptions{
		Config:      cfg,
		Logger:      logger,
		CompactorID: "test-compactor",
		Version:     "test",
	}

	compactor, err := NewCompactor(opts)
	if err != nil {
		t.Fatalf("failed to create compactor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- compactor.Start(ctx)
	}()

	// Wait for compactor to start
	time.Sleep(200 * time.Millisecond)

	// Verify health server is running
	if compactor.healthServer == nil {
		t.Fatal("health server not running")
	}
	healthAddr := compactor.healthServer.Addr()
	if healthAddr == "" {
		t.Fatal("health server has no address")
	}
	t.Logf("Compactor health server on %s", healthAddr)

	// Shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := compactor.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestCompactorHealthEndpoint(t *testing.T) {
	cfg := config.Default()
	cfg.Observability.MetricsAddr = "127.0.0.1:0"
	cfg.Compaction.Enabled = true

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := CompactorOptions{
		Config:      cfg,
		Logger:      logger,
		CompactorID: "test-compactor-health",
		Version:     "test",
	}

	compactor, err := NewCompactor(opts)
	if err != nil {
		t.Fatalf("failed to create compactor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go compactor.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	healthAddr := compactor.healthServer.Addr()

	// Test health endpoint
	resp, err := http.Get("http://" + healthAddr + "/healthz")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	compactor.Shutdown(shutdownCtx)
}

func TestCompactorGracefulShutdown(t *testing.T) {
	cfg := config.Default()
	cfg.Observability.MetricsAddr = "127.0.0.1:0"
	cfg.Compaction.Enabled = true

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := CompactorOptions{
		Config:      cfg,
		Logger:      logger,
		CompactorID: "test-compactor-shutdown",
		Version:     "test",
	}

	compactor, err := NewCompactor(opts)
	if err != nil {
		t.Fatalf("failed to create compactor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go compactor.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Trigger shutdown via context cancellation
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := compactor.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Verify health status after shutdown
	status := compactor.healthServer.CheckHealth()
	if status.Status != "shutting_down" {
		t.Errorf("expected status 'shutting_down', got %q", status.Status)
	}
}

func TestCompactorLockManager(t *testing.T) {
	cfg := config.Default()
	cfg.Observability.MetricsAddr = "127.0.0.1:0"
	cfg.Compaction.Enabled = true

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := CompactorOptions{
		Config:      cfg,
		Logger:      logger,
		CompactorID: "test-compactor-locks",
		Version:     "test",
	}

	compactor, err := NewCompactor(opts)
	if err != nil {
		t.Fatalf("failed to create compactor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go compactor.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Verify lock manager is initialized
	if compactor.lockManager == nil {
		t.Fatal("lock manager not initialized")
	}

	if compactor.lockManager.CompactorID() != "test-compactor-locks" {
		t.Errorf("expected compactor ID 'test-compactor-locks', got %q", compactor.lockManager.CompactorID())
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	compactor.Shutdown(shutdownCtx)
}

func TestCompactorSagaManager(t *testing.T) {
	cfg := config.Default()
	cfg.Observability.MetricsAddr = "127.0.0.1:0"
	cfg.Compaction.Enabled = true

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := CompactorOptions{
		Config:      cfg,
		Logger:      logger,
		CompactorID: "test-compactor-saga",
		Version:     "test",
	}

	compactor, err := NewCompactor(opts)
	if err != nil {
		t.Fatalf("failed to create compactor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go compactor.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Verify saga manager is initialized
	if compactor.sagaManager == nil {
		t.Fatal("saga manager not initialized")
	}

	if compactor.sagaManager.CompactorID() != "test-compactor-saga" {
		t.Errorf("expected compactor ID 'test-compactor-saga', got %q", compactor.sagaManager.CompactorID())
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	compactor.Shutdown(shutdownCtx)
}
