package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction/planner"
	"github.com/dray-io/dray/internal/logging"
)

func TestCompactorStartAndShutdown(t *testing.T) {
	cfg := testConfigWithOxia(t)
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

	healthAddr := waitForHealthServer(t, compactor, errCh)
	t.Logf("Compactor health server on %s", healthAddr)

	// Shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := compactor.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestCompactorHealthEndpoint(t *testing.T) {
	cfg := testConfigWithOxia(t)
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- compactor.Start(ctx)
	}()
	healthAddr := waitForHealthServer(t, compactor, errCh)

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
	cfg := testConfigWithOxia(t)
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- compactor.Start(ctx)
	}()
	waitForHealthServer(t, compactor, errCh)

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
	cfg := testConfigWithOxia(t)
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- compactor.Start(ctx)
	}()
	waitForHealthServer(t, compactor, errCh)

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
	cfg := testConfigWithOxia(t)
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- compactor.Start(ctx)
	}()
	waitForHealthServer(t, compactor, errCh)

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

func TestCompactorPlanCompaction_PrefersRewrite(t *testing.T) {
	wal := &fakeWalPlanner{
		plan: &planner.Result{
			StreamID: "stream-1",
		},
	}
	rewrite := &fakeRewritePlanner{
		plan: &planner.ParquetRewriteResult{
			StreamID: "stream-1",
		},
	}

	compactor := &Compactor{
		planner:        wal,
		parquetPlanner: rewrite,
	}

	plan, err := compactor.planCompaction(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("planCompaction error: %v", err)
	}
	if plan == nil || plan.Kind != planKindParquetRewrite {
		t.Fatalf("expected parquet rewrite plan, got %#v", plan)
	}
	if rewrite.calls != 1 {
		t.Fatalf("expected rewrite planner to be called, got %d", rewrite.calls)
	}
	if wal.calls != 0 {
		t.Fatalf("expected WAL planner not to be called, got %d", wal.calls)
	}
}

func TestCompactorPlanCompaction_FallsBackToWal(t *testing.T) {
	wal := &fakeWalPlanner{
		plan: &planner.Result{
			StreamID: "stream-1",
		},
	}
	rewrite := &fakeRewritePlanner{}

	compactor := &Compactor{
		planner:        wal,
		parquetPlanner: rewrite,
	}

	plan, err := compactor.planCompaction(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("planCompaction error: %v", err)
	}
	if plan == nil || plan.Kind != planKindWAL {
		t.Fatalf("expected WAL plan, got %#v", plan)
	}
	if rewrite.calls != 1 {
		t.Fatalf("expected rewrite planner to be called, got %d", rewrite.calls)
	}
	if wal.calls != 1 {
		t.Fatalf("expected WAL planner to be called, got %d", wal.calls)
	}
}

type fakeWalPlanner struct {
	plan  *planner.Result
	err   error
	calls int
}

func (f *fakeWalPlanner) Plan(_ context.Context, _ string) (*planner.Result, error) {
	f.calls++
	return f.plan, f.err
}

type fakeRewritePlanner struct {
	plan  *planner.ParquetRewriteResult
	err   error
	calls int
}

func (f *fakeRewritePlanner) Plan(_ context.Context, _ string) (*planner.ParquetRewriteResult, error) {
	f.calls++
	return f.plan, f.err
}
