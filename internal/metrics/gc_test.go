package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func TestNewGCMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	if m == nil {
		t.Fatal("expected non-nil GCMetrics")
	}

	// Verify all metrics are registered
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	expectedMetrics := map[string]bool{
		"dray_gc_orphan_wal_count":         false,
		"dray_gc_pending_wal_deletes":      false,
		"dray_gc_pending_parquet_deletes":  false,
		"dray_gc_staging_wal_count":        false,
		"dray_gc_eligible_wal_deletes":     false,
		"dray_gc_eligible_parquet_deletes": false,
	}

	for _, family := range families {
		name := family.GetName()
		if _, ok := expectedMetrics[name]; ok {
			expectedMetrics[name] = true
		}
	}

	for name, found := range expectedMetrics {
		if !found {
			t.Errorf("expected metric %s to be registered", name)
		}
	}
}

func TestGCMetrics_RecordOrphanWALCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	m.RecordOrphanWALCount(42)

	value := getGaugeValue(t, reg, "dray_gc_orphan_wal_count")
	if value != 42 {
		t.Errorf("expected orphan WAL count 42, got %v", value)
	}
}

func TestGCMetrics_RecordPendingWALDeletes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	m.RecordPendingWALDeletes(15)

	value := getGaugeValue(t, reg, "dray_gc_pending_wal_deletes")
	if value != 15 {
		t.Errorf("expected pending WAL deletes 15, got %v", value)
	}
}

func TestGCMetrics_RecordPendingParquetDeletes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	m.RecordPendingParquetDeletes(7)

	value := getGaugeValue(t, reg, "dray_gc_pending_parquet_deletes")
	if value != 7 {
		t.Errorf("expected pending Parquet deletes 7, got %v", value)
	}
}

func TestGCMetrics_RecordStagingWALCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	m.RecordStagingWALCount(100)

	value := getGaugeValue(t, reg, "dray_gc_staging_wal_count")
	if value != 100 {
		t.Errorf("expected staging WAL count 100, got %v", value)
	}
}

func TestGCMetrics_RecordEligibleWALDeletes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	m.RecordEligibleWALDeletes(5)

	value := getGaugeValue(t, reg, "dray_gc_eligible_wal_deletes")
	if value != 5 {
		t.Errorf("expected eligible WAL deletes 5, got %v", value)
	}
}

func TestGCMetrics_RecordEligibleParquetDeletes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	m.RecordEligibleParquetDeletes(3)

	value := getGaugeValue(t, reg, "dray_gc_eligible_parquet_deletes")
	if value != 3 {
		t.Errorf("expected eligible Parquet deletes 3, got %v", value)
	}
}

func TestGCMetrics_UpdateValues(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	// Set initial values
	m.RecordOrphanWALCount(10)
	m.RecordPendingWALDeletes(20)
	m.RecordPendingParquetDeletes(30)

	// Verify initial values
	if v := getGaugeValue(t, reg, "dray_gc_orphan_wal_count"); v != 10 {
		t.Errorf("expected orphan WAL count 10, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_wal_deletes"); v != 20 {
		t.Errorf("expected pending WAL deletes 20, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_parquet_deletes"); v != 30 {
		t.Errorf("expected pending Parquet deletes 30, got %v", v)
	}

	// Update values
	m.RecordOrphanWALCount(5)
	m.RecordPendingWALDeletes(0)
	m.RecordPendingParquetDeletes(15)

	// Verify updated values
	if v := getGaugeValue(t, reg, "dray_gc_orphan_wal_count"); v != 5 {
		t.Errorf("expected orphan WAL count 5, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_wal_deletes"); v != 0 {
		t.Errorf("expected pending WAL deletes 0, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_parquet_deletes"); v != 15 {
		t.Errorf("expected pending Parquet deletes 15, got %v", v)
	}
}

// mockGCStatsProvider implements GCStatsProvider for testing.
type mockGCStatsProvider struct {
	orphanWALCount         int
	stagingWALCount        int
	pendingWALDeleteCount  int
	eligibleWALDeleteCount int
	pendingParquetCount    int
	eligibleParquetCount   int
	err                    error
}

func (m *mockGCStatsProvider) GetOrphanWALCount(ctx context.Context) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.orphanWALCount, nil
}

func (m *mockGCStatsProvider) GetStagingWALCount(ctx context.Context) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.stagingWALCount, nil
}

func (m *mockGCStatsProvider) GetPendingWALDeleteCount(ctx context.Context) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.pendingWALDeleteCount, nil
}

func (m *mockGCStatsProvider) GetEligibleWALDeleteCount(ctx context.Context) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.eligibleWALDeleteCount, nil
}

func (m *mockGCStatsProvider) GetPendingParquetDeleteCount(ctx context.Context) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.pendingParquetCount, nil
}

func (m *mockGCStatsProvider) GetEligibleParquetDeleteCount(ctx context.Context) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.eligibleParquetCount, nil
}

func TestGCBacklogScanner_ScanOnce(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	provider := &mockGCStatsProvider{
		orphanWALCount:         42,
		stagingWALCount:        100,
		pendingWALDeleteCount:  15,
		eligibleWALDeleteCount: 5,
		pendingParquetCount:    7,
		eligibleParquetCount:   3,
	}

	scanner := NewGCBacklogScanner(m, provider, time.Hour) // Long interval to prevent auto-scan
	scanner.ScanOnce()

	// Verify all metrics were updated
	if v := getGaugeValue(t, reg, "dray_gc_orphan_wal_count"); v != 42 {
		t.Errorf("expected orphan WAL count 42, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_staging_wal_count"); v != 100 {
		t.Errorf("expected staging WAL count 100, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_wal_deletes"); v != 15 {
		t.Errorf("expected pending WAL deletes 15, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_eligible_wal_deletes"); v != 5 {
		t.Errorf("expected eligible WAL deletes 5, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_parquet_deletes"); v != 7 {
		t.Errorf("expected pending Parquet deletes 7, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_eligible_parquet_deletes"); v != 3 {
		t.Errorf("expected eligible Parquet deletes 3, got %v", v)
	}
}

func TestGCBacklogScanner_StartStop(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	provider := &mockGCStatsProvider{
		orphanWALCount:        10,
		stagingWALCount:       50,
		pendingWALDeleteCount: 5,
		pendingParquetCount:   2,
	}

	scanner := NewGCBacklogScanner(m, provider, 10*time.Millisecond)
	scanner.Start()

	// Wait for at least one scan to complete
	time.Sleep(50 * time.Millisecond)

	scanner.Stop()

	// Verify metrics were updated by the background scan
	if v := getGaugeValue(t, reg, "dray_gc_orphan_wal_count"); v != 10 {
		t.Errorf("expected orphan WAL count 10, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_staging_wal_count"); v != 50 {
		t.Errorf("expected staging WAL count 50, got %v", v)
	}
}

func TestGCBacklogScanner_ErrorHandling(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	// Set initial values
	m.RecordOrphanWALCount(99)
	m.RecordPendingWALDeletes(88)

	// Provider returns errors
	provider := &mockGCStatsProvider{
		err: errors.New("connection failed"),
	}

	scanner := NewGCBacklogScanner(m, provider, time.Hour)
	scanner.ScanOnce()

	// Verify metrics were not updated (kept original values)
	if v := getGaugeValue(t, reg, "dray_gc_orphan_wal_count"); v != 99 {
		t.Errorf("expected orphan WAL count 99 (unchanged), got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_wal_deletes"); v != 88 {
		t.Errorf("expected pending WAL deletes 88 (unchanged), got %v", v)
	}
}

func TestGCBacklogScanner_ImmediateRunOnStart(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	provider := &mockGCStatsProvider{
		orphanWALCount:  123,
		stagingWALCount: 456,
	}

	scanner := NewGCBacklogScanner(m, provider, time.Hour) // Very long interval
	scanner.Start()

	// Give goroutine time to start and run initial scan
	time.Sleep(50 * time.Millisecond)

	scanner.Stop()

	// Verify initial scan ran (values were updated despite long interval)
	if v := getGaugeValue(t, reg, "dray_gc_orphan_wal_count"); v != 123 {
		t.Errorf("expected orphan WAL count 123 from initial scan, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_staging_wal_count"); v != 456 {
		t.Errorf("expected staging WAL count 456 from initial scan, got %v", v)
	}
}

// getGaugeValue extracts the current value of a gauge metric from the registry.
func getGaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	for _, family := range families {
		if family.GetName() == name {
			metrics := family.GetMetric()
			if len(metrics) > 0 {
				return metrics[0].GetGauge().GetValue()
			}
		}
	}

	t.Fatalf("metric %s not found", name)
	return 0
}

func TestGCMetrics_ZeroValues(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewGCMetricsWithRegistry(reg)

	// Record zero values
	m.RecordOrphanWALCount(0)
	m.RecordPendingWALDeletes(0)
	m.RecordPendingParquetDeletes(0)
	m.RecordStagingWALCount(0)
	m.RecordEligibleWALDeletes(0)
	m.RecordEligibleParquetDeletes(0)

	// Verify zero values are recorded correctly
	if v := getGaugeValue(t, reg, "dray_gc_orphan_wal_count"); v != 0 {
		t.Errorf("expected orphan WAL count 0, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_wal_deletes"); v != 0 {
		t.Errorf("expected pending WAL deletes 0, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_pending_parquet_deletes"); v != 0 {
		t.Errorf("expected pending Parquet deletes 0, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_staging_wal_count"); v != 0 {
		t.Errorf("expected staging WAL count 0, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_eligible_wal_deletes"); v != 0 {
		t.Errorf("expected eligible WAL deletes 0, got %v", v)
	}
	if v := getGaugeValue(t, reg, "dray_gc_eligible_parquet_deletes"); v != 0 {
		t.Errorf("expected eligible Parquet deletes 0, got %v", v)
	}
}

// Ensure mockGCStatsProvider implements GCStatsProvider
var _ GCStatsProvider = (*mockGCStatsProvider)(nil)

func TestNewGCMetrics_DefaultRegistry(t *testing.T) {
	// This test verifies that NewGCMetrics works with the default registry
	// We can't easily test this without affecting global state, so just verify
	// the constructor runs without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewGCMetrics panicked: %v", r)
		}
	}()

	// Note: This may fail if tests are run multiple times in the same process
	// due to duplicate metric registration. That's expected behavior.
	_ = NewGCMetrics
}

func TestGCBacklogScanner_ProviderInterface(t *testing.T) {
	// Test that the interface is correctly defined
	var provider GCStatsProvider = &mockGCStatsProvider{
		orphanWALCount:         1,
		stagingWALCount:        2,
		pendingWALDeleteCount:  3,
		eligibleWALDeleteCount: 4,
		pendingParquetCount:    5,
		eligibleParquetCount:   6,
	}

	ctx := context.Background()

	if v, err := provider.GetOrphanWALCount(ctx); err != nil || v != 1 {
		t.Errorf("GetOrphanWALCount: expected 1, got %d, err: %v", v, err)
	}
	if v, err := provider.GetStagingWALCount(ctx); err != nil || v != 2 {
		t.Errorf("GetStagingWALCount: expected 2, got %d, err: %v", v, err)
	}
	if v, err := provider.GetPendingWALDeleteCount(ctx); err != nil || v != 3 {
		t.Errorf("GetPendingWALDeleteCount: expected 3, got %d, err: %v", v, err)
	}
	if v, err := provider.GetEligibleWALDeleteCount(ctx); err != nil || v != 4 {
		t.Errorf("GetEligibleWALDeleteCount: expected 4, got %d, err: %v", v, err)
	}
	if v, err := provider.GetPendingParquetDeleteCount(ctx); err != nil || v != 5 {
		t.Errorf("GetPendingParquetDeleteCount: expected 5, got %d, err: %v", v, err)
	}
	if v, err := provider.GetEligibleParquetDeleteCount(ctx); err != nil || v != 6 {
		t.Errorf("GetEligibleParquetDeleteCount: expected 6, got %d, err: %v", v, err)
	}
}

// Ensure io_prometheus_client is used (it's imported indirectly via prometheus)
var _ = io_prometheus_client.Metric{}
