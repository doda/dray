package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewOxiaMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOxiaMetricsWithRegistry(reg)

	if m.LatencyHistogram == nil {
		t.Error("expected LatencyHistogram to be non-nil")
	}
	if m.RequestsTotal == nil {
		t.Error("expected RequestsTotal to be non-nil")
	}
	if m.RetriesTotal == nil {
		t.Error("expected RetriesTotal to be non-nil")
	}

	// Initialize metrics so they appear in Gather (Prometheus only shows metrics with observations)
	m.RecordOperation(OpGet, 0.001, true)
	m.RecordRetry(OpGet)

	// Verify metrics are registered
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// We expect 3 metric families: latency histogram, requests total, retries total
	expectedNames := map[string]bool{
		"dray_oxia_operation_latency_seconds": false,
		"dray_oxia_operations_total":          false,
		"dray_oxia_retries_total":             false,
	}

	for _, mf := range mfs {
		if _, ok := expectedNames[mf.GetName()]; ok {
			expectedNames[mf.GetName()] = true
		}
	}

	for name, found := range expectedNames {
		if !found {
			t.Errorf("expected metric %s to be registered", name)
		}
	}
}

func TestOxiaMetrics_RecordOperation(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOxiaMetricsWithRegistry(reg)

	tests := []struct {
		operation string
		duration  float64
		success   bool
		wantLabel string
	}{
		{OpGet, 0.001, true, "success"},
		{OpGet, 0.002, false, "failure"},
		{OpPut, 0.001, true, "success"},
		{OpDelete, 0.001, true, "success"},
		{OpList, 0.001, true, "success"},
		{OpTxn, 0.001, true, "success"},
		{OpPutEphemeral, 0.001, true, "success"},
	}

	for _, tt := range tests {
		m.RecordOperation(tt.operation, tt.duration, tt.success)
	}

	// Check that counters were incremented correctly
	// Get had 2 calls (1 success, 1 failure)
	getSuccessCount := testutil.ToFloat64(m.RequestsTotal.WithLabelValues(OpGet, StatusSuccess))
	if getSuccessCount != 1 {
		t.Errorf("expected get success count 1, got %v", getSuccessCount)
	}

	getFailureCount := testutil.ToFloat64(m.RequestsTotal.WithLabelValues(OpGet, StatusFailure))
	if getFailureCount != 1 {
		t.Errorf("expected get failure count 1, got %v", getFailureCount)
	}

	// Other operations had 1 success each
	for _, op := range []string{OpPut, OpDelete, OpList, OpTxn, OpPutEphemeral} {
		count := testutil.ToFloat64(m.RequestsTotal.WithLabelValues(op, StatusSuccess))
		if count != 1 {
			t.Errorf("expected %s success count 1, got %v", op, count)
		}
	}
}

func TestOxiaMetrics_RecordRetry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOxiaMetricsWithRegistry(reg)

	// Record retries for different operations
	m.RecordRetry(OpGet)
	m.RecordRetry(OpGet)
	m.RecordRetry(OpPut)
	m.RecordRetry(OpTxn)
	m.RecordRetry(OpTxn)
	m.RecordRetry(OpTxn)

	// Check counts
	getRetries := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(OpGet))
	if getRetries != 2 {
		t.Errorf("expected get retries 2, got %v", getRetries)
	}

	putRetries := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(OpPut))
	if putRetries != 1 {
		t.Errorf("expected put retries 1, got %v", putRetries)
	}

	txnRetries := testutil.ToFloat64(m.RetriesTotal.WithLabelValues(OpTxn))
	if txnRetries != 3 {
		t.Errorf("expected txn retries 3, got %v", txnRetries)
	}
}

func TestOxiaMetrics_ConvenienceMethods(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOxiaMetricsWithRegistry(reg)

	// Test convenience methods
	m.RecordGet(0.001, true)
	m.RecordGet(0.002, false)
	m.RecordPut(0.001, true)
	m.RecordDelete(0.001, true)
	m.RecordList(0.001, true)
	m.RecordTxn(0.001, true)
	m.RecordPutEphemeral(0.001, true)

	// Check that counters were incremented
	type check struct {
		op      string
		status  string
		want    float64
	}

	checks := []check{
		{OpGet, StatusSuccess, 1},
		{OpGet, StatusFailure, 1},
		{OpPut, StatusSuccess, 1},
		{OpDelete, StatusSuccess, 1},
		{OpList, StatusSuccess, 1},
		{OpTxn, StatusSuccess, 1},
		{OpPutEphemeral, StatusSuccess, 1},
	}

	for _, c := range checks {
		count := testutil.ToFloat64(m.RequestsTotal.WithLabelValues(c.op, c.status))
		if count != c.want {
			t.Errorf("expected %s/%s count %v, got %v", c.op, c.status, c.want, count)
		}
	}
}

func TestOxiaMetrics_LatencyHistogramBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewOxiaMetricsWithRegistry(reg)

	// Record operations with different latencies
	latencies := []float64{0.0001, 0.001, 0.01, 0.1, 1.0}
	for _, lat := range latencies {
		m.RecordGet(lat, true)
	}

	// Verify histogram has correct count
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "dray_oxia_operation_latency_seconds" {
			found = true
			for _, metric := range mf.GetMetric() {
				hist := metric.GetHistogram()
				if hist != nil && hist.GetSampleCount() == uint64(len(latencies)) {
					// Verify bucket count
					buckets := hist.GetBucket()
					if len(buckets) != len(DefaultOxiaLatencyBuckets) {
						t.Errorf("expected %d buckets, got %d", len(DefaultOxiaLatencyBuckets), len(buckets))
					}
				}
			}
		}
	}

	if !found {
		t.Error("expected to find dray_oxia_operation_latency_seconds metric")
	}
}

func TestDefaultOxiaLatencyBuckets(t *testing.T) {
	// Verify buckets are sorted
	for i := 1; i < len(DefaultOxiaLatencyBuckets); i++ {
		if DefaultOxiaLatencyBuckets[i] <= DefaultOxiaLatencyBuckets[i-1] {
			t.Errorf("buckets not sorted: %v >= %v at index %d",
				DefaultOxiaLatencyBuckets[i-1], DefaultOxiaLatencyBuckets[i], i)
		}
	}

	// Verify reasonable range (0.1ms to 5s)
	if DefaultOxiaLatencyBuckets[0] > 0.001 {
		t.Error("smallest bucket should be <= 1ms")
	}
	if DefaultOxiaLatencyBuckets[len(DefaultOxiaLatencyBuckets)-1] < 1.0 {
		t.Error("largest bucket should be >= 1s")
	}
}
