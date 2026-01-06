package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewProduceMetrics(t *testing.T) {
	// Create a custom registry to avoid polluting the default registry
	reg := prometheus.NewRegistry()
	m := NewProduceMetricsWithRegistry(reg)

	if m.LatencyHistogram == nil {
		t.Fatal("LatencyHistogram is nil")
	}
	if m.RequestsTotal == nil {
		t.Fatal("RequestsTotal is nil")
	}
	if m.MessagesInTotal == nil {
		t.Fatal("MessagesInTotal is nil")
	}
}

func TestProduceMetrics_RecordLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewProduceMetricsWithRegistry(reg)

	// Record some latencies
	m.RecordLatency(0.005, true)  // 5ms success
	m.RecordLatency(0.010, true)  // 10ms success
	m.RecordLatency(0.050, false) // 50ms failure
	m.RecordLatency(0.100, false) // 100ms failure

	// Verify histograms
	successHist := m.LatencyHistogram.WithLabelValues(StatusSuccess)
	failureHist := m.LatencyHistogram.WithLabelValues(StatusFailure)

	successMetric := &dto.Metric{}
	if err := successHist.(prometheus.Metric).Write(successMetric); err != nil {
		t.Fatalf("failed to write success metric: %v", err)
	}
	if got := successMetric.Histogram.GetSampleCount(); got != 2 {
		t.Errorf("success sample count = %d, want 2", got)
	}

	failureMetric := &dto.Metric{}
	if err := failureHist.(prometheus.Metric).Write(failureMetric); err != nil {
		t.Fatalf("failed to write failure metric: %v", err)
	}
	if got := failureMetric.Histogram.GetSampleCount(); got != 2 {
		t.Errorf("failure sample count = %d, want 2", got)
	}

	// Verify counters
	successCounter := m.RequestsTotal.WithLabelValues(StatusSuccess)
	failureCounter := m.RequestsTotal.WithLabelValues(StatusFailure)

	successCounterMetric := &dto.Metric{}
	if err := successCounter.Write(successCounterMetric); err != nil {
		t.Fatalf("failed to write success counter: %v", err)
	}
	if got := successCounterMetric.Counter.GetValue(); got != 2 {
		t.Errorf("success counter = %f, want 2", got)
	}

	failureCounterMetric := &dto.Metric{}
	if err := failureCounter.Write(failureCounterMetric); err != nil {
		t.Fatalf("failed to write failure counter: %v", err)
	}
	if got := failureCounterMetric.Counter.GetValue(); got != 2 {
		t.Errorf("failure counter = %f, want 2", got)
	}
}

func TestProduceMetrics_RecordSuccessAndFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewProduceMetricsWithRegistry(reg)

	m.RecordSuccess(0.001)
	m.RecordSuccess(0.002)
	m.RecordSuccess(0.003)
	m.RecordFailure(0.010)

	successCounter := m.RequestsTotal.WithLabelValues(StatusSuccess)
	failureCounter := m.RequestsTotal.WithLabelValues(StatusFailure)

	successMetric := &dto.Metric{}
	if err := successCounter.Write(successMetric); err != nil {
		t.Fatalf("failed to write success counter: %v", err)
	}
	if got := successMetric.Counter.GetValue(); got != 3 {
		t.Errorf("success counter = %f, want 3", got)
	}

	failureMetric := &dto.Metric{}
	if err := failureCounter.Write(failureMetric); err != nil {
		t.Fatalf("failed to write failure counter: %v", err)
	}
	if got := failureMetric.Counter.GetValue(); got != 1 {
		t.Errorf("failure counter = %f, want 1", got)
	}
}

func TestProduceMetrics_RecordMessages(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewProduceMetricsWithRegistry(reg)

	m.RecordMessages(3)
	m.RecordMessages(2)

	counterMetric := &dto.Metric{}
	if err := m.MessagesInTotal.(prometheus.Metric).Write(counterMetric); err != nil {
		t.Fatalf("failed to write messages counter: %v", err)
	}
	if got := counterMetric.Counter.GetValue(); got != 5 {
		t.Errorf("messages counter = %f, want 5", got)
	}
}

func TestDefaultProduceLatencyBuckets(t *testing.T) {
	// Verify bucket boundaries are in ascending order
	for i := 1; i < len(DefaultProduceLatencyBuckets); i++ {
		if DefaultProduceLatencyBuckets[i] <= DefaultProduceLatencyBuckets[i-1] {
			t.Errorf("bucket %d (%f) is not greater than bucket %d (%f)",
				i, DefaultProduceLatencyBuckets[i], i-1, DefaultProduceLatencyBuckets[i-1])
		}
	}

	// Verify we have enough buckets for good percentile accuracy
	if len(DefaultProduceLatencyBuckets) < 10 {
		t.Errorf("expected at least 10 buckets for good percentile accuracy, got %d", len(DefaultProduceLatencyBuckets))
	}

	// Verify we cover sub-millisecond latencies
	if DefaultProduceLatencyBuckets[0] >= 0.001 {
		t.Errorf("first bucket should be sub-millisecond, got %f", DefaultProduceLatencyBuckets[0])
	}

	// Verify we cover up to at least 5 seconds
	lastBucket := DefaultProduceLatencyBuckets[len(DefaultProduceLatencyBuckets)-1]
	if lastBucket < 5.0 {
		t.Errorf("last bucket should be at least 5s, got %f", lastBucket)
	}
}

func TestProduceMetrics_BucketDistribution(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewProduceMetricsWithRegistry(reg)

	// Record values that should fall into different buckets
	testCases := []float64{
		0.00005, // 0.05ms - should be in first bucket (0.0001)
		0.0003,  // 0.3ms - should be in second bucket (0.0005)
		0.008,   // 8ms - should be around 10ms bucket
		0.03,    // 30ms - should be around 50ms bucket
		0.8,     // 800ms - should be around 1s bucket
	}

	for _, v := range testCases {
		m.RecordSuccess(v)
	}

	hist := m.LatencyHistogram.WithLabelValues(StatusSuccess)
	metric := &dto.Metric{}
	if err := hist.(prometheus.Metric).Write(metric); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}

	if got := metric.Histogram.GetSampleCount(); got != uint64(len(testCases)) {
		t.Errorf("sample count = %d, want %d", got, len(testCases))
	}

	// Verify samples are distributed across buckets
	buckets := metric.Histogram.GetBucket()
	if len(buckets) == 0 {
		t.Fatal("no buckets in histogram")
	}

	// At least some buckets should have counts
	hasNonZeroBucket := false
	for _, b := range buckets {
		if b.GetCumulativeCount() > 0 {
			hasNonZeroBucket = true
			break
		}
	}
	if !hasNonZeroBucket {
		t.Error("expected at least one bucket with counts")
	}
}
