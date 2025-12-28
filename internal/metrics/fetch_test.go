package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewFetchMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewFetchMetricsWithRegistry(reg)

	if m.LatencyHistogram == nil {
		t.Fatal("LatencyHistogram is nil")
	}
	if m.SourceLatencyHistogram == nil {
		t.Fatal("SourceLatencyHistogram is nil")
	}
	if m.RequestsTotal == nil {
		t.Fatal("RequestsTotal is nil")
	}
	if m.SourceRequestsTotal == nil {
		t.Fatal("SourceRequestsTotal is nil")
	}
}

func TestFetchMetrics_RecordLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewFetchMetricsWithRegistry(reg)

	m.RecordLatency(0.005, true)  // 5ms success
	m.RecordLatency(0.010, true)  // 10ms success
	m.RecordLatency(0.050, false) // 50ms failure
	m.RecordLatency(0.100, false) // 100ms failure

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

func TestFetchMetrics_RecordSourceLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewFetchMetricsWithRegistry(reg)

	m.RecordSourceLatency(0.005, SourceWAL)     // 5ms from WAL
	m.RecordSourceLatency(0.010, SourceWAL)     // 10ms from WAL
	m.RecordSourceLatency(0.050, SourceParquet) // 50ms from Parquet
	m.RecordSourceLatency(0.001, SourceNone)    // 1ms empty fetch

	walHist := m.SourceLatencyHistogram.WithLabelValues(SourceWAL)
	parquetHist := m.SourceLatencyHistogram.WithLabelValues(SourceParquet)
	noneHist := m.SourceLatencyHistogram.WithLabelValues(SourceNone)

	walMetric := &dto.Metric{}
	if err := walHist.(prometheus.Metric).Write(walMetric); err != nil {
		t.Fatalf("failed to write WAL metric: %v", err)
	}
	if got := walMetric.Histogram.GetSampleCount(); got != 2 {
		t.Errorf("WAL sample count = %d, want 2", got)
	}

	parquetMetric := &dto.Metric{}
	if err := parquetHist.(prometheus.Metric).Write(parquetMetric); err != nil {
		t.Fatalf("failed to write Parquet metric: %v", err)
	}
	if got := parquetMetric.Histogram.GetSampleCount(); got != 1 {
		t.Errorf("Parquet sample count = %d, want 1", got)
	}

	noneMetric := &dto.Metric{}
	if err := noneHist.(prometheus.Metric).Write(noneMetric); err != nil {
		t.Fatalf("failed to write none metric: %v", err)
	}
	if got := noneMetric.Histogram.GetSampleCount(); got != 1 {
		t.Errorf("none sample count = %d, want 1", got)
	}

	walCounter := m.SourceRequestsTotal.WithLabelValues(SourceWAL)
	parquetCounter := m.SourceRequestsTotal.WithLabelValues(SourceParquet)
	noneCounter := m.SourceRequestsTotal.WithLabelValues(SourceNone)

	walCounterMetric := &dto.Metric{}
	if err := walCounter.Write(walCounterMetric); err != nil {
		t.Fatalf("failed to write WAL counter: %v", err)
	}
	if got := walCounterMetric.Counter.GetValue(); got != 2 {
		t.Errorf("WAL counter = %f, want 2", got)
	}

	parquetCounterMetric := &dto.Metric{}
	if err := parquetCounter.Write(parquetCounterMetric); err != nil {
		t.Fatalf("failed to write Parquet counter: %v", err)
	}
	if got := parquetCounterMetric.Counter.GetValue(); got != 1 {
		t.Errorf("Parquet counter = %f, want 1", got)
	}

	noneCounterMetric := &dto.Metric{}
	if err := noneCounter.Write(noneCounterMetric); err != nil {
		t.Fatalf("failed to write none counter: %v", err)
	}
	if got := noneCounterMetric.Counter.GetValue(); got != 1 {
		t.Errorf("none counter = %f, want 1", got)
	}
}

func TestFetchMetrics_RecordSuccessAndFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewFetchMetricsWithRegistry(reg)

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

func TestDefaultFetchLatencyBuckets(t *testing.T) {
	// Verify bucket boundaries are in ascending order
	for i := 1; i < len(DefaultFetchLatencyBuckets); i++ {
		if DefaultFetchLatencyBuckets[i] <= DefaultFetchLatencyBuckets[i-1] {
			t.Errorf("bucket %d (%f) is not greater than bucket %d (%f)",
				i, DefaultFetchLatencyBuckets[i], i-1, DefaultFetchLatencyBuckets[i-1])
		}
	}

	// Verify we have enough buckets for good percentile accuracy
	if len(DefaultFetchLatencyBuckets) < 10 {
		t.Errorf("expected at least 10 buckets for good percentile accuracy, got %d", len(DefaultFetchLatencyBuckets))
	}

	// Verify we cover sub-millisecond latencies
	if DefaultFetchLatencyBuckets[0] >= 0.001 {
		t.Errorf("first bucket should be sub-millisecond, got %f", DefaultFetchLatencyBuckets[0])
	}

	// Verify we cover up to at least 5 seconds
	lastBucket := DefaultFetchLatencyBuckets[len(DefaultFetchLatencyBuckets)-1]
	if lastBucket < 5.0 {
		t.Errorf("last bucket should be at least 5s, got %f", lastBucket)
	}
}

func TestFetchMetrics_BucketDistribution(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewFetchMetricsWithRegistry(reg)

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
