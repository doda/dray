package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewWALMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWALMetricsWithRegistry(reg)

	if m.SizeHistogram == nil {
		t.Fatal("SizeHistogram is nil")
	}
	if m.FlushLatencyHistogram == nil {
		t.Fatal("FlushLatencyHistogram is nil")
	}
	if m.ObjectsCreatedTotal == nil {
		t.Fatal("ObjectsCreatedTotal is nil")
	}
}

func TestWALMetrics_RecordFlush(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWALMetricsWithRegistry(reg)

	// Record some flush operations
	m.RecordFlush(1024, 0.005)     // 1KB, 5ms
	m.RecordFlush(1048576, 0.050)  // 1MB, 50ms
	m.RecordFlush(4194304, 0.100)  // 4MB, 100ms

	// Verify size histogram
	sizeMetric := &dto.Metric{}
	if err := m.SizeHistogram.Write(sizeMetric); err != nil {
		t.Fatalf("failed to write size metric: %v", err)
	}
	if got := sizeMetric.Histogram.GetSampleCount(); got != 3 {
		t.Errorf("size sample count = %d, want 3", got)
	}

	// Verify latency histogram
	latencyMetric := &dto.Metric{}
	if err := m.FlushLatencyHistogram.Write(latencyMetric); err != nil {
		t.Fatalf("failed to write latency metric: %v", err)
	}
	if got := latencyMetric.Histogram.GetSampleCount(); got != 3 {
		t.Errorf("latency sample count = %d, want 3", got)
	}

	// Verify objects created counter
	counterMetric := &dto.Metric{}
	if err := m.ObjectsCreatedTotal.Write(counterMetric); err != nil {
		t.Fatalf("failed to write counter: %v", err)
	}
	if got := counterMetric.Counter.GetValue(); got != 3 {
		t.Errorf("objects created = %f, want 3", got)
	}
}

func TestWALMetrics_RecordSize(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWALMetricsWithRegistry(reg)

	m.RecordSize(1024)
	m.RecordSize(2048)
	m.RecordSize(4096)

	sizeMetric := &dto.Metric{}
	if err := m.SizeHistogram.Write(sizeMetric); err != nil {
		t.Fatalf("failed to write size metric: %v", err)
	}
	if got := sizeMetric.Histogram.GetSampleCount(); got != 3 {
		t.Errorf("size sample count = %d, want 3", got)
	}
}

func TestWALMetrics_RecordLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWALMetricsWithRegistry(reg)

	m.RecordLatency(0.001)
	m.RecordLatency(0.010)
	m.RecordLatency(0.100)

	latencyMetric := &dto.Metric{}
	if err := m.FlushLatencyHistogram.Write(latencyMetric); err != nil {
		t.Fatalf("failed to write latency metric: %v", err)
	}
	if got := latencyMetric.Histogram.GetSampleCount(); got != 3 {
		t.Errorf("latency sample count = %d, want 3", got)
	}
}

func TestWALMetrics_IncObjectsCreated(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWALMetricsWithRegistry(reg)

	m.IncObjectsCreated()
	m.IncObjectsCreated()
	m.IncObjectsCreated()
	m.IncObjectsCreated()
	m.IncObjectsCreated()

	counterMetric := &dto.Metric{}
	if err := m.ObjectsCreatedTotal.Write(counterMetric); err != nil {
		t.Fatalf("failed to write counter: %v", err)
	}
	if got := counterMetric.Counter.GetValue(); got != 5 {
		t.Errorf("objects created = %f, want 5", got)
	}
}

func TestDefaultWALSizeBuckets(t *testing.T) {
	// Verify bucket boundaries are in ascending order
	for i := 1; i < len(DefaultWALSizeBuckets); i++ {
		if DefaultWALSizeBuckets[i] <= DefaultWALSizeBuckets[i-1] {
			t.Errorf("bucket %d (%f) is not greater than bucket %d (%f)",
				i, DefaultWALSizeBuckets[i], i-1, DefaultWALSizeBuckets[i-1])
		}
	}

	// Verify we have enough buckets for good distribution
	if len(DefaultWALSizeBuckets) < 10 {
		t.Errorf("expected at least 10 buckets for good distribution, got %d", len(DefaultWALSizeBuckets))
	}

	// Verify first bucket is 1KB
	if DefaultWALSizeBuckets[0] != 1024 {
		t.Errorf("first bucket should be 1KB (1024), got %f", DefaultWALSizeBuckets[0])
	}

	// Verify we cover at least 64MB
	lastBucket := DefaultWALSizeBuckets[len(DefaultWALSizeBuckets)-1]
	if lastBucket < 64*1024*1024 {
		t.Errorf("last bucket should be at least 64MB, got %f", lastBucket)
	}
}

func TestDefaultWALFlushLatencyBuckets(t *testing.T) {
	// Verify bucket boundaries are in ascending order
	for i := 1; i < len(DefaultWALFlushLatencyBuckets); i++ {
		if DefaultWALFlushLatencyBuckets[i] <= DefaultWALFlushLatencyBuckets[i-1] {
			t.Errorf("bucket %d (%f) is not greater than bucket %d (%f)",
				i, DefaultWALFlushLatencyBuckets[i], i-1, DefaultWALFlushLatencyBuckets[i-1])
		}
	}

	// Verify we have enough buckets
	if len(DefaultWALFlushLatencyBuckets) < 10 {
		t.Errorf("expected at least 10 buckets, got %d", len(DefaultWALFlushLatencyBuckets))
	}

	// Verify first bucket is 1ms
	if DefaultWALFlushLatencyBuckets[0] != 0.001 {
		t.Errorf("first bucket should be 1ms (0.001), got %f", DefaultWALFlushLatencyBuckets[0])
	}

	// Verify we cover at least 5s
	lastBucket := DefaultWALFlushLatencyBuckets[len(DefaultWALFlushLatencyBuckets)-1]
	if lastBucket < 5.0 {
		t.Errorf("last bucket should be at least 5s, got %f", lastBucket)
	}
}

func TestWALMetrics_SizeBucketDistribution(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWALMetricsWithRegistry(reg)

	// Record values across different size ranges
	testSizes := []int64{
		512,        // 512B - below first bucket
		2048,       // 2KB - between 1KB and 4KB
		100000,     // ~100KB - between 64KB and 256KB
		2000000,    // ~2MB - around 2MB bucket
		10000000,   // ~10MB - around 8MB bucket
	}

	for _, size := range testSizes {
		m.RecordSize(size)
	}

	sizeMetric := &dto.Metric{}
	if err := m.SizeHistogram.Write(sizeMetric); err != nil {
		t.Fatalf("failed to write size metric: %v", err)
	}

	if got := sizeMetric.Histogram.GetSampleCount(); got != uint64(len(testSizes)) {
		t.Errorf("sample count = %d, want %d", got, len(testSizes))
	}

	// Verify samples are distributed across buckets
	buckets := sizeMetric.Histogram.GetBucket()
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

func TestWALMetrics_LatencyBucketDistribution(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWALMetricsWithRegistry(reg)

	// Record values across different latency ranges
	testLatencies := []float64{
		0.0005,  // 0.5ms - below 1ms bucket
		0.003,   // 3ms - between 1ms and 5ms
		0.015,   // 15ms - between 10ms and 25ms
		0.075,   // 75ms - between 50ms and 100ms
		0.3,     // 300ms - between 250ms and 500ms
		2.0,     // 2s - between 1s and 2.5s
	}

	for _, latency := range testLatencies {
		m.RecordLatency(latency)
	}

	latencyMetric := &dto.Metric{}
	if err := m.FlushLatencyHistogram.Write(latencyMetric); err != nil {
		t.Fatalf("failed to write latency metric: %v", err)
	}

	if got := latencyMetric.Histogram.GetSampleCount(); got != uint64(len(testLatencies)) {
		t.Errorf("sample count = %d, want %d", got, len(testLatencies))
	}

	// Verify samples are distributed across buckets
	buckets := latencyMetric.Histogram.GetBucket()
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
