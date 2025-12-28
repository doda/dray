package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func TestObjectStoreMetrics_NewWithRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	if m.LatencyHistogram == nil {
		t.Error("LatencyHistogram should not be nil")
	}
	if m.RequestsTotal == nil {
		t.Error("RequestsTotal should not be nil")
	}
	if m.BytesTotal == nil {
		t.Error("BytesTotal should not be nil")
	}

	// Record some metrics to make them gatherable (Prometheus doesn't expose
	// metrics until they have observations for vec types)
	m.RecordPut(0.01, true, 100)

	// Verify metrics are registered
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}
	if len(mfs) != 3 {
		t.Errorf("Expected 3 metric families, got %d", len(mfs))
	}
}

func TestObjectStoreMetrics_RecordPut(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	m.RecordPut(0.1, true, 1024)
	m.RecordPut(0.2, false, 512)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Check latency histogram
	latencyMF := findMetricFamily(mfs, "dray_objectstore_operation_latency_seconds")
	if latencyMF == nil {
		t.Fatal("dray_objectstore_operation_latency_seconds not found")
	}
	if len(latencyMF.Metric) != 2 {
		t.Errorf("Expected 2 latency metrics (success/failure), got %d", len(latencyMF.Metric))
	}

	// Check request counter
	requestsMF := findMetricFamily(mfs, "dray_objectstore_operations_total")
	if requestsMF == nil {
		t.Fatal("dray_objectstore_operations_total not found")
	}
	successCount := getCounterValue(requestsMF, map[string]string{"operation": OpObjPut, "status": StatusSuccess})
	failureCount := getCounterValue(requestsMF, map[string]string{"operation": OpObjPut, "status": StatusFailure})
	if successCount != 1 {
		t.Errorf("Expected 1 success put, got %f", successCount)
	}
	if failureCount != 1 {
		t.Errorf("Expected 1 failure put, got %f", failureCount)
	}

	// Check bytes written (only for success)
	bytesMF := findMetricFamily(mfs, "dray_objectstore_bytes_total")
	if bytesMF == nil {
		t.Fatal("dray_objectstore_bytes_total not found")
	}
	writtenBytes := getCounterValue(bytesMF, map[string]string{"direction": DirectionWrite})
	if writtenBytes != 1024 {
		t.Errorf("Expected 1024 bytes written, got %f", writtenBytes)
	}
}

func TestObjectStoreMetrics_RecordGet(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	m.RecordGet(0.05, true, 2048)
	m.RecordGet(0.15, false, 0)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	requestsMF := findMetricFamily(mfs, "dray_objectstore_operations_total")
	if requestsMF == nil {
		t.Fatal("dray_objectstore_operations_total not found")
	}
	successCount := getCounterValue(requestsMF, map[string]string{"operation": OpObjGet, "status": StatusSuccess})
	failureCount := getCounterValue(requestsMF, map[string]string{"operation": OpObjGet, "status": StatusFailure})
	if successCount != 1 {
		t.Errorf("Expected 1 success get, got %f", successCount)
	}
	if failureCount != 1 {
		t.Errorf("Expected 1 failure get, got %f", failureCount)
	}

	bytesMF := findMetricFamily(mfs, "dray_objectstore_bytes_total")
	if bytesMF == nil {
		t.Fatal("dray_objectstore_bytes_total not found")
	}
	readBytes := getCounterValue(bytesMF, map[string]string{"direction": DirectionRead})
	if readBytes != 2048 {
		t.Errorf("Expected 2048 bytes read, got %f", readBytes)
	}
}

func TestObjectStoreMetrics_RecordGetRange(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	m.RecordGetRange(0.03, true, 512)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	requestsMF := findMetricFamily(mfs, "dray_objectstore_operations_total")
	if requestsMF == nil {
		t.Fatal("dray_objectstore_operations_total not found")
	}
	count := getCounterValue(requestsMF, map[string]string{"operation": OpObjGetRange, "status": StatusSuccess})
	if count != 1 {
		t.Errorf("Expected 1 get_range, got %f", count)
	}

	bytesMF := findMetricFamily(mfs, "dray_objectstore_bytes_total")
	if bytesMF == nil {
		t.Fatal("dray_objectstore_bytes_total not found")
	}
	readBytes := getCounterValue(bytesMF, map[string]string{"direction": DirectionRead})
	if readBytes != 512 {
		t.Errorf("Expected 512 bytes read, got %f", readBytes)
	}
}

func TestObjectStoreMetrics_RecordHead(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	m.RecordHead(0.01, true)
	m.RecordHead(0.02, false)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	requestsMF := findMetricFamily(mfs, "dray_objectstore_operations_total")
	if requestsMF == nil {
		t.Fatal("dray_objectstore_operations_total not found")
	}
	successCount := getCounterValue(requestsMF, map[string]string{"operation": OpObjHead, "status": StatusSuccess})
	failureCount := getCounterValue(requestsMF, map[string]string{"operation": OpObjHead, "status": StatusFailure})
	if successCount != 1 {
		t.Errorf("Expected 1 success head, got %f", successCount)
	}
	if failureCount != 1 {
		t.Errorf("Expected 1 failure head, got %f", failureCount)
	}
}

func TestObjectStoreMetrics_RecordDelete(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	m.RecordDelete(0.02, true)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	requestsMF := findMetricFamily(mfs, "dray_objectstore_operations_total")
	if requestsMF == nil {
		t.Fatal("dray_objectstore_operations_total not found")
	}
	count := getCounterValue(requestsMF, map[string]string{"operation": OpObjDelete, "status": StatusSuccess})
	if count != 1 {
		t.Errorf("Expected 1 delete, got %f", count)
	}
}

func TestObjectStoreMetrics_RecordList(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	m.RecordList(0.05, true)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	requestsMF := findMetricFamily(mfs, "dray_objectstore_operations_total")
	if requestsMF == nil {
		t.Fatal("dray_objectstore_operations_total not found")
	}
	count := getCounterValue(requestsMF, map[string]string{"operation": OpObjList, "status": StatusSuccess})
	if count != 1 {
		t.Errorf("Expected 1 list, got %f", count)
	}
}

func TestObjectStoreMetrics_RecordBytesReadWrite(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	m.RecordBytesRead(1000)
	m.RecordBytesRead(500)
	m.RecordBytesWritten(2000)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	bytesMF := findMetricFamily(mfs, "dray_objectstore_bytes_total")
	if bytesMF == nil {
		t.Fatal("dray_objectstore_bytes_total not found")
	}
	readBytes := getCounterValue(bytesMF, map[string]string{"direction": DirectionRead})
	writeBytes := getCounterValue(bytesMF, map[string]string{"direction": DirectionWrite})
	if readBytes != 1500 {
		t.Errorf("Expected 1500 bytes read, got %f", readBytes)
	}
	if writeBytes != 2000 {
		t.Errorf("Expected 2000 bytes written, got %f", writeBytes)
	}
}

func TestObjectStoreMetrics_LatencyBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	// Record operations at different latencies
	m.RecordPut(0.001, true, 100)  // 1ms - should hit first bucket
	m.RecordPut(0.05, true, 100)   // 50ms - should hit middle buckets
	m.RecordPut(5.0, true, 100)    // 5s - should hit high buckets

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	latencyMF := findMetricFamily(mfs, "dray_objectstore_operation_latency_seconds")
	if latencyMF == nil {
		t.Fatal("dray_objectstore_operation_latency_seconds not found")
	}

	// Verify the histogram has the expected number of buckets
	for _, metric := range latencyMF.Metric {
		if metric.Histogram == nil {
			continue
		}
		// DefaultObjectStoreLatencyBuckets has 14 buckets
		// (Prometheus client returns only defined buckets, not +Inf)
		if len(metric.Histogram.Bucket) != 14 {
			t.Errorf("Expected 14 buckets, got %d", len(metric.Histogram.Bucket))
		}
		// Verify sample count
		if metric.Histogram.GetSampleCount() != 3 {
			t.Errorf("Expected 3 samples, got %d", metric.Histogram.GetSampleCount())
		}
	}
}

func TestObjectStoreMetrics_ZeroBytesNotRecorded(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewObjectStoreMetricsWithRegistry(reg)

	// Zero bytes should not be recorded
	m.RecordPut(0.01, true, 0)
	m.RecordGet(0.01, true, 0)
	m.RecordGetRange(0.01, true, 0)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	bytesMF := findMetricFamily(mfs, "dray_objectstore_bytes_total")
	if bytesMF != nil {
		for _, metric := range bytesMF.Metric {
			if metric.Counter != nil && metric.Counter.GetValue() != 0 {
				t.Errorf("Expected 0 bytes, got %f", metric.Counter.GetValue())
			}
		}
	}
}

// Helper to find a metric family by name
func findMetricFamily(mfs []*io_prometheus_client.MetricFamily, name string) *io_prometheus_client.MetricFamily {
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

// Helper to get counter value with specific labels
func getCounterValue(mf *io_prometheus_client.MetricFamily, labels map[string]string) float64 {
	for _, metric := range mf.Metric {
		if matchLabels(metric.Label, labels) {
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}
		}
	}
	return 0
}

// Helper to check if metric labels match expected labels
func matchLabels(metricLabels []*io_prometheus_client.LabelPair, expected map[string]string) bool {
	if len(metricLabels) != len(expected) {
		return false
	}
	for _, lp := range metricLabels {
		if expected[lp.GetName()] != lp.GetValue() {
			return false
		}
	}
	return true
}
