package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WALMetrics holds metrics related to WAL operations.
type WALMetrics struct {
	// SizeHistogram tracks WAL object size in bytes.
	// Uses buckets optimized for typical WAL sizes (KB to MB range).
	SizeHistogram prometheus.Histogram

	// FlushLatencyHistogram tracks WAL flush latency in seconds.
	// Covers from fast local flushes to slow network writes.
	FlushLatencyHistogram prometheus.Histogram

	// ObjectsCreatedTotal tracks total WAL objects created.
	// Can be used to compute WAL objects per second via rate().
	ObjectsCreatedTotal prometheus.Counter
}

// DefaultWALSizeBuckets are size buckets for WAL objects in bytes.
// Designed to capture typical WAL sizes from small (KB) to large (MB):
// - Small WALs for low-traffic partitions
// - Medium WALs for typical produce batches
// - Large WALs up to 128MB (upper bound before splitting in most cases)
var DefaultWALSizeBuckets = []float64{
	1024,           // 1KB
	4096,           // 4KB
	16384,          // 16KB
	65536,          // 64KB
	262144,         // 256KB
	524288,         // 512KB
	1048576,        // 1MB
	2097152,        // 2MB
	4194304,        // 4MB
	8388608,        // 8MB
	16777216,       // 16MB
	33554432,       // 32MB
	67108864,       // 64MB
	134217728,      // 128MB
}

// DefaultWALFlushLatencyBuckets are latency buckets for WAL flush operations.
// Covers from fast in-memory operations to slow S3 writes.
var DefaultWALFlushLatencyBuckets = []float64{
	0.001,  // 1ms
	0.005,  // 5ms
	0.01,   // 10ms
	0.025,  // 25ms
	0.05,   // 50ms
	0.1,    // 100ms
	0.25,   // 250ms
	0.5,    // 500ms
	1.0,    // 1s
	2.5,    // 2.5s
	5.0,    // 5s
	10.0,   // 10s
}

// NewWALMetrics creates and registers WAL metrics.
// Uses promauto for automatic registration with the default registry.
func NewWALMetrics() *WALMetrics {
	return &WALMetrics{
		SizeHistogram: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "wal",
				Name:      "object_size_bytes",
				Help:      "WAL object size in bytes.",
				Buckets:   DefaultWALSizeBuckets,
			},
		),
		FlushLatencyHistogram: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "wal",
				Name:      "flush_latency_seconds",
				Help:      "WAL flush latency in seconds.",
				Buckets:   DefaultWALFlushLatencyBuckets,
			},
		),
		ObjectsCreatedTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "wal",
				Name:      "objects_created_total",
				Help:      "Total number of WAL objects created.",
			},
		),
	}
}

// NewWALMetricsWithRegistry creates WAL metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewWALMetricsWithRegistry(reg prometheus.Registerer) *WALMetrics {
	sizeHist := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "wal",
			Name:      "object_size_bytes",
			Help:      "WAL object size in bytes.",
			Buckets:   DefaultWALSizeBuckets,
		},
	)

	flushLatencyHist := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "wal",
			Name:      "flush_latency_seconds",
			Help:      "WAL flush latency in seconds.",
			Buckets:   DefaultWALFlushLatencyBuckets,
		},
	)

	objectsCreatedTotal := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "wal",
			Name:      "objects_created_total",
			Help:      "Total number of WAL objects created.",
		},
	)

	reg.MustRegister(sizeHist)
	reg.MustRegister(flushLatencyHist)
	reg.MustRegister(objectsCreatedTotal)

	return &WALMetrics{
		SizeHistogram:         sizeHist,
		FlushLatencyHistogram: flushLatencyHist,
		ObjectsCreatedTotal:   objectsCreatedTotal,
	}
}

// RecordFlush records a successful WAL flush operation.
// sizeBytes is the WAL object size, durationSeconds is the flush latency.
func (m *WALMetrics) RecordFlush(sizeBytes int64, durationSeconds float64) {
	m.SizeHistogram.Observe(float64(sizeBytes))
	m.FlushLatencyHistogram.Observe(durationSeconds)
	m.ObjectsCreatedTotal.Inc()
}

// RecordSize records a WAL object size.
func (m *WALMetrics) RecordSize(sizeBytes int64) {
	m.SizeHistogram.Observe(float64(sizeBytes))
}

// RecordLatency records a WAL flush latency.
func (m *WALMetrics) RecordLatency(durationSeconds float64) {
	m.FlushLatencyHistogram.Observe(durationSeconds)
}

// IncObjectsCreated increments the WAL objects created counter.
func (m *WALMetrics) IncObjectsCreated() {
	m.ObjectsCreatedTotal.Inc()
}
