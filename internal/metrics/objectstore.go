package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ObjectStoreMetrics holds metrics related to object store operations.
type ObjectStoreMetrics struct {
	// LatencyHistogram tracks object store operation latencies broken down by operation and status.
	// Labels: operation (put, get, get_range, head, delete, list), status (success, failure)
	LatencyHistogram *prometheus.HistogramVec

	// RequestsTotal tracks total object store operations by operation and status.
	RequestsTotal *prometheus.CounterVec

	// BytesTotal tracks total bytes transferred by direction.
	// Labels: direction (read, write)
	BytesTotal *prometheus.CounterVec
}

// Object store operation label values.
const (
	OpObjPut      = "put"
	OpObjGet      = "get"
	OpObjGetRange = "get_range"
	OpObjHead     = "head"
	OpObjDelete   = "delete"
	OpObjList     = "list"
)

// Bytes direction label values.
const (
	DirectionRead  = "read"
	DirectionWrite = "write"
)

// DefaultObjectStoreLatencyBuckets are latency buckets for object store operations.
// Optimized for S3/GCS/Azure blob operations which typically range from tens of ms to seconds.
var DefaultObjectStoreLatencyBuckets = []float64{
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
	30.0,   // 30s
	60.0,   // 60s
}

// NewObjectStoreMetrics creates and registers object store metrics.
// Uses promauto for automatic registration with the default registry.
func NewObjectStoreMetrics() *ObjectStoreMetrics {
	return &ObjectStoreMetrics{
		LatencyHistogram: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "objectstore",
				Name:      "operation_latency_seconds",
				Help:      "Object store operation latency in seconds, broken down by operation and status.",
				Buckets:   DefaultObjectStoreLatencyBuckets,
			},
			[]string{"operation", "status"},
		),
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "objectstore",
				Name:      "operations_total",
				Help:      "Total number of object store operations, broken down by operation and status.",
			},
			[]string{"operation", "status"},
		),
		BytesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "objectstore",
				Name:      "bytes_total",
				Help:      "Total bytes transferred by direction (read/write).",
			},
			[]string{"direction"},
		),
	}
}

// NewObjectStoreMetricsWithRegistry creates object store metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewObjectStoreMetricsWithRegistry(reg prometheus.Registerer) *ObjectStoreMetrics {
	latencyHist := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "objectstore",
			Name:      "operation_latency_seconds",
			Help:      "Object store operation latency in seconds, broken down by operation and status.",
			Buckets:   DefaultObjectStoreLatencyBuckets,
		},
		[]string{"operation", "status"},
	)

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "objectstore",
			Name:      "operations_total",
			Help:      "Total number of object store operations, broken down by operation and status.",
		},
		[]string{"operation", "status"},
	)

	bytesTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "objectstore",
			Name:      "bytes_total",
			Help:      "Total bytes transferred by direction (read/write).",
		},
		[]string{"direction"},
	)

	reg.MustRegister(latencyHist)
	reg.MustRegister(requestsTotal)
	reg.MustRegister(bytesTotal)

	return &ObjectStoreMetrics{
		LatencyHistogram: latencyHist,
		RequestsTotal:    requestsTotal,
		BytesTotal:       bytesTotal,
	}
}

// RecordOperation records an object store operation latency and increments the request counter.
// operation should be one of OpObjPut, OpObjGet, OpObjGetRange, OpObjHead, OpObjDelete, OpObjList.
// success indicates whether the operation succeeded.
func (m *ObjectStoreMetrics) RecordOperation(operation string, durationSeconds float64, success bool) {
	status := StatusFailure
	if success {
		status = StatusSuccess
	}
	m.LatencyHistogram.WithLabelValues(operation, status).Observe(durationSeconds)
	m.RequestsTotal.WithLabelValues(operation, status).Inc()
}

// RecordBytesRead records bytes read from the object store.
func (m *ObjectStoreMetrics) RecordBytesRead(bytes int64) {
	m.BytesTotal.WithLabelValues(DirectionRead).Add(float64(bytes))
}

// RecordBytesWritten records bytes written to the object store.
func (m *ObjectStoreMetrics) RecordBytesWritten(bytes int64) {
	m.BytesTotal.WithLabelValues(DirectionWrite).Add(float64(bytes))
}

// RecordPut records a Put operation.
func (m *ObjectStoreMetrics) RecordPut(durationSeconds float64, success bool, bytes int64) {
	m.RecordOperation(OpObjPut, durationSeconds, success)
	if success && bytes > 0 {
		m.RecordBytesWritten(bytes)
	}
}

// RecordGet records a Get operation.
func (m *ObjectStoreMetrics) RecordGet(durationSeconds float64, success bool, bytes int64) {
	m.RecordOperation(OpObjGet, durationSeconds, success)
	if success && bytes > 0 {
		m.RecordBytesRead(bytes)
	}
}

// RecordGetRange records a GetRange operation.
func (m *ObjectStoreMetrics) RecordGetRange(durationSeconds float64, success bool, bytes int64) {
	m.RecordOperation(OpObjGetRange, durationSeconds, success)
	if success && bytes > 0 {
		m.RecordBytesRead(bytes)
	}
}

// RecordHead records a Head operation.
func (m *ObjectStoreMetrics) RecordHead(durationSeconds float64, success bool) {
	m.RecordOperation(OpObjHead, durationSeconds, success)
}

// RecordDelete records a Delete operation.
func (m *ObjectStoreMetrics) RecordDelete(durationSeconds float64, success bool) {
	m.RecordOperation(OpObjDelete, durationSeconds, success)
}

// RecordList records a List operation.
func (m *ObjectStoreMetrics) RecordList(durationSeconds float64, success bool) {
	m.RecordOperation(OpObjList, durationSeconds, success)
}
