package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// OxiaMetrics holds metrics related to Oxia metadata operations.
type OxiaMetrics struct {
	// LatencyHistogram tracks Oxia operation latencies broken down by operation type and status.
	// Labels: operation (get, put, delete, list, txn, put_ephemeral), status (success, failure)
	LatencyHistogram *prometheus.HistogramVec

	// RequestsTotal tracks total Oxia operations by operation type and status.
	RequestsTotal *prometheus.CounterVec

	// RetriesTotal tracks retry counts by operation type.
	// Retries occur when operations fail transiently and are retried automatically.
	RetriesTotal *prometheus.CounterVec
}

// Oxia operation type label values.
const (
	OpGet         = "get"
	OpPut         = "put"
	OpDelete      = "delete"
	OpList        = "list"
	OpTxn         = "txn"
	OpPutEphemeral = "put_ephemeral"
)

// DefaultOxiaLatencyBuckets are latency buckets for Oxia operations.
// Optimized for metadata operations which are typically fast (sub-ms to tens of ms).
var DefaultOxiaLatencyBuckets = []float64{
	0.0001, // 0.1ms
	0.0005, // 0.5ms
	0.001,  // 1ms
	0.002,  // 2ms
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
}

// NewOxiaMetrics creates and registers Oxia metrics.
// Uses promauto for automatic registration with the default registry.
func NewOxiaMetrics() *OxiaMetrics {
	return &OxiaMetrics{
		LatencyHistogram: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "oxia",
				Name:      "operation_latency_seconds",
				Help:      "Oxia operation latency in seconds, broken down by operation type and status.",
				Buckets:   DefaultOxiaLatencyBuckets,
			},
			[]string{"operation", "status"},
		),
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "oxia",
				Name:      "operations_total",
				Help:      "Total number of Oxia operations, broken down by operation type and status.",
			},
			[]string{"operation", "status"},
		),
		RetriesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "oxia",
				Name:      "retries_total",
				Help:      "Total number of Oxia operation retries, broken down by operation type.",
			},
			[]string{"operation"},
		),
	}
}

// NewOxiaMetricsWithRegistry creates Oxia metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewOxiaMetricsWithRegistry(reg prometheus.Registerer) *OxiaMetrics {
	latencyHist := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "oxia",
			Name:      "operation_latency_seconds",
			Help:      "Oxia operation latency in seconds, broken down by operation type and status.",
			Buckets:   DefaultOxiaLatencyBuckets,
		},
		[]string{"operation", "status"},
	)

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "oxia",
			Name:      "operations_total",
			Help:      "Total number of Oxia operations, broken down by operation type and status.",
		},
		[]string{"operation", "status"},
	)

	retriesTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "oxia",
			Name:      "retries_total",
			Help:      "Total number of Oxia operation retries, broken down by operation type.",
		},
		[]string{"operation"},
	)

	reg.MustRegister(latencyHist)
	reg.MustRegister(requestsTotal)
	reg.MustRegister(retriesTotal)

	return &OxiaMetrics{
		LatencyHistogram: latencyHist,
		RequestsTotal:    requestsTotal,
		RetriesTotal:     retriesTotal,
	}
}

// RecordOperation records an Oxia operation latency and increments the request counter.
// operation should be one of OpGet, OpPut, OpDelete, OpList, OpTxn, OpPutEphemeral.
// success indicates whether the operation succeeded.
func (m *OxiaMetrics) RecordOperation(operation string, durationSeconds float64, success bool) {
	status := StatusFailure
	if success {
		status = StatusSuccess
	}
	m.LatencyHistogram.WithLabelValues(operation, status).Observe(durationSeconds)
	m.RequestsTotal.WithLabelValues(operation, status).Inc()
}

// RecordRetry increments the retry counter for the given operation type.
func (m *OxiaMetrics) RecordRetry(operation string) {
	m.RetriesTotal.WithLabelValues(operation).Inc()
}

// RecordGet records a Get operation.
func (m *OxiaMetrics) RecordGet(durationSeconds float64, success bool) {
	m.RecordOperation(OpGet, durationSeconds, success)
}

// RecordPut records a Put operation.
func (m *OxiaMetrics) RecordPut(durationSeconds float64, success bool) {
	m.RecordOperation(OpPut, durationSeconds, success)
}

// RecordDelete records a Delete operation.
func (m *OxiaMetrics) RecordDelete(durationSeconds float64, success bool) {
	m.RecordOperation(OpDelete, durationSeconds, success)
}

// RecordList records a List operation.
func (m *OxiaMetrics) RecordList(durationSeconds float64, success bool) {
	m.RecordOperation(OpList, durationSeconds, success)
}

// RecordTxn records a Txn operation.
func (m *OxiaMetrics) RecordTxn(durationSeconds float64, success bool) {
	m.RecordOperation(OpTxn, durationSeconds, success)
}

// RecordPutEphemeral records a PutEphemeral operation.
func (m *OxiaMetrics) RecordPutEphemeral(durationSeconds float64, success bool) {
	m.RecordOperation(OpPutEphemeral, durationSeconds, success)
}
