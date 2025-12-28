package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// FetchMetrics holds metrics related to fetch requests.
type FetchMetrics struct {
	// LatencyHistogram tracks fetch request latencies broken down by status.
	// Labels: status (success, failure)
	LatencyHistogram *prometheus.HistogramVec

	// SourceLatencyHistogram tracks fetch latencies broken down by data source.
	// Labels: source (wal, parquet, none)
	SourceLatencyHistogram *prometheus.HistogramVec

	// RequestsTotal tracks total fetch requests by status.
	RequestsTotal *prometheus.CounterVec

	// SourceRequestsTotal tracks fetch requests by source.
	SourceRequestsTotal *prometheus.CounterVec
}

// Fetch source label values.
const (
	SourceWAL     = "wal"
	SourceParquet = "parquet"
	SourceNone    = "none" // For empty fetches (offset >= HWM)
)

// DefaultFetchLatencyBuckets are latency buckets for fetch requests.
// Similar to produce buckets, designed for p50, p99, p999 accuracy.
var DefaultFetchLatencyBuckets = []float64{
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
	10.0,   // 10s
}

// NewFetchMetrics creates and registers fetch metrics.
// Uses promauto for automatic registration with the default registry.
func NewFetchMetrics() *FetchMetrics {
	return &FetchMetrics{
		LatencyHistogram: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "fetch",
				Name:      "latency_seconds",
				Help:      "Fetch request latency in seconds, broken down by success/failure.",
				Buckets:   DefaultFetchLatencyBuckets,
			},
			[]string{"status"},
		),
		SourceLatencyHistogram: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "fetch",
				Name:      "source_latency_seconds",
				Help:      "Fetch request latency in seconds, broken down by data source (wal, parquet, none).",
				Buckets:   DefaultFetchLatencyBuckets,
			},
			[]string{"source"},
		),
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "fetch",
				Name:      "requests_total",
				Help:      "Total number of fetch requests, broken down by status.",
			},
			[]string{"status"},
		),
		SourceRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "fetch",
				Name:      "source_requests_total",
				Help:      "Total number of fetch requests, broken down by source.",
			},
			[]string{"source"},
		),
	}
}

// NewFetchMetricsWithRegistry creates fetch metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewFetchMetricsWithRegistry(reg prometheus.Registerer) *FetchMetrics {
	latencyHist := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "fetch",
			Name:      "latency_seconds",
			Help:      "Fetch request latency in seconds, broken down by success/failure.",
			Buckets:   DefaultFetchLatencyBuckets,
		},
		[]string{"status"},
	)

	sourceLatencyHist := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "fetch",
			Name:      "source_latency_seconds",
			Help:      "Fetch request latency in seconds, broken down by data source (wal, parquet, none).",
			Buckets:   DefaultFetchLatencyBuckets,
		},
		[]string{"source"},
	)

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "fetch",
			Name:      "requests_total",
			Help:      "Total number of fetch requests, broken down by status.",
		},
		[]string{"status"},
	)

	sourceRequestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "fetch",
			Name:      "source_requests_total",
			Help:      "Total number of fetch requests, broken down by source.",
		},
		[]string{"source"},
	)

	reg.MustRegister(latencyHist)
	reg.MustRegister(sourceLatencyHist)
	reg.MustRegister(requestsTotal)
	reg.MustRegister(sourceRequestsTotal)

	return &FetchMetrics{
		LatencyHistogram:       latencyHist,
		SourceLatencyHistogram: sourceLatencyHist,
		RequestsTotal:          requestsTotal,
		SourceRequestsTotal:    sourceRequestsTotal,
	}
}

// RecordLatency records a fetch request latency.
// duration is in seconds, success indicates whether the request succeeded.
func (m *FetchMetrics) RecordLatency(durationSeconds float64, success bool) {
	status := StatusFailure
	if success {
		status = StatusSuccess
	}
	m.LatencyHistogram.WithLabelValues(status).Observe(durationSeconds)
	m.RequestsTotal.WithLabelValues(status).Inc()
}

// RecordSourceLatency records a fetch request latency by data source.
// source should be one of SourceWAL, SourceParquet, or SourceNone.
func (m *FetchMetrics) RecordSourceLatency(durationSeconds float64, source string) {
	m.SourceLatencyHistogram.WithLabelValues(source).Observe(durationSeconds)
	m.SourceRequestsTotal.WithLabelValues(source).Inc()
}

// RecordSuccess is a convenience method to record a successful fetch latency.
func (m *FetchMetrics) RecordSuccess(durationSeconds float64) {
	m.RecordLatency(durationSeconds, true)
}

// RecordFailure is a convenience method to record a failed fetch latency.
func (m *FetchMetrics) RecordFailure(durationSeconds float64) {
	m.RecordLatency(durationSeconds, false)
}
