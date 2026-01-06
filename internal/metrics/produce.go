// Package metrics provides Prometheus metrics for observability.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ProduceMetrics holds metrics related to produce requests.
type ProduceMetrics struct {
	// LatencyHistogram tracks produce request latencies broken down by success/failure.
	// Uses buckets optimized for typical Kafka latencies (sub-millisecond to seconds).
	// Labels: status (success, failure)
	LatencyHistogram *prometheus.HistogramVec

	// RequestsTotal tracks total produce requests by status.
	RequestsTotal *prometheus.CounterVec

	// MessagesInTotal tracks total Kafka records accepted by Dray.
	MessagesInTotal prometheus.Counter
}

// DefaultProduceLatencyBuckets are latency buckets for produce requests.
// Designed to capture p50, p99, p999 accurately:
// - Sub-millisecond for fast local operations
// - Millisecond range for typical operations
// - Up to 10s for worst-case scenarios
var DefaultProduceLatencyBuckets = []float64{
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

// StatusSuccess is the label value for successful produce requests.
const StatusSuccess = "success"

// StatusFailure is the label value for failed produce requests.
const StatusFailure = "failure"

// NewProduceMetrics creates and registers produce metrics.
// Uses promauto for automatic registration with the default registry.
func NewProduceMetrics() *ProduceMetrics {
	return &ProduceMetrics{
		LatencyHistogram: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "produce",
				Name:      "latency_seconds",
				Help:      "Produce request latency in seconds, broken down by success/failure.",
				Buckets:   DefaultProduceLatencyBuckets,
			},
			[]string{"status"},
		),
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "produce",
				Name:      "requests_total",
				Help:      "Total number of produce requests, broken down by status.",
			},
			[]string{"status"},
		),
		MessagesInTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "topic",
				Name:      "messages_in_total",
				Help:      "Total number of Kafka records accepted by Dray.",
			},
		),
	}
}

// NewProduceMetricsWithRegistry creates produce metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewProduceMetricsWithRegistry(reg prometheus.Registerer) *ProduceMetrics {
	latencyHist := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "produce",
			Name:      "latency_seconds",
			Help:      "Produce request latency in seconds, broken down by success/failure.",
			Buckets:   DefaultProduceLatencyBuckets,
		},
		[]string{"status"},
	)

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "produce",
			Name:      "requests_total",
			Help:      "Total number of produce requests, broken down by status.",
		},
		[]string{"status"},
	)

	messagesInTotal := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "topic",
			Name:      "messages_in_total",
			Help:      "Total number of Kafka records accepted by Dray.",
		},
	)

	reg.MustRegister(latencyHist)
	reg.MustRegister(requestsTotal)
	reg.MustRegister(messagesInTotal)

	return &ProduceMetrics{
		LatencyHistogram: latencyHist,
		RequestsTotal:    requestsTotal,
		MessagesInTotal:  messagesInTotal,
	}
}

// RecordLatency records a produce request latency.
// duration is in seconds, success indicates whether the request succeeded.
func (m *ProduceMetrics) RecordLatency(durationSeconds float64, success bool) {
	status := StatusFailure
	if success {
		status = StatusSuccess
	}
	m.LatencyHistogram.WithLabelValues(status).Observe(durationSeconds)
	m.RequestsTotal.WithLabelValues(status).Inc()
}

// RecordSuccess is a convenience method to record a successful produce latency.
func (m *ProduceMetrics) RecordSuccess(durationSeconds float64) {
	m.RecordLatency(durationSeconds, true)
}

// RecordFailure is a convenience method to record a failed produce latency.
func (m *ProduceMetrics) RecordFailure(durationSeconds float64) {
	m.RecordLatency(durationSeconds, false)
}

// RecordMessages increments the total record counter by count.
func (m *ProduceMetrics) RecordMessages(count uint32) {
	if m == nil || m.MessagesInTotal == nil || count == 0 {
		return
	}
	m.MessagesInTotal.Add(float64(count))
}
