package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ConnectionMetrics holds metrics related to TCP connections and request rates.
type ConnectionMetrics struct {
	// ActiveConnections tracks the current number of active TCP connections.
	ActiveConnections prometheus.Gauge

	// RequestsTotal tracks total requests by API type and status.
	// Labels: api (Produce, Fetch, Metadata, etc.), status (success, failure)
	RequestsTotal *prometheus.CounterVec

	// ErrorsTotal tracks request errors by API type and error code.
	// Labels: api, error_code
	ErrorsTotal *prometheus.CounterVec
}

// NewConnectionMetrics creates and registers connection metrics.
// Uses promauto for automatic registration with the default registry.
func NewConnectionMetrics() *ConnectionMetrics {
	return &ConnectionMetrics{
		ActiveConnections: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "server",
				Name:      "active_connections",
				Help:      "Current number of active TCP connections.",
			},
		),
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "server",
				Name:      "requests_total",
				Help:      "Total number of requests, broken down by API type and status.",
			},
			[]string{"api", "status"},
		),
		ErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "server",
				Name:      "errors_total",
				Help:      "Total number of request errors, broken down by API type and error code.",
			},
			[]string{"api", "error_code"},
		),
	}
}

// NewConnectionMetricsWithRegistry creates connection metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewConnectionMetricsWithRegistry(reg prometheus.Registerer) *ConnectionMetrics {
	activeConnections := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "server",
			Name:      "active_connections",
			Help:      "Current number of active TCP connections.",
		},
	)

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "server",
			Name:      "requests_total",
			Help:      "Total number of requests, broken down by API type and status.",
		},
		[]string{"api", "status"},
	)

	errorsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "server",
			Name:      "errors_total",
			Help:      "Total number of request errors, broken down by API type and error code.",
		},
		[]string{"api", "error_code"},
	)

	reg.MustRegister(activeConnections)
	reg.MustRegister(requestsTotal)
	reg.MustRegister(errorsTotal)

	return &ConnectionMetrics{
		ActiveConnections: activeConnections,
		RequestsTotal:     requestsTotal,
		ErrorsTotal:       errorsTotal,
	}
}

// ConnectionOpened increments the active connections counter.
func (m *ConnectionMetrics) ConnectionOpened() {
	m.ActiveConnections.Inc()
}

// ConnectionClosed decrements the active connections counter.
func (m *ConnectionMetrics) ConnectionClosed() {
	m.ActiveConnections.Dec()
}

// RecordRequest records a request by API type.
// apiName is the Kafka API name (e.g., "Produce", "Fetch", "Metadata").
// success indicates whether the request succeeded.
func (m *ConnectionMetrics) RecordRequest(apiName string, success bool) {
	status := StatusFailure
	if success {
		status = StatusSuccess
	}
	m.RequestsTotal.WithLabelValues(apiName, status).Inc()
}

// RecordSuccess is a convenience method to record a successful request.
func (m *ConnectionMetrics) RecordSuccess(apiName string) {
	m.RecordRequest(apiName, true)
}

// RecordFailure is a convenience method to record a failed request.
func (m *ConnectionMetrics) RecordFailure(apiName string) {
	m.RecordRequest(apiName, false)
}

// RecordError records a request error by API type and error code.
// apiName is the Kafka API name, errorCode is the Kafka error code name.
func (m *ConnectionMetrics) RecordError(apiName, errorCode string) {
	m.ErrorsTotal.WithLabelValues(apiName, errorCode).Inc()
}
