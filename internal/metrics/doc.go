// Package metrics provides Prometheus metrics for observability.
//
// This package exposes metrics for various Dray operations including:
//   - Produce request latency (p50, p99, p999) broken down by success/failure
//   - Request counters by status
//
// Metrics are exposed via a dedicated HTTP server on /metrics in Prometheus format.
//
// Usage:
//
//	// Create and register metrics
//	produceMetrics := metrics.NewProduceMetrics()
//
//	// Wire into handlers
//	produceHandler := protocol.NewProduceHandler(...).WithMetrics(produceMetrics)
//
//	// Start metrics server
//	metricsServer := metrics.NewServer(":9090")
//	metricsServer.Start()
package metrics
