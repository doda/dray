// Package metrics provides Prometheus metrics for observability.
//
// This package exposes metrics for various Dray operations including:
//   - Produce request latency (p50, p99, p999) broken down by success/failure
//   - Fetch request latency (p50, p99, p999) broken down by success/failure
//   - Fetch request latency broken down by source (wal, parquet, none)
//   - Request counters by status
//   - WAL flush size histogram (bytes per WAL object)
//   - WAL flush latency histogram (seconds per flush operation)
//   - WAL objects created counter (for computing objects/second via rate())
//
// Metrics are exposed via a dedicated HTTP server on /metrics in Prometheus format.
//
// Usage:
//
//	// Create and register metrics
//	produceMetrics := metrics.NewProduceMetrics()
//	fetchMetrics := metrics.NewFetchMetrics()
//	walMetrics := metrics.NewWALMetrics()
//
//	// Wire into handlers
//	produceHandler := protocol.NewProduceHandler(...).WithMetrics(produceMetrics)
//	fetchHandler := protocol.NewFetchHandler(...).WithMetrics(fetchMetrics)
//	stagingWriter := wal.NewStagingWriter(store, meta, &wal.StagingWriterConfig{Metrics: walMetrics})
//
//	// Start metrics server
//	metricsServer := metrics.NewServer(":9090")
//	metricsServer.Start()
package metrics
