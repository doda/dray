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
//   - Compaction backlog metrics:
//   - Total WAL bytes pending compaction
//   - Total WAL files pending compaction
//   - Per-stream backlog metrics
//   - Backlog exceeded alert gauge (for threshold-based alerting)
//   - GC backlog metrics:
//   - Orphan WAL count (uncommitted WAL objects older than TTL)
//   - Pending WAL deletes (WAL objects awaiting grace period)
//   - Pending Parquet deletes (Parquet files replaced by compaction)
//   - Staging WAL count (total in-flight and orphaned staging markers)
//   - Eligible WAL/Parquet deletes (objects ready for immediate deletion)
//   - Oxia operation metrics:
//     - Get/Put/Delete/List/Txn/PutEphemeral latency histograms
//     - Operation counts by type and status (success/failure)
//     - Retry counts by operation type
//
// Metrics are exposed via a dedicated HTTP server on /metrics in Prometheus format.
//
// Usage:
//
//	// Create and register metrics
//	produceMetrics := metrics.NewProduceMetrics()
//	fetchMetrics := metrics.NewFetchMetrics()
//	walMetrics := metrics.NewWALMetrics()
//	compactionMetrics := metrics.NewCompactionMetrics()
//	gcMetrics := metrics.NewGCMetrics()
//	oxiaMetrics := metrics.NewOxiaMetrics()
//
//	// Wire into handlers
//	produceHandler := protocol.NewProduceHandler(...).WithMetrics(produceMetrics)
//	fetchHandler := protocol.NewFetchHandler(...).WithMetrics(fetchMetrics)
//	stagingWriter := wal.NewStagingWriter(store, meta, &wal.StagingWriterConfig{Metrics: walMetrics})
//
//	// Wrap Oxia store with instrumentation
//	rawStore, _ := oxia.New(ctx, oxia.Config{...})
//	instrumentedStore := metadata.NewInstrumentedStore(rawStore, oxiaMetrics)
//
//	// Configure compaction backlog alerting thresholds
//	compactionMetrics.SetThresholds(1024*1024*1024, 1000) // 1GB or 1000 files
//
//	// Start backlog scanner for periodic updates
//	scanner := metrics.NewBacklogScanner(compactionMetrics, indexLister, 30*time.Second)
//	scanner.Start()
//	defer scanner.Stop()
//
//	// Start GC backlog scanner for periodic updates
//	gcScanner := metrics.NewGCBacklogScanner(gcMetrics, gcStatsProvider, 30*time.Second)
//	gcScanner.Start()
//	defer gcScanner.Stop()
//
//	// Start metrics server
//	metricsServer := metrics.NewServer(":9090")
//	metricsServer.Start()
package metrics
