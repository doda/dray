package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// GCMetrics holds metrics related to garbage collection backlogs.
type GCMetrics struct {
	// OrphanWALCount tracks the number of orphaned WAL staging markers.
	// These are WAL objects that were written but never committed.
	OrphanWALCount prometheus.Gauge

	// PendingWALDeletes tracks the number of WAL objects pending deletion.
	// These are WAL objects whose refcount has reached zero after compaction.
	PendingWALDeletes prometheus.Gauge

	// PendingParquetDeletes tracks the number of Parquet files pending deletion.
	// These are Parquet files replaced by compaction rewrite.
	PendingParquetDeletes prometheus.Gauge

	// StagingWALCount tracks the total number of WAL staging markers.
	// This includes both active (in-flight) and orphaned staging markers.
	StagingWALCount prometheus.Gauge

	// EligibleWALDeletes tracks the number of WAL objects eligible for immediate deletion.
	// These are WAL objects whose grace period has passed.
	EligibleWALDeletes prometheus.Gauge

	// EligibleParquetDeletes tracks the number of Parquet files eligible for immediate deletion.
	// These are Parquet files whose grace period has passed.
	EligibleParquetDeletes prometheus.Gauge
}

// NewGCMetrics creates and registers GC metrics.
// Uses promauto for automatic registration with the default registry.
func NewGCMetrics() *GCMetrics {
	return &GCMetrics{
		OrphanWALCount: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "gc",
				Name:      "orphan_wal_count",
				Help:      "Number of orphaned WAL staging markers (uncommitted WAL objects older than TTL).",
			},
		),
		PendingWALDeletes: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "gc",
				Name:      "pending_wal_deletes",
				Help:      "Number of WAL objects pending deletion (refcount zero, awaiting grace period).",
			},
		),
		PendingParquetDeletes: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "gc",
				Name:      "pending_parquet_deletes",
				Help:      "Number of Parquet files pending deletion (replaced by compaction rewrite).",
			},
		),
		StagingWALCount: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "gc",
				Name:      "staging_wal_count",
				Help:      "Total number of WAL staging markers (in-flight and orphaned).",
			},
		),
		EligibleWALDeletes: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "gc",
				Name:      "eligible_wal_deletes",
				Help:      "Number of WAL objects eligible for immediate deletion (grace period passed).",
			},
		),
		EligibleParquetDeletes: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "gc",
				Name:      "eligible_parquet_deletes",
				Help:      "Number of Parquet files eligible for immediate deletion (grace period passed).",
			},
		),
	}
}

// NewGCMetricsWithRegistry creates GC metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewGCMetricsWithRegistry(reg prometheus.Registerer) *GCMetrics {
	orphanWALCount := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "gc",
			Name:      "orphan_wal_count",
			Help:      "Number of orphaned WAL staging markers (uncommitted WAL objects older than TTL).",
		},
	)

	pendingWALDeletes := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "gc",
			Name:      "pending_wal_deletes",
			Help:      "Number of WAL objects pending deletion (refcount zero, awaiting grace period).",
		},
	)

	pendingParquetDeletes := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "gc",
			Name:      "pending_parquet_deletes",
			Help:      "Number of Parquet files pending deletion (replaced by compaction rewrite).",
		},
	)

	stagingWALCount := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "gc",
			Name:      "staging_wal_count",
			Help:      "Total number of WAL staging markers (in-flight and orphaned).",
		},
	)

	eligibleWALDeletes := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "gc",
			Name:      "eligible_wal_deletes",
			Help:      "Number of WAL objects eligible for immediate deletion (grace period passed).",
		},
	)

	eligibleParquetDeletes := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "gc",
			Name:      "eligible_parquet_deletes",
			Help:      "Number of Parquet files eligible for immediate deletion (grace period passed).",
		},
	)

	reg.MustRegister(orphanWALCount)
	reg.MustRegister(pendingWALDeletes)
	reg.MustRegister(pendingParquetDeletes)
	reg.MustRegister(stagingWALCount)
	reg.MustRegister(eligibleWALDeletes)
	reg.MustRegister(eligibleParquetDeletes)

	return &GCMetrics{
		OrphanWALCount:         orphanWALCount,
		PendingWALDeletes:      pendingWALDeletes,
		PendingParquetDeletes:  pendingParquetDeletes,
		StagingWALCount:        stagingWALCount,
		EligibleWALDeletes:     eligibleWALDeletes,
		EligibleParquetDeletes: eligibleParquetDeletes,
	}
}

// RecordOrphanWALCount updates the orphan WAL count metric.
func (m *GCMetrics) RecordOrphanWALCount(count int64) {
	m.OrphanWALCount.Set(float64(count))
}

// RecordPendingWALDeletes updates the pending WAL deletes metric.
func (m *GCMetrics) RecordPendingWALDeletes(count int64) {
	m.PendingWALDeletes.Set(float64(count))
}

// RecordPendingParquetDeletes updates the pending Parquet deletes metric.
func (m *GCMetrics) RecordPendingParquetDeletes(count int64) {
	m.PendingParquetDeletes.Set(float64(count))
}

// RecordStagingWALCount updates the staging WAL count metric.
func (m *GCMetrics) RecordStagingWALCount(count int64) {
	m.StagingWALCount.Set(float64(count))
}

// RecordEligibleWALDeletes updates the eligible WAL deletes metric.
func (m *GCMetrics) RecordEligibleWALDeletes(count int64) {
	m.EligibleWALDeletes.Set(float64(count))
}

// RecordEligibleParquetDeletes updates the eligible Parquet deletes metric.
func (m *GCMetrics) RecordEligibleParquetDeletes(count int64) {
	m.EligibleParquetDeletes.Set(float64(count))
}

// GCStatsProvider provides GC statistics for metrics collection.
type GCStatsProvider interface {
	// GetOrphanWALCount returns the number of orphaned WAL staging markers.
	GetOrphanWALCount(ctx context.Context) (int, error)
	// GetStagingWALCount returns the total number of WAL staging markers.
	GetStagingWALCount(ctx context.Context) (int, error)
	// GetPendingWALDeleteCount returns the number of WAL objects pending deletion.
	GetPendingWALDeleteCount(ctx context.Context) (int, error)
	// GetEligibleWALDeleteCount returns the number of WAL objects eligible for deletion.
	GetEligibleWALDeleteCount(ctx context.Context) (int, error)
	// GetPendingParquetDeleteCount returns the number of Parquet files pending deletion.
	GetPendingParquetDeleteCount(ctx context.Context) (int, error)
	// GetEligibleParquetDeleteCount returns the number of Parquet files eligible for deletion.
	GetEligibleParquetDeleteCount(ctx context.Context) (int, error)
}

// GCBacklogScanner periodically scans GC state and updates metrics.
type GCBacklogScanner struct {
	metrics  *GCMetrics
	provider GCStatsProvider
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewGCBacklogScanner creates a scanner that periodically updates GC backlog metrics.
func NewGCBacklogScanner(metrics *GCMetrics, provider GCStatsProvider, interval time.Duration) *GCBacklogScanner {
	return &GCBacklogScanner{
		metrics:  metrics,
		provider: provider,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins periodic GC backlog scanning.
func (s *GCBacklogScanner) Start() {
	s.wg.Add(1)
	go s.loop()
}

// Stop halts periodic GC backlog scanning.
func (s *GCBacklogScanner) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// loop runs the periodic scan.
func (s *GCBacklogScanner) loop() {
	defer s.wg.Done()

	// Run immediately on start
	s.scanOnce()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scanOnce()
		}
	}
}

// scanOnce performs a single GC backlog scan.
func (s *GCBacklogScanner) scanOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if orphans, err := s.provider.GetOrphanWALCount(ctx); err != nil {
		slog.Warn("gc backlog scan failed",
			"provider", "orphan_wal_count",
			"error", err,
		)
	} else {
		s.metrics.RecordOrphanWALCount(int64(orphans))
	}

	if staging, err := s.provider.GetStagingWALCount(ctx); err != nil {
		slog.Warn("gc backlog scan failed",
			"provider", "staging_wal_count",
			"error", err,
		)
	} else {
		s.metrics.RecordStagingWALCount(int64(staging))
	}

	if pending, err := s.provider.GetPendingWALDeleteCount(ctx); err != nil {
		slog.Warn("gc backlog scan failed",
			"provider", "pending_wal_delete_count",
			"error", err,
		)
	} else {
		s.metrics.RecordPendingWALDeletes(int64(pending))
	}

	if eligible, err := s.provider.GetEligibleWALDeleteCount(ctx); err != nil {
		slog.Warn("gc backlog scan failed",
			"provider", "eligible_wal_delete_count",
			"error", err,
		)
	} else {
		s.metrics.RecordEligibleWALDeletes(int64(eligible))
	}

	if pending, err := s.provider.GetPendingParquetDeleteCount(ctx); err != nil {
		slog.Warn("gc backlog scan failed",
			"provider", "pending_parquet_delete_count",
			"error", err,
		)
	} else {
		s.metrics.RecordPendingParquetDeletes(int64(pending))
	}

	if eligible, err := s.provider.GetEligibleParquetDeleteCount(ctx); err != nil {
		slog.Warn("gc backlog scan failed",
			"provider", "eligible_parquet_delete_count",
			"error", err,
		)
	} else {
		s.metrics.RecordEligibleParquetDeletes(int64(eligible))
	}
}

// ScanOnce triggers a single scan and updates metrics.
// Useful for testing or on-demand scanning.
func (s *GCBacklogScanner) ScanOnce() {
	s.scanOnce()
}
