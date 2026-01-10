package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CompactionMetrics holds metrics related to compaction backlog and job execution.
type CompactionMetrics struct {
	// PendingBytesGauge tracks total WAL bytes pending compaction across all streams.
	PendingBytesGauge prometheus.Gauge

	// PendingFilesGauge tracks total WAL files pending compaction across all streams.
	PendingFilesGauge prometheus.Gauge

	// BacklogExceededGauge is set to 1 when backlog exceeds threshold, 0 otherwise.
	// This can be used for alerting.
	BacklogExceededGauge prometheus.Gauge

	// StreamPendingBytesGauge tracks WAL bytes pending compaction per stream.
	// Labels: stream_id
	StreamPendingBytesGauge *prometheus.GaugeVec

	// StreamPendingFilesGauge tracks WAL files pending compaction per stream.
	// Labels: stream_id
	StreamPendingFilesGauge *prometheus.GaugeVec

	// JobsActiveGauge tracks the number of active compaction jobs.
	// Labels: job_type (wal, parquet_rewrite)
	JobsActiveGauge *prometheus.GaugeVec

	// JobDurationHistogram tracks compaction job duration in seconds.
	// Labels: job_type (wal, parquet_rewrite), status (success, failed)
	JobDurationHistogram *prometheus.HistogramVec

	// JobBytesProcessedCounter tracks total bytes processed by compaction jobs.
	// Labels: job_type (wal, parquet_rewrite)
	JobBytesProcessedCounter *prometheus.CounterVec

	// JobRecordsProcessedCounter tracks total records processed by compaction jobs.
	// Labels: job_type (wal, parquet_rewrite)
	JobRecordsProcessedCounter *prometheus.CounterVec

	// JobSourceBytesGauge tracks source bytes for the current active job (useful for OOM debugging).
	// Labels: job_type, stream_id
	JobSourceBytesGauge *prometheus.GaugeVec

	// mu protects threshold configuration.
	mu sync.RWMutex
	// bytesThreshold is the threshold in bytes for backlog alerting.
	bytesThreshold int64
	// filesThreshold is the threshold in file count for backlog alerting.
	filesThreshold int64
}

// NewCompactionMetrics creates and registers compaction metrics.
// Uses promauto for automatic registration with the default registry.
func NewCompactionMetrics() *CompactionMetrics {
	return &CompactionMetrics{
		PendingBytesGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "pending_bytes",
				Help:      "Total WAL bytes pending compaction across all streams.",
			},
		),
		PendingFilesGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "pending_files",
				Help:      "Total WAL files pending compaction across all streams.",
			},
		),
		BacklogExceededGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "backlog_exceeded",
				Help:      "Set to 1 when compaction backlog exceeds threshold, 0 otherwise.",
			},
		),
		StreamPendingBytesGauge: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "stream_pending_bytes",
				Help:      "WAL bytes pending compaction per stream.",
			},
			[]string{"stream_id"},
		),
		StreamPendingFilesGauge: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "stream_pending_files",
				Help:      "WAL files pending compaction per stream.",
			},
			[]string{"stream_id"},
		),
		JobsActiveGauge: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "jobs_active",
				Help:      "Number of active compaction jobs.",
			},
			[]string{"job_type"},
		),
		JobDurationHistogram: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "job_duration_seconds",
				Help:      "Duration of compaction jobs in seconds.",
				Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600},
			},
			[]string{"job_type", "status"},
		),
		JobBytesProcessedCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "job_bytes_processed_total",
				Help:      "Total bytes processed by compaction jobs.",
			},
			[]string{"job_type"},
		),
		JobRecordsProcessedCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "job_records_processed_total",
				Help:      "Total records processed by compaction jobs.",
			},
			[]string{"job_type"},
		),
		JobSourceBytesGauge: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "dray",
				Subsystem: "compaction",
				Name:      "job_source_bytes",
				Help:      "Source bytes for current active compaction job (for OOM debugging).",
			},
			[]string{"job_type", "stream_id"},
		),
		bytesThreshold: 0, // 0 means alerting disabled
		filesThreshold: 0, // 0 means alerting disabled
	}
}

// NewCompactionMetricsWithRegistry creates compaction metrics registered with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewCompactionMetricsWithRegistry(reg prometheus.Registerer) *CompactionMetrics {
	pendingBytes := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "pending_bytes",
			Help:      "Total WAL bytes pending compaction across all streams.",
		},
	)

	pendingFiles := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "pending_files",
			Help:      "Total WAL files pending compaction across all streams.",
		},
	)

	backlogExceeded := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "backlog_exceeded",
			Help:      "Set to 1 when compaction backlog exceeds threshold, 0 otherwise.",
		},
	)

	streamPendingBytes := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "stream_pending_bytes",
			Help:      "WAL bytes pending compaction per stream.",
		},
		[]string{"stream_id"},
	)

	streamPendingFiles := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "stream_pending_files",
			Help:      "WAL files pending compaction per stream.",
		},
		[]string{"stream_id"},
	)

	jobsActive := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "jobs_active",
			Help:      "Number of active compaction jobs.",
		},
		[]string{"job_type"},
	)

	jobDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "job_duration_seconds",
			Help:      "Duration of compaction jobs in seconds.",
			Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600},
		},
		[]string{"job_type", "status"},
	)

	jobBytesProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "job_bytes_processed_total",
			Help:      "Total bytes processed by compaction jobs.",
		},
		[]string{"job_type"},
	)

	jobRecordsProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "job_records_processed_total",
			Help:      "Total records processed by compaction jobs.",
		},
		[]string{"job_type"},
	)

	jobSourceBytes := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dray",
			Subsystem: "compaction",
			Name:      "job_source_bytes",
			Help:      "Source bytes for current active compaction job (for OOM debugging).",
		},
		[]string{"job_type", "stream_id"},
	)

	reg.MustRegister(pendingBytes)
	reg.MustRegister(pendingFiles)
	reg.MustRegister(backlogExceeded)
	reg.MustRegister(streamPendingBytes)
	reg.MustRegister(streamPendingFiles)
	reg.MustRegister(jobsActive)
	reg.MustRegister(jobDuration)
	reg.MustRegister(jobBytesProcessed)
	reg.MustRegister(jobRecordsProcessed)
	reg.MustRegister(jobSourceBytes)

	return &CompactionMetrics{
		PendingBytesGauge:          pendingBytes,
		PendingFilesGauge:          pendingFiles,
		BacklogExceededGauge:       backlogExceeded,
		StreamPendingBytesGauge:    streamPendingBytes,
		StreamPendingFilesGauge:    streamPendingFiles,
		JobsActiveGauge:            jobsActive,
		JobDurationHistogram:       jobDuration,
		JobBytesProcessedCounter:   jobBytesProcessed,
		JobRecordsProcessedCounter: jobRecordsProcessed,
		JobSourceBytesGauge:        jobSourceBytes,
		bytesThreshold:             0,
		filesThreshold:             0,
	}
}

// SetThresholds configures the alerting thresholds.
// Set to 0 to disable that threshold. If either threshold is exceeded, the alert fires.
func (m *CompactionMetrics) SetThresholds(bytesThreshold, filesThreshold int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bytesThreshold = bytesThreshold
	m.filesThreshold = filesThreshold
}

// GetThresholds returns the current alerting thresholds.
func (m *CompactionMetrics) GetThresholds() (bytesThreshold, filesThreshold int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bytesThreshold, m.filesThreshold
}

// RecordBacklog updates the compaction backlog metrics.
// totalBytes is the total WAL bytes pending compaction.
// totalFiles is the total WAL files pending compaction.
func (m *CompactionMetrics) RecordBacklog(totalBytes, totalFiles int64) {
	m.PendingBytesGauge.Set(float64(totalBytes))
	m.PendingFilesGauge.Set(float64(totalFiles))

	// Check thresholds and update alert gauge
	m.mu.RLock()
	bytesThreshold := m.bytesThreshold
	filesThreshold := m.filesThreshold
	m.mu.RUnlock()

	exceeded := false
	if bytesThreshold > 0 && totalBytes > bytesThreshold {
		exceeded = true
	}
	if filesThreshold > 0 && totalFiles > filesThreshold {
		exceeded = true
	}

	if exceeded {
		m.BacklogExceededGauge.Set(1)
	} else {
		m.BacklogExceededGauge.Set(0)
	}
}

// RecordStreamBacklog updates the per-stream compaction backlog metrics.
func (m *CompactionMetrics) RecordStreamBacklog(streamID string, bytes, files int64) {
	m.StreamPendingBytesGauge.WithLabelValues(streamID).Set(float64(bytes))
	m.StreamPendingFilesGauge.WithLabelValues(streamID).Set(float64(files))
}

// ClearStreamBacklog removes the metrics for a stream (e.g., after deletion).
func (m *CompactionMetrics) ClearStreamBacklog(streamID string) {
	m.StreamPendingBytesGauge.DeleteLabelValues(streamID)
	m.StreamPendingFilesGauge.DeleteLabelValues(streamID)
}

// JobType represents the type of compaction job.
type JobType string

const (
	// JobTypeWAL is a WAL-to-Parquet compaction job.
	JobTypeWAL JobType = "wal"
	// JobTypeParquetRewrite is a Parquet rewrite/merge job.
	JobTypeParquetRewrite JobType = "parquet_rewrite"
)

// JobTracker tracks metrics for a single compaction job.
// Use StartJob to create one and call Complete or Failed when done.
type JobTracker struct {
	metrics    *CompactionMetrics
	jobType    JobType
	streamID   string
	startTime  time.Time
	sourceSize int64
}

// StartJob begins tracking a new compaction job.
// Call Complete() or Failed() when the job finishes.
func (m *CompactionMetrics) StartJob(jobType JobType, streamID string, sourceSizeBytes int64) *JobTracker {
	if m.JobsActiveGauge != nil {
		m.JobsActiveGauge.WithLabelValues(string(jobType)).Inc()
	}
	if m.JobSourceBytesGauge != nil {
		m.JobSourceBytesGauge.WithLabelValues(string(jobType), streamID).Set(float64(sourceSizeBytes))
	}

	return &JobTracker{
		metrics:    m,
		jobType:    jobType,
		streamID:   streamID,
		startTime:  time.Now(),
		sourceSize: sourceSizeBytes,
	}
}

// Complete marks the job as successfully completed and records metrics.
func (t *JobTracker) Complete(bytesWritten, recordsProcessed int64) {
	duration := time.Since(t.startTime).Seconds()

	if t.metrics.JobsActiveGauge != nil {
		t.metrics.JobsActiveGauge.WithLabelValues(string(t.jobType)).Dec()
	}
	if t.metrics.JobSourceBytesGauge != nil {
		t.metrics.JobSourceBytesGauge.DeleteLabelValues(string(t.jobType), t.streamID)
	}
	if t.metrics.JobDurationHistogram != nil {
		t.metrics.JobDurationHistogram.WithLabelValues(string(t.jobType), "success").Observe(duration)
	}
	if t.metrics.JobBytesProcessedCounter != nil {
		t.metrics.JobBytesProcessedCounter.WithLabelValues(string(t.jobType)).Add(float64(bytesWritten))
	}
	if t.metrics.JobRecordsProcessedCounter != nil {
		t.metrics.JobRecordsProcessedCounter.WithLabelValues(string(t.jobType)).Add(float64(recordsProcessed))
	}
}

// Failed marks the job as failed and records duration.
func (t *JobTracker) Failed() {
	duration := time.Since(t.startTime).Seconds()

	if t.metrics.JobsActiveGauge != nil {
		t.metrics.JobsActiveGauge.WithLabelValues(string(t.jobType)).Dec()
	}
	if t.metrics.JobSourceBytesGauge != nil {
		t.metrics.JobSourceBytesGauge.DeleteLabelValues(string(t.jobType), t.streamID)
	}
	if t.metrics.JobDurationHistogram != nil {
		t.metrics.JobDurationHistogram.WithLabelValues(string(t.jobType), "failed").Observe(duration)
	}
}

// BacklogScanResult contains the results of scanning a stream for compaction backlog.
type BacklogScanResult struct {
	StreamID   string
	WALBytes   int64
	WALFiles   int64
	HasParquet bool // true if stream has any compacted Parquet files
}

// BacklogScanner calculates compaction backlog by scanning stream indices.
type BacklogScanner struct {
	metrics     *CompactionMetrics
	indexLister IndexLister
	interval    time.Duration
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// IndexLister provides access to stream index entries for backlog scanning.
type IndexLister interface {
	// ListStreams returns all stream IDs.
	ListStreams(ctx context.Context) ([]string, error)
	// ListWALEntries returns WAL index entries for a stream that are pending compaction.
	// Returns entries from the oldest up to the first Parquet entry (or all if no Parquet exists).
	ListWALEntries(ctx context.Context, streamID string) ([]WALEntryInfo, error)
}

// WALEntryInfo contains minimal information about a WAL entry for backlog calculation.
type WALEntryInfo struct {
	SizeBytes int64
}

// NewBacklogScanner creates a scanner that periodically updates compaction backlog metrics.
func NewBacklogScanner(metrics *CompactionMetrics, indexLister IndexLister, interval time.Duration) *BacklogScanner {
	return &BacklogScanner{
		metrics:     metrics,
		indexLister: indexLister,
		interval:    interval,
		stopCh:      make(chan struct{}),
	}
}

// Start begins periodic backlog scanning.
func (s *BacklogScanner) Start() {
	s.wg.Add(1)
	go s.loop()
}

// Stop halts periodic backlog scanning.
func (s *BacklogScanner) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// loop runs the periodic scan.
func (s *BacklogScanner) loop() {
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

// scanOnce performs a single backlog scan.
func (s *BacklogScanner) scanOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := s.ScanAll(ctx)
	if err != nil {
		slog.Warn("compaction backlog scan failed",
			"scope", "all_streams",
			"error", err,
		)
		return
	}

	var totalBytes, totalFiles int64
	for _, result := range results {
		totalBytes += result.WALBytes
		totalFiles += result.WALFiles
		s.metrics.RecordStreamBacklog(result.StreamID, result.WALBytes, result.WALFiles)
	}

	s.metrics.RecordBacklog(totalBytes, totalFiles)
}

// ScanAll scans all streams and returns backlog information.
func (s *BacklogScanner) ScanAll(ctx context.Context) ([]BacklogScanResult, error) {
	streams, err := s.indexLister.ListStreams(ctx)
	if err != nil {
		slog.Warn("compaction backlog scan failed to list streams",
			"scope", "list_streams",
			"error", err,
		)
		return nil, err
	}

	results := make([]BacklogScanResult, 0, len(streams))
	for _, streamID := range streams {
		result, err := s.ScanStream(ctx, streamID)
		if err != nil {
			slog.Warn("compaction backlog scan failed for stream",
				"scope", "stream",
				"stream_id", streamID,
				"error", err,
			)
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

// ScanStream scans a single stream for compaction backlog.
func (s *BacklogScanner) ScanStream(ctx context.Context, streamID string) (BacklogScanResult, error) {
	entries, err := s.indexLister.ListWALEntries(ctx, streamID)
	if err != nil {
		return BacklogScanResult{}, err
	}

	var totalBytes int64
	for _, entry := range entries {
		totalBytes += entry.SizeBytes
	}

	return BacklogScanResult{
		StreamID: streamID,
		WALBytes: totalBytes,
		WALFiles: int64(len(entries)),
	}, nil
}

// ScanOnce triggers a single scan and updates metrics.
// Useful for testing or on-demand scanning.
func (s *BacklogScanner) ScanOnce() {
	s.scanOnce()
}
