package metrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewCompactionMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	if m.PendingBytesGauge == nil {
		t.Fatal("PendingBytesGauge is nil")
	}
	if m.PendingFilesGauge == nil {
		t.Fatal("PendingFilesGauge is nil")
	}
	if m.BacklogExceededGauge == nil {
		t.Fatal("BacklogExceededGauge is nil")
	}
	if m.StreamPendingBytesGauge == nil {
		t.Fatal("StreamPendingBytesGauge is nil")
	}
	if m.StreamPendingFilesGauge == nil {
		t.Fatal("StreamPendingFilesGauge is nil")
	}
}

func TestCompactionMetrics_RecordBacklog(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Record some backlog
	m.RecordBacklog(1024*1024, 5) // 1MB, 5 files

	// Verify pending bytes
	bytesMetric := &dto.Metric{}
	if err := m.PendingBytesGauge.Write(bytesMetric); err != nil {
		t.Fatalf("failed to write bytes metric: %v", err)
	}
	if got := bytesMetric.Gauge.GetValue(); got != float64(1024*1024) {
		t.Errorf("pending bytes = %f, want %d", got, 1024*1024)
	}

	// Verify pending files
	filesMetric := &dto.Metric{}
	if err := m.PendingFilesGauge.Write(filesMetric); err != nil {
		t.Fatalf("failed to write files metric: %v", err)
	}
	if got := filesMetric.Gauge.GetValue(); got != 5 {
		t.Errorf("pending files = %f, want 5", got)
	}

	// Verify backlog not exceeded (no threshold set)
	exceededMetric := &dto.Metric{}
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 0 {
		t.Errorf("backlog_exceeded = %f, want 0 (no threshold)", got)
	}
}

func TestCompactionMetrics_RecordBacklogUpdatesValue(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Record initial backlog
	m.RecordBacklog(1000, 10)

	// Update with new values
	m.RecordBacklog(5000, 25)

	// Verify updated pending bytes
	bytesMetric := &dto.Metric{}
	if err := m.PendingBytesGauge.Write(bytesMetric); err != nil {
		t.Fatalf("failed to write bytes metric: %v", err)
	}
	if got := bytesMetric.Gauge.GetValue(); got != 5000 {
		t.Errorf("pending bytes = %f, want 5000", got)
	}

	// Verify updated pending files
	filesMetric := &dto.Metric{}
	if err := m.PendingFilesGauge.Write(filesMetric); err != nil {
		t.Fatalf("failed to write files metric: %v", err)
	}
	if got := filesMetric.Gauge.GetValue(); got != 25 {
		t.Errorf("pending files = %f, want 25", got)
	}
}

func TestCompactionMetrics_ThresholdExceededBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Set threshold
	m.SetThresholds(1024, 0) // 1KB bytes threshold

	// Record backlog below threshold
	m.RecordBacklog(512, 5)

	exceededMetric := &dto.Metric{}
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 0 {
		t.Errorf("backlog_exceeded = %f, want 0 (below threshold)", got)
	}

	// Record backlog above threshold
	m.RecordBacklog(2048, 5)

	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 1 {
		t.Errorf("backlog_exceeded = %f, want 1 (above threshold)", got)
	}
}

func TestCompactionMetrics_ThresholdExceededFiles(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Set threshold
	m.SetThresholds(0, 10) // 10 files threshold

	// Record backlog below threshold
	m.RecordBacklog(1024*1024, 5)

	exceededMetric := &dto.Metric{}
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 0 {
		t.Errorf("backlog_exceeded = %f, want 0 (below threshold)", got)
	}

	// Record backlog above threshold
	m.RecordBacklog(1024*1024, 15)

	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 1 {
		t.Errorf("backlog_exceeded = %f, want 1 (above threshold)", got)
	}
}

func TestCompactionMetrics_ThresholdExceededEither(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Set both thresholds
	m.SetThresholds(1024, 10)

	// Below both
	m.RecordBacklog(512, 5)
	exceededMetric := &dto.Metric{}
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 0 {
		t.Errorf("backlog_exceeded = %f, want 0", got)
	}

	// Exceed bytes only
	m.RecordBacklog(2048, 5)
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 1 {
		t.Errorf("backlog_exceeded = %f, want 1 (bytes exceeded)", got)
	}

	// Exceed files only
	m.RecordBacklog(512, 15)
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 1 {
		t.Errorf("backlog_exceeded = %f, want 1 (files exceeded)", got)
	}

	// Exceed both
	m.RecordBacklog(2048, 15)
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 1 {
		t.Errorf("backlog_exceeded = %f, want 1 (both exceeded)", got)
	}
}

func TestCompactionMetrics_ThresholdClears(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Set threshold and exceed it
	m.SetThresholds(1024, 0)
	m.RecordBacklog(2048, 5)

	exceededMetric := &dto.Metric{}
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 1 {
		t.Errorf("backlog_exceeded = %f, want 1", got)
	}

	// Record backlog below threshold - should clear alert
	m.RecordBacklog(512, 5)
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 0 {
		t.Errorf("backlog_exceeded = %f, want 0 (alert cleared)", got)
	}
}

func TestCompactionMetrics_GetThresholds(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Default thresholds
	bytes, files := m.GetThresholds()
	if bytes != 0 || files != 0 {
		t.Errorf("default thresholds = (%d, %d), want (0, 0)", bytes, files)
	}

	// Set thresholds
	m.SetThresholds(1024*1024, 100)
	bytes, files = m.GetThresholds()
	if bytes != 1024*1024 {
		t.Errorf("bytes threshold = %d, want %d", bytes, 1024*1024)
	}
	if files != 100 {
		t.Errorf("files threshold = %d, want 100", files)
	}
}

func TestCompactionMetrics_RecordStreamBacklog(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Record backlog for multiple streams
	m.RecordStreamBacklog("stream-1", 1024, 2)
	m.RecordStreamBacklog("stream-2", 2048, 3)
	m.RecordStreamBacklog("stream-3", 512, 1)

	// Gather all metrics
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Find stream pending bytes metric
	var streamBytesFound int
	for _, family := range families {
		if family.GetName() == "dray_compaction_stream_pending_bytes" {
			streamBytesFound = len(family.GetMetric())
		}
	}

	if streamBytesFound != 3 {
		t.Errorf("expected 3 stream_pending_bytes metrics, got %d", streamBytesFound)
	}
}

func TestCompactionMetrics_ClearStreamBacklog(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Record backlog
	m.RecordStreamBacklog("stream-1", 1024, 2)
	m.RecordStreamBacklog("stream-2", 2048, 3)

	// Clear one stream
	m.ClearStreamBacklog("stream-1")

	// Gather all metrics
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Find stream pending bytes metric
	var streamBytesFound int
	for _, family := range families {
		if family.GetName() == "dray_compaction_stream_pending_bytes" {
			streamBytesFound = len(family.GetMetric())
		}
	}

	if streamBytesFound != 1 {
		t.Errorf("expected 1 stream_pending_bytes metric after clear, got %d", streamBytesFound)
	}
}

// mockIndexLister implements IndexLister for testing.
type mockIndexLister struct {
	streams    []string
	walEntries map[string][]WALEntryInfo
	listErr    error
	entriesErr error
}

func (m *mockIndexLister) ListStreams(ctx context.Context) ([]string, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.streams, nil
}

func (m *mockIndexLister) ListWALEntries(ctx context.Context, streamID string) ([]WALEntryInfo, error) {
	if m.entriesErr != nil {
		return nil, m.entriesErr
	}
	return m.walEntries[streamID], nil
}

func TestBacklogScanner_ScanStream(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	lister := &mockIndexLister{
		streams: []string{"stream-1"},
		walEntries: map[string][]WALEntryInfo{
			"stream-1": {
				{SizeBytes: 1024},
				{SizeBytes: 2048},
				{SizeBytes: 512},
			},
		},
	}

	scanner := NewBacklogScanner(m, lister, time.Hour)

	result, err := scanner.ScanStream(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("ScanStream failed: %v", err)
	}

	if result.StreamID != "stream-1" {
		t.Errorf("StreamID = %s, want stream-1", result.StreamID)
	}
	if result.WALBytes != 3584 { // 1024 + 2048 + 512
		t.Errorf("WALBytes = %d, want 3584", result.WALBytes)
	}
	if result.WALFiles != 3 {
		t.Errorf("WALFiles = %d, want 3", result.WALFiles)
	}
}

func TestBacklogScanner_ScanAll(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	lister := &mockIndexLister{
		streams: []string{"stream-1", "stream-2"},
		walEntries: map[string][]WALEntryInfo{
			"stream-1": {
				{SizeBytes: 1024},
				{SizeBytes: 2048},
			},
			"stream-2": {
				{SizeBytes: 512},
			},
		},
	}

	scanner := NewBacklogScanner(m, lister, time.Hour)

	results, err := scanner.ScanAll(context.Background())
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Results should contain data for both streams
	var totalBytes, totalFiles int64
	for _, r := range results {
		totalBytes += r.WALBytes
		totalFiles += r.WALFiles
	}

	if totalBytes != 3584 { // 1024 + 2048 + 512
		t.Errorf("total WALBytes = %d, want 3584", totalBytes)
	}
	if totalFiles != 3 {
		t.Errorf("total WALFiles = %d, want 3", totalFiles)
	}
}

func TestBacklogScanner_ScanOnceUpdatesMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	lister := &mockIndexLister{
		streams: []string{"stream-1", "stream-2"},
		walEntries: map[string][]WALEntryInfo{
			"stream-1": {
				{SizeBytes: 1024},
				{SizeBytes: 2048},
			},
			"stream-2": {
				{SizeBytes: 512},
			},
		},
	}

	scanner := NewBacklogScanner(m, lister, time.Hour)

	// Trigger a scan
	scanner.ScanOnce()

	// Check total backlog metrics
	bytesMetric := &dto.Metric{}
	if err := m.PendingBytesGauge.Write(bytesMetric); err != nil {
		t.Fatalf("failed to write bytes metric: %v", err)
	}
	if got := bytesMetric.Gauge.GetValue(); got != 3584 {
		t.Errorf("pending bytes = %f, want 3584", got)
	}

	filesMetric := &dto.Metric{}
	if err := m.PendingFilesGauge.Write(filesMetric); err != nil {
		t.Fatalf("failed to write files metric: %v", err)
	}
	if got := filesMetric.Gauge.GetValue(); got != 3 {
		t.Errorf("pending files = %f, want 3", got)
	}
}

func TestBacklogScanner_StartStop(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	scanCount := 0
	var mu sync.Mutex

	lister := &mockIndexLister{
		streams: []string{"stream-1"},
		walEntries: map[string][]WALEntryInfo{
			"stream-1": {{SizeBytes: 1024}},
		},
	}

	// Wrap lister to count scans
	countingLister := &countingIndexLister{
		inner: lister,
		onListStreams: func() {
			mu.Lock()
			scanCount++
			mu.Unlock()
		},
	}

	scanner := NewBacklogScanner(m, countingLister, 50*time.Millisecond)

	// Start and let it run for a bit
	scanner.Start()
	time.Sleep(200 * time.Millisecond)
	scanner.Stop()

	mu.Lock()
	count := scanCount
	mu.Unlock()

	// Should have scanned at least once (on start) plus some periodic scans
	if count < 2 {
		t.Errorf("expected at least 2 scans, got %d", count)
	}
}

// countingIndexLister wraps an IndexLister to count calls.
type countingIndexLister struct {
	inner         IndexLister
	onListStreams func()
}

func (c *countingIndexLister) ListStreams(ctx context.Context) ([]string, error) {
	if c.onListStreams != nil {
		c.onListStreams()
	}
	return c.inner.ListStreams(ctx)
}

func (c *countingIndexLister) ListWALEntries(ctx context.Context, streamID string) ([]WALEntryInfo, error) {
	return c.inner.ListWALEntries(ctx, streamID)
}

func TestBacklogScanner_HandlesEmptyStreams(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	lister := &mockIndexLister{
		streams:    []string{},
		walEntries: map[string][]WALEntryInfo{},
	}

	scanner := NewBacklogScanner(m, lister, time.Hour)

	results, err := scanner.ScanAll(context.Background())
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestBacklogScanner_HandlesEmptyWALEntries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	lister := &mockIndexLister{
		streams: []string{"stream-1"},
		walEntries: map[string][]WALEntryInfo{
			"stream-1": {}, // empty WAL entries
		},
	}

	scanner := NewBacklogScanner(m, lister, time.Hour)

	result, err := scanner.ScanStream(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("ScanStream failed: %v", err)
	}

	if result.WALBytes != 0 {
		t.Errorf("WALBytes = %d, want 0", result.WALBytes)
	}
	if result.WALFiles != 0 {
		t.Errorf("WALFiles = %d, want 0", result.WALFiles)
	}
}

func TestBacklogScanner_ThresholdAlertTriggered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	// Set threshold
	m.SetThresholds(2000, 0)

	lister := &mockIndexLister{
		streams: []string{"stream-1"},
		walEntries: map[string][]WALEntryInfo{
			"stream-1": {
				{SizeBytes: 1024},
				{SizeBytes: 2048},
			},
		},
	}

	scanner := NewBacklogScanner(m, lister, time.Hour)
	scanner.ScanOnce()

	// Check alert is triggered (3072 > 2000)
	exceededMetric := &dto.Metric{}
	if err := m.BacklogExceededGauge.Write(exceededMetric); err != nil {
		t.Fatalf("failed to write exceeded metric: %v", err)
	}
	if got := exceededMetric.Gauge.GetValue(); got != 1 {
		t.Errorf("backlog_exceeded = %f, want 1 (threshold exceeded)", got)
	}
}

func TestCompactionMetrics_ConcurrentAccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetricsWithRegistry(reg)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.SetThresholds(int64(i*100+j), int64(j))
				m.RecordBacklog(int64(i*j), int64(j))
				m.GetThresholds()
			}
		}(i)
	}
	wg.Wait()
}
