package catalog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewAppender(t *testing.T) {
	catalog := newMockCatalog()

	t.Run("default config values", func(t *testing.T) {
		cfg := DefaultAppenderConfig(catalog)
		appender := NewAppender(cfg)

		if appender.cfg.MaxRetries != 5 {
			t.Errorf("expected MaxRetries 5, got %d", appender.cfg.MaxRetries)
		}
		if appender.cfg.InitialBackoff != 100*time.Millisecond {
			t.Errorf("expected InitialBackoff 100ms, got %v", appender.cfg.InitialBackoff)
		}
		if appender.cfg.MaxBackoff != 10*time.Second {
			t.Errorf("expected MaxBackoff 10s, got %v", appender.cfg.MaxBackoff)
		}
	})

	t.Run("custom config values", func(t *testing.T) {
		cfg := AppenderConfig{
			Catalog:           catalog,
			Namespace:         []string{"custom", "ns"},
			MaxRetries:        10,
			InitialBackoff:    50 * time.Millisecond,
			MaxBackoff:        5 * time.Second,
			BackoffMultiplier: 1.5,
			JitterFactor:      0.2,
		}
		appender := NewAppender(cfg)

		if appender.cfg.MaxRetries != 10 {
			t.Errorf("expected MaxRetries 10, got %d", appender.cfg.MaxRetries)
		}
		if len(appender.cfg.Namespace) != 2 || appender.cfg.Namespace[0] != "custom" {
			t.Errorf("expected namespace [custom ns], got %v", appender.cfg.Namespace)
		}
	})

	t.Run("fills defaults for zero values", func(t *testing.T) {
		cfg := AppenderConfig{Catalog: catalog}
		appender := NewAppender(cfg)

		if appender.cfg.MaxRetries != 5 {
			t.Errorf("expected MaxRetries 5, got %d", appender.cfg.MaxRetries)
		}
		if len(appender.cfg.Namespace) != 1 || appender.cfg.Namespace[0] != "dray" {
			t.Errorf("expected namespace [dray], got %v", appender.cfg.Namespace)
		}
	})
}

func TestAppender_AppendFiles(t *testing.T) {
	ctx := context.Background()

	t.Run("successful append on first try", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "test-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))

		files := []DataFile{
			{
				Path:           "s3://bucket/data/file.parquet",
				Format:         FormatParquet,
				PartitionValue: 0,
				RecordCount:    1000,
				FileSizeBytes:  10240,
			},
		}

		result, err := appender.AppendFiles(ctx, "test-topic", files, nil)
		if err != nil {
			t.Fatalf("AppendFiles failed: %v", err)
		}

		if result.Snapshot == nil {
			t.Error("expected non-nil snapshot")
		}
		if result.Attempts != 1 {
			t.Errorf("expected 1 attempt, got %d", result.Attempts)
		}
		if result.Table == nil {
			t.Error("expected non-nil table")
		}
	})

	t.Run("append with job ID", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "job-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))

		files := []DataFile{
			{Path: "s3://bucket/data/job.parquet", Format: FormatParquet, RecordCount: 100},
		}

		result, err := appender.AppendFilesForStream(ctx, "job-topic", "job-123", files)
		if err != nil {
			t.Fatalf("AppendFilesForStream failed: %v", err)
		}

		if result.Snapshot == nil {
			t.Error("expected non-nil snapshot")
		}
	})

	t.Run("table not found", func(t *testing.T) {
		catalog := newMockCatalog()
		appender := NewAppender(DefaultAppenderConfig(catalog))

		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		_, err := appender.AppendFiles(ctx, "missing-topic", files, nil)
		if !errors.Is(err, ErrTableNotFound) {
			t.Errorf("expected ErrTableNotFound, got %v", err)
		}
	})

	t.Run("empty files slice", func(t *testing.T) {
		catalog := newMockCatalog()
		appender := NewAppender(DefaultAppenderConfig(catalog))

		_, err := appender.AppendFiles(ctx, "topic", []DataFile{}, nil)
		if err == nil {
			t.Error("expected error for empty files")
		}
	})

	t.Run("nil catalog", func(t *testing.T) {
		appender := NewAppender(AppenderConfig{Catalog: nil})

		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}
		_, err := appender.AppendFiles(ctx, "topic", files, nil)
		if !errors.Is(err, ErrCatalogUnavailable) {
			t.Errorf("expected ErrCatalogUnavailable, got %v", err)
		}
	})

	t.Run("snapshot is updated after append", func(t *testing.T) {
		catalog := newMockCatalog()
		identifier := TableIdentifier{Namespace: []string{"dray"}, Name: "snapshot-topic"}
		table, _ := catalog.CreateTableIfMissing(ctx, identifier, CreateTableOptions{Schema: DefaultSchema()})

		// Initially no snapshot
		_, err := table.CurrentSnapshot(ctx)
		if !errors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("expected no snapshot initially, got %v", err)
		}

		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/first.parquet", Format: FormatParquet, RecordCount: 50}}

		result1, err := appender.AppendFiles(ctx, "snapshot-topic", files, nil)
		if err != nil {
			t.Fatalf("first append failed: %v", err)
		}

		// Verify snapshot was created
		snap1, err := table.CurrentSnapshot(ctx)
		if err != nil {
			t.Fatalf("CurrentSnapshot failed: %v", err)
		}
		if snap1.SnapshotID != result1.Snapshot.SnapshotID {
			t.Errorf("snapshot ID mismatch: table=%d result=%d", snap1.SnapshotID, result1.Snapshot.SnapshotID)
		}

		// Append more files
		files2 := []DataFile{{Path: "s3://bucket/second.parquet", Format: FormatParquet, RecordCount: 75}}
		result2, err := appender.AppendFiles(ctx, "snapshot-topic", files2, nil)
		if err != nil {
			t.Fatalf("second append failed: %v", err)
		}

		// Verify new snapshot
		snap2, err := table.CurrentSnapshot(ctx)
		if err != nil {
			t.Fatalf("CurrentSnapshot failed: %v", err)
		}
		if snap2.SnapshotID != result2.Snapshot.SnapshotID {
			t.Errorf("snapshot ID mismatch after second append")
		}
		if snap2.SnapshotID <= snap1.SnapshotID {
			t.Error("new snapshot ID should be greater than previous")
		}
		if snap2.ParentSnapshotID == nil || *snap2.ParentSnapshotID != snap1.SnapshotID {
			t.Error("new snapshot should reference previous as parent")
		}
	})

	t.Run("append with full data file stats", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "stats-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))

		stats := DefaultDataFileStats(0, 100, 199, 1000000, 2000000, 100)
		df := BuildDataFileFromStats("s3://bucket/stats.parquet", 0, 100, 10240, stats)

		result, err := appender.AppendFiles(ctx, "stats-topic", []DataFile{df}, nil)
		if err != nil {
			t.Fatalf("AppendFiles with stats failed: %v", err)
		}
		if result.Snapshot == nil {
			t.Error("expected snapshot")
		}
	})
}

func TestAppender_RetryOnConflict(t *testing.T) {
	ctx := context.Background()

	t.Run("retries on commit conflict", func(t *testing.T) {
		catalog := newConflictMockCatalog(2) // Fail first 2 attempts
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "retry-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		cfg := AppenderConfig{
			Catalog:        catalog,
			Namespace:      []string{"dray"},
			MaxRetries:     5,
			InitialBackoff: 1 * time.Millisecond, // Fast for testing
			MaxBackoff:     10 * time.Millisecond,
		}
		appender := NewAppender(cfg)

		files := []DataFile{{Path: "s3://bucket/retry.parquet", Format: FormatParquet, RecordCount: 100}}

		result, err := appender.AppendFiles(ctx, "retry-topic", files, nil)
		if err != nil {
			t.Fatalf("AppendFiles should succeed after retries: %v", err)
		}

		if result.Attempts != 3 { // Failed 2, succeeded on 3rd
			t.Errorf("expected 3 attempts, got %d", result.Attempts)
		}
	})

	t.Run("fails after max retries", func(t *testing.T) {
		catalog := newConflictMockCatalog(10) // Always fail
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "always-fail-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		cfg := AppenderConfig{
			Catalog:        catalog,
			Namespace:      []string{"dray"},
			MaxRetries:     3,
			InitialBackoff: 1 * time.Millisecond,
		}
		appender := NewAppender(cfg)

		files := []DataFile{{Path: "s3://bucket/fail.parquet", Format: FormatParquet, RecordCount: 100}}

		_, err := appender.AppendFiles(ctx, "always-fail-topic", files, nil)
		if err == nil {
			t.Error("expected error after max retries")
		}
		if !errors.Is(err, ErrCommitConflict) {
			t.Errorf("expected ErrCommitConflict, got %v", err)
		}
	})

	t.Run("does not retry non-conflict errors", func(t *testing.T) {
		errCatalog := newErrorMockCatalog(errors.New("internal error"))
		_, _ = errCatalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "error-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		cfg := AppenderConfig{
			Catalog:        errCatalog,
			Namespace:      []string{"dray"},
			MaxRetries:     5,
			InitialBackoff: 1 * time.Millisecond,
		}
		appender := NewAppender(cfg)

		files := []DataFile{{Path: "s3://bucket/error.parquet", Format: FormatParquet, RecordCount: 100}}

		_, err := appender.AppendFiles(ctx, "error-topic", files, nil)
		if err == nil {
			t.Error("expected error")
		}

		// Should fail immediately, not after retries
		if errCatalog.appendCalls > 1 {
			t.Errorf("should not retry non-conflict errors, called %d times", errCatalog.appendCalls)
		}
	})

	t.Run("respects context cancellation during retry", func(t *testing.T) {
		catalog := newConflictMockCatalog(10) // Always fail
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "cancel-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		cfg := AppenderConfig{
			Catalog:        catalog,
			Namespace:      []string{"dray"},
			MaxRetries:     10,
			InitialBackoff: 100 * time.Millisecond,
		}
		appender := NewAppender(cfg)

		cancelCtx, cancel := context.WithCancel(ctx)

		files := []DataFile{{Path: "s3://bucket/cancel.parquet", Format: FormatParquet, RecordCount: 100}}

		// Cancel after a short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		_, err := appender.AppendFiles(cancelCtx, "cancel-topic", files, nil)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestAppender_BackoffCalculation(t *testing.T) {
	t.Run("backoff with no jitter", func(t *testing.T) {
		appender := &Appender{cfg: AppenderConfig{JitterFactor: 0}}
		backoff := appender.calculateBackoff(100 * time.Millisecond)
		if backoff != 100*time.Millisecond {
			t.Errorf("expected 100ms with no jitter, got %v", backoff)
		}
	})

	t.Run("backoff with jitter is within range", func(t *testing.T) {
		appender := &Appender{cfg: AppenderConfig{JitterFactor: 0.1}}

		// Run multiple times to check jitter range
		for i := 0; i < 100; i++ {
			base := 100 * time.Millisecond
			backoff := appender.calculateBackoff(base)

			minExpected := time.Duration(float64(base) * 0.9)
			maxExpected := time.Duration(float64(base) * 1.1)

			if backoff < minExpected || backoff > maxExpected {
				t.Errorf("backoff %v outside expected range [%v, %v]", backoff, minExpected, maxExpected)
			}
		}
	})
}

func TestBuildDataFileFromStats(t *testing.T) {
	t.Run("with stats", func(t *testing.T) {
		sortOrder := int32(1)
		stats := &DataFileStats{
			ColumnSizes:     map[int32]int64{1: 100, 2: 200},
			ValueCounts:     map[int32]int64{1: 1000},
			NullValueCounts: map[int32]int64{2: 50},
			LowerBounds:     map[int32][]byte{1: {0, 0, 0, 0}},
			UpperBounds:     map[int32][]byte{1: {0, 0, 0, 100}},
			SplitOffsets:    []int64{0, 1024},
			SortOrderID:     &sortOrder,
		}

		df := BuildDataFileFromStats("s3://bucket/file.parquet", 5, 1000, 10240, stats)

		if df.Path != "s3://bucket/file.parquet" {
			t.Errorf("expected path s3://bucket/file.parquet, got %s", df.Path)
		}
		if df.Format != FormatParquet {
			t.Errorf("expected format PARQUET, got %s", df.Format)
		}
		if df.PartitionValue != 5 {
			t.Errorf("expected partition 5, got %d", df.PartitionValue)
		}
		if df.RecordCount != 1000 {
			t.Errorf("expected record count 1000, got %d", df.RecordCount)
		}
		if df.FileSizeBytes != 10240 {
			t.Errorf("expected file size 10240, got %d", df.FileSizeBytes)
		}
		if len(df.ColumnSizes) != 2 {
			t.Errorf("expected 2 column sizes, got %d", len(df.ColumnSizes))
		}
		if df.SortOrderID == nil || *df.SortOrderID != 1 {
			t.Error("expected sort order ID 1")
		}
	})

	t.Run("without stats", func(t *testing.T) {
		df := BuildDataFileFromStats("s3://bucket/no-stats.parquet", 0, 500, 5120, nil)

		if df.Path != "s3://bucket/no-stats.parquet" {
			t.Errorf("expected path, got %s", df.Path)
		}
		if df.ColumnSizes != nil {
			t.Error("expected nil column sizes")
		}
		if df.LowerBounds != nil {
			t.Error("expected nil lower bounds")
		}
	})
}

func TestOffsetBounds(t *testing.T) {
	lower, upper := NewOffsetBounds(100, 200)

	if len(lower) != 8 || len(upper) != 8 {
		t.Errorf("expected 8 byte bounds, got lower=%d upper=%d", len(lower), len(upper))
	}

	// Verify big-endian encoding
	// 100 = 0x64
	if lower[7] != 0x64 {
		t.Errorf("lower bound encoding incorrect")
	}
	// 200 = 0xC8
	if upper[7] != 0xC8 {
		t.Errorf("upper bound encoding incorrect")
	}
}

func TestTimestampBounds(t *testing.T) {
	lower, upper := NewTimestampBounds(1000000, 2000000)

	if len(lower) != 8 || len(upper) != 8 {
		t.Errorf("expected 8 byte bounds")
	}
}

func TestPartitionBound(t *testing.T) {
	bound := NewPartitionBound(42)

	if len(bound) != 4 {
		t.Errorf("expected 4 byte bound, got %d", len(bound))
	}
	// 42 = 0x2A
	if bound[3] != 0x2A {
		t.Errorf("partition bound encoding incorrect: %v", bound)
	}
}

func TestDefaultDataFileStats(t *testing.T) {
	stats := DefaultDataFileStats(3, 100, 199, 1000000, 2000000, 100)

	// Check lower bounds
	if len(stats.LowerBounds) != 3 {
		t.Errorf("expected 3 lower bounds, got %d", len(stats.LowerBounds))
	}
	if _, ok := stats.LowerBounds[FieldIDPartition]; !ok {
		t.Error("missing partition lower bound")
	}
	if _, ok := stats.LowerBounds[FieldIDOffset]; !ok {
		t.Error("missing offset lower bound")
	}
	if _, ok := stats.LowerBounds[FieldIDTimestampMs]; !ok {
		t.Error("missing timestamp lower bound")
	}

	// Check upper bounds
	if len(stats.UpperBounds) != 3 {
		t.Errorf("expected 3 upper bounds, got %d", len(stats.UpperBounds))
	}

	// Check value counts
	if len(stats.ValueCounts) != 3 {
		t.Errorf("expected 3 value counts, got %d", len(stats.ValueCounts))
	}
	if stats.ValueCounts[FieldIDOffset] != 100 {
		t.Errorf("expected offset value count 100, got %d", stats.ValueCounts[FieldIDOffset])
	}
}

// conflictMockCatalog is a mock catalog that returns commit conflicts.
type conflictMockCatalog struct {
	*mockCatalog
	failCount    int32
	maxFailures  int32
	appendCalled int32
}

func newConflictMockCatalog(maxFailures int) *conflictMockCatalog {
	return &conflictMockCatalog{
		mockCatalog: newMockCatalog(),
		maxFailures: int32(maxFailures),
	}
}

func (c *conflictMockCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	table, err := c.mockCatalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return &conflictMockTable{
		mockTable:   table.(*mockTable),
		catalog:     c,
		maxFailures: c.maxFailures,
	}, nil
}

type conflictMockTable struct {
	*mockTable
	catalog     *conflictMockCatalog
	maxFailures int32
}

func (t *conflictMockTable) AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	atomic.AddInt32(&t.catalog.appendCalled, 1)
	if atomic.AddInt32(&t.catalog.failCount, 1) <= t.maxFailures {
		return nil, ErrCommitConflict
	}
	return t.mockTable.AppendFiles(ctx, files, opts)
}

func (t *conflictMockTable) Snapshots(ctx context.Context) ([]Snapshot, error) {
	return t.mockTable.Snapshots(ctx)
}

func (t *conflictMockTable) Refresh(ctx context.Context) error {
	return nil
}

// errorMockCatalog is a mock catalog that returns a specific error.
type errorMockCatalog struct {
	*mockCatalog
	err         error
	appendCalls int
}

func newErrorMockCatalog(err error) *errorMockCatalog {
	return &errorMockCatalog{
		mockCatalog: newMockCatalog(),
		err:         err,
	}
}

func (c *errorMockCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	table, err := c.mockCatalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return &errorMockTable{
		mockTable: table.(*mockTable),
		catalog:   c,
		err:       c.err,
	}, nil
}

type errorMockTable struct {
	*mockTable
	catalog *errorMockCatalog
	err     error
}

func (t *errorMockTable) AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	t.catalog.appendCalls++
	return nil, t.err
}

func (t *errorMockTable) Snapshots(ctx context.Context) ([]Snapshot, error) {
	return t.mockTable.Snapshots(ctx)
}

func TestAppender_IdempotentCommit(t *testing.T) {
	ctx := context.Background()

	t.Run("first commit succeeds and stores job ID", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "idempotent-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		result, err := appender.AppendFilesForStream(ctx, "idempotent-topic", "job-abc-123", files)
		if err != nil {
			t.Fatalf("AppendFilesForStream failed: %v", err)
		}

		if result.IdempotentSkipped {
			t.Error("first commit should not be marked as idempotent skip")
		}
		if result.Attempts != 1 {
			t.Errorf("expected 1 attempt, got %d", result.Attempts)
		}
		if result.Snapshot == nil {
			t.Error("expected snapshot")
		}

		// Verify the job ID was stored in snapshot summary
		if result.Snapshot.Summary == nil {
			t.Fatal("expected snapshot summary")
		}
		if result.Snapshot.Summary[SnapshotPropertyJobID] != "job-abc-123" {
			t.Errorf("expected job ID in summary, got %s", result.Snapshot.Summary[SnapshotPropertyJobID])
		}
	})

	t.Run("retry of same job ID returns idempotent skip", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "retry-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		// First commit
		result1, err := appender.AppendFilesForStream(ctx, "retry-topic", "job-retry-456", files)
		if err != nil {
			t.Fatalf("first AppendFilesForStream failed: %v", err)
		}
		originalSnapshotID := result1.Snapshot.SnapshotID

		// Retry with same job ID (simulating crash recovery)
		result2, err := appender.AppendFilesForStream(ctx, "retry-topic", "job-retry-456", files)
		if err != nil {
			t.Fatalf("retry AppendFilesForStream failed: %v", err)
		}

		if !result2.IdempotentSkipped {
			t.Error("retry should be marked as idempotent skip")
		}
		if result2.Attempts != 0 {
			t.Errorf("idempotent skip should have 0 attempts, got %d", result2.Attempts)
		}
		if result2.Snapshot == nil {
			t.Error("expected existing snapshot on idempotent skip")
		}
		if result2.Snapshot.SnapshotID != originalSnapshotID {
			t.Errorf("expected original snapshot ID %d, got %d", originalSnapshotID, result2.Snapshot.SnapshotID)
		}
	})

	t.Run("different job IDs create separate commits", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "multi-job-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		// First job
		result1, err := appender.AppendFilesForStream(ctx, "multi-job-topic", "job-a", files)
		if err != nil {
			t.Fatalf("first job failed: %v", err)
		}

		// Second job with different ID
		result2, err := appender.AppendFilesForStream(ctx, "multi-job-topic", "job-b", files)
		if err != nil {
			t.Fatalf("second job failed: %v", err)
		}

		if result1.IdempotentSkipped || result2.IdempotentSkipped {
			t.Error("different job IDs should not trigger idempotent skip")
		}
		if result2.Snapshot.SnapshotID <= result1.Snapshot.SnapshotID {
			t.Error("second job should create a new snapshot")
		}
		if result2.Snapshot.Summary[SnapshotPropertyJobID] != "job-b" {
			t.Errorf("expected job-b in summary, got %s", result2.Snapshot.Summary[SnapshotPropertyJobID])
		}
	})

	t.Run("table not found returns error", func(t *testing.T) {
		catalog := newMockCatalog()
		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		_, err := appender.AppendFilesForStream(ctx, "missing-topic", "job-123", files)
		if !errors.Is(err, ErrTableNotFound) {
			t.Errorf("expected ErrTableNotFound, got %v", err)
		}
	})

	t.Run("nil catalog returns error", func(t *testing.T) {
		appender := NewAppender(AppenderConfig{Catalog: nil})
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		_, err := appender.AppendFilesForStream(ctx, "topic", "job-123", files)
		if !errors.Is(err, ErrCatalogUnavailable) {
			t.Errorf("expected ErrCatalogUnavailable, got %v", err)
		}
	})
}

func TestAppender_IsCommitApplied(t *testing.T) {
	ctx := context.Background()

	t.Run("returns false for non-existent table", func(t *testing.T) {
		catalog := newMockCatalog()
		appender := NewAppender(DefaultAppenderConfig(catalog))

		applied, snapshot, err := appender.IsCommitApplied(ctx, "missing-topic", "job-123")
		if err != nil {
			t.Fatalf("IsCommitApplied failed: %v", err)
		}
		if applied {
			t.Error("expected false for non-existent table")
		}
		if snapshot != nil {
			t.Error("expected nil snapshot for non-existent table")
		}
	})

	t.Run("returns false for table with no matching job", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "check-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		// Create a commit with a different job ID
		_, err := appender.AppendFilesForStream(ctx, "check-topic", "other-job", files)
		if err != nil {
			t.Fatalf("AppendFilesForStream failed: %v", err)
		}

		applied, snapshot, err := appender.IsCommitApplied(ctx, "check-topic", "job-123")
		if err != nil {
			t.Fatalf("IsCommitApplied failed: %v", err)
		}
		if applied {
			t.Error("expected false for non-matching job ID")
		}
		if snapshot != nil {
			t.Error("expected nil snapshot for non-matching job ID")
		}
	})

	t.Run("returns true for matching job ID", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "applied-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		// Create a commit with the job ID we'll check
		result, err := appender.AppendFilesForStream(ctx, "applied-topic", "job-xyz", files)
		if err != nil {
			t.Fatalf("AppendFilesForStream failed: %v", err)
		}

		applied, snapshot, err := appender.IsCommitApplied(ctx, "applied-topic", "job-xyz")
		if err != nil {
			t.Fatalf("IsCommitApplied failed: %v", err)
		}
		if !applied {
			t.Error("expected true for matching job ID")
		}
		if snapshot == nil {
			t.Fatal("expected non-nil snapshot")
		}
		if snapshot.SnapshotID != result.Snapshot.SnapshotID {
			t.Errorf("expected snapshot ID %d, got %d", result.Snapshot.SnapshotID, snapshot.SnapshotID)
		}
	})

	t.Run("nil catalog returns error", func(t *testing.T) {
		appender := NewAppender(AppenderConfig{Catalog: nil})

		_, _, err := appender.IsCommitApplied(ctx, "topic", "job-123")
		if !errors.Is(err, ErrCatalogUnavailable) {
			t.Errorf("expected ErrCatalogUnavailable, got %v", err)
		}
	})

	t.Run("finds job in multiple snapshots", func(t *testing.T) {
		catalog := newMockCatalog()
		_, _ = catalog.CreateTableIfMissing(ctx, TableIdentifier{
			Namespace: []string{"dray"},
			Name:      "multi-snap-topic",
		}, CreateTableOptions{Schema: DefaultSchema()})

		appender := NewAppender(DefaultAppenderConfig(catalog))
		files := []DataFile{{Path: "s3://bucket/data.parquet", Format: FormatParquet, RecordCount: 100}}

		// Create multiple commits
		_, _ = appender.AppendFilesForStream(ctx, "multi-snap-topic", "job-1", files)
		result2, _ := appender.AppendFilesForStream(ctx, "multi-snap-topic", "job-2", files)
		_, _ = appender.AppendFilesForStream(ctx, "multi-snap-topic", "job-3", files)

		// Check for the middle job
		applied, snapshot, err := appender.IsCommitApplied(ctx, "multi-snap-topic", "job-2")
		if err != nil {
			t.Fatalf("IsCommitApplied failed: %v", err)
		}
		if !applied {
			t.Error("expected to find job-2")
		}
		if snapshot.SnapshotID != result2.Snapshot.SnapshotID {
			t.Errorf("expected snapshot ID %d, got %d", result2.Snapshot.SnapshotID, snapshot.SnapshotID)
		}
	})
}

func TestSnapshotPropertyJobID(t *testing.T) {
	if SnapshotPropertyJobID != "dray.job-id" {
		t.Errorf("expected SnapshotPropertyJobID to be 'dray.job-id', got %s", SnapshotPropertyJobID)
	}
}
