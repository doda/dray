package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
)

// mockStreamQuerier implements planner.StreamQuerier for testing.
type mockStreamQuerier struct {
	entries map[string][]index.IndexEntry
}

func (m *mockStreamQuerier) ListIndexEntries(_ context.Context, streamID string, _ int) ([]index.IndexEntry, error) {
	return m.entries[streamID], nil
}

func TestScheduler_PlanWALCompaction(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": {
				{
					StreamID:    "stream-1",
					StartOffset: 0,
					EndOffset:   100,
					FileType:    index.FileTypeWAL,
					ChunkLength: 1000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
				{
					StreamID:    "stream-1",
					StartOffset: 100,
					EndOffset:   200,
					FileType:    index.FileTypeWAL,
					ChunkLength: 2000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
			},
		},
	}

	cfg := DefaultSchedulerConfig()
	cfg.WALPlannerConfig.MaxAgeMs = 300000 // 5 minutes
	scheduler := NewScheduler(cfg, querier)

	plan, err := scheduler.Plan(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan == nil {
		t.Fatal("Plan() = nil, want non-nil WAL compaction plan")
	}
	if plan.Type != CompactionTypeWAL {
		t.Errorf("Type = %s, want %s", plan.Type, CompactionTypeWAL)
	}
	if len(plan.Entries) != 2 {
		t.Errorf("len(Entries) = %d, want 2", len(plan.Entries))
	}
	if plan.StartOffset != 0 {
		t.Errorf("StartOffset = %d, want 0", plan.StartOffset)
	}
	if plan.EndOffset != 200 {
		t.Errorf("EndOffset = %d, want 200", plan.EndOffset)
	}
}

func TestScheduler_PlanParquetRewrite(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-20 * time.Minute).UnixMilli()

	// Only Parquet entries - no WAL entries to compact
	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": {
				{
					StreamID:         "stream-1",
					StartOffset:      0,
					EndOffset:        100,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/a.parquet",
					ParquetSizeBytes: 10,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      100,
					EndOffset:        200,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/b.parquet",
					ParquetSizeBytes: 12,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      200,
					EndOffset:        300,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/c.parquet",
					ParquetSizeBytes: 9,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      300,
					EndOffset:        400,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/d.parquet",
					ParquetSizeBytes: 11,
					CreatedAtMs:      oldTime,
				},
			},
		},
	}

	cfg := DefaultSchedulerConfig()
	cfg.ParquetRewriteConfig.MinFiles = 4
	cfg.ParquetRewriteConfig.SmallFileThresholdBytes = 20
	cfg.ParquetRewriteConfig.MinAgeMs = int64((10 * time.Minute).Milliseconds())
	scheduler := NewScheduler(cfg, querier)

	plan, err := scheduler.Plan(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan == nil {
		t.Fatal("Plan() = nil, want non-nil Parquet rewrite plan")
	}
	if plan.Type != CompactionTypeParquetRewrite {
		t.Errorf("Type = %s, want %s", plan.Type, CompactionTypeParquetRewrite)
	}
	if len(plan.Entries) != 4 {
		t.Errorf("len(Entries) = %d, want 4", len(plan.Entries))
	}
	if plan.StartOffset != 0 {
		t.Errorf("StartOffset = %d, want 0", plan.StartOffset)
	}
	if plan.EndOffset != 400 {
		t.Errorf("EndOffset = %d, want 400", plan.EndOffset)
	}
}

func TestScheduler_WALTakesPriority(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-20 * time.Minute).UnixMilli()

	// Both WAL and eligible Parquet entries
	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": {
				// 4 Parquet entries first (eligible for rewrite)
				{
					StreamID:         "stream-1",
					StartOffset:      0,
					EndOffset:        100,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/a.parquet",
					ParquetSizeBytes: 10,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      100,
					EndOffset:        200,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/b.parquet",
					ParquetSizeBytes: 12,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      200,
					EndOffset:        300,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/c.parquet",
					ParquetSizeBytes: 9,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      300,
					EndOffset:        400,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/d.parquet",
					ParquetSizeBytes: 11,
					CreatedAtMs:      oldTime,
				},
				// WAL entries after
				{
					StreamID:    "stream-1",
					StartOffset: 400,
					EndOffset:   500,
					FileType:    index.FileTypeWAL,
					ChunkLength: 1000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
				{
					StreamID:    "stream-1",
					StartOffset: 500,
					EndOffset:   600,
					FileType:    index.FileTypeWAL,
					ChunkLength: 2000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
			},
		},
	}

	cfg := DefaultSchedulerConfig()
	cfg.WALPlannerConfig.MaxAgeMs = int64((5 * time.Minute).Milliseconds())
	cfg.ParquetRewriteConfig.MinFiles = 4
	cfg.ParquetRewriteConfig.SmallFileThresholdBytes = 20
	cfg.ParquetRewriteConfig.MinAgeMs = int64((10 * time.Minute).Milliseconds())
	scheduler := NewScheduler(cfg, querier)

	plan, err := scheduler.Plan(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan == nil {
		t.Fatal("Plan() = nil, want non-nil plan")
	}
	// WAL compaction should take priority
	if plan.Type != CompactionTypeWAL {
		t.Errorf("Type = %s, want %s (WAL takes priority)", plan.Type, CompactionTypeWAL)
	}
	if len(plan.Entries) != 2 {
		t.Errorf("len(Entries) = %d, want 2 WAL entries", len(plan.Entries))
	}
}

func TestScheduler_DisabledWALCompaction(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-20 * time.Minute).UnixMilli()

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": {
				// 4 Parquet entries
				{
					StreamID:         "stream-1",
					StartOffset:      0,
					EndOffset:        100,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/a.parquet",
					ParquetSizeBytes: 10,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      100,
					EndOffset:        200,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/b.parquet",
					ParquetSizeBytes: 12,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      200,
					EndOffset:        300,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/c.parquet",
					ParquetSizeBytes: 9,
					CreatedAtMs:      oldTime,
				},
				{
					StreamID:         "stream-1",
					StartOffset:      300,
					EndOffset:        400,
					FileType:         index.FileTypeParquet,
					ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/d.parquet",
					ParquetSizeBytes: 11,
					CreatedAtMs:      oldTime,
				},
				// WAL entries after
				{
					StreamID:    "stream-1",
					StartOffset: 400,
					EndOffset:   500,
					FileType:    index.FileTypeWAL,
					ChunkLength: 1000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
				{
					StreamID:    "stream-1",
					StartOffset: 500,
					EndOffset:   600,
					FileType:    index.FileTypeWAL,
					ChunkLength: 2000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
			},
		},
	}

	cfg := DefaultSchedulerConfig()
	cfg.EnableWALCompaction = false // Disable WAL compaction
	cfg.ParquetRewriteConfig.MinFiles = 4
	cfg.ParquetRewriteConfig.SmallFileThresholdBytes = 20
	cfg.ParquetRewriteConfig.MinAgeMs = int64((10 * time.Minute).Milliseconds())
	scheduler := NewScheduler(cfg, querier)

	plan, err := scheduler.Plan(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan == nil {
		t.Fatal("Plan() = nil, want non-nil plan")
	}
	// Should plan Parquet rewrite since WAL is disabled
	if plan.Type != CompactionTypeParquetRewrite {
		t.Errorf("Type = %s, want %s when WAL disabled", plan.Type, CompactionTypeParquetRewrite)
	}
	if len(plan.Entries) != 4 {
		t.Errorf("len(Entries) = %d, want 4 Parquet entries", len(plan.Entries))
	}
}

func TestScheduler_NoPlan(t *testing.T) {
	// No entries at all
	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{},
	}

	scheduler := NewScheduler(DefaultSchedulerConfig(), querier)

	plan, err := scheduler.Plan(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan != nil {
		t.Errorf("Plan() = %v, want nil for empty stream", plan)
	}
}

func TestScheduler_InvalidStreamID(t *testing.T) {
	querier := &mockStreamQuerier{}
	scheduler := NewScheduler(DefaultSchedulerConfig(), querier)

	_, err := scheduler.Plan(context.Background(), "")
	if err != ErrInvalidStreamID {
		t.Errorf("Plan(\"\") error = %v, want ErrInvalidStreamID", err)
	}
}

func TestDefaultSchedulerConfig(t *testing.T) {
	cfg := DefaultSchedulerConfig()

	if !cfg.EnableWALCompaction {
		t.Error("EnableWALCompaction = false, want true")
	}
	if !cfg.EnableParquetRewrite {
		t.Error("EnableParquetRewrite = false, want true")
	}
	if cfg.WALPlannerConfig.MaxAgeMs != 300000 {
		t.Errorf("WALPlannerConfig.MaxAgeMs = %d, want 300000", cfg.WALPlannerConfig.MaxAgeMs)
	}
}
