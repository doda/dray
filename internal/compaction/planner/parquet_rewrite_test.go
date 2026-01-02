package planner

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
)

func TestParquetRewritePlan_SelectsSmallParquetFiles(t *testing.T) {
	now := time.Now()
	entries := []index.IndexEntry{
		{
			StreamID:         "stream-1",
			StartOffset:      0,
			EndOffset:        100,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/a.parquet",
			ParquetSizeBytes: 10,
			CreatedAtMs:      now.Add(-20 * time.Minute).UnixMilli(),
		},
		{
			StreamID:         "stream-1",
			StartOffset:      100,
			EndOffset:        200,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/b.parquet",
			ParquetSizeBytes: 12,
			CreatedAtMs:      now.Add(-20 * time.Minute).UnixMilli(),
		},
		{
			StreamID:         "stream-1",
			StartOffset:      200,
			EndOffset:        300,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/c.parquet",
			ParquetSizeBytes: 9,
			CreatedAtMs:      now.Add(-20 * time.Minute).UnixMilli(),
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultParquetRewriteConfig()
	cfg.MinFiles = 2
	cfg.SmallFileThresholdBytes = 20
	cfg.MinAgeMs = int64((10 * time.Minute).Milliseconds())
	cfg.TargetFileSizeBytes = 25

	planner := NewParquetRewritePlanner(cfg, querier)
	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("PlanWithTime() error = %v", err)
	}
	if result == nil {
		t.Fatalf("PlanWithTime() returned nil, want selection")
	}
	if len(result.Entries) != 3 {
		t.Fatalf("len(Entries) = %d, want 3", len(result.Entries))
	}
	if result.StartOffset != 0 || result.EndOffset != 300 {
		t.Fatalf("unexpected offset range: %d-%d", result.StartOffset, result.EndOffset)
	}
	if result.TotalSizeBytes != 31 {
		t.Fatalf("TotalSizeBytes = %d, want 31", result.TotalSizeBytes)
	}
}

func TestParquetRewritePlan_RespectsMaxFiles(t *testing.T) {
	now := time.Now()
	entries := []index.IndexEntry{
		{
			StreamID:         "stream-1",
			StartOffset:      0,
			EndOffset:        100,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/a.parquet",
			ParquetSizeBytes: 10,
			CreatedAtMs:      now.Add(-20 * time.Minute).UnixMilli(),
		},
		{
			StreamID:         "stream-1",
			StartOffset:      100,
			EndOffset:        200,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/b.parquet",
			ParquetSizeBytes: 11,
			CreatedAtMs:      now.Add(-20 * time.Minute).UnixMilli(),
		},
		{
			StreamID:         "stream-1",
			StartOffset:      200,
			EndOffset:        300,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/c.parquet",
			ParquetSizeBytes: 12,
			CreatedAtMs:      now.Add(-20 * time.Minute).UnixMilli(),
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultParquetRewriteConfig()
	cfg.MinFiles = 2
	cfg.MaxFiles = 2
	cfg.SmallFileThresholdBytes = 20
	cfg.MinAgeMs = int64((10 * time.Minute).Milliseconds())

	planner := NewParquetRewritePlanner(cfg, querier)
	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("PlanWithTime() error = %v", err)
	}
	if result == nil {
		t.Fatalf("PlanWithTime() returned nil, want selection")
	}
	if len(result.Entries) != 2 {
		t.Fatalf("len(Entries) = %d, want 2", len(result.Entries))
	}
	if result.EndOffset != 200 {
		t.Fatalf("EndOffset = %d, want 200", result.EndOffset)
	}
}

func TestParquetRewritePlan_SkipsWhenBelowMinFiles(t *testing.T) {
	now := time.Now()
	entries := []index.IndexEntry{
		{
			StreamID:         "stream-1",
			StartOffset:      0,
			EndOffset:        100,
			FileType:         index.FileTypeParquet,
			ParquetPath:      "compaction/v1/topic=foo/partition=0/date=2024/01/01/a.parquet",
			ParquetSizeBytes: 10,
			CreatedAtMs:      now.Add(-20 * time.Minute).UnixMilli(),
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultParquetRewriteConfig()
	cfg.MinFiles = 2
	cfg.SmallFileThresholdBytes = 20
	cfg.MinAgeMs = int64((10 * time.Minute).Milliseconds())

	planner := NewParquetRewritePlanner(cfg, querier)
	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("PlanWithTime() error = %v", err)
	}
	if result != nil {
		t.Fatalf("PlanWithTime() = %v, want nil", result)
	}
}
