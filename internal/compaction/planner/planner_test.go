package planner

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
)

// mockStreamQuerier is a mock implementation of StreamQuerier for testing.
type mockStreamQuerier struct {
	entries map[string][]index.IndexEntry
}

func (m *mockStreamQuerier) ListIndexEntries(_ context.Context, streamID string, _ int) ([]index.IndexEntry, error) {
	return m.entries[streamID], nil
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxAgeMs != 300000 {
		t.Errorf("MaxAgeMs = %d, want 300000", cfg.MaxAgeMs)
	}
	if cfg.MaxFilesToMerge != 10 {
		t.Errorf("MaxFilesToMerge = %d, want 10", cfg.MaxFilesToMerge)
	}
	if cfg.MinEntries != 2 {
		t.Errorf("MinEntries = %d, want 2", cfg.MinEntries)
	}
	if cfg.MaxSizeBytes != 512*1024*1024 {
		t.Errorf("MaxSizeBytes = %d, want %d", cfg.MaxSizeBytes, 512*1024*1024)
	}
}

func TestPlan_EmptyStream(t *testing.T) {
	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{},
	}
	planner := New(DefaultConfig(), querier)

	result, err := planner.Plan(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result != nil {
		t.Errorf("Plan() = %v, want nil for empty stream", result)
	}
}

func TestPlan_OnlyParquetEntries(t *testing.T) {
	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": {
				{
					StreamID:    "stream-1",
					StartOffset: 0,
					EndOffset:   100,
					FileType:    index.FileTypeParquet,
					ChunkLength: 1000,
					CreatedAtMs: time.Now().Add(-10 * time.Minute).UnixMilli(),
				},
				{
					StreamID:    "stream-1",
					StartOffset: 100,
					EndOffset:   200,
					FileType:    index.FileTypeParquet,
					ChunkLength: 1000,
					CreatedAtMs: time.Now().Add(-5 * time.Minute).UnixMilli(),
				},
			},
		},
	}
	planner := New(DefaultConfig(), querier)

	result, err := planner.Plan(context.Background(), "stream-1")
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result != nil {
		t.Errorf("Plan() = %v, want nil for Parquet-only stream", result)
	}
}

func TestPlan_BelowMinEntries(t *testing.T) {
	now := time.Now()
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
					CreatedAtMs: now.Add(-10 * time.Minute).UnixMilli(),
				},
			},
		},
	}
	cfg := DefaultConfig()
	cfg.MinEntries = 2
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result != nil {
		t.Errorf("Plan() = %v, want nil when below MinEntries", result)
	}
}

func TestPlan_SelectsOldWALEntries(t *testing.T) {
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
				{
					StreamID:    "stream-1",
					StartOffset: 200,
					EndOffset:   300,
					FileType:    index.FileTypeWAL,
					ChunkLength: 1500,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
			},
		},
	}

	cfg := DefaultConfig()
	cfg.MaxAgeMs = 300000 // 5 minutes
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	if len(result.Entries) != 3 {
		t.Errorf("len(Entries) = %d, want 3", len(result.Entries))
	}
	if result.TotalSizeBytes != 4500 {
		t.Errorf("TotalSizeBytes = %d, want 4500", result.TotalSizeBytes)
	}
	if result.TotalRecordCount != 300 {
		t.Errorf("TotalRecordCount = %d, want 300", result.TotalRecordCount)
	}
	if result.StartOffset != 0 {
		t.Errorf("StartOffset = %d, want 0", result.StartOffset)
	}
	if result.EndOffset != 300 {
		t.Errorf("EndOffset = %d, want 300", result.EndOffset)
	}
}

func TestPlan_StopsAtYoungEntries(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()
	youngTime := now.Add(-1 * time.Minute).UnixMilli() // Too young

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
				{
					StreamID:    "stream-1",
					StartOffset: 200,
					EndOffset:   300,
					FileType:    index.FileTypeWAL,
					ChunkLength: 1500,
					RecordCount: 100,
					CreatedAtMs: youngTime, // This entry is too young
				},
			},
		},
	}

	cfg := DefaultConfig()
	cfg.MaxAgeMs = 300000 // 5 minutes
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	// Should only include the first 2 entries (the old ones)
	if len(result.Entries) != 2 {
		t.Errorf("len(Entries) = %d, want 2", len(result.Entries))
	}
	if result.TotalSizeBytes != 3000 {
		t.Errorf("TotalSizeBytes = %d, want 3000", result.TotalSizeBytes)
	}
	if result.EndOffset != 200 {
		t.Errorf("EndOffset = %d, want 200", result.EndOffset)
	}
}

func TestPlan_RespectsMaxFilesToMerge(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	entries := make([]index.IndexEntry, 10)
	for i := 0; i < 10; i++ {
		entries[i] = index.IndexEntry{
			StreamID:    "stream-1",
			StartOffset: int64(i * 100),
			EndOffset:   int64((i + 1) * 100),
			FileType:    index.FileTypeWAL,
			ChunkLength: 1000,
			RecordCount: 100,
			CreatedAtMs: oldTime,
		}
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultConfig()
	cfg.MaxFilesToMerge = 5
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	if len(result.Entries) != 5 {
		t.Errorf("len(Entries) = %d, want 5 (MaxFilesToMerge)", len(result.Entries))
	}
	if result.EndOffset != 500 {
		t.Errorf("EndOffset = %d, want 500", result.EndOffset)
	}
}

func TestPlan_RespectsMaxSizeBytes(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	entries := []index.IndexEntry{
		{
			StreamID:    "stream-1",
			StartOffset: 0,
			EndOffset:   100,
			FileType:    index.FileTypeWAL,
			ChunkLength: 50 * 1024 * 1024, // 50 MiB
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 100,
			EndOffset:   200,
			FileType:    index.FileTypeWAL,
			ChunkLength: 50 * 1024 * 1024, // 50 MiB
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 200,
			EndOffset:   300,
			FileType:    index.FileTypeWAL,
			ChunkLength: 50 * 1024 * 1024, // 50 MiB - this would exceed 100 MiB
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultConfig()
	cfg.MaxSizeBytes = 100 * 1024 * 1024 // 100 MiB
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	// Should only include first 2 entries (100 MiB total)
	if len(result.Entries) != 2 {
		t.Errorf("len(Entries) = %d, want 2", len(result.Entries))
	}
	expectedSize := int64(100 * 1024 * 1024)
	if result.TotalSizeBytes != expectedSize {
		t.Errorf("TotalSizeBytes = %d, want %d", result.TotalSizeBytes, expectedSize)
	}
}

func TestPlan_AllowsFirstEntryOverMaxSize(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	// Single entry larger than MaxSizeBytes
	entries := []index.IndexEntry{
		{
			StreamID:    "stream-1",
			StartOffset: 0,
			EndOffset:   100,
			FileType:    index.FileTypeWAL,
			ChunkLength: 200 * 1024 * 1024, // 200 MiB, larger than max
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 100,
			EndOffset:   200,
			FileType:    index.FileTypeWAL,
			ChunkLength: 50 * 1024 * 1024, // 50 MiB
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultConfig()
	cfg.MaxSizeBytes = 100 * 1024 * 1024 // 100 MiB
	cfg.MinEntries = 1
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	// Should include the first entry even though it exceeds max size
	if len(result.Entries) != 1 {
		t.Errorf("len(Entries) = %d, want 1", len(result.Entries))
	}
	expectedSize := int64(200 * 1024 * 1024)
	if result.TotalSizeBytes != expectedSize {
		t.Errorf("TotalSizeBytes = %d, want %d", result.TotalSizeBytes, expectedSize)
	}
}

func TestPlan_StopsAtParquetEntry(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	entries := []index.IndexEntry{
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
			FileType:    index.FileTypeParquet, // Already compacted - stops here
			ChunkLength: 2000,
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 200,
			EndOffset:   300,
			FileType:    index.FileTypeWAL,
			ChunkLength: 1500,
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 300,
			EndOffset:   400,
			FileType:    index.FileTypeWAL,
			ChunkLength: 1500,
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultConfig()
	cfg.MinEntries = 1
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	// Should only include the first WAL entry - stops at Parquet for contiguity
	if len(result.Entries) != 1 {
		t.Errorf("len(Entries) = %d, want 1", len(result.Entries))
	}
	if result.TotalSizeBytes != 1000 {
		t.Errorf("TotalSizeBytes = %d, want 1000", result.TotalSizeBytes)
	}
	if result.EndOffset != 100 {
		t.Errorf("EndOffset = %d, want 100", result.EndOffset)
	}
}

func TestPlan_StopsAtGapInOffsets(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	// WAL entries with a gap (entry 2 starts at 250, but entry 1 ends at 200)
	entries := []index.IndexEntry{
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
			ChunkLength: 1000,
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 250, // Gap! Should be 200
			EndOffset:   350,
			FileType:    index.FileTypeWAL,
			ChunkLength: 1000,
			RecordCount: 100,
			CreatedAtMs: oldTime,
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultConfig()
	cfg.MinEntries = 1
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	// Should only include the first 2 contiguous WAL entries
	if len(result.Entries) != 2 {
		t.Errorf("len(Entries) = %d, want 2", len(result.Entries))
	}
	if result.TotalSizeBytes != 2000 {
		t.Errorf("TotalSizeBytes = %d, want 2000", result.TotalSizeBytes)
	}
	if result.EndOffset != 200 {
		t.Errorf("EndOffset = %d, want 200", result.EndOffset)
	}
}

func TestPlan_DisableAgeBasedCompaction(t *testing.T) {
	now := time.Now()
	// Entry created just now
	youngTime := now.UnixMilli()

	entries := []index.IndexEntry{
		{
			StreamID:    "stream-1",
			StartOffset: 0,
			EndOffset:   100,
			FileType:    index.FileTypeWAL,
			ChunkLength: 1000,
			RecordCount: 100,
			CreatedAtMs: youngTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 100,
			EndOffset:   200,
			FileType:    index.FileTypeWAL,
			ChunkLength: 1000,
			RecordCount: 100,
			CreatedAtMs: youngTime,
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultConfig()
	cfg.MaxAgeMs = 0 // Disable age-based compaction
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result when age check is disabled")
	}

	// Should include all entries when age check is disabled
	if len(result.Entries) != 2 {
		t.Errorf("len(Entries) = %d, want 2", len(result.Entries))
	}
}

func TestPlan_DisableSizeLimit(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	// Create many large entries
	entries := make([]index.IndexEntry, 5)
	for i := 0; i < 5; i++ {
		entries[i] = index.IndexEntry{
			StreamID:    "stream-1",
			StartOffset: int64(i * 100),
			EndOffset:   int64((i + 1) * 100),
			FileType:    index.FileTypeWAL,
			ChunkLength: 1024 * 1024 * 1024, // 1 GiB each
			RecordCount: 100,
			CreatedAtMs: oldTime,
		}
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	cfg := DefaultConfig()
	cfg.MaxSizeBytes = 0 // Disable size limit
	cfg.MaxFilesToMerge = 100
	planner := New(cfg, querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	// Should include all entries when size limit is disabled
	if len(result.Entries) != 5 {
		t.Errorf("len(Entries) = %d, want 5", len(result.Entries))
	}
}

func TestPlan_StreamID(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-abc": {
				{
					StreamID:    "stream-abc",
					StartOffset: 0,
					EndOffset:   100,
					FileType:    index.FileTypeWAL,
					ChunkLength: 1000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
				{
					StreamID:    "stream-abc",
					StartOffset: 100,
					EndOffset:   200,
					FileType:    index.FileTypeWAL,
					ChunkLength: 1000,
					RecordCount: 100,
					CreatedAtMs: oldTime,
				},
			},
		},
	}

	planner := New(DefaultConfig(), querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-abc", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	if result.StreamID != "stream-abc" {
		t.Errorf("StreamID = %q, want %q", result.StreamID, "stream-abc")
	}
}

func TestPlan_ContiguousOffsets(t *testing.T) {
	now := time.Now()
	oldTime := now.Add(-10 * time.Minute).UnixMilli()

	entries := []index.IndexEntry{
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
			EndOffset:   250, // Non-standard count
			FileType:    index.FileTypeWAL,
			ChunkLength: 1500,
			RecordCount: 150,
			CreatedAtMs: oldTime,
		},
		{
			StreamID:    "stream-1",
			StartOffset: 250,
			EndOffset:   500,
			FileType:    index.FileTypeWAL,
			ChunkLength: 2500,
			RecordCount: 250,
			CreatedAtMs: oldTime,
		},
	}

	querier := &mockStreamQuerier{
		entries: map[string][]index.IndexEntry{
			"stream-1": entries,
		},
	}

	planner := New(DefaultConfig(), querier)

	result, err := planner.PlanWithTime(context.Background(), "stream-1", now)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result == nil {
		t.Fatal("Plan() = nil, want non-nil result")
	}

	// Verify contiguous coverage
	if result.StartOffset != 0 {
		t.Errorf("StartOffset = %d, want 0", result.StartOffset)
	}
	if result.EndOffset != 500 {
		t.Errorf("EndOffset = %d, want 500", result.EndOffset)
	}
	if result.TotalRecordCount != 500 {
		t.Errorf("TotalRecordCount = %d, want 500", result.TotalRecordCount)
	}
}
