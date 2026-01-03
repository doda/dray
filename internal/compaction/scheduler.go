// Package compaction implements stream compaction for the Dray broker.
// This file implements the compaction scheduler that coordinates WAL-to-Parquet
// and Parquet small-file rewrite compaction.
package compaction

import (
	"context"

	"github.com/dray-io/dray/internal/compaction/planner"
	"github.com/dray-io/dray/internal/index"
)

// SchedulerConfig contains configuration for the compaction scheduler.
type SchedulerConfig struct {
	// WALPlannerConfig configures WAL-to-Parquet compaction planning.
	WALPlannerConfig planner.Config

	// ParquetRewriteConfig configures small-file Parquet rewrite planning.
	ParquetRewriteConfig planner.ParquetRewriteConfig

	// EnableWALCompaction enables WAL-to-Parquet compaction.
	EnableWALCompaction bool

	// EnableParquetRewrite enables Parquet small-file rewrite compaction.
	EnableParquetRewrite bool
}

// DefaultSchedulerConfig returns a SchedulerConfig with sensible defaults.
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		WALPlannerConfig:     planner.DefaultConfig(),
		ParquetRewriteConfig: planner.DefaultParquetRewriteConfig(),
		EnableWALCompaction:  true,
		EnableParquetRewrite: true,
	}
}

// CompactionType indicates the type of compaction planned.
type CompactionType string

const (
	// CompactionTypeWAL indicates WAL-to-Parquet compaction.
	CompactionTypeWAL CompactionType = "WAL"

	// CompactionTypeParquetRewrite indicates Parquet small-file rewrite compaction.
	CompactionTypeParquetRewrite CompactionType = "PARQUET_REWRITE"
)

// CompactionPlan describes a planned compaction operation.
type CompactionPlan struct {
	// Type indicates the compaction type.
	Type CompactionType

	// StreamID is the stream to compact.
	StreamID string

	// Entries are the index entries to compact.
	Entries []index.IndexEntry

	// TotalSizeBytes is the total size of entries to compact.
	TotalSizeBytes int64

	// StartOffset is the first offset covered by the entries.
	StartOffset int64

	// EndOffset is the end offset (exclusive) covered by the entries.
	EndOffset int64

	// TotalRecordCount is the total record count (only for WAL compaction).
	TotalRecordCount uint32
}

// Scheduler coordinates compaction planning for streams.
// It evaluates both WAL-to-Parquet and Parquet rewrite compaction,
// selecting the appropriate operation based on stream state and configuration.
type Scheduler struct {
	cfg             SchedulerConfig
	walPlanner      *planner.Planner
	rewritePlanner  *planner.ParquetRewritePlanner
}

// NewScheduler creates a new compaction scheduler.
func NewScheduler(cfg SchedulerConfig, streams planner.StreamQuerier) *Scheduler {
	return &Scheduler{
		cfg:            cfg,
		walPlanner:     planner.New(cfg.WALPlannerConfig, streams),
		rewritePlanner: planner.NewParquetRewritePlanner(cfg.ParquetRewriteConfig, streams),
	}
}

// Plan evaluates the stream and returns a compaction plan if one is needed.
// WAL compaction takes priority over Parquet rewrite if both are eligible.
// Returns nil if no compaction is needed.
func (s *Scheduler) Plan(ctx context.Context, streamID string) (*CompactionPlan, error) {
	if streamID == "" {
		return nil, ErrInvalidStreamID
	}

	// First, check for WAL compaction (higher priority)
	if s.cfg.EnableWALCompaction {
		walResult, err := s.walPlanner.Plan(ctx, streamID)
		if err != nil {
			return nil, err
		}
		if walResult != nil {
			return &CompactionPlan{
				Type:             CompactionTypeWAL,
				StreamID:         walResult.StreamID,
				Entries:          walResult.Entries,
				TotalSizeBytes:   walResult.TotalSizeBytes,
				StartOffset:      walResult.StartOffset,
				EndOffset:        walResult.EndOffset,
				TotalRecordCount: walResult.TotalRecordCount,
			}, nil
		}
	}

	// Next, check for Parquet rewrite compaction
	if s.cfg.EnableParquetRewrite {
		rewriteResult, err := s.rewritePlanner.Plan(ctx, streamID)
		if err != nil {
			return nil, err
		}
		if rewriteResult != nil {
			return &CompactionPlan{
				Type:           CompactionTypeParquetRewrite,
				StreamID:       rewriteResult.StreamID,
				Entries:        rewriteResult.Entries,
				TotalSizeBytes: rewriteResult.TotalSizeBytes,
				StartOffset:    rewriteResult.StartOffset,
				EndOffset:      rewriteResult.EndOffset,
			}, nil
		}
	}

	return nil, nil
}
