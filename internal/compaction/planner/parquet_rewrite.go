package planner

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/dray-io/dray/internal/index"
)

// ParquetRewriteConfig controls small-file rewrite planning.
type ParquetRewriteConfig struct {
	// MinAgeMs requires Parquet files to be older than this threshold.
	MinAgeMs int64
	// SmallFileThresholdBytes marks files smaller than this as eligible.
	SmallFileThresholdBytes int64
	// TargetFileSizeBytes stops planning once total size reaches this target.
	TargetFileSizeBytes int64
	// MaxMergeBytes caps the total size of selected files.
	MaxMergeBytes int64
	// MinFiles is the minimum number of files required to plan a rewrite.
	MinFiles int
	// MaxFiles is the maximum number of files to include in one rewrite plan.
	MaxFiles int
}

// DefaultParquetRewriteConfig returns sensible defaults for rewrite planning.
func DefaultParquetRewriteConfig() ParquetRewriteConfig {
	return ParquetRewriteConfig{
		MinAgeMs:                 10 * 60 * 1000,
		SmallFileThresholdBytes:  64 * 1024 * 1024,
		TargetFileSizeBytes:      256 * 1024 * 1024,
		MaxMergeBytes:            512 * 1024 * 1024,
		MinFiles:                 4,
		MaxFiles:                 50,
	}
}

// ParquetRewriteResult describes a planned small-file rewrite.
type ParquetRewriteResult struct {
	StreamID       string
	Entries        []index.IndexEntry
	TotalSizeBytes int64
	StartOffset    int64
	EndOffset      int64
}

// ParquetRewritePlanner selects small Parquet files for rewrite compaction.
// The planner is invoked by the compaction Scheduler to identify files eligible
// for merging into larger files.
type ParquetRewritePlanner struct {
	cfg     ParquetRewriteConfig
	streams StreamQuerier
}

// NewParquetRewritePlanner creates a new rewrite planner.
func NewParquetRewritePlanner(cfg ParquetRewriteConfig, streams StreamQuerier) *ParquetRewritePlanner {
	if cfg.MinFiles == 0 {
		cfg.MinFiles = 4
	}
	if cfg.MaxFiles == 0 {
		cfg.MaxFiles = 50
	}
	return &ParquetRewritePlanner{
		cfg:     cfg,
		streams: streams,
	}
}

// Plan selects Parquet files eligible for rewrite based on the configured thresholds.
func (p *ParquetRewritePlanner) Plan(ctx context.Context, streamID string) (*ParquetRewriteResult, error) {
	return p.PlanWithTime(ctx, streamID, time.Now())
}

// PlanWithTime is like Plan but allows specifying the current time for testing.
func (p *ParquetRewritePlanner) PlanWithTime(ctx context.Context, streamID string, now time.Time) (*ParquetRewriteResult, error) {
	entries, err := p.streams.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}

	nowMs := now.UnixMilli()
	var selected []index.IndexEntry
	var totalSize int64
	var expectedStartOffset int64 = -1
	var partitionID *int32

	for _, entry := range entries {
		if entry.FileType != index.FileTypeParquet {
			if len(selected) > 0 {
				break
			}
			continue
		}

		entrySize, ok := parquetSizeToInt64(entry.ParquetSizeBytes)
		if !ok {
			if len(selected) > 0 {
				break
			}
			continue
		}

		if p.cfg.SmallFileThresholdBytes > 0 && entrySize >= p.cfg.SmallFileThresholdBytes {
			if len(selected) > 0 {
				break
			}
			continue
		}

		if p.cfg.MinAgeMs > 0 && nowMs-entry.CreatedAtMs < p.cfg.MinAgeMs {
			if len(selected) > 0 {
				break
			}
			continue
		}

		entryPartition, hasPartition := parseParquetPartition(entry.ParquetPath)
		if partitionID != nil && hasPartition && *partitionID != entryPartition {
			break
		}
		if partitionID == nil && hasPartition {
			partitionID = &entryPartition
		}

		if expectedStartOffset >= 0 && entry.StartOffset != expectedStartOffset {
			break
		}

		if p.cfg.MaxFiles > 0 && len(selected) >= p.cfg.MaxFiles {
			break
		}

		if p.cfg.MaxMergeBytes > 0 && totalSize+entrySize > p.cfg.MaxMergeBytes && len(selected) > 0 {
			break
		}

		selected = append(selected, entry)
		totalSize += entrySize
		expectedStartOffset = entry.EndOffset

		if p.cfg.TargetFileSizeBytes > 0 && totalSize >= p.cfg.TargetFileSizeBytes {
			break
		}
	}

	if len(selected) < p.cfg.MinFiles {
		return nil, nil
	}

	return &ParquetRewriteResult{
		StreamID:       streamID,
		Entries:        selected,
		TotalSizeBytes: totalSize,
		StartOffset:    selected[0].StartOffset,
		EndOffset:      selected[len(selected)-1].EndOffset,
	}, nil
}

func parquetSizeToInt64(size uint64) (int64, bool) {
	if size == 0 {
		return 0, false
	}
	if size > uint64(^uint64(0)>>1) {
		return 0, false
	}
	return int64(size), true
}

func parseParquetPartition(path string) (int32, bool) {
	needle := "partition="
	idx := strings.Index(path, needle)
	if idx == -1 {
		return 0, false
	}
	start := idx + len(needle)
	end := strings.IndexByte(path[start:], '/')
	if end == -1 {
		end = len(path) - start
	}

	value, err := strconv.ParseInt(path[start:start+end], 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(value), true
}
