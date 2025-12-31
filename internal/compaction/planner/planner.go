// Package planner selects WAL entries for compaction based on triggers.
package planner

import (
	"context"
	"time"

	"github.com/dray-io/dray/internal/index"
)

// Config contains the compaction trigger thresholds.
type Config struct {
	// MaxAgeMs is the maximum age in milliseconds for a WAL entry before it
	// becomes eligible for compaction. Entries older than this will be selected.
	// Setting to 0 disables age-based compaction.
	MaxAgeMs int64

	// MaxFilesToMerge is the maximum number of WAL entries to compact in one job.
	// This limits the size of a single compaction operation.
	MaxFilesToMerge int

	// MinEntries is the minimum number of contiguous WAL entries required to
	// trigger compaction. If fewer entries are eligible, compaction is skipped.
	MinEntries int

	// MaxSizeBytes is the maximum cumulative size in bytes of entries to compact.
	// If set, compaction will stop adding entries once this threshold is reached.
	// Setting to 0 disables size-based limiting.
	MaxSizeBytes int64
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		MaxAgeMs:        300000, // 5 minutes
		MaxFilesToMerge: 10,
		MinEntries:      2,
		MaxSizeBytes:    512 * 1024 * 1024, // 512 MiB
	}
}

// Result contains the compaction plan for a stream.
type Result struct {
	// StreamID is the stream being compacted.
	StreamID string

	// Entries are the index entries selected for compaction.
	// These are contiguous WAL entries that will be merged into Parquet.
	Entries []index.IndexEntry

	// TotalSizeBytes is the total size of all selected entries.
	TotalSizeBytes int64

	// TotalRecordCount is the total number of records in all selected entries.
	TotalRecordCount uint32

	// StartOffset is the first offset covered by the selected entries.
	StartOffset int64

	// EndOffset is the end offset (exclusive) covered by the selected entries.
	EndOffset int64
}

// Planner selects WAL entries for compaction based on configuration policies.
type Planner struct {
	cfg     Config
	streams StreamQuerier
}

// StreamQuerier provides access to stream index data.
type StreamQuerier interface {
	// ListIndexEntries lists all index entries for a stream.
	ListIndexEntries(ctx context.Context, streamID string, limit int) ([]index.IndexEntry, error)
}

// New creates a new Planner with the given configuration and stream querier.
func New(cfg Config, streams StreamQuerier) *Planner {
	return &Planner{
		cfg:     cfg,
		streams: streams,
	}
}

// Plan queries the index entries for a stream and selects contiguous WAL entries
// for compaction based on the configured triggers.
//
// The selection algorithm:
// 1. Query all index entries for the stream
// 2. Skip any PARQUET entries at the beginning (already compacted)
// 3. Select contiguous WAL entries starting from the first WAL entry
// 4. Stop at any subsequent PARQUET entry or gap in offsets
// 5. Apply max age policy: include entries older than MaxAgeMs
// 6. Apply file count policy: limit to MaxFilesToMerge entries
// 7. Apply size policy: stop if MaxSizeBytes is exceeded
// 8. Return nil if fewer than MinEntries are selected
func (p *Planner) Plan(ctx context.Context, streamID string) (*Result, error) {
	now := time.Now().UnixMilli()

	// Query all index entries for the stream
	entries, err := p.streams.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	// Select contiguous WAL entries based on policies
	var selected []index.IndexEntry
	var totalSize int64
	var totalRecords uint32
	var expectedStartOffset int64 = -1
	var foundFirstWAL bool

	for _, entry := range entries {
		// Skip PARQUET entries at the beginning (already compacted)
		// Once we start collecting WAL entries, hitting a PARQUET breaks contiguity
		if entry.FileType != index.FileTypeWAL {
			if foundFirstWAL {
				// Hit a PARQUET entry after we started collecting WAL entries
				// This breaks contiguity, so stop here
				break
			}
			// Haven't found first WAL yet, skip this PARQUET entry
			continue
		}
		foundFirstWAL = true

		// Verify contiguity: each entry's StartOffset must match the previous EndOffset
		if expectedStartOffset >= 0 && entry.StartOffset != expectedStartOffset {
			// Gap in offsets - stop selection to maintain contiguity
			break
		}

		// Check max file count limit
		if p.cfg.MaxFilesToMerge > 0 && len(selected) >= p.cfg.MaxFilesToMerge {
			break
		}

		// Check max size limit before adding
		entrySize := int64(entry.ChunkLength)
		if p.cfg.MaxSizeBytes > 0 && totalSize+entrySize > p.cfg.MaxSizeBytes && len(selected) > 0 {
			break
		}

		// Apply age policy: only include entries older than MaxAgeMs
		// If MaxAgeMs is 0, skip age filtering (all WAL entries are eligible)
		if p.cfg.MaxAgeMs > 0 {
			entryAge := now - entry.CreatedAtMs
			if entryAge < p.cfg.MaxAgeMs {
				// Entry is too young; since entries are ordered by offset (oldest first),
				// all subsequent entries will also be too young
				break
			}
		}

		selected = append(selected, entry)
		totalSize += entrySize
		totalRecords += entry.RecordCount
		expectedStartOffset = entry.EndOffset
	}

	if len(selected) == 0 {
		return nil, nil
	}

	// Check minimum entries threshold
	if len(selected) < p.cfg.MinEntries {
		return nil, nil
	}

	// Build result
	return &Result{
		StreamID:         streamID,
		Entries:          selected,
		TotalSizeBytes:   totalSize,
		TotalRecordCount: totalRecords,
		StartOffset:      selected[0].StartOffset,
		EndOffset:        selected[len(selected)-1].EndOffset,
	}, nil
}

// PlanWithTime is like Plan but allows specifying the current time for testing.
func (p *Planner) PlanWithTime(ctx context.Context, streamID string, now time.Time) (*Result, error) {
	nowMs := now.UnixMilli()

	// Query all index entries for the stream
	entries, err := p.streams.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	// Select contiguous WAL entries based on policies
	var selected []index.IndexEntry
	var totalSize int64
	var totalRecords uint32
	var expectedStartOffset int64 = -1
	var foundFirstWAL bool

	for _, entry := range entries {
		// Skip PARQUET entries at the beginning (already compacted)
		// Once we start collecting WAL entries, hitting a PARQUET breaks contiguity
		if entry.FileType != index.FileTypeWAL {
			if foundFirstWAL {
				// Hit a PARQUET entry after we started collecting WAL entries
				// This breaks contiguity, so stop here
				break
			}
			// Haven't found first WAL yet, skip this PARQUET entry
			continue
		}
		foundFirstWAL = true

		// Verify contiguity: each entry's StartOffset must match the previous EndOffset
		if expectedStartOffset >= 0 && entry.StartOffset != expectedStartOffset {
			// Gap in offsets - stop selection to maintain contiguity
			break
		}

		// Check max file count limit
		if p.cfg.MaxFilesToMerge > 0 && len(selected) >= p.cfg.MaxFilesToMerge {
			break
		}

		// Check max size limit before adding
		entrySize := int64(entry.ChunkLength)
		if p.cfg.MaxSizeBytes > 0 && totalSize+entrySize > p.cfg.MaxSizeBytes && len(selected) > 0 {
			break
		}

		// Apply age policy
		if p.cfg.MaxAgeMs > 0 {
			entryAge := nowMs - entry.CreatedAtMs
			if entryAge < p.cfg.MaxAgeMs {
				break
			}
		}

		selected = append(selected, entry)
		totalSize += entrySize
		totalRecords += entry.RecordCount
		expectedStartOffset = entry.EndOffset
	}

	if len(selected) == 0 {
		return nil, nil
	}

	// Check minimum entries threshold
	if len(selected) < p.cfg.MinEntries {
		return nil, nil
	}

	return &Result{
		StreamID:         streamID,
		Entries:          selected,
		TotalSizeBytes:   totalSize,
		TotalRecordCount: totalRecords,
		StartOffset:      selected[0].StartOffset,
		EndOffset:        selected[len(selected)-1].EndOffset,
	}, nil
}
