// Package catalog implements Iceberg catalog clients for Dray's stream-table duality.
package catalog

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
)

// SnapshotPropertyJobID is the snapshot summary property key for tracking the
// compaction job ID. This enables idempotent commit detection per SPEC.md 11.7.
const SnapshotPropertyJobID = "dray.job-id"

// AppenderConfig configures the data file appender.
type AppenderConfig struct {
	// Catalog is the Iceberg catalog to use for table operations.
	Catalog Catalog

	// Namespace is the Iceberg namespace for tables.
	// Default: ["dray"]
	Namespace []string

	// MaxRetries is the maximum number of retries on commit conflict.
	// Default: 5
	MaxRetries int

	// InitialBackoff is the initial backoff duration.
	// Default: 100ms
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration.
	// Default: 10s
	MaxBackoff time.Duration

	// BackoffMultiplier is the multiplier for exponential backoff.
	// Default: 2.0
	BackoffMultiplier float64

	// JitterFactor is the random jitter factor (0-1).
	// Default: 0.1
	JitterFactor float64
}

// DefaultAppenderConfig returns sensible defaults for AppenderConfig.
func DefaultAppenderConfig(catalog Catalog) AppenderConfig {
	return AppenderConfig{
		Catalog:           catalog,
		Namespace:         []string{"dray"},
		MaxRetries:        5,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        10 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFactor:      0.1,
	}
}

// Appender handles data file appending to Iceberg tables with retry logic.
// It implements the AppendFiles operation per SPEC.md section 11.6 with
// proper handling of commit conflicts through exponential backoff retry.
type Appender struct {
	cfg AppenderConfig
}

// NewAppender creates a new Appender with the given configuration.
func NewAppender(cfg AppenderConfig) *Appender {
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 5
	}
	if cfg.InitialBackoff == 0 {
		cfg.InitialBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 10 * time.Second
	}
	if cfg.BackoffMultiplier == 0 {
		cfg.BackoffMultiplier = 2.0
	}
	if cfg.JitterFactor == 0 {
		cfg.JitterFactor = 0.1
	}
	if len(cfg.Namespace) == 0 {
		cfg.Namespace = []string{"dray"}
	}

	return &Appender{cfg: cfg}
}

// AppendResult contains the result of a successful append operation.
type AppendResult struct {
	// Snapshot is the new snapshot created by the append.
	// For idempotent skips, this is the existing snapshot that contained the commit.
	Snapshot *Snapshot

	// Table is the table that was appended to.
	Table Table

	// Attempts is the number of attempts made (1 = first try succeeded).
	// For idempotent skips, this is 0.
	Attempts int

	// IdempotentSkipped indicates the commit was skipped because it was
	// already applied in a prior snapshot. This enables safe retry of
	// compaction jobs per SPEC.md section 11.7.
	IdempotentSkipped bool
}

// AppendFiles appends data files to an Iceberg table with retry on conflict.
//
// This method:
//  1. Loads the table from the catalog
//  2. Attempts to append the data files
//  3. On commit conflict, refreshes the table and retries with exponential backoff
//  4. Returns the new snapshot on success
//
// The files parameter must contain valid DataFile entries with:
//   - Path: Full object storage path to the Parquet file
//   - Format: File format (use FormatParquet for Dray)
//   - PartitionValue: Kafka partition ID
//   - RecordCount: Number of records in the file
//   - FileSizeBytes: File size in bytes
//   - Optional: ColumnSizes, ValueCounts, NullValueCounts, LowerBounds, UpperBounds
//
// Returns ErrTableNotFound if the table does not exist.
// Returns error after MaxRetries if all attempts fail.
func (a *Appender) AppendFiles(ctx context.Context, topicName string, files []DataFile, opts *AppendFilesOptions) (*AppendResult, error) {
	if a.cfg.Catalog == nil {
		return nil, ErrCatalogUnavailable
	}

	if len(files) == 0 {
		return nil, errors.New("no files to append")
	}

	identifier := TableIdentifier{
		Namespace: a.cfg.Namespace,
		Name:      topicName,
	}

	table, err := a.cfg.Catalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}

	backoff := a.cfg.InitialBackoff
	var lastErr error

	for attempt := 1; attempt <= a.cfg.MaxRetries; attempt++ {
		snapshot, err := table.AppendFiles(ctx, files, opts)
		if err == nil {
			return &AppendResult{
				Snapshot: snapshot,
				Table:    table,
				Attempts: attempt,
			}, nil
		}

		// Check if this is a commit conflict that we should retry
		if !errors.Is(err, ErrCommitConflict) {
			return nil, err
		}

		lastErr = err

		// Don't retry if this was the last attempt
		if attempt == a.cfg.MaxRetries {
			break
		}

		// Calculate backoff with jitter
		sleepDuration := a.calculateBackoff(backoff)

		// Wait before retrying
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleepDuration):
		}

		// Refresh the table to get latest metadata before retry
		if err := table.Refresh(ctx); err != nil {
			return nil, fmt.Errorf("failed to refresh table before retry: %w", err)
		}

		// Increase backoff for next iteration
		backoff = time.Duration(float64(backoff) * a.cfg.BackoffMultiplier)
		if backoff > a.cfg.MaxBackoff {
			backoff = a.cfg.MaxBackoff
		}
	}

	return nil, fmt.Errorf("commit failed after %d attempts: %w", a.cfg.MaxRetries, lastErr)
}

// calculateBackoff calculates the sleep duration with jitter.
func (a *Appender) calculateBackoff(base time.Duration) time.Duration {
	if a.cfg.JitterFactor <= 0 {
		return base
	}

	// Add random jitter: base * (1 +/- jitterFactor)
	jitterRange := float64(base) * a.cfg.JitterFactor
	jitter := (rand.Float64() * 2 * jitterRange) - jitterRange
	result := time.Duration(float64(base) + jitter)

	if result < 0 {
		result = 0
	}
	return result
}

// AppendFilesForStream is a convenience method that appends files for a compaction job.
// It includes the job ID in the snapshot properties for idempotent retries per SPEC.md 11.7.
//
// This method implements idempotent commit semantics:
//  1. Before attempting the commit, it checks if the job ID was already applied
//  2. If the job was already committed, returns the existing snapshot with IdempotentSkipped=true
//  3. Otherwise proceeds with the normal append operation
//
// This ensures that crash-recovery retries of compaction jobs are safe and do not
// create duplicate data in the Iceberg table.
func (a *Appender) AppendFilesForStream(ctx context.Context, topicName, jobID string, files []DataFile) (*AppendResult, error) {
	if a.cfg.Catalog == nil {
		return nil, ErrCatalogUnavailable
	}

	identifier := TableIdentifier{
		Namespace: a.cfg.Namespace,
		Name:      topicName,
	}

	table, err := a.cfg.Catalog.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}

	// Check if this job ID was already committed (idempotent retry detection)
	existingSnapshot, err := a.findCommitByJobID(ctx, table, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to check for prior commit: %w", err)
	}

	if existingSnapshot != nil {
		slog.Info("iceberg commit already applied, skipping",
			"topic", topicName,
			"jobId", jobID,
			"snapshotId", existingSnapshot.SnapshotID,
		)
		return &AppendResult{
			Snapshot:          existingSnapshot,
			Table:             table,
			Attempts:          0,
			IdempotentSkipped: true,
		}, nil
	}

	// Proceed with normal append
	opts := &AppendFilesOptions{
		SnapshotProperties: map[string]string{
			SnapshotPropertyJobID: jobID,
		},
	}
	return a.AppendFiles(ctx, topicName, files, opts)
}

// findCommitByJobID searches the table's snapshots for one with a matching job ID.
// Returns the snapshot if found, nil if not found.
func (a *Appender) findCommitByJobID(ctx context.Context, table Table, jobID string) (*Snapshot, error) {
	snapshots, err := table.Snapshots(ctx)
	if err != nil {
		return nil, err
	}

	for i := range snapshots {
		snap := &snapshots[i]
		if snap.Summary != nil {
			if snap.Summary[SnapshotPropertyJobID] == jobID {
				return snap, nil
			}
		}
	}

	return nil, nil
}

// IsCommitApplied checks whether a compaction job with the given ID has already
// been committed to the Iceberg table. This enables idempotent retry detection
// per SPEC.md section 11.7.
func (a *Appender) IsCommitApplied(ctx context.Context, topicName, jobID string) (bool, *Snapshot, error) {
	if a.cfg.Catalog == nil {
		return false, nil, ErrCatalogUnavailable
	}

	identifier := TableIdentifier{
		Namespace: a.cfg.Namespace,
		Name:      topicName,
	}

	table, err := a.cfg.Catalog.LoadTable(ctx, identifier)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return false, nil, nil
		}
		return false, nil, err
	}

	snapshot, err := a.findCommitByJobID(ctx, table, jobID)
	if err != nil {
		return false, nil, err
	}

	return snapshot != nil, snapshot, nil
}

// BuildDataFileFromStats creates a DataFile from compaction output statistics.
// This is a helper to build the DataFile struct from compaction worker output.
func BuildDataFileFromStats(path string, partition int32, recordCount, fileSizeBytes int64, stats *DataFileStats) DataFile {
	df := DataFile{
		Path:           path,
		Format:         FormatParquet,
		PartitionValue: partition,
		RecordCount:    recordCount,
		FileSizeBytes:  fileSizeBytes,
	}

	if stats != nil {
		df.ColumnSizes = stats.ColumnSizes
		df.ValueCounts = stats.ValueCounts
		df.NullValueCounts = stats.NullValueCounts
		df.LowerBounds = stats.LowerBounds
		df.UpperBounds = stats.UpperBounds
		df.SplitOffsets = stats.SplitOffsets
		df.SortOrderID = stats.SortOrderID
	}

	return df
}

// DataFileStats contains optional statistics for a data file.
// These are used for query optimization in Iceberg.
type DataFileStats struct {
	// ColumnSizes maps field IDs to their total size in bytes.
	ColumnSizes map[int32]int64

	// ValueCounts maps field IDs to their non-null value counts.
	ValueCounts map[int32]int64

	// NullValueCounts maps field IDs to their null value counts.
	NullValueCounts map[int32]int64

	// LowerBounds maps field IDs to their lower bound values.
	LowerBounds map[int32][]byte

	// UpperBounds maps field IDs to their upper bound values.
	UpperBounds map[int32][]byte

	// SplitOffsets contains the byte offsets of split points for parallel reads.
	SplitOffsets []int64

	// SortOrderID references the sort order, if the file is sorted.
	SortOrderID *int32
}

// NewOffsetBounds creates lower and upper bound byte arrays for offset field.
// Offsets are encoded as big-endian int64 for proper lexicographic comparison.
func NewOffsetBounds(minOffset, maxOffset int64) (lower, upper []byte) {
	lower = make([]byte, 8)
	upper = make([]byte, 8)
	binary.BigEndian.PutUint64(lower, uint64(minOffset))
	binary.BigEndian.PutUint64(upper, uint64(maxOffset))
	return lower, upper
}

// NewTimestampBounds creates lower and upper bound byte arrays for timestamp field.
// Timestamps are encoded as big-endian int64 for proper lexicographic comparison.
func NewTimestampBounds(minTs, maxTs int64) (lower, upper []byte) {
	lower = make([]byte, 8)
	upper = make([]byte, 8)
	binary.BigEndian.PutUint64(lower, uint64(minTs))
	binary.BigEndian.PutUint64(upper, uint64(maxTs))
	return lower, upper
}

// NewPartitionBound creates a bound byte array for partition field.
// Partitions are encoded as big-endian int32.
func NewPartitionBound(partition int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(partition))
	return b
}

// DefaultDataFileStats creates DataFileStats with offset and timestamp bounds.
// This is a helper for the common case where only offset and timestamp stats are known.
func DefaultDataFileStats(partition int32, minOffset, maxOffset, minTs, maxTs int64, recordCount int64) *DataFileStats {
	lowerOffset, upperOffset := NewOffsetBounds(minOffset, maxOffset)
	lowerTs, upperTs := NewTimestampBounds(minTs, maxTs)
	partitionBound := NewPartitionBound(partition)

	return &DataFileStats{
		LowerBounds: map[int32][]byte{
			FieldIDPartition:   partitionBound,
			FieldIDOffset:      lowerOffset,
			FieldIDTimestampMs: lowerTs,
		},
		UpperBounds: map[int32][]byte{
			FieldIDPartition:   partitionBound,
			FieldIDOffset:      upperOffset,
			FieldIDTimestampMs: upperTs,
		},
		ValueCounts: map[int32]int64{
			FieldIDPartition:   recordCount,
			FieldIDOffset:      recordCount,
			FieldIDTimestampMs: recordCount,
		},
	}
}
