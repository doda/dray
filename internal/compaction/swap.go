// Package compaction implements stream compaction for the Dray broker.
// This file implements atomic index swap: remove WAL entries, insert Parquet entry.
package compaction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dray-io/dray/internal/gc"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/produce"
)

// IndexSwapper errors.
var (
	// ErrNoEntriesToSwap is returned when there are no index entries to swap.
	ErrNoEntriesToSwap = errors.New("compaction: no index entries to swap")

	// ErrOffsetMismatch is returned when the Parquet entry offsets don't match
	// the combined WAL entry offsets.
	ErrOffsetMismatch = errors.New("compaction: offset range mismatch")
)

const defaultParquetGCGracePeriodMs int64 = 10 * 60 * 1000

// SwapRequest contains parameters for the atomic index swap operation.
type SwapRequest struct {
	// StreamID is the stream whose index entries are being swapped.
	StreamID string

	// WALIndexKeys are the keys of WAL index entries to remove.
	// These must be contiguous and cover the same offset range as the Parquet entry.
	WALIndexKeys []string

	// ParquetIndexKeys are the keys of Parquet index entries to remove (for re-compaction).
	// When compacting multiple Parquet files into one, these old Parquet entries are replaced.
	ParquetIndexKeys []string

	// ParquetEntry is the new Parquet index entry to insert.
	ParquetEntry index.IndexEntry

	// MetaDomain is the metadata domain for the WAL objects.
	MetaDomain int

	// GracePeriodMs is the grace period before old Parquet files can be deleted.
	// If 0, defaults to 10 minutes.
	GracePeriodMs int64

	// IcebergEnabled indicates whether Parquet files are tracked in Iceberg metadata.
	IcebergEnabled bool

	// IcebergRemovalConfirmed indicates old Parquet files were removed from Iceberg metadata.
	IcebergRemovalConfirmed bool
}

// SwapResult contains the result of the atomic index swap.
type SwapResult struct {
	// NewIndexKey is the key of the new Parquet index entry.
	NewIndexKey string

	// DecrementedWALObjects are the WAL object IDs whose refcounts were decremented.
	DecrementedWALObjects []string

	// WALObjectsReadyForGC are WAL objects whose refcount reached zero and are ready for GC.
	WALObjectsReadyForGC []string

	// ParquetFilesScheduledForGC are Parquet file paths that were scheduled for GC.
	// These are old Parquet files replaced during re-compaction.
	ParquetFilesScheduledForGC []string

	// ParquetGCCandidates are old Parquet files eligible for GC once the job reaches DONE.
	ParquetGCCandidates []ParquetGCCandidate

	// ParquetGCGracePeriodMs is the grace period to apply when scheduling GC at DONE.
	ParquetGCGracePeriodMs int64
}

// IndexSwapper handles atomic index swaps during compaction.
// It removes WAL index entries and inserts a Parquet index entry
// in a single metadata transaction per spec section 11.
type IndexSwapper struct {
	meta metadata.MetadataStore
}

// NewIndexSwapper creates a new index swapper.
func NewIndexSwapper(meta metadata.MetadataStore) *IndexSwapper {
	return &IndexSwapper{meta: meta}
}

// Swap atomically replaces WAL and/or Parquet index entries with a new Parquet index entry.
//
// The operation:
//  1. Validates that source entries are contiguous and match new Parquet entry offsets
//  2. Deletes all WAL index entries
//  3. Deletes all old Parquet index entries (for re-compaction)
//  4. Inserts the new Parquet index entry with correct cumulative size
//  5. Decrements refcounts for affected WAL objects
//  6. Marks WAL objects for GC if refcount reaches zero
//  7. Schedules old Parquet files for GC with grace period
//
// All operations are executed in a single metadata transaction to ensure atomicity.
// Per invariant I5 (spec 3.5), readers never see a partial index state during swap.
func (s *IndexSwapper) Swap(ctx context.Context, req SwapRequest) (*SwapResult, error) {
	if len(req.WALIndexKeys) == 0 && len(req.ParquetIndexKeys) == 0 {
		return nil, ErrNoEntriesToSwap
	}

	if req.StreamID == "" {
		return nil, ErrInvalidStreamID
	}

	// First, read all WAL entries to validate and extract WAL object IDs
	walEntries := make([]index.IndexEntry, 0, len(req.WALIndexKeys))
	walObjectIDs := make(map[string]bool)

	for _, key := range req.WALIndexKeys {
		result, err := s.meta.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("compaction: get WAL entry %s: %w", key, err)
		}
		if !result.Exists {
			return nil, fmt.Errorf("compaction: WAL entry not found: %s", key)
		}

		var entry index.IndexEntry
		if err := json.Unmarshal(result.Value, &entry); err != nil {
			return nil, fmt.Errorf("compaction: unmarshal WAL entry: %w", err)
		}

		if entry.FileType != index.FileTypeWAL {
			return nil, fmt.Errorf("compaction: entry %s is not a WAL entry", key)
		}

		walEntries = append(walEntries, entry)
		if entry.WalID != "" {
			walObjectIDs[entry.WalID] = true
		}
	}

	// Read all Parquet entries to validate and collect for GC
	parquetEntries := make([]index.IndexEntry, 0, len(req.ParquetIndexKeys))
	for _, key := range req.ParquetIndexKeys {
		result, err := s.meta.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("compaction: get Parquet entry %s: %w", key, err)
		}
		if !result.Exists {
			return nil, fmt.Errorf("compaction: Parquet entry not found: %s", key)
		}

		var entry index.IndexEntry
		if err := json.Unmarshal(result.Value, &entry); err != nil {
			return nil, fmt.Errorf("compaction: unmarshal Parquet entry: %w", err)
		}

		if entry.FileType != index.FileTypeParquet {
			return nil, fmt.Errorf("compaction: entry %s is not a Parquet entry", key)
		}

		parquetEntries = append(parquetEntries, entry)
	}

	// Combine all source entries for validation
	allSourceEntries := append(walEntries, parquetEntries...)
	if err := s.validateOffsets(allSourceEntries, req.ParquetEntry); err != nil {
		return nil, err
	}

	// Calculate the cumulative size for the new Parquet entry.
	// We need to find the entry just before our range and add Parquet size to its cumulative size.
	prevCumulativeSize, err := s.getPreviousCumulativeSize(ctx, req.StreamID, req.ParquetEntry.StartOffset)
	if err != nil {
		return nil, fmt.Errorf("compaction: get previous cumulative size: %w", err)
	}

	// Set cumulative size on the Parquet entry
	parquetEntry := req.ParquetEntry
	parquetEntry.CumulativeSize = prevCumulativeSize + int64(parquetEntry.ParquetSizeBytes)

	// Create the new Parquet index key
	newIndexKey, err := keys.OffsetIndexKeyPath(req.StreamID, parquetEntry.EndOffset, parquetEntry.CumulativeSize)
	if err != nil {
		return nil, fmt.Errorf("compaction: create index key: %w", err)
	}

	parquetEntryBytes, err := json.Marshal(parquetEntry)
	if err != nil {
		return nil, fmt.Errorf("compaction: marshal Parquet entry: %w", err)
	}

	// Execute the atomic swap transaction
	// We use the stream's offset-index prefix as the scope key
	scopeKey := keys.OffsetIndexPrefix(req.StreamID)

	walIDs := make([]string, 0, len(walObjectIDs))
	for walID := range walObjectIDs {
		walIDs = append(walIDs, walID)
	}
	sort.Strings(walIDs)

	const maxTxnRetries = 3
	var decrementedWALObjects []string
	var walObjectsReadyForGC []string

	for attempt := 0; attempt < maxTxnRetries; attempt++ {
		var txnDecremented []string
		var txnGCReady []string

		err = s.meta.Txn(ctx, scopeKey, func(txn metadata.Txn) error {
			// Delete all WAL index entries
			for _, key := range req.WALIndexKeys {
				txn.Delete(key)
			}

			// Delete all old Parquet index entries (for re-compaction)
			for _, key := range req.ParquetIndexKeys {
				txn.Delete(key)
			}

			// Insert the new Parquet index entry
			txn.Put(newIndexKey, parquetEntryBytes)

			// Decrement WAL refcounts within the same transaction
			for _, walID := range walIDs {
				decremented, gcReady, err := s.decrementWALRefCountInTxn(txn, req.MetaDomain, walID)
				if err != nil {
					return err
				}
				if decremented {
					txnDecremented = append(txnDecremented, walID)
				}
				if gcReady {
					txnGCReady = append(txnGCReady, walID)
				}
			}

			return nil
		})

		if err == nil {
			decrementedWALObjects = txnDecremented
			walObjectsReadyForGC = txnGCReady
			break
		}

		if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
			if attempt < maxTxnRetries-1 {
				continue
			}
		}

		return nil, fmt.Errorf("compaction: index swap transaction: %w", err)
	}

	result := &SwapResult{
		NewIndexKey:                newIndexKey,
		DecrementedWALObjects:      decrementedWALObjects,
		WALObjectsReadyForGC:       walObjectsReadyForGC,
		ParquetFilesScheduledForGC: make([]string, 0, len(parquetEntries)),
		ParquetGCCandidates:        make([]ParquetGCCandidate, 0, len(parquetEntries)),
	}

	if len(parquetEntries) > 0 {
		gracePeriodMs := req.GracePeriodMs
		if gracePeriodMs <= 0 {
			gracePeriodMs = defaultParquetGCGracePeriodMs
		}
		result.ParquetGCGracePeriodMs = gracePeriodMs

		for _, entry := range parquetEntries {
			result.ParquetGCCandidates = append(result.ParquetGCCandidates, ParquetGCCandidate{
				Path:                    entry.ParquetPath,
				CreatedAtMs:             entry.CreatedAtMs,
				SizeBytes:               int64(entry.ParquetSizeBytes),
				IcebergEnabled:          req.IcebergEnabled,
				IcebergRemovalConfirmed: req.IcebergRemovalConfirmed || !req.IcebergEnabled,
			})
		}
	}

	return result, nil
}

// validateOffsets checks that source entries are contiguous and match new Parquet entry offsets.
func (s *IndexSwapper) validateOffsets(sourceEntries []index.IndexEntry, newParquetEntry index.IndexEntry) error {
	if len(sourceEntries) == 0 {
		return ErrNoEntriesToSwap
	}

	// Sort entries by start offset to ensure contiguous checks are reliable.
	sort.Slice(sourceEntries, func(i, j int) bool {
		return sourceEntries[i].StartOffset < sourceEntries[j].StartOffset
	})

	// Check that entries are contiguous
	for i := 1; i < len(sourceEntries); i++ {
		if sourceEntries[i].StartOffset != sourceEntries[i-1].EndOffset {
			return fmt.Errorf("%w: gap between entries at offset %d", ErrOffsetMismatch, sourceEntries[i-1].EndOffset)
		}
	}

	// Check that combined source range matches new Parquet entry
	firstStart := sourceEntries[0].StartOffset
	lastEnd := sourceEntries[len(sourceEntries)-1].EndOffset

	if firstStart != newParquetEntry.StartOffset {
		return fmt.Errorf("%w: start offset mismatch: source %d, new Parquet %d",
			ErrOffsetMismatch, firstStart, newParquetEntry.StartOffset)
	}

	if lastEnd != newParquetEntry.EndOffset {
		return fmt.Errorf("%w: end offset mismatch: source %d, new Parquet %d",
			ErrOffsetMismatch, lastEnd, newParquetEntry.EndOffset)
	}

	return nil
}

// getPreviousCumulativeSize finds the cumulative size of the entry just before startOffset.
func (s *IndexSwapper) getPreviousCumulativeSize(ctx context.Context, streamID string, startOffset int64) (int64, error) {
	if startOffset == 0 {
		// No previous entry, cumulative size starts at 0
		return 0, nil
	}

	// List entries with endOffset <= startOffset
	// The last such entry is the one just before our range
	startKey, err := keys.OffsetIndexStartKey(streamID, 0)
	if err != nil {
		return 0, fmt.Errorf("build start key: %w", err)
	}
	endKey := keys.OffsetIndexEndKey(streamID)
	kvs, err := s.meta.List(ctx, startKey, endKey, 0)
	if err != nil {
		return 0, err
	}

	var prevCumulativeSize int64
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			return 0, err
		}

		if entry.EndOffset <= startOffset {
			// This entry is before our range
			prevCumulativeSize = entry.CumulativeSize
		} else {
			// We've passed our range
			break
		}
	}

	return prevCumulativeSize, nil
}

// decrementWALRefCount atomically decrements the refcount for a WAL object.
// Returns true if the refcount reached zero and the object is ready for GC.
func (s *IndexSwapper) decrementWALRefCountInTxn(txn metadata.Txn, metaDomain int, walID string) (bool, bool, error) {
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)

	value, version, err := txn.Get(walObjectKey)
	if err != nil {
		if errors.Is(err, metadata.ErrKeyNotFound) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("get WAL object record: %w", err)
	}

	var record produce.WALObjectRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return false, false, fmt.Errorf("unmarshal WAL object record: %w", err)
	}

	if record.RefCount <= 1 {
		gcKey := keys.WALGCKeyPath(metaDomain, walID)
		gcRecord := gc.WALGCRecord{
			Path:          record.Path,
			DeleteAfterMs: time.Now().Add(10 * time.Minute).UnixMilli(),
			CreatedAt:     record.CreatedAt,
			SizeBytes:     record.SizeBytes,
		}
		gcRecordBytes, err := json.Marshal(gcRecord)
		if err != nil {
			return false, false, fmt.Errorf("marshal GC record: %w", err)
		}
		txn.DeleteWithVersion(walObjectKey, version)
		txn.Put(gcKey, gcRecordBytes)
		return true, true, nil
	}

	record.RefCount--
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return false, false, fmt.Errorf("marshal updated record: %w", err)
	}
	txn.PutWithVersion(walObjectKey, recordBytes, version)
	return true, false, nil
}

// SwapFromJob performs an index swap using information from a compaction job.
// This is a convenience method for the compaction worker.
func (s *IndexSwapper) SwapFromJob(ctx context.Context, job *Job, parquetPath string, parquetSizeBytes int64, parquetRecordCount int64, metaDomain int) (*SwapResult, error) {
	if job.StreamID == "" {
		return nil, ErrInvalidStreamID
	}
	if parquetRecordCount <= 0 {
		return nil, fmt.Errorf("compaction: parquet record count must be positive")
	}

	// Build the list of WAL index entries to swap
	// The job should have the source offset range
	startKey, err := keys.OffsetIndexStartKey(job.StreamID, 0)
	if err != nil {
		return nil, fmt.Errorf("build start key: %w", err)
	}
	endKey := keys.OffsetIndexEndKey(job.StreamID)
	kvs, err := s.meta.List(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, fmt.Errorf("list index entries: %w", err)
	}

	var walIndexKeys []string
	var minTimestamp, maxTimestamp int64

	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			return nil, fmt.Errorf("unmarshal index entry: %w", err)
		}

		// Only consider WAL entries in the job's offset range
		if entry.FileType != index.FileTypeWAL {
			continue
		}
		if entry.StartOffset < job.SourceStartOffset || entry.EndOffset > job.SourceEndOffset {
			continue
		}

		walIndexKeys = append(walIndexKeys, kv.Key)

		if minTimestamp == 0 || entry.MinTimestampMs < minTimestamp {
			minTimestamp = entry.MinTimestampMs
		}
		if entry.MaxTimestampMs > maxTimestamp {
			maxTimestamp = entry.MaxTimestampMs
		}
	}

	if len(walIndexKeys) == 0 {
		return nil, ErrNoEntriesToSwap
	}

	// Create the Parquet index entry
	parquetEntry := index.IndexEntry{
		StreamID:         job.StreamID,
		StartOffset:      job.SourceStartOffset,
		EndOffset:        job.SourceEndOffset,
		FileType:         index.FileTypeParquet,
		RecordCount:      uint32(parquetRecordCount),
		MessageCount:     uint32(parquetRecordCount),
		MinTimestampMs:   minTimestamp,
		MaxTimestampMs:   maxTimestamp,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      parquetPath,
		ParquetSizeBytes: uint64(parquetSizeBytes),
	}

	return s.Swap(ctx, SwapRequest{
		StreamID:     job.StreamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   metaDomain,
	})
}
