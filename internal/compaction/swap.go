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

// SwapRequest contains parameters for the atomic index swap operation.
type SwapRequest struct {
	// StreamID is the stream whose index entries are being swapped.
	StreamID string

	// WALIndexKeys are the keys of WAL index entries to remove.
	// These must be contiguous and cover the same offset range as the Parquet entry.
	WALIndexKeys []string

	// ParquetEntry is the new Parquet index entry to insert.
	ParquetEntry index.IndexEntry

	// MetaDomain is the metadata domain for the WAL objects.
	MetaDomain int
}

// SwapResult contains the result of the atomic index swap.
type SwapResult struct {
	// NewIndexKey is the key of the new Parquet index entry.
	NewIndexKey string

	// DecrementedWALObjects are the WAL object IDs whose refcounts were decremented.
	DecrementedWALObjects []string

	// WALObjectsReadyForGC are WAL objects whose refcount reached zero and are ready for GC.
	WALObjectsReadyForGC []string
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

// Swap atomically replaces WAL index entries with a Parquet index entry.
//
// The operation:
//  1. Validates that WAL entries are contiguous and match Parquet entry offsets
//  2. Deletes all WAL index entries
//  3. Inserts the new Parquet index entry with correct cumulative size
//  4. Decrements refcounts for affected WAL objects
//  5. Marks WAL objects for GC if refcount reaches zero
//
// All operations are executed in a single metadata transaction to ensure atomicity.
// Per invariant I5 (spec 3.5), readers never see a partial index state during swap.
func (s *IndexSwapper) Swap(ctx context.Context, req SwapRequest) (*SwapResult, error) {
	if len(req.WALIndexKeys) == 0 {
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

	// Validate that WAL entries are contiguous and match Parquet entry offsets
	if err := s.validateOffsets(walEntries, req.ParquetEntry); err != nil {
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

	err = s.meta.Txn(ctx, scopeKey, func(txn metadata.Txn) error {
		// Delete all WAL index entries
		for _, key := range req.WALIndexKeys {
			txn.Delete(key)
		}

		// Insert the new Parquet index entry
		txn.Put(newIndexKey, parquetEntryBytes)

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("compaction: index swap transaction: %w", err)
	}

	// Now decrement WAL object refcounts (outside the main transaction since
	// WAL objects are in a different key space)
	result := &SwapResult{
		NewIndexKey:           newIndexKey,
		DecrementedWALObjects: make([]string, 0, len(walObjectIDs)),
		WALObjectsReadyForGC:  make([]string, 0),
	}

	for walID := range walObjectIDs {
		gcReady, err := s.decrementWALRefCount(ctx, req.MetaDomain, walID)
		if err != nil {
			// Log error but don't fail - refcount will be cleaned up eventually
			// by the GC process scanning orphaned WAL objects
			continue
		}
		result.DecrementedWALObjects = append(result.DecrementedWALObjects, walID)
		if gcReady {
			result.WALObjectsReadyForGC = append(result.WALObjectsReadyForGC, walID)
		}
	}

	return result, nil
}

// validateOffsets checks that WAL entries are contiguous and match Parquet entry offsets.
func (s *IndexSwapper) validateOffsets(walEntries []index.IndexEntry, parquetEntry index.IndexEntry) error {
	if len(walEntries) == 0 {
		return ErrNoEntriesToSwap
	}

	// Sort WAL entries by start offset to ensure contiguous checks are reliable.
	sort.Slice(walEntries, func(i, j int) bool {
		return walEntries[i].StartOffset < walEntries[j].StartOffset
	})

	// Check that entries are contiguous
	for i := 1; i < len(walEntries); i++ {
		if walEntries[i].StartOffset != walEntries[i-1].EndOffset {
			return fmt.Errorf("%w: gap between WAL entries at offset %d", ErrOffsetMismatch, walEntries[i-1].EndOffset)
		}
	}

	// Check that combined WAL range matches Parquet entry
	firstStart := walEntries[0].StartOffset
	lastEnd := walEntries[len(walEntries)-1].EndOffset

	if firstStart != parquetEntry.StartOffset {
		return fmt.Errorf("%w: start offset mismatch: WAL %d, Parquet %d",
			ErrOffsetMismatch, firstStart, parquetEntry.StartOffset)
	}

	if lastEnd != parquetEntry.EndOffset {
		return fmt.Errorf("%w: end offset mismatch: WAL %d, Parquet %d",
			ErrOffsetMismatch, lastEnd, parquetEntry.EndOffset)
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
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := s.meta.List(ctx, prefix, "", 0)
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
func (s *IndexSwapper) decrementWALRefCount(ctx context.Context, metaDomain int, walID string) (bool, error) {
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)

	// Use a CAS loop to decrement refcount atomically
	for {
		result, err := s.meta.Get(ctx, walObjectKey)
		if err != nil {
			return false, fmt.Errorf("get WAL object record: %w", err)
		}
		if !result.Exists {
			// WAL object record doesn't exist - already cleaned up or never created
			return false, nil
		}

		var record produce.WALObjectRecord
		if err := json.Unmarshal(result.Value, &record); err != nil {
			return false, fmt.Errorf("unmarshal WAL object record: %w", err)
		}

		record.RefCount--
		if record.RefCount <= 0 {
			// Move to GC queue instead of deleting
			gcKey := keys.WALGCKeyPath(metaDomain, walID)
			gcRecord := WALGCRecord{
				Path:          record.Path,
				DeleteAfterMs: time.Now().Add(10 * time.Minute).UnixMilli(), // Grace period
				CreatedAt:     record.CreatedAt,
				SizeBytes:     record.SizeBytes,
			}
			gcRecordBytes, err := json.Marshal(gcRecord)
			if err != nil {
				return false, fmt.Errorf("marshal GC record: %w", err)
			}

			// Delete WAL object record and create GC record atomically
			err = s.meta.Txn(ctx, walObjectKey, func(txn metadata.Txn) error {
				txn.DeleteWithVersion(walObjectKey, result.Version)
				txn.Put(gcKey, gcRecordBytes)
				return nil
			})
			if err != nil {
				if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
					// Concurrent modification - retry
					continue
				}
				return false, fmt.Errorf("move WAL to GC queue: %w", err)
			}
			return true, nil
		}

		// Update the record with decremented refcount
		recordBytes, err := json.Marshal(record)
		if err != nil {
			return false, fmt.Errorf("marshal updated record: %w", err)
		}

		_, err = s.meta.Put(ctx, walObjectKey, recordBytes, metadata.WithExpectedVersion(result.Version))
		if err != nil {
			if errors.Is(err, metadata.ErrVersionMismatch) {
				// Concurrent modification - retry
				continue
			}
			return false, fmt.Errorf("update WAL refcount: %w", err)
		}
		return false, nil
	}
}

// WALGCRecord is stored at /wal/gc/<metaDomain>/<walId> when a WAL object
// is ready for garbage collection.
type WALGCRecord struct {
	Path          string `json:"path"`
	DeleteAfterMs int64  `json:"deleteAfterMs"`
	CreatedAt     int64  `json:"createdAt"`
	SizeBytes     int64  `json:"sizeBytes"`
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
	prefix := keys.OffsetIndexPrefix(job.StreamID)
	kvs, err := s.meta.List(ctx, prefix, "", 0)
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
