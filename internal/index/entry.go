package index

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// FileType indicates whether an index entry points to WAL or Parquet data.
type FileType string

const (
	FileTypeWAL     FileType = "WAL"
	FileTypeParquet FileType = "PARQUET"
)

// BatchIndexEntry describes a single record batch within a WAL chunk.
// Used for efficient offset lookup without scanning the whole chunk.
type BatchIndexEntry struct {
	// BatchStartOffsetDelta is the offset delta from startOffset to batch start.
	BatchStartOffsetDelta uint32 `json:"batchStartOffsetDelta"`
	// BatchLastOffsetDelta is the offset delta from startOffset to batch last record.
	BatchLastOffsetDelta uint32 `json:"batchLastOffsetDelta"`
	// BatchOffsetInChunk is the byte offset of this batch within the chunk.
	BatchOffsetInChunk uint32 `json:"batchOffsetInChunk"`
	// BatchLength is the batch size in bytes.
	BatchLength uint32 `json:"batchLength"`
	// MinTimestampMs is the minimum timestamp in this batch.
	MinTimestampMs int64 `json:"minTimestampMs"`
	// MaxTimestampMs is the maximum timestamp in this batch.
	MaxTimestampMs int64 `json:"maxTimestampMs"`
}

// IndexEntry represents an entry in the offset index per SPEC 6.3.3.
// It maps offset ranges to either WAL or Parquet storage locations.
type IndexEntry struct {
	// Common fields
	StreamID       string   `json:"streamId"`
	StartOffset    int64    `json:"startOffset"`
	EndOffset      int64    `json:"endOffset"`
	CumulativeSize int64    `json:"cumulativeSize"`
	CreatedAtMs    int64    `json:"createdAtMs"`
	FileType       FileType `json:"fileType"`
	RecordCount    uint32   `json:"recordCount"`
	MessageCount   uint32   `json:"messageCount"`
	MinTimestampMs int64    `json:"minTimestampMs"`
	MaxTimestampMs int64    `json:"maxTimestampMs"`

	// WAL-specific fields
	WalID       string            `json:"walId,omitempty"`
	WalPath     string            `json:"walPath,omitempty"`
	ChunkOffset uint64            `json:"chunkOffset,omitempty"`
	ChunkLength uint32            `json:"chunkLength,omitempty"`
	BatchIndex  []BatchIndexEntry `json:"batchIndex,omitempty"`

	// Parquet-specific fields
	ParquetID         string `json:"parquetId,omitempty"`
	ParquetPath       string `json:"parquetPath,omitempty"`
	ParquetSizeBytes  uint64 `json:"parquetSizeBytes,omitempty"`
	IcebergDataFileID string `json:"icebergDataFileId,omitempty"`
}

// Common errors for index entry operations.
var (
	// ErrInvalidRecordCount is returned when record count is zero or negative.
	ErrInvalidRecordCount = errors.New("index: record count must be positive")

	// ErrInvalidChunkSize is returned when chunk size is zero.
	ErrInvalidChunkSize = errors.New("index: chunk size must be positive")
)

// AppendRequest contains the parameters for appending an index entry.
type AppendRequest struct {
	// StreamID is the stream to append to.
	StreamID string
	// RecordCount is the number of records in this entry.
	RecordCount uint32
	// ChunkSizeBytes is the size of this chunk in bytes (for cumulative size).
	ChunkSizeBytes int64
	// CreatedAtMs is the creation timestamp.
	CreatedAtMs int64
	// MinTimestampMs is the minimum record timestamp.
	MinTimestampMs int64
	// MaxTimestampMs is the maximum record timestamp.
	MaxTimestampMs int64

	// WAL-specific fields
	WalID       string
	WalPath     string
	ChunkOffset uint64
	ChunkLength uint32
	BatchIndex  []BatchIndexEntry
}

// AppendResult contains the result of appending an index entry.
type AppendResult struct {
	// StartOffset is the first offset assigned (inclusive).
	StartOffset int64
	// EndOffset is the last offset + 1 (exclusive), same as new HWM.
	EndOffset int64
	// NewHWMVersion is the version of the HWM after the update.
	NewHWMVersion metadata.Version
	// IndexKey is the full key of the created index entry.
	IndexKey string
}

// AppendIndexEntry atomically:
//   - Reads current hwm with version
//   - Allocates offset range: startOffset=hwm, endOffset=hwm+recordCount
//   - Creates index key with zero-padded offsetEnd and cumulativeSize
//   - Creates IndexEntry value with all fields per spec 6.3.3
//   - Updates hwm and creates index entry in a single transaction
//
// This ensures linearizable write ordering per partition (invariant I1).
func (sm *StreamManager) AppendIndexEntry(ctx context.Context, req AppendRequest) (*AppendResult, error) {
	if req.RecordCount == 0 {
		return nil, ErrInvalidRecordCount
	}
	if req.ChunkSizeBytes <= 0 {
		return nil, ErrInvalidChunkSize
	}

	hwmKey := keys.HwmKeyPath(req.StreamID)
	var result AppendResult

	err := sm.store.Txn(ctx, hwmKey, func(txn metadata.Txn) error {
		// Step 1: Read current hwm with version
		hwmValue, hwmVersion, err := txn.Get(hwmKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrStreamNotFound
			}
			return err
		}

		currentHWM, err := DecodeHWM(hwmValue)
		if err != nil {
			return err
		}

		// Step 2: Allocate offset range
		startOffset := currentHWM
		endOffset := currentHWM + int64(req.RecordCount)

		// Step 3: Calculate cumulative size
		// We need to read the previous index entry to get the previous cumulative size.
		// If no entries exist, cumulative size starts at 0.
		prevCumulativeSize := int64(0)

		// Lookup the last entry by listing only the current HWM offsetEnd key.
		startKey, err := keys.OffsetIndexStartKey(req.StreamID, currentHWM)
		if err != nil {
			return err
		}
		endKey := keys.OffsetIndexEndKey(req.StreamID)
		entries, err := sm.store.List(ctx, startKey, endKey, 1)
		if err != nil {
			return err
		}

		if len(entries) > 0 {
			lastEntry := entries[0]
			var lastIndexEntry IndexEntry
			if err := json.Unmarshal(lastEntry.Value, &lastIndexEntry); err != nil {
				return err
			}
			prevCumulativeSize = lastIndexEntry.CumulativeSize
		}

		cumulativeSize := prevCumulativeSize + req.ChunkSizeBytes

		// Step 4: Create index key with zero-padded offsetEnd and cumulativeSize
		indexKey, err := keys.OffsetIndexKeyPath(req.StreamID, endOffset, cumulativeSize)
		if err != nil {
			return err
		}

		// Step 5: Create IndexEntry value
		entry := IndexEntry{
			StreamID:       req.StreamID,
			StartOffset:    startOffset,
			EndOffset:      endOffset,
			CumulativeSize: cumulativeSize,
			CreatedAtMs:    req.CreatedAtMs,
			FileType:       FileTypeWAL,
			RecordCount:    req.RecordCount,
			MessageCount:   req.RecordCount, // Same as recordCount for Kafka records
			MinTimestampMs: req.MinTimestampMs,
			MaxTimestampMs: req.MaxTimestampMs,
			WalID:          req.WalID,
			WalPath:        req.WalPath,
			ChunkOffset:    req.ChunkOffset,
			ChunkLength:    req.ChunkLength,
			BatchIndex:     req.BatchIndex,
		}

		entryBytes, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		// Step 6: Atomically update hwm and create index entry
		txn.PutWithVersion(hwmKey, EncodeHWM(endOffset), hwmVersion)
		txn.Put(indexKey, entryBytes)

		// Populate result
		result = AppendResult{
			StartOffset:   startOffset,
			EndOffset:     endOffset,
			NewHWMVersion: hwmVersion + 1,
			IndexKey:      indexKey,
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// GetIndexEntry retrieves an index entry by its key.
func (sm *StreamManager) GetIndexEntry(ctx context.Context, indexKey string) (*IndexEntry, metadata.Version, error) {
	result, err := sm.store.Get(ctx, indexKey)
	if err != nil {
		return nil, 0, err
	}
	if !result.Exists {
		return nil, 0, metadata.ErrKeyNotFound
	}

	var entry IndexEntry
	if err := json.Unmarshal(result.Value, &entry); err != nil {
		return nil, 0, err
	}

	return &entry, result.Version, nil
}

// ListIndexEntries lists all index entries for a stream.
func (sm *StreamManager) ListIndexEntries(ctx context.Context, streamID string, limit int) ([]IndexEntry, error) {
	// Use full-depth keys for Oxia hierarchical key compatibility
	startKey, err := keys.OffsetIndexStartKey(streamID, 0)
	if err != nil {
		return nil, err
	}
	endKey := keys.OffsetIndexEndKey(streamID)
	kvs, err := sm.store.List(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, err
	}

	entries := make([]IndexEntry, 0, len(kvs))
	for _, kv := range kvs {
		var entry IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// LookupResult contains the result of an offset lookup.
type LookupResult struct {
	// Entry is the index entry that contains the requested offset.
	Entry *IndexEntry
	// Found is true if an entry containing the offset was found.
	Found bool
	// OffsetBeyondHWM is true if the requested offset is >= hwm.
	OffsetBeyondHWM bool
	// HWM is the current high watermark for the stream.
	HWM int64
}

// ErrOffsetBeyondHWM is returned when the requested offset is beyond the high watermark.
var ErrOffsetBeyondHWM = errors.New("index: offset beyond high watermark")

// LookupOffset finds the index entry that contains the requested offset.
// It uses a range query starting at the requested offset to find the smallest
// entry where endOffset > fetchOffset (i.e., the entry whose range includes fetchOffset).
//
// The key insight is that offset index keys are sorted by offsetEnd (zero-padded),
// so a List query starting at the fetchOffset will return entries in ascending order
// of their endOffset. The first entry where endOffset > fetchOffset is the one we want.
//
// Returns:
//   - LookupResult with Found=true and Entry populated if found
//   - LookupResult with Found=false and OffsetBeyondHWM=true if offset >= hwm
//   - Error if the stream doesn't exist or on other failures
//
// This supports both WAL and Parquet entry types transparently.
func (sm *StreamManager) LookupOffset(ctx context.Context, streamID string, fetchOffset int64) (*LookupResult, error) {
	// First, get the current HWM to check if the offset is beyond it
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		return nil, err
	}

	// Check if offset is beyond HWM
	if fetchOffset >= hwm {
		return &LookupResult{
			Found:           false,
			OffsetBeyondHWM: true,
			HWM:             hwm,
		}, nil
	}

	// Handle negative offset (shouldn't happen but be defensive)
	if fetchOffset < 0 {
		fetchOffset = 0
	}

	// Build the start and end keys for the range query.
	// We need to find entries where endOffset > fetchOffset.
	// Since keys are sorted by offsetEnd (zero-padded), we start the query at fetchOffset+1.
	// This will give us entries with endOffset >= fetchOffset+1, which means endOffset > fetchOffset.
	//
	// IMPORTANT: Oxia uses hierarchical key sorting that groups keys by path depth.
	// Both startKey and endKey must have the same number of '/' segments for the
	// range query to work correctly. The OffsetIndex keys have format:
	// .../offset-index/<offsetEndZ>/<cumulativeSizeZ>
	startKey, err := keys.OffsetIndexStartKey(streamID, fetchOffset+1)
	if err != nil {
		return nil, err
	}

	// Use OffsetIndexEndKey which has the same segment count as startKey
	endKey := keys.OffsetIndexEndKey(streamID)

	// Query for just one entry - the first one found will be the smallest
	// endOffset > fetchOffset by the key ordering.
	kvs, err := sm.store.List(ctx, startKey, endKey, 1)
	if err != nil {
		return nil, err
	}

	if len(kvs) == 0 {
		// No entry found with endOffset > fetchOffset.
		// This could happen if the offset is valid but there's a gap in the index.
		// Return not found but not beyond HWM (since we checked that above).
		return &LookupResult{
			Found: false,
			HWM:   hwm,
		}, nil
	}

	// Parse the entry
	var entry IndexEntry
	if err := json.Unmarshal(kvs[0].Value, &entry); err != nil {
		return nil, err
	}

	// Verify the entry actually contains our offset
	// The entry covers [startOffset, endOffset), so we need startOffset <= fetchOffset < endOffset
	if fetchOffset >= entry.StartOffset && fetchOffset < entry.EndOffset {
		return &LookupResult{
			Entry: &entry,
			Found: true,
			HWM:   hwm,
		}, nil
	}

	// The entry doesn't contain our offset (gap in the index or corruption)
	return &LookupResult{
		Found: false,
		HWM:   hwm,
	}, nil
}

// LookupOffsetWithBounds finds the index entry for the requested offset
// and also returns the valid offset range for this stream.
// This is useful for ListOffsets (EARLIEST/LATEST) responses.
func (sm *StreamManager) LookupOffsetWithBounds(ctx context.Context, streamID string, fetchOffset int64) (*LookupResult, int64, error) {
	// Verify the stream exists first
	_, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		return nil, 0, err
	}

	// Get the earliest available offset by listing the first entry
	// Use full-depth keys for Oxia hierarchical key compatibility
	startKey, err := keys.OffsetIndexStartKey(streamID, 0)
	if err != nil {
		return nil, 0, err
	}
	endKey := keys.OffsetIndexEndKey(streamID)
	kvs, err := sm.store.List(ctx, startKey, endKey, 1)
	if err != nil {
		return nil, 0, err
	}

	var earliestOffset int64 = 0
	if len(kvs) > 0 {
		var firstEntry IndexEntry
		if err := json.Unmarshal(kvs[0].Value, &firstEntry); err != nil {
			return nil, 0, err
		}
		earliestOffset = firstEntry.StartOffset
	}

	// Now do the lookup
	result, err := sm.LookupOffset(ctx, streamID, fetchOffset)
	if err != nil {
		return nil, 0, err
	}

	return result, earliestOffset, nil
}

// TimestampLookupResult contains the result of a timestamp-based lookup.
type TimestampLookupResult struct {
	// Offset is the first offset with timestamp >= requested timestamp.
	// -1 if no matching offset was found.
	Offset int64
	// Timestamp is the timestamp of the record at Offset.
	// -1 if no matching offset was found.
	Timestamp int64
	// Found is true if a matching offset was found.
	Found bool
}

// TimestampScanner scans storage entries to locate the first offset with
// timestamp >= the requested timestamp when batch index data is unavailable.
type TimestampScanner interface {
	ScanOffsetByTimestamp(ctx context.Context, entry *IndexEntry, timestamp int64) (int64, int64, bool, error)
}

// LookupOffsetByTimestamp finds the first offset whose record timestamp >= the requested timestamp.
// It uses a binary search over index entries using their min/max timestamps.
//
// Per SPEC section 10.4:
//   - Use Parquet file stats / index entry min/max timestamps if available
//   - Else fallback to WAL batchIndex min/max timestamps
//
// This implementation avoids full index scans by using bounded range queries.
// For monotonic timestamps (the common case), it performs O(log N) List calls.
// For non-monotonic timestamps, it uses paginated iteration.
//
// Returns:
//   - TimestampLookupResult with Found=true and Offset/Timestamp populated if found
//   - TimestampLookupResult with Found=false if no record >= timestamp exists
//   - Error if the stream doesn't exist or on other failures
func (sm *StreamManager) LookupOffsetByTimestamp(ctx context.Context, streamID string, timestamp int64) (*TimestampLookupResult, error) {
	// First verify stream exists and get HWM
	hwm, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		return nil, err
	}

	// If stream is empty, return not found
	if hwm == 0 {
		return &TimestampLookupResult{
			Offset:    -1,
			Timestamp: -1,
			Found:     false,
		}, nil
	}

	endKey := keys.OffsetIndexEndKey(streamID)

	// Quick path: Load first entry to check if timestamp is before all entries
	startKey, err := keys.OffsetIndexStartKey(streamID, 0)
	if err != nil {
		return nil, err
	}
	kvs, err := sm.store.List(ctx, startKey, endKey, 1)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return &TimestampLookupResult{
			Offset:    -1,
			Timestamp: -1,
			Found:     false,
		}, nil
	}

	var firstEntry IndexEntry
	if err := json.Unmarshal(kvs[0].Value, &firstEntry); err != nil {
		return nil, err
	}

	// If timestamp <= first entry's max, first entry is our candidate
	if firstEntry.MaxTimestampMs >= timestamp {
		return sm.resolveTimestampEntry(ctx, &firstEntry, timestamp)
	}

	// Quick path: Load last entry to check if timestamp is after all entries
	// Use HWM as upper bound to get the last entry
	lastStartKey, err := keys.OffsetIndexStartKey(streamID, hwm)
	if err != nil {
		return nil, err
	}
	lastKvs, err := sm.store.List(ctx, lastStartKey, endKey, 1)
	if err != nil {
		return nil, err
	}
	if len(lastKvs) == 0 {
		// No entry at HWM, try loading from near the end
		// This shouldn't happen with a valid HWM but handle gracefully
		return sm.lookupTimestampPaginated(ctx, streamID, timestamp)
	}

	var lastEntry IndexEntry
	if err := json.Unmarshal(lastKvs[0].Value, &lastEntry); err != nil {
		return nil, err
	}

	// Check for monotonic timestamps - if last.Max >= first.Max, likely monotonic
	// For monotonic case, use binary search with bounded loading
	if lastEntry.MaxTimestampMs >= firstEntry.MaxTimestampMs {
		// If timestamp > last entry's max in monotonic case, no matching offset exists
		if lastEntry.MaxTimestampMs < timestamp {
			return &TimestampLookupResult{
				Offset:    -1,
				Timestamp: -1,
				Found:     false,
			}, nil
		}
		return sm.lookupTimestampBinarySearch(ctx, streamID, timestamp, firstEntry.EndOffset, lastEntry.EndOffset)
	}

	// Non-monotonic timestamps: use paginated iteration
	// Note: For non-monotonic case, we cannot short-circuit based on last entry's max
	// since earlier entries might have higher max timestamps
	return sm.lookupTimestampPaginated(ctx, streamID, timestamp)
}

// lookupTimestampBinarySearch finds the entry containing the timestamp using binary search.
// It uses offset bounds to narrow down the search, loading O(log N) entries.
func (sm *StreamManager) lookupTimestampBinarySearch(ctx context.Context, streamID string, timestamp int64, loOffset, hiOffset int64) (*TimestampLookupResult, error) {
	endKey := keys.OffsetIndexEndKey(streamID)

	for loOffset < hiOffset {
		midOffset := (loOffset + hiOffset) / 2

		// Load entry at or after midOffset
		startKey, err := keys.OffsetIndexStartKey(streamID, midOffset)
		if err != nil {
			return nil, err
		}
		kvs, err := sm.store.List(ctx, startKey, endKey, 1)
		if err != nil {
			return nil, err
		}
		if len(kvs) == 0 {
			// No entries found, narrow to lower half
			hiOffset = midOffset
			continue
		}

		var entry IndexEntry
		if err := json.Unmarshal(kvs[0].Value, &entry); err != nil {
			return nil, err
		}

		if entry.MaxTimestampMs >= timestamp {
			// This entry or an earlier one contains the timestamp
			if entry.StartOffset == loOffset || entry.EndOffset <= loOffset+1 {
				// This is the first entry in range, it's our candidate
				return sm.resolveTimestampEntry(ctx, &entry, timestamp)
			}
			// Could be an earlier entry, narrow search
			hiOffset = entry.EndOffset
		} else {
			// Timestamp is after this entry, search in later entries
			loOffset = entry.EndOffset
		}
	}

	// Load the entry at loOffset to get the final candidate
	startKey, err := keys.OffsetIndexStartKey(streamID, loOffset)
	if err != nil {
		return nil, err
	}
	kvs, err := sm.store.List(ctx, startKey, endKey, 1)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return &TimestampLookupResult{
			Offset:    -1,
			Timestamp: -1,
			Found:     false,
		}, nil
	}

	var entry IndexEntry
	if err := json.Unmarshal(kvs[0].Value, &entry); err != nil {
		return nil, err
	}

	if entry.MaxTimestampMs >= timestamp {
		return sm.resolveTimestampEntry(ctx, &entry, timestamp)
	}

	return &TimestampLookupResult{
		Offset:    -1,
		Timestamp: -1,
		Found:     false,
	}, nil
}

// lookupTimestampPaginated handles non-monotonic timestamp cases with paginated iteration.
// It loads entries in batches to avoid loading everything at once.
const timestampLookupPageSize = 100

func (sm *StreamManager) lookupTimestampPaginated(ctx context.Context, streamID string, timestamp int64) (*TimestampLookupResult, error) {
	endKey := keys.OffsetIndexEndKey(streamID)
	var lastEndOffset int64 = 0

	for {
		startKey, err := keys.OffsetIndexStartKey(streamID, lastEndOffset)
		if err != nil {
			return nil, err
		}
		kvs, err := sm.store.List(ctx, startKey, endKey, timestampLookupPageSize)
		if err != nil {
			return nil, err
		}
		if len(kvs) == 0 {
			break
		}

		for _, kv := range kvs {
			var entry IndexEntry
			if err := json.Unmarshal(kv.Value, &entry); err != nil {
				return nil, err
			}

			if entry.MaxTimestampMs >= timestamp {
				return sm.resolveTimestampEntry(ctx, &entry, timestamp)
			}
			lastEndOffset = entry.EndOffset
		}

		if len(kvs) < timestampLookupPageSize {
			break
		}
	}

	return &TimestampLookupResult{
		Offset:    -1,
		Timestamp: -1,
		Found:     false,
	}, nil
}

// resolveTimestampEntry finds the exact offset within an entry that matches the timestamp.
func (sm *StreamManager) resolveTimestampEntry(ctx context.Context, entry *IndexEntry, timestamp int64) (*TimestampLookupResult, error) {
	// If the entry's MinTimestamp is already >= requested, return start offset
	if entry.MinTimestampMs >= timestamp {
		return &TimestampLookupResult{
			Offset:    entry.StartOffset,
			Timestamp: entry.MinTimestampMs,
			Found:     true,
		}, nil
	}

	// The timestamp is within this entry's range. Try to narrow down using batchIndex.
	if len(entry.BatchIndex) > 0 {
		if batch, ok := findBatchByTimestamp(entry.BatchIndex, timestamp); ok {
			offset := entry.StartOffset + int64(batch.BatchStartOffsetDelta)
			ts := batch.MinTimestampMs
			if ts < timestamp {
				ts = batch.MaxTimestampMs
			}
			return &TimestampLookupResult{
				Offset:    offset,
				Timestamp: ts,
				Found:     true,
			}, nil
		}
	}

	if sm.timestampScanner != nil {
		offset, ts, found, err := sm.timestampScanner.ScanOffsetByTimestamp(ctx, entry, timestamp)
		if err != nil {
			return nil, err
		}
		if found {
			return &TimestampLookupResult{
				Offset:    offset,
				Timestamp: ts,
				Found:     true,
			}, nil
		}
	}

	// Fallback: return start of entry if no batchIndex or scanner available
	return &TimestampLookupResult{
		Offset:    entry.StartOffset,
		Timestamp: entry.MinTimestampMs,
		Found:     true,
	}, nil
}

func findEntryByTimestamp(entries []IndexEntry, timestamp int64) int {
	if len(entries) == 0 {
		return -1
	}

	monotonic := true
	lastMax := entries[0].MaxTimestampMs
	for i := 1; i < len(entries); i++ {
		if entries[i].MaxTimestampMs < lastMax {
			monotonic = false
			break
		}
		lastMax = entries[i].MaxTimestampMs
	}

	if !monotonic {
		for i := range entries {
			if entries[i].MaxTimestampMs >= timestamp {
				return i
			}
		}
		return -1
	}

	lo, hi := 0, len(entries)-1
	candidateIdx := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if entries[mid].MaxTimestampMs >= timestamp {
			candidateIdx = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	return candidateIdx
}

func findBatchByTimestamp(batchIndex []BatchIndexEntry, timestamp int64) (BatchIndexEntry, bool) {
	if len(batchIndex) == 0 {
		return BatchIndexEntry{}, false
	}

	monotonic := true
	lastMax := batchIndex[0].MaxTimestampMs
	for i := 1; i < len(batchIndex); i++ {
		if batchIndex[i].MaxTimestampMs < lastMax {
			monotonic = false
			break
		}
		lastMax = batchIndex[i].MaxTimestampMs
	}

	if !monotonic {
		for _, batch := range batchIndex {
			if batch.MaxTimestampMs >= timestamp {
				return batch, true
			}
		}
		return BatchIndexEntry{}, false
	}

	lo, hi := 0, len(batchIndex)-1
	candidateIdx := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if batchIndex[mid].MaxTimestampMs >= timestamp {
			candidateIdx = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	if candidateIdx == -1 {
		return BatchIndexEntry{}, false
	}

	return batchIndex[candidateIdx], true
}

// GetEarliestOffset returns the earliest available offset for a stream.
// This is typically 0 unless retention has deleted earlier offsets.
func (sm *StreamManager) GetEarliestOffset(ctx context.Context, streamID string) (int64, error) {
	// Verify stream exists first
	_, _, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		return 0, err
	}

	// Get the first index entry
	// Use full-depth keys for Oxia hierarchical key compatibility
	startKey, err := keys.OffsetIndexStartKey(streamID, 0)
	if err != nil {
		return 0, err
	}
	endKey := keys.OffsetIndexEndKey(streamID)
	kvs, err := sm.store.List(ctx, startKey, endKey, 1)
	if err != nil {
		return 0, err
	}

	if len(kvs) == 0 {
		return 0, nil // Empty stream, earliest is 0
	}

	var firstEntry IndexEntry
	if err := json.Unmarshal(kvs[0].Value, &firstEntry); err != nil {
		return 0, err
	}

	return firstEntry.StartOffset, nil
}
