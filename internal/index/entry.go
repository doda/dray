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
	ParquetID          string `json:"parquetId,omitempty"`
	ParquetPath        string `json:"parquetPath,omitempty"`
	ParquetSizeBytes   uint64 `json:"parquetSizeBytes,omitempty"`
	IcebergDataFileID  string `json:"icebergDataFileId,omitempty"`
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

		// List the last index entry for this stream to get previous cumulative size
		prefix := keys.OffsetIndexPrefix(req.StreamID)
		entries, err := sm.store.List(ctx, prefix, "", 0)
		if err != nil {
			return err
		}

		if len(entries) > 0 {
			// Find the entry with the highest offset (last in lexicographic order)
			lastEntry := entries[len(entries)-1]
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
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := sm.store.List(ctx, prefix, "", limit)
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
