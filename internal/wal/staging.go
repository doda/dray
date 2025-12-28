package wal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/google/uuid"
)

// StagingMarker contains metadata about a WAL object that is being written.
// It is stored at /wal/staging/<metaDomain>/<walId> before the WAL object
// is fully committed. Per spec section 9.7, orphaned WAL objects (where
// staging marker exists but no commit) can be safely garbage collected
// after wal.orphan_ttl (default 24h).
type StagingMarker struct {
	// Path is the object storage key where the WAL will be written.
	Path string `json:"path"`

	// CreatedAt is the creation timestamp in milliseconds since Unix epoch.
	CreatedAt int64 `json:"createdAt"`

	// SizeBytes is the total size of the WAL object in bytes.
	SizeBytes int64 `json:"sizeBytes"`
}

// MarshalJSON returns the JSON encoding of the staging marker.
func (m *StagingMarker) MarshalJSON() ([]byte, error) {
	type Alias StagingMarker
	return json.Marshal((*Alias)(m))
}

// UnmarshalJSON parses the JSON encoding of a staging marker.
func (m *StagingMarker) UnmarshalJSON(data []byte) error {
	type Alias StagingMarker
	return json.Unmarshal(data, (*Alias)(m))
}

// StagingWriteResult extends WriteResult with staging marker information.
type StagingWriteResult struct {
	WriteResult

	// StagingKey is the metadata key where the staging marker was written.
	// This key should be deleted atomically in the commit transaction.
	StagingKey string
}

// StagingWriterConfig configures the staging-aware WAL writer.
type StagingWriterConfig struct {
	// PathFormatter generates object storage paths. If nil, DefaultPathFormatter is used.
	PathFormatter PathFormatter

	// Metrics is an optional WAL metrics recorder. If nil, no metrics are recorded.
	Metrics *metrics.WALMetrics
}

// StagingWriter wraps Writer with staging marker support for orphan detection.
// Per spec section 9.7, it writes a staging marker before the WAL object
// is written to object storage. The staging marker is then deleted in the
// commit transaction.
type StagingWriter struct {
	store         objectstore.Store
	metaStore     metadata.MetadataStore
	pathFormatter PathFormatter
	metaDomain    *uint32
	chunks        []Chunk
	closed        bool
	metrics       *metrics.WALMetrics
}

// NewStagingWriter creates a new staging-aware WAL writer.
// The metaStore is used to write staging markers for orphan detection.
func NewStagingWriter(store objectstore.Store, metaStore metadata.MetadataStore, cfg *StagingWriterConfig) *StagingWriter {
	var pf PathFormatter = &DefaultPathFormatter{}
	var walMetrics *metrics.WALMetrics
	if cfg != nil {
		if cfg.PathFormatter != nil {
			pf = cfg.PathFormatter
		}
		walMetrics = cfg.Metrics
	}
	return &StagingWriter{
		store:         store,
		metaStore:     metaStore,
		pathFormatter: pf,
		chunks:        make([]Chunk, 0),
		metrics:       walMetrics,
	}
}

// AddChunk adds a chunk to be written in the next WAL object.
// Returns ErrMetaDomainMismatch if the chunk's stream belongs to a different MetaDomain.
func (w *StagingWriter) AddChunk(chunk Chunk, metaDomain uint32) error {
	if w.closed {
		return ErrWriterClosed
	}

	if w.metaDomain == nil {
		w.metaDomain = &metaDomain
	} else if *w.metaDomain != metaDomain {
		return ErrMetaDomainMismatch
	}

	w.chunks = append(w.chunks, chunk)
	return nil
}

// ChunkCount returns the number of chunks currently buffered.
func (w *StagingWriter) ChunkCount() int {
	return len(w.chunks)
}

// MetaDomain returns the MetaDomain for this writer, or nil if no chunks have been added.
func (w *StagingWriter) MetaDomain() *uint32 {
	return w.metaDomain
}

// Flush writes all buffered chunks to object storage as a single WAL object.
// Per spec section 9.7, the flush process is:
//  1. Generate WAL ID and encode the WAL data
//  2. Write staging marker to metadata store with path, createdAt, sizeBytes
//  3. Write WAL object to object storage
//  4. Return the staging key for deletion in commit transaction
//
// If the metadata commit transaction fails after Flush succeeds, the WAL object
// will be orphaned. The GC worker will clean up orphans by scanning staging
// markers older than wal.orphan_ttl.
func (w *StagingWriter) Flush(ctx context.Context) (*StagingWriteResult, error) {
	if w.closed {
		return nil, ErrWriterClosed
	}

	if len(w.chunks) == 0 {
		return nil, ErrEmptyWAL
	}

	flushStart := time.Now()
	walID := uuid.New()
	createdAt := time.Now().UnixMilli()
	metaDomain := *w.metaDomain

	wal := NewWAL(walID, metaDomain, createdAt)
	for _, chunk := range w.chunks {
		wal.AddChunk(chunk)
	}

	data, err := EncodeToBytes(wal)
	if err != nil {
		return nil, fmt.Errorf("wal: encoding failed: %w", err)
	}

	path := w.pathFormatter.FormatPath(metaDomain, walID)
	size := int64(len(data))

	// Write staging marker before WAL object write
	stagingKey := keys.WALStagingKeyPath(int(metaDomain), walID.String())
	stagingMarker := &StagingMarker{
		Path:      path,
		CreatedAt: createdAt,
		SizeBytes: size,
	}
	stagingData, err := json.Marshal(stagingMarker)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to marshal staging marker: %w", err)
	}

	_, err = w.metaStore.Put(ctx, stagingKey, stagingData)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to write staging marker: %w", err)
	}

	// Write WAL object to object storage
	err = w.store.Put(ctx, path, bytes.NewReader(data), size, "application/octet-stream")
	if err != nil {
		// Note: staging marker remains - it will be cleaned up by orphan GC
		return nil, fmt.Errorf("wal: write to object store failed: %w", err)
	}

	// Calculate chunk layouts to get byte offsets and lengths
	layouts := CalculateChunkLayouts(wal)

	result := &StagingWriteResult{
		WriteResult: WriteResult{
			WalID:           walID,
			Path:            path,
			MetaDomain:      metaDomain,
			CreatedAtUnixMs: createdAt,
			Size:            size,
			ChunkOffsets:    make([]ChunkOffset, len(layouts)),
		},
		StagingKey: stagingKey,
	}

	for i, layout := range layouts {
		result.ChunkOffsets[i] = ChunkOffset{
			StreamID:       layout.StreamID,
			RecordCount:    layout.RecordCount,
			BatchCount:     layout.BatchCount,
			MinTimestampMs: layout.MinTimestampMs,
			MaxTimestampMs: layout.MaxTimestampMs,
			ByteOffset:     layout.ByteOffset,
			ByteLength:     layout.ByteLength,
		}
	}

	// Record WAL metrics if configured
	if w.metrics != nil {
		flushDuration := time.Since(flushStart).Seconds()
		w.metrics.RecordFlush(size, flushDuration)
	}

	w.Reset()
	return result, nil
}

// Reset clears all buffered chunks and resets the MetaDomain.
func (w *StagingWriter) Reset() {
	w.chunks = make([]Chunk, 0)
	w.metaDomain = nil
}

// Close closes the writer and releases resources.
func (w *StagingWriter) Close() error {
	w.closed = true
	w.chunks = nil
	return nil
}

// ParseStagingMarker parses a staging marker from JSON bytes.
func ParseStagingMarker(data []byte) (*StagingMarker, error) {
	var marker StagingMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, fmt.Errorf("wal: failed to parse staging marker: %w", err)
	}
	return &marker, nil
}

// DeleteStagingKey deletes a staging marker from the metadata store.
// This should be called as part of the commit transaction.
func DeleteStagingKey(txn metadata.Txn, stagingKey string) {
	txn.Delete(stagingKey)
}
