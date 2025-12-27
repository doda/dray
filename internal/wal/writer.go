package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dray-io/dray/internal/objectstore"
	"github.com/google/uuid"
)

// Common errors returned by Writer.
var (
	// ErrMetaDomainMismatch is returned when trying to add a chunk from a different MetaDomain.
	ErrMetaDomainMismatch = errors.New("wal: metadomain mismatch - all chunks must be from the same metadomain")

	// ErrEmptyWAL is returned when trying to flush a WAL with no chunks.
	ErrEmptyWAL = errors.New("wal: cannot flush empty WAL")

	// ErrWriterClosed is returned when trying to use a closed writer.
	ErrWriterClosed = errors.New("wal: writer is closed")
)

// WriteResult contains metadata about a successfully written WAL object.
type WriteResult struct {
	// WalID is the unique identifier for this WAL object.
	WalID uuid.UUID

	// Path is the object storage key where the WAL was written.
	Path string

	// MetaDomain is the metadata domain this WAL belongs to.
	MetaDomain uint32

	// CreatedAtUnixMs is the creation timestamp in milliseconds.
	CreatedAtUnixMs int64

	// Size is the total size in bytes of the written WAL object.
	Size int64

	// ChunkOffsets maps each streamId to its assigned offset information.
	ChunkOffsets []ChunkOffset
}

// ChunkOffset describes the offset information for a chunk in the written WAL.
type ChunkOffset struct {
	// StreamID is the stream identifier.
	StreamID uint64

	// RecordCount is the number of records in this chunk.
	RecordCount uint32

	// BatchCount is the number of batches in this chunk.
	BatchCount uint32

	// MinTimestampMs is the minimum record timestamp in this chunk.
	MinTimestampMs int64

	// MaxTimestampMs is the maximum record timestamp in this chunk.
	MaxTimestampMs int64
}

// PathFormatter generates object storage paths for WAL objects.
type PathFormatter interface {
	// FormatPath returns the object key for a WAL with the given parameters.
	FormatPath(metaDomain uint32, walID uuid.UUID) string
}

// DefaultPathFormatter implements PathFormatter with a standard path format.
type DefaultPathFormatter struct {
	// Prefix is an optional prefix for all WAL paths.
	Prefix string
}

// FormatPath returns a path in the format: {prefix}wal/domain={metaDomain}/{walId}.wo
func (f *DefaultPathFormatter) FormatPath(metaDomain uint32, walID uuid.UUID) string {
	if f.Prefix == "" {
		return fmt.Sprintf("wal/domain=%d/%s.wo", metaDomain, walID.String())
	}
	return fmt.Sprintf("%s/wal/domain=%d/%s.wo", f.Prefix, metaDomain, walID.String())
}

// WriterConfig configures the WAL writer.
type WriterConfig struct {
	// PathFormatter generates object storage paths. If nil, DefaultPathFormatter is used.
	PathFormatter PathFormatter
}

// Writer batches multi-stream entries and writes WAL objects to storage.
// All chunks added to a Writer must belong to the same MetaDomain.
type Writer struct {
	store         objectstore.Store
	pathFormatter PathFormatter
	metaDomain    *uint32 // nil until first chunk is added
	chunks        []Chunk
	closed        bool
}

// NewWriter creates a new WAL writer that writes to the given object store.
func NewWriter(store objectstore.Store, cfg *WriterConfig) *Writer {
	var pf PathFormatter = &DefaultPathFormatter{}
	if cfg != nil && cfg.PathFormatter != nil {
		pf = cfg.PathFormatter
	}
	return &Writer{
		store:         store,
		pathFormatter: pf,
		chunks:        make([]Chunk, 0),
	}
}

// AddChunk adds a chunk to be written in the next WAL object.
// Returns ErrMetaDomainMismatch if the chunk's stream belongs to a different MetaDomain
// than previously added chunks. The metaDomain parameter must be consistent for all chunks.
func (w *Writer) AddChunk(chunk Chunk, metaDomain uint32) error {
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
func (w *Writer) ChunkCount() int {
	return len(w.chunks)
}

// MetaDomain returns the MetaDomain for this writer, or nil if no chunks have been added.
func (w *Writer) MetaDomain() *uint32 {
	return w.metaDomain
}

// Flush writes all buffered chunks to object storage as a single WAL object.
// Returns the write result with metadata about the written WAL.
// After a successful flush, the writer is reset and can accept new chunks.
func (w *Writer) Flush(ctx context.Context) (*WriteResult, error) {
	if w.closed {
		return nil, ErrWriterClosed
	}

	if len(w.chunks) == 0 {
		return nil, ErrEmptyWAL
	}

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

	err = w.store.Put(ctx, path, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("wal: write to object store failed: %w", err)
	}

	result := &WriteResult{
		WalID:           walID,
		Path:            path,
		MetaDomain:      metaDomain,
		CreatedAtUnixMs: createdAt,
		Size:            int64(len(data)),
		ChunkOffsets:    make([]ChunkOffset, len(w.chunks)),
	}

	for i, chunk := range w.chunks {
		result.ChunkOffsets[i] = ChunkOffset{
			StreamID:       chunk.StreamID,
			RecordCount:    chunk.RecordCount,
			BatchCount:     uint32(len(chunk.Batches)),
			MinTimestampMs: chunk.MinTimestampMs,
			MaxTimestampMs: chunk.MaxTimestampMs,
		}
	}

	w.Reset()
	return result, nil
}

// Reset clears all buffered chunks and resets the MetaDomain.
// This allows reusing the Writer for a new WAL object.
func (w *Writer) Reset() {
	w.chunks = make([]Chunk, 0)
	w.metaDomain = nil
}

// Close closes the writer and releases resources.
// Any buffered chunks that haven't been flushed are discarded.
func (w *Writer) Close() error {
	w.closed = true
	w.chunks = nil
	return nil
}
