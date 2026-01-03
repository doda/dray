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

	// ErrDuplicateStreamID is returned when multiple chunks share the same StreamID in a single WAL.
	ErrDuplicateStreamID = errors.New("wal: duplicate stream ID in WAL")

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

	// ByteOffset is the byte offset of this chunk's body within the WAL object.
	ByteOffset uint64

	// ByteLength is the length in bytes of this chunk's body.
	ByteLength uint32
}

// PathFormatter generates object storage paths for WAL objects.
type PathFormatter interface {
	// FormatPath returns the object key for a WAL with the given parameters.
	FormatPath(metaDomain uint32, walID uuid.UUID, createdAt time.Time, zoneID string) string
}

// DefaultPathFormatter implements PathFormatter with a standard path format.
type DefaultPathFormatter struct {
	// Prefix is an optional prefix for all WAL paths.
	Prefix string
}

// FormatPath returns a path in the format:
// {prefix}wal/v1/zone={zoneId}/domain={metaDomain}/date=YYYY/MM/DD/{walId}.wo
func (f *DefaultPathFormatter) FormatPath(metaDomain uint32, walID uuid.UUID, createdAt time.Time, zoneID string) string {
	zoneID = normalizeZoneID(zoneID)
	createdAt = createdAt.UTC()
	path := fmt.Sprintf(
		"wal/v1/zone=%s/domain=%d/date=%04d/%02d/%02d/%s.wo",
		zoneID,
		metaDomain,
		createdAt.Year(),
		createdAt.Month(),
		createdAt.Day(),
		walID.String(),
	)
	if f.Prefix == "" {
		return path
	}
	return fmt.Sprintf("%s/%s", f.Prefix, path)
}

// WriterConfig configures the WAL writer.
type WriterConfig struct {
	// PathFormatter generates object storage paths. If nil, DefaultPathFormatter is used.
	PathFormatter PathFormatter
	// ZoneID is the broker zone identifier to include in WAL paths.
	ZoneID string
}

// Writer batches multi-stream entries and writes WAL objects to storage.
// All chunks added to a Writer must belong to the same MetaDomain.
type Writer struct {
	store         objectstore.Store
	pathFormatter PathFormatter
	zoneID        string
	metaDomain    *uint32 // nil until first chunk is added
	chunks        []Chunk
	closed        bool
}

// NewWriter creates a new WAL writer that writes to the given object store.
func NewWriter(store objectstore.Store, cfg *WriterConfig) *Writer {
	var pf PathFormatter = &DefaultPathFormatter{}
	zoneID := ""
	if cfg != nil && cfg.PathFormatter != nil {
		pf = cfg.PathFormatter
	}
	if cfg != nil {
		zoneID = cfg.ZoneID
	}
	return &Writer{
		store:         store,
		pathFormatter: pf,
		zoneID:        normalizeZoneID(zoneID),
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
	now := time.Now().UTC()
	createdAt := now.UnixMilli()
	metaDomain := *w.metaDomain

	wal := NewWAL(walID, metaDomain, createdAt)
	for _, chunk := range w.chunks {
		wal.AddChunk(chunk)
	}

	data, err := EncodeToBytes(wal)
	if err != nil {
		return nil, fmt.Errorf("wal: encoding failed: %w", err)
	}

	path := w.pathFormatter.FormatPath(metaDomain, walID, now, w.zoneID)

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

	layouts, err := CalculateChunkLayouts(wal)
	if err != nil {
		return nil, fmt.Errorf("wal: chunk layout failed: %w", err)
	}
	layoutByStream := make(map[uint64]ChunkLayout, len(layouts))
	for _, layout := range layouts {
		layoutByStream[layout.StreamID] = layout
	}

	for i, chunk := range w.chunks {
		layout := layoutByStream[chunk.StreamID]
		result.ChunkOffsets[i] = ChunkOffset{
			StreamID:       chunk.StreamID,
			RecordCount:    chunk.RecordCount,
			BatchCount:     uint32(len(chunk.Batches)),
			MinTimestampMs: chunk.MinTimestampMs,
			MaxTimestampMs: chunk.MaxTimestampMs,
			ByteOffset:     layout.ByteOffset,
			ByteLength:     layout.ByteLength,
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

func normalizeZoneID(zoneID string) string {
	if zoneID == "" {
		return "unknown"
	}
	return zoneID
}

// DefaultMultipartThreshold is the size threshold above which WALs use multipart upload.
// Default is 5MB which is also the minimum part size for S3.
const DefaultMultipartThreshold = 5 * 1024 * 1024

// DefaultPartSize is the target size for each multipart upload part.
// Default is 5MB which is the minimum part size for S3.
const DefaultPartSize = 5 * 1024 * 1024

// StreamingWriterConfig configures the streaming WAL writer.
type StreamingWriterConfig struct {
	// PathFormatter generates object storage paths. If nil, DefaultPathFormatter is used.
	PathFormatter PathFormatter
	// ZoneID is the broker zone identifier to include in WAL paths.
	ZoneID string
	// MultipartThreshold is the WAL size above which multipart upload is used.
	// Default is 5MB.
	MultipartThreshold int64
	// PartSize is the target size for each multipart upload part.
	// Default is 5MB.
	PartSize int64
}

// StreamingWriter batches multi-stream entries and writes WAL objects to storage
// using streaming/multipart uploads to avoid buffering entire files in memory.
// For small WALs (below MultipartThreshold), it uses regular Put.
// For large WALs, it uses multipart upload.
type StreamingWriter struct {
	store              objectstore.Store
	multipartStore     objectstore.MultipartStore
	pathFormatter      PathFormatter
	zoneID             string
	metaDomain         *uint32
	chunks             []Chunk
	closed             bool
	multipartThreshold int64
	partSize           int64
}

// NewStreamingWriter creates a new streaming WAL writer.
// If the store implements MultipartStore, multipart uploads are enabled for large WALs.
func NewStreamingWriter(store objectstore.Store, cfg *StreamingWriterConfig) *StreamingWriter {
	var pf PathFormatter = &DefaultPathFormatter{}
	zoneID := ""
	multipartThreshold := int64(DefaultMultipartThreshold)
	partSize := int64(DefaultPartSize)

	if cfg != nil {
		if cfg.PathFormatter != nil {
			pf = cfg.PathFormatter
		}
		zoneID = cfg.ZoneID
		if cfg.MultipartThreshold > 0 {
			multipartThreshold = cfg.MultipartThreshold
		}
		if cfg.PartSize > 0 {
			partSize = cfg.PartSize
		}
	}

	var multipartStore objectstore.MultipartStore
	if ms, ok := store.(objectstore.MultipartStore); ok {
		multipartStore = ms
	}

	return &StreamingWriter{
		store:              store,
		multipartStore:     multipartStore,
		pathFormatter:      pf,
		zoneID:             normalizeZoneID(zoneID),
		chunks:             make([]Chunk, 0),
		multipartThreshold: multipartThreshold,
		partSize:           partSize,
	}
}

// AddChunk adds a chunk to be written in the next WAL object.
func (w *StreamingWriter) AddChunk(chunk Chunk, metaDomain uint32) error {
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
func (w *StreamingWriter) ChunkCount() int {
	return len(w.chunks)
}

// MetaDomain returns the MetaDomain for this writer, or nil if no chunks have been added.
func (w *StreamingWriter) MetaDomain() *uint32 {
	return w.metaDomain
}

// Flush writes all buffered chunks to object storage as a single WAL object.
// For WALs larger than MultipartThreshold, it uses streaming multipart upload.
func (w *StreamingWriter) Flush(ctx context.Context) (*WriteResult, error) {
	if w.closed {
		return nil, ErrWriterClosed
	}

	if len(w.chunks) == 0 {
		return nil, ErrEmptyWAL
	}

	walID := uuid.New()
	now := time.Now().UTC()
	createdAt := now.UnixMilli()
	metaDomain := *w.metaDomain

	wal := NewWAL(walID, metaDomain, createdAt)
	for _, chunk := range w.chunks {
		wal.AddChunk(chunk)
	}

	// Calculate expected size to determine if we should use multipart
	expectedSize, err := CalculateEncodedSize(wal)
	if err != nil {
		return nil, fmt.Errorf("wal: size calculation failed: %w", err)
	}

	path := w.pathFormatter.FormatPath(metaDomain, walID, now, w.zoneID)

	var size int64
	if w.multipartStore != nil && int64(expectedSize) >= w.multipartThreshold {
		size, err = w.flushMultipart(ctx, wal, path)
	} else {
		size, err = w.flushSimple(ctx, wal, path)
	}

	if err != nil {
		return nil, err
	}

	result := &WriteResult{
		WalID:           walID,
		Path:            path,
		MetaDomain:      metaDomain,
		CreatedAtUnixMs: createdAt,
		Size:            size,
		ChunkOffsets:    make([]ChunkOffset, len(w.chunks)),
	}

	layouts, err := CalculateChunkLayouts(wal)
	if err != nil {
		return nil, fmt.Errorf("wal: chunk layout failed: %w", err)
	}
	layoutByStream := make(map[uint64]ChunkLayout, len(layouts))
	for _, layout := range layouts {
		layoutByStream[layout.StreamID] = layout
	}

	for i, chunk := range w.chunks {
		layout := layoutByStream[chunk.StreamID]
		result.ChunkOffsets[i] = ChunkOffset{
			StreamID:       chunk.StreamID,
			RecordCount:    chunk.RecordCount,
			BatchCount:     uint32(len(chunk.Batches)),
			MinTimestampMs: chunk.MinTimestampMs,
			MaxTimestampMs: chunk.MaxTimestampMs,
			ByteOffset:     layout.ByteOffset,
			ByteLength:     layout.ByteLength,
		}
	}

	w.Reset()
	return result, nil
}

// flushSimple writes WAL using a single Put operation (for small WALs).
func (w *StreamingWriter) flushSimple(ctx context.Context, wal *WAL, path string) (int64, error) {
	data, err := EncodeToBytes(wal)
	if err != nil {
		return 0, fmt.Errorf("wal: encoding failed: %w", err)
	}

	err = w.store.Put(ctx, path, bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return 0, fmt.Errorf("wal: write to object store failed: %w", err)
	}

	return int64(len(data)), nil
}

// flushMultipart writes WAL using streaming multipart upload (for large WALs).
func (w *StreamingWriter) flushMultipart(ctx context.Context, wal *WAL, path string) (int64, error) {
	upload, err := w.multipartStore.CreateMultipartUpload(ctx, path, "application/octet-stream")
	if err != nil {
		return 0, fmt.Errorf("wal: create multipart upload failed: %w", err)
	}

	// Use a partWriter that buffers and uploads parts
	pw := newPartWriter(ctx, upload, w.partSize)

	encoder := NewStreamingEncoder(pw)
	bytesWritten, err := encoder.Encode(wal)
	if err != nil {
		if abortErr := upload.Abort(ctx); abortErr != nil {
			return 0, fmt.Errorf("wal: encoding failed: %w (abort failed: %v)", err, abortErr)
		}
		return 0, fmt.Errorf("wal: encoding failed: %w", err)
	}

	// Flush any remaining buffered data
	if err := pw.Flush(); err != nil {
		if abortErr := upload.Abort(ctx); abortErr != nil {
			return 0, fmt.Errorf("wal: flush failed: %w (abort failed: %v)", err, abortErr)
		}
		return 0, fmt.Errorf("wal: flush failed: %w", err)
	}

	// Complete the multipart upload
	if err := upload.Complete(ctx, pw.ETags()); err != nil {
		return 0, fmt.Errorf("wal: complete multipart upload failed: %w", err)
	}

	return bytesWritten, nil
}

// Reset clears all buffered chunks and resets the MetaDomain.
func (w *StreamingWriter) Reset() {
	w.chunks = make([]Chunk, 0)
	w.metaDomain = nil
}

// Close closes the writer and releases resources.
func (w *StreamingWriter) Close() error {
	w.closed = true
	w.chunks = nil
	return nil
}

// partWriter buffers writes and uploads parts when buffer reaches partSize.
type partWriter struct {
	ctx      context.Context
	upload   objectstore.MultipartUpload
	partSize int64
	buffer   *bytes.Buffer
	partNum  int
	etags    []string
	err      error
}

func newPartWriter(ctx context.Context, upload objectstore.MultipartUpload, partSize int64) *partWriter {
	return &partWriter{
		ctx:      ctx,
		upload:   upload,
		partSize: partSize,
		buffer:   bytes.NewBuffer(make([]byte, 0, partSize)),
	}
}

func (pw *partWriter) Write(p []byte) (int, error) {
	if pw.err != nil {
		return 0, pw.err
	}

	written := 0
	for len(p) > 0 {
		// How much can we write to buffer before it's full?
		remaining := pw.partSize - int64(pw.buffer.Len())
		if int64(len(p)) <= remaining {
			// Just buffer it
			n, _ := pw.buffer.Write(p)
			written += n
			break
		}

		// Write what we can to fill the buffer
		n, _ := pw.buffer.Write(p[:remaining])
		written += n
		p = p[remaining:]

		// Upload the full part
		if err := pw.uploadPart(); err != nil {
			pw.err = err
			return written, err
		}
	}

	return written, nil
}

func (pw *partWriter) uploadPart() error {
	if pw.buffer.Len() == 0 {
		return nil
	}

	pw.partNum++
	data := pw.buffer.Bytes()
	etag, err := pw.upload.UploadPart(pw.ctx, pw.partNum, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	pw.etags = append(pw.etags, etag)
	pw.buffer.Reset()
	return nil
}

// Flush uploads any remaining buffered data.
func (pw *partWriter) Flush() error {
	if pw.err != nil {
		return pw.err
	}
	return pw.uploadPart()
}

// ETags returns the ETags of all uploaded parts.
func (pw *partWriter) ETags() []string {
	return pw.etags
}
