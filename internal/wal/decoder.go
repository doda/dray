package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/google/uuid"
)

var (
	ErrInvalidMagic       = errors.New("invalid magic bytes")
	ErrUnsupportedVersion = errors.New("unsupported WAL version")
	ErrInvalidCRC         = errors.New("CRC32C checksum mismatch")
	ErrTruncatedHeader    = errors.New("truncated WAL header")
	ErrTruncatedIndex     = errors.New("truncated chunk index")
	ErrTruncatedChunk     = errors.New("truncated chunk body")
	ErrTruncatedFooter    = errors.New("truncated CRC32C footer")
	ErrInvalidOffset      = errors.New("invalid chunk offset")
)

// Decoder reads WAL objects from bytes or an io.Reader.
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a new WAL decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decode reads and parses a complete WAL from the underlying reader.
func (d *Decoder) Decode() (*WAL, error) {
	data, err := io.ReadAll(d.r)
	if err != nil {
		return nil, fmt.Errorf("reading WAL data: %w", err)
	}
	return DecodeFromBytes(data)
}

// DecodeFromBytes parses a WAL from a byte slice.
func DecodeFromBytes(data []byte) (*WAL, error) {
	if len(data) < HeaderSize {
		return nil, ErrTruncatedHeader
	}

	// Parse and validate header
	header, err := parseHeader(data[:HeaderSize])
	if err != nil {
		return nil, err
	}

	// Check if footer is present and validate CRC if so
	hasFooter := hasFooter(data, header)
	if hasFooter {
		if err := validateCRC(data); err != nil {
			return nil, err
		}
	}

	// Parse chunk index
	indexEntries, err := parseChunkIndex(data, header, hasFooter)
	if err != nil {
		return nil, err
	}

	// Parse chunk bodies
	chunks, err := parseChunkBodies(data, indexEntries, header.ChunkIndexOffset)
	if err != nil {
		return nil, err
	}

	return &WAL{
		WalID:           header.WalID,
		MetaDomain:      header.MetaDomain,
		CreatedAtUnixMs: header.CreatedAtUnixMs,
		Chunks:          chunks,
	}, nil
}

// parseHeader parses the 49-byte WAL header.
func parseHeader(data []byte) (*Header, error) {
	if len(data) < HeaderSize {
		return nil, ErrTruncatedHeader
	}

	header := &Header{}
	offset := 0

	// Magic (7 bytes)
	copy(header.Magic[:], data[offset:offset+7])
	if string(header.Magic[:]) != MagicBytes {
		return nil, fmt.Errorf("%w: got %q, want %q", ErrInvalidMagic, string(header.Magic[:]), MagicBytes)
	}
	offset += 7

	// Version (2 bytes, big-endian)
	header.Version = binary.BigEndian.Uint16(data[offset : offset+2])
	if header.Version != Version {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, header.Version, Version)
	}
	offset += 2

	// WalID (16 bytes UUID)
	copy(header.WalID[:], data[offset:offset+16])
	offset += 16

	// MetaDomain (4 bytes)
	header.MetaDomain = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// CreatedAtUnixMs (8 bytes)
	header.CreatedAtUnixMs = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// ChunkCount (4 bytes)
	header.ChunkCount = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// ChunkIndexOffset (8 bytes)
	header.ChunkIndexOffset = binary.BigEndian.Uint64(data[offset : offset+8])

	return header, nil
}

// hasFooter determines if a CRC32C footer is present in the WAL data.
// The footer is optional in WAL v1. We detect its presence by checking if
// the data length matches the expected size with a footer.
func hasFooter(data []byte, header *Header) bool {
	indexSize := uint64(header.ChunkCount) * uint64(ChunkIndexEntrySize)
	expectedWithFooter := header.ChunkIndexOffset + indexSize + FooterSize
	return uint64(len(data)) == expectedWithFooter
}

// validateCRC checks the CRC32C footer against the computed checksum.
func validateCRC(data []byte) error {
	if len(data) < FooterSize {
		return ErrTruncatedFooter
	}

	crcOffset := len(data) - FooterSize
	storedCRC := binary.BigEndian.Uint32(data[crcOffset:])
	computedCRC := crc32.Checksum(data[:crcOffset], crc32cTable)

	if storedCRC != computedCRC {
		return fmt.Errorf("%w: stored %08x, computed %08x", ErrInvalidCRC, storedCRC, computedCRC)
	}

	return nil
}

// parseChunkIndex parses all chunk index entries from the WAL.
func parseChunkIndex(data []byte, header *Header, footerPresent bool) ([]ChunkIndexEntry, error) {
	if header.ChunkCount == 0 {
		return nil, nil
	}

	if header.ChunkIndexOffset < uint64(HeaderSize) {
		return nil, fmt.Errorf("%w: chunk index offset %d before header end %d", ErrInvalidOffset, header.ChunkIndexOffset, HeaderSize)
	}

	dataLen := uint64(len(data))
	indexSize := uint64(header.ChunkCount) * uint64(ChunkIndexEntrySize)
	if header.ChunkIndexOffset+indexSize < header.ChunkIndexOffset {
		return nil, ErrTruncatedIndex
	}
	indexEnd := header.ChunkIndexOffset + indexSize

	// Validate index bounds
	var maxIndexEnd uint64
	if footerPresent {
		maxIndexEnd = dataLen - uint64(FooterSize)
	} else {
		maxIndexEnd = dataLen
	}
	if indexEnd > maxIndexEnd {
		return nil, ErrTruncatedIndex
	}

	indexStart := int(header.ChunkIndexOffset)
	entries := make([]ChunkIndexEntry, header.ChunkCount)
	for i := uint32(0); i < header.ChunkCount; i++ {
		entryStart := indexStart + int(i)*ChunkIndexEntrySize
		entry, err := parseChunkIndexEntry(data[entryStart : entryStart+ChunkIndexEntrySize])
		if err != nil {
			return nil, fmt.Errorf("parsing chunk index entry %d: %w", i, err)
		}
		entries[i] = *entry
	}

	return entries, nil
}

// parseChunkIndexEntry parses a single 44-byte chunk index entry.
func parseChunkIndexEntry(data []byte) (*ChunkIndexEntry, error) {
	if len(data) < ChunkIndexEntrySize {
		return nil, ErrTruncatedIndex
	}

	entry := &ChunkIndexEntry{}
	offset := 0

	// StreamID (8 bytes)
	entry.StreamID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// ChunkOffset (8 bytes)
	entry.ChunkOffset = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// ChunkLength (4 bytes)
	entry.ChunkLength = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// RecordCount (4 bytes)
	entry.RecordCount = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// BatchCount (4 bytes)
	entry.BatchCount = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// MinTimestampMs (8 bytes)
	entry.MinTimestampMs = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// MaxTimestampMs (8 bytes)
	entry.MaxTimestampMs = int64(binary.BigEndian.Uint64(data[offset : offset+8]))

	return entry, nil
}

// parseChunkBodies parses chunk bodies using the index entries.
func parseChunkBodies(data []byte, entries []ChunkIndexEntry, chunkIndexOffset uint64) ([]Chunk, error) {
	chunks := make([]Chunk, len(entries))

	for i, entry := range entries {
		chunk, err := parseChunkBody(data, entry, chunkIndexOffset)
		if err != nil {
			return nil, fmt.Errorf("parsing chunk %d (streamID=%d): %w", i, entry.StreamID, err)
		}
		chunks[i] = *chunk
	}

	return chunks, nil
}

// parseChunkBody parses a single chunk body.
func parseChunkBody(data []byte, entry ChunkIndexEntry, chunkIndexOffset uint64) (*Chunk, error) {
	chunkStart := entry.ChunkOffset
	chunkEnd := chunkStart + uint64(entry.ChunkLength)
	if chunkEnd < chunkStart {
		return nil, ErrInvalidOffset
	}
	if chunkStart < uint64(HeaderSize) {
		return nil, fmt.Errorf("%w: chunk starts at %d, before header end %d", ErrInvalidOffset, chunkStart, HeaderSize)
	}
	if chunkEnd > chunkIndexOffset {
		return nil, fmt.Errorf("%w: chunk end %d exceeds chunk index offset %d", ErrInvalidOffset, chunkEnd, chunkIndexOffset)
	}
	if chunkEnd > uint64(len(data)) {
		return nil, fmt.Errorf("%w: chunk end %d exceeds data length %d", ErrTruncatedChunk, chunkEnd, len(data))
	}

	chunkData := data[int(chunkStart):int(chunkEnd)]
	batches, err := parseBatches(chunkData, entry.BatchCount)
	if err != nil {
		return nil, err
	}

	return &Chunk{
		StreamID:       entry.StreamID,
		Batches:        batches,
		RecordCount:    entry.RecordCount,
		MinTimestampMs: entry.MinTimestampMs,
		MaxTimestampMs: entry.MaxTimestampMs,
	}, nil
}

// parseBatches parses batch entries from chunk body data.
func parseBatches(data []byte, batchCount uint32) ([]BatchEntry, error) {
	batches := make([]BatchEntry, 0, batchCount)
	offset := 0

	for i := uint32(0); i < batchCount; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("%w: batch %d length field at offset %d", ErrTruncatedChunk, i, offset)
		}

		batchLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(batchLen) > len(data) {
			return nil, fmt.Errorf("%w: batch %d data (len=%d) at offset %d", ErrTruncatedChunk, i, batchLen, offset)
		}

		batchData := make([]byte, batchLen)
		copy(batchData, data[offset:offset+int(batchLen)])
		offset += int(batchLen)

		batches = append(batches, BatchEntry{Data: batchData})
	}

	if offset != len(data) {
		return nil, fmt.Errorf("%w: chunk has %d trailing bytes", ErrInvalidOffset, len(data)-offset)
	}

	return batches, nil
}

// DecodeHeader parses only the WAL header from bytes, useful for quick metadata access.
func DecodeHeader(data []byte) (*Header, error) {
	return parseHeader(data)
}

// DecodeHeaderFromReader reads and parses only the WAL header.
func DecodeHeaderFromReader(r io.Reader) (*Header, error) {
	data := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, data); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrTruncatedHeader
		}
		return nil, fmt.Errorf("reading header: %w", err)
	}
	return parseHeader(data)
}

// ValidateCRC checks if the CRC32C footer matches the data.
// This is useful for validating WAL data before full parsing.
func ValidateCRC(data []byte) error {
	return validateCRC(data)
}

// GetWALID extracts the WAL ID from raw bytes without full parsing.
func GetWALID(data []byte) (uuid.UUID, error) {
	header, err := DecodeHeader(data)
	if err != nil {
		return uuid.UUID{}, err
	}
	return header.WalID, nil
}

// RangeReader provides byte-range read access to WAL data.
// This interface allows streaming decoding without loading the entire WAL into memory.
type RangeReader interface {
	// ReadRange reads bytes from the specified byte range [start, end] inclusive.
	// Returns the data and any error. If end is beyond the data length, returns
	// available data up to the end.
	ReadRange(start, end int64) ([]byte, error)
}

// StreamingDecoder provides memory-efficient WAL decoding by reading only the
// header and chunk index on initialization. Individual chunks can be read on demand.
type StreamingDecoder struct {
	reader  RangeReader
	header  *Header
	entries []ChunkIndexEntry
}

// NewStreamingDecoder creates a streaming decoder from a RangeReader.
// It reads only the header and chunk index (not chunk bodies) on creation.
// The memory footprint is O(HeaderSize + ChunkCount * ChunkIndexEntrySize).
func NewStreamingDecoder(reader RangeReader) (*StreamingDecoder, error) {
	// Read header
	headerData, err := reader.ReadRange(0, int64(HeaderSize-1))
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}
	if len(headerData) < HeaderSize {
		return nil, ErrTruncatedHeader
	}

	header, err := parseHeader(headerData)
	if err != nil {
		return nil, err
	}

	// Read chunk index if there are chunks
	var entries []ChunkIndexEntry
	if header.ChunkCount > 0 {
		indexSize := int64(header.ChunkCount) * int64(ChunkIndexEntrySize)
		indexStart := int64(header.ChunkIndexOffset)
		indexEnd := indexStart + indexSize - 1

		indexData, err := reader.ReadRange(indexStart, indexEnd)
		if err != nil {
			return nil, fmt.Errorf("reading chunk index: %w", err)
		}
		if int64(len(indexData)) < indexSize {
			return nil, ErrTruncatedIndex
		}

		entries = make([]ChunkIndexEntry, header.ChunkCount)
		for i := uint32(0); i < header.ChunkCount; i++ {
			entryStart := int(i) * ChunkIndexEntrySize
			entry, err := parseChunkIndexEntry(indexData[entryStart : entryStart+ChunkIndexEntrySize])
			if err != nil {
				return nil, fmt.Errorf("parsing chunk index entry %d: %w", i, err)
			}
			entries[i] = *entry
		}
	}

	return &StreamingDecoder{
		reader:  reader,
		header:  header,
		entries: entries,
	}, nil
}

// Header returns the WAL header.
func (d *StreamingDecoder) Header() *Header {
	return d.header
}

// ChunkCount returns the number of chunks in the WAL.
func (d *StreamingDecoder) ChunkCount() int {
	return len(d.entries)
}

// ChunkIndex returns the chunk index entry at the given position.
// Returns nil if the index is out of bounds.
func (d *StreamingDecoder) ChunkIndex(i int) *ChunkIndexEntry {
	if i < 0 || i >= len(d.entries) {
		return nil
	}
	entry := d.entries[i]
	return &entry
}

// ChunkEntries returns all chunk index entries.
func (d *StreamingDecoder) ChunkEntries() []ChunkIndexEntry {
	result := make([]ChunkIndexEntry, len(d.entries))
	copy(result, d.entries)
	return result
}

// ReadChunk reads and parses a single chunk by index, returning the parsed Chunk.
// This is the only method that reads chunk body data.
func (d *StreamingDecoder) ReadChunk(i int) (*Chunk, error) {
	if i < 0 || i >= len(d.entries) {
		return nil, fmt.Errorf("chunk index %d out of bounds (0-%d)", i, len(d.entries)-1)
	}

	entry := d.entries[i]
	chunkStart := int64(entry.ChunkOffset)
	chunkEnd := chunkStart + int64(entry.ChunkLength) - 1

	chunkData, err := d.reader.ReadRange(chunkStart, chunkEnd)
	if err != nil {
		return nil, fmt.Errorf("reading chunk %d data: %w", i, err)
	}
	if len(chunkData) != int(entry.ChunkLength) {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", ErrTruncatedChunk, entry.ChunkLength, len(chunkData))
	}

	batches, err := parseBatches(chunkData, entry.BatchCount)
	if err != nil {
		return nil, fmt.Errorf("parsing chunk %d batches: %w", i, err)
	}

	return &Chunk{
		StreamID:       entry.StreamID,
		Batches:        batches,
		RecordCount:    entry.RecordCount,
		MinTimestampMs: entry.MinTimestampMs,
		MaxTimestampMs: entry.MaxTimestampMs,
	}, nil
}

// ReadChunkRaw reads raw chunk body bytes without parsing batches.
// Useful when chunk data needs to be forwarded without modification.
func (d *StreamingDecoder) ReadChunkRaw(i int) ([]byte, error) {
	if i < 0 || i >= len(d.entries) {
		return nil, fmt.Errorf("chunk index %d out of bounds (0-%d)", i, len(d.entries)-1)
	}

	entry := d.entries[i]
	chunkStart := int64(entry.ChunkOffset)
	chunkEnd := chunkStart + int64(entry.ChunkLength) - 1

	chunkData, err := d.reader.ReadRange(chunkStart, chunkEnd)
	if err != nil {
		return nil, fmt.Errorf("reading chunk %d data: %w", i, err)
	}
	if len(chunkData) != int(entry.ChunkLength) {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", ErrTruncatedChunk, entry.ChunkLength, len(chunkData))
	}

	return chunkData, nil
}

// DecodeAll reads all chunks and returns a complete WAL object.
// This loads all chunk data into memory, similar to DecodeFromBytes.
func (d *StreamingDecoder) DecodeAll() (*WAL, error) {
	chunks := make([]Chunk, len(d.entries))
	for i := range d.entries {
		chunk, err := d.ReadChunk(i)
		if err != nil {
			return nil, err
		}
		chunks[i] = *chunk
	}

	return &WAL{
		WalID:           d.header.WalID,
		MetaDomain:      d.header.MetaDomain,
		CreatedAtUnixMs: d.header.CreatedAtUnixMs,
		Chunks:          chunks,
	}, nil
}

// BytesRangeReader wraps a byte slice to implement RangeReader.
type BytesRangeReader struct {
	data []byte
}

// NewBytesRangeReader creates a RangeReader from a byte slice.
func NewBytesRangeReader(data []byte) *BytesRangeReader {
	return &BytesRangeReader{data: data}
}

// ReadRange implements RangeReader for a byte slice.
func (r *BytesRangeReader) ReadRange(start, end int64) ([]byte, error) {
	if start < 0 {
		return nil, fmt.Errorf("invalid start offset %d", start)
	}
	if start >= int64(len(r.data)) {
		return nil, io.EOF
	}
	if end >= int64(len(r.data)) {
		end = int64(len(r.data)) - 1
	}
	if end < start {
		return nil, fmt.Errorf("invalid range: end %d < start %d", end, start)
	}
	result := make([]byte, end-start+1)
	copy(result, r.data[start:end+1])
	return result, nil
}
