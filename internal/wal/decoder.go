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
	if len(data) < HeaderSize+FooterSize {
		return nil, ErrTruncatedHeader
	}

	// Parse and validate header
	header, err := parseHeader(data[:HeaderSize])
	if err != nil {
		return nil, err
	}

	// Validate CRC32C footer
	if err := validateCRC(data); err != nil {
		return nil, err
	}

	// Parse chunk index
	indexEntries, err := parseChunkIndex(data, header)
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
func parseChunkIndex(data []byte, header *Header) ([]ChunkIndexEntry, error) {
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

	// Validate index bounds (must not overlap with footer)
	if indexEnd > dataLen-uint64(FooterSize) {
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
