package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"sort"
)

// crc32cTable is the Castagnoli polynomial table used for CRC32C.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Encoder writes WAL objects to an io.Writer.
type Encoder struct {
	w io.Writer
}

// NewEncoder creates a new WAL encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode writes the complete WAL to the underlying writer.
// Returns the total bytes written and any error encountered.
func (e *Encoder) Encode(wal *WAL) (int64, error) {
	// Sort chunks by StreamID as required by spec
	sortedChunks := make([]Chunk, len(wal.Chunks))
	copy(sortedChunks, wal.Chunks)
	sort.Slice(sortedChunks, func(i, j int) bool {
		return sortedChunks[i].StreamID < sortedChunks[j].StreamID
	})

	// Calculate layout: header -> chunk bodies -> chunk index -> footer
	// We need to know chunk index offset before writing header
	chunkBodiesOffset := uint64(HeaderSize)

	// Calculate chunk body sizes
	type chunkLayout struct {
		offset uint64
		length uint32
	}
	layouts := make([]chunkLayout, len(sortedChunks))
	currentOffset := chunkBodiesOffset

	for i, chunk := range sortedChunks {
		layouts[i].offset = currentOffset
		size := calculateChunkBodySize(chunk)
		layouts[i].length = size
		currentOffset += uint64(size)
	}

	chunkIndexOffset := currentOffset

	// Build the complete WAL bytes so we can compute CRC32C
	totalSize := chunkIndexOffset + uint64(ChunkIndexEntrySize*len(sortedChunks)) + FooterSize
	buf := make([]byte, totalSize)

	// Encode header
	offset := 0
	offset += encodeHeader(buf[offset:], wal, uint32(len(sortedChunks)), chunkIndexOffset)

	// Encode chunk bodies
	for i, chunk := range sortedChunks {
		if uint64(offset) != layouts[i].offset {
			panic("layout mismatch")
		}
		offset += encodeChunkBody(buf[offset:], chunk)
	}

	// Encode chunk index
	if uint64(offset) != chunkIndexOffset {
		panic("chunk index offset mismatch")
	}
	for i, chunk := range sortedChunks {
		offset += encodeChunkIndexEntry(buf[offset:], chunk, layouts[i].offset, layouts[i].length)
	}

	// Calculate and encode CRC32C footer (covers everything before footer)
	crc := crc32.Checksum(buf[:offset], crc32cTable)
	binary.BigEndian.PutUint32(buf[offset:], crc)
	offset += FooterSize

	// Write to underlying writer
	n, err := e.w.Write(buf)
	return int64(n), err
}

// EncodeToBytes encodes a WAL and returns the bytes.
func EncodeToBytes(wal *WAL) ([]byte, error) {
	// Sort chunks by StreamID
	sortedChunks := make([]Chunk, len(wal.Chunks))
	copy(sortedChunks, wal.Chunks)
	sort.Slice(sortedChunks, func(i, j int) bool {
		return sortedChunks[i].StreamID < sortedChunks[j].StreamID
	})

	// Calculate layout
	chunkBodiesOffset := uint64(HeaderSize)

	type chunkLayout struct {
		offset uint64
		length uint32
	}
	layouts := make([]chunkLayout, len(sortedChunks))
	currentOffset := chunkBodiesOffset

	for i, chunk := range sortedChunks {
		layouts[i].offset = currentOffset
		size := calculateChunkBodySize(chunk)
		layouts[i].length = size
		currentOffset += uint64(size)
	}

	chunkIndexOffset := currentOffset
	totalSize := chunkIndexOffset + uint64(ChunkIndexEntrySize*len(sortedChunks)) + FooterSize
	buf := make([]byte, totalSize)

	offset := 0
	offset += encodeHeader(buf[offset:], wal, uint32(len(sortedChunks)), chunkIndexOffset)

	for i, chunk := range sortedChunks {
		offset += encodeChunkBody(buf[offset:], chunk)
		_ = layouts[i] // used in chunk index
	}

	for i, chunk := range sortedChunks {
		offset += encodeChunkIndexEntry(buf[offset:], chunk, layouts[i].offset, layouts[i].length)
	}

	crc := crc32.Checksum(buf[:offset], crc32cTable)
	binary.BigEndian.PutUint32(buf[offset:], crc)

	return buf, nil
}

// encodeHeader writes the 49-byte header and returns bytes written.
func encodeHeader(buf []byte, wal *WAL, chunkCount uint32, chunkIndexOffset uint64) int {
	offset := 0

	// Magic (7 bytes)
	copy(buf[offset:], MagicBytes)
	offset += 7

	// Version (2 bytes, big-endian)
	binary.BigEndian.PutUint16(buf[offset:], Version)
	offset += 2

	// WalID (16 bytes UUID)
	copy(buf[offset:], wal.WalID[:])
	offset += 16

	// MetaDomain (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:], wal.MetaDomain)
	offset += 4

	// CreatedAtUnixMs (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], uint64(wal.CreatedAtUnixMs))
	offset += 8

	// ChunkCount (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:], chunkCount)
	offset += 4

	// ChunkIndexOffset (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], chunkIndexOffset)
	offset += 8

	return offset // should be 49
}

// encodeChunkIndexEntry writes a 44-byte chunk index entry and returns bytes written.
func encodeChunkIndexEntry(buf []byte, chunk Chunk, chunkOffset uint64, chunkLength uint32) int {
	offset := 0

	// StreamID (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], chunk.StreamID)
	offset += 8

	// ChunkOffset (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], chunkOffset)
	offset += 8

	// ChunkLength (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:], chunkLength)
	offset += 4

	// RecordCount (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:], chunk.RecordCount)
	offset += 4

	// BatchCount (4 bytes)
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(chunk.Batches)))
	offset += 4

	// MinTimestampMs (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], uint64(chunk.MinTimestampMs))
	offset += 8

	// MaxTimestampMs (8 bytes)
	binary.BigEndian.PutUint64(buf[offset:], uint64(chunk.MaxTimestampMs))
	offset += 8

	return offset // should be 44
}

// encodeChunkBody writes chunk body (batch entries) and returns bytes written.
func encodeChunkBody(buf []byte, chunk Chunk) int {
	offset := 0
	for _, batch := range chunk.Batches {
		// BatchLength (4 bytes)
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(batch.Data)))
		offset += 4
		// BatchBytes
		copy(buf[offset:], batch.Data)
		offset += len(batch.Data)
	}
	return offset
}

// calculateChunkBodySize returns the size in bytes of a chunk body.
func calculateChunkBodySize(chunk Chunk) uint32 {
	var size uint32
	for _, batch := range chunk.Batches {
		size += 4 // batch length field
		size += uint32(len(batch.Data))
	}
	return size
}

// CalculateEncodedSize returns the expected size of an encoded WAL.
func CalculateEncodedSize(wal *WAL) uint64 {
	var chunkBodySize uint64
	for _, chunk := range wal.Chunks {
		chunkBodySize += uint64(calculateChunkBodySize(chunk))
	}
	return uint64(HeaderSize) +
		chunkBodySize +
		uint64(ChunkIndexEntrySize*len(wal.Chunks)) +
		FooterSize
}
