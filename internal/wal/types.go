package wal

import (
	"github.com/google/uuid"
)

// MagicBytes is the magic string that identifies a Dray WAL v1 file.
const MagicBytes = "DRAYWO1"

// Version is the current WAL format version.
const Version uint16 = 1

// HeaderSize is the fixed size of the WAL header in bytes.
const HeaderSize = 49

// ChunkIndexEntrySize is the fixed size of each chunk index entry.
const ChunkIndexEntrySize = 44

// FooterSize is the size of the CRC32C footer.
const FooterSize = 4

// Header represents the WAL file header (49 bytes).
type Header struct {
	// Magic is the file type identifier, must be "DRAYWO1".
	Magic [7]byte
	// Version is the format version, currently 1.
	Version uint16
	// WalID is the unique identifier for this WAL object.
	WalID uuid.UUID
	// MetaDomain is the metadata domain this WAL belongs to.
	MetaDomain uint32
	// CreatedAtUnixMs is the creation timestamp in milliseconds since Unix epoch.
	CreatedAtUnixMs int64
	// ChunkCount is the number of stream chunks in this WAL.
	ChunkCount uint32
	// ChunkIndexOffset is the byte offset to the chunk index section.
	ChunkIndexOffset uint64
}

// ChunkIndexEntry describes a chunk in the WAL, one per stream.
type ChunkIndexEntry struct {
	// StreamID is the stream identifier.
	StreamID uint64
	// ChunkOffset is the byte offset to the chunk body in the file.
	ChunkOffset uint64
	// ChunkLength is the length of the chunk body in bytes.
	ChunkLength uint32
	// RecordCount is the total number of Kafka records in this chunk.
	RecordCount uint32
	// BatchCount is the number of Kafka record batches in this chunk.
	BatchCount uint32
	// MinTimestampMs is the minimum record timestamp in this chunk.
	MinTimestampMs int64
	// MaxTimestampMs is the maximum record timestamp in this chunk.
	MaxTimestampMs int64
}

// BatchEntry represents a single Kafka record batch in a chunk.
type BatchEntry struct {
	// Data is the raw Kafka record batch bytes.
	Data []byte
}

// Chunk represents a stream's data within the WAL.
type Chunk struct {
	// StreamID is the stream identifier.
	StreamID uint64
	// Batches contains the Kafka record batches for this stream.
	Batches []BatchEntry
	// RecordCount is the total record count (sum across all batches).
	RecordCount uint32
	// MinTimestampMs is the minimum timestamp across all batches.
	MinTimestampMs int64
	// MaxTimestampMs is the maximum timestamp across all batches.
	MaxTimestampMs int64
}

// WAL represents a complete WAL object ready for encoding.
type WAL struct {
	// WalID is the unique identifier for this WAL.
	WalID uuid.UUID
	// MetaDomain is the metadata domain this WAL belongs to.
	MetaDomain uint32
	// CreatedAtUnixMs is the creation timestamp.
	CreatedAtUnixMs int64
	// Chunks contains the stream chunks, will be sorted by StreamID during encoding.
	Chunks []Chunk
}

// NewWAL creates a new WAL object with the given parameters.
func NewWAL(walID uuid.UUID, metaDomain uint32, createdAtUnixMs int64) *WAL {
	return &WAL{
		WalID:           walID,
		MetaDomain:      metaDomain,
		CreatedAtUnixMs: createdAtUnixMs,
		Chunks:          make([]Chunk, 0),
	}
}

// AddChunk adds a chunk to the WAL. Chunks will be sorted by StreamID during encoding.
func (w *WAL) AddChunk(chunk Chunk) {
	w.Chunks = append(w.Chunks, chunk)
}
