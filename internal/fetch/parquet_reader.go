package fetch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// crc32cTable is the Castagnoli polynomial table for CRC32C.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Common errors for Parquet reading.
var (
	// ErrParquetNotFound is returned when the Parquet file cannot be located.
	ErrParquetNotFound = errors.New("fetch: parquet file not found")

	// ErrInvalidParquet is returned when Parquet data is corrupt or invalid.
	ErrInvalidParquet = errors.New("fetch: invalid parquet data")

	// ErrOffsetNotInParquet is returned when the requested offset is not in the Parquet file.
	ErrOffsetNotInParquet = errors.New("fetch: offset not in parquet file")
)

// ParquetReader reads records from Parquet files and reconstructs Kafka batches.
type ParquetReader struct {
	store objectstore.Store
}

// NewParquetReader creates a new Parquet reader.
func NewParquetReader(store objectstore.Store) *ParquetReader {
	return &ParquetReader{store: store}
}

// ParquetRecord represents a single record from the Parquet schema.
// Matches SPEC 5.3 schema.
type ParquetRecord struct {
	Partition   int32  `parquet:"partition"`
	Offset      int64  `parquet:"offset"`
	TimestampMs int64  `parquet:"timestamp_ms"`
	Key         []byte `parquet:"key,optional"`
	Value       []byte `parquet:"value,optional"`
	// Headers is a list of key-value pairs - handled separately
	// Attributes, ProducerId, ProducerEpoch, BaseSequence are optional
}

// ParquetHeader represents a record header from Parquet.
type ParquetHeader struct {
	Key   string `parquet:"key"`
	Value []byte `parquet:"value,optional"`
}

// ParquetRecordWithHeaders combines the record with headers.
type ParquetRecordWithHeaders struct {
	Partition     int32           `parquet:"partition"`
	Offset        int64           `parquet:"offset"`
	TimestampMs   int64           `parquet:"timestamp_ms"`
	Key           []byte          `parquet:"key,optional"`
	Value         []byte          `parquet:"value,optional"`
	Headers       []ParquetHeader `parquet:"headers,list"`
	ProducerID    *int64          `parquet:"producer_id,optional"`
	ProducerEpoch *int32          `parquet:"producer_epoch,optional"`
	BaseSequence  *int32          `parquet:"base_sequence,optional"`
	Attributes    int32           `parquet:"attributes"`
}

// ReadBatches reads records from a Parquet file and reconstructs Kafka batches.
// It returns uncompressed record batches (v2 format) ready for offset patching.
func (r *ParquetReader) ReadBatches(ctx context.Context, entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (*FetchResult, error) {
	if entry.FileType != index.FileTypeParquet {
		return nil, fmt.Errorf("fetch: expected Parquet entry, got %s", entry.FileType)
	}

	// Read the entire Parquet file from object storage
	rc, err := r.store.Get(ctx, entry.ParquetPath)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrParquetNotFound, entry.ParquetPath)
		}
		return nil, fmt.Errorf("fetch: reading parquet file: %w", err)
	}
	defer rc.Close()

	// Read all data into memory
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("fetch: reading parquet data: %w", err)
	}

	// Parse Parquet file
	reader := parquet.NewGenericReader[ParquetRecordWithHeaders](
		newParquetBytesFile(data),
	)
	defer reader.Close()

	// Read all records
	numRows := reader.NumRows()
	records := make([]ParquetRecordWithHeaders, int(numRows))
	n, err := reader.Read(records)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("fetch: parsing parquet records: %w", err)
	}
	records = records[:n]

	// Filter to records at or after fetchOffset and within the entry's range
	var filteredRecords []ParquetRecordWithHeaders
	for _, rec := range records {
		if rec.Offset >= fetchOffset && rec.Offset >= entry.StartOffset && rec.Offset < entry.EndOffset {
			filteredRecords = append(filteredRecords, rec)
		}
	}

	if len(filteredRecords) == 0 {
		return nil, ErrOffsetNotInParquet
	}

	// Reconstruct Kafka batches from the filtered records
	// For simplicity, we create one batch per record (v1 acceptable per spec)
	// This can be optimized later to batch multiple records together
	result := &FetchResult{
		Batches:     make([][]byte, 0),
		StartOffset: -1,
	}

	var totalBytes int64

	for _, rec := range filteredRecords {
		batch := buildKafkaRecordBatch(rec)

		// Check maxBytes limit
		if maxBytes > 0 && totalBytes+int64(len(batch)) > maxBytes && len(result.Batches) > 0 {
			break
		}

		result.Batches = append(result.Batches, batch)
		totalBytes += int64(len(batch))

		if result.StartOffset < 0 {
			result.StartOffset = rec.Offset
		}
		result.EndOffset = rec.Offset + 1
	}

	result.TotalBytes = totalBytes
	return result, nil
}

// buildKafkaRecordBatch constructs an uncompressed Kafka record batch (v2 format)
// from a single Parquet record.
//
// Kafka RecordBatch format (v2):
// - baseOffset (8 bytes) - i64
// - batchLength (4 bytes) - i32
// - partitionLeaderEpoch (4 bytes) - i32
// - magic (1 byte) = 2
// - crc (4 bytes) - u32 (of remaining bytes)
// - attributes (2 bytes) - i16
// - lastOffsetDelta (4 bytes) - i32
// - firstTimestamp (8 bytes) - i64
// - maxTimestamp (8 bytes) - i64
// - producerId (8 bytes) - i64
// - producerEpoch (2 bytes) - i16
// - firstSequence (4 bytes) - i32
// - recordCount (4 bytes) - i32
// - records...
func buildKafkaRecordBatch(rec ParquetRecordWithHeaders) []byte {
	// Build the record first, then wrap it in a batch
	record := buildKafkaRecord(rec)
	recordsLen := len(record)

	// Calculate batch length (everything after batchLength field)
	// partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2) +
	// lastOffsetDelta(4) + firstTimestamp(8) + maxTimestamp(8) +
	// producerId(8) + producerEpoch(2) + firstSequence(4) + recordCount(4) +
	// records
	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + recordsLen

	// Total batch size
	totalSize := 8 + 4 + batchLength // baseOffset + batchLength + rest

	batch := make([]byte, totalSize)
	offset := 0

	// baseOffset (will be patched later, use 0)
	binary.BigEndian.PutUint64(batch[offset:], 0)
	offset += 8

	// batchLength
	binary.BigEndian.PutUint32(batch[offset:], uint32(batchLength))
	offset += 4

	// partitionLeaderEpoch (0 = unknown)
	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	// magic = 2 (record batch format)
	batch[offset] = 2
	offset += 1

	// crc placeholder (calculated over remaining bytes)
	crcOffset := offset
	offset += 4

	// Start of CRC region
	crcStart := offset

	// attributes (0 = no compression, no timestamp type, etc.)
	binary.BigEndian.PutUint16(batch[offset:], uint16(int16(rec.Attributes)))
	offset += 2

	// lastOffsetDelta (0 for single record)
	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	// firstTimestamp
	binary.BigEndian.PutUint64(batch[offset:], uint64(rec.TimestampMs))
	offset += 8

	// maxTimestamp
	binary.BigEndian.PutUint64(batch[offset:], uint64(rec.TimestampMs))
	offset += 8

	// producerId (-1 = no idempotent producer)
	producerID := int64(-1)
	if rec.ProducerID != nil {
		producerID = *rec.ProducerID
	}
	binary.BigEndian.PutUint64(batch[offset:], uint64(producerID))
	offset += 8

	// producerEpoch (-1)
	producerEpoch := int16(-1)
	if rec.ProducerEpoch != nil {
		producerEpoch = int16(*rec.ProducerEpoch)
	}
	binary.BigEndian.PutUint16(batch[offset:], uint16(producerEpoch))
	offset += 2

	// firstSequence (-1)
	baseSequence := int32(-1)
	if rec.BaseSequence != nil {
		baseSequence = *rec.BaseSequence
	}
	binary.BigEndian.PutUint32(batch[offset:], uint32(baseSequence))
	offset += 4

	// recordCount (1)
	binary.BigEndian.PutUint32(batch[offset:], 1)
	offset += 4

	// Copy the record
	copy(batch[offset:], record)

	// Calculate CRC32C of the data after the CRC field
	crc := crc32c(batch[crcStart:])
	binary.BigEndian.PutUint32(batch[crcOffset:], crc)

	return batch
}

// buildKafkaRecord builds a single Kafka record in the v2 format.
//
// Record format:
// - length (varint)
// - attributes (1 byte)
// - timestampDelta (varint)
// - offsetDelta (varint)
// - keyLength (varint)
// - key (bytes)
// - valueLength (varint)
// - value (bytes)
// - headerCount (varint)
// - headers...
func buildKafkaRecord(rec ParquetRecordWithHeaders) []byte {
	// Build the record body first (without length prefix)
	var body []byte

	// attributes (1 byte, 0)
	body = append(body, 0)

	// timestampDelta (varint, 0 for single record in batch)
	body = appendVarint(body, 0)

	// offsetDelta (varint, 0 for single record in batch)
	body = appendVarint(body, 0)

	// keyLength (varint, -1 for null)
	if rec.Key == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(rec.Key)))
		body = append(body, rec.Key...)
	}

	// valueLength (varint, -1 for null)
	if rec.Value == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(rec.Value)))
		body = append(body, rec.Value...)
	}

	// headerCount (varint)
	body = appendVarint(body, int64(len(rec.Headers)))

	// headers
	for _, h := range rec.Headers {
		// header key length (varint)
		body = appendVarint(body, int64(len(h.Key)))
		body = append(body, []byte(h.Key)...)

		// header value length (varint, -1 for null)
		if h.Value == nil {
			body = appendVarint(body, -1)
		} else {
			body = appendVarint(body, int64(len(h.Value)))
			body = append(body, h.Value...)
		}
	}

	// Prepend the length as varint
	var result []byte
	result = appendVarint(result, int64(len(body)))
	result = append(result, body...)

	return result
}

// appendVarint appends a signed varint to the byte slice using zigzag encoding.
func appendVarint(b []byte, v int64) []byte {
	// Zigzag encode
	uv := uint64((v << 1) ^ (v >> 63))
	// Write as unsigned varint
	for uv >= 0x80 {
		b = append(b, byte(uv)|0x80)
		uv >>= 7
	}
	b = append(b, byte(uv))
	return b
}

// crc32c calculates CRC32C (Castagnoli) checksum.
func crc32c(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// parquetBytesFile implements parquet.File for reading from an in-memory byte slice.
type parquetBytesFile struct {
	data []byte
}

func newParquetBytesFile(data []byte) *parquetBytesFile {
	return &parquetBytesFile{data: data}
}

func (f *parquetBytesFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *parquetBytesFile) Size() int64 {
	return int64(len(f.data))
}
