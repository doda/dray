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

// DefaultBatchTargetSize is the target size for aggregated batches (64KB).
const DefaultBatchTargetSize = 64 * 1024

// DefaultBatchMaxRecords is the maximum records per aggregated batch.
const DefaultBatchMaxRecords = 1000

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
	Partition int32  `parquet:"partition"`
	Offset    int64  `parquet:"offset"`
	Timestamp int64  `parquet:"timestamp,timestamp(millisecond)"`
	Key       []byte `parquet:"key,optional"`
	Value     []byte `parquet:"value,optional"`
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
	Timestamp     int64           `parquet:"timestamp,timestamp(millisecond)"`
	Key           []byte          `parquet:"key,optional"`
	Value         []byte          `parquet:"value,optional"`
	Headers       []ParquetHeader `parquet:"headers,list"`
	ProducerID    *int64          `parquet:"producer_id,optional"`
	ProducerEpoch *int32          `parquet:"producer_epoch,optional"`
	BaseSequence  *int32          `parquet:"base_sequence,optional"`
	Attributes    int32           `parquet:"attributes"`
	RecordCRC     *int32          `parquet:"record_crc,optional"`
}

// ReadBatches reads records from a Parquet file and reconstructs Kafka batches.
// It returns uncompressed record batches (v2 format) ready for offset patching.
func (r *ParquetReader) ReadBatches(ctx context.Context, entry *index.IndexEntry, fetchOffset int64, maxBytes int64) (*FetchResult, error) {
	if entry.FileType != index.FileTypeParquet {
		return nil, fmt.Errorf("fetch: expected Parquet entry, got %s", entry.FileType)
	}

	meta, err := r.store.Head(ctx, entry.ParquetPath)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrParquetNotFound, entry.ParquetPath)
		}
		return nil, fmt.Errorf("fetch: reading parquet metadata: %w", err)
	}

	if meta.Size == 0 {
		return nil, ErrInvalidParquet
	}

	readerAt := newObjectStoreReaderAt(ctx, r.store, entry.ParquetPath, meta.Size)
	parquetFile, err := parquet.OpenFile(readerAt, meta.Size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidParquet, err)
	}

	rangeStart := fetchOffset
	if rangeStart < entry.StartOffset {
		rangeStart = entry.StartOffset
	}
	rangeEnd := entry.EndOffset
	if rangeEnd <= rangeStart {
		return nil, ErrOffsetNotInParquet
	}

	rowGroups, err := selectRowGroupsByOffset(parquetFile, rangeStart, rangeEnd)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidParquet, err)
	}
	if len(rowGroups) == 0 {
		return nil, ErrOffsetNotInParquet
	}

	result := &FetchResult{
		Batches:     make([][]byte, 0),
		StartOffset: -1,
	}

	agg := newRecordAggregator(DefaultBatchTargetSize, DefaultBatchMaxRecords)
	var totalBytes int64
	var found bool
	var batchBaseOffset int64 = -1
	var batchBaseTimestamp int64

	for _, rowGroup := range rowGroups {
		groupReader := parquet.NewGenericRowGroupReader[ParquetRecordWithHeaders](rowGroup)
		rows := make([]ParquetRecordWithHeaders, 256)

		for {
			n, err := groupReader.Read(rows)
			if n > 0 {
				for _, rec := range rows[:n] {
					if rec.Offset < rangeStart || rec.Offset >= rangeEnd {
						continue
					}

					// Track first record's offset for the result
					if result.StartOffset < 0 {
						result.StartOffset = rec.Offset
					}
					result.EndOffset = rec.Offset + 1

					// Track batch base values for delta calculation
					if batchBaseOffset < 0 {
						batchBaseOffset = rec.Offset
						batchBaseTimestamp = rec.Timestamp
					}

					offsetDelta := int32(rec.Offset - batchBaseOffset)
					timestampDelta := rec.Timestamp - batchBaseTimestamp

					shouldFlush := agg.add(rec, offsetDelta, timestampDelta)
					found = true

					if shouldFlush {
						batch := agg.flush()
						batchSize := int64(len(batch))

						// Check maxBytes limit before adding batch
						if maxBytes > 0 && totalBytes+batchSize > maxBytes && len(result.Batches) > 0 {
							groupReader.Close()
							result.TotalBytes = totalBytes
							return result, nil
						}

						result.Batches = append(result.Batches, batch)
						totalBytes += batchSize

						// Reset base values for next batch
						batchBaseOffset = -1
					}
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				groupReader.Close()
				return nil, fmt.Errorf("%w: %v", ErrInvalidParquet, err)
			}
		}

		if err := groupReader.Close(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidParquet, err)
		}
	}

	// Flush any remaining records in the aggregator
	if agg.count() > 0 {
		batch := agg.flush()
		batchSize := int64(len(batch))

		// Check maxBytes limit for final batch
		if maxBytes > 0 && totalBytes+batchSize > maxBytes && len(result.Batches) > 0 {
			result.TotalBytes = totalBytes
			return result, nil
		}

		result.Batches = append(result.Batches, batch)
		totalBytes += batchSize
	}

	if !found {
		return nil, ErrOffsetNotInParquet
	}

	result.TotalBytes = totalBytes
	return result, nil
}

// recordAggregator accumulates Parquet records and emits aggregated Kafka batches.
type recordAggregator struct {
	records        []ParquetRecordWithHeaders
	recordBytes    [][]byte
	currentSize    int
	targetSize     int
	maxRecords     int
	firstOffset    int64
	firstTimestamp int64
	maxTimestamp   int64
}

// newRecordAggregator creates a new record aggregator with the given limits.
func newRecordAggregator(targetSize, maxRecords int) *recordAggregator {
	return &recordAggregator{
		records:     make([]ParquetRecordWithHeaders, 0, maxRecords),
		recordBytes: make([][]byte, 0, maxRecords),
		targetSize:  targetSize,
		maxRecords:  maxRecords,
	}
}

// add adds a record to the aggregator. Returns true if a batch should be flushed.
func (a *recordAggregator) add(rec ParquetRecordWithHeaders, offsetDelta int32, timestampDelta int64) bool {
	// Build the individual record bytes with proper deltas
	recBytes := buildKafkaRecordWithDeltas(rec, offsetDelta, timestampDelta)

	// Track first record metadata
	if len(a.records) == 0 {
		a.firstOffset = rec.Offset
		a.firstTimestamp = rec.Timestamp
		a.maxTimestamp = rec.Timestamp
	}
	if rec.Timestamp > a.maxTimestamp {
		a.maxTimestamp = rec.Timestamp
	}

	a.records = append(a.records, rec)
	a.recordBytes = append(a.recordBytes, recBytes)
	a.currentSize += len(recBytes)

	// Return true if we should flush (size or count limit reached)
	return a.currentSize >= a.targetSize || len(a.records) >= a.maxRecords
}

// count returns the number of buffered records.
func (a *recordAggregator) count() int {
	return len(a.records)
}

// flush builds and returns a Kafka batch from the buffered records, then resets the aggregator.
func (a *recordAggregator) flush() []byte {
	if len(a.records) == 0 {
		return nil
	}

	batch := a.buildBatch()
	a.reset()
	return batch
}

// reset clears the aggregator state.
func (a *recordAggregator) reset() {
	a.records = a.records[:0]
	a.recordBytes = a.recordBytes[:0]
	a.currentSize = 0
	a.firstOffset = 0
	a.firstTimestamp = 0
	a.maxTimestamp = 0
}

// buildBatch constructs a Kafka record batch from the aggregated records.
func (a *recordAggregator) buildBatch() []byte {
	if len(a.records) == 0 {
		return nil
	}

	// Concatenate all record bytes
	var recordsLen int
	for _, rb := range a.recordBytes {
		recordsLen += len(rb)
	}

	// Use attributes from first record (they should be consistent within a batch)
	attributes := int16(a.records[0].Attributes) &^ 0x07 // clear compression bits

	// Use producer info from first record if available
	producerID := int64(-1)
	producerEpoch := int16(-1)
	baseSequence := int32(-1)
	if a.records[0].ProducerID != nil {
		producerID = *a.records[0].ProducerID
	}
	if a.records[0].ProducerEpoch != nil {
		producerEpoch = int16(*a.records[0].ProducerEpoch)
	}
	if a.records[0].BaseSequence != nil {
		baseSequence = *a.records[0].BaseSequence
	}

	lastOffsetDelta := int32(len(a.records) - 1)

	// Calculate batch length
	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + recordsLen
	totalSize := 8 + 4 + batchLength

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

	// crc placeholder
	crcOffset := offset
	offset += 4

	// Start of CRC region
	crcStart := offset

	// attributes
	binary.BigEndian.PutUint16(batch[offset:], uint16(attributes))
	offset += 2

	// lastOffsetDelta
	binary.BigEndian.PutUint32(batch[offset:], uint32(lastOffsetDelta))
	offset += 4

	// firstTimestamp
	binary.BigEndian.PutUint64(batch[offset:], uint64(a.firstTimestamp))
	offset += 8

	// maxTimestamp
	binary.BigEndian.PutUint64(batch[offset:], uint64(a.maxTimestamp))
	offset += 8

	// producerId
	binary.BigEndian.PutUint64(batch[offset:], uint64(producerID))
	offset += 8

	// producerEpoch
	binary.BigEndian.PutUint16(batch[offset:], uint16(producerEpoch))
	offset += 2

	// firstSequence
	binary.BigEndian.PutUint32(batch[offset:], uint32(baseSequence))
	offset += 4

	// recordCount
	binary.BigEndian.PutUint32(batch[offset:], uint32(len(a.records)))
	offset += 4

	// Copy all records
	for _, rb := range a.recordBytes {
		copy(batch[offset:], rb)
		offset += len(rb)
	}

	// Calculate CRC32C
	crc := crc32c(batch[crcStart:])
	binary.BigEndian.PutUint32(batch[crcOffset:], crc)

	return batch
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

	// attributes: clear compression bits since we emit uncompressed batches.
	attributes := int16(rec.Attributes) &^ 0x07
	binary.BigEndian.PutUint16(batch[offset:], uint16(attributes))
	offset += 2

	// lastOffsetDelta (0 for single record)
	binary.BigEndian.PutUint32(batch[offset:], 0)
	offset += 4

	// firstTimestamp
	binary.BigEndian.PutUint64(batch[offset:], uint64(rec.Timestamp))
	offset += 8

	// maxTimestamp
	binary.BigEndian.PutUint64(batch[offset:], uint64(rec.Timestamp))
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
// This is used for single-record batches where offsetDelta and timestampDelta are 0.
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
	return buildKafkaRecordWithDeltas(rec, 0, 0)
}

// buildKafkaRecordWithDeltas builds a Kafka record with specified offset and timestamp deltas.
// This is used for multi-record batches where records have increasing deltas.
func buildKafkaRecordWithDeltas(rec ParquetRecordWithHeaders, offsetDelta int32, timestampDelta int64) []byte {
	// Build the record body first (without length prefix)
	var body []byte

	// attributes (1 byte, 0)
	body = append(body, 0)

	// timestampDelta (varint)
	body = appendVarint(body, timestampDelta)

	// offsetDelta (varint)
	body = appendVarint(body, int64(offsetDelta))

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

type objectStoreReaderAt struct {
	ctx   context.Context
	store objectstore.Store
	key   string
	size  int64
}

func newObjectStoreReaderAt(ctx context.Context, store objectstore.Store, key string, size int64) *objectStoreReaderAt {
	return &objectStoreReaderAt{
		ctx:   ctx,
		store: store,
		key:   key,
		size:  size,
	}
}

func (r *objectStoreReaderAt) Size() int64 {
	return r.size
}

func (r *objectStoreReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("fetch: invalid offset %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	rc, err := r.store.GetRange(r.ctx, r.key, off, end)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	readLen := int(end - off + 1)
	n, err := io.ReadFull(rc, p[:readLen])
	if err != nil && err != io.ErrUnexpectedEOF {
		return n, err
	}
	if readLen < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func selectRowGroupsByOffset(file *parquet.File, rangeStart, rangeEnd int64) ([]parquet.RowGroup, error) {
	if rangeEnd <= rangeStart {
		return nil, nil
	}

	offsetColumn, ok := file.Schema().Lookup("offset")
	rowGroups := file.RowGroups()
	if !ok {
		return rowGroups, nil
	}

	columnIndex := offsetColumn.ColumnIndex
	selected := make([]parquet.RowGroup, 0, len(rowGroups))
	for _, rowGroup := range rowGroups {
		chunks := rowGroup.ColumnChunks()
		if columnIndex >= len(chunks) {
			selected = append(selected, rowGroup)
			continue
		}
		boundsChunk, ok := chunks[columnIndex].(interface {
			Bounds() (parquet.Value, parquet.Value, bool)
		})
		if !ok {
			selected = append(selected, rowGroup)
			continue
		}
		minValue, maxValue, ok := boundsChunk.Bounds()
		if !ok || minValue.IsNull() || maxValue.IsNull() {
			selected = append(selected, rowGroup)
			continue
		}
		minOffset := minValue.Int64()
		maxOffset := maxValue.Int64()
		if maxOffset < rangeStart || minOffset >= rangeEnd {
			continue
		}
		selected = append(selected, rowGroup)
	}

	return selected, nil
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
