// Package worker converts WAL entries to Parquet and performs index swap.
package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"runtime"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/pierrec/lz4/v4"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/projection"
	"github.com/dray-io/dray/internal/wal"
)

// crc32cTable is the CRC32C (Castagnoli) polynomial table used by Kafka record batches.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Converter reads WAL entries for a stream and converts them to Parquet.
type Converter struct {
	store       objectstore.Store
	projections map[string]projection.TopicProjection
}

// NewConverter creates a new WAL-to-Parquet converter.
func NewConverter(store objectstore.Store, projections []projection.TopicProjection) *Converter {
	return &Converter{
		store:       store,
		projections: projection.ByTopic(projection.Normalize(projections)),
	}
}

// ProjectionFields returns the configured projected fields for a topic.
func (c *Converter) ProjectionFields(topicName string) []projection.FieldSpec {
	if proj, ok := c.projections[topicName]; ok {
		return proj.Fields
	}
	return nil
}

// ConvertResult contains the result of a WAL-to-Parquet conversion.
type ConvertResult struct {
	// ParquetData is the raw Parquet file bytes.
	ParquetData []byte
	// Stats contains file-level statistics.
	Stats FileStats
	// RecordCount is the total number of records converted.
	RecordCount int64
}

// PartitionedConvertResult contains results from partition-aware conversion.
// Each result represents a separate Parquet file for one partition value.
type PartitionedConvertResult struct {
	// Results is a list of conversion results, one per partition value.
	Results []PartitionedFile
	// TotalRecords is the total number of records across all partitions.
	TotalRecords int64
}

// PartitionedFile represents a single Parquet file for one partition value.
type PartitionedFile struct {
	// HourValue is the hour partition value (hours since epoch).
	// For hour(created_at) partitioning.
	HourValue int64
	// ParquetData is the raw Parquet file bytes.
	ParquetData []byte
	// Stats contains file-level statistics.
	Stats FileStats
	// RecordCount is the number of records in this partition.
	RecordCount int64
}

// Kafka compression types (bits 0-2 of attributes)
const (
	compressionNone   = 0
	compressionGzip   = 1
	compressionSnappy = 2
	compressionLz4    = 3
	compressionZstd   = 4
)

// Convert reads WAL entries for the given index entries and converts them to Parquet.
// All entries must be WAL type and belong to the same stream.
func (c *Converter) Convert(ctx context.Context, entries []*index.IndexEntry, partition int32, topicName string, schema *parquet.Schema) (*ConvertResult, error) {
	if len(entries) == 0 {
		return nil, errors.New("converter: no entries to convert")
	}

	// Validate all entries are WAL type and belong to same stream
	streamID := entries[0].StreamID
	for _, e := range entries {
		if e.FileType != index.FileTypeWAL {
			return nil, fmt.Errorf("converter: expected WAL entry, got %s", e.FileType)
		}
		if e.StreamID != streamID {
			return nil, errors.New("converter: entries must belong to same stream")
		}
	}

	// Prepare projection settings for this topic.
	projectionFields := c.ProjectionFields(topicName)
	projector := newRecordProjector(projectionFields)

	allRecords, err := c.collectRecords(ctx, entries, partition, projector)
	if err != nil {
		return nil, err
	}
	if len(allRecords) == 0 {
		return nil, errors.New("converter: no records extracted from WAL entries")
	}

	// Write records to Parquet with projection bounds tracking.
	parquetSchema := schema
	if parquetSchema == nil {
		parquetSchema = BuildParquetSchema(projectionFields)
	}
	parquetData, stats, err := WriteToBufferWithProjections(parquetSchema, allRecords, projectionFields)
	if err != nil {
		return nil, fmt.Errorf("converter: writing parquet: %w", err)
	}

	return &ConvertResult{
		ParquetData: parquetData,
		Stats:       stats,
		RecordCount: int64(len(allRecords)),
	}, nil
}

// ConvertPartitioned reads WAL entries and converts them to multiple Parquet files,
// one per partition value (hour). This is required for Iceberg tables with time-based
// partitioning, as each data file must belong to exactly one partition.
func (c *Converter) ConvertPartitioned(ctx context.Context, entries []*index.IndexEntry, partition int32, topicName string, schema *parquet.Schema) (*PartitionedConvertResult, error) {
	if len(entries) == 0 {
		return nil, errors.New("converter: no entries to convert")
	}

	// Validate all entries are WAL type and belong to same stream
	streamID := entries[0].StreamID
	for _, e := range entries {
		if e.FileType != index.FileTypeWAL {
			return nil, fmt.Errorf("converter: expected WAL entry, got %s", e.FileType)
		}
		if e.StreamID != streamID {
			return nil, errors.New("converter: entries must belong to same stream")
		}
	}

	// Prepare projection settings for this topic.
	projectionFields := c.ProjectionFields(topicName)
	projector := newRecordProjector(projectionFields)

	allRecords, err := c.collectRecords(ctx, entries, partition, projector)
	if err != nil {
		return nil, err
	}
	if len(allRecords) == 0 {
		return nil, errors.New("converter: no records extracted from WAL entries")
	}

	// Group records by hour value based on created_at projected field
	recordsByHour := make(map[int64][]Record)
	for _, rec := range allRecords {
		// Use created_at from projected fields if available, otherwise fall back to Kafka timestamp
		var hourValue int64
		if createdAt, ok := rec.Projected["created_at"]; ok {
			switch v := createdAt.(type) {
			case int64:
				hourValue = v / (3600 * 1000)
			case float64:
				hourValue = int64(v) / (3600 * 1000)
			default:
				// Fallback to Kafka timestamp if created_at is not a number
				hourValue = rec.Timestamp / (3600 * 1000)
			}
		} else {
			// Fallback to Kafka timestamp if created_at not projected
			hourValue = rec.Timestamp / (3600 * 1000)
		}
		recordsByHour[hourValue] = append(recordsByHour[hourValue], rec)
	}

	// Build schema once
	parquetSchema := schema
	if parquetSchema == nil {
		parquetSchema = BuildParquetSchema(projectionFields)
	}

	// Write separate Parquet file for each hour
	result := &PartitionedConvertResult{
		Results:      make([]PartitionedFile, 0, len(recordsByHour)),
		TotalRecords: int64(len(allRecords)),
	}

	for hourValue, records := range recordsByHour {
		parquetData, stats, err := WriteToBufferWithProjections(parquetSchema, records, projectionFields)
		if err != nil {
			return nil, fmt.Errorf("converter: writing parquet for hour %d: %w", hourValue, err)
		}

		result.Results = append(result.Results, PartitionedFile{
			HourValue:   hourValue,
			ParquetData: parquetData,
			Stats:       stats,
			RecordCount: int64(len(records)),
		})
	}

	return result, nil
}

// collectRecords fans out WAL chunk reads across entries using a bounded worker pool.
func (c *Converter) collectRecords(ctx context.Context, entries []*index.IndexEntry, partition int32, projector *recordProjector) ([]Record, error) {
	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(entries) {
		workerCount = len(entries)
	}

	sem := make(chan struct{}, workerCount)
	results := make([][]Record, len(entries))
	errCh := make(chan error, len(entries))
	var wg sync.WaitGroup

	for i, entry := range entries {
		wg.Add(1)
		go func(idx int, ent *index.IndexEntry) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}

			recs, err := c.extractRecordsFromEntry(ctx, ent, partition, projector)
			if err != nil {
				errCh <- fmt.Errorf("converter: processing entry at offset %d: %w", ent.StartOffset, err)
				return
			}
			results[idx] = recs
		}(i, entry)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	total := 0
	for _, recs := range results {
		total += len(recs)
	}

	allRecords := make([]Record, 0, total)
	for _, recs := range results {
		allRecords = append(allRecords, recs...)
	}

	return allRecords, nil
}

// extractRecordsFromEntry reads a single WAL entry and extracts all records.
func (c *Converter) extractRecordsFromEntry(ctx context.Context, entry *index.IndexEntry, partition int32, projector *recordProjector) ([]Record, error) {
	// Range-read the chunk data from object storage
	startByte := int64(entry.ChunkOffset)
	endByte := startByte + int64(entry.ChunkLength) - 1

	rc, err := c.store.GetRange(ctx, entry.WalPath, startByte, endByte)
	if err != nil {
		return nil, fmt.Errorf("reading WAL chunk: %w", err)
	}
	defer rc.Close()

	chunkData, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading chunk data: %w", err)
	}

	if len(chunkData) != int(entry.ChunkLength) {
		return nil, fmt.Errorf("expected %d bytes, got %d", entry.ChunkLength, len(chunkData))
	}

	// Parse batches from chunk data
	batches, err := parseBatchesFromChunk(chunkData)
	if err != nil {
		return nil, fmt.Errorf("parsing batches: %w", err)
	}

	// Extract records from all batches
	var allRecords []Record
	currentOffset := entry.StartOffset

	for _, batchData := range batches {
		records, err := extractRecordsFromBatch(batchData, partition, currentOffset, projector)
		if err != nil {
			return nil, fmt.Errorf("extracting records at offset %d: %w", currentOffset, err)
		}
		allRecords = append(allRecords, records...)
		currentOffset += int64(len(records))
	}

	return allRecords, nil
}

// parseBatchesFromChunk parses the chunk data and extracts batch bytes.
// The chunk format is: [batchLen (4 bytes), batchData (batchLen bytes)] repeated.
func parseBatchesFromChunk(chunkData []byte) ([][]byte, error) {
	var batches [][]byte
	offset := 0

	for offset < len(chunkData) {
		// Read batch length prefix (4 bytes)
		if offset+4 > len(chunkData) {
			return nil, fmt.Errorf("truncated batch length at offset %d", offset)
		}
		batchLen := int(binary.BigEndian.Uint32(chunkData[offset : offset+4]))
		offset += 4

		if batchLen <= 0 {
			return nil, fmt.Errorf("invalid batch length %d at offset %d", batchLen, offset-4)
		}

		if offset+batchLen > len(chunkData) {
			return nil, fmt.Errorf("batch truncated at offset %d (need %d, have %d)", offset, batchLen, len(chunkData)-offset)
		}

		batchData := make([]byte, batchLen)
		copy(batchData, chunkData[offset:offset+batchLen])
		batches = append(batches, batchData)
		offset += batchLen
	}

	return batches, nil
}

// ErrBatchCRCMismatch is returned when a batch's CRC checksum doesn't match.
var ErrBatchCRCMismatch = errors.New("batch CRC mismatch")

// validateBatchCRC validates the CRC32C checksum of a Kafka v2 record batch.
// The CRC covers everything from attributes (byte 21) to the end of the batch.
// Returns nil if the CRC is valid, or an error describing the mismatch.
func validateBatchCRC(batchData []byte) error {
	if len(batchData) < 21 {
		return errors.New("batch too small for CRC validation")
	}

	// Magic byte is at offset 16 (after baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4))
	magic := batchData[16]
	if magic != 2 {
		return fmt.Errorf("unsupported magic byte %d, expected 2", magic)
	}

	// CRC is stored at bytes 17-20 (after magic byte)
	storedCRC := binary.BigEndian.Uint32(batchData[17:21])

	// CRC is computed over bytes 21 to end (attributes through records)
	computedCRC := crc32.Checksum(batchData[21:], crc32cTable)

	if storedCRC != computedCRC {
		return fmt.Errorf("%w: stored=0x%08x, computed=0x%08x", ErrBatchCRCMismatch, storedCRC, computedCRC)
	}

	return nil
}

// extractRecordsFromBatch parses a Kafka record batch and extracts individual records.
// It validates the batch CRC and handles decompression if the batch is compressed.
func extractRecordsFromBatch(batchData []byte, partition int32, baseOffset int64, projector *recordProjector) ([]Record, error) {
	if len(batchData) < 61 {
		return nil, errors.New("batch too small")
	}

	// Validate CRC before processing
	if err := validateBatchCRC(batchData); err != nil {
		return nil, fmt.Errorf("CRC validation failed: %w", err)
	}

	// Parse batch header fields
	// Kafka RecordBatch format (v2):
	// - baseOffset (8) - we use the assigned baseOffset
	// - batchLength (4)
	// - partitionLeaderEpoch (4)
	// - magic (1) = 2
	// - crc (4)
	// - attributes (2)
	// - lastOffsetDelta (4)
	// - firstTimestamp (8)
	// - maxTimestamp (8)
	// - producerId (8)
	// - producerEpoch (2)
	// - firstSequence (4)
	// - recordCount (4)
	// Total header: 61 bytes

	attributes := int16(binary.BigEndian.Uint16(batchData[21:23]))
	firstTimestamp := int64(binary.BigEndian.Uint64(batchData[27:35]))
	producerID := int64(binary.BigEndian.Uint64(batchData[43:51]))
	producerEpoch := int16(binary.BigEndian.Uint16(batchData[51:53]))
	baseSequence := int32(binary.BigEndian.Uint32(batchData[53:57]))
	recordCount := int32(binary.BigEndian.Uint32(batchData[57:61]))

	if recordCount <= 0 {
		return nil, errors.New("invalid record count")
	}

	// Get compression type from attributes (bits 0-2)
	compressionType := int(attributes & 0x07)

	// Records start after the header
	recordsData := batchData[61:]

	// Decompress if necessary
	if compressionType != compressionNone {
		decompressed, err := decompressRecords(recordsData, compressionType)
		if err != nil {
			return nil, fmt.Errorf("decompressing records: %w", err)
		}
		recordsData = decompressed
	}

	// Parse individual records
	records := make([]Record, 0, recordCount)
	offset := 0

	for i := int32(0); i < recordCount; i++ {
		if offset >= len(recordsData) {
			return nil, fmt.Errorf("unexpected end of records at index %d", i)
		}

		rec, bytesRead, err := parseRecord(recordsData[offset:], partition, baseOffset+int64(i), firstTimestamp, producerID, producerEpoch, baseSequence, attributes, projector)
		if err != nil {
			return nil, fmt.Errorf("parsing record %d: %w", i, err)
		}

		records = append(records, rec)
		offset += bytesRead
	}

	return records, nil
}

// decompressRecords decompresses the records data based on compression type.
func decompressRecords(data []byte, compressionType int) ([]byte, error) {
	switch compressionType {
	case compressionGzip:
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer reader.Close()
		return io.ReadAll(reader)

	case compressionSnappy:
		return snappy.Decode(nil, data)

	case compressionLz4:
		reader := lz4.NewReader(bytes.NewReader(data))
		return io.ReadAll(reader)

	case compressionZstd:
		decoder, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("zstd reader: %w", err)
		}
		defer decoder.Close()
		return io.ReadAll(decoder)

	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// parseRecord parses a single Kafka v2 record and returns a Parquet Record.
// Record format:
// - length (varint) - length of the record excluding this field
// - attributes (1 byte)
// - timestampDelta (varint)
// - offsetDelta (varint)
// - keyLength (varint, -1 for null)
// - key (bytes)
// - valueLength (varint, -1 for null)
// - value (bytes)
// - headerCount (varint)
// - headers...
func parseRecord(data []byte, partition int32, offset int64, firstTimestamp int64, producerID int64, producerEpoch int16, baseSequence int32, batchAttributes int16, projector *recordProjector) (Record, int, error) {
	pos := 0

	// Read record length (varint)
	recordLen, bytesRead := readVarint(data[pos:])
	if bytesRead <= 0 {
		return Record{}, 0, errors.New("failed to read record length")
	}
	pos += bytesRead
	recordStart := pos

	// Read attributes (1 byte)
	if pos >= len(data) {
		return Record{}, 0, errors.New("unexpected end of record at attributes")
	}
	_ = data[pos] // record attributes (unused, different from batch attributes)
	pos++

	// Read timestampDelta (varint)
	timestampDelta, bytesRead := readVarint(data[pos:])
	if bytesRead <= 0 {
		return Record{}, 0, errors.New("failed to read timestamp delta")
	}
	pos += bytesRead

	// Read offsetDelta (varint) - we ignore this and use our assigned offset
	_, bytesRead = readVarint(data[pos:])
	if bytesRead <= 0 {
		return Record{}, 0, errors.New("failed to read offset delta")
	}
	pos += bytesRead

	// Read keyLength (varint, -1 for null)
	keyLen, bytesRead := readVarint(data[pos:])
	if bytesRead <= 0 {
		return Record{}, 0, errors.New("failed to read key length")
	}
	pos += bytesRead

	// Read key
	var key []byte
	if keyLen >= 0 {
		if pos+int(keyLen) > len(data) {
			return Record{}, 0, errors.New("key truncated")
		}
		key = make([]byte, keyLen)
		copy(key, data[pos:pos+int(keyLen)])
		pos += int(keyLen)
	}

	// Read valueLength (varint, -1 for null)
	valueLen, bytesRead := readVarint(data[pos:])
	if bytesRead <= 0 {
		return Record{}, 0, errors.New("failed to read value length")
	}
	pos += bytesRead

	// Read value
	var value []byte
	if valueLen >= 0 {
		if pos+int(valueLen) > len(data) {
			return Record{}, 0, errors.New("value truncated")
		}
		value = make([]byte, valueLen)
		copy(value, data[pos:pos+int(valueLen)])
		pos += int(valueLen)
	}

	// Read header count (varint)
	headerCount, bytesRead := readVarint(data[pos:])
	if bytesRead <= 0 {
		return Record{}, 0, errors.New("failed to read header count")
	}
	pos += bytesRead
	if headerCount < 0 {
		return Record{}, 0, errors.New("invalid negative header count")
	}

	// Read headers
	headers := make([]Header, 0, headerCount)
	for i := int64(0); i < headerCount; i++ {
		// Header key length (varint)
		hKeyLen, bytesRead := readVarint(data[pos:])
		if bytesRead <= 0 {
			return Record{}, 0, fmt.Errorf("failed to read header %d key length", i)
		}
		pos += bytesRead

		// Header key
		var hKey string
		if hKeyLen >= 0 {
			if pos+int(hKeyLen) > len(data) {
				return Record{}, 0, fmt.Errorf("header %d key truncated", i)
			}
			hKey = string(data[pos : pos+int(hKeyLen)])
			pos += int(hKeyLen)
		}

		// Header value length (varint, -1 for null)
		hValueLen, bytesRead := readVarint(data[pos:])
		if bytesRead <= 0 {
			return Record{}, 0, fmt.Errorf("failed to read header %d value length", i)
		}
		pos += bytesRead

		// Header value
		var hValue []byte
		if hValueLen >= 0 {
			if pos+int(hValueLen) > len(data) {
				return Record{}, 0, fmt.Errorf("header %d value truncated", i)
			}
			hValue = make([]byte, hValueLen)
			copy(hValue, data[pos:pos+int(hValueLen)])
			pos += int(hValueLen)
		}

		headers = append(headers, Header{Key: hKey, Value: hValue})
	}

	// Validate we read exactly recordLen bytes
	bytesConsumed := pos - recordStart
	if int64(bytesConsumed) != recordLen {
		return Record{}, 0, fmt.Errorf("record length mismatch: expected %d, consumed %d", recordLen, bytesConsumed)
	}

	// Build the Record for Parquet
	rec := Record{
		Partition:  partition,
		Offset:     offset,
		Timestamp:  firstTimestamp + timestampDelta,
		Key:        key,
		Value:      value,
		Headers:    headers,
		Attributes: int32(batchAttributes),
	}

	rec.Projected = projectValue(value, rec.Timestamp, projector)

	// Set producer fields if idempotent producer was used
	if producerID >= 0 {
		rec.ProducerID = &producerID
		epoch := int32(producerEpoch)
		rec.ProducerEpoch = &epoch
		rec.BaseSequence = &baseSequence
	}

	// Compute and set record CRC for debugging/validation
	recCRC := computeRecordCRC(key, value, headers)
	rec.RecordCRC = &recCRC

	return rec, pos, nil
}

// computeRecordCRC calculates a CRC32C over the record's content (key, value, headers).
// This is used for debugging/validation per SPEC 5.3.
func computeRecordCRC(key, value []byte, headers []Header) int32 {
	h := crc32.New(crc32cTable)
	if key != nil {
		h.Write(key)
	}
	if value != nil {
		h.Write(value)
	}
	for _, hdr := range headers {
		h.Write([]byte(hdr.Key))
		if hdr.Value != nil {
			h.Write(hdr.Value)
		}
	}
	return int32(h.Sum32())
}

// readVarint reads a zigzag-encoded signed varint from the data.
// Returns the decoded value and number of bytes consumed.
func readVarint(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	// Read unsigned varint
	var uv uint64
	var shift uint
	var bytesRead int

	for i := 0; i < len(data) && i < 10; i++ {
		b := data[i]
		uv |= uint64(b&0x7F) << shift
		bytesRead++
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}

	if bytesRead == 0 {
		return 0, 0
	}

	// Zigzag decode: (uv >> 1) ^ -(uv & 1)
	v := int64((uv >> 1) ^ -(uv & 1))
	return v, bytesRead
}

// ConvertWALToParquet is a convenience function that converts WAL data directly to Parquet.
// This is useful when you have the WAL bytes in memory (e.g., for testing).
func ConvertWALToParquet(walData []byte, partition int32, startOffset int64, projectionFields []projection.FieldSpec) (*ConvertResult, error) {
	// Decode the WAL
	decoded, err := wal.DecodeFromBytes(walData)
	if err != nil {
		return nil, fmt.Errorf("decoding WAL: %w", err)
	}

	if len(decoded.Chunks) == 0 {
		return nil, errors.New("WAL has no chunks")
	}

	projector := newRecordProjector(projectionFields)

	// Convert all chunks to records
	var allRecords []Record
	currentOffset := startOffset

	for _, chunk := range decoded.Chunks {
		for _, batch := range chunk.Batches {
			records, err := extractRecordsFromBatch(batch.Data, partition, currentOffset, projector)
			if err != nil {
				return nil, fmt.Errorf("extracting records: %w", err)
			}
			allRecords = append(allRecords, records...)
			currentOffset += int64(len(records))
		}
	}

	if len(allRecords) == 0 {
		return nil, errors.New("no records extracted from WAL")
	}

	// Write to Parquet
	parquetSchema := BuildParquetSchema(projectionFields)
	parquetData, stats, err := WriteToBuffer(parquetSchema, allRecords)
	if err != nil {
		return nil, fmt.Errorf("writing parquet: %w", err)
	}

	return &ConvertResult{
		ParquetData: parquetData,
		Stats:       stats,
		RecordCount: int64(len(allRecords)),
	}, nil
}

// WriteParquetToStorage writes Parquet data to object storage.
func (c *Converter) WriteParquetToStorage(ctx context.Context, path string, data []byte) error {
	return c.store.Put(ctx, path, bytes.NewReader(data), int64(len(data)), "application/x-parquet")
}

// StreamingConverter reads WAL entries and converts them to Parquet in a streaming fashion.
// Unlike Converter, it processes batches incrementally and writes to Parquet as it goes,
// never holding all records in memory at once.
type StreamingConverter struct {
	store objectstore.Store
}

// NewStreamingConverter creates a new streaming WAL-to-Parquet converter.
func NewStreamingConverter(store objectstore.Store) *StreamingConverter {
	return &StreamingConverter{store: store}
}

// StreamingConvertConfig configures streaming conversion behavior.
type StreamingConvertConfig struct {
	// BatchSize is the number of records to accumulate before writing to Parquet.
	// Default is 1000 records.
	BatchSize int
}

// DefaultStreamingConvertConfig returns the default configuration for streaming conversion.
func DefaultStreamingConvertConfig() StreamingConvertConfig {
	return StreamingConvertConfig{
		BatchSize: 1000,
	}
}

// ConvertStreaming reads WAL entries and converts them to Parquet incrementally.
// It processes each WAL chunk separately, writing records to Parquet as batches,
// avoiding loading all records into memory at once.
func (c *StreamingConverter) ConvertStreaming(ctx context.Context, entries []*index.IndexEntry, partition int32, cfg StreamingConvertConfig) (*ConvertResult, error) {
	if len(entries) == 0 {
		return nil, errors.New("streaming converter: no entries to convert")
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}

	// Validate all entries are WAL type and belong to same stream
	streamID := entries[0].StreamID
	for _, e := range entries {
		if e.FileType != index.FileTypeWAL {
			return nil, fmt.Errorf("streaming converter: expected WAL entry, got %s", e.FileType)
		}
		if e.StreamID != streamID {
			return nil, errors.New("streaming converter: entries must belong to same stream")
		}
	}

	// Create Parquet writer with base schema (streaming converter doesn't support projections)
	schema := BuildParquetSchema(nil)
	writer := NewWriter(schema)
	recordBatch := make([]Record, 0, cfg.BatchSize)
	currentOffset := entries[0].StartOffset

	// Process each WAL entry's chunks incrementally
	for _, entry := range entries {
		rangeReader := NewStoreRangeReader(ctx, c.store, entry.WalPath)
		decoder, err := wal.NewStreamingDecoder(rangeReader)
		if err != nil {
			return nil, fmt.Errorf("streaming converter: creating decoder for %s: %w", entry.WalPath, err)
		}

		// Find the chunk(s) that match this entry
		for chunkIdx := 0; chunkIdx < decoder.ChunkCount(); chunkIdx++ {
			chunkInfo := decoder.ChunkIndex(chunkIdx)
			if chunkInfo == nil {
				continue
			}

			// Check if this chunk matches our index entry
			if chunkInfo.ChunkOffset != entry.ChunkOffset || chunkInfo.ChunkLength != entry.ChunkLength {
				continue
			}

			chunk, err := decoder.ReadChunk(chunkIdx)
			if err != nil {
				return nil, fmt.Errorf("streaming converter: reading chunk %d from %s: %w", chunkIdx, entry.WalPath, err)
			}

			// Process batches within the chunk
			for _, batch := range chunk.Batches {
				records, err := extractRecordsFromBatch(batch.Data, partition, currentOffset, nil)
				if err != nil {
					return nil, fmt.Errorf("streaming converter: extracting records at offset %d: %w", currentOffset, err)
				}

				for _, rec := range records {
					recordBatch = append(recordBatch, rec)
					currentOffset++

					// Flush batch when it reaches the configured size
					if len(recordBatch) >= cfg.BatchSize {
						if err := writer.WriteRecords(recordBatch); err != nil {
							return nil, fmt.Errorf("streaming converter: writing records: %w", err)
						}
						recordBatch = recordBatch[:0]
					}
				}
			}
		}
	}

	// Flush any remaining records
	if len(recordBatch) > 0 {
		if err := writer.WriteRecords(recordBatch); err != nil {
			return nil, fmt.Errorf("streaming converter: writing final records: %w", err)
		}
	}

	// Close the writer and get the result
	parquetData, stats, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("streaming converter: closing writer: %w", err)
	}

	return &ConvertResult{
		ParquetData: parquetData,
		Stats:       stats,
		RecordCount: stats.RecordCount,
	}, nil
}

// StreamingConvertWAL reads a WAL file and converts it to Parquet in a streaming fashion.
// This is useful for converting entire WAL files without loading all data into memory.
func (c *StreamingConverter) StreamingConvertWAL(ctx context.Context, walPath string, partition int32, startOffset int64, cfg StreamingConvertConfig) (*ConvertResult, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}

	rangeReader := NewStoreRangeReader(ctx, c.store, walPath)
	decoder, err := wal.NewStreamingDecoder(rangeReader)
	if err != nil {
		return nil, fmt.Errorf("streaming converter: creating decoder for %s: %w", walPath, err)
	}

	if decoder.ChunkCount() == 0 {
		return nil, errors.New("streaming converter: WAL has no chunks")
	}

	schema := BuildParquetSchema(nil)
	writer := NewWriter(schema)
	recordBatch := make([]Record, 0, cfg.BatchSize)
	currentOffset := startOffset

	// Process each chunk in the WAL
	for chunkIdx := 0; chunkIdx < decoder.ChunkCount(); chunkIdx++ {
		chunk, err := decoder.ReadChunk(chunkIdx)
		if err != nil {
			return nil, fmt.Errorf("streaming converter: reading chunk %d: %w", chunkIdx, err)
		}

		for _, batch := range chunk.Batches {
			records, err := extractRecordsFromBatch(batch.Data, partition, currentOffset, nil)
			if err != nil {
				return nil, fmt.Errorf("streaming converter: extracting records at offset %d: %w", currentOffset, err)
			}

			for _, rec := range records {
				recordBatch = append(recordBatch, rec)
				currentOffset++

				if len(recordBatch) >= cfg.BatchSize {
					if err := writer.WriteRecords(recordBatch); err != nil {
						return nil, fmt.Errorf("streaming converter: writing records: %w", err)
					}
					recordBatch = recordBatch[:0]
				}
			}
		}
	}

	if len(recordBatch) > 0 {
		if err := writer.WriteRecords(recordBatch); err != nil {
			return nil, fmt.Errorf("streaming converter: writing final records: %w", err)
		}
	}

	parquetData, stats, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("streaming converter: closing writer: %w", err)
	}

	return &ConvertResult{
		ParquetData: parquetData,
		Stats:       stats,
		RecordCount: stats.RecordCount,
	}, nil
}

// StoreRangeReader adapts objectstore.Store to wal.RangeReader interface.
// It provides byte-range reads from object storage for streaming WAL decoding.
type StoreRangeReader struct {
	ctx   context.Context
	store objectstore.Store
	key   string
	mu    sync.Mutex
}

// NewStoreRangeReader creates a new range reader backed by object storage.
func NewStoreRangeReader(ctx context.Context, store objectstore.Store, key string) *StoreRangeReader {
	return &StoreRangeReader{
		ctx:   ctx,
		store: store,
		key:   key,
	}
}

// ReadRange implements wal.RangeReader for object storage.
func (r *StoreRangeReader) ReadRange(start, end int64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rc, err := r.store.GetRange(r.ctx, r.key, start, end)
	if err != nil {
		return nil, fmt.Errorf("store range read [%d:%d]: %w", start, end, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading range data: %w", err)
	}

	return data, nil
}

// StreamingConvertWALFromBytes converts WAL data to Parquet in a streaming fashion.
// This is a convenience function for when WAL bytes are already in memory
// but you want to process them in streaming mode for consistent behavior.
func StreamingConvertWALFromBytes(walData []byte, partition int32, startOffset int64, cfg StreamingConvertConfig) (*ConvertResult, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}

	rangeReader := wal.NewBytesRangeReader(walData)
	decoder, err := wal.NewStreamingDecoder(rangeReader)
	if err != nil {
		return nil, fmt.Errorf("streaming converter: creating decoder: %w", err)
	}

	if decoder.ChunkCount() == 0 {
		return nil, errors.New("streaming converter: WAL has no chunks")
	}

	schema := BuildParquetSchema(nil)
	writer := NewWriter(schema)
	recordBatch := make([]Record, 0, cfg.BatchSize)
	currentOffset := startOffset

	for chunkIdx := 0; chunkIdx < decoder.ChunkCount(); chunkIdx++ {
		chunk, err := decoder.ReadChunk(chunkIdx)
		if err != nil {
			return nil, fmt.Errorf("streaming converter: reading chunk %d: %w", chunkIdx, err)
		}

		for _, batch := range chunk.Batches {
			records, err := extractRecordsFromBatch(batch.Data, partition, currentOffset, nil)
			if err != nil {
				return nil, fmt.Errorf("streaming converter: extracting records at offset %d: %w", currentOffset, err)
			}

			for _, rec := range records {
				recordBatch = append(recordBatch, rec)
				currentOffset++

				if len(recordBatch) >= cfg.BatchSize {
					if err := writer.WriteRecords(recordBatch); err != nil {
						return nil, fmt.Errorf("streaming converter: writing records: %w", err)
					}
					recordBatch = recordBatch[:0]
				}
			}
		}
	}

	if len(recordBatch) > 0 {
		if err := writer.WriteRecords(recordBatch); err != nil {
			return nil, fmt.Errorf("streaming converter: writing final records: %w", err)
		}
	}

	parquetData, stats, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("streaming converter: closing writer: %w", err)
	}

	return &ConvertResult{
		ParquetData: parquetData,
		Stats:       stats,
		RecordCount: stats.RecordCount,
	}, nil
}
