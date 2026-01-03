package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/parquet-go/parquet-go"
)

const (
	compressionNone   = 0
	compressionGzip   = 1
	compressionSnappy = 2
	compressionLz4    = 3
	compressionZstd   = 4
)

// FindOffsetByTimestamp scans a WAL entry to find the first offset with timestamp >= requested.
func (r *WALReader) FindOffsetByTimestamp(ctx context.Context, entry *index.IndexEntry, timestamp int64) (int64, int64, bool, error) {
	if entry.FileType != index.FileTypeWAL {
		return -1, -1, false, fmt.Errorf("fetch: expected WAL entry, got %s", entry.FileType)
	}

	startByte := int64(entry.ChunkOffset)
	endByte := startByte + int64(entry.ChunkLength) - 1
	chunkData, err := r.readChunk(ctx, entry.WalPath, startByte, endByte, int(entry.ChunkLength))
	if err != nil {
		return -1, -1, false, err
	}

	offset := 0
	baseOffset := entry.StartOffset

	for offset < len(chunkData) {
		if offset+4 > len(chunkData) {
			return -1, -1, false, fmt.Errorf("%w: missing batch length", ErrInvalidChunk)
		}
		batchLen := int(binary.BigEndian.Uint32(chunkData[offset : offset+4]))
		offset += 4

		if batchLen <= 0 {
			return -1, -1, false, fmt.Errorf("%w: invalid batch length %d", ErrInvalidChunk, batchLen)
		}
		if offset+batchLen > len(chunkData) {
			return -1, -1, false, fmt.Errorf("%w: batch truncated at offset %d", ErrInvalidChunk, offset)
		}

		batchData := chunkData[offset : offset+batchLen]
		recordCount := r.getRecordCount(batchData)
		if recordCount <= 0 {
			return -1, -1, false, fmt.Errorf("%w: invalid record count", ErrInvalidChunk)
		}

		recOffset, recTs, found, err := findRecordByTimestamp(batchData, baseOffset, timestamp)
		if err != nil {
			return -1, -1, false, err
		}
		if found {
			return recOffset, recTs, true, nil
		}

		baseOffset += recordCount
		offset += batchLen
	}

	return -1, -1, false, nil
}

// FindOffsetByTimestamp scans a Parquet entry to find the first offset with timestamp >= requested.
// It uses row group min/max timestamp stats to prune row groups that cannot contain the target timestamp.
func (r *ParquetReader) FindOffsetByTimestamp(ctx context.Context, entry *index.IndexEntry, timestamp int64) (int64, int64, bool, error) {
	if entry.FileType != index.FileTypeParquet {
		return -1, -1, false, fmt.Errorf("fetch: expected Parquet entry, got %s", entry.FileType)
	}

	meta, err := r.store.Head(ctx, entry.ParquetPath)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return -1, -1, false, fmt.Errorf("%w: %s", ErrParquetNotFound, entry.ParquetPath)
		}
		return -1, -1, false, fmt.Errorf("fetch: reading parquet metadata: %w", err)
	}

	if meta.Size == 0 {
		return -1, -1, false, ErrInvalidParquet
	}

	readerAt := newObjectStoreReaderAt(ctx, r.store, entry.ParquetPath, meta.Size)
	parquetFile, err := parquet.OpenFile(readerAt, meta.Size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return -1, -1, false, fmt.Errorf("%w: %v", ErrInvalidParquet, err)
	}

	rowGroups := selectRowGroupsByTimestamp(parquetFile, timestamp)
	if len(rowGroups) == 0 {
		return -1, -1, false, nil
	}

	var (
		found     bool
		bestOff   int64
		bestTs    int64
		readBatch = make([]ParquetRecordWithHeaders, 128)
	)

	for _, rowGroup := range rowGroups {
		groupReader := parquet.NewGenericRowGroupReader[ParquetRecordWithHeaders](rowGroup)

		for {
			n, err := groupReader.Read(readBatch)
			if n > 0 {
				for _, rec := range readBatch[:n] {
					if rec.Offset < entry.StartOffset || rec.Offset >= entry.EndOffset {
						continue
					}
					if rec.Timestamp >= timestamp {
						if !found || rec.Offset < bestOff {
							bestOff = rec.Offset
							bestTs = rec.Timestamp
							found = true
						}
					}
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				groupReader.Close()
				return -1, -1, false, fmt.Errorf("fetch: parsing parquet records: %w", err)
			}
		}

		if err := groupReader.Close(); err != nil {
			return -1, -1, false, fmt.Errorf("fetch: closing row group reader: %w", err)
		}

		// If we found a match in this row group, we can stop.
		// Row groups are assumed to be in timestamp order, so once we find the first
		// offset >= timestamp, any subsequent matches in other row groups would have
		// higher offsets (which we don't want - we want the minimum offset).
		if found {
			break
		}
	}

	if !found {
		return -1, -1, false, nil
	}

	return bestOff, bestTs, true, nil
}

// selectRowGroupsByTimestamp selects row groups that could contain records with timestamp >= target.
// It uses the min/max timestamp column stats to prune row groups.
func selectRowGroupsByTimestamp(file *parquet.File, timestamp int64) []parquet.RowGroup {
	timestampColumn, ok := file.Schema().Lookup("timestamp_ms")
	rowGroups := file.RowGroups()
	if !ok {
		return rowGroups
	}

	columnIndex := timestampColumn.ColumnIndex
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
		maxTs := maxValue.Int64()
		// Skip row groups where max timestamp < target (no records can match)
		if maxTs < timestamp {
			continue
		}
		selected = append(selected, rowGroup)
	}

	return selected
}

func findRecordByTimestamp(batchData []byte, baseOffset int64, timestamp int64) (int64, int64, bool, error) {
	if len(batchData) < 61 {
		return -1, -1, false, fmt.Errorf("%w: batch too small", ErrInvalidChunk)
	}

	firstTimestamp := int64(binary.BigEndian.Uint64(batchData[27:35]))
	maxTimestamp := int64(binary.BigEndian.Uint64(batchData[35:43]))
	if timestamp <= firstTimestamp {
		return baseOffset, firstTimestamp, true, nil
	}
	if timestamp > maxTimestamp {
		return -1, -1, false, nil
	}

	attributes := int16(binary.BigEndian.Uint16(batchData[21:23]))
	compressionType := int(attributes & 0x07)

	recordsData := batchData[61:]
	if compressionType != compressionNone {
		decompressed, err := decompressRecords(recordsData, compressionType)
		if err != nil {
			return -1, -1, false, fmt.Errorf("fetch: decompressing records: %w", err)
		}
		recordsData = decompressed
	}

	recordCount := int(binary.BigEndian.Uint32(batchData[57:61]))
	if recordCount <= 0 {
		return -1, -1, false, fmt.Errorf("%w: invalid record count", ErrInvalidChunk)
	}

	pos := 0
	for i := 0; i < recordCount; i++ {
		if pos >= len(recordsData) {
			return -1, -1, false, fmt.Errorf("%w: unexpected end of records", ErrInvalidChunk)
		}

		recordLen, bytesRead := readVarint(recordsData[pos:])
		if bytesRead <= 0 {
			return -1, -1, false, fmt.Errorf("%w: failed to read record length", ErrInvalidChunk)
		}
		pos += bytesRead
		if recordLen < 0 {
			return -1, -1, false, fmt.Errorf("%w: negative record length", ErrInvalidChunk)
		}

		recordStart := pos
		if pos >= len(recordsData) {
			return -1, -1, false, fmt.Errorf("%w: record truncated", ErrInvalidChunk)
		}
		pos++ // attributes

		tsDelta, bytesRead := readVarint(recordsData[pos:])
		if bytesRead <= 0 {
			return -1, -1, false, fmt.Errorf("%w: failed to read timestamp delta", ErrInvalidChunk)
		}
		pos += bytesRead

		offsetDelta, bytesRead := readVarint(recordsData[pos:])
		if bytesRead <= 0 {
			return -1, -1, false, fmt.Errorf("%w: failed to read offset delta", ErrInvalidChunk)
		}
		pos += bytesRead
		if offsetDelta < 0 {
			return -1, -1, false, fmt.Errorf("%w: negative offset delta", ErrInvalidChunk)
		}

		recTimestamp := firstTimestamp + tsDelta
		if recTimestamp >= timestamp {
			return baseOffset + offsetDelta, recTimestamp, true, nil
		}

		remaining := int(recordLen) - (pos - recordStart)
		if remaining < 0 {
			return -1, -1, false, fmt.Errorf("%w: record length mismatch", ErrInvalidChunk)
		}
		if pos+remaining > len(recordsData) {
			return -1, -1, false, fmt.Errorf("%w: record truncated", ErrInvalidChunk)
		}
		pos += remaining
	}

	return -1, -1, false, nil
}

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

// readVarint reads a zigzag-encoded signed varint from the data.
func readVarint(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

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

	v := int64((uv >> 1) ^ -(uv & 1))
	return v, bytesRead
}
