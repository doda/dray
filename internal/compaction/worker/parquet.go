// Package worker converts WAL entries to Parquet and performs index swap.
package worker

import (
	"bytes"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

// Record represents a single Kafka record in the Parquet schema.
// This matches SPEC section 5.3 - Compacted Parquet schema.
// Column names must match the Iceberg schema in catalog/schema.go.
// Field IDs must match the Iceberg field IDs for proper column mapping.
type Record struct {
	Partition     int32    `parquet:"partition"`
	Offset        int64    `parquet:"offset"`
	Timestamp     int64    `parquet:"timestamp,timestamp(millisecond)"`
	Key           []byte   `parquet:"key,optional"`
	Value         []byte   `parquet:"value,optional"`
	Headers       []Header `parquet:"headers,list,optional"`
	ProducerID    *int64   `parquet:"producer_id,optional"`
	ProducerEpoch *int32   `parquet:"producer_epoch,optional"`
	BaseSequence  *int32   `parquet:"base_sequence,optional"`
	Attributes    int32    `parquet:"attributes"`
	RecordCRC     *int32   `parquet:"record_crc,optional"`
}

// Header represents a single Kafka record header.
// SPEC 5.3: list<struct<key:string, value:binary>> - ordered list preserving duplicates.
type Header struct {
	Key   string `parquet:"key"`
	Value []byte `parquet:"value,optional"`
}

// FileStats contains min/max statistics from a Parquet file.
type FileStats struct {
	MinOffset    int64
	MaxOffset    int64
	MinTimestamp int64
	MaxTimestamp int64
	RecordCount  int64
	SizeBytes    int64
}

// Writer writes Kafka records to Parquet format.
type Writer struct {
	buf        bytes.Buffer
	writer     *parquet.GenericWriter[Record]
	stats      FileStats
	firstWrite bool
}

// NewWriter creates a new Parquet writer.
func NewWriter() *Writer {
	return &Writer{
		firstWrite: true,
	}
}

// WriteRecords writes a slice of records to the Parquet file.
// Records should be provided in offset order for a single partition.
func (w *Writer) WriteRecords(records []Record) error {
	if len(records) == 0 {
		return nil
	}

	// Initialize writer on first call
	if w.writer == nil {
		w.writer = parquet.NewGenericWriter[Record](&w.buf)
	}

	// Write records
	n, err := w.writer.Write(records)
	if err != nil {
		return fmt.Errorf("parquet: write records: %w", err)
	}
	if n != len(records) {
		return fmt.Errorf("parquet: wrote %d of %d records", n, len(records))
	}

	// Update stats
	for _, rec := range records {
		if w.firstWrite {
			w.stats.MinOffset = rec.Offset
			w.stats.MaxOffset = rec.Offset
			w.stats.MinTimestamp = rec.Timestamp
			w.stats.MaxTimestamp = rec.Timestamp
			w.firstWrite = false
		} else {
			if rec.Offset < w.stats.MinOffset {
				w.stats.MinOffset = rec.Offset
			}
			if rec.Offset > w.stats.MaxOffset {
				w.stats.MaxOffset = rec.Offset
			}
			if rec.Timestamp < w.stats.MinTimestamp {
				w.stats.MinTimestamp = rec.Timestamp
			}
			if rec.Timestamp > w.stats.MaxTimestamp {
				w.stats.MaxTimestamp = rec.Timestamp
			}
		}
		w.stats.RecordCount++
	}

	return nil
}

// Close finalizes the Parquet file and returns its contents.
func (w *Writer) Close() ([]byte, FileStats, error) {
	if w.writer == nil {
		return nil, FileStats{}, fmt.Errorf("parquet: no records written")
	}

	if err := w.writer.Close(); err != nil {
		return nil, FileStats{}, fmt.Errorf("parquet: close: %w", err)
	}

	data := w.buf.Bytes()
	w.stats.SizeBytes = int64(len(data))

	return data, w.stats, nil
}

// GenerateParquetID generates a unique ID for a Parquet file.
func GenerateParquetID() string {
	return uuid.New().String()
}

// GenerateParquetPath generates the object storage path for a Parquet file.
// Format: s3://<bucket>/<prefix>/compaction/v1/topic=<topic>/partition=<p>/date=YYYY/MM/DD/<parquetId>.parquet
// This function returns only the relative path portion.
func GenerateParquetPath(topic string, partition int32, date string, parquetID string) string {
	return fmt.Sprintf("compaction/v1/topic=%s/partition=%d/date=%s/%s.parquet", topic, partition, date, parquetID)
}

// WriteToBuffer writes records to a buffer and returns the Parquet data.
// This is a convenience function for simple use cases.
func WriteToBuffer(records []Record) ([]byte, FileStats, error) {
	w := NewWriter()
	if err := w.WriteRecords(records); err != nil {
		return nil, FileStats{}, err
	}
	return w.Close()
}

// Reader reads records from a Parquet file.
type Reader struct {
	reader *parquet.GenericReader[Record]
}

// NewReader creates a new Parquet reader from the given data.
func NewReader(data []byte) (*Reader, error) {
	file := newBytesFile(data)
	reader := parquet.NewGenericReader[Record](file)
	return &Reader{
		reader: reader,
	}, nil
}

// NumRows returns the number of rows in the Parquet file.
func (r *Reader) NumRows() int64 {
	return r.reader.NumRows()
}

// ReadAll reads all records from the Parquet file.
func (r *Reader) ReadAll() ([]Record, error) {
	numRows := r.NumRows()
	if numRows == 0 {
		return nil, nil
	}

	if numRows > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("parquet: row count %d exceeds int capacity", numRows)
	}
	records := make([]Record, int(numRows))
	n, err := r.reader.Read(records)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("parquet: read records: %w", err)
	}
	return records[:n], nil
}

// Close closes the reader.
func (r *Reader) Close() error {
	return r.reader.Close()
}

// bytesFile implements parquet.File for reading from an in-memory byte slice.
type bytesFile struct {
	data []byte
}

func newBytesFile(data []byte) *bytesFile {
	return &bytesFile{data: data}
}

func (f *bytesFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *bytesFile) Size() int64 {
	return int64(len(f.data))
}
