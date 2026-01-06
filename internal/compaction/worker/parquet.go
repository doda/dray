// Package worker converts WAL entries to Parquet and performs index swap.
package worker

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/projection"
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
	Timestamp     int64    `parquet:"timestamp,timestamp(microsecond)"`
	Key           []byte   `parquet:"key,optional"`
	Value         []byte   `parquet:"value,optional"`
	Headers       []Header `parquet:"headers,list,optional"`
	ProducerID    *int64   `parquet:"producer_id,optional"`
	ProducerEpoch *int32   `parquet:"producer_epoch,optional"`
	BaseSequence  *int32   `parquet:"base_sequence,optional"`
	Attributes    int32    `parquet:"attributes"`
	RecordCRC     *int32   `parquet:"record_crc,optional"`

	// Projected columns derived from the value payload.
	Projected map[string]any `parquet:"-"`
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

const parquetTimestampScale = int64(1000)

// Writer writes Kafka records to Parquet format.
type Writer struct {
	buf        bytes.Buffer
	writer     *parquet.Writer
	schema     *parquet.Schema
	stats      FileStats
	firstWrite bool
}

// NewWriter creates a new Parquet writer.
func NewWriter(schema *parquet.Schema) *Writer {
	return &Writer{
		firstWrite: true,
		schema:     schema,
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
		if w.schema == nil {
			return fmt.Errorf("parquet: schema is required")
		}
		w.writer = parquet.NewWriter(&w.buf, w.schema)
	}

	rows := make([]parquet.Row, 0, len(records))
	for _, rec := range records {
		row := w.schema.Deconstruct(nil, recordToMap(rec))
		rows = append(rows, row)
	}

	// Write records
	n, err := w.writer.WriteRows(rows)
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
// WriteToBuffer writes records to a buffer with the provided schema.
func WriteToBuffer(schema *parquet.Schema, records []Record) ([]byte, FileStats, error) {
	w := NewWriter(schema)
	if err := w.WriteRecords(records); err != nil {
		return nil, FileStats{}, err
	}
	return w.Close()
}

func recordToMap(rec Record) map[string]any {
	row := map[string]any{
		"partition":      rec.Partition,
		"offset":         rec.Offset,
		"timestamp":      scaleParquetTimestamp(rec.Timestamp),
		"key":            optionalBytes(rec.Key),
		"value":          optionalBytes(rec.Value),
		"headers":        headersToMap(rec.Headers),
		"producer_id":    derefInt64(rec.ProducerID),
		"producer_epoch": derefInt32(rec.ProducerEpoch),
		"base_sequence":  derefInt32(rec.BaseSequence),
		"attributes":     rec.Attributes,
		"record_crc":     derefInt32(rec.RecordCRC),
	}
	for key, value := range rec.Projected {
		row[key] = scaleProjectedTimestampValue(key, value)
	}
	return row
}

func optionalBytes(value []byte) any {
	if value == nil {
		return nil
	}
	return value
}

func scaleParquetTimestamp(value int64) int64 {
	return value * parquetTimestampScale
}

func normalizeParquetTimestamp(value int64) int64 {
	return value / parquetTimestampScale
}

func scaleProjectedTimestampValue(name string, value any) any {
	if !strings.HasSuffix(name, "_at") {
		return value
	}
	switch v := value.(type) {
	case int64:
		return v * parquetTimestampScale
	case float64:
		return v * float64(parquetTimestampScale)
	default:
		return value
	}
}

func derefInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func derefInt32(value *int32) any {
	if value == nil {
		return nil
	}
	return *value
}

func headersToMap(headers []Header) []map[string]any {
	if len(headers) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(headers))
	for _, header := range headers {
		out = append(out, map[string]any{
			"key":   header.Key,
			"value": header.Value,
		})
	}
	return out
}

// BuildParquetSchema returns a Parquet schema for the base record plus projections.
func BuildParquetSchema(fields []projection.FieldSpec) *parquet.Schema {
	base := parquet.Group{
		"partition":      parquet.FieldID(parquet.Leaf(parquet.Int32Type), catalog.FieldIDPartition),
		"offset":         parquet.FieldID(parquet.Leaf(parquet.Int64Type), catalog.FieldIDOffset),
		"timestamp":      parquet.FieldID(parquet.Timestamp(parquet.Microsecond), catalog.FieldIDTimestamp),
		"key":            parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), catalog.FieldIDKey),
		"value":          parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), catalog.FieldIDValue),
		"producer_id":    parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int64Type)), catalog.FieldIDProducerID),
		"producer_epoch": parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), catalog.FieldIDProducerEpoch),
		"base_sequence":  parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), catalog.FieldIDBaseSequence),
		"attributes":     parquet.FieldID(parquet.Leaf(parquet.Int32Type), catalog.FieldIDAttributes),
		"record_crc":     parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), catalog.FieldIDRecordCRC),
	}

	headersElement := parquet.FieldID(parquet.Group{
		"key":   parquet.FieldID(parquet.String(), catalog.FieldIDHeaderKey),
		"value": parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), catalog.FieldIDHeaderValue),
	}, catalog.FieldIDHeadersElement)
	headersNode := parquet.FieldID(parquet.Optional(parquet.List(headersElement)), catalog.FieldIDHeaders)
	base["headers"] = headersNode

	for i, field := range fields {
		if field.Name == "" {
			continue
		}
		fieldID := catalog.ProjectionFieldIDBase + (i * catalog.ProjectionFieldIDStride)
		base[field.Name] = parquetNodeForField(field, fieldID)
	}

	return parquet.NewSchema("dray_record", base)
}

// BuildParquetSchemaFromIceberg returns a Parquet schema aligned to an Iceberg schema's field IDs.
func BuildParquetSchemaFromIceberg(schema *iceberg.Schema, fields []projection.FieldSpec) (*parquet.Schema, error) {
	if schema == nil {
		return nil, fmt.Errorf("parquet: iceberg schema is required")
	}

	partitionField, err := fieldByName(schema, "partition")
	if err != nil {
		return nil, err
	}
	offsetField, err := fieldByName(schema, "offset")
	if err != nil {
		return nil, err
	}
	timestampField, err := fieldByName(schema, "timestamp")
	if err != nil {
		return nil, err
	}
	keyField, err := fieldByName(schema, "key")
	if err != nil {
		return nil, err
	}
	valueField, err := fieldByName(schema, "value")
	if err != nil {
		return nil, err
	}
	headersField, err := fieldByName(schema, "headers")
	if err != nil {
		return nil, err
	}
	producerIDField, err := fieldByName(schema, "producer_id")
	if err != nil {
		return nil, err
	}
	producerEpochField, err := fieldByName(schema, "producer_epoch")
	if err != nil {
		return nil, err
	}
	baseSequenceField, err := fieldByName(schema, "base_sequence")
	if err != nil {
		return nil, err
	}
	attributesField, err := fieldByName(schema, "attributes")
	if err != nil {
		return nil, err
	}
	recordCRCField, err := fieldByName(schema, "record_crc")
	if err != nil {
		return nil, err
	}

	headersList, err := listTypeForField(headersField, "headers")
	if err != nil {
		return nil, err
	}
	headersStruct, err := structTypeForList(headersList, "headers")
	if err != nil {
		return nil, err
	}
	headerKeyID, err := structFieldID(headersStruct, "key", "headers")
	if err != nil {
		return nil, err
	}
	headerValueID, err := structFieldID(headersStruct, "value", "headers")
	if err != nil {
		return nil, err
	}

	base := parquet.Group{
		"attributes":     parquet.FieldID(parquet.Leaf(parquet.Int32Type), attributesField.ID),
		"base_sequence":  parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), baseSequenceField.ID),
		"key":            parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), keyField.ID),
		"offset":         parquet.FieldID(parquet.Leaf(parquet.Int64Type), offsetField.ID),
		"partition":      parquet.FieldID(parquet.Leaf(parquet.Int32Type), partitionField.ID),
		"producer_epoch": parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), producerEpochField.ID),
		"producer_id":    parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int64Type)), producerIDField.ID),
		"record_crc":     parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), recordCRCField.ID),
		"timestamp":      parquet.FieldID(parquet.Timestamp(parquet.Microsecond), timestampField.ID),
		"value":          parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), valueField.ID),
	}

	headersElement := parquet.FieldID(parquet.Group{
		"key":   parquet.FieldID(parquet.String(), headerKeyID),
		"value": parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), headerValueID),
	}, headersList.ElementID)
	headersNode := parquet.FieldID(parquet.Optional(parquet.List(headersElement)), headersField.ID)
	base["headers"] = headersNode

	for _, field := range fields {
		if field.Name == "" {
			continue
		}
		schemaField, err := fieldByName(schema, field.Name)
		if err != nil {
			return nil, err
		}
		elementID := 0
		if isListProjection(field.Type) {
			listType, err := listTypeForField(schemaField, field.Name)
			if err != nil {
				return nil, err
			}
			elementID = listType.ElementID
		}
		node, err := parquetNodeForFieldWithIDs(field, schemaField.ID, elementID)
		if err != nil {
			return nil, err
		}
		base[field.Name] = node
	}

	return parquet.NewSchema("dray_record", base), nil
}

func parquetNodeForField(field projection.FieldSpec, fieldID int) parquet.Node {
	switch field.Type {
	case projection.FieldTypeString:
		return parquet.FieldID(parquet.Optional(parquet.String()), fieldID)
	case projection.FieldTypeInt32:
		return parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), fieldID)
	case projection.FieldTypeInt64:
		return parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int64Type)), fieldID)
	case projection.FieldTypeBool:
		return parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.BooleanType)), fieldID)
	case projection.FieldTypeTimestampMs:
		return parquet.FieldID(parquet.Optional(parquet.Timestamp(parquet.Microsecond)), fieldID)
	case projection.FieldTypeStringList:
		return parquet.FieldID(parquet.Optional(parquet.List(parquet.FieldID(parquet.String(), fieldID+1))), fieldID)
	case projection.FieldTypeInt64List:
		return parquet.FieldID(parquet.Optional(parquet.List(parquet.FieldID(parquet.Leaf(parquet.Int64Type), fieldID+1))), fieldID)
	case projection.FieldTypeBoolList:
		return parquet.FieldID(parquet.Optional(parquet.List(parquet.FieldID(parquet.Leaf(parquet.BooleanType), fieldID+1))), fieldID)
	default:
		return parquet.FieldID(parquet.Optional(parquet.String()), fieldID)
	}
}

func parquetNodeForFieldWithIDs(field projection.FieldSpec, fieldID, elementID int) (parquet.Node, error) {
	switch field.Type {
	case projection.FieldTypeString:
		return parquet.FieldID(parquet.Optional(parquet.String()), fieldID), nil
	case projection.FieldTypeInt32:
		return parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int32Type)), fieldID), nil
	case projection.FieldTypeInt64:
		return parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int64Type)), fieldID), nil
	case projection.FieldTypeBool:
		return parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.BooleanType)), fieldID), nil
	case projection.FieldTypeTimestampMs:
		return parquet.FieldID(parquet.Optional(parquet.Timestamp(parquet.Microsecond)), fieldID), nil
	case projection.FieldTypeStringList:
		if elementID == 0 {
			return nil, fmt.Errorf("parquet: list field %q missing element id", field.Name)
		}
		return parquet.FieldID(parquet.Optional(parquet.List(parquet.FieldID(parquet.String(), elementID))), fieldID), nil
	case projection.FieldTypeInt64List:
		if elementID == 0 {
			return nil, fmt.Errorf("parquet: list field %q missing element id", field.Name)
		}
		return parquet.FieldID(parquet.Optional(parquet.List(parquet.FieldID(parquet.Leaf(parquet.Int64Type), elementID))), fieldID), nil
	case projection.FieldTypeBoolList:
		if elementID == 0 {
			return nil, fmt.Errorf("parquet: list field %q missing element id", field.Name)
		}
		return parquet.FieldID(parquet.Optional(parquet.List(parquet.FieldID(parquet.Leaf(parquet.BooleanType), elementID))), fieldID), nil
	default:
		return parquet.FieldID(parquet.Optional(parquet.String()), fieldID), nil
	}
}

func isListProjection(fieldType projection.FieldType) bool {
	switch fieldType {
	case projection.FieldTypeStringList, projection.FieldTypeInt64List, projection.FieldTypeBoolList:
		return true
	default:
		return false
	}
}

func fieldByName(schema *iceberg.Schema, name string) (iceberg.NestedField, error) {
	field, ok := schema.FindFieldByName(name)
	if !ok {
		return iceberg.NestedField{}, fmt.Errorf("parquet: iceberg schema missing field %q", name)
	}
	return field, nil
}

func listTypeForField(field iceberg.NestedField, name string) (*iceberg.ListType, error) {
	switch typed := field.Type.(type) {
	case *iceberg.ListType:
		return typed, nil
	default:
		return nil, fmt.Errorf("parquet: iceberg field %q is not list type", name)
	}
}

func structTypeForList(listType *iceberg.ListType, fieldName string) (*iceberg.StructType, error) {
	switch typed := listType.Element.(type) {
	case *iceberg.StructType:
		return typed, nil
	default:
		return nil, fmt.Errorf("parquet: iceberg field %q list element is not struct", fieldName)
	}
}

func structFieldID(structType *iceberg.StructType, fieldName, parentName string) (int, error) {
	for _, field := range structType.FieldList {
		if field.Name == fieldName {
			return field.ID, nil
		}
	}
	return 0, fmt.Errorf("parquet: iceberg field %q missing %q", parentName, fieldName)
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
	for i := 0; i < n; i++ {
		records[i].Timestamp = normalizeParquetTimestamp(records[i].Timestamp)
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
