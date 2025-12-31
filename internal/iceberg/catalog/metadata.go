package catalog

import (
	"encoding/base64"
	"strconv"
)

// TableMetadata represents the Iceberg table metadata stored in JSON format.
// This follows the Iceberg Table Metadata spec (v1/v2).
type TableMetadata struct {
	FormatVersion      int                `json:"format-version"`
	TableUUID          string             `json:"table-uuid"`
	Location           string             `json:"location"`
	LastUpdatedMs      int64              `json:"last-updated-ms"`
	LastColumnID       int32              `json:"last-column-id"`
	Schemas            []IcebergSchema    `json:"schemas"`
	CurrentSchemaID    int32              `json:"current-schema-id"`
	PartitionSpecs     []IcebergPartitionSpec `json:"partition-specs"`
	DefaultSpecID      int32              `json:"default-spec-id"`
	LastPartitionID    int32              `json:"last-partition-id"`
	Properties         map[string]string  `json:"properties,omitempty"`
	CurrentSnapshotID  *int64             `json:"current-snapshot-id,omitempty"`
	Snapshots          []IcebergSnapshot  `json:"snapshots,omitempty"`
	SnapshotLog        []SnapshotLogEntry `json:"snapshot-log,omitempty"`
	SortOrders         []IcebergSortOrder `json:"sort-orders,omitempty"`
	DefaultSortOrderID int32              `json:"default-sort-order-id"`
}

type IcebergSchema struct {
	Type               string         `json:"type"`
	SchemaID           int32          `json:"schema-id"`
	Fields             []IcebergField `json:"fields"`
	IdentifierFieldIDs []int32        `json:"identifier-field-ids,omitempty"`
}

type IcebergField struct {
	ID       int32  `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
	Doc      string `json:"doc,omitempty"`
}

type IcebergPartitionSpec struct {
	SpecID int32                  `json:"spec-id"`
	Fields []IcebergPartitionField `json:"fields"`
}

type IcebergPartitionField struct {
	SourceID  int32  `json:"source-id"`
	FieldID   int32  `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

type IcebergSnapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list,omitempty"`
	Summary          map[string]string `json:"summary,omitempty"`
	SchemaID         *int32            `json:"schema-id,omitempty"`
}

type SnapshotLogEntry struct {
	TimestampMs int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

type IcebergSortOrder struct {
	OrderID int32             `json:"order-id"`
	Fields  []IcebergSortField `json:"fields"`
}

type IcebergSortField struct {
	Transform string `json:"transform"`
	SourceID  int32  `json:"source-id"`
	Direction string `json:"direction"`
	NullOrder string `json:"null-order"`
}

// IcebergDataFile represents a data file in the Iceberg metadata (manifest entries).
type IcebergDataFile struct {
	// Content type: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
	Content int `json:"content"`

	// FilePath is the full path to the data file
	FilePath string `json:"file-path"`

	// FileFormat: AVRO, ORC, PARQUET
	FileFormat string `json:"file-format"`

	// PartitionData for identity partitions
	Partition map[string]interface{} `json:"partition,omitempty"`

	// RecordCount is the number of records in the file
	RecordCount int64 `json:"record-count"`

	// FileSizeInBytes is the total file size
	FileSizeInBytes int64 `json:"file-size-in-bytes"`

	// ColumnSizes maps field ID to column size in bytes
	ColumnSizes map[string]int64 `json:"column-sizes,omitempty"`

	// ValueCounts maps field ID to value count
	ValueCounts map[string]int64 `json:"value-counts,omitempty"`

	// NullValueCounts maps field ID to null value count
	NullValueCounts map[string]int64 `json:"null-value-counts,omitempty"`

	// NanValueCounts maps field ID to NaN value count
	NanValueCounts map[string]int64 `json:"nan-value-counts,omitempty"`

	// LowerBounds maps field ID to lower bound (base64 encoded)
	LowerBounds map[string]string `json:"lower-bounds,omitempty"`

	// UpperBounds maps field ID to upper bound (base64 encoded)
	UpperBounds map[string]string `json:"upper-bounds,omitempty"`

	// SplitOffsets for efficient parallel reads
	SplitOffsets []int64 `json:"split-offsets,omitempty"`

	// SortOrderID if the file is sorted
	SortOrderID *int32 `json:"sort-order-id,omitempty"`

	// SpecID is the partition spec ID
	SpecID int32 `json:"spec-id,omitempty"`

	// SequenceNumber for Iceberg v2
	SequenceNumber *int64 `json:"sequence-number,omitempty"`
}

// Conversion functions

func schemaToIceberg(s Schema) IcebergSchema {
	fields := make([]IcebergField, len(s.Fields))
	for i, f := range s.Fields {
		fields[i] = IcebergField{
			ID:       f.ID,
			Name:     f.Name,
			Required: f.Required,
			Type:     f.Type,
			Doc:      f.Doc,
		}
	}
	return IcebergSchema{
		Type:     "struct",
		SchemaID: s.SchemaID,
		Fields:   fields,
	}
}

func schemaFromIceberg(s IcebergSchema) Schema {
	fields := make([]Field, len(s.Fields))
	for i, f := range s.Fields {
		fields[i] = Field{
			ID:       f.ID,
			Name:     f.Name,
			Required: f.Required,
			Type:     f.Type,
			Doc:      f.Doc,
		}
	}
	return Schema{
		SchemaID: s.SchemaID,
		Fields:   fields,
	}
}

func partitionSpecToIceberg(spec *PartitionSpec) *IcebergPartitionSpec {
	if spec == nil {
		return nil
	}
	fields := make([]IcebergPartitionField, len(spec.Fields))
	for i, f := range spec.Fields {
		fields[i] = IcebergPartitionField{
			SourceID:  f.SourceID,
			FieldID:   f.FieldID,
			Name:      f.Name,
			Transform: f.Transform,
		}
	}
	return &IcebergPartitionSpec{
		SpecID: spec.SpecID,
		Fields: fields,
	}
}

func snapshotFromIceberg(s IcebergSnapshot) *Snapshot {
	op := SnapshotOperation(OpAppend)
	if s.Summary != nil {
		if opStr, ok := s.Summary["operation"]; ok {
			op = SnapshotOperation(opStr)
		}
	}
	return &Snapshot{
		SnapshotID:       s.SnapshotID,
		ParentSnapshotID: s.ParentSnapshotID,
		SequenceNumber:   s.SequenceNumber,
		TimestampMs:      s.TimestampMs,
		ManifestListPath: s.ManifestList,
		Operation:        op,
		Summary:          s.Summary,
	}
}

// dataFileToIceberg converts a DataFile to the Iceberg representation.
func dataFileToIceberg(f DataFile, specID int32, seqNum int64) IcebergDataFile {
	api := IcebergDataFile{
		Content:         0, // DATA file type
		FilePath:        f.Path,
		FileFormat:      string(f.Format),
		RecordCount:     f.RecordCount,
		FileSizeInBytes: f.FileSizeBytes,
		SpecID:          specID,
		SequenceNumber:  &seqNum,
		SplitOffsets:    f.SplitOffsets,
		SortOrderID:     f.SortOrderID,
	}

	// Always set the partition value - partition 0 is valid for Kafka
	api.Partition = map[string]interface{}{
		"partition": f.PartitionValue,
	}

	// Convert column sizes (field ID -> size)
	if len(f.ColumnSizes) > 0 {
		api.ColumnSizes = make(map[string]int64, len(f.ColumnSizes))
		for k, v := range f.ColumnSizes {
			api.ColumnSizes[strconv.FormatInt(int64(k), 10)] = v
		}
	}

	// Convert value counts
	if len(f.ValueCounts) > 0 {
		api.ValueCounts = make(map[string]int64, len(f.ValueCounts))
		for k, v := range f.ValueCounts {
			api.ValueCounts[strconv.FormatInt(int64(k), 10)] = v
		}
	}

	// Convert null value counts
	if len(f.NullValueCounts) > 0 {
		api.NullValueCounts = make(map[string]int64, len(f.NullValueCounts))
		for k, v := range f.NullValueCounts {
			api.NullValueCounts[strconv.FormatInt(int64(k), 10)] = v
		}
	}

	// Convert lower bounds (base64 encode)
	if len(f.LowerBounds) > 0 {
		api.LowerBounds = make(map[string]string, len(f.LowerBounds))
		for k, v := range f.LowerBounds {
			api.LowerBounds[strconv.FormatInt(int64(k), 10)] = base64.StdEncoding.EncodeToString(v)
		}
	}

	// Convert upper bounds (base64 encode)
	if len(f.UpperBounds) > 0 {
		api.UpperBounds = make(map[string]string, len(f.UpperBounds))
		for k, v := range f.UpperBounds {
			api.UpperBounds[strconv.FormatInt(int64(k), 10)] = base64.StdEncoding.EncodeToString(v)
		}
	}

	return api
}
