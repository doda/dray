package catalog

import (
	"bytes"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// Iceberg Avro schemas for Manifest List and Manifest File.
// These follow the Iceberg v2 spec.

const manifestListSchema = `{
  "type": "record",
  "name": "manifest_list",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_data_files_count", "type": "int", "field-id": 504, "default": 0},
    {"name": "existing_data_files_count", "type": "int", "field-id": 505, "default": 0},
    {"name": "deleted_data_files_count", "type": "int", "field-id": 506, "default": 0},
    {"name": "added_rows_count", "type": "long", "field-id": 507, "default": 0},
    {"name": "existing_rows_count", "type": "long", "field-id": 508, "default": 0},
    {"name": "deleted_rows_count", "type": "long", "field-id": 509, "default": 0},
    {"name": "partitions", "type": ["null", {
      "type": "array",
      "items": {
        "type": "record",
        "name": "field_summary",
        "fields": [
          {"name": "contains_null", "type": "boolean", "field-id": 510},
          {"name": "contains_nan", "type": ["null", "boolean"], "field-id": 511, "default": null},
          {"name": "lower_bound", "type": ["null", "bytes"], "field-id": 512, "default": null},
          {"name": "upper_bound", "type": ["null", "bytes"], "field-id": 513, "default": null}
        ]
      }
    }], "field-id": 510, "default": null},
    {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 519, "default": null},
    {"name": "content", "type": "int", "field-id": 517, "default": 0},
    {"name": "sequence_number", "type": "long", "field-id": 518, "default": 0}
  ]
}`

const manifestFileSchema = `{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int", "field-id": 0},
    {"name": "snapshot_id", "type": ["null", "long"], "field-id": 1},
    {"name": "sequence_number", "type": ["null", "long"], "field-id": 3, "default": null},
    {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4, "default": null},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "data_file",
      "fields": [
        {"name": "content", "type": "int", "field-id": 134, "default": 0},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "partition", "type": {
          "type": "record",
          "name": "partition_data",
          "fields": [
            {"name": "partition", "type": "int", "field-id": 1000}
          ]
        }, "field-id": 102},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
        {"name": "column_sizes", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "column_size", "fields": [
            {"name": "key", "type": "int", "field-id": 117},
            {"name": "value", "type": "long", "field-id": 118}
          ]
        }}], "field-id": 108, "default": null},
        {"name": "value_counts", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "value_count", "fields": [
            {"name": "key", "type": "int", "field-id": 119},
            {"name": "value", "type": "long", "field-id": 120}
          ]
        }}], "field-id": 109, "default": null},
        {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "null_value_count", "fields": [
            {"name": "key", "type": "int", "field-id": 121},
            {"name": "value", "type": "long", "field-id": 122}
          ]
        }}], "field-id": 110, "default": null},
        {"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "nan_value_count", "fields": [
            {"name": "key", "type": "int", "field-id": 123},
            {"name": "value", "type": "long", "field-id": 124}
          ]
        }}], "field-id": 137, "default": null},
        {"name": "lower_bounds", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "lower_bound", "fields": [
            {"name": "key", "type": "int", "field-id": 126},
            {"name": "value", "type": "bytes", "field-id": 127}
          ]
        }}], "field-id": 125, "default": null},
        {"name": "upper_bounds", "type": ["null", {"type": "array", "items": {
          "type": "record", "name": "upper_bound", "fields": [
            {"name": "key", "type": "int", "field-id": 129},
            {"name": "value", "type": "bytes", "field-id": 130}
          ]
        }}], "field-id": 128, "default": null},
        {"name": "key_metadata", "type": ["null", "bytes"], "field-id": 131, "default": null},
        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "field-id": 132, "default": null},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "field-id": 135, "default": null},
        {"name": "sort_order_id", "type": ["null", "int"], "field-id": 140, "default": null}
      ]
    }, "field-id": 2}
  ]
}`

type AvroManifestEntry struct {
	Status             int32        `avro:"status"`
	SnapshotID         *int64       `avro:"snapshot_id"`
	SequenceNumber     *int64       `avro:"sequence_number"`
	FileSequenceNumber *int64       `avro:"file_sequence_number"`
	DataFile           AvroDataFile `avro:"data_file"`
}

type AvroDataFile struct {
	Content            int32           `avro:"content"`
	FilePath           string          `avro:"file_path"`
	FileFormat         string          `avro:"file_format"`
	Partition          AvroPartition   `avro:"partition"`
	RecordCount        int64           `avro:"record_count"`
	FileSizeInBytes    int64           `avro:"file_size_in_bytes"`
	ColumnSizes        *[]AvroKeyValue `avro:"column_sizes"`
	ValueCounts        *[]AvroKeyValue `avro:"value_counts"`
	NullValueCounts    *[]AvroKeyValue `avro:"null_value_counts"`
	NanValueCounts     *[]AvroKeyValue `avro:"nan_value_counts"`
	LowerBounds        *[]AvroKeyBytes `avro:"lower_bounds"`
	UpperBounds        *[]AvroKeyBytes `avro:"upper_bounds"`
	KeyMetadata        *[]byte         `avro:"key_metadata"`
	SplitOffsets       *[]int64        `avro:"split_offsets"`
	EqualityIDs        *[]int32        `avro:"equality_ids"`
	SortOrderID        *int32          `avro:"sort_order_id"`
}

type AvroPartition struct {
	Partition int32 `avro:"partition"`
}

type AvroKeyValue struct {
	Key   int32 `avro:"key"`
	Value int64 `avro:"value"`
}

type AvroKeyBytes struct {
	Key   int32  `avro:"key"`
	Value []byte `avro:"value"`
}

type AvroManifestListEntry struct {
	ManifestPath           string               `avro:"manifest_path"`
	ManifestLength         int64                `avro:"manifest_length"`
	PartitionSpecID        int32                `avro:"partition_spec_id"`
	AddedSnapshotID        int64                `avro:"added_snapshot_id"`
	AddedDataFilesCount    int32                `avro:"added_data_files_count"`
	ExistingDataFilesCount int32                `avro:"existing_data_files_count"`
	DeletedDataFilesCount  int32                `avro:"deleted_data_files_count"`
	AddedRowsCount         int64                `avro:"added_rows_count"`
	ExistingRowsCount      int64                `avro:"existing_rows_count"`
	DeletedRowsCount       int64                `avro:"deleted_rows_count"`
	Partitions             *[]AvroFieldSummary  `avro:"partitions"`
	KeyMetadata            *[]byte              `avro:"key_metadata"`
	Content                int32                `avro:"content"`
	SequenceNumber         int64                `avro:"sequence_number"`
}

type AvroFieldSummary struct {
	ContainsNull bool    `avro:"contains_null"`
	ContainsNan  *bool   `avro:"contains_nan"`
	LowerBound   *[]byte `avro:"lower_bound"`
	UpperBound   *[]byte `avro:"upper_bound"`
}

// writeManifestFile encodes data files into an Iceberg manifest file (Avro OCF).
func writeManifestFile(files []DataFile, snapshotID int64, seqNum int64) ([]byte, error) {
	schema, err := avro.Parse(manifestFileSchema)
	if err != nil {
		return nil, fmt.Errorf("parsing manifest file schema: %w", err)
	}

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema.String(), buf)
	if err != nil {
		return nil, fmt.Errorf("creating ocf encoder: %w", err)
	}

	for _, f := range files {
		entry := AvroManifestEntry{
			Status:             1, // ADDED
			SnapshotID:         &snapshotID,
			SequenceNumber:     &seqNum,
			FileSequenceNumber: &seqNum,
			DataFile:           toAvroDataFile(f),
		}
		if err := enc.Encode(entry); err != nil {
			return nil, fmt.Errorf("encoding manifest entry: %w", err)
		}
	}

	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("closing ocf encoder: %w", err)
	}

	return buf.Bytes(), nil
}

// writeManifestList encodes manifest list entries into an Iceberg manifest list file (Avro OCF).
func writeManifestList(entries []AvroManifestListEntry) ([]byte, error) {
	schema, err := avro.Parse(manifestListSchema)
	if err != nil {
		return nil, fmt.Errorf("parsing manifest list schema: %w", err)
	}

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema.String(), buf)
	if err != nil {
		return nil, fmt.Errorf("creating ocf encoder: %w", err)
	}

	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return nil, fmt.Errorf("encoding manifest list entry: %w", err)
		}
	}

	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("closing ocf encoder: %w", err)
	}

	return buf.Bytes(), nil
}

func toAvroDataFile(f DataFile) AvroDataFile {
	adf := AvroDataFile{
		Content:         0,
		FilePath:        f.Path,
		FileFormat:      string(f.Format),
		Partition:       AvroPartition{Partition: f.PartitionValue},
		RecordCount:     f.RecordCount,
		FileSizeInBytes: f.FileSizeBytes,
		SortOrderID:     f.SortOrderID,
	}

	if len(f.ColumnSizes) > 0 {
		kv := make([]AvroKeyValue, 0, len(f.ColumnSizes))
		for k, v := range f.ColumnSizes {
			kv = append(kv, AvroKeyValue{Key: k, Value: v})
		}
		adf.ColumnSizes = &kv
	}

	if len(f.ValueCounts) > 0 {
		kv := make([]AvroKeyValue, 0, len(f.ValueCounts))
		for k, v := range f.ValueCounts {
			kv = append(kv, AvroKeyValue{Key: k, Value: v})
		}
		adf.ValueCounts = &kv
	}

	if len(f.NullValueCounts) > 0 {
		kv := make([]AvroKeyValue, 0, len(f.NullValueCounts))
		for k, v := range f.NullValueCounts {
			kv = append(kv, AvroKeyValue{Key: k, Value: v})
		}
		adf.NullValueCounts = &kv
	}

	if len(f.LowerBounds) > 0 {
		kb := make([]AvroKeyBytes, 0, len(f.LowerBounds))
		for k, v := range f.LowerBounds {
			kb = append(kb, AvroKeyBytes{Key: k, Value: v})
		}
		adf.LowerBounds = &kb
	}

	if len(f.UpperBounds) > 0 {
		kb := make([]AvroKeyBytes, 0, len(f.UpperBounds))
		for k, v := range f.UpperBounds {
			kb = append(kb, AvroKeyBytes{Key: k, Value: v})
		}
		adf.UpperBounds = &kb
	}

	if len(f.SplitOffsets) > 0 {
		adf.SplitOffsets = &f.SplitOffsets
	}

	return adf
}
