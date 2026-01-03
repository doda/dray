package catalog

import (
	"encoding/json"

	"github.com/apache/iceberg-go"
)

// Dray schema field IDs. These are stable and should not change.
const (
	FieldIDPartition     = 1
	FieldIDOffset        = 2
	FieldIDTimestampMs   = 3
	FieldIDKey           = 4
	FieldIDValue         = 5
	FieldIDHeaders       = 6
	FieldIDProducerID    = 7
	FieldIDProducerEpoch = 8
	FieldIDBaseSequence  = 9
	FieldIDAttributes    = 10
	FieldIDRecordCRC     = 14

	FieldIDHeadersElement = 11
	FieldIDHeaderKey      = 12
	FieldIDHeaderValue    = 13
)

// DefaultSchema returns the default Dray table schema as defined in SPEC.md section 5.3.
func DefaultSchema() *iceberg.Schema {
	headersType := &iceberg.ListType{
		ElementID: FieldIDHeadersElement,
		Element: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{
					ID:       FieldIDHeaderKey,
					Name:     "key",
					Type:     iceberg.PrimitiveTypes.String,
					Required: true,
				},
				{
					ID:       FieldIDHeaderValue,
					Name:     "value",
					Type:     iceberg.PrimitiveTypes.Binary,
					Required: false,
				},
			},
		},
		ElementRequired: false,
	}

	return iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       FieldIDPartition,
			Name:     "partition",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: true,
			Doc:      "Kafka partition ID",
		},
		iceberg.NestedField{
			ID:       FieldIDOffset,
			Name:     "offset",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
			Doc:      "Kafka offset",
		},
		iceberg.NestedField{
			ID:       FieldIDTimestampMs,
			Name:     "timestamp_ms",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
			Doc:      "Record timestamp in milliseconds (UTC)",
		},
		iceberg.NestedField{
			ID:       FieldIDKey,
			Name:     "key",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: false,
			Doc:      "Record key",
		},
		iceberg.NestedField{
			ID:       FieldIDValue,
			Name:     "value",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: false,
			Doc:      "Record value",
		},
		iceberg.NestedField{
			ID:       FieldIDHeaders,
			Name:     "headers",
			Type:     headersType,
			Required: false,
			Doc:      "Record headers",
		},
		iceberg.NestedField{
			ID:       FieldIDProducerID,
			Name:     "producer_id",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: false,
			Doc:      "Producer ID for idempotent producers (future use)",
		},
		iceberg.NestedField{
			ID:       FieldIDProducerEpoch,
			Name:     "producer_epoch",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: false,
			Doc:      "Producer epoch for idempotent producers (future use)",
		},
		iceberg.NestedField{
			ID:       FieldIDBaseSequence,
			Name:     "base_sequence",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: false,
			Doc:      "Base sequence for idempotent producers (future use)",
		},
		iceberg.NestedField{
			ID:       FieldIDAttributes,
			Name:     "attributes",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: true,
			Doc:      "Record batch attributes",
		},
		iceberg.NestedField{
			ID:       FieldIDRecordCRC,
			Name:     "record_crc",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: false,
			Doc:      "Record CRC32C for debugging/validation (optional)",
		},
	)
}

// DefaultPartitionSpec returns the default partition spec for Dray tables.
// Per SPEC.md section 5.3, the default is to partition by the "partition" field (identity).
func DefaultPartitionSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceID:  FieldIDPartition,
		FieldID:   1000, // Partition field IDs start at 1000 per Iceberg spec
		Name:      "partition",
		Transform: iceberg.IdentityTransform{},
	})
}

// DefaultNameMapping returns the JSON name mapping for Iceberg tables.
// This is required because parquet-go doesn't write Iceberg field IDs to parquet files.
// The name mapping tells Iceberg how to map column names to field IDs.
func DefaultNameMapping() string {
	mapping := DefaultSchema().NameMapping()
	payload, err := json.Marshal(mapping)
	if err != nil {
		return ""
	}
	return string(payload)
}

// DefaultTableProperties returns the default table properties for a Dray table.
func DefaultTableProperties(topicName, clusterID string) TableProperties {
	return TableProperties{
		PropertyDrayTopic:            topicName,
		PropertyDrayClusterID:        clusterID,
		PropertyDraySchemaVersion:    "1",
		PropertySchemaNameMappingDef: DefaultNameMapping(),
	}
}
