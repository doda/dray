package catalog

import (
	"encoding/json"

	"github.com/apache/iceberg-go"
	"github.com/dray-io/dray/internal/projection"
)

// Dray schema field IDs. These are stable and should not change.
const (
	FieldIDPartition     = 1
	FieldIDOffset        = 2
	FieldIDTimestamp     = 3
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

const (
	ProjectionFieldIDBase   = 100
	ProjectionFieldIDStride = 2
)

// DefaultSchema returns the base Dray table schema as defined in SPEC.md section 5.3.
func DefaultSchema() *iceberg.Schema {
	return SchemaWithProjections(nil)
}

// SchemaWithProjections builds a schema with optional projected columns.
func SchemaWithProjections(fields []projection.FieldSpec) *iceberg.Schema {
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

	nested := []iceberg.NestedField{
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
			ID:       FieldIDTimestamp,
			Name:     "timestamp",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
			Doc:      "Kafka record timestamp",
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
	}

	for i, field := range fields {
		if field.Name == "" {
			continue
		}
		fieldID := ProjectionFieldIDBase + (i * ProjectionFieldIDStride)
		nested = append(nested, projectionField(field, fieldID))
	}

	return iceberg.NewSchema(0, nested...)
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

// NameMappingForSchema returns the JSON name mapping for the provided schema.
// This is only needed when Parquet files do not include Iceberg field IDs.
// The name mapping tells Iceberg how to map column names to field IDs.
func NameMappingForSchema(schema *iceberg.Schema) string {
	if schema == nil {
		return ""
	}
	mapping := schema.NameMapping()
	payload, err := json.Marshal(mapping)
	if err != nil {
		return ""
	}
	return string(payload)
}

// DefaultNameMapping returns the JSON name mapping for the default schema.
func DefaultNameMapping() string {
	return NameMappingForSchema(DefaultSchema())
}

// TablePropertiesForSchema returns the table properties for a given schema.
func TablePropertiesForSchema(topicName, clusterID string, schema *iceberg.Schema) TableProperties {
	return TableProperties{
		PropertyDrayTopic:         topicName,
		PropertyDrayClusterID:     clusterID,
		PropertyDraySchemaVersion: "2",
	}
}

// DefaultTableProperties returns the default table properties for a Dray table.
func DefaultTableProperties(topicName, clusterID string) TableProperties {
	return TablePropertiesForSchema(topicName, clusterID, DefaultSchema())
}

func projectionField(field projection.FieldSpec, fieldID int) iceberg.NestedField {
	switch field.Type {
	case projection.FieldTypeString:
		return iceberg.NestedField{ID: fieldID, Name: field.Name, Type: iceberg.PrimitiveTypes.String, Required: false}
	case projection.FieldTypeInt32:
		return iceberg.NestedField{ID: fieldID, Name: field.Name, Type: iceberg.PrimitiveTypes.Int32, Required: false}
	case projection.FieldTypeInt64:
		return iceberg.NestedField{ID: fieldID, Name: field.Name, Type: iceberg.PrimitiveTypes.Int64, Required: false}
	case projection.FieldTypeBool:
		return iceberg.NestedField{ID: fieldID, Name: field.Name, Type: iceberg.PrimitiveTypes.Bool, Required: false}
	case projection.FieldTypeTimestampMs:
		return iceberg.NestedField{ID: fieldID, Name: field.Name, Type: iceberg.PrimitiveTypes.TimestampTz, Required: false}
	case projection.FieldTypeStringList:
		return iceberg.NestedField{
			ID:       fieldID,
			Name:     field.Name,
			Type:     &iceberg.ListType{ElementID: fieldID + 1, Element: iceberg.PrimitiveTypes.String, ElementRequired: false},
			Required: false,
		}
	case projection.FieldTypeInt64List:
		return iceberg.NestedField{
			ID:       fieldID,
			Name:     field.Name,
			Type:     &iceberg.ListType{ElementID: fieldID + 1, Element: iceberg.PrimitiveTypes.Int64, ElementRequired: false},
			Required: false,
		}
	case projection.FieldTypeBoolList:
		return iceberg.NestedField{
			ID:       fieldID,
			Name:     field.Name,
			Type:     &iceberg.ListType{ElementID: fieldID + 1, Element: iceberg.PrimitiveTypes.Bool, ElementRequired: false},
			Required: false,
		}
	default:
		return iceberg.NestedField{ID: fieldID, Name: field.Name, Type: iceberg.PrimitiveTypes.String, Required: false}
	}
}
