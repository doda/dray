package catalog

// Iceberg type constants for Dray schema fields.
const (
	TypeInt    = "int"
	TypeLong   = "long"
	TypeBinary = "binary"
	TypeString = "string"
)

// Dray schema field IDs. These are stable and should not change.
const (
	FieldIDPartition     int32 = 1
	FieldIDOffset        int32 = 2
	FieldIDTimestampMs   int32 = 3
	FieldIDKey           int32 = 4
	FieldIDValue         int32 = 5
	FieldIDHeaders       int32 = 6
	FieldIDProducerID    int32 = 7
	FieldIDProducerEpoch int32 = 8
	FieldIDBaseSequence  int32 = 9
	FieldIDAttributes    int32 = 10
)

// DefaultSchema returns the default Dray table schema as defined in SPEC.md section 5.3.
//
// The schema contains:
//   - partition (int32): Kafka partition ID
//   - offset (int64): Kafka offset
//   - timestamp_ms (int64): Record timestamp
//   - key (binary): Record key (nullable)
//   - value (binary): Record value (nullable)
//   - headers (list<struct<key:string,value:binary>>): Record headers
//   - producer_id (int64, nullable): For future idempotent producer support
//   - producer_epoch (int16, nullable): For future idempotent producer support
//   - base_sequence (int32, nullable): For future idempotent producer support
//   - attributes (int): Record batch attributes
func DefaultSchema() Schema {
	return Schema{
		SchemaID: 0,
		Fields: []Field{
			{
				ID:       FieldIDPartition,
				Name:     "partition",
				Type:     TypeInt,
				Required: true,
				Doc:      "Kafka partition ID",
			},
			{
				ID:       FieldIDOffset,
				Name:     "offset",
				Type:     TypeLong,
				Required: true,
				Doc:      "Kafka offset",
			},
			{
				ID:       FieldIDTimestampMs,
				Name:     "timestamp_ms",
				Type:     TypeLong,
				Required: true,
				Doc:      "Record timestamp in milliseconds",
			},
			{
				ID:       FieldIDKey,
				Name:     "key",
				Type:     TypeBinary,
				Required: false,
				Doc:      "Record key",
			},
			{
				ID:       FieldIDValue,
				Name:     "value",
				Type:     TypeBinary,
				Required: false,
				Doc:      "Record value",
			},
			{
				ID:       FieldIDHeaders,
				Name:     "headers",
				Type:     "list<struct<key:string,value:binary>>",
				Required: false,
				Doc:      "Record headers as ordered list (duplicate keys allowed)",
			},
			{
				ID:       FieldIDProducerID,
				Name:     "producer_id",
				Type:     TypeLong,
				Required: false,
				Doc:      "Producer ID for idempotent producers (future use)",
			},
			{
				ID:       FieldIDProducerEpoch,
				Name:     "producer_epoch",
				Type:     TypeInt,
				Required: false,
				Doc:      "Producer epoch for idempotent producers (future use)",
			},
			{
				ID:       FieldIDBaseSequence,
				Name:     "base_sequence",
				Type:     TypeInt,
				Required: false,
				Doc:      "Base sequence for idempotent producers (future use)",
			},
			{
				ID:       FieldIDAttributes,
				Name:     "attributes",
				Type:     TypeInt,
				Required: true,
				Doc:      "Record batch attributes",
			},
		},
	}
}

// DefaultPartitionSpec returns the default partition spec for Dray tables.
// Per SPEC.md section 5.3, the default is to partition by the "partition" field (identity).
func DefaultPartitionSpec() PartitionSpec {
	return PartitionSpec{
		SpecID: 0,
		Fields: []PartitionField{
			{
				SourceID:  FieldIDPartition,
				FieldID:   1000, // Partition field IDs start at 1000 per Iceberg spec
				Name:      "partition",
				Transform: "identity",
			},
		},
	}
}

// DefaultTableProperties returns the default table properties for a Dray table.
func DefaultTableProperties(topicName, clusterID string) TableProperties {
	return TableProperties{
		PropertyDrayTopic:         topicName,
		PropertyDrayClusterID:     clusterID,
		PropertyDraySchemaVersion: "1",
	}
}
