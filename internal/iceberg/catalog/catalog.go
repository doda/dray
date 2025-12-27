// Package catalog defines the Iceberg catalog interface for stream-table duality.
//
// This package provides the abstraction layer for Iceberg catalog operations
// required by Dray's compaction service. It supports the following use cases:
//
//   - Loading existing tables for append operations during compaction
//   - Creating tables when new topics are created (duality mode)
//   - Appending Parquet data files after compaction
//   - Retrieving current snapshot for consistency checks
//
// The interface is designed to work with the Iceberg REST catalog spec as the
// primary target, with optional adapters for AWS Glue and other catalogs.
//
// See SPEC.md section 11.8 for detailed requirements.
package catalog

import (
	"context"
	"errors"
)

// Common errors returned by catalog operations.
var (
	// ErrTableNotFound is returned when a table does not exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrTableAlreadyExists is returned when attempting to create a table that exists.
	ErrTableAlreadyExists = errors.New("table already exists")

	// ErrSnapshotNotFound is returned when no snapshot exists for a table.
	ErrSnapshotNotFound = errors.New("snapshot not found")

	// ErrCommitConflict is returned when a commit fails due to concurrent modification.
	ErrCommitConflict = errors.New("commit conflict")

	// ErrCatalogUnavailable is returned when the catalog service is unreachable.
	ErrCatalogUnavailable = errors.New("catalog unavailable")
)

// FileFormat represents the format of a data file.
type FileFormat string

const (
	// FormatParquet represents Parquet file format.
	FormatParquet FileFormat = "PARQUET"

	// FormatAvro represents Avro file format.
	FormatAvro FileFormat = "AVRO"

	// FormatORC represents ORC file format.
	FormatORC FileFormat = "ORC"
)

// DataFile represents a data file in an Iceberg table.
// This corresponds to the Parquet files produced by Dray's compaction service.
//
// The schema for Dray data files is defined in SPEC.md section 5.3:
//   - partition (int32): Kafka partition ID
//   - offset (int64): Kafka offset
//   - timestamp_ms (int64): Record timestamp
//   - key (binary): Record key
//   - value (binary): Record value
//   - headers (list<struct<key:string,value:binary>>): Record headers
//   - producer_id (int64, nullable): For future idempotent producer support
//   - producer_epoch (int16, nullable): For future idempotent producer support
//   - base_sequence (int32, nullable): For future idempotent producer support
//   - attributes (int): Record batch attributes
type DataFile struct {
	// Path is the full object storage path to the data file.
	// Example: s3://bucket/prefix/compaction/v1/topic=foo/partition=0/date=2025/01/15/abc123.parquet
	Path string

	// Format is the file format (always PARQUET for Dray).
	Format FileFormat

	// PartitionValue is the partition identity value for this file.
	// For Dray, this is the Kafka partition ID.
	PartitionValue int32

	// RecordCount is the number of records in the file.
	RecordCount int64

	// FileSizeBytes is the file size in bytes.
	FileSizeBytes int64

	// ColumnSizes maps field IDs to their total size in bytes.
	ColumnSizes map[int32]int64

	// ValueCounts maps field IDs to their non-null value counts.
	ValueCounts map[int32]int64

	// NullValueCounts maps field IDs to their null value counts.
	NullValueCounts map[int32]int64

	// LowerBounds maps field IDs to their lower bound values.
	// For offset column, this is the minimum offset in the file.
	LowerBounds map[int32][]byte

	// UpperBounds maps field IDs to their upper bound values.
	// For offset column, this is the maximum offset in the file.
	UpperBounds map[int32][]byte

	// SplitOffsets contains the byte offsets of split points for parallel reads.
	SplitOffsets []int64

	// SortOrderID references the sort order, if the file is sorted.
	SortOrderID *int32
}

// SnapshotOperation describes the type of operation that created a snapshot.
type SnapshotOperation string

const (
	// OpAppend indicates data files were appended.
	OpAppend SnapshotOperation = "append"

	// OpReplace indicates data files were replaced.
	OpReplace SnapshotOperation = "replace"

	// OpOverwrite indicates data was overwritten.
	OpOverwrite SnapshotOperation = "overwrite"

	// OpDelete indicates data files were deleted.
	OpDelete SnapshotOperation = "delete"
)

// Snapshot represents an Iceberg table snapshot.
// A snapshot captures the state of a table at a point in time.
type Snapshot struct {
	// SnapshotID is the unique identifier for this snapshot.
	SnapshotID int64

	// ParentSnapshotID is the ID of the parent snapshot, or nil for the first snapshot.
	ParentSnapshotID *int64

	// SequenceNumber is the sequence number of this snapshot within the table.
	SequenceNumber int64

	// TimestampMs is the timestamp when this snapshot was created.
	TimestampMs int64

	// ManifestListPath is the path to the manifest list file.
	ManifestListPath string

	// Operation describes what type of operation created this snapshot.
	Operation SnapshotOperation

	// Summary contains snapshot summary properties.
	// Common keys include:
	//   - "operation": the operation type
	//   - "added-data-files": number of data files added
	//   - "added-records": number of records added
	//   - "total-records": total record count in table
	//   - "total-data-files": total data file count
	Summary map[string]string
}

// Schema represents an Iceberg table schema.
type Schema struct {
	// SchemaID is the unique identifier for this schema version.
	SchemaID int32

	// Fields describes the columns in the schema.
	Fields []Field
}

// Field represents a column in an Iceberg schema.
type Field struct {
	// ID is the unique field identifier.
	ID int32

	// Name is the column name.
	Name string

	// Type is the Iceberg type string (e.g., "long", "binary", "string").
	Type string

	// Required indicates whether the field is required (non-nullable).
	Required bool

	// Doc is an optional documentation string.
	Doc string
}

// PartitionSpec defines how table data is partitioned.
type PartitionSpec struct {
	// SpecID is the unique identifier for this partition spec.
	SpecID int32

	// Fields defines the partition fields.
	Fields []PartitionField
}

// PartitionField describes a partition transformation.
type PartitionField struct {
	// SourceID is the source field ID.
	SourceID int32

	// FieldID is this partition field's ID.
	FieldID int32

	// Name is the partition field name.
	Name string

	// Transform is the transformation (e.g., "identity", "day", "bucket[N]").
	Transform string
}

// TableProperties contains table configuration properties.
// Dray uses these to store topic metadata.
type TableProperties map[string]string

// Dray-specific table property keys.
const (
	// PropertyDrayTopic is the Kafka topic name associated with this table.
	PropertyDrayTopic = "dray.topic"

	// PropertyDrayClusterID is the Dray cluster identifier.
	PropertyDrayClusterID = "dray.cluster_id"

	// PropertyDraySchemaVersion tracks the Dray schema version.
	PropertyDraySchemaVersion = "dray.schema_version"
)

// CreateTableOptions contains options for table creation.
type CreateTableOptions struct {
	// Schema is the table schema.
	Schema Schema

	// PartitionSpec defines the partition strategy.
	// Default for Dray: partition by "partition" (identity).
	PartitionSpec *PartitionSpec

	// Properties are initial table properties.
	Properties TableProperties

	// Location is the table location in object storage.
	// If empty, the catalog determines the location.
	Location string
}

// AppendFilesOptions contains options for appending data files.
type AppendFilesOptions struct {
	// SnapshotProperties are additional properties to set on the new snapshot.
	// Useful for tracking commit IDs for idempotent retries.
	SnapshotProperties map[string]string

	// ExpectedSnapshotID, if set, requires the current snapshot to match.
	// This enables optimistic concurrency control.
	ExpectedSnapshotID *int64
}

// Table represents an Iceberg table with operations needed by Dray.
type Table interface {
	// Identifier returns the table identifier (namespace + name).
	Identifier() TableIdentifier

	// Schema returns the current table schema.
	Schema() Schema

	// CurrentSnapshot returns the current snapshot, or nil if the table is empty.
	// Returns ErrSnapshotNotFound if the table has no snapshots.
	CurrentSnapshot(ctx context.Context) (*Snapshot, error)

	// Snapshots returns all snapshots for the table.
	Snapshots(ctx context.Context) ([]Snapshot, error)

	// AppendFiles appends data files to the table, creating a new snapshot.
	// This is the primary operation used by Dray's compaction service.
	// Returns the new snapshot on success.
	AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error)

	// Properties returns the table properties.
	Properties() TableProperties

	// Location returns the table's base location in object storage.
	Location() string

	// Refresh reloads the table metadata from the catalog.
	Refresh(ctx context.Context) error
}

// TableIdentifier uniquely identifies a table within a catalog.
type TableIdentifier struct {
	// Namespace is the table namespace (e.g., "dray", "production.dray").
	Namespace []string

	// Name is the table name (typically the Kafka topic name).
	Name string
}

// String returns the fully qualified table name.
func (t TableIdentifier) String() string {
	if len(t.Namespace) == 0 {
		return t.Name
	}
	result := ""
	for i, ns := range t.Namespace {
		if i > 0 {
			result += "."
		}
		result += ns
	}
	return result + "." + t.Name
}

// Catalog is the interface for Iceberg catalog operations.
// This is the main entry point for Dray's Iceberg integration.
//
// Implementations:
//   - RestCatalog: Iceberg REST catalog (primary target)
//   - GlueCatalog: AWS Glue catalog (via REST endpoint)
//
// Thread Safety:
//
//	Catalog implementations must be safe for concurrent use.
//
// Error Handling:
//
//	All methods return ErrCatalogUnavailable if the catalog service
//	cannot be reached. Callers should handle this gracefully, as
//	Dray's produce/fetch operations must remain available even when
//	the Iceberg catalog is down (per SPEC.md section 11.2).
type Catalog interface {
	// LoadTable loads a table by identifier.
	// Returns ErrTableNotFound if the table does not exist.
	LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error)

	// CreateTableIfMissing creates a table if it doesn't exist, or returns the existing table.
	// This is used when creating Kafka topics in duality mode.
	// Returns the table (existing or newly created).
	CreateTableIfMissing(ctx context.Context, identifier TableIdentifier, opts CreateTableOptions) (Table, error)

	// GetCurrentSnapshot returns the current snapshot for a table.
	// This is a convenience method that loads the table and returns its current snapshot.
	// Returns ErrTableNotFound if the table does not exist.
	// Returns ErrSnapshotNotFound if the table has no snapshots.
	GetCurrentSnapshot(ctx context.Context, identifier TableIdentifier) (*Snapshot, error)

	// AppendDataFiles appends data files to a table.
	// This is a convenience method that loads the table and appends files.
	// Returns ErrTableNotFound if the table does not exist.
	// Returns ErrCommitConflict if the commit fails due to concurrent modification.
	AppendDataFiles(ctx context.Context, identifier TableIdentifier, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error)

	// DropTable drops a table.
	// Returns ErrTableNotFound if the table does not exist.
	DropTable(ctx context.Context, identifier TableIdentifier) error

	// ListTables lists all tables in a namespace.
	ListTables(ctx context.Context, namespace []string) ([]TableIdentifier, error)

	// TableExists checks if a table exists.
	TableExists(ctx context.Context, identifier TableIdentifier) (bool, error)

	// Close releases resources held by the catalog.
	Close() error
}
