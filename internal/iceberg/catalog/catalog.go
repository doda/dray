// Package catalog defines the Iceberg catalog interface for stream-table duality.
//
// This package provides the abstraction layer for Iceberg catalog operations
// required by Dray's compaction service. It uses apache/iceberg-go for catalog
// and table operations, while keeping a minimal interface for Dray components.
//
// See SPEC.md section 11.8 for detailed requirements.
package catalog

import (
	"context"
	"errors"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
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

	// ErrOperationNotSupported is returned when a catalog implementation lacks an operation.
	ErrOperationNotSupported = errors.New("operation not supported")
)

// DataFile represents a data file in an Iceberg table.
// This corresponds to the Parquet files produced by Dray's compaction service.
type DataFile struct {
	// Path is the full object storage path to the data file.
	// Example: s3://bucket/prefix/compaction/v1/topic=foo/partition=0/date=2025/01/15/abc123.parquet
	Path string

	// RecordCount is the number of records in the file.
	RecordCount int64

	// FileSizeBytes is the file size in bytes.
	FileSizeBytes int64

	// PartitionValue is the Kafka partition ID for this file.
	PartitionValue int32

	// LowerBounds maps field IDs to their lower bound values.
	// Used for query pruning in Iceberg.
	LowerBounds map[int][]byte

	// UpperBounds maps field IDs to their upper bound values.
	// Used for query pruning in Iceberg.
	UpperBounds map[int][]byte

	// NullValueCounts maps field IDs to their null value counts.
	NullValueCounts map[int]int64

	// ValueCounts maps field IDs to their non-null value counts.
	ValueCounts map[int]int64

	// ColumnSizes maps field IDs to their total size in bytes.
	ColumnSizes map[int]int64
}

// TableProperties contains table configuration properties.
// Dray uses these to store topic metadata.
type TableProperties = iceberg.Properties

// Dray-specific table property keys.
const (
	// PropertyDrayTopic is the Kafka topic name associated with this table.
	PropertyDrayTopic = "dray.topic"

	// PropertyDrayClusterID is the Dray cluster identifier.
	PropertyDrayClusterID = "dray.cluster_id"

	// PropertyDraySchemaVersion tracks the Dray schema version.
	PropertyDraySchemaVersion = "dray.schema_version"

	// PropertySchemaNameMappingDef is the Iceberg name mapping for files without field IDs.
	// This is required because parquet-go doesn't write Iceberg field IDs to parquet files.
	PropertySchemaNameMappingDef = "schema.name-mapping.default"
)

// CreateTableOptions contains options for table creation.
type CreateTableOptions struct {
	// Schema is the table schema.
	Schema *iceberg.Schema

	// PartitionSpec defines the partition strategy.
	// Default for Dray: partition by "partition" (identity).
	PartitionSpec *iceberg.PartitionSpec

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
	SnapshotProperties iceberg.Properties

	// ExpectedSnapshotID, if set, requires the current snapshot to match.
	// This enables optimistic concurrency control.
	ExpectedSnapshotID *int64
}

// ReplaceFilesOptions contains options for replacing files in a table.
type ReplaceFilesOptions struct {
	// SnapshotProperties are additional properties to set on the new snapshot.
	SnapshotProperties iceberg.Properties

	// ExpectedSnapshotID, if set, requires the current snapshot to match.
	ExpectedSnapshotID *int64
}

// Table represents an Iceberg table with operations needed by Dray.
type Table interface {
	// Identifier returns the table identifier (namespace + name).
	Identifier() TableIdentifier

	// Schema returns the current table schema.
	Schema() *iceberg.Schema

	// CurrentSnapshot returns the current snapshot, or nil if the table is empty.
	// Returns ErrSnapshotNotFound if the table has no snapshots.
	CurrentSnapshot(ctx context.Context) (*table.Snapshot, error)

	// Snapshots returns all snapshots for the table.
	Snapshots(ctx context.Context) ([]table.Snapshot, error)

	// AppendFiles appends data files to the table, creating a new snapshot.
	AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*table.Snapshot, error)

	// ReplaceFiles replaces existing files with new files, creating a new snapshot.
	ReplaceFiles(ctx context.Context, added []DataFile, removed []DataFile, opts *ReplaceFilesOptions) (*table.Snapshot, error)

	// Properties returns the table properties.
	Properties() TableProperties

	// Location returns the table's base location in object storage.
	Location() string

	// Refresh reloads the table metadata from the catalog.
	Refresh(ctx context.Context) error
}

// TableIdentifier uniquely identifies a table within a catalog.
type TableIdentifier = table.Identifier

// NewTableIdentifier builds a TableIdentifier from namespace and table name.
func NewTableIdentifier(namespace []string, name string) TableIdentifier {
	identifier := make([]string, 0, len(namespace)+1)
	identifier = append(identifier, namespace...)
	identifier = append(identifier, name)
	return identifier
}

// TableIdentifierString returns a dot-separated string for a table identifier.
func TableIdentifierString(identifier TableIdentifier) string {
	return strings.Join(identifier, ".")
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
	GetCurrentSnapshot(ctx context.Context, identifier TableIdentifier) (*table.Snapshot, error)

	// AppendDataFiles appends data files to a table.
	// This is a convenience method that loads the table and appends files.
	// Returns ErrTableNotFound if the table does not exist.
	// Returns ErrCommitConflict if the commit fails due to concurrent modification.
	AppendDataFiles(ctx context.Context, identifier TableIdentifier, files []DataFile, opts *AppendFilesOptions) (*table.Snapshot, error)

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
