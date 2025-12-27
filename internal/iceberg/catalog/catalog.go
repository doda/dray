// Package catalog defines the Iceberg catalog interface.
package catalog

import "context"

// Snapshot represents an Iceberg table snapshot.
type Snapshot struct {
	SnapshotID int64
	ParentID   *int64
	Timestamp  int64
	Summary    map[string]string
}

// DataFile represents a data file in an Iceberg table.
type DataFile struct {
	Path        string
	Format      string
	RecordCount int64
	FileSizeBytes int64
	LowerBounds map[int32][]byte
	UpperBounds map[int32][]byte
}

// Table represents an Iceberg table.
type Table interface {
	// Name returns the table name.
	Name() string

	// CurrentSnapshot returns the current snapshot.
	CurrentSnapshot(ctx context.Context) (*Snapshot, error)

	// AppendFiles appends data files to the table.
	AppendFiles(ctx context.Context, files []DataFile) error
}

// Catalog is the interface for Iceberg catalog operations.
type Catalog interface {
	// LoadTable loads a table by name.
	LoadTable(ctx context.Context, name string) (Table, error)

	// CreateTable creates a new table if it doesn't exist.
	CreateTableIfNotExists(ctx context.Context, name string, schema interface{}) (Table, error)

	// DropTable drops a table.
	DropTable(ctx context.Context, name string) error

	// Close releases resources.
	Close() error
}
