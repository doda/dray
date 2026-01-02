package catalog

import (
	"context"
	"errors"

	"github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	_ "github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/catalog/sql"
)

// IcebergCatalog wraps an apache/iceberg-go catalog to satisfy Dray's Catalog interface.
type IcebergCatalog struct {
	cat icecatalog.Catalog
}

// CatalogConfig configures iceberg-go catalog loading.
type CatalogConfig struct {
	Type      string
	URI       string
	Warehouse string
	Props     iceberg.Properties
}

// LoadCatalog creates a new IcebergCatalog using iceberg-go catalog loading.
func LoadCatalog(ctx context.Context, cfg CatalogConfig) (*IcebergCatalog, error) {
	props := iceberg.Properties{}
	if cfg.Props != nil {
		for k, v := range cfg.Props {
			props[k] = v
		}
	}
	catalogType := cfg.Type
	if catalogType == "http" || catalogType == "https" {
		catalogType = "rest"
	}
	if catalogType != "" {
		props["type"] = catalogType
	}
	if cfg.URI != "" {
		props["uri"] = cfg.URI
	}
	if cfg.Warehouse != "" {
		props["warehouse"] = cfg.Warehouse
	}

	cat, err := icecatalog.Load(ctx, "dray", props)
	if err != nil {
		return nil, err
	}

	return &IcebergCatalog{cat: cat}, nil
}

// LoadTable loads a table by identifier.
func (c *IcebergCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	tbl, err := c.cat.LoadTable(ctx, identifier)
	if err != nil {
		return nil, mapCatalogError(err)
	}
	return &icebergTable{tbl: tbl}, nil
}

// CreateTableIfMissing creates a table if it doesn't exist, or returns the existing table.
func (c *IcebergCatalog) CreateTableIfMissing(ctx context.Context, identifier TableIdentifier, opts CreateTableOptions) (Table, error) {
	tbl, err := c.cat.CreateTable(ctx, identifier, opts.Schema,
		icecatalog.WithLocation(opts.Location),
		icecatalog.WithPartitionSpec(opts.PartitionSpec),
		icecatalog.WithProperties(opts.Properties),
	)
	if err == nil {
		return &icebergTable{tbl: tbl}, nil
	}
	if errors.Is(err, icecatalog.ErrTableAlreadyExists) {
		return c.LoadTable(ctx, identifier)
	}
	return nil, mapCatalogError(err)
}

// GetCurrentSnapshot returns the current snapshot for a table.
func (c *IcebergCatalog) GetCurrentSnapshot(ctx context.Context, identifier TableIdentifier) (*table.Snapshot, error) {
	tbl, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	snap, err := tbl.CurrentSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, ErrSnapshotNotFound
	}
	return snap, nil
}

// AppendDataFiles appends data files to a table.
func (c *IcebergCatalog) AppendDataFiles(ctx context.Context, identifier TableIdentifier, files []DataFile, opts *AppendFilesOptions) (*table.Snapshot, error) {
	tbl, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}

	return tbl.AppendFiles(ctx, files, opts)
}

// DropTable drops a table.
func (c *IcebergCatalog) DropTable(ctx context.Context, identifier TableIdentifier) error {
	if err := c.cat.DropTable(ctx, identifier); err != nil {
		return mapCatalogError(err)
	}
	return nil
}

// ListTables lists all tables in a namespace.
func (c *IcebergCatalog) ListTables(ctx context.Context, namespace []string) ([]TableIdentifier, error) {
	tables := []TableIdentifier{}
	for id, err := range c.cat.ListTables(ctx, namespace) {
		if err != nil {
			return nil, mapCatalogError(err)
		}
		tables = append(tables, id)
	}
	return tables, nil
}

// TableExists checks if a table exists.
func (c *IcebergCatalog) TableExists(ctx context.Context, identifier TableIdentifier) (bool, error) {
	exists, err := c.cat.CheckTableExists(ctx, identifier)
	if err != nil {
		return false, mapCatalogError(err)
	}
	return exists, nil
}

// Close releases resources held by the catalog.
func (c *IcebergCatalog) Close() error {
	return nil
}

func mapCatalogError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, icecatalog.ErrNoSuchTable):
		return ErrTableNotFound
	case errors.Is(err, icecatalog.ErrTableAlreadyExists):
		return ErrTableAlreadyExists
	case errors.Is(err, rest.ErrCommitFailed), errors.Is(err, rest.ErrCommitStateUnknown):
		return ErrCommitConflict
	case errors.Is(err, rest.ErrServiceUnavailable):
		return ErrCatalogUnavailable
	}
	return err
}

type icebergTable struct {
	tbl *table.Table
}

func (t *icebergTable) Identifier() TableIdentifier {
	return t.tbl.Identifier()
}

func (t *icebergTable) Schema() *iceberg.Schema {
	return t.tbl.Schema()
}

func (t *icebergTable) CurrentSnapshot(ctx context.Context) (*table.Snapshot, error) {
	snap := t.tbl.CurrentSnapshot()
	if snap == nil {
		return nil, ErrSnapshotNotFound
	}
	return snap, nil
}

func (t *icebergTable) Snapshots(ctx context.Context) ([]table.Snapshot, error) {
	return t.tbl.Metadata().Snapshots(), nil
}

func (t *icebergTable) AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*table.Snapshot, error) {
	if len(files) == 0 {
		return nil, errors.New("no files to append")
	}

	if opts != nil && opts.ExpectedSnapshotID != nil {
		if snap := t.tbl.CurrentSnapshot(); snap == nil || snap.SnapshotID != *opts.ExpectedSnapshotID {
			return nil, ErrCommitConflict
		}
	}

	paths := make([]string, 0, len(files))
	for _, f := range files {
		if f.Path == "" {
			return nil, errors.New("data file path cannot be empty")
		}
		paths = append(paths, f.Path)
	}

	props := iceberg.Properties{}
	if opts != nil && opts.SnapshotProperties != nil {
		props = opts.SnapshotProperties
	}

	txn := t.tbl.NewTransaction()
	if err := txn.AddFiles(ctx, paths, props, false); err != nil {
		return nil, mapCatalogError(err)
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, mapCatalogError(err)
	}
	t.tbl = updated

	snap := updated.CurrentSnapshot()
	if snap == nil {
		return nil, ErrSnapshotNotFound
	}
	return snap, nil
}

func (t *icebergTable) ReplaceFiles(ctx context.Context, added []DataFile, removed []DataFile, opts *ReplaceFilesOptions) (*table.Snapshot, error) {
	if len(added) == 0 && len(removed) == 0 {
		return nil, errors.New("no files to replace")
	}

	if opts != nil && opts.ExpectedSnapshotID != nil {
		if snap := t.tbl.CurrentSnapshot(); snap == nil || snap.SnapshotID != *opts.ExpectedSnapshotID {
			return nil, ErrCommitConflict
		}
	}

	addedPaths := make([]string, 0, len(added))
	for _, f := range added {
		if f.Path == "" {
			return nil, errors.New("data file path cannot be empty")
		}
		addedPaths = append(addedPaths, f.Path)
	}

	removedPaths := make([]string, 0, len(removed))
	for _, f := range removed {
		if f.Path == "" {
			return nil, errors.New("data file path cannot be empty")
		}
		removedPaths = append(removedPaths, f.Path)
	}

	props := iceberg.Properties{}
	if opts != nil && opts.SnapshotProperties != nil {
		props = opts.SnapshotProperties
	}

	txn := t.tbl.NewTransaction()
	if err := txn.ReplaceDataFiles(ctx, removedPaths, addedPaths, props); err != nil {
		return nil, mapCatalogError(err)
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, mapCatalogError(err)
	}
	t.tbl = updated

	snap := updated.CurrentSnapshot()
	if snap == nil {
		return nil, ErrSnapshotNotFound
	}
	return snap, nil
}

func (t *icebergTable) Properties() TableProperties {
	return t.tbl.Properties()
}

func (t *icebergTable) Location() string {
	return t.tbl.Location()
}

func (t *icebergTable) Refresh(ctx context.Context) error {
	return t.tbl.Refresh(ctx)
}
