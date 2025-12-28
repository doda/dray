package catalog

import (
	"context"
	"errors"
	"testing"
)

// mockTable is a test implementation of the Table interface.
type mockTable struct {
	identifier TableIdentifier
	schema     Schema
	snapshots  []Snapshot
	props      TableProperties
	location   string
}

func (t *mockTable) Identifier() TableIdentifier { return t.identifier }
func (t *mockTable) Schema() Schema              { return t.schema }
func (t *mockTable) Properties() TableProperties { return t.props }
func (t *mockTable) Location() string            { return t.location }

func (t *mockTable) CurrentSnapshot(ctx context.Context) (*Snapshot, error) {
	if len(t.snapshots) == 0 {
		return nil, ErrSnapshotNotFound
	}
	return &t.snapshots[len(t.snapshots)-1], nil
}

func (t *mockTable) Snapshots(ctx context.Context) ([]Snapshot, error) {
	return t.snapshots, nil
}

func (t *mockTable) AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	var parentID *int64
	nextSnapshotID := int64(1)
	if len(t.snapshots) > 0 {
		parentID = &t.snapshots[len(t.snapshots)-1].SnapshotID
		nextSnapshotID = t.snapshots[len(t.snapshots)-1].SnapshotID + 1
	}

	summary := map[string]string{
		"added-data-files": "1",
	}
	// Copy snapshot properties from options to summary
	if opts != nil && opts.SnapshotProperties != nil {
		for k, v := range opts.SnapshotProperties {
			summary[k] = v
		}
	}

	newSnapshot := Snapshot{
		SnapshotID:       nextSnapshotID,
		ParentSnapshotID: parentID,
		TimestampMs:      1234567890,
		Operation:        OpAppend,
		Summary:          summary,
	}
	t.snapshots = append(t.snapshots, newSnapshot)
	return &t.snapshots[len(t.snapshots)-1], nil
}

func (t *mockTable) Refresh(ctx context.Context) error {
	return nil
}

// Verify mockTable implements Table interface.
var _ Table = (*mockTable)(nil)

// mockCatalog is a test implementation of the Catalog interface.
type mockCatalog struct {
	tables map[string]*mockTable
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		tables: make(map[string]*mockTable),
	}
}

func (c *mockCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	t, ok := c.tables[identifier.String()]
	if !ok {
		return nil, ErrTableNotFound
	}
	return t, nil
}

func (c *mockCatalog) CreateTableIfMissing(ctx context.Context, identifier TableIdentifier, opts CreateTableOptions) (Table, error) {
	key := identifier.String()
	if t, ok := c.tables[key]; ok {
		return t, nil
	}
	t := &mockTable{
		identifier: identifier,
		schema:     opts.Schema,
		props:      opts.Properties,
		location:   opts.Location,
		snapshots:  nil,
	}
	c.tables[key] = t
	return t, nil
}

func (c *mockCatalog) GetCurrentSnapshot(ctx context.Context, identifier TableIdentifier) (*Snapshot, error) {
	t, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return t.CurrentSnapshot(ctx)
}

func (c *mockCatalog) AppendDataFiles(ctx context.Context, identifier TableIdentifier, files []DataFile, opts *AppendFilesOptions) (*Snapshot, error) {
	t, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return t.AppendFiles(ctx, files, opts)
}

func (c *mockCatalog) DropTable(ctx context.Context, identifier TableIdentifier) error {
	key := identifier.String()
	if _, ok := c.tables[key]; !ok {
		return ErrTableNotFound
	}
	delete(c.tables, key)
	return nil
}

func (c *mockCatalog) ListTables(ctx context.Context, namespace []string) ([]TableIdentifier, error) {
	var result []TableIdentifier
	for _, t := range c.tables {
		result = append(result, t.identifier)
	}
	return result, nil
}

func (c *mockCatalog) TableExists(ctx context.Context, identifier TableIdentifier) (bool, error) {
	_, ok := c.tables[identifier.String()]
	return ok, nil
}

func (c *mockCatalog) Close() error {
	return nil
}

// Verify mockCatalog implements Catalog interface.
var _ Catalog = (*mockCatalog)(nil)

func TestTableIdentifierString(t *testing.T) {
	tests := []struct {
		name       string
		identifier TableIdentifier
		expected   string
	}{
		{
			name:       "no namespace",
			identifier: TableIdentifier{Name: "my_topic"},
			expected:   "my_topic",
		},
		{
			name:       "single namespace",
			identifier: TableIdentifier{Namespace: []string{"dray"}, Name: "my_topic"},
			expected:   "dray.my_topic",
		},
		{
			name:       "nested namespace",
			identifier: TableIdentifier{Namespace: []string{"production", "dray"}, Name: "my_topic"},
			expected:   "production.dray.my_topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.identifier.String(); got != tt.expected {
				t.Errorf("TableIdentifier.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestCatalogLoadTable(t *testing.T) {
	ctx := context.Background()
	catalog := newMockCatalog()

	identifier := TableIdentifier{Namespace: []string{"dray"}, Name: "test_topic"}

	// Table should not exist initially.
	_, err := catalog.LoadTable(ctx, identifier)
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}

	// Create the table.
	opts := CreateTableOptions{
		Schema:     DefaultSchema(),
		Properties: DefaultTableProperties("test_topic", "test-cluster"),
		Location:   "s3://bucket/prefix/iceberg/test_topic",
	}
	table, err := catalog.CreateTableIfMissing(ctx, identifier, opts)
	if err != nil {
		t.Fatalf("CreateTableIfMissing failed: %v", err)
	}

	if table.Identifier().String() != identifier.String() {
		t.Errorf("table identifier = %q, want %q", table.Identifier().String(), identifier.String())
	}

	// Should be able to load it now.
	loaded, err := catalog.LoadTable(ctx, identifier)
	if err != nil {
		t.Fatalf("LoadTable failed: %v", err)
	}
	if loaded.Identifier().String() != identifier.String() {
		t.Errorf("loaded table identifier = %q, want %q", loaded.Identifier().String(), identifier.String())
	}
}

func TestCatalogCreateTableIfMissing(t *testing.T) {
	ctx := context.Background()
	catalog := newMockCatalog()

	identifier := TableIdentifier{Namespace: []string{"dray"}, Name: "test_topic"}
	opts := CreateTableOptions{
		Schema:     DefaultSchema(),
		Properties: DefaultTableProperties("test_topic", "test-cluster"),
	}

	// First call creates the table.
	table1, err := catalog.CreateTableIfMissing(ctx, identifier, opts)
	if err != nil {
		t.Fatalf("CreateTableIfMissing failed: %v", err)
	}

	// Second call returns the existing table.
	table2, err := catalog.CreateTableIfMissing(ctx, identifier, opts)
	if err != nil {
		t.Fatalf("CreateTableIfMissing failed: %v", err)
	}

	if table1.Identifier().String() != table2.Identifier().String() {
		t.Error("CreateTableIfMissing returned different tables")
	}
}

func TestCatalogTableExists(t *testing.T) {
	ctx := context.Background()
	catalog := newMockCatalog()

	identifier := TableIdentifier{Namespace: []string{"dray"}, Name: "test_topic"}

	exists, err := catalog.TableExists(ctx, identifier)
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if exists {
		t.Error("expected table to not exist")
	}

	// Create the table.
	opts := CreateTableOptions{Schema: DefaultSchema()}
	_, err = catalog.CreateTableIfMissing(ctx, identifier, opts)
	if err != nil {
		t.Fatalf("CreateTableIfMissing failed: %v", err)
	}

	exists, err = catalog.TableExists(ctx, identifier)
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Error("expected table to exist")
	}
}

func TestCatalogDropTable(t *testing.T) {
	ctx := context.Background()
	catalog := newMockCatalog()

	identifier := TableIdentifier{Namespace: []string{"dray"}, Name: "test_topic"}

	// Dropping non-existent table should fail.
	err := catalog.DropTable(ctx, identifier)
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}

	// Create and then drop.
	opts := CreateTableOptions{Schema: DefaultSchema()}
	_, err = catalog.CreateTableIfMissing(ctx, identifier, opts)
	if err != nil {
		t.Fatalf("CreateTableIfMissing failed: %v", err)
	}

	err = catalog.DropTable(ctx, identifier)
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// Should not exist anymore.
	exists, _ := catalog.TableExists(ctx, identifier)
	if exists {
		t.Error("table should not exist after drop")
	}
}

func TestTableCurrentSnapshot(t *testing.T) {
	ctx := context.Background()
	catalog := newMockCatalog()

	identifier := TableIdentifier{Namespace: []string{"dray"}, Name: "test_topic"}
	opts := CreateTableOptions{Schema: DefaultSchema()}
	table, _ := catalog.CreateTableIfMissing(ctx, identifier, opts)

	// New table has no snapshots.
	_, err := table.CurrentSnapshot(ctx)
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Errorf("expected ErrSnapshotNotFound for new table, got %v", err)
	}
}

func TestDataFileTypes(t *testing.T) {
	// Verify DataFile can be constructed with all fields.
	df := DataFile{
		Path:           "s3://bucket/path/to/file.parquet",
		Format:         FormatParquet,
		PartitionValue: 0,
		RecordCount:    1000,
		FileSizeBytes:  1024 * 1024,
		ColumnSizes:    map[int32]int64{1: 100, 2: 200},
		ValueCounts:    map[int32]int64{1: 1000, 2: 1000},
		NullValueCounts: map[int32]int64{4: 50, 5: 100},
		LowerBounds:    map[int32][]byte{2: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		UpperBounds:    map[int32][]byte{2: []byte{0, 0, 0, 0, 0, 0, 3, 232}},
		SplitOffsets:   []int64{0, 512 * 1024},
		SortOrderID:    nil,
	}

	if df.Format != FormatParquet {
		t.Errorf("expected format %q, got %q", FormatParquet, df.Format)
	}
	if df.RecordCount != 1000 {
		t.Errorf("expected record count 1000, got %d", df.RecordCount)
	}
}

func TestSnapshotOperations(t *testing.T) {
	// Verify snapshot operation constants.
	ops := []SnapshotOperation{OpAppend, OpReplace, OpOverwrite, OpDelete}
	expected := []string{"append", "replace", "overwrite", "delete"}

	for i, op := range ops {
		if string(op) != expected[i] {
			t.Errorf("expected operation %q, got %q", expected[i], op)
		}
	}
}

func TestDefaultSchema(t *testing.T) {
	schema := DefaultSchema()

	if len(schema.Fields) != 10 {
		t.Errorf("expected 10 fields in default schema, got %d", len(schema.Fields))
	}

	// Verify key fields exist.
	fieldNames := make(map[string]bool)
	for _, f := range schema.Fields {
		fieldNames[f.Name] = true
	}

	required := []string{"partition", "offset", "timestamp_ms", "key", "value", "headers", "attributes"}
	for _, name := range required {
		if !fieldNames[name] {
			t.Errorf("missing required field %q in default schema", name)
		}
	}
}

func TestDefaultPartitionSpec(t *testing.T) {
	spec := DefaultPartitionSpec()

	if len(spec.Fields) != 1 {
		t.Errorf("expected 1 partition field, got %d", len(spec.Fields))
	}

	if spec.Fields[0].Name != "partition" {
		t.Errorf("expected partition field name 'partition', got %q", spec.Fields[0].Name)
	}

	if spec.Fields[0].Transform != "identity" {
		t.Errorf("expected identity transform, got %q", spec.Fields[0].Transform)
	}
}

func TestDefaultTableProperties(t *testing.T) {
	props := DefaultTableProperties("my-topic", "my-cluster")

	if props[PropertyDrayTopic] != "my-topic" {
		t.Errorf("expected topic 'my-topic', got %q", props[PropertyDrayTopic])
	}

	if props[PropertyDrayClusterID] != "my-cluster" {
		t.Errorf("expected cluster 'my-cluster', got %q", props[PropertyDrayClusterID])
	}

	if props[PropertyDraySchemaVersion] != "1" {
		t.Errorf("expected schema version '1', got %q", props[PropertyDraySchemaVersion])
	}
}

func TestErrors(t *testing.T) {
	// Verify error variables are distinct.
	errs := []error{
		ErrTableNotFound,
		ErrTableAlreadyExists,
		ErrSnapshotNotFound,
		ErrCommitConflict,
		ErrCatalogUnavailable,
	}

	for i := 0; i < len(errs); i++ {
		for j := i + 1; j < len(errs); j++ {
			if errors.Is(errs[i], errs[j]) {
				t.Errorf("errors should be distinct: %v and %v", errs[i], errs[j])
			}
		}
	}
}

func TestFileFormatConstants(t *testing.T) {
	if FormatParquet != "PARQUET" {
		t.Errorf("FormatParquet = %q, want PARQUET", FormatParquet)
	}
	if FormatAvro != "AVRO" {
		t.Errorf("FormatAvro = %q, want AVRO", FormatAvro)
	}
	if FormatORC != "ORC" {
		t.Errorf("FormatORC = %q, want ORC", FormatORC)
	}
}
