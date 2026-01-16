package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

type mockCatalog struct {
	loadCount int
	table     Table
}

func (m *mockCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (Table, error) {
	m.loadCount++
	if m.table == nil {
		m.table = &mockTable{id: identifier}
	}
	return m.table, nil
}

func (m *mockCatalog) CreateTableIfMissing(ctx context.Context, identifier TableIdentifier, opts CreateTableOptions) (Table, error) {
	return nil, ErrOperationNotSupported
}

func (m *mockCatalog) GetCurrentSnapshot(ctx context.Context, identifier TableIdentifier) (*table.Snapshot, error) {
	return nil, ErrOperationNotSupported
}

func (m *mockCatalog) AppendDataFiles(ctx context.Context, identifier TableIdentifier, files []DataFile, opts *AppendFilesOptions) (*table.Snapshot, error) {
	return nil, ErrOperationNotSupported
}

func (m *mockCatalog) DropTable(ctx context.Context, identifier TableIdentifier) error {
	return ErrOperationNotSupported
}

func (m *mockCatalog) ListTables(ctx context.Context, namespace []string) ([]TableIdentifier, error) {
	return nil, ErrOperationNotSupported
}

func (m *mockCatalog) TableExists(ctx context.Context, identifier TableIdentifier) (bool, error) {
	return false, ErrOperationNotSupported
}

func (m *mockCatalog) Close() error {
	return nil
}

type mockTable struct {
	id           TableIdentifier
	snapshots    []table.Snapshot
	appendCalls  int
	replaceCalls int
}

func (m *mockTable) Identifier() TableIdentifier {
	return m.id
}

func (m *mockTable) Schema() *iceberg.Schema {
	return nil
}

func (m *mockTable) CurrentSnapshot(ctx context.Context) (*table.Snapshot, error) {
	return nil, ErrSnapshotNotFound
}

func (m *mockTable) Snapshots(ctx context.Context) ([]table.Snapshot, error) {
	return m.snapshots, nil
}

func (m *mockTable) AppendFiles(ctx context.Context, files []DataFile, opts *AppendFilesOptions) (*table.Snapshot, error) {
	m.appendCalls++
	return &table.Snapshot{SnapshotID: int64(m.appendCalls)}, nil
}

func (m *mockTable) ReplaceFiles(ctx context.Context, added []DataFile, removed []DataFile, opts *ReplaceFilesOptions) (*table.Snapshot, error) {
	m.replaceCalls++
	return &table.Snapshot{SnapshotID: int64(m.replaceCalls)}, nil
}

func (m *mockTable) Properties() TableProperties {
	return nil
}

func (m *mockTable) Location() string {
	return ""
}

func (m *mockTable) Refresh(ctx context.Context) error {
	return nil
}

func (m *mockTable) ExpireSnapshots(ctx context.Context, olderThan time.Duration, retainLast int) error {
	return nil
}

func TestAppenderTableCacheReuse(t *testing.T) {
	tbl := &mockTable{id: TableIdentifier{"dray", "topic"}}
	cat := &mockCatalog{table: tbl}
	cfg := DefaultAppenderConfig(cat)
	cfg.TableCacheTTL = time.Minute
	app := NewAppender(cfg)

	files := []DataFile{
		{
			Path:           "s3://bucket/topic/file.parquet",
			RecordCount:    1,
			FileSizeBytes:  1,
			PartitionValue: 0,
		},
	}

	if _, err := app.AppendFilesForStream(context.Background(), "topic", "job-1", files); err != nil {
		t.Fatalf("append job-1 failed: %v", err)
	}
	if _, err := app.AppendFilesForStream(context.Background(), "topic", "job-2", files); err != nil {
		t.Fatalf("append job-2 failed: %v", err)
	}

	if cat.loadCount != 1 {
		t.Fatalf("expected 1 table load, got %d", cat.loadCount)
	}
}

func TestAppenderTableCacheDisabled(t *testing.T) {
	tbl := &mockTable{id: TableIdentifier{"dray", "topic"}}
	cat := &mockCatalog{table: tbl}
	cfg := DefaultAppenderConfig(cat)
	cfg.TableCacheTTL = 0
	app := NewAppender(cfg)

	files := []DataFile{
		{
			Path:           "s3://bucket/topic/file.parquet",
			RecordCount:    1,
			FileSizeBytes:  1,
			PartitionValue: 0,
		},
	}

	if _, err := app.AppendFilesForStream(context.Background(), "topic", "job-1", files); err != nil {
		t.Fatalf("append job-1 failed: %v", err)
	}
	if _, err := app.AppendFilesForStream(context.Background(), "topic", "job-2", files); err != nil {
		t.Fatalf("append job-2 failed: %v", err)
	}

	if cat.loadCount != 2 {
		t.Fatalf("expected 2 table loads, got %d", cat.loadCount)
	}
}
