package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/sql"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	_ "github.com/duckdb/duckdb-go/v2"
	_ "modernc.org/sqlite"
)

func TestIcebergDuckDBAppendRead(t *testing.T) {
	ctx := context.Background()
	cat, props, cleanup := newDuckDBCatalog(t, ctx)
	defer cleanup()

	topicName := "duckdb-append-topic"
	tableRef := createDuckDBTable(t, ctx, cat, props, topicName)

	records := []worker.Record{
		{Partition: 0, Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("v1"), Attributes: 0},
		{Partition: 0, Offset: 1, Timestamp: time.Now().UnixMilli(), Key: []byte("k2"), Value: []byte("v2"), Attributes: 0},
	}
	dataPath := writeParquetFile(t, tableRef.Location(), "append", records)

	appender := catalog.NewAppender(catalog.DefaultAppenderConfig(cat))
	if _, err := appender.AppendFilesForStream(ctx, topicName, "job-append", []catalog.DataFile{{Path: dataPath}}); err != nil {
		t.Fatalf("append files failed: %v", err)
	}

	rows := queryIcebergRows(t, tableRef)
	if len(rows) != len(records) {
		t.Fatalf("expected %d rows, got %d", len(records), len(rows))
	}
	if rows[0].Offset != 0 || string(rows[0].Value) != "v1" {
		t.Fatalf("unexpected first row: offset=%d value=%s", rows[0].Offset, rows[0].Value)
	}
	if rows[1].Offset != 1 || string(rows[1].Value) != "v2" {
		t.Fatalf("unexpected second row: offset=%d value=%s", rows[1].Offset, rows[1].Value)
	}
}

func TestIcebergDuckDBReplaceFiles(t *testing.T) {
	ctx := context.Background()
	cat, props, cleanup := newDuckDBCatalog(t, ctx)
	defer cleanup()

	topicName := "duckdb-rewrite-topic"
	tableRef := createDuckDBTable(t, ctx, cat, props, topicName)

	oldRecords := []worker.Record{
		{Partition: 0, Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("old"), Attributes: 0},
	}
	oldPath := writeParquetFile(t, tableRef.Location(), "old", oldRecords)

	appender := catalog.NewAppender(catalog.DefaultAppenderConfig(cat))
	if _, err := appender.AppendFilesForStream(ctx, topicName, "job-old", []catalog.DataFile{{Path: oldPath}}); err != nil {
		t.Fatalf("append old files failed: %v", err)
	}

	newRecords := []worker.Record{
		{Partition: 0, Offset: 0, Timestamp: time.Now().UnixMilli(), Key: []byte("k1"), Value: []byte("new"), Attributes: 0},
	}
	newPath := writeParquetFile(t, tableRef.Location(), "new", newRecords)

	if _, err := appender.ReplaceFilesForStream(ctx, topicName, "job-rewrite", []catalog.DataFile{{Path: newPath}}, []catalog.DataFile{{Path: oldPath}}); err != nil {
		t.Fatalf("replace files failed: %v", err)
	}

	rows := queryIcebergRows(t, tableRef)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after rewrite, got %d", len(rows))
	}
	if string(rows[0].Value) != "new" {
		t.Fatalf("expected rewritten value to be 'new', got %s", rows[0].Value)
	}
}

type duckDBRow struct {
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

func newDuckDBCatalog(t *testing.T, ctx context.Context) (*catalog.IcebergCatalog, iceberg.Properties, func()) {
	t.Helper()

	tempDir := t.TempDir()
	warehouse := filepath.Join(tempDir, "warehouse")
	dbPath := filepath.Join(tempDir, "catalog.sqlite")
	warehouseURI := "file://" + warehouse

	props := iceberg.Properties{
		"type":        "sql",
		"uri":         dbPath,
		"warehouse":   warehouseURI,
		"sql.driver":  "sqlite",
		"sql.dialect": "sqlite",
	}
	props["dray"] = "dray"

	wrappedCat, err := catalog.LoadCatalog(ctx, catalog.CatalogConfig{
		Type:      "sql",
		URI:       dbPath,
		Warehouse: warehouseURI,
		Props: iceberg.Properties{
			"sql.driver":  "sqlite",
			"sql.dialect": "sqlite",
			"dray":        "dray",
		},
	})
	if err != nil {
		t.Fatalf("load wrapped catalog failed: %v", err)
	}

	return wrappedCat, props, func() {
		_ = wrappedCat.Close()
	}
}

func createDuckDBTable(t *testing.T, ctx context.Context, cat *catalog.IcebergCatalog, props iceberg.Properties, topicName string) catalog.Table {
	t.Helper()

	baseCat, err := icecatalog.Load(ctx, "dray", props)
	if err != nil {
		t.Fatalf("load iceberg sql catalog failed: %v", err)
	}
	if err := baseCat.CreateNamespace(ctx, []string{"dray"}, nil); err != nil && !errors.Is(err, icecatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create namespace failed: %v", err)
	}
	namespaces, err := baseCat.ListNamespaces(ctx, []string{})
	if err != nil {
		t.Fatalf("list namespaces failed: %v", err)
	}
	found := false
	for _, ns := range namespaces {
		if len(ns) == 1 && ns[0] == "dray" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("namespace dray not found after create, got: %v", namespaces)
	}

	spec := catalog.DefaultPartitionSpec()
	identifier := catalog.NewTableIdentifier([]string{"dray"}, topicName)
	_, err = baseCat.CreateTable(ctx, identifier, catalog.DefaultSchema(),
		icecatalog.WithPartitionSpec(&spec),
		icecatalog.WithProperties(catalog.DefaultTableProperties(topicName, "test-cluster")),
	)
	if err != nil && !errors.Is(err, icecatalog.ErrTableAlreadyExists) {
		t.Fatalf("create table failed: %v", err)
	}

	wrappedTable, err := cat.LoadTable(ctx, identifier)
	if err != nil {
		t.Fatalf("load table failed: %v", err)
	}
	return wrappedTable
}

func writeParquetFile(t *testing.T, tableLocation, name string, records []worker.Record) string {
	t.Helper()

	data, _, err := worker.WriteToBuffer(worker.BuildParquetSchema(nil), records)
	if err != nil {
		t.Fatalf("write parquet buffer failed: %v", err)
	}

	localLocation := strings.TrimPrefix(tableLocation, "file://")
	dataPath := filepath.Join(localLocation, "data", name+".parquet")
	if err := os.MkdirAll(filepath.Dir(dataPath), 0o755); err != nil {
		t.Fatalf("mkdir data dir failed: %v", err)
	}
	if err := os.WriteFile(dataPath, data, 0o644); err != nil {
		t.Fatalf("write parquet file failed: %v", err)
	}

	if strings.HasPrefix(tableLocation, "file://") {
		return "file://" + dataPath
	}
	return dataPath
}

func queryIcebergRows(t *testing.T, tableRef catalog.Table) []duckDBRow {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("INSTALL iceberg"); err != nil {
		t.Fatalf("duckdb install iceberg failed: %v", err)
	}
	if _, err := db.Exec("LOAD iceberg"); err != nil {
		t.Fatalf("duckdb load iceberg failed: %v", err)
	}
	if _, err := db.Exec("SET unsafe_enable_version_guessing = true"); err != nil {
		t.Fatalf("duckdb enable version guessing failed: %v", err)
	}

	tablePath := strings.TrimPrefix(tableRef.Location(), "file://")
	query := fmt.Sprintf(`SELECT "partition", "offset", "key", "value" FROM iceberg_scan('%s') ORDER BY "offset"`, escapeDuckDBString(tablePath))
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("duckdb iceberg scan failed: %v", err)
	}
	defer rows.Close()

	var results []duckDBRow
	for rows.Next() {
		var row duckDBRow
		if err := rows.Scan(&row.Partition, &row.Offset, &row.Key, &row.Value); err != nil {
			t.Fatalf("scan iceberg row failed: %v", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("duckdb rows error: %v", err)
	}
	return results
}

func escapeDuckDBString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
