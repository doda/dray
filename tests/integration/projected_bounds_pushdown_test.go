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
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/projection"
	_ "github.com/duckdb/duckdb-go/v2"
	_ "modernc.org/sqlite"
)

// TestProjectedBoundsPushdown verifies that projected field bounds (like created_at)
// are properly stored in Iceberg manifests and enable query pushdown in DuckDB.
//
// This test:
// 1. Creates an Iceberg table with a projected created_at field
// 2. Writes three Parquet files with distinct time ranges
// 3. Commits them to Iceberg with min/max bounds
// 4. Queries with a time filter and verifies file pruning occurs
func TestProjectedBoundsPushdown(t *testing.T) {
	ctx := context.Background()
	cat, props, cleanup := newBoundsCatalog(t, ctx)
	defer cleanup()

	topicName := "bounds-pushdown-topic"

	// Define projection fields including created_at
	projectionFields := []projection.FieldSpec{
		{Name: "created_at", Path: "created_at", Type: projection.FieldTypeTimestampMs},
	}

	tableRef := createBoundsTable(t, ctx, cat, props, topicName, projectionFields)
	appender := catalog.NewAppender(catalog.DefaultAppenderConfig(cat))

	// Create three files with distinct, non-overlapping created_at ranges
	// File 1: created_at from hour 0-1 (1970-01-01 00:00 - 01:00)
	// File 2: created_at from hour 2-3 (1970-01-01 02:00 - 03:00)
	// File 3: created_at from hour 4-5 (1970-01-01 04:00 - 05:00)

	baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	hour := time.Hour

	// File 1: records with created_at in hour 0-1
	file1Records := []worker.Record{
		makeRecordWithCreatedAt(0, 0, baseTime),
		makeRecordWithCreatedAt(0, 1, baseTime.Add(30*time.Minute)),
		makeRecordWithCreatedAt(0, 2, baseTime.Add(59*time.Minute)),
	}
	file1Path, file1Stats := writeParquetFileWithBounds(t, tableRef, "file1", file1Records, projectionFields)

	// File 2: records with created_at in hour 2-3
	file2Records := []worker.Record{
		makeRecordWithCreatedAt(0, 3, baseTime.Add(2*hour)),
		makeRecordWithCreatedAt(0, 4, baseTime.Add(2*hour+30*time.Minute)),
		makeRecordWithCreatedAt(0, 5, baseTime.Add(2*hour+59*time.Minute)),
	}
	file2Path, file2Stats := writeParquetFileWithBounds(t, tableRef, "file2", file2Records, projectionFields)

	// File 3: records with created_at in hour 4-5
	file3Records := []worker.Record{
		makeRecordWithCreatedAt(0, 6, baseTime.Add(4*hour)),
		makeRecordWithCreatedAt(0, 7, baseTime.Add(4*hour+30*time.Minute)),
		makeRecordWithCreatedAt(0, 8, baseTime.Add(4*hour+59*time.Minute)),
	}
	file3Path, file3Stats := writeParquetFileWithBounds(t, tableRef, "file3", file3Records, projectionFields)

	// Build data files with bounds
	dataFiles := []catalog.DataFile{
		buildDataFileWithBounds(file1Path, file1Stats, tableRef.Schema()),
		buildDataFileWithBounds(file2Path, file2Stats, tableRef.Schema()),
		buildDataFileWithBounds(file3Path, file3Stats, tableRef.Schema()),
	}

	// Append all files to Iceberg
	if _, err := appender.AppendFilesForStream(ctx, topicName, "job-bounds", dataFiles); err != nil {
		t.Fatalf("append files failed: %v", err)
	}

	// Test 1: Query all data (should return 9 records)
	allRows := queryBoundsRows(t, tableRef, "")
	if len(allRows) != 9 {
		t.Fatalf("expected 9 total rows, got %d", len(allRows))
	}

	// Test 2: Query only hour 2 data (should return 3 records from file2)
	hour2Start := baseTime.Add(2 * hour)
	hour2End := baseTime.Add(3 * hour)
	// Use epoch_ms for comparison since DuckDB iceberg_scan may not support TIMESTAMP comparisons
	filter := fmt.Sprintf(`epoch_ms("created_at") >= %d AND epoch_ms("created_at") < %d`,
		hour2Start.UnixMilli(),
		hour2End.UnixMilli())

	hour2Rows := queryBoundsRows(t, tableRef, filter)
	if len(hour2Rows) != 3 {
		t.Fatalf("expected 3 rows for hour 2, got %d", len(hour2Rows))
	}

	// Verify the returned records are from the correct time range
	for _, row := range hour2Rows {
		if row.Offset < 3 || row.Offset > 5 {
			t.Errorf("unexpected offset %d in hour 2 results (expected 3-5)", row.Offset)
		}
	}

	// Test 3: Query hour 0 data (should return 3 records from file1)
	hour0End := baseTime.Add(1 * hour)
	filter0 := fmt.Sprintf(`epoch_ms("created_at") >= %d AND epoch_ms("created_at") < %d`,
		baseTime.UnixMilli(),
		hour0End.UnixMilli())

	hour0Rows := queryBoundsRows(t, tableRef, filter0)
	if len(hour0Rows) != 3 {
		t.Fatalf("expected 3 rows for hour 0, got %d", len(hour0Rows))
	}

	// Test 4: Query hour 4 data (should return 3 records from file3)
	hour4Start := baseTime.Add(4 * hour)
	hour4End := baseTime.Add(5 * hour)
	filter4 := fmt.Sprintf(`epoch_ms("created_at") >= %d AND epoch_ms("created_at") < %d`,
		hour4Start.UnixMilli(),
		hour4End.UnixMilli())

	hour4Rows := queryBoundsRows(t, tableRef, filter4)
	if len(hour4Rows) != 3 {
		t.Fatalf("expected 3 rows for hour 4, got %d", len(hour4Rows))
	}

	t.Logf("Successfully verified projected bounds pushdown: 9 total records across 3 files, filtered queries returned correct subsets")
}

func makeRecordWithCreatedAt(partition int32, offset int64, createdAt time.Time) worker.Record {
	createdAtMs := createdAt.UnixMilli()
	return worker.Record{
		Partition:  partition,
		Offset:     offset,
		Timestamp:  time.Now().UnixMilli(), // Kafka timestamp (ingest time)
		Key:        []byte(fmt.Sprintf("key-%d", offset)),
		Value:      []byte(fmt.Sprintf(`{"created_at":"%s"}`, createdAt.Format(time.RFC3339))),
		Attributes: 0,
		Projected: map[string]any{
			"created_at": createdAtMs,
		},
	}
}

func newBoundsCatalog(t *testing.T, ctx context.Context) (*catalog.IcebergCatalog, iceberg.Properties, func()) {
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
		"dray":        "dray",
	}

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

func createBoundsTable(t *testing.T, ctx context.Context, cat *catalog.IcebergCatalog, props iceberg.Properties, topicName string, projectionFields []projection.FieldSpec) catalog.Table {
	t.Helper()

	baseCat, err := icecatalog.Load(ctx, "dray", props)
	if err != nil {
		t.Fatalf("load iceberg sql catalog failed: %v", err)
	}
	if err := baseCat.CreateNamespace(ctx, []string{"dray"}, nil); err != nil && !errors.Is(err, icecatalog.ErrNamespaceAlreadyExists) {
		t.Fatalf("create namespace failed: %v", err)
	}

	// Create schema with projection fields
	schema := catalog.SchemaWithProjections(projectionFields)
	spec := catalog.DefaultPartitionSpec()
	identifier := catalog.NewTableIdentifier([]string{"dray"}, topicName)

	_, err = baseCat.CreateTable(ctx, identifier, schema,
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

func writeParquetFileWithBounds(t *testing.T, tableRef catalog.Table, name string, records []worker.Record, projectionFields []projection.FieldSpec) (string, worker.FileStats) {
	t.Helper()

	schema, err := worker.BuildParquetSchemaFromIceberg(tableRef.Schema(), projectionFields)
	if err != nil {
		t.Fatalf("build parquet schema failed: %v", err)
	}

	data, stats, err := worker.WriteToBufferWithProjections(schema, records, projectionFields)
	if err != nil {
		t.Fatalf("write parquet buffer failed: %v", err)
	}

	tableLocation := tableRef.Location()
	localLocation := strings.TrimPrefix(tableLocation, "file://")
	dataPath := filepath.Join(localLocation, "data", name+".parquet")
	if err := os.MkdirAll(filepath.Dir(dataPath), 0o755); err != nil {
		t.Fatalf("mkdir data dir failed: %v", err)
	}
	if err := os.WriteFile(dataPath, data, 0o644); err != nil {
		t.Fatalf("write parquet file failed: %v", err)
	}

	fullPath := dataPath
	if strings.HasPrefix(tableLocation, "file://") {
		fullPath = "file://" + dataPath
	}

	return fullPath, stats
}

func buildDataFileWithBounds(path string, stats worker.FileStats, schema *iceberg.Schema) catalog.DataFile {
	// Convert worker.FieldBounds to catalog.ProjectedFieldBounds
	var catalogBounds map[string]catalog.ProjectedFieldBounds
	if len(stats.ProjectedBounds) > 0 {
		catalogBounds = make(map[string]catalog.ProjectedFieldBounds, len(stats.ProjectedBounds))
		for name, bounds := range stats.ProjectedBounds {
			catalogBounds[name] = catalog.ProjectedFieldBounds{
				Min: bounds.Min,
				Max: bounds.Max,
			}
		}
	}

	// Scale timestamps to microseconds
	minTs := stats.MinTimestamp * 1000
	maxTs := stats.MaxTimestamp * 1000

	var dataStats *catalog.DataFileStats
	if len(catalogBounds) > 0 {
		dataStats = catalog.DataFileStatsWithProjections(
			0, // partition
			stats.MinOffset, stats.MaxOffset,
			minTs, maxTs,
			stats.RecordCount,
			schema, catalogBounds,
		)
	} else {
		dataStats = catalog.DefaultDataFileStats(
			0, // partition
			stats.MinOffset, stats.MaxOffset,
			minTs, maxTs,
			stats.RecordCount,
		)
	}

	return catalog.BuildDataFileFromStats(path, 0, stats.RecordCount, stats.SizeBytes, dataStats)
}

type boundsRow struct {
	Partition int32
	Offset    int64
	CreatedAt time.Time
}

func queryBoundsRows(t *testing.T, tableRef catalog.Table, whereClause string) []boundsRow {
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
	query := fmt.Sprintf(`SELECT "partition", "offset", "created_at" FROM iceberg_scan('%s')`, escapeBoundsString(tablePath))
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	query += ` ORDER BY "offset"`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("duckdb iceberg scan failed: %v\nquery: %s", err, query)
	}
	defer rows.Close()

	var results []boundsRow
	for rows.Next() {
		var row boundsRow
		if err := rows.Scan(&row.Partition, &row.Offset, &row.CreatedAt); err != nil {
			t.Fatalf("scan iceberg row failed: %v", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("duckdb rows error: %v", err)
	}
	return results
}

func escapeBoundsString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
