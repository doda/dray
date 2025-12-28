package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
)

// TestIcebergParquetMatch verifies that Parquet files committed to an Iceberg
// table match the stream offsets. This ensures consistency between the
// streaming layer and the table layer.
func TestIcebergParquetMatch(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newIcebergTestObjectStore()
	ctx := context.Background()

	topicName := "iceberg-match-topic"

	// Create topic with one partition
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, topicName, 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Step 1: Produce records with known content
	// We produce 3 batches with different record counts
	t.Log("Step 1: Producing records with known content...")
	recordsPerBatch := []int{5, 8, 7} // 20 total records
	var totalRecords int
	for _, count := range recordsPerBatch {
		totalRecords += count
	}

	for i, recordCount := range recordsPerBatch {
		produceReq := buildCompactionTestProduceRequest(topicName, 0, recordCount, i)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("produce batch %d failed with error code %d", i, partResp.ErrorCode)
		}
		t.Logf("  Produced batch %d: %d records, BaseOffset=%d", i, recordCount, partResp.BaseOffset)
	}

	// Verify records are fetchable from WAL
	walRecords := fetchAllRecords(t, fetchHandler, ctx, topicName, 0, 0)
	if len(walRecords) != totalRecords {
		t.Fatalf("expected %d records from WAL, got %d", totalRecords, len(walRecords))
	}
	t.Logf("  Verified %d records in WAL", len(walRecords))

	// Step 2: Run compaction with Iceberg commit
	t.Log("Step 2: Running compaction with Iceberg commit...")
	icebergCatalog := newIcebergMatchTestCatalog()

	// Create the Iceberg table for this topic
	tableIdentifier := catalog.TableIdentifier{
		Namespace: []string{"dray"},
		Name:      topicName,
	}
	icebergTable, err := icebergCatalog.CreateTableIfMissing(ctx, tableIdentifier, catalog.CreateTableOptions{
		Schema:     catalog.DefaultSchema(),
		Properties: catalog.DefaultTableProperties(topicName, "test-cluster"),
	})
	if err != nil {
		t.Fatalf("failed to create Iceberg table: %v", err)
	}
	t.Logf("  Created Iceberg table: %s", tableIdentifier.String())

	// Run compaction and get the Parquet file details
	parquetPath, minOffset, maxOffset, recordCount := runCompactionWithIceberg(t, ctx, streamID, metaStore, objStore, 0, topicName)
	t.Logf("  Compacted to Parquet: %s (offsets %d-%d, %d records)", parquetPath, minOffset, maxOffset, recordCount)

	// Commit the Parquet file to Iceberg
	// Note: maxOffset from compaction is exclusive (HWM), so last record offset is maxOffset-1
	lastRecordOffset := maxOffset - 1
	dataFile := catalog.DataFile{
		Path:           parquetPath,
		Format:         catalog.FormatParquet,
		PartitionValue: 0,
		RecordCount:    int64(recordCount),
		FileSizeBytes:  objStore.getObjectSize(parquetPath),
		LowerBounds: map[int32][]byte{
			catalog.FieldIDOffset: int64ToBytes(minOffset),
		},
		UpperBounds: map[int32][]byte{
			catalog.FieldIDOffset: int64ToBytes(lastRecordOffset),
		},
	}

	snapshot, err := icebergTable.AppendFiles(ctx, []catalog.DataFile{dataFile}, nil)
	if err != nil {
		t.Fatalf("failed to append files to Iceberg table: %v", err)
	}
	t.Logf("  Created Iceberg snapshot: ID=%d, added-data-files=%s",
		snapshot.SnapshotID, snapshot.Summary["added-data-files"])

	// Step 3: Query Iceberg table snapshot
	t.Log("Step 3: Querying Iceberg table snapshot...")
	currentSnapshot, err := icebergTable.CurrentSnapshot(ctx)
	if err != nil {
		t.Fatalf("failed to get current snapshot: %v", err)
	}
	if currentSnapshot == nil {
		t.Fatal("expected non-nil current snapshot")
	}
	t.Logf("  Current snapshot ID: %d", currentSnapshot.SnapshotID)
	t.Logf("  Snapshot operation: %s", currentSnapshot.Operation)

	// Get the data files from the table
	trackingTable := icebergTable.(*icebergMatchTestTable)
	dataFiles := trackingTable.GetDataFiles()
	if len(dataFiles) == 0 {
		t.Fatal("expected at least one data file in Iceberg table")
	}
	t.Logf("  Found %d data file(s) in table", len(dataFiles))

	// Step 4: Verify data files cover expected offsets
	t.Log("Step 4: Verifying data files cover expected offsets...")

	// Verify the data file covers the expected offset range
	df := dataFiles[0]
	t.Logf("  Data file: %s", df.Path)
	t.Logf("    Record count: %d", df.RecordCount)
	t.Logf("    File size: %d bytes", df.FileSizeBytes)

	// Verify offset bounds
	if df.LowerBounds != nil {
		lowerOffset := bytesToInt64(df.LowerBounds[catalog.FieldIDOffset])
		t.Logf("    Lower offset bound: %d", lowerOffset)
		if lowerOffset != 0 {
			t.Errorf("expected lower offset bound 0, got %d", lowerOffset)
		}
	}
	if df.UpperBounds != nil {
		upperOffset := bytesToInt64(df.UpperBounds[catalog.FieldIDOffset])
		t.Logf("    Upper offset bound: %d", upperOffset)
		expectedUpperOffset := int64(totalRecords - 1)
		if upperOffset != expectedUpperOffset {
			t.Errorf("expected upper offset bound %d, got %d", expectedUpperOffset, upperOffset)
		}
	}

	// Verify record count matches
	if df.RecordCount != int64(totalRecords) {
		t.Errorf("data file record count %d does not match produced records %d",
			df.RecordCount, totalRecords)
	}

	// Verify the data file path matches the object store
	if _, err := objStore.Head(ctx, df.Path); err != nil {
		t.Errorf("data file %s not found in object store: %v", df.Path, err)
	}

	// Verify stream HWM matches
	hwm, _, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM: %v", err)
	}
	if hwm != int64(totalRecords) {
		t.Errorf("HWM %d does not match total records %d", hwm, totalRecords)
	}
	t.Logf("  Stream HWM verified: %d", hwm)

	t.Log("All verifications passed - Parquet files in Iceberg table match stream offsets")
}

// TestIcebergParquetMatch_MultipleCompactions verifies that multiple compaction
// cycles result in multiple data files in the Iceberg table with correct offset ranges.
func TestIcebergParquetMatch_MultipleCompactions(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newIcebergTestObjectStore()
	ctx := context.Background()

	topicName := "multi-compact-topic"

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, topicName, 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	icebergCatalog := newIcebergMatchTestCatalog()
	tableIdentifier := catalog.TableIdentifier{
		Namespace: []string{"dray"},
		Name:      topicName,
	}
	icebergTable, err := icebergCatalog.CreateTableIfMissing(ctx, tableIdentifier, catalog.CreateTableOptions{
		Schema: catalog.DefaultSchema(),
	})
	if err != nil {
		t.Fatalf("failed to create Iceberg table: %v", err)
	}

	// Run two produce-compact cycles
	cycles := []struct {
		recordCount int
		startOffset int64
	}{
		{10, 0},
		{15, 10},
	}

	for i, cycle := range cycles {
		t.Logf("Cycle %d: producing %d records starting at offset %d", i+1, cycle.recordCount, cycle.startOffset)

		// Produce records
		produceReq := buildCompactionTestProduceRequest(topicName, 0, cycle.recordCount, i)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)
		if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("produce failed in cycle %d", i+1)
		}

		// Run compaction
		parquetPath, minOff, maxOff, recCount := runCompactionWithIceberg(t, ctx, streamID, metaStore, objStore, 0, topicName)

		// Append to Iceberg (maxOff is exclusive, so last record offset is maxOff-1)
		lastOff := maxOff - 1
		dataFile := catalog.DataFile{
			Path:           parquetPath,
			Format:         catalog.FormatParquet,
			PartitionValue: 0,
			RecordCount:    int64(recCount),
			FileSizeBytes:  objStore.getObjectSize(parquetPath),
			LowerBounds: map[int32][]byte{
				catalog.FieldIDOffset: int64ToBytes(minOff),
			},
			UpperBounds: map[int32][]byte{
				catalog.FieldIDOffset: int64ToBytes(lastOff),
			},
		}

		_, err := icebergTable.AppendFiles(ctx, []catalog.DataFile{dataFile}, nil)
		if err != nil {
			t.Fatalf("failed to append files in cycle %d: %v", i+1, err)
		}
	}

	// Verify all data files in Iceberg table
	trackingTable := icebergTable.(*icebergMatchTestTable)
	dataFiles := trackingTable.GetDataFiles()

	if len(dataFiles) != len(cycles) {
		t.Fatalf("expected %d data files, got %d", len(cycles), len(dataFiles))
	}

	// Verify each data file covers its expected offset range
	// Note: For consecutive compaction, each cycle's offsets are relative to its own batch,
	// but in reality each cycle covers only the new records from that cycle.
	// We need to track cumulative offsets across cycles.
	cumulativeOffset := int64(0)
	for i, df := range dataFiles {
		lowerOffset := bytesToInt64(df.LowerBounds[catalog.FieldIDOffset])
		upperOffset := bytesToInt64(df.UpperBounds[catalog.FieldIDOffset])

		expectedLower := cumulativeOffset
		expectedUpper := cumulativeOffset + int64(cycles[i].recordCount) - 1

		if lowerOffset != expectedLower {
			t.Errorf("data file %d: expected lower offset %d, got %d", i, expectedLower, lowerOffset)
		}
		if upperOffset != expectedUpper {
			t.Errorf("data file %d: expected upper offset %d, got %d", i, expectedUpper, upperOffset)
		}
		if df.RecordCount != int64(cycles[i].recordCount) {
			t.Errorf("data file %d: expected %d records, got %d", i, cycles[i].recordCount, df.RecordCount)
		}

		t.Logf("Data file %d: offsets %d-%d, %d records - OK", i, lowerOffset, upperOffset, df.RecordCount)
		cumulativeOffset += int64(cycles[i].recordCount)
	}

	// Verify snapshots
	snapshots, err := icebergTable.Snapshots(ctx)
	if err != nil {
		t.Fatalf("failed to get snapshots: %v", err)
	}
	if len(snapshots) != len(cycles) {
		t.Errorf("expected %d snapshots, got %d", len(cycles), len(snapshots))
	}

	t.Logf("Verified %d compaction cycles with correct Iceberg commits", len(cycles))
}

// TestIcebergParquetMatch_OffsetContinuity verifies that consecutive compaction
// cycles produce data files with non-overlapping, contiguous offset ranges.
func TestIcebergParquetMatch_OffsetContinuity(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newIcebergTestObjectStore()
	ctx := context.Background()

	topicName := "offset-continuity-topic"

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, topicName, 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	icebergCatalog := newIcebergMatchTestCatalog()
	tableIdentifier := catalog.TableIdentifier{
		Namespace: []string{"dray"},
		Name:      topicName,
	}
	icebergTable, err := icebergCatalog.CreateTableIfMissing(ctx, tableIdentifier, catalog.CreateTableOptions{
		Schema: catalog.DefaultSchema(),
	})
	if err != nil {
		t.Fatalf("failed to create Iceberg table: %v", err)
	}

	// Produce and compact 3 batches
	recordCounts := []int{5, 8, 12}
	var allDataFiles []catalog.DataFile

	runningOffset := int64(0)
	for i, count := range recordCounts {
		// Produce
		produceReq := buildCompactionTestProduceRequest(topicName, 0, count, i)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)
		if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("produce failed at batch %d", i)
		}

		// Compact (maxOff is exclusive, so last record offset is maxOff-1)
		parquetPath, minOff, maxOff, recCount := runCompactionWithIceberg(t, ctx, streamID, metaStore, objStore, 0, topicName)
		lastOff := maxOff - 1

		dataFile := catalog.DataFile{
			Path:           parquetPath,
			Format:         catalog.FormatParquet,
			PartitionValue: 0,
			RecordCount:    int64(recCount),
			FileSizeBytes:  objStore.getObjectSize(parquetPath),
			LowerBounds: map[int32][]byte{
				catalog.FieldIDOffset: int64ToBytes(minOff),
			},
			UpperBounds: map[int32][]byte{
				catalog.FieldIDOffset: int64ToBytes(lastOff),
			},
		}
		allDataFiles = append(allDataFiles, dataFile)

		_, err := icebergTable.AppendFiles(ctx, []catalog.DataFile{dataFile}, nil)
		if err != nil {
			t.Fatalf("failed to append files at batch %d: %v", i, err)
		}

		runningOffset += int64(count)
	}

	// Verify offset continuity
	trackingTable := icebergTable.(*icebergMatchTestTable)
	dataFiles := trackingTable.GetDataFiles()

	var prevUpperOffset int64 = -1
	for i, df := range dataFiles {
		lowerOffset := bytesToInt64(df.LowerBounds[catalog.FieldIDOffset])
		upperOffset := bytesToInt64(df.UpperBounds[catalog.FieldIDOffset])

		if i == 0 {
			if lowerOffset != 0 {
				t.Errorf("first data file should start at offset 0, got %d", lowerOffset)
			}
		} else {
			expectedLower := prevUpperOffset + 1
			if lowerOffset != expectedLower {
				t.Errorf("data file %d: expected lower offset %d (prev upper + 1), got %d", i, expectedLower, lowerOffset)
			}
		}

		if upperOffset < lowerOffset {
			t.Errorf("data file %d: upper offset %d < lower offset %d", i, upperOffset, lowerOffset)
		}

		t.Logf("Data file %d: offsets %d-%d (contiguous: %v)", i, lowerOffset, upperOffset, lowerOffset == prevUpperOffset+1 || i == 0)
		prevUpperOffset = upperOffset
	}

	// Verify total offset range
	if prevUpperOffset != int64(runningOffset-1) {
		t.Errorf("final upper offset %d does not match expected %d", prevUpperOffset, runningOffset-1)
	}

	t.Log("Offset continuity verified across all data files")
}

// icebergMatchTestCatalog is a mock catalog that tracks data files for verification.
type icebergMatchTestCatalog struct {
	tables map[string]*icebergMatchTestTable
}

func newIcebergMatchTestCatalog() *icebergMatchTestCatalog {
	return &icebergMatchTestCatalog{
		tables: make(map[string]*icebergMatchTestTable),
	}
}

func (c *icebergMatchTestCatalog) LoadTable(ctx context.Context, identifier catalog.TableIdentifier) (catalog.Table, error) {
	t, ok := c.tables[identifier.String()]
	if !ok {
		return nil, catalog.ErrTableNotFound
	}
	return t, nil
}

func (c *icebergMatchTestCatalog) CreateTableIfMissing(ctx context.Context, identifier catalog.TableIdentifier, opts catalog.CreateTableOptions) (catalog.Table, error) {
	key := identifier.String()
	if t, ok := c.tables[key]; ok {
		return t, nil
	}
	t := &icebergMatchTestTable{
		identifier: identifier,
		schema:     opts.Schema,
		props:      opts.Properties,
		location:   opts.Location,
		snapshots:  nil,
		dataFiles:  nil,
	}
	c.tables[key] = t
	return t, nil
}

func (c *icebergMatchTestCatalog) GetCurrentSnapshot(ctx context.Context, identifier catalog.TableIdentifier) (*catalog.Snapshot, error) {
	t, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return t.CurrentSnapshot(ctx)
}

func (c *icebergMatchTestCatalog) AppendDataFiles(ctx context.Context, identifier catalog.TableIdentifier, files []catalog.DataFile, opts *catalog.AppendFilesOptions) (*catalog.Snapshot, error) {
	t, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return t.AppendFiles(ctx, files, opts)
}

func (c *icebergMatchTestCatalog) DropTable(ctx context.Context, identifier catalog.TableIdentifier) error {
	key := identifier.String()
	if _, ok := c.tables[key]; !ok {
		return catalog.ErrTableNotFound
	}
	delete(c.tables, key)
	return nil
}

func (c *icebergMatchTestCatalog) ListTables(ctx context.Context, namespace []string) ([]catalog.TableIdentifier, error) {
	var result []catalog.TableIdentifier
	for _, t := range c.tables {
		result = append(result, t.identifier)
	}
	return result, nil
}

func (c *icebergMatchTestCatalog) TableExists(ctx context.Context, identifier catalog.TableIdentifier) (bool, error) {
	_, ok := c.tables[identifier.String()]
	return ok, nil
}

func (c *icebergMatchTestCatalog) Close() error {
	return nil
}

var _ catalog.Catalog = (*icebergMatchTestCatalog)(nil)

// icebergMatchTestTable is a mock table that tracks appended data files.
type icebergMatchTestTable struct {
	identifier catalog.TableIdentifier
	schema     catalog.Schema
	props      catalog.TableProperties
	location   string
	snapshots  []catalog.Snapshot
	dataFiles  []catalog.DataFile
}

func (t *icebergMatchTestTable) Identifier() catalog.TableIdentifier { return t.identifier }
func (t *icebergMatchTestTable) Schema() catalog.Schema              { return t.schema }
func (t *icebergMatchTestTable) Properties() catalog.TableProperties { return t.props }
func (t *icebergMatchTestTable) Location() string                    { return t.location }

func (t *icebergMatchTestTable) CurrentSnapshot(ctx context.Context) (*catalog.Snapshot, error) {
	if len(t.snapshots) == 0 {
		return nil, catalog.ErrSnapshotNotFound
	}
	return &t.snapshots[len(t.snapshots)-1], nil
}

func (t *icebergMatchTestTable) Snapshots(ctx context.Context) ([]catalog.Snapshot, error) {
	return t.snapshots, nil
}

func (t *icebergMatchTestTable) AppendFiles(ctx context.Context, files []catalog.DataFile, opts *catalog.AppendFilesOptions) (*catalog.Snapshot, error) {
	// Track the data files
	t.dataFiles = append(t.dataFiles, files...)

	var parentID *int64
	nextSnapshotID := int64(1)
	if len(t.snapshots) > 0 {
		parentID = &t.snapshots[len(t.snapshots)-1].SnapshotID
		nextSnapshotID = t.snapshots[len(t.snapshots)-1].SnapshotID + 1
	}

	addedRecords := int64(0)
	for _, f := range files {
		addedRecords += f.RecordCount
	}

	summary := map[string]string{
		"added-data-files": strconv.Itoa(len(files)),
		"added-records":    strconv.FormatInt(addedRecords, 10),
	}
	if opts != nil && opts.SnapshotProperties != nil {
		for k, v := range opts.SnapshotProperties {
			summary[k] = v
		}
	}

	newSnapshot := catalog.Snapshot{
		SnapshotID:       nextSnapshotID,
		ParentSnapshotID: parentID,
		TimestampMs:      time.Now().UnixMilli(),
		Operation:        catalog.OpAppend,
		Summary:          summary,
	}
	t.snapshots = append(t.snapshots, newSnapshot)
	return &t.snapshots[len(t.snapshots)-1], nil
}

func (t *icebergMatchTestTable) Refresh(ctx context.Context) error {
	return nil
}

// GetDataFiles returns all data files appended to this table.
func (t *icebergMatchTestTable) GetDataFiles() []catalog.DataFile {
	return t.dataFiles
}

var _ catalog.Table = (*icebergMatchTestTable)(nil)

// icebergTestObjectStore extends the compaction test object store with size lookup.
type icebergTestObjectStore struct {
	objects map[string][]byte
}

func newIcebergTestObjectStore() *icebergTestObjectStore {
	return &icebergTestObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *icebergTestObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.objects[key] = data
	return nil
}

func (m *icebergTestObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *icebergTestObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *icebergTestObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if start < 0 || start >= int64(len(data)) {
		return nil, objectstore.ErrInvalidRange
	}
	if end < 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return io.NopCloser(bytes.NewReader(data[start:end+1])), nil
}

func (m *icebergTestObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *icebergTestObjectStore) Delete(ctx context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

func (m *icebergTestObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	var result []objectstore.ObjectMeta
	for key, data := range m.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, objectstore.ObjectMeta{
				Key:  key,
				Size: int64(len(data)),
			})
		}
	}
	return result, nil
}

func (m *icebergTestObjectStore) Close() error {
	return nil
}

func (m *icebergTestObjectStore) getObjectSize(key string) int64 {
	if data, ok := m.objects[key]; ok {
		return int64(len(data))
	}
	return 0
}

var _ objectstore.Store = (*icebergTestObjectStore)(nil)

// runCompactionWithIceberg runs compaction and returns details for Iceberg commit.
func runCompactionWithIceberg(t *testing.T, ctx context.Context, streamID string, metaStore metadata.MetadataStore, objStore *icebergTestObjectStore, partition int32, topicName string) (parquetPath string, minOffset, maxOffset int64, recordCount int64) {
	t.Helper()

	// Get all WAL index entries using metadata store List
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	// Parse entries and collect WAL entries
	var walEntries []*index.IndexEntry
	var walIndexKeys []string
	minOffset, maxOffset = 0, 0

	for i, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
			walIndexKeys = append(walIndexKeys, kv.Key)
			if len(walEntries) == 1 || entry.StartOffset < minOffset {
				minOffset = entry.StartOffset
			}
			if entry.EndOffset > maxOffset {
				maxOffset = entry.EndOffset
			}
		}
		_ = i
	}

	if len(walEntries) == 0 {
		t.Fatal("no WAL entries to compact")
	}

	// Convert WAL to Parquet
	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, partition)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	// Write Parquet to object store
	date := time.Now().Format("2006-01-02")
	parquetID := worker.GenerateParquetID()
	parquetPath = worker.GenerateParquetPath(topicName, partition, date, parquetID)
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Create Parquet index entry
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      minOffset,
		EndOffset:        maxOffset,
		FileType:         index.FileTypeParquet,
		RecordCount:      uint32(convertResult.RecordCount),
		MessageCount:     uint32(convertResult.RecordCount),
		CreatedAtMs:      time.Now().UnixMilli(),
		MinTimestampMs:   convertResult.Stats.MinTimestamp,
		MaxTimestampMs:   convertResult.Stats.MaxTimestamp,
		ParquetPath:      parquetPath,
		ParquetSizeBytes: uint64(len(convertResult.ParquetData)),
	}

	// Execute index swap
	swapper := compaction.NewIndexSwapper(metaStore)
	_, err = swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   0,
	})
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}

	return parquetPath, minOffset, maxOffset, convertResult.RecordCount
}

// Helper functions
func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func bytesToInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}
