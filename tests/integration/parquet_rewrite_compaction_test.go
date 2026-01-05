package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
)

// TestParquetRewriteCompaction verifies that the scheduler plans and executes
// Parquet small-file rewrite compaction correctly.
func TestParquetRewriteCompaction(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	objStore := newParquetRewriteTestObjectStore()
	streamManager := index.NewStreamManager(metaStore)

	// Create a stream
	streamID := "rewrite-test-stream-001"
	if err := streamManager.CreateStreamWithID(ctx, streamID, "rewrite-test-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create 5 small Parquet files - this should trigger rewrite compaction
	numFiles := 5
	recordsPerFile := 10
	oldTime := time.Now().Add(-20 * time.Minute).UnixMilli()

	var allExpectedRecords []worker.Record
	for fileIdx := 0; fileIdx < numFiles; fileIdx++ {
		// Create records for this file
		records := make([]worker.Record, recordsPerFile)
		startOffset := int64(fileIdx * recordsPerFile)
		for i := 0; i < recordsPerFile; i++ {
			records[i] = worker.Record{
				Partition: 0,
				Offset:    startOffset + int64(i),
				Timestamp: oldTime + int64(i),
				Key:       []byte("key"),
				Value:     []byte("value"),
			}
		}
		allExpectedRecords = append(allExpectedRecords, records...)

		// Write to Parquet
		parquetData, _, err := worker.WriteToBuffer(worker.BuildParquetSchema(nil), records)
		if err != nil {
			t.Fatalf("WriteToBuffer failed: %v", err)
		}

		// Store in object store
		parquetPath := worker.GenerateParquetPath("rewrite-test-topic", 0, "2024-01-01", worker.GenerateParquetID())
		objStore.objects[parquetPath] = parquetData

		// Create index entry
		entry := index.IndexEntry{
			StreamID:         streamID,
			StartOffset:      startOffset,
			EndOffset:        startOffset + int64(recordsPerFile),
			FileType:         index.FileTypeParquet,
			RecordCount:      uint32(recordsPerFile),
			MessageCount:     uint32(recordsPerFile),
			CreatedAtMs:      oldTime,
			MinTimestampMs:   oldTime,
			MaxTimestampMs:   oldTime + int64(recordsPerFile-1),
			ParquetPath:      parquetPath,
			ParquetSizeBytes: uint64(len(parquetData)),
			CumulativeSize:   int64((fileIdx + 1) * len(parquetData)),
		}

		// Write index entry
		indexKey, err := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		if err != nil {
			t.Fatalf("failed to create index key: %v", err)
		}
		entryBytes, _ := json.Marshal(entry)
		if _, err := metaStore.Put(ctx, indexKey, entryBytes); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	t.Logf("Created %d small Parquet files with %d records each", numFiles, recordsPerFile)

	// Verify initial state - should have 5 index entries
	entriesBefore, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list entries: %v", err)
	}
	if len(entriesBefore) != numFiles {
		t.Fatalf("expected %d index entries before rewrite, got %d", numFiles, len(entriesBefore))
	}

	// Create scheduler and plan compaction
	schedulerCfg := compaction.DefaultSchedulerConfig()
	schedulerCfg.EnableWALCompaction = false // We only have Parquet entries
	schedulerCfg.ParquetRewriteConfig.MinFiles = 4
	schedulerCfg.ParquetRewriteConfig.MinAgeMs = int64((10 * time.Minute).Milliseconds())
	schedulerCfg.ParquetRewriteConfig.SmallFileThresholdBytes = 10 * 1024 * 1024 // 10MB threshold

	scheduler := compaction.NewScheduler(schedulerCfg, streamManager)
	plan, err := scheduler.Plan(ctx, streamID)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan == nil {
		t.Fatal("Plan() returned nil, expected Parquet rewrite plan")
	}
	if plan.Type != compaction.CompactionTypeParquetRewrite {
		t.Fatalf("Plan().Type = %s, expected %s", plan.Type, compaction.CompactionTypeParquetRewrite)
	}
	if len(plan.Entries) < 4 {
		t.Fatalf("Plan selected %d files, expected at least 4", len(plan.Entries))
	}

	t.Logf("Scheduler planned Parquet rewrite with %d files covering offsets %d-%d",
		len(plan.Entries), plan.StartOffset, plan.EndOffset)

	// Execute the rewrite compaction
	runParquetRewriteCompaction(t, ctx, streamID, metaStore, objStore, plan)

	// Verify after rewrite - should have fewer index entries
	entriesAfter, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list entries after rewrite: %v", err)
	}

	// We expect the number of entries to decrease
	if len(entriesAfter) >= numFiles {
		t.Errorf("expected fewer index entries after rewrite, got %d (was %d)", len(entriesAfter), numFiles)
	}
	t.Logf("After rewrite: %d index entries (was %d)", len(entriesAfter), numFiles)

	// Find the merged Parquet entry
	var mergedEntry *index.IndexEntry
	for _, e := range entriesAfter {
		if e.FileType == index.FileTypeParquet && e.StartOffset == plan.StartOffset && e.EndOffset == plan.EndOffset {
			mergedEntry = &e
			break
		}
	}
	if mergedEntry == nil {
		t.Fatal("Could not find merged Parquet entry")
	}

	t.Logf("Merged Parquet entry: path=%s, size=%d bytes, records=%d",
		mergedEntry.ParquetPath, mergedEntry.ParquetSizeBytes, mergedEntry.RecordCount)

	// Verify the merged Parquet file contains correct data
	fetcher := fetch.NewFetcher(objStore, streamManager)
	parquetReader := fetch.NewParquetReader(objStore)

	fetchResult, err := parquetReader.ReadBatches(ctx, mergedEntry, plan.StartOffset, 1024*1024)
	if err != nil {
		t.Fatalf("ReadBatches() error = %v", err)
	}

	t.Logf("Fetched %d batches from merged Parquet, offsets %d-%d",
		len(fetchResult.Batches), fetchResult.StartOffset, fetchResult.EndOffset)

	// Verify the record count matches
	expectedRecordCount := int(plan.EndOffset - plan.StartOffset)
	if fetchResult.EndOffset-fetchResult.StartOffset != int64(expectedRecordCount) {
		t.Errorf("expected %d records, got offset range of %d",
			expectedRecordCount, fetchResult.EndOffset-fetchResult.StartOffset)
	}

	_ = fetcher // Used implicitly through the test
	t.Log("Parquet rewrite compaction completed successfully")
}

// TestParquetRewriteCompaction_NotEnoughFiles verifies that rewrite is not
// planned when there are fewer files than the minimum threshold.
func TestParquetRewriteCompaction_NotEnoughFiles(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	objStore := newParquetRewriteTestObjectStore()
	streamManager := index.NewStreamManager(metaStore)

	streamID := "rewrite-test-stream-002"
	if err := streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create only 2 small Parquet files - below the 4 file minimum
	numFiles := 2
	recordsPerFile := 10
	oldTime := time.Now().Add(-20 * time.Minute).UnixMilli()

	for fileIdx := 0; fileIdx < numFiles; fileIdx++ {
		records := make([]worker.Record, recordsPerFile)
		startOffset := int64(fileIdx * recordsPerFile)
		for i := 0; i < recordsPerFile; i++ {
			records[i] = worker.Record{
				Partition: 0,
				Offset:    startOffset + int64(i),
				Timestamp: oldTime + int64(i),
			}
		}

		parquetData, _, err := worker.WriteToBuffer(worker.BuildParquetSchema(nil), records)
		if err != nil {
			t.Fatalf("WriteToBuffer failed: %v", err)
		}

		parquetPath := worker.GenerateParquetPath("test-topic", 0, "2024-01-01", worker.GenerateParquetID())
		objStore.objects[parquetPath] = parquetData

		entry := index.IndexEntry{
			StreamID:         streamID,
			StartOffset:      startOffset,
			EndOffset:        startOffset + int64(recordsPerFile),
			FileType:         index.FileTypeParquet,
			RecordCount:      uint32(recordsPerFile),
			CreatedAtMs:      oldTime,
			ParquetPath:      parquetPath,
			ParquetSizeBytes: uint64(len(parquetData)),
			CumulativeSize:   int64((fileIdx + 1) * len(parquetData)),
		}

		indexKey, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		entryBytes, _ := json.Marshal(entry)
		metaStore.Put(ctx, indexKey, entryBytes)
	}

	// Plan should return nil (no rewrite needed)
	schedulerCfg := compaction.DefaultSchedulerConfig()
	schedulerCfg.EnableWALCompaction = false
	schedulerCfg.ParquetRewriteConfig.MinFiles = 4

	scheduler := compaction.NewScheduler(schedulerCfg, streamManager)
	plan, err := scheduler.Plan(ctx, streamID)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan != nil {
		t.Errorf("Plan() = %v, expected nil when only %d files exist (below threshold)", plan.Type, numFiles)
	}

	t.Logf("Correctly skipped rewrite compaction with only %d files", numFiles)
}

// TestParquetRewriteCompaction_DataConsistency verifies that all records are
// preserved correctly after rewrite compaction.
func TestParquetRewriteCompaction_DataConsistency(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	objStore := newParquetRewriteTestObjectStore()
	streamManager := index.NewStreamManager(metaStore)

	streamID := "rewrite-test-stream-003"
	if err := streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create 4 small Parquet files with distinctive data
	numFiles := 4
	recordsPerFile := 5
	oldTime := time.Now().Add(-20 * time.Minute).UnixMilli()

	var originalRecords []worker.Record
	for fileIdx := 0; fileIdx < numFiles; fileIdx++ {
		records := make([]worker.Record, recordsPerFile)
		startOffset := int64(fileIdx * recordsPerFile)
		for i := 0; i < recordsPerFile; i++ {
			offset := startOffset + int64(i)
			records[i] = worker.Record{
				Partition: 0,
				Offset:    offset,
				Timestamp: oldTime + offset,
				Key:       []byte("key"),
				Value:     []byte("value"),
			}
			originalRecords = append(originalRecords, records[i])
		}

		parquetData, _, err := worker.WriteToBuffer(worker.BuildParquetSchema(nil), records)
		if err != nil {
			t.Fatalf("WriteToBuffer failed: %v", err)
		}

		parquetPath := worker.GenerateParquetPath("test-topic", 0, "2024-01-01", worker.GenerateParquetID())
		objStore.objects[parquetPath] = parquetData

		entry := index.IndexEntry{
			StreamID:         streamID,
			StartOffset:      startOffset,
			EndOffset:        startOffset + int64(recordsPerFile),
			FileType:         index.FileTypeParquet,
			RecordCount:      uint32(recordsPerFile),
			CreatedAtMs:      oldTime,
			MinTimestampMs:   oldTime + startOffset,
			MaxTimestampMs:   oldTime + startOffset + int64(recordsPerFile-1),
			ParquetPath:      parquetPath,
			ParquetSizeBytes: uint64(len(parquetData)),
			CumulativeSize:   int64((fileIdx + 1) * len(parquetData)),
		}

		indexKey, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		entryBytes, _ := json.Marshal(entry)
		metaStore.Put(ctx, indexKey, entryBytes)
	}

	// Plan and execute rewrite
	schedulerCfg := compaction.DefaultSchedulerConfig()
	schedulerCfg.EnableWALCompaction = false
	schedulerCfg.ParquetRewriteConfig.MinFiles = 4
	schedulerCfg.ParquetRewriteConfig.MinAgeMs = int64((10 * time.Minute).Milliseconds())
	schedulerCfg.ParquetRewriteConfig.SmallFileThresholdBytes = 10 * 1024 * 1024

	scheduler := compaction.NewScheduler(schedulerCfg, streamManager)
	plan, err := scheduler.Plan(ctx, streamID)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan == nil {
		t.Fatal("Plan() returned nil")
	}

	runParquetRewriteCompaction(t, ctx, streamID, metaStore, objStore, plan)

	// Read merged Parquet and verify all records are present
	entriesAfter, _ := streamManager.ListIndexEntries(ctx, streamID, 0)
	var mergedEntry *index.IndexEntry
	for _, e := range entriesAfter {
		if e.FileType == index.FileTypeParquet {
			mergedEntry = &e
			break
		}
	}
	if mergedEntry == nil {
		t.Fatal("Could not find merged entry")
	}

	// Read merged Parquet data
	mergedData := objStore.objects[mergedEntry.ParquetPath]
	reader, err := worker.NewReader(mergedData)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer reader.Close()

	mergedRecords, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	// Verify record count
	if len(mergedRecords) != len(originalRecords) {
		t.Fatalf("expected %d records, got %d", len(originalRecords), len(mergedRecords))
	}

	// Verify each record
	for i, orig := range originalRecords {
		merged := mergedRecords[i]
		if orig.Offset != merged.Offset {
			t.Errorf("record %d: offset mismatch - original=%d, merged=%d", i, orig.Offset, merged.Offset)
		}
		if orig.Timestamp != merged.Timestamp {
			t.Errorf("record %d: timestamp mismatch - original=%d, merged=%d", i, orig.Timestamp, merged.Timestamp)
		}
		if !bytes.Equal(orig.Key, merged.Key) {
			t.Errorf("record %d: key mismatch", i)
		}
		if !bytes.Equal(orig.Value, merged.Value) {
			t.Errorf("record %d: value mismatch", i)
		}
	}

	t.Logf("Data consistency verified: all %d records preserved correctly", len(originalRecords))
}

// runParquetRewriteCompaction executes a Parquet rewrite compaction based on the plan.
func runParquetRewriteCompaction(t *testing.T, ctx context.Context, streamID string, metaStore metadata.MetadataStore, objStore *parquetRewriteTestObjectStore, plan *compaction.CompactionPlan) {
	t.Helper()

	if plan.Type != compaction.CompactionTypeParquetRewrite {
		t.Fatalf("expected PARQUET_REWRITE plan, got %s", plan.Type)
	}

	// Collect Parquet entries from plan
	parquetEntries := make([]*index.IndexEntry, len(plan.Entries))
	parquetIndexKeys := make([]string, len(plan.Entries))

	for i := range plan.Entries {
		entry := plan.Entries[i]
		parquetEntries[i] = &entry
		indexKey, err := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		if err != nil {
			t.Fatalf("failed to create index key: %v", err)
		}
		parquetIndexKeys[i] = indexKey
	}

	t.Logf("Merging %d Parquet files covering offsets %d-%d", len(parquetEntries), plan.StartOffset, plan.EndOffset)

	// Merge Parquet files
	merger := worker.NewParquetMerger(objStore)
	mergeResult, err := merger.Merge(ctx, parquetEntries)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}

	// Write merged Parquet
	parquetPath := worker.GenerateParquetPath("test-topic", 0, time.Now().Format("2006-01-02"), worker.GenerateParquetID())
	if err := merger.WriteParquetToStorage(ctx, parquetPath, mergeResult.ParquetData); err != nil {
		t.Fatalf("WriteParquetToStorage() error = %v", err)
	}

	t.Logf("Wrote merged Parquet: %s (%d bytes, %d records)", parquetPath, len(mergeResult.ParquetData), mergeResult.RecordCount)

	// Create merged Parquet index entry
	mergedEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      plan.StartOffset,
		EndOffset:        plan.EndOffset,
		FileType:         index.FileTypeParquet,
		RecordCount:      uint32(mergeResult.RecordCount),
		MessageCount:     uint32(mergeResult.RecordCount),
		CreatedAtMs:      time.Now().UnixMilli(),
		MinTimestampMs:   mergeResult.Stats.MinTimestamp,
		MaxTimestampMs:   mergeResult.Stats.MaxTimestamp,
		ParquetPath:      parquetPath,
		ParquetSizeBytes: uint64(len(mergeResult.ParquetData)),
	}

	// Execute index swap (replacing old Parquet entries with new merged entry)
	swapper := compaction.NewIndexSwapper(metaStore)
	swapResult, err := swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:         streamID,
		ParquetIndexKeys: parquetIndexKeys,
		ParquetEntry:     mergedEntry,
		MetaDomain:       0,
	})
	if err != nil {
		t.Fatalf("Swap() error = %v", err)
	}

	t.Logf("Index swap complete: new key=%s", swapResult.NewIndexKey)
}

// parquetRewriteTestObjectStore is a mock object store for rewrite tests.
type parquetRewriteTestObjectStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func newParquetRewriteTestObjectStore() *parquetRewriteTestObjectStore {
	return &parquetRewriteTestObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *parquetRewriteTestObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return nil
}

func (m *parquetRewriteTestObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *parquetRewriteTestObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *parquetRewriteTestObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
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
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (m *parquetRewriteTestObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{Key: key, Size: int64(len(data))}, nil
}

func (m *parquetRewriteTestObjectStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func (m *parquetRewriteTestObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []objectstore.ObjectMeta
	for key, data := range m.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, objectstore.ObjectMeta{Key: key, Size: int64(len(data))})
		}
	}
	return result, nil
}

func (m *parquetRewriteTestObjectStore) Close() error {
	return nil
}
