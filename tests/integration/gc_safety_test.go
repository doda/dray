package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/gc"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestGCSafety_NeverDeletesLiveData verifies that the garbage collection system
// never deletes data that is still referenced by active index entries.
//
// This test follows the task verification steps:
// 1. Produce records to stream
// 2. Run compaction
// 3. Verify fetch still works
// 4. Trigger GC
// 5. Verify fetch still works after GC
func TestGCSafety_NeverDeletesLiveData(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newGCSafetyTestObjectStore()
	ctx := context.Background()

	// Create topic with one partition
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "gc-safety-test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "gc-safety-test-topic", 0); err != nil {
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

	// Step 1: Produce records to stream
	t.Log("Step 1: Produce records to stream")
	recordsPerBatch := []int{3, 5, 7} // 15 total records across 3 batches
	var totalRecords int
	for _, count := range recordsPerBatch {
		totalRecords += count
	}

	for i, recordCount := range recordsPerBatch {
		produceReq := buildGCSafetyProduceRequest("gc-safety-test-topic", 0, recordCount, i)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("produce batch %d failed with error code %d", i, partResp.ErrorCode)
		}
		t.Logf("  Produced batch %d: %d records, BaseOffset=%d", i, recordCount, partResp.BaseOffset)
	}

	// Verify fetch works before compaction
	t.Log("  Verifying fetch works before compaction...")
	beforeCompactionRecords := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "gc-safety-test-topic", 0, 0)
	if len(beforeCompactionRecords) != totalRecords {
		t.Fatalf("expected %d records before compaction, got %d", totalRecords, len(beforeCompactionRecords))
	}
	t.Logf("  Fetched %d records before compaction", len(beforeCompactionRecords))

	// Count WAL objects before compaction
	walObjectsBefore := objStore.countWALObjects()
	t.Logf("  WAL objects before compaction: %d", walObjectsBefore)

	// Step 2: Run compaction (WAL -> Parquet)
	t.Log("Step 2: Run compaction (WAL -> Parquet)")
	runGCSafetyCompaction(t, ctx, streamID, metaStore, objStore, 0)
	t.Log("  Compaction complete")

	// Verify index entries are now Parquet type
	entries, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 index entry after compaction, got %d", len(entries))
	}
	if entries[0].FileType != index.FileTypeParquet {
		t.Fatalf("expected Parquet entry after compaction, got %s", entries[0].FileType)
	}
	t.Logf("  Index entry: FileType=%s, StartOffset=%d, EndOffset=%d, Path=%s",
		entries[0].FileType, entries[0].StartOffset, entries[0].EndOffset, entries[0].ParquetPath)

	// Step 3: Verify fetch still works after compaction
	t.Log("Step 3: Verify fetch still works after compaction")
	afterCompactionRecords := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "gc-safety-test-topic", 0, 0)
	if len(afterCompactionRecords) != totalRecords {
		t.Fatalf("expected %d records after compaction, got %d", totalRecords, len(afterCompactionRecords))
	}

	// Verify records match
	for i := 0; i < totalRecords; i++ {
		before := beforeCompactionRecords[i]
		after := afterCompactionRecords[i]
		if before.Offset != after.Offset {
			t.Errorf("record %d: offset mismatch - before=%d, after=%d", i, before.Offset, after.Offset)
		}
		if !bytes.Equal(before.Key, after.Key) {
			t.Errorf("record %d: key mismatch", i)
		}
		if !bytes.Equal(before.Value, after.Value) {
			t.Errorf("record %d: value mismatch", i)
		}
	}
	t.Logf("  Fetched %d records after compaction - all match", len(afterCompactionRecords))

	// Count Parquet objects after compaction
	parquetObjectsAfterCompaction := objStore.countParquetObjects()
	t.Logf("  Parquet objects after compaction: %d", parquetObjectsAfterCompaction)

	// Step 4: Trigger GC
	t.Log("Step 4: Trigger GC")

	// Check pending WAL GC records before running GC
	walGCWorker := gc.NewWALGCWorker(metaStore, objStore, gc.WALGCWorkerConfig{
		NumDomains: 4,
		BatchSize:  100,
	})
	pendingWALGC, err := walGCWorker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("failed to get pending WAL GC count: %v", err)
	}
	t.Logf("  Pending WAL GC records: %d", pendingWALGC)

	// Modify the delete-after time on all GC records to make them eligible now
	// This simulates the grace period having passed
	makeWALGCRecordsEligible(t, ctx, metaStore, 4)

	// Now run GC
	eligibleWALGC, err := walGCWorker.GetEligibleCount(ctx)
	if err != nil {
		t.Fatalf("failed to get eligible WAL GC count: %v", err)
	}
	t.Logf("  Eligible WAL GC records (after time adjustment): %d", eligibleWALGC)
	if eligibleWALGC == 0 {
		t.Fatalf("expected eligible WAL GC records after time adjustment, got 0")
	}

	// Run the actual GC scan
	if err := walGCWorker.ScanOnce(ctx); err != nil {
		t.Fatalf("WAL GC scan failed: %v", err)
	}
	t.Log("  WAL GC scan complete")

	// Check how many objects were deleted
	walObjectsAfterGC := objStore.countWALObjects()
	t.Logf("  WAL objects after GC: %d (deleted %d)", walObjectsAfterGC, walObjectsBefore-walObjectsAfterGC)

	// Parquet GC worker (for re-compaction scenarios)
	parquetGCWorker := gc.NewParquetGCWorker(metaStore, objStore, gc.ParquetGCWorkerConfig{
		BatchSize: 100,
	})
	pendingParquetGC, err := parquetGCWorker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("failed to get pending Parquet GC count: %v", err)
	}
	t.Logf("  Pending Parquet GC records: %d", pendingParquetGC)

	// Run Parquet GC scan
	if err := parquetGCWorker.ScanOnce(ctx); err != nil {
		t.Fatalf("Parquet GC scan failed: %v", err)
	}
	t.Log("  Parquet GC scan complete")

	// Verify Parquet objects are still present (should not be deleted since they're live)
	parquetObjectsAfterGC := objStore.countParquetObjects()
	t.Logf("  Parquet objects after GC: %d", parquetObjectsAfterGC)

	if parquetObjectsAfterGC != parquetObjectsAfterCompaction {
		t.Errorf("Parquet objects were deleted! Expected %d, got %d",
			parquetObjectsAfterCompaction, parquetObjectsAfterGC)
	}

	// Step 5: Verify fetch still works after GC
	t.Log("Step 5: Verify fetch still works after GC")
	afterGCRecords := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "gc-safety-test-topic", 0, 0)
	if len(afterGCRecords) != totalRecords {
		t.Fatalf("expected %d records after GC, got %d - GC may have deleted live data!", totalRecords, len(afterGCRecords))
	}

	// Verify records still match original
	for i := 0; i < totalRecords; i++ {
		before := beforeCompactionRecords[i]
		afterGC := afterGCRecords[i]
		if before.Offset != afterGC.Offset {
			t.Errorf("record %d: offset mismatch after GC - before=%d, after=%d", i, before.Offset, afterGC.Offset)
		}
		if !bytes.Equal(before.Key, afterGC.Key) {
			t.Errorf("record %d: key mismatch after GC", i)
		}
		if !bytes.Equal(before.Value, afterGC.Value) {
			t.Errorf("record %d: value mismatch after GC", i)
		}
	}

	t.Logf("  Fetched %d records after GC - all match original", len(afterGCRecords))
	t.Log("GC Safety Test PASSED: GC never deleted live data")
}

// TestGCSafety_WALGCOnlyDeletesUnreferencedObjects verifies that WAL GC only
// deletes WAL objects after all references (index entries) are removed.
func TestGCSafety_WALGCOnlyDeletesUnreferencedObjects(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newGCSafetyTestObjectStore()
	ctx := context.Background()

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "wal-gc-test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "wal-gc-test-topic", 0); err != nil {
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

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Produce records (creates WAL objects)
	produceReq := buildGCSafetyProduceRequest("wal-gc-test-topic", 0, 5, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed")
	}

	walObjectsBefore := objStore.countWALObjects()
	t.Logf("WAL objects after produce: %d", walObjectsBefore)

	// Verify fetch works
	records := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "wal-gc-test-topic", 0, 0)
	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}

	// Run GC WITHOUT running compaction first
	// WAL objects should NOT be deleted because they're still referenced
	walGCWorker := gc.NewWALGCWorker(metaStore, objStore, gc.WALGCWorkerConfig{
		NumDomains: 4,
		BatchSize:  100,
	})

	// Check if there are any GC records (there shouldn't be any yet)
	pendingBefore, _ := walGCWorker.GetPendingCount(ctx)
	t.Logf("Pending WAL GC records before compaction: %d", pendingBefore)

	// Run GC scan
	if err := walGCWorker.ScanOnce(ctx); err != nil {
		t.Fatalf("WAL GC scan failed: %v", err)
	}

	walObjectsAfterGC := objStore.countWALObjects()
	t.Logf("WAL objects after GC (no compaction): %d", walObjectsAfterGC)

	// WAL objects should still exist because they're referenced by index entries
	if walObjectsAfterGC != walObjectsBefore {
		t.Errorf("WAL objects were deleted before compaction! Expected %d, got %d",
			walObjectsBefore, walObjectsAfterGC)
	}

	// Fetch should still work
	recordsAfterGC := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "wal-gc-test-topic", 0, 0)
	if len(recordsAfterGC) != 5 {
		t.Fatalf("expected 5 records after GC, got %d", len(recordsAfterGC))
	}

	t.Log("PASSED: WAL GC correctly preserves referenced objects")
}

// TestGCSafety_ParquetGCOnlyDeletesUnreferencedFiles verifies that Parquet GC
// only deletes Parquet files that are no longer in the index.
func TestGCSafety_ParquetGCOnlyDeletesUnreferencedFiles(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newGCSafetyTestObjectStore()
	ctx := context.Background()

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "parquet-gc-test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "parquet-gc-test-topic", 0); err != nil {
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

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Produce records
	produceReq := buildGCSafetyProduceRequest("parquet-gc-test-topic", 0, 5, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed")
	}

	// Run compaction
	runGCSafetyCompaction(t, ctx, streamID, metaStore, objStore, 0)

	parquetObjectsBefore := objStore.countParquetObjects()
	t.Logf("Parquet objects after compaction: %d", parquetObjectsBefore)

	// Verify fetch works
	records := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "parquet-gc-test-topic", 0, 0)
	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}

	// Run Parquet GC - should not delete the active Parquet file
	parquetGCWorker := gc.NewParquetGCWorker(metaStore, objStore, gc.ParquetGCWorkerConfig{
		BatchSize: 100,
	})

	// Check if there are any pending Parquet GC records
	pendingBefore, _ := parquetGCWorker.GetPendingCount(ctx)
	t.Logf("Pending Parquet GC records: %d", pendingBefore)

	// Run GC scan
	if err := parquetGCWorker.ScanOnce(ctx); err != nil {
		t.Fatalf("Parquet GC scan failed: %v", err)
	}

	parquetObjectsAfter := objStore.countParquetObjects()
	t.Logf("Parquet objects after GC: %d", parquetObjectsAfter)

	// The active Parquet file should still exist
	if parquetObjectsAfter != parquetObjectsBefore {
		t.Errorf("Active Parquet file was deleted! Expected %d, got %d",
			parquetObjectsBefore, parquetObjectsAfter)
	}

	// Fetch should still work
	recordsAfterGC := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "parquet-gc-test-topic", 0, 0)
	if len(recordsAfterGC) != 5 {
		t.Fatalf("expected 5 records after GC, got %d", len(recordsAfterGC))
	}

	t.Log("PASSED: Parquet GC correctly preserves referenced files")
}

// TestGCSafety_MultipleCompactionCycles tests that GC works correctly
// across multiple compaction cycles.
func TestGCSafety_MultipleCompactionCycles(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newGCSafetyTestObjectStore()
	ctx := context.Background()

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "multi-compact-gc-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "multi-compact-gc-topic", 0); err != nil {
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

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	walGCWorker := gc.NewWALGCWorker(metaStore, objStore, gc.WALGCWorkerConfig{
		NumDomains: 4,
		BatchSize:  100,
	})

	totalRecords := 0

	// Cycle 1: Produce, compact, GC
	t.Log("Cycle 1: Produce, compact, GC")
	produceReq := buildGCSafetyProduceRequest("multi-compact-gc-topic", 0, 5, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed")
	}
	totalRecords += 5

	runGCSafetyCompaction(t, ctx, streamID, metaStore, objStore, 0)
	makeWALGCRecordsEligible(t, ctx, metaStore, 4)
	walGCWorker.ScanOnce(ctx)

	records := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "multi-compact-gc-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("cycle 1: expected %d records, got %d", totalRecords, len(records))
	}
	t.Logf("  After cycle 1: %d records accessible", len(records))

	// Cycle 2: More produce, compact, GC
	t.Log("Cycle 2: More produce, compact, GC")
	produceReq = buildGCSafetyProduceRequest("multi-compact-gc-topic", 0, 3, 1)
	produceResp = produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed")
	}
	totalRecords += 3

	// Run compaction on new data only (this tests partial compaction)
	// For simplicity, we compact all WAL entries
	runGCSafetyCompactionWALOnly(t, ctx, streamID, metaStore, objStore, 0)
	makeWALGCRecordsEligible(t, ctx, metaStore, 4)
	walGCWorker.ScanOnce(ctx)

	records = fetchAllGCSafetyRecords(t, fetchHandler, ctx, "multi-compact-gc-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("cycle 2: expected %d records, got %d", totalRecords, len(records))
	}
	t.Logf("  After cycle 2: %d records accessible", len(records))

	// Final verification
	t.Log("Final verification: All records still accessible after multiple cycles")
	finalRecords := fetchAllGCSafetyRecords(t, fetchHandler, ctx, "multi-compact-gc-topic", 0, 0)
	if len(finalRecords) != totalRecords {
		t.Fatalf("final check: expected %d records, got %d", totalRecords, len(finalRecords))
	}

	// Verify offset continuity
	for i, rec := range finalRecords {
		if rec.Offset != int64(i) {
			t.Errorf("offset discontinuity at index %d: expected offset %d, got %d", i, i, rec.Offset)
		}
	}

	t.Logf("PASSED: All %d records accessible after multiple compaction/GC cycles", totalRecords)
}

// makeWALGCRecordsEligible modifies WAL GC records to make them immediately eligible for deletion.
func makeWALGCRecordsEligible(t *testing.T, ctx context.Context, metaStore metadata.MetadataStore, numDomains int) {
	t.Helper()

	updated := 0
	for domain := 0; domain < numDomains; domain++ {
		prefix := keys.WALGCDomainPrefix(domain)
		kvs, err := metaStore.List(ctx, prefix, "", 0)
		if err != nil {
			t.Fatalf("failed to list WAL GC records for domain %d: %v", domain, err)
		}

		for _, kv := range kvs {
			var record gc.WALGCRecord
			if err := json.Unmarshal(kv.Value, &record); err != nil {
				t.Fatalf("failed to unmarshal WAL GC record %s: %v", kv.Key, err)
			}

			// Set deleteAfterMs to 0 (already expired)
			record.DeleteAfterMs = 0

			recordBytes, err := json.Marshal(record)
			if err != nil {
				t.Fatalf("failed to marshal WAL GC record %s: %v", kv.Key, err)
			}

			if _, err := metaStore.Put(ctx, kv.Key, recordBytes); err != nil {
				t.Fatalf("failed to update WAL GC record %s: %v", kv.Key, err)
			}
			updated++
		}
	}
	if updated == 0 {
		t.Fatalf("no WAL GC records updated; expected at least one")
	}
}

// runGCSafetyCompaction runs compaction for a stream (same as runCompaction but isolated for this test file).
func runGCSafetyCompaction(t *testing.T, ctx context.Context, streamID string, metaStore metadata.MetadataStore, objStore objectstore.Store, partition int32) {
	t.Helper()

	// Get all WAL index entries
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	if len(kvs) == 0 {
		t.Fatal("no index entries to compact")
	}

	// Parse entries and collect WAL entries
	var walEntries []*index.IndexEntry
	var walIndexKeys []string
	var minOffset, maxOffset int64 = 0, 0

	for i, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
			walIndexKeys = append(walIndexKeys, kv.Key)
			if i == 0 || entry.StartOffset < minOffset {
				minOffset = entry.StartOffset
			}
			if entry.EndOffset > maxOffset {
				maxOffset = entry.EndOffset
			}
		}
	}

	if len(walEntries) == 0 {
		t.Log("No WAL entries to compact")
		return
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
	parquetPath := worker.GenerateParquetPath("test-topic", partition, date, parquetID)
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Calculate the metaDomain based on streamID (using the same 4 domains as the test config)
	metaDomain := int(metadata.CalculateMetaDomain(streamID, 4))

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
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}
}

// runGCSafetyCompactionWALOnly compacts only WAL entries (not Parquet).
func runGCSafetyCompactionWALOnly(t *testing.T, ctx context.Context, streamID string, metaStore metadata.MetadataStore, objStore objectstore.Store, partition int32) {
	t.Helper()

	// Get only WAL index entries
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	var walIndexKeys []string
	var minOffset, maxOffset int64 = -1, -1

	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
			walIndexKeys = append(walIndexKeys, kv.Key)
			if minOffset == -1 || entry.StartOffset < minOffset {
				minOffset = entry.StartOffset
			}
			if entry.EndOffset > maxOffset {
				maxOffset = entry.EndOffset
			}
		}
	}

	if len(walEntries) == 0 {
		t.Log("No WAL entries to compact")
		return
	}

	// Convert WAL to Parquet
	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, partition)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	// Write Parquet
	date := time.Now().Format("2006-01-02")
	parquetID := worker.GenerateParquetID()
	parquetPath := worker.GenerateParquetPath("test-topic", partition, date, parquetID)
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Calculate the metaDomain based on streamID (using the same 4 domains as the test config)
	metaDomain := int(metadata.CalculateMetaDomain(streamID, 4))

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

	swapper := compaction.NewIndexSwapper(metaStore)
	_, err = swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}
}

// fetchAllGCSafetyRecords fetches all records from the given offset.
func fetchAllGCSafetyRecords(t *testing.T, handler *protocol.FetchHandler, ctx context.Context, topic string, partition int32, startOffset int64) []ExtractedRecord {
	t.Helper()

	var allRecords []ExtractedRecord
	currentOffset := startOffset

	for {
		fetchReq := buildGCSafetyFetchRequest(topic, partition, currentOffset)
		fetchResp := handler.Handle(ctx, 12, fetchReq)

		if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
			t.Fatalf("unexpected empty fetch response")
		}

		partResp := fetchResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("fetch failed with error code %d at offset %d", partResp.ErrorCode, currentOffset)
		}

		if len(partResp.RecordBatches) == 0 {
			break
		}

		records := parseGCSafetyRecordBatches(t, partResp.RecordBatches)
		if len(records) == 0 {
			break
		}

		for _, rec := range records {
			if rec.Offset >= startOffset {
				allRecords = append(allRecords, rec)
			}
		}
		currentOffset = records[len(records)-1].Offset + 1

		if currentOffset >= partResp.HighWatermark {
			break
		}
	}

	return allRecords
}

func buildGCSafetyFetchRequest(topic string, partition int32, fetchOffset int64) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(12)
	req.MaxBytes = 1024 * 1024

	topicReq := kmsg.NewFetchRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewFetchRequestTopicPartition()
	partReq.Partition = partition
	partReq.FetchOffset = fetchOffset
	partReq.PartitionMaxBytes = 1024 * 1024

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// parseGCSafetyRecordBatches parses record batches (reuse logic from compaction test).
func parseGCSafetyRecordBatches(t *testing.T, data []byte) []ExtractedRecord {
	t.Helper()
	return parseRecordBatches(t, data)
}

func buildGCSafetyProduceRequest(topic string, partition int32, recordCount int, batchID int) *kmsg.ProduceRequest {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.SetVersion(9)

	topicReq := kmsg.NewProduceRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewProduceRequestTopicPartition()
	partReq.Partition = partition
	partReq.Records = buildGCSafetyRecordBatch(recordCount, batchID)

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

func buildGCSafetyRecordBatch(recordCount int, batchID int) []byte {
	var records []byte
	ts := time.Now().UnixMilli()

	for i := 0; i < recordCount; i++ {
		record := buildGCSafetyRecord(i, batchID)
		records = append(records, record...)
	}

	batch := make([]byte, 61+len(records))

	binary.BigEndian.PutUint64(batch[0:8], 0)
	batchLength := 49 + len(records)
	binary.BigEndian.PutUint32(batch[8:12], uint32(batchLength))
	binary.BigEndian.PutUint32(batch[12:16], 0)
	batch[16] = 2
	binary.BigEndian.PutUint16(batch[21:23], 0)
	binary.BigEndian.PutUint32(batch[23:27], uint32(recordCount-1))
	binary.BigEndian.PutUint64(batch[27:35], uint64(ts))
	binary.BigEndian.PutUint64(batch[35:43], uint64(ts))
	binary.BigEndian.PutUint64(batch[43:51], 0xffffffffffffffff)
	binary.BigEndian.PutUint16(batch[51:53], 0xffff)
	binary.BigEndian.PutUint32(batch[53:57], 0xffffffff)
	binary.BigEndian.PutUint32(batch[57:61], uint32(recordCount))

	copy(batch[61:], records)

	table := crc32.MakeTable(crc32.Castagnoli)
	crcValue := crc32.Checksum(batch[21:], table)
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

func buildGCSafetyRecord(recordID int, batchID int) []byte {
	var body []byte

	body = append(body, 0)
	body = appendGCSafetyVarint(body, 0)
	body = appendGCSafetyVarint(body, int64(recordID))

	key := []byte(fmt.Sprintf("key-b%d-r%d", batchID, recordID))
	body = appendGCSafetyVarint(body, int64(len(key)))
	body = append(body, key...)

	value := []byte(fmt.Sprintf("value-batch%d-record%d-data", batchID, recordID))
	body = appendGCSafetyVarint(body, int64(len(value)))
	body = append(body, value...)

	body = appendGCSafetyVarint(body, 0)

	var result []byte
	result = appendGCSafetyVarint(result, int64(len(body)))
	result = append(result, body...)

	return result
}

func appendGCSafetyVarint(b []byte, v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		b = append(b, byte(uv)|0x80)
		uv >>= 7
	}
	b = append(b, byte(uv))
	return b
}

// gcSafetyTestObjectStore tracks object counts for testing.
type gcSafetyTestObjectStore struct {
	objects map[string][]byte
}

func newGCSafetyTestObjectStore() *gcSafetyTestObjectStore {
	return &gcSafetyTestObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *gcSafetyTestObjectStore) countWALObjects() int {
	count := 0
	for key := range m.objects {
		if len(key) > 4 && key[:4] == "wal/" {
			count++
		}
	}
	return count
}

func (m *gcSafetyTestObjectStore) countParquetObjects() int {
	count := 0
	for key := range m.objects {
		if len(key) >= 8 && key[len(key)-8:] == ".parquet" {
			count++
		}
	}
	return count
}

func (m *gcSafetyTestObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.objects[key] = data
	return nil
}

func (m *gcSafetyTestObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *gcSafetyTestObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *gcSafetyTestObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
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

func (m *gcSafetyTestObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *gcSafetyTestObjectStore) Delete(ctx context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

func (m *gcSafetyTestObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
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

func (m *gcSafetyTestObjectStore) Close() error {
	return nil
}

var _ objectstore.Store = (*gcSafetyTestObjectStore)(nil)
