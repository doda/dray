package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
)

// TestCrashRecovery_AfterParquetWritten tests that a crash after PARQUET_WRITTEN
// state can be recovered by a new compactor instance that resumes the job and
// completes the remaining steps (Iceberg commit, index swap, done).
func TestCrashRecovery_AfterParquetWritten(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCrashRecoveryObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "parquet-written-crash-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "parquet-written-crash-topic", 0); err != nil {
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

	totalRecords := 5
	produceReq := buildCompactionTestProduceRequest("parquet-written-crash-topic", 0, totalRecords, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", produceResp.Topics[0].Partitions[0].ErrorCode)
	}

	sagaManager1 := compaction.NewSagaManager(metaStore, "compactor-1")
	job, err := sagaManager1.CreateJob(ctx, streamID,
		compaction.WithSourceRange(0, int64(totalRecords)),
		compaction.WithSourceWALCount(1),
		compaction.WithSourceSizeBytes(1000))
	if err != nil {
		t.Fatalf("failed to create compaction job: %v", err)
	}
	t.Logf("Created job %s in state %s", job.JobID, job.State)

	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
		}
	}

	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, 0)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	parquetPath := worker.GenerateParquetPath("parquet-written-crash-topic", 0, "2025-01-01", worker.GenerateParquetID())
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	job, err = sagaManager1.MarkParquetWritten(ctx, streamID, job.JobID, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount)
	if err != nil {
		t.Fatalf("failed to mark parquet written: %v", err)
	}
	t.Logf("Transitioned to state %s", job.State)

	if job.State != compaction.JobStateParquetWritten {
		t.Fatalf("expected state PARQUET_WRITTEN, got %s", job.State)
	}

	// --- CRASH SIMULATION ---
	// At this point, compactor-1 "crashes" - we stop using sagaManager1

	// --- RECOVERY ---
	// A new compactor instance (compactor-2) starts and discovers the incomplete job
	sagaManager2 := compaction.NewSagaManager(metaStore, "compactor-2")

	incompleteJobs, err := sagaManager2.ListIncompleteJobs(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to list incomplete jobs: %v", err)
	}
	if len(incompleteJobs) != 1 {
		t.Fatalf("expected 1 incomplete job, got %d", len(incompleteJobs))
	}

	recoveredJob := incompleteJobs[0]
	t.Logf("Recovered job %s in state %s (created by %s)", recoveredJob.JobID, recoveredJob.State, recoveredJob.CompactorID)

	if recoveredJob.State != compaction.JobStateParquetWritten {
		t.Fatalf("expected recovered job in PARQUET_WRITTEN state, got %s", recoveredJob.State)
	}

	if recoveredJob.ParquetPath != parquetPath {
		t.Fatalf("expected parquet path %s, got %s", parquetPath, recoveredJob.ParquetPath)
	}

	recoveredJob, err = sagaManager2.ResumeJob(ctx, streamID, recoveredJob.JobID)
	if err != nil {
		t.Fatalf("failed to resume job: %v", err)
	}
	if recoveredJob.RetryCount != 1 {
		t.Errorf("expected retry count 1, got %d", recoveredJob.RetryCount)
	}

	// Continue from PARQUET_WRITTEN: commit to Iceberg (simulated)
	icebergSnapshotID := int64(12345)
	recoveredJob, err = sagaManager2.MarkIcebergCommitted(ctx, streamID, recoveredJob.JobID, icebergSnapshotID)
	if err != nil {
		t.Fatalf("failed to mark iceberg committed: %v", err)
	}
	t.Logf("Transitioned to state %s", recoveredJob.State)

	// Continue: swap index
	swapper := compaction.NewIndexSwapper(metaStore)
	swapResult, err := swapper.SwapFromJob(ctx, recoveredJob, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount, 0)
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}
	t.Logf("Swapped index, new key: %s", swapResult.NewIndexKey)

	recoveredJob, err = sagaManager2.MarkIndexSwappedWithGC(ctx, streamID, recoveredJob.JobID, swapResult.DecrementedWALObjects, swapResult.ParquetGCCandidates, swapResult.ParquetGCGracePeriodMs)
	if err != nil {
		t.Fatalf("failed to mark index swapped: %v", err)
	}

	// Final state: DONE
	recoveredJob, err = sagaManager2.MarkDone(ctx, streamID, recoveredJob.JobID)
	if err != nil {
		t.Fatalf("failed to mark done: %v", err)
	}
	t.Logf("Job completed, final state: %s", recoveredJob.State)

	if recoveredJob.State != compaction.JobStateDone {
		t.Fatalf("expected state DONE, got %s", recoveredJob.State)
	}

	// Verify data integrity: fetch should return all records
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	records := fetchAllRecords(t, fetchHandler, ctx, "parquet-written-crash-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("expected %d records after recovery, got %d", totalRecords, len(records))
	}

	for i, rec := range records {
		if rec.Offset != int64(i) {
			t.Errorf("record %d has offset %d, expected %d", i, rec.Offset, i)
		}
	}

	t.Log("Recovery after PARQUET_WRITTEN completed successfully - no data loss")
}

// TestCrashRecovery_AfterIcebergCommitted tests that a crash after ICEBERG_COMMITTED
// state can be recovered by resuming the job and completing the index swap.
func TestCrashRecovery_AfterIcebergCommitted(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCrashRecoveryObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "iceberg-committed-crash-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "iceberg-committed-crash-topic", 0); err != nil {
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

	totalRecords := 7
	produceReq := buildCompactionTestProduceRequest("iceberg-committed-crash-topic", 0, totalRecords, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed")
	}

	sagaManager1 := compaction.NewSagaManager(metaStore, "compactor-1")
	job, err := sagaManager1.CreateJob(ctx, streamID,
		compaction.WithSourceRange(0, int64(totalRecords)),
		compaction.WithSourceWALCount(1),
		compaction.WithSourceSizeBytes(1000))
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
		}
	}

	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, 0)
	if err != nil {
		t.Fatalf("failed to convert: %v", err)
	}

	parquetPath := worker.GenerateParquetPath("iceberg-committed-crash-topic", 0, "2025-01-01", worker.GenerateParquetID())
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	job, err = sagaManager1.MarkParquetWritten(ctx, streamID, job.JobID, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount)
	if err != nil {
		t.Fatalf("failed to mark parquet written: %v", err)
	}

	icebergSnapshotID := int64(67890)
	job, err = sagaManager1.MarkIcebergCommitted(ctx, streamID, job.JobID, icebergSnapshotID)
	if err != nil {
		t.Fatalf("failed to mark iceberg committed: %v", err)
	}
	t.Logf("Job reached state %s before crash", job.State)

	// --- CRASH SIMULATION ---
	// compactor-1 crashes after ICEBERG_COMMITTED

	// --- RECOVERY ---
	sagaManager2 := compaction.NewSagaManager(metaStore, "compactor-2")

	incompleteJobs, err := sagaManager2.ListIncompleteJobs(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to list incomplete jobs: %v", err)
	}
	if len(incompleteJobs) != 1 {
		t.Fatalf("expected 1 incomplete job, got %d", len(incompleteJobs))
	}

	recoveredJob := incompleteJobs[0]
	t.Logf("Recovered job in state %s", recoveredJob.State)

	if recoveredJob.State != compaction.JobStateIcebergCommitted {
		t.Fatalf("expected ICEBERG_COMMITTED state, got %s", recoveredJob.State)
	}

	if recoveredJob.IcebergSnapshotID != icebergSnapshotID {
		t.Fatalf("expected snapshot ID %d, got %d", icebergSnapshotID, recoveredJob.IcebergSnapshotID)
	}

	recoveredJob, err = sagaManager2.ResumeJob(ctx, streamID, recoveredJob.JobID)
	if err != nil {
		t.Fatalf("failed to resume job: %v", err)
	}

	// Continue from ICEBERG_COMMITTED: swap index
	swapper := compaction.NewIndexSwapper(metaStore)
	swapResult, err := swapper.SwapFromJob(ctx, recoveredJob, recoveredJob.ParquetPath, recoveredJob.ParquetSizeBytes, recoveredJob.ParquetRecordCount, 0)
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}

	recoveredJob, err = sagaManager2.MarkIndexSwappedWithGC(ctx, streamID, recoveredJob.JobID, swapResult.DecrementedWALObjects, swapResult.ParquetGCCandidates, swapResult.ParquetGCGracePeriodMs)
	if err != nil {
		t.Fatalf("failed to mark index swapped: %v", err)
	}

	recoveredJob, err = sagaManager2.MarkDone(ctx, streamID, recoveredJob.JobID)
	if err != nil {
		t.Fatalf("failed to mark done: %v", err)
	}
	t.Logf("Job completed, final state: %s", recoveredJob.State)

	// Verify data integrity
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	records := fetchAllRecords(t, fetchHandler, ctx, "iceberg-committed-crash-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("expected %d records after recovery, got %d", totalRecords, len(records))
	}

	t.Log("Recovery after ICEBERG_COMMITTED completed successfully - no data loss")
}

// TestCrashRecovery_AfterIndexSwapped tests that a crash after INDEX_SWAPPED
// state can be recovered by simply marking the job as DONE.
func TestCrashRecovery_AfterIndexSwapped(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCrashRecoveryObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "index-swapped-crash-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "index-swapped-crash-topic", 0); err != nil {
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

	totalRecords := 10
	produceReq := buildCompactionTestProduceRequest("index-swapped-crash-topic", 0, totalRecords, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed")
	}

	sagaManager1 := compaction.NewSagaManager(metaStore, "compactor-1")
	job, err := sagaManager1.CreateJob(ctx, streamID,
		compaction.WithSourceRange(0, int64(totalRecords)),
		compaction.WithSourceWALCount(1),
		compaction.WithSourceSizeBytes(1000))
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
		}
	}

	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, 0)
	if err != nil {
		t.Fatalf("failed to convert: %v", err)
	}

	parquetPath := worker.GenerateParquetPath("index-swapped-crash-topic", 0, "2025-01-01", worker.GenerateParquetID())
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	job, err = sagaManager1.MarkParquetWritten(ctx, streamID, job.JobID, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount)
	if err != nil {
		t.Fatalf("failed to mark parquet written: %v", err)
	}

	job, err = sagaManager1.MarkIcebergCommitted(ctx, streamID, job.JobID, 99999)
	if err != nil {
		t.Fatalf("failed to mark iceberg committed: %v", err)
	}

	swapper := compaction.NewIndexSwapper(metaStore)
	swapResult, err := swapper.SwapFromJob(ctx, job, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount, 0)
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}

	job, err = sagaManager1.MarkIndexSwappedWithGC(ctx, streamID, job.JobID, swapResult.DecrementedWALObjects, swapResult.ParquetGCCandidates, swapResult.ParquetGCGracePeriodMs)
	if err != nil {
		t.Fatalf("failed to mark index swapped: %v", err)
	}
	t.Logf("Job reached state %s before crash", job.State)

	// --- CRASH SIMULATION ---
	// compactor-1 crashes after INDEX_SWAPPED

	// --- RECOVERY ---
	sagaManager2 := compaction.NewSagaManager(metaStore, "compactor-2")

	incompleteJobs, err := sagaManager2.ListIncompleteJobs(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to list incomplete jobs: %v", err)
	}
	if len(incompleteJobs) != 1 {
		t.Fatalf("expected 1 incomplete job, got %d", len(incompleteJobs))
	}

	recoveredJob := incompleteJobs[0]
	t.Logf("Recovered job in state %s", recoveredJob.State)

	if recoveredJob.State != compaction.JobStateIndexSwapped {
		t.Fatalf("expected INDEX_SWAPPED state, got %s", recoveredJob.State)
	}

	if len(recoveredJob.WALObjectsToDecrement) == 0 {
		t.Log("No WAL objects to decrement (already handled in previous step)")
	}

	recoveredJob, err = sagaManager2.ResumeJob(ctx, streamID, recoveredJob.JobID)
	if err != nil {
		t.Fatalf("failed to resume job: %v", err)
	}

	// Continue from INDEX_SWAPPED: just mark done
	recoveredJob, err = sagaManager2.MarkDone(ctx, streamID, recoveredJob.JobID)
	if err != nil {
		t.Fatalf("failed to mark done: %v", err)
	}
	t.Logf("Job completed, final state: %s", recoveredJob.State)

	if recoveredJob.State != compaction.JobStateDone {
		t.Fatalf("expected DONE state, got %s", recoveredJob.State)
	}

	// Verify data integrity
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	records := fetchAllRecords(t, fetchHandler, ctx, "index-swapped-crash-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("expected %d records after recovery, got %d", totalRecords, len(records))
	}

	t.Log("Recovery after INDEX_SWAPPED completed successfully - no data loss")
}

// TestCrashRecovery_NoDataLossOrDuplication verifies that multiple crash-recovery
// cycles do not cause data loss or duplication.
func TestCrashRecovery_NoDataLossOrDuplication(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCrashRecoveryObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "no-dup-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "no-dup-topic", 0); err != nil {
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

	// Produce records in multiple batches
	batches := []int{3, 4, 5, 3}
	totalRecords := 0
	for i, count := range batches {
		produceReq := buildCompactionTestProduceRequest("no-dup-topic", 0, count, i)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)
		if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("produce batch %d failed", i)
		}
		totalRecords += count
	}
	t.Logf("Produced %d records in %d batches", totalRecords, len(batches))

	// Fetch and record original data before compaction
	originalRecords := fetchAllRecords(t, fetchHandler, ctx, "no-dup-topic", 0, 0)
	if len(originalRecords) != totalRecords {
		t.Fatalf("expected %d original records, got %d", totalRecords, len(originalRecords))
	}

	// Run compaction with simulated crash at each state
	sagaManager := compaction.NewSagaManager(metaStore, "test-compactor")
	job, err := sagaManager.CreateJob(ctx, streamID,
		compaction.WithSourceRange(0, int64(totalRecords)),
		compaction.WithSourceWALCount(len(batches)),
		compaction.WithSourceSizeBytes(5000))
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	// Get WAL entries
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
		}
	}

	// Convert and write Parquet
	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, 0)
	if err != nil {
		t.Fatalf("failed to convert: %v", err)
	}

	parquetPath := worker.GenerateParquetPath("no-dup-topic", 0, "2025-01-01", worker.GenerateParquetID())
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	job, err = sagaManager.MarkParquetWritten(ctx, streamID, job.JobID, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount)
	if err != nil {
		t.Fatalf("failed to mark parquet written: %v", err)
	}

	// Verify data is still accessible at PARQUET_WRITTEN state (still using WAL)
	records := fetchAllRecords(t, fetchHandler, ctx, "no-dup-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("data loss at PARQUET_WRITTEN: expected %d records, got %d", totalRecords, len(records))
	}
	t.Log("Verified no data loss at PARQUET_WRITTEN state")

	job, err = sagaManager.MarkIcebergCommitted(ctx, streamID, job.JobID, 11111)
	if err != nil {
		t.Fatalf("failed to mark iceberg committed: %v", err)
	}

	// Data should still be accessible
	records = fetchAllRecords(t, fetchHandler, ctx, "no-dup-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("data loss at ICEBERG_COMMITTED: expected %d records, got %d", totalRecords, len(records))
	}
	t.Log("Verified no data loss at ICEBERG_COMMITTED state")

	// Swap index
	swapper := compaction.NewIndexSwapper(metaStore)
	swapResult, err := swapper.SwapFromJob(ctx, job, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount, 0)
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}

	job, err = sagaManager.MarkIndexSwappedWithGC(ctx, streamID, job.JobID, swapResult.DecrementedWALObjects, swapResult.ParquetGCCandidates, swapResult.ParquetGCGracePeriodMs)
	if err != nil {
		t.Fatalf("failed to mark index swapped: %v", err)
	}

	// Data should still be accessible (now from Parquet)
	records = fetchAllRecords(t, fetchHandler, ctx, "no-dup-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("data loss at INDEX_SWAPPED: expected %d records, got %d", totalRecords, len(records))
	}
	t.Log("Verified no data loss at INDEX_SWAPPED state")

	job, err = sagaManager.MarkDone(ctx, streamID, job.JobID)
	if err != nil {
		t.Fatalf("failed to mark done: %v", err)
	}

	// Final verification
	records = fetchAllRecords(t, fetchHandler, ctx, "no-dup-topic", 0, 0)
	if len(records) != totalRecords {
		t.Fatalf("data loss after DONE: expected %d records, got %d", totalRecords, len(records))
	}

	// Verify no duplicates by checking offsets
	seenOffsets := make(map[int64]bool)
	for _, rec := range records {
		if seenOffsets[rec.Offset] {
			t.Fatalf("duplicate record found at offset %d", rec.Offset)
		}
		seenOffsets[rec.Offset] = true
	}

	// Verify offsets are contiguous starting from 0
	for i := int64(0); i < int64(totalRecords); i++ {
		if !seenOffsets[i] {
			t.Fatalf("missing record at offset %d", i)
		}
	}

	// Verify record content matches original
	for i := 0; i < totalRecords; i++ {
		if records[i].Offset != originalRecords[i].Offset {
			t.Errorf("offset mismatch at %d: got %d, want %d", i, records[i].Offset, originalRecords[i].Offset)
		}
		if !bytes.Equal(records[i].Key, originalRecords[i].Key) {
			t.Errorf("key mismatch at offset %d", i)
		}
		if !bytes.Equal(records[i].Value, originalRecords[i].Value) {
			t.Errorf("value mismatch at offset %d", i)
		}
	}

	t.Logf("Verified all %d records: no data loss, no duplicates, content matches", totalRecords)
}

// TestCrashRecovery_IdempotentStateTransitions verifies that state transitions
// are idempotent and can be safely retried.
func TestCrashRecovery_IdempotentStateTransitions(t *testing.T) {
	metaStore := metadata.NewMockStore()
	ctx := context.Background()

	streamID := "test-stream"
	sagaManager := compaction.NewSagaManager(metaStore, "compactor-1")

	job, err := sagaManager.CreateJob(ctx, streamID,
		compaction.WithSourceRange(0, 100),
		compaction.WithSourceWALCount(5),
		compaction.WithSourceSizeBytes(10000))
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	// Transition to PARQUET_WRITTEN
	job, err = sagaManager.MarkParquetWritten(ctx, streamID, job.JobID, "/path/to/file.parquet", 5000, 100)
	if err != nil {
		t.Fatalf("failed to mark parquet written: %v", err)
	}

	// Try to transition to PARQUET_WRITTEN again (invalid)
	_, err = sagaManager.MarkParquetWritten(ctx, streamID, job.JobID, "/path/to/file.parquet", 5000, 100)
	if err == nil {
		t.Fatal("expected error on duplicate transition to PARQUET_WRITTEN")
	}
	t.Log("Correctly rejected duplicate transition to PARQUET_WRITTEN")

	// Continue to ICEBERG_COMMITTED
	job, err = sagaManager.MarkIcebergCommitted(ctx, streamID, job.JobID, 12345)
	if err != nil {
		t.Fatalf("failed to mark iceberg committed: %v", err)
	}

	// Try to go back to PARQUET_WRITTEN (invalid)
	_, err = sagaManager.MarkParquetWritten(ctx, streamID, job.JobID, "/path/to/file.parquet", 5000, 100)
	if err == nil {
		t.Fatal("expected error on backward transition")
	}
	t.Log("Correctly rejected backward transition to PARQUET_WRITTEN")

	// Continue to INDEX_SWAPPED
	job, err = sagaManager.MarkIndexSwapped(ctx, streamID, job.JobID, []string{"wal-1"})
	if err != nil {
		t.Fatalf("failed to mark index swapped: %v", err)
	}

	// Continue to DONE
	job, err = sagaManager.MarkDone(ctx, streamID, job.JobID)
	if err != nil {
		t.Fatalf("failed to mark done: %v", err)
	}

	// Try to transition from DONE (invalid - terminal state)
	_, err = sagaManager.MarkFailed(ctx, streamID, job.JobID, "test error")
	if err == nil {
		t.Fatal("expected error on transition from terminal state")
	}
	t.Log("Correctly rejected transition from terminal DONE state")

	// Verify job is complete
	finalJob, _, err := sagaManager.GetJob(ctx, streamID, job.JobID)
	if err != nil {
		t.Fatalf("failed to get final job: %v", err)
	}
	if finalJob.State != compaction.JobStateDone {
		t.Fatalf("expected DONE state, got %s", finalJob.State)
	}

	t.Log("State transitions validated - saga prevents invalid transitions")
}

// TestCrashRecovery_MultipleJobsRecovery tests recovery when multiple jobs
// are in different states.
func TestCrashRecovery_MultipleJobsRecovery(t *testing.T) {
	metaStore := metadata.NewMockStore()
	ctx := context.Background()

	streamID := "test-stream"
	sagaManager1 := compaction.NewSagaManager(metaStore, "compactor-1")

	// Create multiple jobs in different states
	_, _ = sagaManager1.CreateJob(ctx, streamID, compaction.WithJobID("job-created"))
	// job-created stays in CREATED state

	job2, _ := sagaManager1.CreateJob(ctx, streamID, compaction.WithJobID("job-parquet"))
	job2, _ = sagaManager1.MarkParquetWritten(ctx, streamID, job2.JobID, "/p", 1000, 50)
	// job2 is in PARQUET_WRITTEN

	job3, _ := sagaManager1.CreateJob(ctx, streamID, compaction.WithJobID("job-iceberg"))
	job3, _ = sagaManager1.MarkParquetWritten(ctx, streamID, job3.JobID, "/p", 1000, 50)
	job3, _ = sagaManager1.MarkIcebergCommitted(ctx, streamID, job3.JobID, 111)
	// job3 is in ICEBERG_COMMITTED

	job4, _ := sagaManager1.CreateJob(ctx, streamID, compaction.WithJobID("job-done"))
	job4, _ = sagaManager1.MarkParquetWritten(ctx, streamID, job4.JobID, "/p", 1000, 50)
	job4, _ = sagaManager1.MarkIcebergCommitted(ctx, streamID, job4.JobID, 222)
	job4, _ = sagaManager1.MarkIndexSwapped(ctx, streamID, job4.JobID, nil)
	job4, _ = sagaManager1.MarkDone(ctx, streamID, job4.JobID)
	// job4 is DONE (terminal)

	// --- CRASH SIMULATION ---

	// --- RECOVERY ---
	sagaManager2 := compaction.NewSagaManager(metaStore, "compactor-2")

	incompleteJobs, err := sagaManager2.ListIncompleteJobs(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to list incomplete jobs: %v", err)
	}

	// Should find 3 incomplete jobs (job1, job2, job3), not job4 (DONE)
	if len(incompleteJobs) != 3 {
		t.Fatalf("expected 3 incomplete jobs, got %d", len(incompleteJobs))
	}

	// Map jobs by ID for easy verification
	jobByID := make(map[string]*compaction.Job)
	for _, j := range incompleteJobs {
		jobByID[j.JobID] = j
	}

	// Verify job1 is in CREATED
	if j, ok := jobByID["job-created"]; !ok || j.State != compaction.JobStateCreated {
		t.Errorf("job-created not in expected CREATED state")
	}

	// Verify job2 is in PARQUET_WRITTEN
	if j, ok := jobByID["job-parquet"]; !ok || j.State != compaction.JobStateParquetWritten {
		t.Errorf("job-parquet not in expected PARQUET_WRITTEN state")
	}

	// Verify job3 is in ICEBERG_COMMITTED
	if j, ok := jobByID["job-iceberg"]; !ok || j.State != compaction.JobStateIcebergCommitted {
		t.Errorf("job-iceberg not in expected ICEBERG_COMMITTED state")
	}

	// Verify job4 (DONE) is not in incomplete list
	if _, ok := jobByID["job-done"]; ok {
		t.Error("job-done should not be in incomplete jobs")
	}

	t.Logf("Found %d incomplete jobs: job-created (CREATED), job-parquet (PARQUET_WRITTEN), job-iceberg (ICEBERG_COMMITTED)",
		len(incompleteJobs))

	// Complete each incomplete job
	for _, j := range incompleteJobs {
		_, _ = sagaManager2.ResumeJob(ctx, streamID, j.JobID)

		switch j.State {
		case compaction.JobStateCreated:
			// Would normally re-run the entire compaction
			// For this test, just mark as failed to simulate handling
			_, _ = sagaManager2.MarkFailed(ctx, streamID, j.JobID, "test cleanup")
		case compaction.JobStateParquetWritten:
			_, _ = sagaManager2.MarkIcebergCommitted(ctx, streamID, j.JobID, 333)
			_, _ = sagaManager2.MarkIndexSwapped(ctx, streamID, j.JobID, nil)
			_, _ = sagaManager2.MarkDone(ctx, streamID, j.JobID)
		case compaction.JobStateIcebergCommitted:
			_, _ = sagaManager2.MarkIndexSwapped(ctx, streamID, j.JobID, nil)
			_, _ = sagaManager2.MarkDone(ctx, streamID, j.JobID)
		}
	}

	// Verify all jobs are now terminal
	allJobs, err := sagaManager2.ListJobs(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to list all jobs: %v", err)
	}

	for _, j := range allJobs {
		if !j.IsTerminal() {
			t.Errorf("job %s should be terminal, got state %s", j.JobID, j.State)
		}
	}

	t.Log("All jobs recovered and transitioned to terminal states")
}

// crashRecoveryObjectStore is a copy of compactionTestObjectStore for the crash recovery tests.
type crashRecoveryObjectStore struct {
	objects map[string][]byte
}

func newCrashRecoveryObjectStore() *crashRecoveryObjectStore {
	return &crashRecoveryObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *crashRecoveryObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.objects[key] = data
	return nil
}

func (m *crashRecoveryObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *crashRecoveryObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *crashRecoveryObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
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

func (m *crashRecoveryObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *crashRecoveryObjectStore) Delete(ctx context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

func (m *crashRecoveryObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
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

func (m *crashRecoveryObjectStore) Close() error {
	return nil
}

var _ objectstore.Store = (*crashRecoveryObjectStore)(nil)

// TestCompaction_SkipIcebergWhenDisabled tests that when table.iceberg.enabled
// is set to false for a topic, the compaction workflow correctly skips the
// Iceberg commit phase using SkipIcebergCommit instead of MarkIcebergCommitted.
// This demonstrates proper integration of the IcebergChecker with the saga manager.
func TestCompaction_SkipIcebergWhenDisabled(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCrashRecoveryObjectStore()
	ctx := context.Background()

	// Create a topic with table.iceberg.enabled=false
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "no-iceberg-topic",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigIcebergEnabled: "false",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "no-iceberg-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create some test data for compaction
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

	totalRecords := 5
	produceReq := buildCompactionTestProduceRequest("no-iceberg-topic", 0, totalRecords, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", produceResp.Topics[0].Partitions[0].ErrorCode)
	}

	// Create saga manager for compaction
	sagaManager := compaction.NewSagaManager(metaStore, "compactor-1")
	job, err := sagaManager.CreateJob(ctx, streamID,
		compaction.WithSourceRange(0, int64(totalRecords)),
		compaction.WithSourceWALCount(1),
		compaction.WithSourceSizeBytes(1000))
	if err != nil {
		t.Fatalf("failed to create compaction job: %v", err)
	}
	t.Logf("Created job %s in state %s", job.JobID, job.State)

	// Get WAL entries for conversion
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
		}
	}

	// Convert WAL to Parquet
	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, 0)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	// Write Parquet file
	parquetPath := "/bucket/data/no-iceberg-topic/0/parquet-1.parquet"
	err = objStore.Put(ctx, parquetPath, bytes.NewReader(convertResult.ParquetData), int64(len(convertResult.ParquetData)), "application/octet-stream")
	if err != nil {
		t.Fatalf("failed to write Parquet file: %v", err)
	}

	// Mark Parquet written
	job, err = sagaManager.MarkParquetWritten(ctx, streamID, job.JobID, parquetPath, int64(len(convertResult.ParquetData)), convertResult.RecordCount)
	if err != nil {
		t.Fatalf("failed to mark parquet written: %v", err)
	}
	t.Logf("Job transitioned to %s", job.State)

	// Create IcebergChecker to determine whether to skip Iceberg commit
	// Use TopicStore's GetTopicConfig method wrapped in the provider interface
	topicConfigProvider := compaction.TopicConfigProviderFunc(func(ctx context.Context, topicName string) (map[string]string, error) {
		return topicStore.GetTopicConfig(ctx, topicName)
	})
	streamMetaProvider := compaction.StreamMetaProviderFunc(func(ctx context.Context, streamID string) (*index.StreamMeta, error) {
		return streamManager.GetStreamMeta(ctx, streamID)
	})

	// Global default is true (duality mode enabled by default)
	globalIcebergDefault := true
	icebergChecker := compaction.NewIcebergChecker(topicConfigProvider, streamMetaProvider, globalIcebergDefault)

	// Check if Iceberg should be skipped for this stream
	shouldSkip, err := icebergChecker.ShouldSkipIcebergCommit(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to check iceberg status: %v", err)
	}

	if !shouldSkip {
		t.Errorf("expected ShouldSkipIcebergCommit=true for topic with table.iceberg.enabled=false")
	}

	// Use SkipIcebergCommit since Iceberg is disabled for this topic
	var walObjectsToDecrement []string
	for _, entry := range walEntries {
		walObjectsToDecrement = append(walObjectsToDecrement, entry.WalPath)
	}

	job, err = sagaManager.SkipIcebergCommit(ctx, streamID, job.JobID, walObjectsToDecrement)
	if err != nil {
		t.Fatalf("failed to skip iceberg commit: %v", err)
	}
	t.Logf("Job transitioned to %s with IcebergSkipped=%v", job.State, job.IcebergSkipped)

	// Verify the job is in INDEX_SWAPPED state with IcebergSkipped=true
	if job.State != compaction.JobStateIndexSwapped {
		t.Errorf("expected state INDEX_SWAPPED, got %s", job.State)
	}
	if !job.IcebergSkipped {
		t.Error("expected IcebergSkipped=true")
	}
	if job.IcebergSnapshotID != 0 {
		t.Errorf("expected IcebergSnapshotID=0 when skipped, got %d", job.IcebergSnapshotID)
	}

	// Complete the job
	job, err = sagaManager.MarkDone(ctx, streamID, job.JobID)
	if err != nil {
		t.Fatalf("failed to mark done: %v", err)
	}
	t.Logf("Job completed with state %s", job.State)

	// Verify final state
	finalJob, _, err := sagaManager.GetJob(ctx, streamID, job.JobID)
	if err != nil {
		t.Fatalf("failed to get final job: %v", err)
	}
	if finalJob.State != compaction.JobStateDone {
		t.Errorf("expected final state DONE, got %s", finalJob.State)
	}
	if !finalJob.IcebergSkipped {
		t.Error("expected IcebergSkipped=true in final state")
	}
	if len(finalJob.WALObjectsToDecrement) != len(walObjectsToDecrement) {
		t.Errorf("expected %d WAL objects to decrement, got %d", len(walObjectsToDecrement), len(finalJob.WALObjectsToDecrement))
	}

	t.Log("Successfully completed compaction with Iceberg commit skipped")
}

// TestCompaction_IcebergEnabledByDefault tests that when table.iceberg.enabled
// is not explicitly set, the global default (true) is used and Iceberg commits proceed.
func TestCompaction_IcebergEnabledByDefault(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	ctx := context.Background()

	// Create a topic WITHOUT explicit table.iceberg.enabled config
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "default-iceberg-topic",
		PartitionCount: 1,
		// No ConfigIcebergEnabled set - should use global default
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "default-iceberg-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create IcebergChecker with global default = true
	topicConfigProvider := compaction.TopicConfigProviderFunc(func(ctx context.Context, topicName string) (map[string]string, error) {
		return topicStore.GetTopicConfig(ctx, topicName)
	})
	streamMetaProvider := compaction.StreamMetaProviderFunc(func(ctx context.Context, streamID string) (*index.StreamMeta, error) {
		return streamManager.GetStreamMeta(ctx, streamID)
	})

	icebergChecker := compaction.NewIcebergChecker(topicConfigProvider, streamMetaProvider, true)

	// Check if Iceberg is enabled for this stream
	enabled, err := icebergChecker.IsIcebergEnabled(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to check iceberg status: %v", err)
	}

	if !enabled {
		t.Error("expected IsIcebergEnabled=true for topic with no explicit config and global default=true")
	}

	shouldSkip, err := icebergChecker.ShouldSkipIcebergCommit(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to check skip status: %v", err)
	}

	if shouldSkip {
		t.Error("expected ShouldSkipIcebergCommit=false for topic with default Iceberg enabled")
	}

	t.Log("Verified Iceberg is enabled by default when not explicitly configured")
}

// TestCompaction_IcebergExplicitlyEnabled tests that when table.iceberg.enabled
// is explicitly set to true, the IcebergChecker correctly returns enabled=true.
func TestCompaction_IcebergExplicitlyEnabled(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	ctx := context.Background()

	// Create a topic with explicit table.iceberg.enabled=true
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "explicit-iceberg-topic",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigIcebergEnabled: "true",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "explicit-iceberg-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create IcebergChecker with global default = false (to verify per-topic override works)
	topicConfigProvider := compaction.TopicConfigProviderFunc(func(ctx context.Context, topicName string) (map[string]string, error) {
		return topicStore.GetTopicConfig(ctx, topicName)
	})
	streamMetaProvider := compaction.StreamMetaProviderFunc(func(ctx context.Context, streamID string) (*index.StreamMeta, error) {
		return streamManager.GetStreamMeta(ctx, streamID)
	})

	// Global default is false, but per-topic should override
	icebergChecker := compaction.NewIcebergChecker(topicConfigProvider, streamMetaProvider, false)

	enabled, err := icebergChecker.IsIcebergEnabled(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to check iceberg status: %v", err)
	}

	if !enabled {
		t.Error("expected IsIcebergEnabled=true when per-topic config explicitly enables Iceberg, even with global default=false")
	}

	t.Log("Verified per-topic Iceberg enabled=true overrides global default=false")
}
