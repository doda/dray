package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/gc"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
)

func TestIcebergRecompactionGCOrdering(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newRecompactionTestObjectStore()
	ctx := context.Background()

	topicName := "iceberg-recompaction-gc-ordering"

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
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

	recordCount := 6
	produceReq := buildCompactionTestProduceRequest(topicName, 0, recordCount, 0)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)
	if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("produce failed with error code %d", produceResp.Topics[0].Partitions[0].ErrorCode)
	}

	oldIndexKey, oldEntry := runInitialParquetCompactionForRecompaction(t, ctx, streamID, topicName, metaStore, objStore, 0)

	if _, err := objStore.Head(ctx, oldEntry.ParquetPath); err != nil {
		t.Fatalf("expected old Parquet file to exist: %v", err)
	}

	events := make([]string, 0, 3)

	sagaManager := compaction.NewSagaManager(metaStore, "recompactor")
	job, err := sagaManager.CreateJob(ctx, streamID,
		compaction.WithSourceRange(oldEntry.StartOffset, oldEntry.EndOffset),
		compaction.WithSourceWALCount(0),
		compaction.WithSourceSizeBytes(int64(oldEntry.ParquetSizeBytes)),
	)
	if err != nil {
		t.Fatalf("failed to create compaction job: %v", err)
	}

	oldData, err := readObject(ctx, objStore, oldEntry.ParquetPath)
	if err != nil {
		t.Fatalf("failed to read old Parquet data: %v", err)
	}

	newParquetPath := worker.GenerateParquetPath(topicName, 0, time.Now().Format("2006-01-02"), worker.GenerateParquetID())
	if err := objStore.Put(ctx, newParquetPath, bytes.NewReader(oldData), int64(len(oldData)), "application/octet-stream"); err != nil {
		t.Fatalf("failed to write new Parquet file: %v", err)
	}

	job, err = sagaManager.MarkParquetWritten(ctx, streamID, job.JobID, newParquetPath, int64(len(oldData)), int64(oldEntry.RecordCount))
	if err != nil {
		t.Fatalf("failed to mark Parquet written: %v", err)
	}
	if job.State != compaction.JobStateParquetWritten {
		t.Fatalf("expected job state PARQUET_WRITTEN, got %s", job.State)
	}

	job, err = sagaManager.MarkIcebergCommitted(ctx, streamID, job.JobID, 101)
	if err != nil {
		t.Fatalf("failed to mark Iceberg committed: %v", err)
	}
	events = append(events, "iceberg-committed")

	swapper := compaction.NewIndexSwapper(metaStore)
	swapResult, err := swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:            streamID,
		ParquetIndexKeys:    []string{oldIndexKey},
		ParquetEntry:        buildRecompactionEntry(oldEntry, newParquetPath, len(oldData)),
		MetaDomain:          0,
		GracePeriodMs:       1,
		IcebergEnabled:      true,
		IcebergRemovalConfirmed: false,
	})
	if err != nil {
		t.Fatalf("failed to swap Parquet index: %v", err)
	}
	events = append(events, "index-swapped")

	job, err = sagaManager.MarkIndexSwappedWithGC(ctx, streamID, job.JobID, swapResult.DecrementedWALObjects, swapResult.ParquetGCCandidates, swapResult.ParquetGCGracePeriodMs)
	if err != nil {
		t.Fatalf("failed to mark index swapped: %v", err)
	}

	parquetGCWorker := gc.NewParquetGCWorker(metaStore, objStore, gc.ParquetGCWorkerConfig{
		ScanIntervalMs: 1,
		GracePeriodMs:  1,
		BatchSize:      100,
	})

	pendingBeforeDone, err := parquetGCWorker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("failed to get Parquet GC count: %v", err)
	}
	if pendingBeforeDone != 0 {
		t.Fatalf("expected no Parquet GC markers before DONE, got %d", pendingBeforeDone)
	}

	job, err = sagaManager.MarkDone(ctx, streamID, job.JobID)
	if err != nil {
		t.Fatalf("failed to mark job DONE: %v", err)
	}
	if job.State != compaction.JobStateDone {
		t.Fatalf("expected job state DONE, got %s", job.State)
	}

	pendingAfterDone, err := parquetGCWorker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("failed to get Parquet GC count after DONE: %v", err)
	}
	if pendingAfterDone != 1 {
		t.Fatalf("expected 1 Parquet GC marker after DONE, got %d", pendingAfterDone)
	}

	gcRecord, gcVersion := getParquetGCRecord(t, ctx, metaStore, streamID, oldEntry.ParquetPath)
	if !gcRecord.IcebergEnabled {
		t.Fatal("expected Parquet GC record to be Iceberg-enabled")
	}
	if gcRecord.IcebergRemovalConfirmed {
		t.Fatal("expected Iceberg removal to be unconfirmed before confirmation")
	}

	gcRecord.DeleteAfterMs = time.Now().Add(-1 * time.Minute).UnixMilli()
	if err := putParquetGCRecord(ctx, metaStore, streamID, oldEntry.ParquetPath, gcRecord, gcVersion); err != nil {
		t.Fatalf("failed to update GC record: %v", err)
	}

	if err := parquetGCWorker.ScanOnce(ctx); err != nil {
		t.Fatalf("Parquet GC scan failed: %v", err)
	}
	if _, err := objStore.Head(ctx, oldEntry.ParquetPath); err != nil {
		t.Fatalf("expected old Parquet file to remain before Iceberg removal confirmation: %v", err)
	}

	gcRecord, _ = getParquetGCRecord(t, ctx, metaStore, streamID, oldEntry.ParquetPath)
	if gcRecord.IcebergRemovalConfirmed {
		t.Fatal("expected GC record to remain unconfirmed after scan")
	}

	if err := gc.ConfirmParquetIcebergRemoval(ctx, metaStore, streamID, oldEntry.ParquetPath); err != nil {
		t.Fatalf("failed to confirm Iceberg removal: %v", err)
	}
	events = append(events, "iceberg-removal-confirmed")

	if err := parquetGCWorker.ScanOnce(ctx); err != nil {
		t.Fatalf("Parquet GC scan failed after Iceberg removal confirmation: %v", err)
	}

	if _, err := objStore.Head(ctx, oldEntry.ParquetPath); !errorsIsNotFound(err) {
		t.Fatalf("expected old Parquet file to be deleted after Iceberg removal confirmation")
	}

	if _, err := objStore.Head(ctx, newParquetPath); err != nil {
		t.Fatalf("expected new Parquet file to remain after GC: %v", err)
	}

	pendingAfterGC, err := parquetGCWorker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("failed to get Parquet GC count after deletion: %v", err)
	}
	if pendingAfterGC != 0 {
		t.Fatalf("expected no Parquet GC markers after deletion, got %d", pendingAfterGC)
	}

	expectedEvents := []string{"iceberg-committed", "index-swapped", "iceberg-removal-confirmed"}
	if !reflect.DeepEqual(events, expectedEvents) {
		t.Fatalf("unexpected event ordering: got %v, want %v", events, expectedEvents)
	}
}

type recompactionTestObjectStore struct {
	*compactionTestObjectStore
	mu       sync.Mutex
	deletes  []string
}

func newRecompactionTestObjectStore() *recompactionTestObjectStore {
	return &recompactionTestObjectStore{
		compactionTestObjectStore: newCompactionTestObjectStore(),
	}
}

func (m *recompactionTestObjectStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	m.deletes = append(m.deletes, key)
	m.mu.Unlock()
	return m.compactionTestObjectStore.Delete(ctx, key)
}

var _ objectstore.Store = (*recompactionTestObjectStore)(nil)

func runInitialParquetCompactionForRecompaction(t *testing.T, ctx context.Context, streamID, topicName string, metaStore metadata.MetadataStore, objStore objectstore.Store, partition int32) (string, index.IndexEntry) {
	t.Helper()

	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	var walIndexKeys []string
	var minOffset, maxOffset int64

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
		t.Fatal("no WAL entries to compact")
	}

	converter := worker.NewConverter(objStore)
	convertResult, err := converter.Convert(ctx, walEntries, partition)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	parquetPath := worker.GenerateParquetPath(topicName, partition, time.Now().Format("2006-01-02"), worker.GenerateParquetID())
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

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
	swapResult, err := swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   0,
	})
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}

	result, err := metaStore.Get(ctx, swapResult.NewIndexKey)
	if err != nil {
		t.Fatalf("failed to read Parquet index entry: %v", err)
	}
	if !result.Exists {
		t.Fatalf("expected Parquet index entry at %s", swapResult.NewIndexKey)
	}

	var storedEntry index.IndexEntry
	if err := json.Unmarshal(result.Value, &storedEntry); err != nil {
		t.Fatalf("failed to parse Parquet index entry: %v", err)
	}

	return swapResult.NewIndexKey, storedEntry
}

func buildRecompactionEntry(oldEntry index.IndexEntry, parquetPath string, dataSize int) index.IndexEntry {
	return index.IndexEntry{
		StreamID:         oldEntry.StreamID,
		StartOffset:      oldEntry.StartOffset,
		EndOffset:        oldEntry.EndOffset,
		FileType:         index.FileTypeParquet,
		RecordCount:      oldEntry.RecordCount,
		MessageCount:     oldEntry.MessageCount,
		CreatedAtMs:      time.Now().UnixMilli(),
		MinTimestampMs:   oldEntry.MinTimestampMs,
		MaxTimestampMs:   oldEntry.MaxTimestampMs,
		ParquetPath:      parquetPath,
		ParquetSizeBytes: uint64(dataSize),
	}
}

func readObject(ctx context.Context, store objectstore.Store, key string) ([]byte, error) {
	reader, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func parquetIDFromPath(parquetPath string) string {
	name := path.Base(parquetPath)
	return strings.TrimSuffix(name, ".parquet")
}

func getParquetGCRecord(t *testing.T, ctx context.Context, metaStore metadata.MetadataStore, streamID, parquetPath string) (gc.ParquetGCRecord, metadata.Version) {
	t.Helper()

	parquetID := parquetIDFromPath(parquetPath)
	gcKey := keys.ParquetGCKeyPath(streamID, parquetID)
	result, err := metaStore.Get(ctx, gcKey)
	if err != nil {
		t.Fatalf("failed to read Parquet GC record: %v", err)
	}
	if !result.Exists {
		t.Fatalf("expected Parquet GC record at %s", gcKey)
	}

	var record gc.ParquetGCRecord
	if err := json.Unmarshal(result.Value, &record); err != nil {
		t.Fatalf("failed to parse Parquet GC record: %v", err)
	}

	return record, result.Version
}

func putParquetGCRecord(ctx context.Context, metaStore metadata.MetadataStore, streamID, parquetPath string, record gc.ParquetGCRecord, version metadata.Version) error {
	parquetID := parquetIDFromPath(parquetPath)
	gcKey := keys.ParquetGCKeyPath(streamID, parquetID)
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = metaStore.Put(ctx, gcKey, payload, metadata.WithExpectedVersion(version))
	return err
}

func errorsIsNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err == objectstore.ErrNotFound
}
