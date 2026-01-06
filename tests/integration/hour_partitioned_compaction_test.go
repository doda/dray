package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/projection"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHourPartitionedCompactionMultiFile(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := objectstore.NewMockStore()
	ctx := context.Background()

	topicName := "hour-partitioned-topic"
	partition := int32(0)

	// Configure topic
	topicResult, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := topicResult.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, topicName, partition); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Setup producer with value projection for created_at
	projections := []projection.TopicProjection{
		{
			Topic:  topicName,
			Format: "json",
			Fields: []projection.FieldSpec{
				{Name: "created_at", Type: projection.FieldTypeInt64},
			},
		},
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 1,
	})
	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     1,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Produce records in different hours
	baseTime := time.Now().Truncate(time.Hour).UnixMilli()
	hours := []int64{0, 1, 2}
	for _, h := range hours {
		createdAt := baseTime + h*3600*1000
		val, _ := json.Marshal(map[string]any{"created_at": createdAt})
		
		batch := buildSimpleRecordBatchWithTimestamp(1, createdAt, val)
		req := kmsg.NewPtrProduceRequest()
		req.Acks = -1
		req.SetVersion(9)

		topicReq := kmsg.NewProduceRequestTopic()
		topicReq.Topic = topicName

		partReq := kmsg.NewProduceRequestTopicPartition()
		partReq.Partition = partition
		partReq.Records = batch

		topicReq.Partitions = append(topicReq.Partitions, partReq)
		req.Topics = append(req.Topics, topicReq)
		
		resp := produceHandler.Handle(ctx, 9, req)
		if resp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("produce failed for hour %d: %d", h, resp.Topics[0].Partitions[0].ErrorCode)
		}
	}

	// Manual compaction execution for test control
	sagaManager := compaction.NewSagaManager(metaStore, "test-compactor")
	converter := worker.NewConverter(objStore, projections)
	indexSwapper := compaction.NewIndexSwapper(metaStore)

	entries, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	walEntries := make([]*index.IndexEntry, 0)
	for i := range entries {
		if entries[i].FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entries[i])
		}
	}

	if len(walEntries) == 0 {
		t.Fatal("no WAL entries found for compaction")
	}

	// Create job
	job, err := sagaManager.CreateJob(ctx, streamID,
		compaction.WithSourceRange(walEntries[0].StartOffset, walEntries[len(walEntries)-1].EndOffset),
		compaction.WithSourceWALCount(len(walEntries)),
	)
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	// Perform partitioned conversion
	partResult, err := converter.ConvertPartitioned(ctx, walEntries, partition, topicName, nil)
	if err != nil {
		t.Fatalf("ConvertPartitioned failed: %v", err)
	}

	if len(partResult.Results) != len(hours) {
		t.Fatalf("expected %d partitioned files, got %d", len(hours), len(partResult.Results))
	}

	var parquetPaths []string
	var totalBytes int64
	var minTS, maxTS int64
	for i, pf := range partResult.Results {
		path := fmt.Sprintf("parquet/%s/job-%d.parquet", streamID, i)
		if err := objStore.Put(ctx, path, bytes.NewReader(pf.ParquetData), int64(len(pf.ParquetData)), ""); err != nil {
			t.Fatalf("failed to write parquet: %v", err)
		}
		parquetPaths = append(parquetPaths, path)
		totalBytes += int64(len(pf.ParquetData))
		if i == 0 || pf.Stats.MinTimestamp < minTS {
			minTS = pf.Stats.MinTimestamp
		}
		if i == 0 || pf.Stats.MaxTimestamp > maxTS {
			maxTS = pf.Stats.MaxTimestamp
		}
	}

	// Mark written with multiple paths
	job, err = sagaManager.MarkParquetWritten(ctx, streamID, job.JobID, parquetPaths, totalBytes, int64(len(hours)), minTS, maxTS)
	if err != nil {
		t.Fatalf("MarkParquetWritten failed: %v", err)
	}

	// Perform index swap
	swapResult, err := indexSwapper.SwapFromJob(ctx, job, 0)
	if err != nil {
		t.Fatalf("index swap failed: %v", err)
	}

	// Verify index entry
	newEntry, _, err := streamManager.GetIndexEntry(ctx, swapResult.NewIndexKey)
	if err != nil {
		t.Fatalf("failed to get new index entry: %v", err)
	}

	if len(newEntry.ParquetPaths) != len(hours) {
		t.Errorf("expected %d parquet paths in index entry, got %d", len(hours), len(newEntry.ParquetPaths))
	}

	// Verify fetch works across all files
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchResult, err := fetcher.Fetch(ctx, &fetch.FetchRequest{
		StreamID:    streamID,
		FetchOffset: 0,
		MaxBytes:    1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	// Check that we got all records back across all batches
	totalFetched := 0
	for _, batch := range fetchResult.Batches {
		if len(batch) >= 61 {
			count := binary.BigEndian.Uint32(batch[57:61])
			totalFetched += int(count)
		}
	}
	if totalFetched != len(hours) {
		t.Errorf("expected %d records total, got %d", len(hours), totalFetched)
	}
}

// Helper to build a record batch with a specific timestamp and value
func buildSimpleRecordBatchWithTimestamp(recordCount int, ts int64, value []byte) []byte {
	var records []byte
	for i := 0; i < recordCount; i++ {
		body := []byte{0}
		body = appendVarintEncodeLocal(body, 0)
		body = appendVarintEncodeLocal(body, int64(i))
		key := []byte("key")
		body = appendVarintEncodeLocal(body, int64(len(key)))
		body = append(body, key...)
		body = appendVarintEncodeLocal(body, int64(len(value)))
		body = append(body, value...)
		body = appendVarintEncodeLocal(body, 0)
		
		var record []byte
		record = appendVarintEncodeLocal(record, int64(len(body)))
		record = append(record, body...)
		records = append(records, record...)
	}

	batch := make([]byte, 61+len(records))
	binary.BigEndian.PutUint64(batch[0:8], 0)
	binary.BigEndian.PutUint32(batch[8:12], uint32(49+len(records)))
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
	
	crcValue := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], crcValue)

	return batch
}

func appendVarintEncodeLocal(b []byte, v int64) []byte {
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		b = append(b, byte(uv)|0x80)
		uv >>= 7
	}
	b = append(b, byte(uv))
	return b
}
