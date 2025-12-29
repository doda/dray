package integration

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/gc"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
)

func TestFetchLogStartOffset_RespectsRetention(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	retentionMs := int64(60000)
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "retention-topic",
		PartitionCount: 1,
		Config:         map[string]string{topics.ConfigRetentionMs: "60000"},
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "retention-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	nowMs := time.Now().UnixMilli()
	oldTimestamp := nowMs - (2 * time.Hour).Milliseconds()

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    oldTimestamp,
		MinTimestampMs: oldTimestamp,
		MaxTimestampMs: oldTimestamp,
		WalID:          "wal-old",
		WalPath:        "wal/old",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append old index entry: %v", err)
	}

	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: 100,
		CreatedAtMs:    nowMs,
		MinTimestampMs: nowMs,
		MaxTimestampMs: nowMs,
		WalID:          "wal-new",
		WalPath:        "wal/new",
		ChunkOffset:    0,
		ChunkLength:    100,
	})
	if err != nil {
		t.Fatalf("failed to append new index entry: %v", err)
	}

	worker := gc.NewRetentionWorker(metaStore, objStore, topicStore, gc.RetentionWorkerConfig{
		ScanIntervalMs: 1000,
		NumDomains:     4,
		GracePeriodMs:  0,
	})
	if err := worker.EnforceStream(ctx, streamID, retentionMs, -1); err != nil {
		t.Fatalf("failed to enforce retention: %v", err)
	}

	earliestOffset, err := streamManager.GetEarliestOffset(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to read earliest offset: %v", err)
	}
	if earliestOffset != 5 {
		t.Fatalf("expected earliest offset 5 after retention, got %d", earliestOffset)
	}

	hwm, _, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to read HWM: %v", err)
	}

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	fetchReq := buildFetchRequest("retention-topic", 0, hwm)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	if len(fetchResp.Topics) != 1 || len(fetchResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected fetch response structure")
	}

	partResp := fetchResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Fatalf("fetch failed with error code %d", partResp.ErrorCode)
	}
	if partResp.LogStartOffset != earliestOffset {
		t.Fatalf("expected LogStartOffset %d, got %d", earliestOffset, partResp.LogStartOffset)
	}
}
