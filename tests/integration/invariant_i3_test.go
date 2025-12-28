package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
)

// TestInvariantI3_CrashAfterWALBeforeMetadata tests invariant I3:
// If a crash occurs after WAL write but before metadata commit,
// records are NOT visible via fetch after restart.
//
// This test simulates the crash scenario by:
// 1. Writing WAL object successfully to object storage
// 2. Failing the metadata commit (simulating crash before completion)
// 3. Creating a "new broker instance" (simulating restart)
// 4. Verifying records are NOT visible via fetch
func TestInvariantI3_CrashAfterWALBeforeMetadata(t *testing.T) {
	ctx := context.Background()

	// Shared object store - persists across "crash"
	objStore := newMockObjectStore()

	// Create metadata store that will fail on metadata commit
	metaStore := newCrashingMetadataStore()

	topicStore := topics.NewStore(metaStore.MockStore)
	streamManager := index.NewStreamManager(metaStore.MockStore)

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i3-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i3-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Record initial HWM
	hwmBefore, _, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get initial HWM: %v", err)
	}
	if hwmBefore != 0 {
		t.Fatalf("expected initial HWM=0, got %d", hwmBefore)
	}

	// Create committer with the crashing metadata store
	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Step 1 & 2: Produce records - WAL will write successfully but metadata commit will fail
	produceReq := buildProduceRequest("invariant-i3-topic", 0, 5)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)

	// Verify produce failed (error returned to client)
	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode == 0 {
		t.Fatalf("expected produce to fail, but got success with BaseOffset=%d", partResp.BaseOffset)
	}
	t.Logf("Produce correctly failed with error code %d (simulated crash)", partResp.ErrorCode)

	// Close buffer (simulating crash cleanup)
	buffer.Close()

	// Verify WAL object exists in object storage (was written before crash)
	walObjects, err := objStore.List(ctx, "")
	if err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}
	if len(walObjects) == 0 {
		t.Fatal("expected WAL object to exist in storage after partial commit")
	}
	t.Logf("WAL object exists in storage: %s (orphaned due to crash)", walObjects[0].Key)

	// Step 3: Simulate broker restart with fresh handlers using same stores
	// The key insight: metadata was never committed, so index entries don't exist

	// Create new handlers (simulating new broker instance after restart)
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Step 4: Verify records are NOT visible via fetch
	fetchReq := buildFetchRequest("invariant-i3-topic", 0, 0)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Logf("fetch returned error code %d (expected for no data)", fetchPartResp.ErrorCode)
	}

	// HWM should still be 0 (no metadata committed)
	if fetchPartResp.HighWatermark != 0 {
		t.Errorf("expected HWM=0 after crash, got %d", fetchPartResp.HighWatermark)
	}

	// Should have no record batches (data not visible)
	if len(fetchPartResp.RecordBatches) != 0 {
		t.Errorf("expected no record batches after crash, got %d bytes", len(fetchPartResp.RecordBatches))
	}

	// Verify HWM in metadata is still 0
	hwmAfter, _, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM after crash: %v", err)
	}
	if hwmAfter != 0 {
		t.Errorf("expected HWM=0 in metadata after crash, got %d", hwmAfter)
	}

	// Verify no index entries exist
	entries, err := streamManager.ListIndexEntries(ctx, streamID, 10)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 index entries after crash, got %d", len(entries))
	}

	t.Logf("PASS: Records NOT visible after crash (HWM=%d, entries=%d, WAL orphaned in storage)",
		fetchPartResp.HighWatermark, len(entries))
}

// TestInvariantI3_SubsequentProduceAfterCrash tests that after a crash scenario,
// subsequent successful produces still work correctly and are visible.
func TestInvariantI3_SubsequentProduceAfterCrash(t *testing.T) {
	ctx := context.Background()

	objStore := newMockObjectStore()
	metaStore := newCrashingMetadataStore()

	topicStore := topics.NewStore(metaStore.MockStore)
	streamManager := index.NewStreamManager(metaStore.MockStore)

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i3-recovery-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i3-recovery-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// First produce - fails due to crash
	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Produce that will fail
	produceReq := buildProduceRequest("invariant-i3-recovery-topic", 0, 3)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)

	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode == 0 {
		t.Fatalf("expected first produce to fail")
	}
	t.Logf("First produce failed as expected (crash simulation)")

	buffer.Close()

	// Simulate recovery - metadata store now works
	metaStore.stopCrashing()

	// Create new handlers (simulating restart)
	committer2 := produce.NewCommitter(objStore, metaStore.MockStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer2 := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer2.CreateFlushHandler(),
	})
	defer buffer2.Close()

	produceHandler2 := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer2,
	)

	// Second produce - should succeed
	produceReq2 := buildProduceRequest("invariant-i3-recovery-topic", 0, 5)
	produceResp2 := produceHandler2.Handle(ctx, 9, produceReq2)

	partResp2 := produceResp2.Topics[0].Partitions[0]
	if partResp2.ErrorCode != 0 {
		t.Fatalf("expected second produce to succeed, got error code %d", partResp2.ErrorCode)
	}

	// Offset should start from 0 (first produce was never committed)
	if partResp2.BaseOffset != 0 {
		t.Errorf("expected BaseOffset=0 after crash recovery, got %d", partResp2.BaseOffset)
	}

	// Verify fetch returns the new records
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	fetchReq := buildFetchRequest("invariant-i3-recovery-topic", 0, 0)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Errorf("fetch failed with error code %d", fetchPartResp.ErrorCode)
	}

	// HWM should be 5 (from successful second produce)
	if fetchPartResp.HighWatermark != 5 {
		t.Errorf("expected HWM=5 after recovery, got %d", fetchPartResp.HighWatermark)
	}

	// Should have record batches
	if len(fetchPartResp.RecordBatches) == 0 {
		t.Error("expected record batches after successful produce")
	}

	t.Logf("PASS: After crash recovery, subsequent produce succeeded (BaseOffset=0, HWM=%d)",
		fetchPartResp.HighWatermark)
}

// TestInvariantI3_StagingMarkerRemains tests that the staging marker remains
// after a crash, allowing orphan GC to identify and clean up the orphaned WAL.
func TestInvariantI3_StagingMarkerRemains(t *testing.T) {
	ctx := context.Background()

	objStore := newMockObjectStore()
	metaStore := newCrashingMetadataStore()

	topicStore := topics.NewStore(metaStore.MockStore)
	streamManager := index.NewStreamManager(metaStore.MockStore)

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i3-staging-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i3-staging-topic", 0); err != nil {
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

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Produce that will fail
	produceReq := buildProduceRequest("invariant-i3-staging-topic", 0, 3)
	produceHandler.Handle(ctx, 9, produceReq)

	buffer.Close()

	// Verify staging marker exists (for orphan GC)
	allKeys := metaStore.MockStore.GetAllKeys()
	foundStaging := false
	for _, key := range allKeys {
		if len(key) > 12 && key[:12] == "/wal/staging" {
			foundStaging = true
			t.Logf("Staging marker found: %s", key)
			break
		}
	}

	if !foundStaging {
		t.Error("staging marker should remain after crash (for orphan GC)")
	}

	// Verify NO WAL object record exists (metadata commit failed)
	foundWALRecord := false
	for _, key := range allKeys {
		if len(key) > 12 && key[:12] == "/wal/objects" {
			foundWALRecord = true
			break
		}
	}

	if foundWALRecord {
		t.Error("WAL object record should NOT exist after crash (metadata not committed)")
	}

	t.Logf("PASS: Staging marker remains, WAL record does not exist (orphan GC can clean up)")
}

// TestInvariantI3_MultipleStreamsPartialCrash tests that if a crash occurs
// while committing metadata for multiple streams, none are visible.
func TestInvariantI3_MultipleStreamsPartialCrash(t *testing.T) {
	ctx := context.Background()

	objStore := newMockObjectStore()
	metaStore := newCrashingMetadataStore()

	topicStore := topics.NewStore(metaStore.MockStore)
	streamManager := index.NewStreamManager(metaStore.MockStore)

	// Create topic with 2 partitions
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i3-multi-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for _, p := range result.Partitions {
		if err := streamManager.CreateStreamWithID(ctx, p.StreamID, "invariant-i3-multi-topic", p.Partition); err != nil {
			t.Fatalf("failed to create stream for partition %d: %v", p.Partition, err)
		}
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

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Produce to partition 0 (will fail)
	produceReq0 := buildProduceRequest("invariant-i3-multi-topic", 0, 3)
	produceResp0 := produceHandler.Handle(ctx, 9, produceReq0)
	if produceResp0.Topics[0].Partitions[0].ErrorCode == 0 {
		t.Fatalf("expected produce to partition 0 to fail")
	}

	// Produce to partition 1 (will also fail)
	produceReq1 := buildProduceRequest("invariant-i3-multi-topic", 1, 5)
	produceResp1 := produceHandler.Handle(ctx, 9, produceReq1)
	if produceResp1.Topics[0].Partitions[0].ErrorCode == 0 {
		t.Fatalf("expected produce to partition 1 to fail")
	}

	buffer.Close()

	// Verify neither partition has visible data
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	for partition := int32(0); partition < 2; partition++ {
		fetchReq := buildFetchRequest("invariant-i3-multi-topic", partition, 0)
		fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

		fetchPartResp := fetchResp.Topics[0].Partitions[0]
		if fetchPartResp.HighWatermark != 0 {
			t.Errorf("partition %d: expected HWM=0, got %d", partition, fetchPartResp.HighWatermark)
		}
		if len(fetchPartResp.RecordBatches) != 0 {
			t.Errorf("partition %d: expected no record batches, got %d bytes",
				partition, len(fetchPartResp.RecordBatches))
		}
	}

	t.Logf("PASS: Both partitions show no data after crash (HWM=0 for all)")
}

// TestInvariantI3_WALExistsButNotVisible verifies the core invariant:
// even though the WAL object exists in object storage, it is not visible
// through the fetch path because no index entry points to it.
func TestInvariantI3_WALExistsButNotVisible(t *testing.T) {
	ctx := context.Background()

	objStore := newMockObjectStore()
	metaStore := newCrashingMetadataStore()

	topicStore := topics.NewStore(metaStore.MockStore)
	streamManager := index.NewStreamManager(metaStore.MockStore)

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i3-orphan-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i3-orphan-topic", 0); err != nil {
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

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Produce records (will fail due to crash simulation)
	produceReq := buildProduceRequest("invariant-i3-orphan-topic", 0, 5)
	produceHandler.Handle(ctx, 9, produceReq)

	buffer.Close()

	// Count WAL objects in storage - should be at least one (orphaned)
	walObjects, err := objStore.List(ctx, "")
	if err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}

	walCount := 0
	for _, obj := range walObjects {
		if len(obj.Key) > 0 {
			walCount++
			t.Logf("WAL object in storage: %s (size=%d)", obj.Key, obj.Size)
		}
	}

	if walCount == 0 {
		t.Fatal("expected at least one WAL object in storage")
	}

	// Now create fetch handler and verify WAL content is NOT accessible
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	fetchReq := buildFetchRequest("invariant-i3-orphan-topic", 0, 0)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]

	// Key assertion: even though WAL exists, HWM is 0 and no data visible
	if fetchPartResp.HighWatermark != 0 {
		t.Errorf("expected HWM=0 (WAL not indexed), got %d", fetchPartResp.HighWatermark)
	}

	if len(fetchPartResp.RecordBatches) != 0 {
		t.Errorf("expected no record batches (WAL not indexed), got %d bytes",
			len(fetchPartResp.RecordBatches))
	}

	t.Logf("PASS: WAL exists (%d objects) but not visible (HWM=0, no batches)", walCount)
}

// crashingMetadataStore wraps MockStore and fails on Txn calls to simulate crash.
type crashingMetadataStore struct {
	*metadata.MockStore
	mu        sync.Mutex
	crashing  bool
	crashErr  error
}

func newCrashingMetadataStore() *crashingMetadataStore {
	return &crashingMetadataStore{
		MockStore: metadata.NewMockStore(),
		crashing:  true,
		crashErr:  errors.New("simulated broker crash during metadata commit"),
	}
}

func (s *crashingMetadataStore) Txn(ctx context.Context, partitionKey string, fn func(metadata.Txn) error) error {
	s.mu.Lock()
	crashing := s.crashing
	s.mu.Unlock()

	if crashing {
		return s.crashErr
	}
	return s.MockStore.Txn(ctx, partitionKey, fn)
}

func (s *crashingMetadataStore) stopCrashing() {
	s.mu.Lock()
	s.crashing = false
	s.mu.Unlock()
}

var _ metadata.MetadataStore = (*crashingMetadataStore)(nil)
