package integration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestInvariantI4_CrashAfterMetadataCommit tests invariant I4:
// If a crash occurs after metadata commit but before the response is sent
// to the client, duplicates may exist but offsets remain monotonic.
//
// This invariant ensures that:
// 1. Records committed before crash are visible
// 2. Client retry produces new records (duplicates)
// 3. All offsets across both commits are strictly monotonic
// 4. No gaps exist in the offset sequence
func TestInvariantI4_CrashAfterMetadataCommit(t *testing.T) {
	ctx := context.Background()

	// Shared object store - persists across "crash"
	objStore := newMockObjectStore()
	metaStore := metadata.NewMockStore()

	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i4-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i4-topic", 0); err != nil {
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

	// Create committer and buffer for first produce
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

	// Step 1: Produce records - this will SUCCEED (metadata commit happens)
	firstRecordCount := 5
	produceReq := buildProduceRequest("invariant-i4-topic", 0, firstRecordCount)
	produceResp := produceHandler.Handle(ctx, 9, produceReq)

	// Verify produce succeeded
	partResp := produceResp.Topics[0].Partitions[0]
	if partResp.ErrorCode != 0 {
		t.Fatalf("expected first produce to succeed, got error code %d", partResp.ErrorCode)
	}

	firstBaseOffset := partResp.BaseOffset
	t.Logf("First produce succeeded: BaseOffset=%d (records 0-%d)", firstBaseOffset, firstRecordCount-1)

	// Verify HWM was updated
	hwmAfterFirst, _, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM after first produce: %v", err)
	}
	if hwmAfterFirst != int64(firstRecordCount) {
		t.Fatalf("expected HWM=%d after first produce, got %d", firstRecordCount, hwmAfterFirst)
	}
	t.Logf("HWM after first produce: %d", hwmAfterFirst)

	// Step 2: Simulate crash - close buffer and "forget" the response was sent
	buffer.Close()
	t.Log("Simulated crash after metadata commit (records are durable)")

	// Step 3: Client retries the produce (thinks it failed because no response)
	// Create new handlers (simulating broker restart)
	committer2 := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
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

	// Client retries same produce (this creates duplicates)
	secondRecordCount := 5 // Same count as "retry" of first produce
	produceReq2 := buildProduceRequest("invariant-i4-topic", 0, secondRecordCount)
	produceResp2 := produceHandler2.Handle(ctx, 9, produceReq2)

	partResp2 := produceResp2.Topics[0].Partitions[0]
	if partResp2.ErrorCode != 0 {
		t.Fatalf("expected retry produce to succeed, got error code %d", partResp2.ErrorCode)
	}

	secondBaseOffset := partResp2.BaseOffset
	t.Logf("Retry produce succeeded: BaseOffset=%d (records %d-%d)",
		secondBaseOffset, secondBaseOffset, secondBaseOffset+int64(secondRecordCount)-1)

	// Step 4: Verify invariants

	// 4a: Second produce starts where first ended (offsets are monotonic, no gaps)
	expectedSecondBase := firstBaseOffset + int64(firstRecordCount)
	if secondBaseOffset != expectedSecondBase {
		t.Errorf("expected retry BaseOffset=%d (first=%d + count=%d), got %d",
			expectedSecondBase, firstBaseOffset, firstRecordCount, secondBaseOffset)
	}

	// 4b: HWM reflects all records (both produces)
	hwmAfterSecond, _, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM after retry: %v", err)
	}
	expectedHWM := int64(firstRecordCount + secondRecordCount)
	if hwmAfterSecond != expectedHWM {
		t.Errorf("expected HWM=%d after retry, got %d", expectedHWM, hwmAfterSecond)
	}
	t.Logf("HWM after retry: %d", hwmAfterSecond)

	// 4c: Fetch all records and verify they're visible
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	fetchReq := buildFetchRequest("invariant-i4-topic", 0, 0)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.ErrorCode != 0 {
		t.Errorf("fetch failed with error code %d", fetchPartResp.ErrorCode)
	}

	if fetchPartResp.HighWatermark != expectedHWM {
		t.Errorf("expected fetch HWM=%d, got %d", expectedHWM, fetchPartResp.HighWatermark)
	}

	if len(fetchPartResp.RecordBatches) == 0 {
		t.Error("expected record batches in fetch response")
	}

	// 4d: Verify index entries are contiguous with no gaps
	entries, err := streamManager.ListIndexEntries(ctx, streamID, 100)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	if len(entries) < 2 {
		t.Logf("Have %d index entries (both produces may share WAL)", len(entries))
	}

	// Verify entries cover the full offset range
	var totalRecords int64
	for _, entry := range entries {
		totalRecords += entry.EndOffset - entry.StartOffset
	}
	if totalRecords != expectedHWM {
		t.Errorf("expected %d total records in index, got %d", expectedHWM, totalRecords)
	}

	t.Logf("PASS: Duplicates exist but offsets are monotonic (first=%d-%d, retry=%d-%d, HWM=%d)",
		firstBaseOffset, firstBaseOffset+int64(firstRecordCount)-1,
		secondBaseOffset, secondBaseOffset+int64(secondRecordCount)-1,
		fetchPartResp.HighWatermark)
}

// TestInvariantI4_MultipleRetries tests that multiple retries still maintain
// monotonic offset ordering.
func TestInvariantI4_MultipleRetries(t *testing.T) {
	ctx := context.Background()

	objStore := newMockObjectStore()
	metaStore := metadata.NewMockStore()

	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i4-retries-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i4-retries-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	var allBaseOffsets []int64
	expectedNextOffset := int64(0)
	numCycles := 5
	recordsPerCycle := 3

	for cycle := 0; cycle < numCycles; cycle++ {
		// Create new handlers each cycle (simulating crash/restart)
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

		produceReq := buildProduceRequest("invariant-i4-retries-topic", 0, recordsPerCycle)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("cycle %d: produce failed with error code %d", cycle, partResp.ErrorCode)
		}

		// Verify base offset matches expected
		if partResp.BaseOffset != expectedNextOffset {
			t.Errorf("cycle %d: expected BaseOffset=%d, got %d",
				cycle, expectedNextOffset, partResp.BaseOffset)
		}

		allBaseOffsets = append(allBaseOffsets, partResp.BaseOffset)
		expectedNextOffset += int64(recordsPerCycle)

		t.Logf("Cycle %d: produced at offset %d", cycle, partResp.BaseOffset)

		buffer.Close()
	}

	// Verify all base offsets are strictly increasing
	for i := 1; i < len(allBaseOffsets); i++ {
		if allBaseOffsets[i] <= allBaseOffsets[i-1] {
			t.Errorf("base offsets not strictly increasing: offset[%d]=%d <= offset[%d]=%d",
				i, allBaseOffsets[i], i-1, allBaseOffsets[i-1])
		}
	}

	// Verify final HWM
	hwm, _, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get final HWM: %v", err)
	}

	expectedHWM := int64(numCycles * recordsPerCycle)
	if hwm != expectedHWM {
		t.Errorf("expected final HWM=%d, got %d", expectedHWM, hwm)
	}

	// Verify fetch returns all records
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	fetchReq := buildFetchRequest("invariant-i4-retries-topic", 0, 0)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]
	if fetchPartResp.HighWatermark != expectedHWM {
		t.Errorf("expected fetch HWM=%d, got %d", expectedHWM, fetchPartResp.HighWatermark)
	}

	t.Logf("PASS: %d cycles of crash/retry maintained monotonic offsets (HWM=%d)", numCycles, hwm)
}

// TestInvariantI4_WithKafkaClient tests I4 using a real Kafka client to simulate
// the client-side retry behavior after a "crash" (simulated by restarting broker).
func TestInvariantI4_WithKafkaClient(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "invariant-i4-client-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// First produce batch
	client1, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create first client: %v", err)
	}

	firstBatchSize := 5
	var firstOffsets []int64
	for i := 0; i < firstBatchSize; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-original-%d", i)),
			Value: []byte(fmt.Sprintf("value-original-%d", i)),
		}
		result := client1.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("first batch produce failed: %v", result.FirstErr())
		}
		for _, r := range result {
			firstOffsets = append(firstOffsets, r.Record.Offset)
		}
	}
	client1.Close()
	t.Logf("First batch: offsets %v", firstOffsets)

	// Simulate the scenario: metadata was committed, but client thinks request failed.
	// Client creates new connection and retries (producing duplicates).

	client2, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create retry client: %v", err)
	}

	// "Retry" the same content (in real scenario, client would resend same data)
	retryBatchSize := 5
	var retryOffsets []int64
	for i := 0; i < retryBatchSize; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-retry-%d", i)),
			Value: []byte(fmt.Sprintf("value-retry-%d", i)),
		}
		result := client2.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("retry batch produce failed: %v", result.FirstErr())
		}
		for _, r := range result {
			retryOffsets = append(retryOffsets, r.Record.Offset)
		}
	}
	client2.Close()
	t.Logf("Retry batch: offsets %v", retryOffsets)

	// Verify retry offsets start after first batch
	if len(retryOffsets) > 0 && len(firstOffsets) > 0 {
		if retryOffsets[0] <= firstOffsets[len(firstOffsets)-1] {
			t.Errorf("retry offsets should start after first batch: retry[0]=%d <= first[-1]=%d",
				retryOffsets[0], firstOffsets[len(firstOffsets)-1])
		}
	}

	// Verify all offsets combined are monotonic
	allOffsets := append(firstOffsets, retryOffsets...)
	for i := 1; i < len(allOffsets); i++ {
		if allOffsets[i] <= allOffsets[i-1] {
			t.Errorf("combined offsets not monotonic: offset[%d]=%d <= offset[%d]=%d",
				i, allOffsets[i], i-1, allOffsets[i-1])
		}
	}

	// Fetch all records and verify
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var fetchedOffsets []int64
	totalExpected := firstBatchSize + retryBatchSize

	for len(fetchedOffsets) < totalExpected {
		fetches := consumerClient.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedOffsets = append(fetchedOffsets, r.Offset)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	// Verify fetched offsets are monotonic
	for i := 1; i < len(fetchedOffsets); i++ {
		if fetchedOffsets[i] <= fetchedOffsets[i-1] {
			t.Errorf("fetched offsets not monotonic: offset[%d]=%d <= offset[%d]=%d",
				i, fetchedOffsets[i], i-1, fetchedOffsets[i-1])
		}
	}

	if len(fetchedOffsets) != totalExpected {
		t.Errorf("expected %d records, got %d", totalExpected, len(fetchedOffsets))
	}

	t.Logf("PASS: Duplicates exist with monotonic offsets (fetched %d records)", len(fetchedOffsets))
}

// TestInvariantI4_ConcurrentProducersAfterCrash tests that concurrent producers
// after a crash scenario still maintain monotonic offsets.
func TestInvariantI4_ConcurrentProducersAfterCrash(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "invariant-i4-concurrent-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// First: single producer commits successfully
	client1, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create initial client: %v", err)
	}

	initialRecords := 10
	for i := 0; i < initialRecords; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("initial-key-%d", i)),
			Value: []byte(fmt.Sprintf("initial-value-%d", i)),
		}
		result := client1.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("initial produce failed: %v", result.FirstErr())
		}
	}
	client1.Close()
	t.Logf("Initial producer: committed %d records", initialRecords)

	// Now simulate crash recovery with concurrent "retry" producers
	numProducers := 3
	recordsPerProducer := 10

	var mu sync.Mutex
	allOffsets := make(map[int64]int) // offset -> producer ID

	var wg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		producerID := p
		wg.Add(1)

		go func() {
			defer wg.Done()

			client, err := kgo.NewClient(
				kgo.SeedBrokers(broker.addr),
				kgo.DefaultProduceTopic(topicName),
				kgo.RequiredAcks(kgo.AllISRAcks()),
				kgo.DisableIdempotentWrite(),
			)
			if err != nil {
				t.Errorf("producer %d: failed to create client: %v", producerID, err)
				return
			}
			defer client.Close()

			for i := 0; i < recordsPerProducer; i++ {
				record := &kgo.Record{
					Topic: topicName,
					Key:   []byte(fmt.Sprintf("retry-p%d-key-%d", producerID, i)),
					Value: []byte(fmt.Sprintf("retry-p%d-value-%d", producerID, i)),
				}
				result := client.ProduceSync(ctx, record)
				if result.FirstErr() != nil {
					t.Errorf("producer %d: produce failed: %v", producerID, result.FirstErr())
					continue
				}

				for _, r := range result {
					mu.Lock()
					if existing, exists := allOffsets[r.Record.Offset]; exists {
						t.Errorf("duplicate offset %d from producer %d (already from %d)",
							r.Record.Offset, producerID, existing)
					}
					allOffsets[r.Record.Offset] = producerID
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// Collect and verify offsets
	var sortedOffsets []int64
	for offset := range allOffsets {
		sortedOffsets = append(sortedOffsets, offset)
	}
	sort.Slice(sortedOffsets, func(i, j int) bool { return sortedOffsets[i] < sortedOffsets[j] })

	// Verify no gaps from initial records onward
	if len(sortedOffsets) > 0 {
		// First concurrent offset should be at initialRecords
		if sortedOffsets[0] != int64(initialRecords) {
			t.Errorf("expected first concurrent offset=%d, got %d",
				initialRecords, sortedOffsets[0])
		}

		// All offsets should be contiguous
		for i := 1; i < len(sortedOffsets); i++ {
			if sortedOffsets[i]-sortedOffsets[i-1] != 1 {
				t.Errorf("gap in offsets: offset[%d]=%d, offset[%d]=%d",
					i-1, sortedOffsets[i-1], i, sortedOffsets[i])
			}
		}
	}

	totalExpected := initialRecords + numProducers*recordsPerProducer

	// Verify via fetch
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create consumer client: %v", err)
	}
	defer consumerClient.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var fetchedOffsets []int64
	for len(fetchedOffsets) < totalExpected {
		fetches := consumerClient.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedOffsets = append(fetchedOffsets, r.Offset)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	// Verify fetched offsets are strictly increasing
	for i := 1; i < len(fetchedOffsets); i++ {
		if fetchedOffsets[i] <= fetchedOffsets[i-1] {
			t.Errorf("fetched offsets not monotonic: offset[%d]=%d <= offset[%d]=%d",
				i, fetchedOffsets[i], i-1, fetchedOffsets[i-1])
		}
	}

	if len(fetchedOffsets) != totalExpected {
		t.Errorf("expected %d records, got %d", totalExpected, len(fetchedOffsets))
	}

	t.Logf("PASS: Concurrent retries after crash maintain monotonic offsets (%d initial + %d concurrent = %d total)",
		initialRecords, numProducers*recordsPerProducer, len(fetchedOffsets))
}

// TestInvariantI4_DuplicatesAreVisible tests that duplicate records from retries
// are actually visible (not silently dropped) and have unique offsets.
func TestInvariantI4_DuplicatesAreVisible(t *testing.T) {
	ctx := context.Background()

	objStore := newMockObjectStore()
	metaStore := metadata.NewMockStore()

	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i4-visible-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i4-visible-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// First produce
	committer1 := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})
	buffer1 := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer1.CreateFlushHandler(),
	})
	produceHandler1 := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer1,
	)

	produceReq1 := buildProduceRequest("invariant-i4-visible-topic", 0, 3)
	produceResp1 := produceHandler1.Handle(ctx, 9, produceReq1)

	partResp1 := produceResp1.Topics[0].Partitions[0]
	if partResp1.ErrorCode != 0 {
		t.Fatalf("first produce failed: %d", partResp1.ErrorCode)
	}
	firstBase := partResp1.BaseOffset
	t.Logf("First produce: offsets %d-%d", firstBase, firstBase+2)

	buffer1.Close()

	// "Retry" produce (same logical data but will get new offsets)
	committer2 := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
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

	produceReq2 := buildProduceRequest("invariant-i4-visible-topic", 0, 3)
	produceResp2 := produceHandler2.Handle(ctx, 9, produceReq2)

	partResp2 := produceResp2.Topics[0].Partitions[0]
	if partResp2.ErrorCode != 0 {
		t.Fatalf("retry produce failed: %d", partResp2.ErrorCode)
	}
	secondBase := partResp2.BaseOffset
	t.Logf("Retry produce: offsets %d-%d", secondBase, secondBase+2)

	// Both sets of records should be visible
	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Fetch all
	fetchReq := buildFetchRequest("invariant-i4-visible-topic", 0, 0)
	fetchResp := fetchHandler.Handle(ctx, 12, fetchReq)

	fetchPartResp := fetchResp.Topics[0].Partitions[0]

	// Should have 6 records total (3 + 3)
	expectedHWM := int64(6)
	if fetchPartResp.HighWatermark != expectedHWM {
		t.Errorf("expected HWM=%d (both produces visible), got %d",
			expectedHWM, fetchPartResp.HighWatermark)
	}

	if len(fetchPartResp.RecordBatches) == 0 {
		t.Error("expected record batches (duplicates should be visible)")
	}

	// Verify index entries cover full range
	entries, err := streamManager.ListIndexEntries(ctx, streamID, 100)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var totalRecordsInIndex int64
	for _, entry := range entries {
		totalRecordsInIndex += entry.EndOffset - entry.StartOffset
	}
	if totalRecordsInIndex != expectedHWM {
		t.Errorf("expected %d records in index, got %d", expectedHWM, totalRecordsInIndex)
	}

	t.Logf("PASS: Both original (%d-%d) and duplicate (%d-%d) records visible (HWM=%d)",
		firstBase, firstBase+2, secondBase, secondBase+2, fetchPartResp.HighWatermark)
}
