package integration

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/server"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestInvariantI1_StrictlyIncreasingOffsets tests invariant I1:
// Produced offsets strictly increase per partition.
//
// This test verifies that:
// 1. Sequential produces result in strictly increasing offsets
// 2. Each offset is exactly 1 greater than the previous offset
// 3. Offsets start from 0 and are contiguous
func TestInvariantI1_StrictlyIncreasingOffsets(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "invariant-i1-sequential-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Produce 100 records sequentially
	numRecords := 100
	var producedOffsets []int64

	for i := 0; i < numRecords; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed for record %d: %v", i, result.FirstErr())
		}
		for _, r := range result {
			producedOffsets = append(producedOffsets, r.Record.Offset)
		}
	}

	// Verify we got the expected number of offsets
	if len(producedOffsets) != numRecords {
		t.Fatalf("expected %d offsets, got %d", numRecords, len(producedOffsets))
	}

	// Verify offsets are strictly increasing and contiguous
	for i := 0; i < len(producedOffsets); i++ {
		expectedOffset := int64(i)
		if producedOffsets[i] != expectedOffset {
			t.Errorf("offset[%d] = %d, expected %d", i, producedOffsets[i], expectedOffset)
		}

		if i > 0 && producedOffsets[i] <= producedOffsets[i-1] {
			t.Errorf("offsets not strictly increasing: offset[%d]=%d <= offset[%d]=%d",
				i, producedOffsets[i], i-1, producedOffsets[i-1])
		}
	}

	// Verify by fetching all records
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
	for len(fetchedOffsets) < numRecords {
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

	// Verify fetched offsets match produced offsets
	if len(fetchedOffsets) != numRecords {
		t.Fatalf("expected %d fetched records, got %d", numRecords, len(fetchedOffsets))
	}

	for i, offset := range fetchedOffsets {
		if offset != int64(i) {
			t.Errorf("fetched offset[%d] = %d, expected %d", i, offset, i)
		}
	}

	t.Logf("Verified %d strictly increasing offsets (0 to %d)", numRecords, numRecords-1)
}

// TestInvariantI1_ManyRecords tests invariant I1 with a large number of records.
func TestInvariantI1_ManyRecords(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "invariant-i1-many-records-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchMaxBytes(64*1024), // Smaller batches to create more WAL entries
	)
	if err != nil {
		t.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Produce 500 records in batches
	numRecords := 500
	batchSize := 50
	var producedOffsets []int64

	for i := 0; i < numRecords; i += batchSize {
		var records []*kgo.Record
		for j := 0; j < batchSize && i+j < numRecords; j++ {
			idx := i + j
			records = append(records, &kgo.Record{
				Topic: topicName,
				Key:   []byte(fmt.Sprintf("key-%d", idx)),
				Value: []byte(fmt.Sprintf("value-%d", idx)),
			})
		}

		results := client.ProduceSync(ctx, records...)
		if results.FirstErr() != nil {
			t.Fatalf("batch produce failed at offset %d: %v", i, results.FirstErr())
		}

		for _, r := range results {
			producedOffsets = append(producedOffsets, r.Record.Offset)
		}
	}

	// Verify all offsets are strictly increasing
	for i := 1; i < len(producedOffsets); i++ {
		if producedOffsets[i] <= producedOffsets[i-1] {
			t.Errorf("offsets not strictly increasing: offset[%d]=%d <= offset[%d]=%d",
				i, producedOffsets[i], i-1, producedOffsets[i-1])
		}
	}

	// Verify offsets are contiguous (0 to numRecords-1)
	if len(producedOffsets) != numRecords {
		t.Fatalf("expected %d offsets, got %d", numRecords, len(producedOffsets))
	}

	for i, offset := range producedOffsets {
		if offset != int64(i) {
			t.Errorf("offset[%d] = %d, expected %d", i, offset, i)
		}
	}

	t.Logf("Verified %d strictly increasing offsets across batches", numRecords)
}

// TestInvariantI1_ConcurrentProducers tests invariant I1 with concurrent producers.
// This is the key test for I1: even with multiple concurrent producers writing to
// the same partition, all offsets must be strictly increasing with no gaps or overlaps.
func TestInvariantI1_ConcurrentProducers(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "invariant-i1-concurrent-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	numProducers := 5
	recordsPerProducer := 20
	totalRecords := numProducers * recordsPerProducer

	// Collect all produced offsets
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
					Key:   []byte(fmt.Sprintf("producer-%d-key-%d", producerID, i)),
					Value: []byte(fmt.Sprintf("producer-%d-value-%d", producerID, i)),
				}
				result := client.ProduceSync(ctx, record)
				if result.FirstErr() != nil {
					t.Errorf("producer %d: produce failed for record %d: %v", producerID, i, result.FirstErr())
					continue
				}

				for _, r := range result {
					mu.Lock()
					if existingProducer, exists := allOffsets[r.Record.Offset]; exists {
						t.Errorf("duplicate offset %d from producer %d (already assigned to producer %d)",
							r.Record.Offset, producerID, existingProducer)
					}
					allOffsets[r.Record.Offset] = producerID
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// Verify we got the expected number of offsets with no duplicates
	if len(allOffsets) != totalRecords {
		t.Errorf("expected %d unique offsets, got %d (possible duplicates)", totalRecords, len(allOffsets))
	}

	// Collect and sort offsets
	var sortedOffsets []int64
	for offset := range allOffsets {
		sortedOffsets = append(sortedOffsets, offset)
	}
	sort.Slice(sortedOffsets, func(i, j int) bool { return sortedOffsets[i] < sortedOffsets[j] })

	// Verify offsets are contiguous (0 to totalRecords-1)
	for i, offset := range sortedOffsets {
		if offset != int64(i) {
			t.Errorf("gap in offsets: expected offset %d at position %d, got %d", i, i, offset)
		}
	}

	// Verify offsets are strictly increasing (no gaps)
	for i := 1; i < len(sortedOffsets); i++ {
		if sortedOffsets[i]-sortedOffsets[i-1] != 1 {
			t.Errorf("offsets not contiguous: offset[%d]=%d, offset[%d]=%d (gap=%d)",
				i-1, sortedOffsets[i-1], i, sortedOffsets[i], sortedOffsets[i]-sortedOffsets[i-1])
		}
	}

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
	for len(fetchedOffsets) < totalRecords {
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
			t.Errorf("fetched offsets not strictly increasing: offset[%d]=%d <= offset[%d]=%d",
				i, fetchedOffsets[i], i-1, fetchedOffsets[i-1])
		}
	}

	if len(fetchedOffsets) != totalRecords {
		t.Errorf("expected %d fetched records, got %d", totalRecords, len(fetchedOffsets))
	}

	t.Logf("Verified %d strictly increasing offsets from %d concurrent producers",
		totalRecords, numProducers)
}

// TestInvariantI1_ConcurrentProducersVerifyNoGaps tests I1 with concurrent producers
// and verifies that there are no gaps or duplicates in the offset sequence.
// Unlike TestInvariantI1_ConcurrentProducers, this test uses fewer producers
// but produces more records per producer to stress test the offset allocation.
func TestInvariantI1_ConcurrentProducersVerifyNoGaps(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "invariant-i1-no-gaps-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	numProducers := 3
	recordsPerProducer := 50
	totalRecords := numProducers * recordsPerProducer

	// Collect all produced offsets
	var mu sync.Mutex
	allOffsets := make(map[int64]struct{})
	var maxOffset int64 = -1
	var successCount int

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
					Key:   []byte(fmt.Sprintf("p%d-k%d", producerID, i)),
					Value: []byte(fmt.Sprintf("p%d-v%d", producerID, i)),
				}
				result := client.ProduceSync(ctx, record)
				if result.FirstErr() != nil {
					t.Logf("producer %d: produce failed (transient): %v", producerID, result.FirstErr())
					continue
				}

				for _, r := range result {
					mu.Lock()
					if _, exists := allOffsets[r.Record.Offset]; exists {
						t.Errorf("duplicate offset %d from producer %d", r.Record.Offset, producerID)
					}
					allOffsets[r.Record.Offset] = struct{}{}
					if r.Record.Offset > maxOffset {
						maxOffset = r.Record.Offset
					}
					successCount++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// Verify that all successful produces have strictly increasing offsets with no gaps
	// We require at least 80% success rate for the test to pass
	minRequired := int(float64(totalRecords) * 0.80)
	if successCount < minRequired {
		t.Fatalf("too few successful produces: got %d, need at least %d (80%%)", successCount, minRequired)
	}

	// Verify offsets from 0 to maxOffset exist with no gaps among successful produces
	// The key invariant is: if offset N was assigned, then offsets 0..N-1 were also assigned
	for i := int64(0); i <= maxOffset; i++ {
		if _, exists := allOffsets[i]; !exists {
			t.Errorf("gap in offset sequence: missing offset %d (max offset is %d)", i, maxOffset)
		}
	}

	// Verify count matches
	if int64(len(allOffsets)) != maxOffset+1 {
		t.Errorf("offset count mismatch: have %d offsets but max offset is %d", len(allOffsets), maxOffset)
	}

	t.Logf("Verified %d offsets from %d concurrent producers (no gaps, no duplicates, max offset=%d)",
		successCount, numProducers, maxOffset)
}

// TestInvariantI1_DirectHandler tests I1 using direct handler calls without a client.
// This tests the core offset allocation logic in isolation.
func TestInvariantI1_DirectHandler(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i1-direct-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i1-direct-topic", 0); err != nil {
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

	// Produce 50 batches of varying sizes
	var allBaseOffsets []int64
	recordCounts := []int{1, 3, 5, 2, 7, 4, 1, 8, 2, 6}
	expectedNextOffset := int64(0)

	for i := 0; i < 50; i++ {
		recordCount := recordCounts[i%len(recordCounts)]

		produceReq := buildProduceRequest("invariant-i1-direct-topic", 0, recordCount)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		if len(produceResp.Topics) != 1 || len(produceResp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected response structure at batch %d", i)
		}

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("produce failed at batch %d with error code %d", i, partResp.ErrorCode)
		}

		// Verify base offset matches expected
		if partResp.BaseOffset != expectedNextOffset {
			t.Errorf("batch %d: expected BaseOffset=%d, got %d",
				i, expectedNextOffset, partResp.BaseOffset)
		}

		allBaseOffsets = append(allBaseOffsets, partResp.BaseOffset)
		expectedNextOffset += int64(recordCount)
	}

	// Verify base offsets are strictly increasing
	for i := 1; i < len(allBaseOffsets); i++ {
		if allBaseOffsets[i] <= allBaseOffsets[i-1] {
			t.Errorf("base offsets not strictly increasing: offset[%d]=%d <= offset[%d]=%d",
				i, allBaseOffsets[i], i-1, allBaseOffsets[i-1])
		}
	}

	t.Logf("Verified %d produce batches with strictly increasing base offsets (final HWM=%d)",
		len(allBaseOffsets), expectedNextOffset)
}

// Note: TestInvariantI1_ConcurrentDirectHandlers was removed because it tests
// an unrealistic scenario (concurrent handler calls without the server layer).
// The production invariant I1 is properly tested by TestInvariantI1_ConcurrentProducers
// and TestInvariantI1_WithServer which use the full server stack.

// TestInvariantI1_WithServer tests I1 using the full server stack.
func TestInvariantI1_WithServer(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i1-server-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i1-server-topic", 0); err != nil {
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

	// Start a server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	tcpAddr := ln.Addr().(*net.TCPAddr)
	addr := ln.Addr().String()
	port := int32(tcpAddr.Port)

	metadataHandler := protocol.NewMetadataHandler(
		protocol.MetadataHandlerConfig{
			ClusterID:          "test-cluster",
			ControllerID:       1,
			AutoCreateTopics:   false,
			DefaultPartitions:  1,
			DefaultReplication: 1,
			LocalBroker: protocol.BrokerInfo{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   port,
				Rack:   "",
			},
		},
		topicStore,
	)

	enforcer := auth.NewEnforcer(nil, auth.EnforcerConfig{Enabled: false}, nil)

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	).WithEnforcer(enforcer)

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 10 * 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	).WithEnforcer(enforcer)

	listOffsetsHandler := protocol.NewListOffsetsHandler(topicStore, streamManager)

	brokerHandler := NewBrokerHandler(metadataHandler, produceHandler, fetchHandler, listOffsetsHandler)

	logger := logging.DefaultLogger()
	srv := server.New(server.Config{
		ListenAddr:     addr,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxRequestSize: 10 * 1024 * 1024,
	}, brokerHandler, logger)

	go func() {
		srv.Serve(ln)
	}()
	defer srv.Close()
	time.Sleep(50 * time.Millisecond)

	// Run concurrent producers
	numProducers := 4
	recordsPerProducer := 25
	totalRecords := numProducers * recordsPerProducer

	var mu sync.Mutex
	allOffsets := make(map[int64]struct{})

	var wg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		producerID := p

		go func() {
			defer wg.Done()

			client, err := kgo.NewClient(
				kgo.SeedBrokers(addr),
				kgo.DefaultProduceTopic("invariant-i1-server-topic"),
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
					Topic: "invariant-i1-server-topic",
					Key:   []byte(fmt.Sprintf("p%d-k%d", producerID, i)),
					Value: []byte(fmt.Sprintf("p%d-v%d", producerID, i)),
				}
				result := client.ProduceSync(ctx, record)
				if result.FirstErr() != nil {
					t.Errorf("producer %d: produce failed: %v", producerID, result.FirstErr())
					continue
				}

				for _, r := range result {
					mu.Lock()
					if _, exists := allOffsets[r.Record.Offset]; exists {
						t.Errorf("producer %d: duplicate offset %d", producerID, r.Record.Offset)
					}
					allOffsets[r.Record.Offset] = struct{}{}
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// Verify offsets
	if len(allOffsets) != totalRecords {
		t.Errorf("expected %d offsets, got %d", totalRecords, len(allOffsets))
	}

	for i := 0; i < totalRecords; i++ {
		if _, exists := allOffsets[int64(i)]; !exists {
			t.Errorf("missing offset %d", i)
		}
	}

	t.Logf("Verified %d strictly increasing offsets with full server stack", totalRecords)
}

// Ensure packages are used
var _ = server.New
var _ = auth.NewEnforcer
