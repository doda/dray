package integration

import (
	"context"
	"fmt"
	"net"
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
	"github.com/twmb/franz-go/pkg/kmsg"
)

// BrokerHandler implements server.Handler and routes requests to appropriate handlers.
type BrokerHandler struct {
	decoder         *protocol.Decoder
	encoder         *protocol.Encoder
	apiVersions     *protocol.ApiVersionsHandler
	metadataHandler *protocol.MetadataHandler
	produceHandler  *protocol.ProduceHandler
	fetchHandler    *protocol.FetchHandler
	listOffsets     *protocol.ListOffsetsHandler
}

// NewBrokerHandler creates a new broker handler with all protocol handlers.
func NewBrokerHandler(
	metadataHandler *protocol.MetadataHandler,
	produceHandler *protocol.ProduceHandler,
	fetchHandler *protocol.FetchHandler,
	listOffsets *protocol.ListOffsetsHandler,
) *BrokerHandler {
	return &BrokerHandler{
		decoder:         protocol.NewDecoder(),
		encoder:         protocol.NewEncoder(),
		apiVersions:     protocol.NewApiVersionsHandler(),
		metadataHandler: metadataHandler,
		produceHandler:  produceHandler,
		fetchHandler:    fetchHandler,
		listOffsets:     listOffsets,
	}
}

// HandleRequest implements server.Handler.
func (h *BrokerHandler) HandleRequest(ctx context.Context, header *server.RequestHeader, payload []byte) ([]byte, error) {
	apiKey := header.APIKey
	version := header.APIVersion
	correlationID := header.CorrelationID

	switch apiKey {
	case 18: // ApiVersions
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ApiVersions request: %w", err)
		}
		apiReq := req.Inner().(*kmsg.ApiVersionsRequest)
		resp := h.apiVersions.Handle(version, apiReq)
		respBytes, err := h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))
		if err != nil {
			return nil, err
		}
		return respBytes, nil

	case 3: // Metadata
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Metadata request: %w", err)
		}
		metaReq := req.Inner().(*kmsg.MetadataRequest)
		zoneID := protocol.ParseZoneID(header.ClientID)
		resp := h.metadataHandler.Handle(ctx, version, metaReq, zoneID)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 0: // Produce
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Produce request: %w", err)
		}
		produceReq := req.Inner().(*kmsg.ProduceRequest)
		resp := h.produceHandler.Handle(ctx, version, produceReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 1: // Fetch
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Fetch request: %w", err)
		}
		fetchReq := req.Inner().(*kmsg.FetchRequest)
		resp := h.fetchHandler.Handle(ctx, version, fetchReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 2: // ListOffsets
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ListOffsets request: %w", err)
		}
		listReq := req.Inner().(*kmsg.ListOffsetsRequest)
		resp := h.listOffsets.Handle(ctx, version, listReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	default:
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
}

func (h *BrokerHandler) encodeResponse(correlationID int32, version int16, resp *protocol.Response) ([]byte, error) {
	resp.SetVersion(version)
	return h.encoder.EncodeResponseWithCorrelationID(correlationID, resp), nil
}

// testBroker encapsulates all the components needed to run a test broker.
type testBroker struct {
	metaStore     *metadata.MockStore
	topicStore    *topics.Store
	streamManager *index.StreamManager
	objStore      *mockObjectStore
	buffer        *produce.Buffer
	committer     *produce.Committer
	server        *server.Server
	addr          string
	port          int32
	autoCreate    bool
	t             *testing.T
}

// newTestBroker creates a new test broker with all components wired up.
// Must call start() to get the actual port before use.
func newTestBroker(t *testing.T) *testBroker {
	return newTestBrokerWithAutoCreate(t, false)
}

func newTestBrokerWithAutoCreate(t *testing.T, autoCreate bool) *testBroker {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 10 * 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush for tests
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})

	return &testBroker{
		metaStore:     metaStore,
		topicStore:    topicStore,
		streamManager: streamManager,
		objStore:      objStore,
		buffer:        buffer,
		committer:     committer,
		autoCreate:    autoCreate,
		t:             t,
	}
}

// start starts the test broker and returns when it's ready to accept connections.
func (b *testBroker) start() {
	// First, create a listener to get the port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.t.Fatalf("failed to create listener: %v", err)
	}

	// Extract port from the listener address
	tcpAddr := ln.Addr().(*net.TCPAddr)
	b.port = int32(tcpAddr.Port)
	b.addr = ln.Addr().String()

	// Now create handlers with the actual port
	metadataHandler := protocol.NewMetadataHandler(
		protocol.MetadataHandlerConfig{
			ClusterID:          "test-cluster",
			ControllerID:       1,
			AutoCreateTopics:   b.autoCreate,
			DefaultPartitions:  1,
			DefaultReplication: 1,
			LocalBroker: protocol.BrokerInfo{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   b.port,
				Rack:   "",
			},
		},
		b.topicStore,
		b.streamManager,
	)

	// Create a disabled enforcer - ACL enforcement is off so all operations are allowed.
	// This demonstrates proper wiring while not blocking test operations.
	enforcer := auth.NewEnforcer(nil, auth.EnforcerConfig{Enabled: false}, nil)

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		b.topicStore,
		b.buffer,
	).WithEnforcer(enforcer)

	fetcher := fetch.NewFetcher(b.objStore, b.streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 10 * 1024 * 1024},
		b.topicStore,
		fetcher,
		b.streamManager,
	).WithEnforcer(enforcer)

	listOffsetsHandler := protocol.NewListOffsetsHandler(b.topicStore, b.streamManager)

	brokerHandler := NewBrokerHandler(metadataHandler, produceHandler, fetchHandler, listOffsetsHandler)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelDebug)

	b.server = server.New(server.Config{
		ListenAddr:     b.addr,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxRequestSize: 10 * 1024 * 1024,
	}, brokerHandler, logger)

	// Start the server with the pre-created listener
	go func() {
		b.server.Serve(ln)
	}()

	// Wait a bit for the server to be ready
	time.Sleep(50 * time.Millisecond)
}

// stop shuts down the test broker.
func (b *testBroker) stop() {
	b.buffer.Close()
	b.server.Close()
}

// createTopic creates a topic with the specified partition count.
func (b *testBroker) createTopic(ctx context.Context, name string, partitions int32) error {
	result, err := b.topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           name,
		PartitionCount: partitions,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}

	for _, p := range result.Partitions {
		if err := b.streamManager.CreateStreamWithID(ctx, p.StreamID, name, p.Partition); err != nil {
			return err
		}
	}
	return nil
}

// TestProduceFetchIntegration tests the produce and fetch flow with a real Kafka client.
func TestProduceFetchIntegration(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "test-topic"

	// Create the topic before producing
	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create franz-go client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DefaultProduceTopic(topicName),
		kgo.ProducerBatchMaxBytes(1024*1024),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Produce messages
	messages := []string{
		"message-0",
		"message-1",
		"message-2",
		"message-3",
		"message-4",
	}

	var producedOffsets []int64
	for i, msg := range messages {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Errorf("produce failed for message %d: %v", i, result.FirstErr())
			continue
		}
		for _, r := range result {
			producedOffsets = append(producedOffsets, r.Record.Offset)
			t.Logf("Produced message %d at offset %d", i, r.Record.Offset)
		}
	}

	// Verify offsets are monotonically increasing
	if len(producedOffsets) != len(messages) {
		t.Fatalf("expected %d produced offsets, got %d", len(messages), len(producedOffsets))
	}

	// Fetch messages from offset 0
	t.Log("Fetching messages from offset 0...")

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Use a separate client for consuming
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

	var fetchedMessages []string
	var fetchedOffsets []int64

	for len(fetchedMessages) < len(messages) {
		fetches := consumerClient.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				t.Logf("Fetch error: topic=%s partition=%d err=%v", err.Topic, err.Partition, err.Err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			fetchedMessages = append(fetchedMessages, string(r.Value))
			fetchedOffsets = append(fetchedOffsets, r.Offset)
			t.Logf("Fetched message at offset %d: %s", r.Offset, string(r.Value))
		})

		if fetchCtx.Err() != nil {
			break
		}
	}

	// Verify message content
	if len(fetchedMessages) != len(messages) {
		t.Errorf("expected %d fetched messages, got %d", len(messages), len(fetchedMessages))
	}

	// Verify offsets are monotonically increasing
	for i := 1; i < len(fetchedOffsets); i++ {
		if fetchedOffsets[i] <= fetchedOffsets[i-1] {
			t.Errorf("offsets not monotonically increasing: offset[%d]=%d <= offset[%d]=%d",
				i, fetchedOffsets[i], i-1, fetchedOffsets[i-1])
		}
	}

	t.Logf("Successfully produced and fetched %d messages", len(fetchedMessages))
}

func TestProduceFetchIntegration_AutoCreateTopic(t *testing.T) {
	broker := newTestBrokerWithAutoCreate(t, true)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "auto-created-produce-fetch"

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

	metaReq := kmsg.NewPtrMetadataRequest()
	metaReq.AllowAutoTopicCreation = true
	metaTopic := kmsg.NewMetadataRequestTopic()
	metaTopic.Topic = &topicName
	metaReq.Topics = append(metaReq.Topics, metaTopic)
	if _, err := metaReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("metadata request failed: %v", err)
	}

	result := client.ProduceSync(ctx, &kgo.Record{
		Topic: topicName,
		Key:   []byte("key"),
		Value: []byte("auto-created"),
	})
	if result.FirstErr() != nil {
		t.Fatalf("produce failed: %v", result.FirstErr())
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create consumer client: %v", err)
	}
	defer consumer.Close()

	var fetched string
	for fetched == "" && fetchCtx.Err() == nil {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if fetched == "" {
				fetched = string(r.Value)
			}
		})
	}

	if fetched != "auto-created" {
		t.Fatalf("expected fetched message to be %q, got %q", "auto-created", fetched)
	}
}

// TestProduceFetchIntegration_MultiplePartitions tests produce/fetch with multiple partitions.
func TestProduceFetchIntegration_MultiplePartitions(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "multi-partition-topic"
	numPartitions := int32(3)

	// Create the topic
	if err := broker.createTopic(ctx, topicName, numPartitions); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create franz-go client with manual partitioner
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Produce messages to different partitions
	messagesPerPartition := 3
	for partition := int32(0); partition < numPartitions; partition++ {
		for i := 0; i < messagesPerPartition; i++ {
			record := &kgo.Record{
				Topic:     topicName,
				Partition: partition,
				Key:       []byte(fmt.Sprintf("key-p%d-%d", partition, i)),
				Value:     []byte(fmt.Sprintf("partition-%d-message-%d", partition, i)),
			}
			result := client.ProduceSync(ctx, record)
			if result.FirstErr() != nil {
				t.Errorf("produce to partition %d failed: %v", partition, result.FirstErr())
			}
			for _, r := range result {
				t.Logf("Produced to partition %d at offset %d", partition, r.Record.Offset)
			}
		}
	}

	// Create consumer client
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

	// Fetch all messages
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	partitionCounts := make(map[int32]int)
	partitionOffsets := make(map[int32][]int64)
	totalExpected := int(numPartitions) * messagesPerPartition

	for sum := 0; sum < totalExpected; {
		fetches := consumerClient.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			partitionCounts[r.Partition]++
			partitionOffsets[r.Partition] = append(partitionOffsets[r.Partition], r.Offset)
			sum++
			t.Logf("Fetched from partition %d at offset %d: %s", r.Partition, r.Offset, string(r.Value))
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	// Verify each partition has the expected number of messages
	for partition := int32(0); partition < numPartitions; partition++ {
		if partitionCounts[partition] != messagesPerPartition {
			t.Errorf("partition %d: expected %d messages, got %d",
				partition, messagesPerPartition, partitionCounts[partition])
		}

		// Verify offsets within partition are monotonically increasing
		offsets := partitionOffsets[partition]
		for i := 1; i < len(offsets); i++ {
			if offsets[i] <= offsets[i-1] {
				t.Errorf("partition %d: offsets not monotonic: %d <= %d",
					partition, offsets[i], offsets[i-1])
			}
		}
	}

	t.Logf("Successfully produced and fetched %d messages across %d partitions",
		totalExpected, numPartitions)
}

// TestProduceFetchIntegration_ContentVerification verifies exact message content.
func TestProduceFetchIntegration_ContentVerification(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "content-verification-topic"

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

	// Produce messages with specific content
	testCases := []struct {
		key   string
		value string
	}{
		{"key-1", "Hello, World!"},
		{"key-2", "The quick brown fox jumps over the lazy dog"},
		{"key-3", "Special chars: !@#$%^&*()"},
		{"key-4", "ASCII only test string"},
		{"key-5", "Empty value next"},
		{"key-6", ""},
		{"", "Empty key"},
	}

	for _, tc := range testCases {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(tc.key),
			Value: []byte(tc.value),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Errorf("produce failed for key=%q: %v", tc.key, result.FirstErr())
		}
	}

	// Consume and verify
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

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	fetched := make(map[string]string)
	for len(fetched) < len(testCases) {
		fetches := consumerClient.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetched[string(r.Key)] = string(r.Value)
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	// Verify content matches
	for _, tc := range testCases {
		if v, ok := fetched[tc.key]; !ok {
			t.Errorf("key %q not found in fetched messages", tc.key)
		} else if v != tc.value {
			t.Errorf("key %q: expected value %q, got %q", tc.key, tc.value, v)
		}
	}

	t.Logf("Successfully verified content of %d messages", len(testCases))
}

// TestProduceFetchIntegration_OffsetMonotonicity tests that offsets are strictly increasing.
func TestProduceFetchIntegration_OffsetMonotonicity(t *testing.T) {
	broker := newTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "offset-monotonicity-topic"

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

	// Produce multiple batches of messages
	numBatches := 5
	messagesPerBatch := 10
	var allOffsets []int64

	for batch := 0; batch < numBatches; batch++ {
		var records []*kgo.Record
		for i := 0; i < messagesPerBatch; i++ {
			records = append(records, &kgo.Record{
				Topic: topicName,
				Key:   []byte(fmt.Sprintf("batch-%d-key-%d", batch, i)),
				Value: []byte(fmt.Sprintf("batch-%d-value-%d", batch, i)),
			})
		}

		results := client.ProduceSync(ctx, records...)
		if results.FirstErr() != nil {
			t.Fatalf("batch %d produce failed: %v", batch, results.FirstErr())
		}

		for _, r := range results {
			allOffsets = append(allOffsets, r.Record.Offset)
		}
	}

	// Verify produced offsets are monotonically increasing
	for i := 1; i < len(allOffsets); i++ {
		if allOffsets[i] <= allOffsets[i-1] {
			t.Errorf("produced offsets not monotonic at index %d: %d <= %d",
				i, allOffsets[i], allOffsets[i-1])
		}
	}

	// Consume and verify fetch offsets
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
	expected := numBatches * messagesPerBatch

	for len(fetchedOffsets) < expected {
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

	// Verify fetched offsets are monotonically increasing
	for i := 1; i < len(fetchedOffsets); i++ {
		if fetchedOffsets[i] <= fetchedOffsets[i-1] {
			t.Errorf("fetched offsets not monotonic at index %d: %d <= %d",
				i, fetchedOffsets[i], fetchedOffsets[i-1])
		}
	}

	if len(fetchedOffsets) != expected {
		t.Fatalf("expected %d fetched messages, got %d", expected, len(fetchedOffsets))
	}

	// Verify the last offset is what we expect
	expectedLastOffset := int64(expected - 1)
	if fetchedOffsets[len(fetchedOffsets)-1] != expectedLastOffset {
		t.Errorf("expected last offset %d, got %d",
			expectedLastOffset, fetchedOffsets[len(fetchedOffsets)-1])
	}

	t.Logf("Verified %d offsets are monotonically increasing (0 to %d)",
		len(fetchedOffsets), fetchedOffsets[len(fetchedOffsets)-1])
}

// Ensure mockObjectStore is available (from read_your_writes_test.go)
var _ = newMockObjectStore
