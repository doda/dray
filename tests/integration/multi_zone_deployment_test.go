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
	"github.com/dray-io/dray/internal/routing"
	"github.com/dray-io/dray/internal/server"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type zoneBroker struct {
	t             *testing.T
	metaStore     *metadata.MockStore
	objStore      *mockObjectStore
	registry      *routing.Registry
	topicStore    *topics.Store
	streamManager *index.StreamManager
	buffer        *produce.Buffer
	committer     *produce.Committer
	server        *server.Server
	clusterID     string
	brokerID      string
	zoneID        string
	addr          string
	port          int32
	nodeID        int32
}

func newZoneBroker(t *testing.T, metaStore *metadata.MockStore, objStore *mockObjectStore, clusterID, brokerID, zoneID string, nodeID int32) *zoneBroker {
	return &zoneBroker{
		t:         t,
		metaStore: metaStore,
		objStore:  objStore,
		clusterID: clusterID,
		brokerID:  brokerID,
		zoneID:    zoneID,
		nodeID:    nodeID,
	}
}

func (b *zoneBroker) start(ctx context.Context) {
	b.t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.t.Fatalf("failed to create listener: %v", err)
	}

	tcpAddr := ln.Addr().(*net.TCPAddr)
	b.port = int32(tcpAddr.Port)
	b.addr = ln.Addr().String()

	b.registry = routing.NewRegistry(b.metaStore, routing.RegistryConfig{
		ClusterID:           b.clusterID,
		BrokerID:            b.brokerID,
		NodeID:              b.nodeID,
		ZoneID:              b.zoneID,
		AdvertisedListeners: []string{b.addr},
	})
	if err := b.registry.Register(ctx); err != nil {
		b.t.Fatalf("failed to register broker %s: %v", b.brokerID, err)
	}

	b.topicStore = topics.NewStore(b.metaStore)
	b.streamManager = index.NewStreamManager(b.metaStore)
	b.committer = produce.NewCommitter(b.objStore, b.metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})
	b.buffer = produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 10 * 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        b.committer.CreateFlushHandler(),
	})

	adapter := routing.NewAffinityListerAdapter(b.registry)
	metadataHandler := protocol.NewMetadataHandler(protocol.MetadataHandlerConfig{
		ClusterID:          b.clusterID,
		ControllerID:       1,
		AutoCreateTopics:   false,
		DefaultPartitions:  1,
		DefaultReplication: 1,
		LocalBroker: protocol.BrokerInfo{
			NodeID: b.nodeID,
			Host:   "127.0.0.1",
			Port:   b.port,
			Rack:   b.zoneID,
		},
		BrokerLister:   adapter,
		LeaderSelector: adapter,
	}, b.topicStore)

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

	go func() {
		b.server.Serve(ln)
	}()

	time.Sleep(50 * time.Millisecond)
}

func (b *zoneBroker) stop(ctx context.Context) {
	b.t.Helper()
	if b.buffer != nil {
		b.buffer.Close()
	}
	if b.server != nil {
		b.server.Close()
	}
	if b.registry != nil {
		if err := b.registry.Deregister(ctx); err != nil {
			b.t.Fatalf("failed to deregister broker %s: %v", b.brokerID, err)
		}
	}
}

func createTopicWithStreams(ctx context.Context, t *testing.T, metaStore *metadata.MockStore, name string, partitions int32) {
	t.Helper()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           name,
		PartitionCount: partitions,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for _, p := range result.Partitions {
		if err := streamManager.CreateStreamWithID(ctx, p.StreamID, name, p.Partition); err != nil {
			t.Fatalf("failed to create stream for partition %d: %v", p.Partition, err)
		}
	}
}

func requestMetadata(ctx context.Context, t *testing.T, client *kgo.Client, topic string) *kmsg.MetadataResponse {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	req.SetVersion(9)
	if topic != "" {
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = &topic
		req.Topics = append(req.Topics, reqTopic)
	}
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("metadata request failed: %v", err)
	}
	return resp
}

func assertZoneBrokers(t *testing.T, resp *kmsg.MetadataResponse, zone string, expectedNodeIDs []int32) {
	t.Helper()
	expected := make(map[int32]struct{}, len(expectedNodeIDs))
	for _, id := range expectedNodeIDs {
		expected[id] = struct{}{}
	}
	if len(resp.Brokers) != len(expectedNodeIDs) {
		t.Fatalf("expected %d brokers for zone %s, got %d", len(expectedNodeIDs), zone, len(resp.Brokers))
	}
	for _, broker := range resp.Brokers {
		if broker.Rack == nil || *broker.Rack != zone {
			t.Fatalf("expected broker rack %s, got %v", zone, broker.Rack)
		}
		if _, ok := expected[broker.NodeID]; !ok {
			t.Fatalf("unexpected broker %d for zone %s", broker.NodeID, zone)
		}
	}
	if len(resp.Topics) > 0 {
		for _, partition := range resp.Topics[0].Partitions {
			if _, ok := expected[partition.Leader]; !ok {
				t.Fatalf("partition %d leader %d not in zone %s", partition.Partition, partition.Leader, zone)
			}
		}
	}
}

func newZoneClient(t *testing.T, addr, zoneID string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.ClientID(fmt.Sprintf("zone_id=%s", zoneID)),
		kgo.DisableIdempotentWrite(),
	}
	base = append(base, opts...)
	client, err := kgo.NewClient(base...)
	if err != nil {
		t.Fatalf("failed to create client for zone %s: %v", zoneID, err)
	}
	return client
}

func TestMultiZoneDeployment(t *testing.T) {
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	clusterID := "multi-zone-cluster"
	brokers := []*zoneBroker{
		newZoneBroker(t, metaStore, objStore, clusterID, "broker-a", "zone-a", 1),
		newZoneBroker(t, metaStore, objStore, clusterID, "broker-b", "zone-b", 2),
		newZoneBroker(t, metaStore, objStore, clusterID, "broker-c", "zone-c", 3),
	}

	for _, broker := range brokers {
		broker.start(ctx)
	}
	defer func() {
		for _, broker := range brokers {
			broker.stop(ctx)
		}
	}()

	topicName := "multi-zone-topic"
	createTopicWithStreams(ctx, t, metaStore, topicName, 1)

	zoneAClient := newZoneClient(t, brokers[0].addr, "zone-a")
	defer zoneAClient.Close()
	zoneBClient := newZoneClient(t, brokers[1].addr, "zone-b")
	defer zoneBClient.Close()
	zoneCClient := newZoneClient(t, brokers[2].addr, "zone-c")
	defer zoneCClient.Close()

	metaCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	respA := requestMetadata(metaCtx, t, zoneAClient, topicName)
	assertZoneBrokers(t, respA, "zone-a", []int32{1})

	respB := requestMetadata(metaCtx, t, zoneBClient, topicName)
	assertZoneBrokers(t, respB, "zone-b", []int32{2})

	respC := requestMetadata(metaCtx, t, zoneCClient, topicName)
	assertZoneBrokers(t, respC, "zone-c", []int32{3})

	fallbackClient := newZoneClient(t, brokers[0].addr, "zone-d")
	defer fallbackClient.Close()
	fallbackResp := requestMetadata(metaCtx, t, fallbackClient, topicName)
	if len(fallbackResp.Brokers) != 3 {
		t.Fatalf("expected 3 brokers for fallback, got %d", len(fallbackResp.Brokers))
	}
	zones := make(map[string]struct{})
	for _, broker := range fallbackResp.Brokers {
		if broker.Rack != nil {
			zones[*broker.Rack] = struct{}{}
		}
	}
	for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
		if _, ok := zones[zone]; !ok {
			t.Fatalf("fallback metadata missing zone %s", zone)
		}
	}

	records := []struct {
		client *kgo.Client
		value  string
	}{
		{zoneAClient, "zone-a-message-1"},
		{zoneAClient, "zone-a-message-2"},
		{zoneBClient, "zone-b-message-1"},
		{zoneBClient, "zone-b-message-2"},
	}

	for _, record := range records {
		result := record.client.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte(record.value),
		})
		if err := result.FirstErr(); err != nil {
			t.Fatalf("produce failed for %s: %v", record.value, err)
		}
	}

	consumer := newZoneClient(
		t,
		brokers[2].addr,
		"zone-c",
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer fetchCancel()

	expected := map[string]struct{}{
		"zone-a-message-1": {},
		"zone-a-message-2": {},
		"zone-b-message-1": {},
		"zone-b-message-2": {},
	}

	for len(expected) > 0 {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			delete(expected, string(r.Value))
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if len(expected) > 0 {
		missing := make([]string, 0, len(expected))
		for value := range expected {
			missing = append(missing, value)
		}
		t.Fatalf("missing fetched records: %v", missing)
	}
}
