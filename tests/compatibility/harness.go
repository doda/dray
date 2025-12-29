// Package compatibility provides a Kafka client compatibility test harness
// using franz-go to verify Dray behaves like a real Kafka broker.
package compatibility

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/groups"
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

// TestBroker encapsulates a fully-functional test broker with all protocol handlers.
type TestBroker struct {
	metaStore     *metadata.MockStore
	topicStore    *topics.Store
	groupStore    *groups.Store
	streamManager *index.StreamManager
	objStore      *mockObjectStore
	buffer        *produce.Buffer
	committer     *produce.Committer
	server        *server.Server
	handler       *CompatibilityHandler
	addr          string
	port          int32
	t             *testing.T
}

// NewTestBroker creates a new test broker with all components wired up.
func NewTestBroker(t *testing.T) *TestBroker {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	groupStore := groups.NewStore(metaStore)
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

	return &TestBroker{
		metaStore:     metaStore,
		topicStore:    topicStore,
		groupStore:    groupStore,
		streamManager: streamManager,
		objStore:      objStore,
		buffer:        buffer,
		committer:     committer,
		t:             t,
	}
}

// Start starts the test broker and returns when it's ready.
func (b *TestBroker) Start() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.t.Fatalf("failed to create listener: %v", err)
	}

	tcpAddr := ln.Addr().(*net.TCPAddr)
	b.port = int32(tcpAddr.Port)
	b.addr = ln.Addr().String()

	enforcer := auth.NewEnforcer(nil, auth.EnforcerConfig{Enabled: false}, nil)

	b.handler = NewCompatibilityHandler(
		b.metaStore,
		b.topicStore,
		b.groupStore,
		b.buffer,
		b.objStore,
		b.streamManager,
		b.port,
		enforcer,
	)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelDebug)

	b.server = server.New(server.Config{
		ListenAddr:     b.addr,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxRequestSize: 10 * 1024 * 1024,
	}, b.handler, logger)

	go func() {
		b.server.Serve(ln)
	}()

	time.Sleep(50 * time.Millisecond)
}

// Stop shuts down the test broker.
func (b *TestBroker) Stop() {
	b.buffer.Close()
	b.server.Close()
}

// Addr returns the broker address for client connections.
func (b *TestBroker) Addr() string {
	return b.addr
}

// CreateTopic creates a topic with the specified partition count.
func (b *TestBroker) CreateTopic(ctx context.Context, name string, partitions int32) error {
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

// NewClient creates a franz-go client configured to connect to this broker.
func (b *TestBroker) NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	baseOpts := []kgo.Opt{
		kgo.SeedBrokers(b.addr),
		kgo.DisableIdempotentWrite(),
	}
	allOpts := append(baseOpts, opts...)
	return kgo.NewClient(allOpts...)
}

// CompatibilityHandler routes requests to appropriate protocol handlers.
type CompatibilityHandler struct {
	decoder            *protocol.Decoder
	encoder            *protocol.Encoder
	apiVersions        *protocol.ApiVersionsHandler
	metadataHandler    *protocol.MetadataHandler
	produceHandler     *protocol.ProduceHandler
	fetchHandler       *protocol.FetchHandler
	listOffsets        *protocol.ListOffsetsHandler
	createTopics       *protocol.CreateTopicsHandler
	deleteTopics       *protocol.DeleteTopicsHandler
	findCoordinator    *protocol.FindCoordinatorHandler
	joinGroup          *protocol.JoinGroupHandler
	syncGroup          *protocol.SyncGroupHandler
	heartbeat          *protocol.HeartbeatHandler
	leaveGroup         *protocol.LeaveGroupHandler
	offsetCommit       *protocol.OffsetCommitHandler
	offsetFetch        *protocol.OffsetFetchHandler
	describeGroups     *protocol.DescribeGroupsHandler
	listGroups         *protocol.ListGroupsHandler
	deleteGroups       *protocol.DeleteGroupsHandler
	describeConfigs    *protocol.DescribeConfigsHandler
	incrementalAlter   *protocol.IncrementalAlterConfigsHandler
	describeCluster    *protocol.DescribeClusterHandler
	consumerHeartbeat  *protocol.ConsumerGroupHeartbeatHandler
	consumerDescribe   *protocol.ConsumerGroupDescribeHandler
}

// NewCompatibilityHandler creates a handler with all protocol handlers.
func NewCompatibilityHandler(
	metaStore *metadata.MockStore,
	topicStore *topics.Store,
	groupStore *groups.Store,
	buffer *produce.Buffer,
	objStore *mockObjectStore,
	streamManager *index.StreamManager,
	port int32,
	enforcer *auth.Enforcer,
) *CompatibilityHandler {
	metadataHandler := protocol.NewMetadataHandler(
		protocol.MetadataHandlerConfig{
			ClusterID:          "compatibility-test-cluster",
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
		streamManager,
	)

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

	createTopicsHandler := protocol.NewCreateTopicsHandler(
		protocol.CreateTopicsHandlerConfig{
			DefaultPartitions:       1,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil, // No Iceberg catalog for tests
	)

	deleteTopicsHandler := protocol.NewDeleteTopicsHandler(
		protocol.DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil, // No Iceberg catalog for tests
	)

	findCoordinatorHandler := protocol.NewFindCoordinatorHandler(
		protocol.FindCoordinatorHandlerConfig{
			LocalBroker: protocol.BrokerInfo{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   port,
			},
		},
	)

	joinGroupHandler := protocol.NewJoinGroupHandler(
		protocol.JoinGroupHandlerConfig{
			MinSessionTimeoutMs: 1000,
			MaxSessionTimeoutMs: 30000,
			RebalanceTimeoutMs:  5000,
		},
		groupStore,
		nil,
	)

	syncGroupHandler := protocol.NewSyncGroupHandler(groupStore, nil)
	heartbeatHandler := protocol.NewHeartbeatHandler(groupStore, nil)
	leaveGroupHandler := protocol.NewLeaveGroupHandler(groupStore, nil, joinGroupHandler)
	offsetCommitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	offsetFetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)
	describeGroupsHandler := protocol.NewDescribeGroupsHandler(groupStore, nil)
	listGroupsHandler := protocol.NewListGroupsHandler(groupStore)
	deleteGroupsHandler := protocol.NewDeleteGroupsHandler(groupStore, nil, metaStore)

	describeConfigsHandler := protocol.NewDescribeConfigsHandler(topicStore, 1)
	incrementalAlterHandler := protocol.NewIncrementalAlterConfigsHandler(topicStore)
	describeClusterHandler := protocol.NewDescribeClusterHandler(
		protocol.DescribeClusterHandlerConfig{
			ClusterID:    "compatibility-test-cluster",
			ControllerID: 1,
			LocalBroker: protocol.BrokerInfo{
				NodeID: 1,
				Host:   "127.0.0.1",
				Port:   port,
			},
		},
	)

	consumerHeartbeatHandler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)
	consumerDescribeHandler := protocol.NewConsumerGroupDescribeHandler(groupStore, nil)

	return &CompatibilityHandler{
		decoder:           protocol.NewDecoder(),
		encoder:           protocol.NewEncoder(),
		apiVersions:       protocol.NewApiVersionsHandler(),
		metadataHandler:   metadataHandler,
		produceHandler:    produceHandler,
		fetchHandler:      fetchHandler,
		listOffsets:       listOffsetsHandler,
		createTopics:      createTopicsHandler,
		deleteTopics:      deleteTopicsHandler,
		findCoordinator:   findCoordinatorHandler,
		joinGroup:         joinGroupHandler,
		syncGroup:         syncGroupHandler,
		heartbeat:         heartbeatHandler,
		leaveGroup:        leaveGroupHandler,
		offsetCommit:      offsetCommitHandler,
		offsetFetch:       offsetFetchHandler,
		describeGroups:    describeGroupsHandler,
		listGroups:        listGroupsHandler,
		deleteGroups:      deleteGroupsHandler,
		describeConfigs:   describeConfigsHandler,
		incrementalAlter:  incrementalAlterHandler,
		describeCluster:   describeClusterHandler,
		consumerHeartbeat: consumerHeartbeatHandler,
		consumerDescribe:  consumerDescribeHandler,
	}
}

// HandleRequest implements server.Handler.
func (h *CompatibilityHandler) HandleRequest(ctx context.Context, header *server.RequestHeader, payload []byte) ([]byte, error) {
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
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

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

	case 19: // CreateTopics
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode CreateTopics request: %w", err)
		}
		createReq := req.Inner().(*kmsg.CreateTopicsRequest)
		resp := h.createTopics.Handle(ctx, version, createReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 20: // DeleteTopics
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DeleteTopics request: %w", err)
		}
		deleteReq := req.Inner().(*kmsg.DeleteTopicsRequest)
		resp := h.deleteTopics.Handle(ctx, version, deleteReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 10: // FindCoordinator
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode FindCoordinator request: %w", err)
		}
		findReq := req.Inner().(*kmsg.FindCoordinatorRequest)
		zoneID := protocol.ParseZoneID(header.ClientID)
		resp := h.findCoordinator.Handle(ctx, version, findReq, zoneID)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 11: // JoinGroup
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode JoinGroup request: %w", err)
		}
		joinReq := req.Inner().(*kmsg.JoinGroupRequest)
		resp := h.joinGroup.Handle(ctx, version, joinReq, header.ClientID)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 14: // SyncGroup
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode SyncGroup request: %w", err)
		}
		syncReq := req.Inner().(*kmsg.SyncGroupRequest)
		resp := h.syncGroup.Handle(ctx, version, syncReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 12: // Heartbeat
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Heartbeat request: %w", err)
		}
		hbReq := req.Inner().(*kmsg.HeartbeatRequest)
		resp := h.heartbeat.Handle(ctx, version, hbReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 13: // LeaveGroup
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode LeaveGroup request: %w", err)
		}
		leaveReq := req.Inner().(*kmsg.LeaveGroupRequest)
		resp := h.leaveGroup.Handle(ctx, version, leaveReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 8: // OffsetCommit
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode OffsetCommit request: %w", err)
		}
		commitReq := req.Inner().(*kmsg.OffsetCommitRequest)
		resp := h.offsetCommit.Handle(ctx, version, commitReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 9: // OffsetFetch
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode OffsetFetch request: %w", err)
		}
		fetchReq := req.Inner().(*kmsg.OffsetFetchRequest)
		resp := h.offsetFetch.Handle(ctx, version, fetchReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 15: // DescribeGroups
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DescribeGroups request: %w", err)
		}
		descReq := req.Inner().(*kmsg.DescribeGroupsRequest)
		resp := h.describeGroups.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 16: // ListGroups
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ListGroups request: %w", err)
		}
		listReq := req.Inner().(*kmsg.ListGroupsRequest)
		resp := h.listGroups.Handle(ctx, version, listReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 42: // DeleteGroups
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DeleteGroups request: %w", err)
		}
		deleteReq := req.Inner().(*kmsg.DeleteGroupsRequest)
		resp := h.deleteGroups.Handle(ctx, version, deleteReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 32: // DescribeConfigs
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DescribeConfigs request: %w", err)
		}
		descReq := req.Inner().(*kmsg.DescribeConfigsRequest)
		resp := h.describeConfigs.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 44: // IncrementalAlterConfigs
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode IncrementalAlterConfigs request: %w", err)
		}
		alterReq := req.Inner().(*kmsg.IncrementalAlterConfigsRequest)
		resp := h.incrementalAlter.Handle(ctx, version, alterReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 60: // DescribeCluster
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DescribeCluster request: %w", err)
		}
		descReq := req.Inner().(*kmsg.DescribeClusterRequest)
		resp := h.describeCluster.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 68: // ConsumerGroupHeartbeat
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ConsumerGroupHeartbeat request: %w", err)
		}
		hbReq := req.Inner().(*kmsg.ConsumerGroupHeartbeatRequest)
		resp := h.consumerHeartbeat.Handle(ctx, version, hbReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 69: // ConsumerGroupDescribe
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ConsumerGroupDescribe request: %w", err)
		}
		descReq := req.Inner().(*kmsg.ConsumerGroupDescribeRequest)
		resp := h.consumerDescribe.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	default:
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
}

func (h *CompatibilityHandler) encodeResponse(correlationID int32, version int16, resp *protocol.Response) ([]byte, error) {
	resp.SetVersion(version)
	return h.encoder.EncodeResponseWithCorrelationID(correlationID, resp), nil
}

// TestResult captures the outcome of a compatibility test.
type TestResult struct {
	Name    string
	Passed  bool
	Message string
	Error   error
}

// CompatibilityTestSuite runs a collection of compatibility tests.
type CompatibilityTestSuite struct {
	broker  *TestBroker
	results []TestResult
	t       *testing.T
}

// NewCompatibilityTestSuite creates a new test suite.
func NewCompatibilityTestSuite(t *testing.T) *CompatibilityTestSuite {
	broker := NewTestBroker(t)
	broker.Start()
	return &CompatibilityTestSuite{
		broker: broker,
		t:      t,
	}
}

// Close shuts down the test suite.
func (s *CompatibilityTestSuite) Close() {
	s.broker.Stop()
}

// Broker returns the underlying test broker.
func (s *CompatibilityTestSuite) Broker() *TestBroker {
	return s.broker
}

// RecordResult adds a test result.
func (s *CompatibilityTestSuite) RecordResult(name string, passed bool, message string, err error) {
	s.results = append(s.results, TestResult{
		Name:    name,
		Passed:  passed,
		Message: message,
		Error:   err,
	})
}

// Results returns all recorded results.
func (s *CompatibilityTestSuite) Results() []TestResult {
	return s.results
}
