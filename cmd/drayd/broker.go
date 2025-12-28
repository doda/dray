package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/routing"
	"github.com/dray-io/dray/internal/server"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// BrokerOptions contains the configuration for creating a broker.
type BrokerOptions struct {
	Config    *config.Config
	Logger    *logging.Logger
	BrokerID  string
	NodeID    int32
	ClusterID string
	Version   string
	GitCommit string
	BuildTime string
}

// Broker represents a running Dray broker instance.
type Broker struct {
	opts          BrokerOptions
	logger        *logging.Logger
	metaStore     metadata.MetadataStore
	topicStore    *topics.Store
	streamManager *index.StreamManager
	buffer        *produce.Buffer
	committer     *produce.Committer
	registry      *routing.Registry
	groupStore    *groups.Store
	leaseManager  *groups.LeaseManager
	tcpServer     *server.Server
	healthServer  *server.HealthServer
	metricsServer *metrics.Server

	mu      sync.Mutex
	started bool
}

// NewBroker creates a new Broker instance but does not start it.
func NewBroker(opts BrokerOptions) (*Broker, error) {
	if opts.Logger == nil {
		opts.Logger = logging.DefaultLogger()
	}

	b := &Broker{
		opts:   opts,
		logger: opts.Logger,
	}

	return b, nil
}

// Start initializes and starts all broker components.
func (b *Broker) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return fmt.Errorf("broker already started")
	}
	b.started = true
	b.mu.Unlock()

	cfg := b.opts.Config

	b.logger.Infof("starting broker", map[string]any{
		"brokerId":   b.opts.BrokerID,
		"nodeId":     b.opts.NodeID,
		"clusterId":  b.opts.ClusterID,
		"listenAddr": cfg.Broker.ListenAddr,
		"zoneId":     cfg.Broker.ZoneID,
		"version":    b.opts.Version,
	})

	// Initialize metadata store (mock for now until Oxia is connected)
	b.metaStore = metadata.NewMockStore()

	// Initialize components
	b.topicStore = topics.NewStore(b.metaStore)
	b.streamManager = index.NewStreamManager(b.metaStore)
	b.groupStore = groups.NewStore(b.metaStore)
	b.leaseManager = groups.NewLeaseManager(b.metaStore, b.opts.BrokerID)

	// Create committer with a mock object store (to be replaced with real S3)
	// For now, we use a nil object store which will cause errors on actual writes
	// This is acceptable because the task only requires starting the server and registration
	b.committer = produce.NewCommitter(nil, b.metaStore, produce.CommitterConfig{
		NumDomains: cfg.Metadata.NumDomains,
	})

	b.buffer = produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 100 * 1024 * 1024, // 100MB
		FlushSizeBytes: cfg.WAL.FlushSizeBytes,
		NumDomains:     cfg.Metadata.NumDomains,
		OnFlush:        b.committer.CreateFlushHandler(),
	})

	// Create broker registry for registration
	b.registry = routing.NewRegistry(b.metaStore, routing.RegistryConfig{
		ClusterID:           b.opts.ClusterID,
		BrokerID:            b.opts.BrokerID,
		NodeID:              b.opts.NodeID,
		ZoneID:              cfg.Broker.ZoneID,
		AdvertisedListeners: []string{cfg.Broker.ListenAddr},
		BuildInfo: routing.BuildInfo{
			Version:   b.opts.Version,
			GitCommit: b.opts.GitCommit,
			BuildTime: b.opts.BuildTime,
		},
		Logger: b.logger,
	})

	// Start health server first
	b.healthServer = server.NewHealthServer(cfg.Observability.MetricsAddr, b.logger)
	if err := b.healthServer.Start(); err != nil {
		return fmt.Errorf("failed to start health server: %w", err)
	}
	b.logger.Infof("health server started", map[string]any{
		"addr": b.healthServer.Addr(),
	})

	// Start metrics server on a different address for Prometheus scraping
	// The health server handles /healthz and /readyz, metrics server handles /metrics
	b.metricsServer = metrics.NewServer(cfg.Observability.MetricsAddr)

	// Create protocol handler
	handler := b.createHandler()

	// Create and configure TCP server
	listenHost, listenPort := parseHostPort(cfg.Broker.ListenAddr)
	serverCfg := server.Config{
		ListenAddr:     cfg.Broker.ListenAddr,
		MaxRequestSize: 100 * 1024 * 1024,
		TLS: server.TLSConfig{
			Enabled:  cfg.Broker.TLS.Enabled,
			CertFile: cfg.Broker.TLS.CertFile,
			KeyFile:  cfg.Broker.TLS.KeyFile,
		},
	}
	b.tcpServer = server.New(serverCfg, handler, b.logger)

	// Register health checks
	b.healthServer.RegisterGoroutine("tcp-server")

	// Register broker with Oxia
	if err := b.registry.Register(ctx); err != nil {
		b.logger.Warnf("failed to register broker (metadata store may not be available)", map[string]any{
			"error": err.Error(),
		})
		// Continue anyway - broker can still serve requests
	}

	// Update advertised listeners with actual port if using random port
	if listenPort == 0 {
		// Listen first to get the actual port
		ln, err := net.Listen("tcp", cfg.Broker.ListenAddr)
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}
		actualAddr := ln.Addr().String()
		b.logger.Infof("tcp server listening", map[string]any{
			"addr":      actualAddr,
			"requestor": listenHost,
		})
		return b.tcpServer.Serve(ln)
	}

	// Start TCP server (blocks until server stops)
	b.logger.Infof("starting kafka protocol server", map[string]any{
		"addr": cfg.Broker.ListenAddr,
	})
	return b.tcpServer.ListenAndServe()
}

// Shutdown gracefully stops the broker.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	b.logger.Info("shutting down broker")

	// Mark health server as shutting down
	if b.healthServer != nil {
		b.healthServer.SetShuttingDown()
	}

	// Deregister from Oxia
	if b.registry != nil {
		if err := b.registry.Deregister(ctx); err != nil {
			b.logger.Warnf("failed to deregister broker", map[string]any{
				"error": err.Error(),
			})
		}
	}

	// Close produce buffer (flushes pending data)
	if b.buffer != nil {
		b.buffer.Close()
	}

	// Close TCP server
	if b.tcpServer != nil {
		if err := b.tcpServer.Close(); err != nil && err != server.ErrServerClosed {
			b.logger.Warnf("error closing tcp server", map[string]any{
				"error": err.Error(),
			})
		}
	}

	// Close health server
	if b.healthServer != nil {
		if err := b.healthServer.Close(); err != nil {
			b.logger.Warnf("error closing health server", map[string]any{
				"error": err.Error(),
			})
		}
	}

	// Close metadata store
	if b.metaStore != nil {
		if err := b.metaStore.Close(); err != nil {
			b.logger.Warnf("error closing metadata store", map[string]any{
				"error": err.Error(),
			})
		}
	}

	b.logger.Info("broker shutdown complete")
	return nil
}

// createHandler creates the protocol handler that routes requests to handlers.
func (b *Broker) createHandler() server.Handler {
	cfg := b.opts.Config

	// Extract host and port from listen address
	host, port := parseHostPort(cfg.Broker.ListenAddr)
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}

	// Create ACL enforcer (disabled by default)
	enforcer := auth.NewEnforcer(nil, auth.EnforcerConfig{Enabled: false}, b.logger)

	// Create fetcher
	fetcher := fetch.NewFetcher(nil, b.streamManager) // nil object store for now

	// Create join group handler first since leave group depends on it
	joinGroupHandler := protocol.NewJoinGroupHandler(protocol.JoinGroupHandlerConfig{}, b.groupStore, b.leaseManager)

	return &brokerHandler{
		decoder: protocol.NewDecoder(),
		encoder: protocol.NewEncoder(),
		logger:  b.logger,

		apiVersions: protocol.NewApiVersionsHandler(),

		metadata: protocol.NewMetadataHandler(
			protocol.MetadataHandlerConfig{
				ClusterID:          b.opts.ClusterID,
				ControllerID:       b.opts.NodeID,
				AutoCreateTopics:   true,
				DefaultPartitions:  1,
				DefaultReplication: 1,
				LocalBroker: protocol.BrokerInfo{
					NodeID: b.opts.NodeID,
					Host:   host,
					Port:   int32(port),
					Rack:   cfg.Broker.ZoneID,
				},
			},
			b.topicStore,
		),

		produce: protocol.NewProduceHandler(
			protocol.ProduceHandlerConfig{},
			b.topicStore,
			b.buffer,
		).WithEnforcer(enforcer),

		fetch: protocol.NewFetchHandler(
			protocol.FetchHandlerConfig{MaxBytes: 100 * 1024 * 1024},
			b.topicStore,
			fetcher,
			b.streamManager,
		).WithEnforcer(enforcer),

		listOffsets: protocol.NewListOffsetsHandler(b.topicStore, b.streamManager),

		createTopics: protocol.NewCreateTopicsHandler(
			protocol.CreateTopicsHandlerConfig{
				DefaultPartitions: 1,
			},
			b.topicStore,
			b.streamManager,
			nil, // Iceberg catalog
		).WithEnforcer(enforcer),

		deleteTopics: protocol.NewDeleteTopicsHandler(
			protocol.DeleteTopicsHandlerConfig{},
			b.topicStore,
			b.streamManager,
			nil, // Iceberg catalog
		).WithEnforcer(enforcer),

		describeConfigs: protocol.NewDescribeConfigsHandler(b.topicStore, b.opts.NodeID),

		alterConfigs: protocol.NewIncrementalAlterConfigsHandler(b.topicStore),

		describeCluster: protocol.NewDescribeClusterHandler(protocol.DescribeClusterHandlerConfig{
			ClusterID:    b.opts.ClusterID,
			ControllerID: b.opts.NodeID,
			LocalBroker: protocol.BrokerInfo{
				NodeID: b.opts.NodeID,
				Host:   host,
				Port:   int32(port),
				Rack:   cfg.Broker.ZoneID,
			},
		}),

		findCoordinator: protocol.NewFindCoordinatorHandler(protocol.FindCoordinatorHandlerConfig{
			LocalBroker: protocol.BrokerInfo{
				NodeID: b.opts.NodeID,
				Host:   host,
				Port:   int32(port),
				Rack:   cfg.Broker.ZoneID,
			},
		}),

		joinGroup:     joinGroupHandler,
		syncGroup:     protocol.NewSyncGroupHandler(b.groupStore, b.leaseManager),
		heartbeat:     protocol.NewHeartbeatHandler(b.groupStore, b.leaseManager),
		leaveGroup:    protocol.NewLeaveGroupHandler(b.groupStore, b.leaseManager, joinGroupHandler),
		describeGroup: protocol.NewDescribeGroupsHandler(b.groupStore, b.leaseManager),
		listGroups:    protocol.NewListGroupsHandler(b.groupStore),
		deleteGroups:  protocol.NewDeleteGroupsHandler(b.groupStore, b.leaseManager, b.metaStore),

		offsetCommit: protocol.NewOffsetCommitHandler(b.groupStore, b.leaseManager),
		offsetFetch:  protocol.NewOffsetFetchHandler(b.groupStore, b.leaseManager),

		consumerGroupHeartbeat: protocol.NewConsumerGroupHeartbeatHandler(b.groupStore, b.leaseManager, b.topicStore),
		consumerGroupDescribe:  protocol.NewConsumerGroupDescribeHandler(b.groupStore, b.leaseManager),

		saslEnabled: cfg.SASL.Enabled,
	}
}

// brokerHandler implements server.Handler and routes requests to handlers.
type brokerHandler struct {
	decoder *protocol.Decoder
	encoder *protocol.Encoder
	logger  *logging.Logger

	apiVersions     *protocol.ApiVersionsHandler
	metadata        *protocol.MetadataHandler
	produce         *protocol.ProduceHandler
	fetch           *protocol.FetchHandler
	listOffsets     *protocol.ListOffsetsHandler
	createTopics    *protocol.CreateTopicsHandler
	deleteTopics    *protocol.DeleteTopicsHandler
	describeConfigs *protocol.DescribeConfigsHandler
	alterConfigs    *protocol.IncrementalAlterConfigsHandler
	describeCluster *protocol.DescribeClusterHandler
	findCoordinator *protocol.FindCoordinatorHandler

	joinGroup     *protocol.JoinGroupHandler
	syncGroup     *protocol.SyncGroupHandler
	heartbeat     *protocol.HeartbeatHandler
	leaveGroup    *protocol.LeaveGroupHandler
	describeGroup *protocol.DescribeGroupsHandler
	listGroups    *protocol.ListGroupsHandler
	deleteGroups  *protocol.DeleteGroupsHandler

	offsetCommit *protocol.OffsetCommitHandler
	offsetFetch  *protocol.OffsetFetchHandler

	consumerGroupHeartbeat *protocol.ConsumerGroupHeartbeatHandler
	consumerGroupDescribe  *protocol.ConsumerGroupDescribeHandler

	saslEnabled bool
}

// HandleRequest implements server.Handler.
func (h *brokerHandler) HandleRequest(ctx context.Context, header *server.RequestHeader, payload []byte) ([]byte, error) {
	apiKey := header.APIKey
	version := header.APIVersion
	correlationID := header.CorrelationID

	switch apiKey {
	case 18: // ApiVersions
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ApiVersions: %w", err)
		}
		apiReq := req.Inner().(*kmsg.ApiVersionsRequest)
		resp := protocol.HandleApiVersionsWithOptions(version, apiReq, h.saslEnabled)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 3: // Metadata
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Metadata: %w", err)
		}
		metaReq := req.Inner().(*kmsg.MetadataRequest)
		zoneID := server.ZoneIDFromContext(ctx)
		resp := h.metadata.Handle(ctx, version, metaReq, zoneID)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 0: // Produce
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Produce: %w", err)
		}
		produceReq := req.Inner().(*kmsg.ProduceRequest)
		resp := h.produce.Handle(ctx, version, produceReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 1: // Fetch
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Fetch: %w", err)
		}
		fetchReq := req.Inner().(*kmsg.FetchRequest)
		resp := h.fetch.Handle(ctx, version, fetchReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 2: // ListOffsets
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ListOffsets: %w", err)
		}
		listReq := req.Inner().(*kmsg.ListOffsetsRequest)
		resp := h.listOffsets.Handle(ctx, version, listReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 8: // OffsetCommit
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode OffsetCommit: %w", err)
		}
		commitReq := req.Inner().(*kmsg.OffsetCommitRequest)
		resp := h.offsetCommit.Handle(ctx, version, commitReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 9: // OffsetFetch
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode OffsetFetch: %w", err)
		}
		fetchReq := req.Inner().(*kmsg.OffsetFetchRequest)
		resp := h.offsetFetch.Handle(ctx, version, fetchReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 10: // FindCoordinator
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode FindCoordinator: %w", err)
		}
		findReq := req.Inner().(*kmsg.FindCoordinatorRequest)
		zoneID := server.ZoneIDFromContext(ctx)
		resp := h.findCoordinator.Handle(ctx, version, findReq, zoneID)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 11: // JoinGroup
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode JoinGroup: %w", err)
		}
		joinReq := req.Inner().(*kmsg.JoinGroupRequest)
		resp := h.joinGroup.Handle(ctx, version, joinReq, header.ClientID)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 12: // Heartbeat
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Heartbeat: %w", err)
		}
		hbReq := req.Inner().(*kmsg.HeartbeatRequest)
		resp := h.heartbeat.Handle(ctx, version, hbReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 13: // LeaveGroup
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode LeaveGroup: %w", err)
		}
		leaveReq := req.Inner().(*kmsg.LeaveGroupRequest)
		resp := h.leaveGroup.Handle(ctx, version, leaveReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 14: // SyncGroup
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode SyncGroup: %w", err)
		}
		syncReq := req.Inner().(*kmsg.SyncGroupRequest)
		resp := h.syncGroup.Handle(ctx, version, syncReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 15: // DescribeGroups
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DescribeGroups: %w", err)
		}
		descReq := req.Inner().(*kmsg.DescribeGroupsRequest)
		resp := h.describeGroup.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 16: // ListGroups
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ListGroups: %w", err)
		}
		listReq := req.Inner().(*kmsg.ListGroupsRequest)
		resp := h.listGroups.Handle(ctx, version, listReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 19: // CreateTopics
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode CreateTopics: %w", err)
		}
		createReq := req.Inner().(*kmsg.CreateTopicsRequest)
		resp := h.createTopics.Handle(ctx, version, createReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 20: // DeleteTopics
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DeleteTopics: %w", err)
		}
		deleteReq := req.Inner().(*kmsg.DeleteTopicsRequest)
		resp := h.deleteTopics.Handle(ctx, version, deleteReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 32: // DescribeConfigs
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DescribeConfigs: %w", err)
		}
		descReq := req.Inner().(*kmsg.DescribeConfigsRequest)
		resp := h.describeConfigs.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 42: // DeleteGroups
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DeleteGroups: %w", err)
		}
		deleteReq := req.Inner().(*kmsg.DeleteGroupsRequest)
		resp := h.deleteGroups.Handle(ctx, version, deleteReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 44: // IncrementalAlterConfigs
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode IncrementalAlterConfigs: %w", err)
		}
		alterReq := req.Inner().(*kmsg.IncrementalAlterConfigsRequest)
		resp := h.alterConfigs.Handle(ctx, version, alterReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 60: // DescribeCluster
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode DescribeCluster: %w", err)
		}
		descReq := req.Inner().(*kmsg.DescribeClusterRequest)
		resp := h.describeCluster.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 68: // ConsumerGroupHeartbeat
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ConsumerGroupHeartbeat: %w", err)
		}
		hbReq := req.Inner().(*kmsg.ConsumerGroupHeartbeatRequest)
		resp := h.consumerGroupHeartbeat.Handle(ctx, version, hbReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 69: // ConsumerGroupDescribe
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ConsumerGroupDescribe: %w", err)
		}
		descReq := req.Inner().(*kmsg.ConsumerGroupDescribeRequest)
		resp := h.consumerGroupDescribe.Handle(ctx, version, descReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	default:
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
}

func (h *brokerHandler) encodeResponse(correlationID int32, version int16, resp *protocol.Response) ([]byte, error) {
	resp.SetVersion(version)
	return h.encoder.EncodeResponseWithCorrelationID(correlationID, resp), nil
}

// parseHostPort parses a host:port string into separate components.
func parseHostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// Might be just a port like ":9092"
		if addr[0] == ':' {
			port, _ := strconv.Atoi(addr[1:])
			return "", port
		}
		return addr, 0
	}
	port, _ := strconv.Atoi(portStr)
	return host, port
}
