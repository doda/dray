package integration

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type saslBrokerHandler struct {
	decoder       *protocol.Decoder
	encoder       *protocol.Encoder
	apiVersions   *protocol.ApiVersionsHandler
	metadata      *protocol.MetadataHandler
	produce       *protocol.ProduceHandler
	fetch         *protocol.FetchHandler
	listOffsets   *protocol.ListOffsetsHandler
	authenticator *auth.SASLAuthenticator
}

func newSASLBrokerHandler(
	metadata *protocol.MetadataHandler,
	produce *protocol.ProduceHandler,
	fetch *protocol.FetchHandler,
	listOffsets *protocol.ListOffsetsHandler,
	authenticator *auth.SASLAuthenticator,
) *saslBrokerHandler {
	return &saslBrokerHandler{
		decoder:       protocol.NewDecoder(),
		encoder:       protocol.NewEncoder(),
		apiVersions:   protocol.NewApiVersionsHandler(),
		metadata:      metadata,
		produce:       produce,
		fetch:         fetch,
		listOffsets:   listOffsets,
		authenticator: authenticator,
	}
}

func (h *saslBrokerHandler) HandleRequest(ctx context.Context, header *server.RequestHeader, payload []byte) ([]byte, error) {
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
		resp := protocol.HandleApiVersionsWithOptions(version, apiReq, true)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 17: // SASLHandshake
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode SASLHandshake: %w", err)
		}
		hsReq := req.Inner().(*kmsg.SASLHandshakeRequest)
		resp := h.authenticator.HandleSASLHandshake(version, hsReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 36: // SASLAuthenticate
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode SASLAuthenticate: %w", err)
		}
		authReq := req.Inner().(*kmsg.SASLAuthenticateRequest)
		username, _, parseErr := parsePlainAuthBytes(authReq.SASLAuthBytes)
		resp := h.authenticator.HandleSASLAuthenticate(version, authReq, auth.MechanismPLAIN)
		if parseErr == nil && resp.ErrorCode == auth.ErrorCodeNone {
			server.SetAuthenticated(ctx, username)
		}
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 3: // Metadata
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Metadata request: %w", err)
		}
		metaReq := req.Inner().(*kmsg.MetadataRequest)
		zoneID := protocol.ParseZoneID(header.ClientID)
		resp := h.metadata.Handle(ctx, version, metaReq, zoneID)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 0: // Produce
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Produce request: %w", err)
		}
		produceReq := req.Inner().(*kmsg.ProduceRequest)
		resp := h.produce.Handle(ctx, version, produceReq)
		return h.encodeResponse(correlationID, version, protocol.WrapResponse(resp))

	case 1: // Fetch
		req, err := h.decoder.DecodeRequest(apiKey, version, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Fetch request: %w", err)
		}
		fetchReq := req.Inner().(*kmsg.FetchRequest)
		resp := h.fetch.Handle(ctx, version, fetchReq)
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

func (h *saslBrokerHandler) encodeResponse(correlationID int32, version int16, resp *protocol.Response) ([]byte, error) {
	resp.SetVersion(version)
	return h.encoder.EncodeResponseWithCorrelationID(correlationID, resp), nil
}

type saslACLTestBroker struct {
	metaStore     *metadata.MockStore
	topicStore    *topics.Store
	streamManager *index.StreamManager
	objStore      *mockObjectStore
	buffer        *produce.Buffer
	committer     *produce.Committer
	server        *server.Server
	addr          string
	port          int32
	t             *testing.T

	aclStore      *auth.ACLStore
	aclCache      *auth.ACLCache
	enforcer      *auth.Enforcer
	credentials   *auth.CredentialStore
	authenticator *auth.SASLAuthenticator
}

func newSASLACLTestBroker(t *testing.T) *saslACLTestBroker {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newMockObjectStore()

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 10 * 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})

	credentials := auth.NewCredentialStore()
	credentials.Add("alice", "alice-pass")
	credentials.Add("bob", "bob-pass")

	aclStore := auth.NewACLStore(metaStore)
	aclCache := auth.NewACLCache(aclStore, metaStore)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)
	enforcer := auth.NewEnforcer(aclCache, auth.EnforcerConfig{Enabled: true}, logger)

	return &saslACLTestBroker{
		metaStore:     metaStore,
		topicStore:    topicStore,
		streamManager: streamManager,
		objStore:      objStore,
		buffer:        buffer,
		committer:     committer,
		t:             t,
		aclStore:      aclStore,
		aclCache:      aclCache,
		enforcer:      enforcer,
		credentials:   credentials,
		authenticator: auth.NewSASLAuthenticator(credentials, auth.MechanismPLAIN),
	}
}

func (b *saslACLTestBroker) start() {
	ctx := context.Background()
	if err := b.aclCache.Start(ctx); err != nil {
		b.t.Fatalf("failed to start ACL cache: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.t.Fatalf("failed to create listener: %v", err)
	}

	tcpAddr := ln.Addr().(*net.TCPAddr)
	b.port = int32(tcpAddr.Port)
	b.addr = ln.Addr().String()

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
				Port:   b.port,
				Rack:   "",
			},
		},
		b.topicStore,
		b.streamManager,
	)

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		b.topicStore,
		b.buffer,
	).WithEnforcer(b.enforcer)

	fetcher := fetch.NewFetcher(b.objStore, b.streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 10 * 1024 * 1024},
		b.topicStore,
		fetcher,
		b.streamManager,
	).WithEnforcer(b.enforcer)

	listOffsetsHandler := protocol.NewListOffsetsHandler(b.topicStore, b.streamManager)

	brokerHandler := newSASLBrokerHandler(
		metadataHandler,
		produceHandler,
		fetchHandler,
		listOffsetsHandler,
		b.authenticator,
	)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

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

func (b *saslACLTestBroker) stop() {
	b.buffer.Close()
	b.server.Close()
	b.aclCache.Stop()
}

func (b *saslACLTestBroker) createTopic(ctx context.Context, name string, partitions int32) error {
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

func parsePlainAuthBytes(data []byte) (string, string, error) {
	if len(data) == 0 {
		return "", "", errors.New("empty auth data")
	}
	parts := bytes.Split(data, []byte{0})
	if len(parts) != 3 {
		return "", "", errors.New("invalid auth data")
	}
	username := string(parts[1])
	password := string(parts[2])
	if username == "" {
		return "", "", errors.New("empty username")
	}
	return username, password, nil
}

func TestSASLPlain_ACLEndToEnd(t *testing.T) {
	broker := newSASLACLTestBroker(t)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "sasl-acl-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := broker.aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: topicName,
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationRead,
		Permission:   auth.PermissionAllow,
	}); err != nil {
		t.Fatalf("failed to create read ACL: %v", err)
	}
	if err := broker.aclStore.CreateACL(ctx, &auth.ACLEntry{
		ResourceType: auth.ResourceTypeTopic,
		ResourceName: topicName,
		PatternType:  auth.PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    auth.OperationWrite,
		Permission:   auth.PermissionAllow,
	}); err != nil {
		t.Fatalf("failed to create write ACL: %v", err)
	}
	broker.aclCache.Invalidate(ctx)

	aliceClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.SASL(plain.Auth{User: "alice", Pass: "alice-pass"}.AsMechanism()),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create alice client: %v", err)
	}
	defer aliceClient.Close()

	bobClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.SASL(plain.Auth{User: "bob", Pass: "bob-pass"}.AsMechanism()),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create bob client: %v", err)
	}
	defer bobClient.Close()

	produceCtx, cancelProduce := context.WithTimeout(ctx, 10*time.Second)
	defer cancelProduce()

	aliceResult := aliceClient.ProduceSync(produceCtx, &kgo.Record{
		Topic: topicName,
		Key:   []byte("alice-key"),
		Value: []byte("alice-value"),
	})
	if err := aliceResult.FirstErr(); err != nil {
		t.Fatalf("alice produce failed: %v", err)
	}

	bobResult := bobClient.ProduceSync(produceCtx, &kgo.Record{
		Topic: topicName,
		Key:   []byte("bob-key"),
		Value: []byte("bob-value"),
	})
	if err := bobResult.FirstErr(); err == nil || !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("expected bob produce to fail with TopicAuthorizationFailed, got %v", err)
	}

	aliceConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.SASL(plain.Auth{User: "alice", Pass: "alice-pass"}.AsMechanism()),
		kgo.ConsumeTopics(topicName),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create alice consumer: %v", err)
	}
	defer aliceConsumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var aliceFetched []string
	for len(aliceFetched) == 0 && fetchCtx.Err() == nil {
		fetches := aliceConsumer.PollFetches(fetchCtx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("alice fetch errors: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			aliceFetched = append(aliceFetched, string(r.Value))
		})
	}
	if len(aliceFetched) == 0 {
		t.Fatal("expected alice to fetch at least one record")
	}

	denyCtx, denyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer denyCancel()

	fetchReq := kmsg.NewPtrFetchRequest()
	fetchReq.Version = 12
	fetchReq.ReplicaID = -1
	fetchReq.MaxWaitMillis = 100
	fetchReq.MinBytes = 1
	fetchReq.MaxBytes = 1024 * 1024
	fetchReq.Topics = []kmsg.FetchRequestTopic{{
		Topic: topicName,
		Partitions: []kmsg.FetchRequestTopicPartition{{
			Partition:         0,
			FetchOffset:       0,
			PartitionMaxBytes: 1024 * 1024,
		}},
	}}

	fetchResp, err := fetchReq.RequestWith(denyCtx, bobClient)
	if err != nil {
		t.Fatalf("bob fetch request failed: %v", err)
	}
	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected bob fetch response to include topic partitions")
	}
	if fetchResp.Topics[0].Partitions[0].ErrorCode != auth.ErrCodeTopicAuthorizationFailed {
		t.Fatalf("expected bob fetch to fail with TopicAuthorizationFailed, got %d", fetchResp.Topics[0].Partitions[0].ErrorCode)
	}
}
