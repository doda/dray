package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func generateTestCertificate(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Dray Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPath = filepath.Join(dir, "cert.pem")
	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("failed to create cert file: %v", err)
	}
	pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certFile.Close()

	keyPath = filepath.Join(dir, "key.pem")
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("failed to create key file: %v", err)
	}
	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}
	pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
	keyFile.Close()

	return certPath, keyPath
}

// tlsTestBroker is similar to testBroker but with TLS support.
type tlsTestBroker struct {
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
	certPath      string
	keyPath       string
}

func newTLSTestBroker(t *testing.T, certPath, keyPath string) *tlsTestBroker {
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

	return &tlsTestBroker{
		metaStore:     metaStore,
		topicStore:    topicStore,
		streamManager: streamManager,
		objStore:      objStore,
		buffer:        buffer,
		committer:     committer,
		t:             t,
		certPath:      certPath,
		keyPath:       keyPath,
	}
}

func (b *tlsTestBroker) start() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.t.Fatalf("failed to create listener: %v", err)
	}
	ln.Close()

	tcpAddr := ln.Addr().(*net.TCPAddr)
	b.port = int32(tcpAddr.Port)
	b.addr = fmt.Sprintf("127.0.0.1:%d", b.port)

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
	)

	fetcher := fetch.NewFetcher(b.objStore, b.streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 10 * 1024 * 1024},
		b.topicStore,
		fetcher,
		b.streamManager,
	)

	listOffsetsHandler := protocol.NewListOffsetsHandler(b.topicStore, b.streamManager)

	brokerHandler := NewBrokerHandler(metadataHandler, produceHandler, fetchHandler, listOffsetsHandler)

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	b.server = server.New(server.Config{
		ListenAddr:     b.addr,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxRequestSize: 10 * 1024 * 1024,
		TLS: server.TLSConfig{
			Enabled:  true,
			CertFile: b.certPath,
			KeyFile:  b.keyPath,
		},
	}, brokerHandler, logger)

	go func() {
		b.server.ListenAndServe()
	}()

	time.Sleep(100 * time.Millisecond)
}

func (b *tlsTestBroker) stop() {
	b.buffer.Close()
	b.server.Close()
}

func (b *tlsTestBroker) createTopic(ctx context.Context, name string, partitions int32) error {
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

func TestTLS_KafkaClientApiVersions(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCertificate(t, dir)

	broker := newTLSTestBroker(t, certPath, keyPath)
	broker.start()
	defer broker.stop()

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DialTLSConfig(tlsConfig),
	)
	if err != nil {
		t.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Ping(ctx)
	if err != nil {
		t.Fatalf("Kafka client ping failed: %v", err)
	}

	t.Log("Successfully connected to TLS-enabled broker and performed API version negotiation")
}

func TestTLS_KafkaClientProduceFetch(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCertificate(t, dir)

	broker := newTLSTestBroker(t, certPath, keyPath)
	broker.start()
	defer broker.stop()

	ctx := context.Background()
	topicName := "test-tls-topic"

	if err := broker.createTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DialTLSConfig(tlsConfig),
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	testMessages := []string{
		"tls-message-0",
		"tls-message-1",
		"tls-message-2",
	}

	for i, msg := range testMessages {
		record := &kgo.Record{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		}
		result := client.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed for message %d: %v", i, result.FirstErr())
		}
		t.Logf("Produced message %d over TLS", i)
	}

	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DialTLSConfig(tlsConfig),
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

	var fetchedMessages []string
	for len(fetchedMessages) < len(testMessages) {
		fetches := consumerClient.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				t.Logf("Fetch error: %v", err.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedMessages = append(fetchedMessages, string(r.Value))
			t.Logf("Fetched message over TLS: %s", string(r.Value))
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if len(fetchedMessages) != len(testMessages) {
		t.Fatalf("expected %d messages, got %d", len(testMessages), len(fetchedMessages))
	}

	for i, expected := range testMessages {
		if fetchedMessages[i] != expected {
			t.Errorf("message %d: expected %q, got %q", i, expected, fetchedMessages[i])
		}
	}

	t.Log("Successfully produced and fetched messages over TLS")
}

func TestTLS_CertificateReloadDuringConnections(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCertificate(t, dir)

	broker := newTLSTestBroker(t, certPath, keyPath)
	broker.start()
	defer broker.stop()

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	client1, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DialTLSConfig(tlsConfig),
	)
	if err != nil {
		t.Fatalf("failed to create first client: %v", err)
	}
	defer client1.Close()

	ctx := context.Background()
	if err := client1.Ping(ctx); err != nil {
		t.Fatalf("first client ping failed: %v", err)
	}
	t.Log("First client connected successfully")

	// Regenerate the certificate
	generateTestCertificate(t, dir)

	// Reload the certificate
	if err := broker.server.ReloadCertificate(); err != nil {
		t.Fatalf("certificate reload failed: %v", err)
	}
	t.Log("Certificate reloaded successfully")

	// New connections should use the new certificate
	client2, err := kgo.NewClient(
		kgo.SeedBrokers(broker.addr),
		kgo.DialTLSConfig(tlsConfig),
	)
	if err != nil {
		t.Fatalf("failed to create second client: %v", err)
	}
	defer client2.Close()

	if err := client2.Ping(ctx); err != nil {
		t.Fatalf("second client ping failed after cert reload: %v", err)
	}
	t.Log("Second client connected with new certificate")

	// First client should still work (existing connection)
	if err := client1.Ping(ctx); err != nil {
		t.Fatalf("first client ping failed after cert reload: %v", err)
	}
	t.Log("First client still works after certificate reload")

	t.Log("Certificate hot reload test passed")
}
