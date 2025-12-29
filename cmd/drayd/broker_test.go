package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBrokerStartAndShutdown(t *testing.T) {
	// Create minimal config
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0" // Random port
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError) // Suppress logs in tests

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
		Version:   "test",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	// Start broker in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- broker.Start(ctx)
	}()

	// Wait for broker to start
	time.Sleep(200 * time.Millisecond)

	// Verify broker is listening
	if broker.tcpServer == nil || broker.tcpServer.Addr() == nil {
		t.Fatal("broker TCP server not running")
	}
	addr := broker.tcpServer.Addr().String()
	t.Logf("Broker listening on %s", addr)

	// Try to connect
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect to broker: %v", err)
	}
	conn.Close()

	// Verify health server
	if broker.healthServer == nil {
		t.Fatal("health server not running")
	}

	// Shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestBrokerRegistry(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker-123",
		NodeID:    42,
		ClusterID: "test-cluster",
		Version:   "1.0.0",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Verify broker is registered
	if broker.registry == nil {
		t.Fatal("broker registry not initialized")
	}

	if !broker.registry.IsRegistered() {
		t.Error("broker should be registered")
	}

	info := broker.registry.BrokerInfo()
	if info.BrokerID != "test-broker-123" {
		t.Errorf("expected broker ID 'test-broker-123', got %q", info.BrokerID)
	}
	if info.NodeID != 42 {
		t.Errorf("expected node ID 42, got %d", info.NodeID)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	broker.Shutdown(shutdownCtx)

	// After shutdown, broker should be deregistered
	if broker.registry.IsRegistered() {
		t.Error("broker should be deregistered after shutdown")
	}
}

func TestBrokerApiVersions(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	addr := broker.tcpServer.Addr().String()

	// Connect with franz-go client and verify ApiVersions
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Ping should succeed if ApiVersions works
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pingCancel()

	if err := client.Ping(pingCtx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	t.Log("Successfully connected to broker and performed API version negotiation")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	broker.Shutdown(shutdownCtx)
}

func TestBrokerTransactionAPIRejection(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	addr := broker.tcpServer.Addr().String()

	// Helper to send a raw Kafka request and read response
	sendRequest := func(conn net.Conn, apiKey, version int16, correlationID int32, reqBody []byte) ([]byte, error) {
		// Build request header
		header := make([]byte, 8)
		binary.BigEndian.PutUint16(header[0:2], uint16(apiKey))
		binary.BigEndian.PutUint16(header[2:4], uint16(version))
		binary.BigEndian.PutUint32(header[4:8], uint32(correlationID))

		// ClientId (int16 length -1 for null)
		clientID := []byte{0xff, 0xff} // null clientId

		// Build full request
		fullReq := append(header, clientID...)
		fullReq = append(fullReq, reqBody...)

		// Add length prefix
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(fullReq)))
		_, err := conn.Write(lenBuf)
		if err != nil {
			return nil, err
		}
		_, err = conn.Write(fullReq)
		if err != nil {
			return nil, err
		}

		// Read response length
		respLenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, respLenBuf); err != nil {
			return nil, err
		}
		respLen := binary.BigEndian.Uint32(respLenBuf)

		// Read response body
		respBuf := make([]byte, respLen)
		if _, err := io.ReadFull(conn, respBuf); err != nil {
			return nil, err
		}
		return respBuf, nil
	}

	t.Run("AddPartitionsToTxn returns UNSUPPORTED_VERSION", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		// Build AddPartitionsToTxn request body (version 0)
		// TransactionalId (string), ProducerId (int64), ProducerEpoch (int16), Topics (array)
		req := kmsg.NewPtrAddPartitionsToTxnRequest()
		req.SetVersion(0)
		req.TransactionalID = "test-txn"
		req.ProducerID = 1
		req.ProducerEpoch = 0
		topic := kmsg.NewAddPartitionsToTxnRequestTopic()
		topic.Topic = "test-topic"
		topic.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic)

		reqBody := req.AppendTo(nil)

		respBuf, err := sendRequest(conn, 24, 0, 1, reqBody)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		// Parse response: skip correlation ID (4 bytes), then throttle (4 bytes), then topics
		if len(respBuf) < 8 {
			t.Fatalf("response too short: %d bytes", len(respBuf))
		}

		// Parse the response
		resp := kmsg.NewPtrAddPartitionsToTxnResponse()
		resp.SetVersion(0)
		err = resp.ReadFrom(respBuf[4:]) // Skip correlation ID
		if err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Verify error code is UNSUPPORTED_VERSION (35)
		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic in response, got %d", len(resp.Topics))
		}
		if len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("expected 1 partition in response, got %d", len(resp.Topics[0].Partitions))
		}
		if resp.Topics[0].Partitions[0].ErrorCode != 35 { // UNSUPPORTED_VERSION
			t.Errorf("expected error code 35 (UNSUPPORTED_VERSION), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("AddOffsetsToTxn returns UNSUPPORTED_VERSION", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		req := kmsg.NewPtrAddOffsetsToTxnRequest()
		req.SetVersion(0)
		req.TransactionalID = "test-txn"
		req.ProducerID = 1
		req.ProducerEpoch = 0
		req.Group = "test-group"

		reqBody := req.AppendTo(nil)

		respBuf, err := sendRequest(conn, 25, 0, 2, reqBody)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		resp := kmsg.NewPtrAddOffsetsToTxnResponse()
		resp.SetVersion(0)
		err = resp.ReadFrom(respBuf[4:]) // Skip correlation ID
		if err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		if resp.ErrorCode != 35 { // UNSUPPORTED_VERSION
			t.Errorf("expected error code 35 (UNSUPPORTED_VERSION), got %d", resp.ErrorCode)
		}
	})

	t.Run("EndTxn returns UNSUPPORTED_VERSION", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		req := kmsg.NewPtrEndTxnRequest()
		req.SetVersion(0)
		req.TransactionalID = "test-txn"
		req.ProducerID = 1
		req.ProducerEpoch = 0
		req.Commit = true

		reqBody := req.AppendTo(nil)

		respBuf, err := sendRequest(conn, 26, 0, 3, reqBody)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		resp := kmsg.NewPtrEndTxnResponse()
		resp.SetVersion(0)
		err = resp.ReadFrom(respBuf[4:]) // Skip correlation ID
		if err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		if resp.ErrorCode != 35 { // UNSUPPORTED_VERSION
			t.Errorf("expected error code 35 (UNSUPPORTED_VERSION), got %d", resp.ErrorCode)
		}
	})

	t.Run("TxnOffsetCommit returns UNSUPPORTED_VERSION", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		req := kmsg.NewPtrTxnOffsetCommitRequest()
		req.SetVersion(0)
		req.TransactionalID = "test-txn"
		req.Group = "test-group"
		req.ProducerID = 1
		req.ProducerEpoch = 0
		topic := kmsg.NewTxnOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		part := kmsg.NewTxnOffsetCommitRequestTopicPartition()
		part.Partition = 0
		part.Offset = 100
		topic.Partitions = append(topic.Partitions, part)
		req.Topics = append(req.Topics, topic)

		reqBody := req.AppendTo(nil)

		respBuf, err := sendRequest(conn, 28, 0, 4, reqBody)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		resp := kmsg.NewPtrTxnOffsetCommitResponse()
		resp.SetVersion(0)
		err = resp.ReadFrom(respBuf[4:]) // Skip correlation ID
		if err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Verify per-partition error code
		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic in response, got %d", len(resp.Topics))
		}
		if len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("expected 1 partition in response, got %d", len(resp.Topics[0].Partitions))
		}
		if resp.Topics[0].Partitions[0].ErrorCode != 35 { // UNSUPPORTED_VERSION
			t.Errorf("expected error code 35 (UNSUPPORTED_VERSION), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	broker.Shutdown(shutdownCtx)
}

func TestBrokerInterBrokerAPIRejection(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	addr := broker.tcpServer.Addr().String()

	// Helper to send a raw Kafka request and read response
	sendRequest := func(conn net.Conn, apiKey, version int16, correlationID int32, reqBody []byte) ([]byte, error) {
		// Build request header
		header := make([]byte, 8)
		binary.BigEndian.PutUint16(header[0:2], uint16(apiKey))
		binary.BigEndian.PutUint16(header[2:4], uint16(version))
		binary.BigEndian.PutUint32(header[4:8], uint32(correlationID))

		// ClientId (int16 length -1 for null)
		clientID := []byte{0xff, 0xff} // null clientId

		// Build full request
		fullReq := append(header, clientID...)
		fullReq = append(fullReq, reqBody...)

		// Add length prefix
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(fullReq)))
		_, err := conn.Write(lenBuf)
		if err != nil {
			return nil, err
		}
		_, err = conn.Write(fullReq)
		if err != nil {
			return nil, err
		}

		// Read response length
		respLenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, respLenBuf); err != nil {
			return nil, err
		}
		respLen := binary.BigEndian.Uint32(respLenBuf)

		// Read response body
		respBuf := make([]byte, respLen)
		if _, err := io.ReadFull(conn, respBuf); err != nil {
			return nil, err
		}
		return respBuf, nil
	}

	t.Run("LeaderAndISR returns UNSUPPORTED_VERSION", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		req := kmsg.NewPtrLeaderAndISRRequest()
		req.SetVersion(5)
		req.ControllerID = 1
		req.BrokerEpoch = 100

		reqBody := req.AppendTo(nil)

		respBuf, err := sendRequest(conn, 4, 5, 1, reqBody)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		// Parse the response
		// v5 is flexible, so skip correlation ID (4 bytes) + tag buffer (1 byte)
		resp := kmsg.NewPtrLeaderAndISRResponse()
		resp.SetVersion(5)
		if err := resp.ReadFrom(respBuf[5:]); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Verify error code is UNSUPPORTED_VERSION (35)
		if resp.ErrorCode != 35 {
			t.Errorf("expected error code 35 (UNSUPPORTED_VERSION), got %d", resp.ErrorCode)
		}
	})

	t.Run("StopReplica returns UNSUPPORTED_VERSION", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		req := kmsg.NewPtrStopReplicaRequest()
		req.SetVersion(3)
		req.ControllerID = 1
		req.BrokerEpoch = 100

		reqBody := req.AppendTo(nil)

		respBuf, err := sendRequest(conn, 5, 3, 2, reqBody)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		// v3 is flexible, so skip correlation ID (4 bytes) + tag buffer (1 byte)
		resp := kmsg.NewPtrStopReplicaResponse()
		resp.SetVersion(3)
		if err := resp.ReadFrom(respBuf[5:]); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		if resp.ErrorCode != 35 {
			t.Errorf("expected error code 35 (UNSUPPORTED_VERSION), got %d", resp.ErrorCode)
		}
	})

	t.Run("UpdateMetadata returns UNSUPPORTED_VERSION", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		req := kmsg.NewPtrUpdateMetadataRequest()
		req.SetVersion(5)
		req.ControllerID = 1
		req.ControllerEpoch = 10

		reqBody := req.AppendTo(nil)

		respBuf, err := sendRequest(conn, 6, 5, 3, reqBody)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}

		resp := kmsg.NewPtrUpdateMetadataResponse()
		resp.SetVersion(5)
		if err := resp.ReadFrom(respBuf[4:]); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		if resp.ErrorCode != 35 {
			t.Errorf("expected error code 35 (UNSUPPORTED_VERSION), got %d", resp.ErrorCode)
		}
	})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	broker.Shutdown(shutdownCtx)
}

func TestBrokerGracefulShutdown(t *testing.T) {
	cfg := config.Default()
	cfg.Broker.ListenAddr = "127.0.0.1:0"
	cfg.Observability.MetricsAddr = "127.0.0.1:0"

	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	opts := BrokerOptions{
		Config:    cfg,
		Logger:    logger,
		BrokerID:  "test-broker",
		NodeID:    1,
		ClusterID: "test-cluster",
	}

	broker, err := NewBroker(opts)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go broker.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	addr := broker.tcpServer.Addr().String()

	// Create a client connection
	client, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// Verify connection works
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	if err := client.Ping(pingCtx); err != nil {
		pingCancel()
		client.Close()
		t.Fatalf("ping failed: %v", err)
	}
	pingCancel()

	// Trigger shutdown
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Shutdown should complete gracefully
	if err := broker.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Health check should fail after shutdown
	status := broker.healthServer.CheckHealth()
	if status.Status != "shutting_down" {
		t.Errorf("expected status 'shutting_down', got %q", status.Status)
	}

	client.Close()
}
