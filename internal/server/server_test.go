package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/protocol"
)

// echoHandler returns requests with correlation ID prepended
type echoHandler struct {
	mu       sync.Mutex
	requests []*RequestHeader
}

func (h *echoHandler) HandleRequest(ctx context.Context, header *RequestHeader, payload []byte) ([]byte, error) {
	h.mu.Lock()
	h.requests = append(h.requests, header)
	h.mu.Unlock()

	// Return correlation ID + payload
	resp := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(resp[:4], uint32(header.CorrelationID))
	copy(resp[4:], payload)
	return resp, nil
}

func (h *echoHandler) getRequests() []*RequestHeader {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*RequestHeader{}, h.requests...)
}

// errorHandler returns an error on HandleRequest
type errorHandler struct{}

func (h *errorHandler) HandleRequest(ctx context.Context, header *RequestHeader, payload []byte) ([]byte, error) {
	return nil, errors.New("handler error")
}

type blockingHandler struct {
	started chan struct{}
	unblock chan struct{}
}

func (h *blockingHandler) HandleRequest(ctx context.Context, header *RequestHeader, payload []byte) ([]byte, error) {
	select {
	case <-h.started:
	default:
		close(h.started)
	}
	<-h.unblock

	resp := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(resp[:4], uint32(header.CorrelationID))
	copy(resp[4:], payload)
	return resp, nil
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ListenAddr != ":9092" {
		t.Errorf("expected :9092, got %s", cfg.ListenAddr)
	}
	if cfg.MaxRequestSize != 100*1024*1024 {
		t.Errorf("expected 100MB max request size, got %d", cfg.MaxRequestSize)
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("expected 30s read timeout, got %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 30*time.Second {
		t.Errorf("expected 30s write timeout, got %v", cfg.WriteTimeout)
	}
}

func TestServerListenAndServe(t *testing.T) {
	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError) // Suppress logs in tests

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0" // random port

	srv := New(cfg, handler, logger)

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	// Wait for server to start
	time.Sleep(50 * time.Millisecond)

	addr := srv.Addr()
	if addr == nil {
		t.Fatal("server should have an address")
	}

	// Connect to server
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send a request
	request := buildKafkaRequest(18, 0, 12345, "test-client", []byte("hello"))
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	// Read response
	response, err := readKafkaResponse(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	// Check response
	if len(response) < 4 {
		t.Fatalf("response too short: %d", len(response))
	}
	correlationID := int32(binary.BigEndian.Uint32(response[:4]))
	if correlationID != 12345 {
		t.Errorf("expected correlation ID 12345, got %d", correlationID)
	}

	// Verify handler received request
	requests := handler.getRequests()
	if len(requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(requests))
	}
	if requests[0].APIKey != 18 {
		t.Errorf("expected API key 18, got %d", requests[0].APIKey)
	}
	if requests[0].APIVersion != 0 {
		t.Errorf("expected API version 0, got %d", requests[0].APIVersion)
	}
	if requests[0].ClientID != "test-client" {
		t.Errorf("expected client ID 'test-client', got %q", requests[0].ClientID)
	}

	// Close server
	if err := srv.Close(); err != nil {
		t.Errorf("failed to close server: %v", err)
	}

	// Wait for server to stop
	select {
	case err := <-errCh:
		if err != ErrServerClosed {
			t.Errorf("expected ErrServerClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Error("server didn't stop in time")
	}
}

func TestServerMultipleRequests(t *testing.T) {
	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := New(cfg, handler, logger)
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send multiple requests (ApiVersions v2, non-flexible header)
	for i := 0; i < 5; i++ {
		correlationID := int32(1000 + i)
		request := buildKafkaRequest(18, 2, correlationID, "client", []byte("data"))
		if _, err := conn.Write(request); err != nil {
			t.Fatalf("failed to write request %d: %v", i, err)
		}

		response, err := readKafkaResponse(conn)
		if err != nil {
			t.Fatalf("failed to read response %d: %v", i, err)
		}

		gotCorrelationID := int32(binary.BigEndian.Uint32(response[:4]))
		if gotCorrelationID != correlationID {
			t.Errorf("request %d: expected correlation ID %d, got %d", i, correlationID, gotCorrelationID)
		}
	}
}

func TestServerMultipleConnections(t *testing.T) {
	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := New(cfg, handler, logger)
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", srv.Addr().String())
			if err != nil {
				errors <- err
				return
			}
			defer conn.Close()

			correlationID := int32(connID * 100)
			request := buildKafkaRequest(18, 0, correlationID, "client", nil)
			if _, err := conn.Write(request); err != nil {
				errors <- err
				return
			}

			response, err := readKafkaResponse(conn)
			if err != nil {
				errors <- err
				return
			}

			gotCorrelationID := int32(binary.BigEndian.Uint32(response[:4]))
			if gotCorrelationID != correlationID {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("connection error: %v", err)
	}
}

func TestServerHandlerError(t *testing.T) {
	handler := &errorHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := New(cfg, handler, logger)
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	request := buildKafkaRequest(18, 0, 1, "client", nil)
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	// Connection should be closed by server on handler error
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != io.EOF {
		// Either EOF or timeout is acceptable (connection closed)
		if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			t.Errorf("expected EOF or timeout, got %v", err)
		}
	}
}

func TestServerShutdownDrainsInFlight(t *testing.T) {
	handler := &blockingHandler{
		started: make(chan struct{}),
		unblock: make(chan struct{}),
	}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := New(cfg, handler, logger)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	request := buildKafkaRequest(18, 0, 4242, "client", []byte("data"))
	go func() {
		_, _ = conn.Write(request)
	}()

	<-handler.started

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- srv.Shutdown(context.Background())
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown completed early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(handler.unblock)

	response, err := readKafkaResponse(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	gotCorrelationID := int32(binary.BigEndian.Uint32(response[:4]))
	if gotCorrelationID != 4242 {
		t.Errorf("expected correlation ID 4242, got %d", gotCorrelationID)
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not complete")
	}

	select {
	case err := <-errCh:
		if err != ErrServerClosed {
			t.Fatalf("expected ErrServerClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

func TestServerClose(t *testing.T) {
	handler := &echoHandler{}
	logger := logging.DefaultLogger()
	logger.SetLevel(logging.LevelError)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := New(cfg, handler, logger)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	// Close server
	if err := srv.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Close again should return error
	if err := srv.Close(); err != ErrServerClosed {
		t.Errorf("expected ErrServerClosed, got %v", err)
	}

	// ListenAndServe should return ErrServerClosed
	select {
	case err := <-errCh:
		if err != ErrServerClosed {
			t.Errorf("expected ErrServerClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Error("server didn't stop")
	}
}

func TestServerNilLogger(t *testing.T) {
	handler := &echoHandler{}
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := New(cfg, handler, nil) // nil logger
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	request := buildKafkaRequest(18, 0, 1, "client", nil)
	conn.Write(request)

	response, err := readKafkaResponse(conn)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(response) < 4 {
		t.Fatalf("response too short")
	}
}

func TestParseRequestHeader(t *testing.T) {
	tests := []struct {
		name           string
		buf            []byte
		wantAPIKey     int16
		wantAPIVersion int16
		wantCorrID     int32
		wantClientID   string
		wantOffset     int
		wantErr        bool
	}{
		{
			name: "basic header",
			buf: func() []byte {
				b := make([]byte, 14)
				binary.BigEndian.PutUint16(b[0:2], 18)    // apiKey (ApiVersions)
				binary.BigEndian.PutUint16(b[2:4], 2)     // apiVersion (v2, non-flexible)
				binary.BigEndian.PutUint32(b[4:8], 12345) // correlationId
				binary.BigEndian.PutUint16(b[8:10], 4)    // clientId length
				copy(b[10:14], "test")                    // clientId
				return b
			}(),
			wantAPIKey:     18,
			wantAPIVersion: 2,
			wantCorrID:     12345,
			wantClientID:   "test",
			wantOffset:     14,
		},
		{
			name: "null clientId",
			buf: func() []byte {
				b := make([]byte, 10)
				binary.BigEndian.PutUint16(b[0:2], 0) // apiKey
				binary.BigEndian.PutUint16(b[2:4], 0) // apiVersion
				binary.BigEndian.PutUint32(b[4:8], 1) // correlationId
				binary.BigEndian.PutUint16(b[8:10], 0xFFFF) // clientId = -1 (null)
				return b
			}(),
			wantAPIKey:     0,
			wantAPIVersion: 0,
			wantCorrID:     1,
			wantClientID:   "",
			wantOffset:     10,
		},
		{
			name: "empty clientId",
			buf: func() []byte {
				b := make([]byte, 10)
				binary.BigEndian.PutUint16(b[0:2], 3) // apiKey (Metadata)
				binary.BigEndian.PutUint16(b[2:4], 8) // apiVersion (v8, non-flexible, <9)
				binary.BigEndian.PutUint32(b[4:8], 99) // correlationId
				binary.BigEndian.PutUint16(b[8:10], 0) // empty string
				return b
			}(),
			wantAPIKey:     3,
			wantAPIVersion: 8,
			wantCorrID:     99,
			wantClientID:   "",
			wantOffset:     10,
		},
		{
			name:    "too short for header",
			buf:     make([]byte, 7),
			wantErr: true,
		},
		{
			name: "too short for clientId length",
			buf:  make([]byte, 8),
			wantErr: true,
		},
		{
			name: "too short for clientId data",
			buf: func() []byte {
				b := make([]byte, 12)
				binary.BigEndian.PutUint16(b[0:2], 0)
				binary.BigEndian.PutUint16(b[2:4], 0)
				binary.BigEndian.PutUint32(b[4:8], 0)
				binary.BigEndian.PutUint16(b[8:10], 10) // claims 10 bytes
				return b
			}(),
			wantErr: true,
		},
		{
			name: "invalid negative clientId length -2",
			buf: func() []byte {
				b := make([]byte, 10)
				binary.BigEndian.PutUint16(b[0:2], 0)
				binary.BigEndian.PutUint16(b[2:4], 0)
				binary.BigEndian.PutUint32(b[4:8], 0)
				binary.BigEndian.PutUint16(b[8:10], 0xFFFE) // -2, invalid
				return b
			}(),
			wantErr: true,
		},
		{
			name: "invalid negative clientId length -100",
			buf: func() []byte {
				b := make([]byte, 10)
				binary.BigEndian.PutUint16(b[0:2], 0)
				binary.BigEndian.PutUint16(b[2:4], 0)
				binary.BigEndian.PutUint32(b[4:8], 0)
				// -100 as int16 = 0xFF9C
				binary.BigEndian.PutUint16(b[8:10], 0xFF9C) // -100, invalid
				return b
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, offset, err := parseRequestHeader(tt.buf)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRequestHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if header.APIKey != tt.wantAPIKey {
				t.Errorf("APIKey = %d, want %d", header.APIKey, tt.wantAPIKey)
			}
			if header.APIVersion != tt.wantAPIVersion {
				t.Errorf("APIVersion = %d, want %d", header.APIVersion, tt.wantAPIVersion)
			}
			if header.CorrelationID != tt.wantCorrID {
				t.Errorf("CorrelationID = %d, want %d", header.CorrelationID, tt.wantCorrID)
			}
			if header.ClientID != tt.wantClientID {
				t.Errorf("ClientID = %q, want %q", header.ClientID, tt.wantClientID)
			}
			if offset != tt.wantOffset {
				t.Errorf("offset = %d, want %d", offset, tt.wantOffset)
			}
		})
	}
}

func TestReadRequest(t *testing.T) {
	srv := &Server{
		cfg: DefaultConfig(),
	}

	// Build a request with length prefix (ApiVersions v2, non-flexible)
	headerBuf := make([]byte, 14)
	binary.BigEndian.PutUint16(headerBuf[0:2], 18)    // apiKey (ApiVersions)
	binary.BigEndian.PutUint16(headerBuf[2:4], 2)     // apiVersion (v2, non-flexible)
	binary.BigEndian.PutUint32(headerBuf[4:8], 12345) // correlationId
	binary.BigEndian.PutUint16(headerBuf[8:10], 4)    // clientId length
	copy(headerBuf[10:14], "test")

	payload := []byte("hello world")
	body := append(headerBuf, payload...)

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(len(body)))
	buf.Write(body)

	header, gotPayload, err := srv.readRequest(&buf)
	if err != nil {
		t.Fatalf("readRequest failed: %v", err)
	}

	if header.APIKey != 18 {
		t.Errorf("APIKey = %d, want 18", header.APIKey)
	}
	if header.APIVersion != 2 {
		t.Errorf("APIVersion = %d, want 2", header.APIVersion)
	}
	if header.CorrelationID != 12345 {
		t.Errorf("CorrelationID = %d, want 12345", header.CorrelationID)
	}
	if header.ClientID != "test" {
		t.Errorf("ClientID = %q, want %q", header.ClientID, "test")
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("payload = %q, want %q", gotPayload, payload)
	}
}

func TestReadRequestInvalidSize(t *testing.T) {
	srv := &Server{
		cfg: Config{
			MaxRequestSize: 1000,
		},
	}

	// Try to send a request larger than max
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(2000)) // claims 2000 bytes

	_, _, err := srv.readRequest(&buf)
	if err == nil {
		t.Error("expected error for oversized request")
	}
}

func TestWriteResponse(t *testing.T) {
	srv := &Server{}

	var buf bytes.Buffer
	response := []byte("hello world")

	err := srv.writeResponse(&buf, response)
	if err != nil {
		t.Fatalf("writeResponse failed: %v", err)
	}

	// Read length prefix
	var length int32
	binary.Read(&buf, binary.BigEndian, &length)
	if length != int32(len(response)) {
		t.Errorf("length = %d, want %d", length, len(response))
	}

	// Read response body
	body := make([]byte, length)
	buf.Read(body)
	if !bytes.Equal(body, response) {
		t.Errorf("body = %q, want %q", body, response)
	}
}

func TestEncodeResponseHeader(t *testing.T) {
	header := EncodeResponseHeader(42)
	if len(header) != 4 {
		t.Fatalf("header length = %d, want 4", len(header))
	}
	correlationID := int32(binary.BigEndian.Uint32(header))
	if correlationID != 42 {
		t.Errorf("correlationID = %d, want 42", correlationID)
	}
}

func TestAPIKey(t *testing.T) {
	// Just verify it doesn't panic and returns something
	name := APIKey(0)
	if name == "" {
		t.Error("expected non-empty API name")
	}
	name = APIKey(18) // ApiVersions
	if name == "" {
		t.Error("expected non-empty API name for ApiVersions")
	}
}

func TestParseZoneID(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		want     string
	}{
		{
			name:     "simple zone_id",
			clientID: "zone_id=us-east-1a",
			want:     "us-east-1a",
		},
		{
			name:     "zone_id with other fields",
			clientID: "app=myservice,zone_id=us-west-2b,version=1.0",
			want:     "us-west-2b",
		},
		{
			name:     "zone_id first",
			clientID: "zone_id=eu-west-1a,app=test",
			want:     "eu-west-1a",
		},
		{
			name:     "no zone_id",
			clientID: "app=myservice,version=1.0",
			want:     "",
		},
		{
			name:     "empty client_id",
			clientID: "",
			want:     "",
		},
		{
			name:     "zone_id with spaces (trimmed)",
			clientID: " zone_id=us-east-1c , app=test",
			want:     "us-east-1c",
		},
		{
			name:     "plain client_id without k=v format",
			clientID: "my-application",
			want:     "",
		},
		{
			name:     "zone_id empty value",
			clientID: "zone_id=",
			want:     "",
		},
		{
			name:     "zone_id with dashes and numbers",
			clientID: "zone_id=ap-southeast-2a,foo=bar",
			want:     "ap-southeast-2a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseZoneID(tt.clientID)
			if got != tt.want {
				t.Errorf("parseZoneID(%q) = %q, want %q", tt.clientID, got, tt.want)
			}
		})
	}
}

func TestParseRequestHeader_ZoneID(t *testing.T) {
	tests := []struct {
		name       string
		clientID   string
		wantZoneID string
	}{
		{
			name:       "zone_id extracted from clientId",
			clientID:   "zone_id=us-east-1a,app=test",
			wantZoneID: "us-east-1a",
		},
		{
			name:       "no zone_id in clientId",
			clientID:   "my-app-client",
			wantZoneID: "",
		},
		{
			name:       "empty clientId",
			clientID:   "",
			wantZoneID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headerLen := 10 + len(tt.clientID)
			buf := make([]byte, headerLen)
			binary.BigEndian.PutUint16(buf[0:2], 18)    // apiKey
			binary.BigEndian.PutUint16(buf[2:4], 2)     // apiVersion
			binary.BigEndian.PutUint32(buf[4:8], 12345) // correlationId
			binary.BigEndian.PutUint16(buf[8:10], uint16(len(tt.clientID)))
			copy(buf[10:], tt.clientID)

			header, _, err := parseRequestHeader(buf)
			if err != nil {
				t.Fatalf("parseRequestHeader failed: %v", err)
			}

			if header.ClientID != tt.clientID {
				t.Errorf("ClientID = %q, want %q", header.ClientID, tt.clientID)
			}
			if header.ZoneID != tt.wantZoneID {
				t.Errorf("ZoneID = %q, want %q", header.ZoneID, tt.wantZoneID)
			}
		})
	}
}

func TestZoneIDContext(t *testing.T) {
	ctx := context.Background()

	// Test empty context
	if got := protocol.ZoneIDFromContext(ctx); got != "" {
		t.Errorf("ZoneIDFromContext on empty context = %q, want empty", got)
	}

	// Test with zone set
	ctx = protocol.WithZoneID(ctx, "us-east-1a")
	if got := protocol.ZoneIDFromContext(ctx); got != "us-east-1a" {
		t.Errorf("ZoneIDFromContext = %q, want %q", got, "us-east-1a")
	}

	// Test overwriting zone
	ctx = protocol.WithZoneID(ctx, "eu-west-1b")
	if got := protocol.ZoneIDFromContext(ctx); got != "eu-west-1b" {
		t.Errorf("ZoneIDFromContext after overwrite = %q, want %q", got, "eu-west-1b")
	}
}

// Helper functions

func buildKafkaRequest(apiKey, apiVersion int16, correlationID int32, clientID string, payload []byte) []byte {
	// Header: apiKey (2) + apiVersion (2) + correlationId (4) + clientId (2 + len)
	headerLen := 2 + 2 + 4 + 2 + len(clientID)
	totalLen := headerLen + len(payload)

	buf := make([]byte, 4+totalLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))
	binary.BigEndian.PutUint16(buf[4:6], uint16(apiKey))
	binary.BigEndian.PutUint16(buf[6:8], uint16(apiVersion))
	binary.BigEndian.PutUint32(buf[8:12], uint32(correlationID))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:14+len(clientID)], clientID)
	copy(buf[14+len(clientID):], payload)
	return buf
}

func readKafkaResponse(r io.Reader) ([]byte, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf[:])

	response := make([]byte, length)
	if _, err := io.ReadFull(r, response); err != nil {
		return nil, err
	}
	return response, nil
}
