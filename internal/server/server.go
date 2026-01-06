// Package server implements the TCP server for the Kafka wire protocol.
// It handles connection lifecycle, framing, and routing to protocol handlers.
package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/sys/unix"
)

// ErrServerClosed is returned when operations are attempted on a closed server.
var ErrServerClosed = errors.New("server closed")

// Handler processes Kafka API requests and returns responses.
type Handler interface {
	// HandleRequest processes a request and returns the response bytes.
	// The response should NOT include the length prefix - that will be added by the server.
	HandleRequest(ctx context.Context, header *RequestHeader, payload []byte) ([]byte, error)
}

// AuthHandler is an optional interface for handlers that support SASL authentication.
// When implemented, the server will call SetAuthResult after successful SASL authentication.
type AuthHandler interface {
	Handler
	// SetAuthResult is called by handlers when SASL authentication completes.
	// Returns the authenticated username if authentication succeeded.
	SetAuthResult(ctx context.Context, username string)
}

// connStateKey is the context key for storing connection state pointer.
type connStateKey struct{}

// SetAuthenticated sets the authenticated username for the current connection.
// This should be called by SASL handlers after successful authentication.
func SetAuthenticated(ctx context.Context, username string) {
	if v := ctx.Value(connStateKey{}); v != nil {
		state := v.(*connState)
		state.authenticated = true
		state.username = username
	}
}

// RequestHeader contains the parsed Kafka request header.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
	// ZoneID is extracted from ClientID per spec 7.1.
	// If ClientID contains "zone_id=<value>", ZoneID will be set.
	// Empty if not present or invalid.
	ZoneID string
}

// connState tracks per-connection authentication state.
type connState struct {
	authenticated bool
	username      string // authenticated username (empty if not authenticated)
}

// Config holds the TCP server configuration.
type Config struct {
	ListenAddr     string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxRequestSize int32
	TLS            TLSConfig
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ListenAddr:     ":9092",
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxRequestSize: 100 * 1024 * 1024, // 100MB
	}
}

// Server implements a TCP server for the Kafka wire protocol.
type Server struct {
	cfg      Config
	handler  Handler
	logger   *logging.Logger
	listener net.Listener
	metrics  *metrics.ConnectionMetrics

	mu           sync.Mutex
	conns        map[net.Conn]struct{}
	stopping     atomic.Bool
	closed       atomic.Bool
	connWg       sync.WaitGroup
	inflightWg   sync.WaitGroup
	requestMu    sync.Mutex
	connID       atomic.Int64
	certReloader *CertReloader
	bufferPool   sync.Pool // Pool for request read buffers
}

// New creates a new Server with the given configuration and handler.
func New(cfg Config, handler Handler, logger *logging.Logger) *Server {
	if logger == nil {
		logger = logging.DefaultLogger()
	}
	s := &Server{
		cfg:     cfg,
		handler: handler,
		logger:  logger,
		conns:   make(map[net.Conn]struct{}),
	}
	s.bufferPool = sync.Pool{
		New: func() any {
			// Allocate a buffer slice. We use MaxRequestSize as the capacity
			// to avoid reallocations for most requests.
			buf := make([]byte, cfg.MaxRequestSize)
			return &buf
		},
	}
	return s
}

// WithMetrics sets the connection metrics for the server.
// Returns the server for method chaining.
func (s *Server) WithMetrics(m *metrics.ConnectionMetrics) *Server {
	s.metrics = m
	return s
}

// ListenAndServe starts the server on the configured address.
// If TLS is enabled, it creates a TLS listener with certificate hot-reload support.
func (s *Server) ListenAndServe() error {
	if s.cfg.TLS.Enabled {
		ln, reloader, err := NewTLSListener(s.cfg.ListenAddr, s.cfg.TLS, s.logger)
		if err != nil {
			return fmt.Errorf("failed to create TLS listener: %w", err)
		}
		s.certReloader = reloader
		s.certReloader.StartWatcher(30 * time.Second)
		return s.Serve(ln)
	}

	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.ListenAddr, err)
	}
	return s.Serve(ln)
}

// Serve accepts connections on the given listener.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.listener = ln
	s.mu.Unlock()

	s.logger.Infof("server listening", map[string]any{"addr": ln.Addr().String()})

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.stopping.Load() || s.closed.Load() {
				return ErrServerClosed
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				s.logger.Warnf("temporary accept error", map[string]any{"error": err.Error()})
				time.Sleep(5 * time.Millisecond)
				continue
			}
			return fmt.Errorf("accept error: %w", err)
		}

		s.connWg.Add(1)
		go s.handleConn(conn)
	}
}

// Addr returns the listener's address, or nil if not listening.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close shuts down the server immediately.
func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrServerClosed
	}
	s.requestMu.Lock()
	s.stopping.Store(true)
	s.requestMu.Unlock()

	s.mu.Lock()
	if s.listener != nil {
		s.listener.Close()
	}
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()

	if s.certReloader != nil {
		s.certReloader.Stop()
	}

	s.connWg.Wait()
	return nil
}

// StopAccepting stops accepting new connections and new requests on existing connections.
func (s *Server) StopAccepting() error {
	s.requestMu.Lock()
	if s.closed.Load() {
		s.requestMu.Unlock()
		return ErrServerClosed
	}
	if s.stopping.Load() {
		s.requestMu.Unlock()
		return nil
	}
	s.stopping.Store(true)
	s.requestMu.Unlock()

	s.mu.Lock()
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Unlock()

	return nil
}

// Drain waits for in-flight requests to complete, then closes all connections.
func (s *Server) Drain(ctx context.Context) error {
	if s.closed.Load() {
		return ErrServerClosed
	}

	done := make(chan struct{})
	go func() {
		s.inflightWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	s.mu.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()

	if s.certReloader != nil {
		s.certReloader.Stop()
	}

	s.connWg.Wait()
	s.closed.Store(true)

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// Shutdown stops accepting new connections, drains in-flight requests, and closes connections.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.StopAccepting(); err != nil {
		return err
	}
	return s.Drain(ctx)
}

// ReloadCertificate manually triggers a certificate reload.
// Returns an error if TLS is not enabled or if the reload fails.
func (s *Server) ReloadCertificate() error {
	if s.certReloader == nil {
		return errors.New("TLS is not enabled")
	}
	return s.certReloader.Reload()
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.connWg.Done()

	// Create per-connection context that gets cancelled when connection closes.
	// This allows long-running handlers (e.g., long-poll fetch) to exit early.
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()
	defer conn.Close()

	connID := s.connID.Add(1)
	remoteAddr := conn.RemoteAddr().String()

	// Extract client host for ACL checks
	clientHost := auth.ExtractHostFromAddr(conn.RemoteAddr())

	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()

	// Track connection metrics
	if s.metrics != nil {
		s.metrics.ConnectionOpened()
	}

	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()

		// Track connection close
		if s.metrics != nil {
			s.metrics.ConnectionClosed()
		}
	}()

	logger := s.logger.With(map[string]any{
		"connId":     connID,
		"remoteAddr": remoteAddr,
	})
	logger.Debug("connection accepted")

	// Track connection authentication state
	state := &connState{}

	for {
		if s.stopping.Load() || s.closed.Load() {
			return
		}

		if s.cfg.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		}

		header, payload, bufPtr, err := s.readRequest(conn)
		if err != nil {
			if err == io.EOF || s.closed.Load() {
				logger.Debug("connection closed")
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Debug("read timeout")
				return
			}
			if isConnReset(err) {
				logger.Debug("connection reset by peer")
				return
			}
			logger.Warnf("read error", map[string]any{"error": err.Error()})
			return
		}
		if s.stopping.Load() || s.closed.Load() {
			s.putBuffer(bufPtr)
			return
		}

		reqLogger := logger.With(map[string]any{
			"apiKey":        header.APIKey,
			"apiVersion":    header.APIVersion,
			"correlationId": header.CorrelationID,
			"clientId":      header.ClientID,
			"zoneId":        header.ZoneID,
		})
		reqLogger.Debug("request received")

		// Use connection context as parent so handlers can detect connection close.
		ctx := connCtx
		ctx = logging.WithLoggerCtx(ctx, reqLogger)

		// Store connection state in context for SASL handlers to update
		ctx = context.WithValue(ctx, connStateKey{}, state)

		// Set principal and host in context for ACL enforcement
		// If authenticated via SASL, use the authenticated username; otherwise use ANONYMOUS
		principal := auth.MakePrincipal(state.username)
		ctx = auth.WithPrincipal(ctx, principal)
		ctx = auth.WithHost(ctx, clientHost)

		// Store zone_id in context for handlers (per spec 7.1)
		if header.ZoneID != "" {
			ctx = protocol.WithZoneID(ctx, header.ZoneID)
		}

		apiName := APIKey(header.APIKey)
		s.requestMu.Lock()
		if s.stopping.Load() || s.closed.Load() {
			s.requestMu.Unlock()
			return
		}
		s.inflightWg.Add(1)
		s.requestMu.Unlock()

		// Create per-request context that gets cancelled when connection closes.
		// This enables long-running handlers (e.g., long-poll fetch) to exit early.
		reqCtx, reqCancel := context.WithCancel(ctx)

		// Monitor connection for closure while handler is running.
		// Use poll syscall to detect connection state without reading data.
		connMonitorDone := make(chan struct{})
		go s.monitorConnection(conn, reqCtx, reqCancel, connMonitorDone)

		response, err := s.handler.HandleRequest(reqCtx, header, payload)

		// Return the pooled buffer now that handler is done with it
		s.putBuffer(bufPtr)

		// Stop monitoring and wait for goroutine to finish
		reqCancel()
		<-connMonitorDone
		if err != nil {
			reqLogger.Errorf("handler error", map[string]any{"error": err.Error()})
			if s.metrics != nil {
				s.metrics.RecordFailure(apiName)
				s.metrics.RecordError(apiName, "HANDLER_ERROR")
			}
			fallback, fallbackErr := protocol.BuildFallbackErrorResponse(
				header.APIKey,
				header.APIVersion,
				header.CorrelationID,
				payload,
			)
			if fallbackErr != nil {
				reqLogger.Warnf("failed to build fallback response", map[string]any{"error": fallbackErr.Error()})
				s.inflightWg.Done()
				return
			}
			if s.cfg.WriteTimeout > 0 {
				conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
			}
			if err := s.writeResponse(conn, fallback); err != nil {
				reqLogger.Warnf("write error", map[string]any{"error": err.Error()})
				s.inflightWg.Done()
				return
			}
			s.inflightWg.Done()
			reqLogger.Warnf("fallback response sent", map[string]any{"error": err.Error()})
			continue
		}

		// Record successful request
		if s.metrics != nil {
			s.metrics.RecordSuccess(apiName)
		}

		if s.cfg.WriteTimeout > 0 {
			conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		}

		if err := s.writeResponse(conn, response); err != nil {
			reqLogger.Warnf("write error", map[string]any{"error": err.Error()})
			s.inflightWg.Done()
			return
		}

		s.inflightWg.Done()
		reqLogger.Debug("response sent")
	}
}

func isConnReset(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	return strings.Contains(err.Error(), "connection reset by peer")
}

// readRequest reads a Kafka request from the connection.
// Returns the parsed header, payload (body after header), and the pooled buffer.
// The caller MUST return the buffer to the pool via putBuffer() after processing.
func (s *Server) readRequest(r io.Reader) (*RequestHeader, []byte, *[]byte, error) {
	// Read 4-byte length prefix (big-endian)
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, nil, nil, err
	}
	length := int32(binary.BigEndian.Uint32(lengthBuf[:]))

	if length < 0 || length > s.cfg.MaxRequestSize {
		return nil, nil, nil, fmt.Errorf("invalid request size: %d", length)
	}

	// Get a buffer from the pool
	bufPtr := s.getBuffer()
	buf := *bufPtr

	// Use the buffer up to the request length
	requestBuf := buf[:length]
	if _, err := io.ReadFull(r, requestBuf); err != nil {
		s.putBuffer(bufPtr)
		return nil, nil, nil, fmt.Errorf("failed to read request body: %w", err)
	}

	// Parse the request header
	header, headerLen, err := parseRequestHeader(requestBuf)
	if err != nil {
		s.putBuffer(bufPtr)
		return nil, nil, nil, fmt.Errorf("failed to parse request header: %w", err)
	}

	// Payload is everything after the header
	payload := requestBuf[headerLen:]
	return header, payload, bufPtr, nil
}

// getBuffer retrieves a buffer from the pool.
func (s *Server) getBuffer() *[]byte {
	return s.bufferPool.Get().(*[]byte)
}

// putBuffer returns a buffer to the pool after clearing it.
func (s *Server) putBuffer(bufPtr *[]byte) {
	if bufPtr == nil {
		return
	}
	// Clear the buffer to avoid leaking data between requests
	buf := *bufPtr
	clear(buf)
	s.bufferPool.Put(bufPtr)
}

// parseRequestHeader parses the Kafka request header from the buffer.
// Returns the header and the number of bytes consumed.
// Handles both flexible (v2 header) and non-flexible (v1 header) formats.
func parseRequestHeader(buf []byte) (*RequestHeader, int, error) {
	if len(buf) < 8 {
		return nil, 0, errors.New("request too short for header")
	}

	header := &RequestHeader{
		APIKey:        int16(binary.BigEndian.Uint16(buf[0:2])),
		APIVersion:    int16(binary.BigEndian.Uint16(buf[2:4])),
		CorrelationID: int32(binary.BigEndian.Uint32(buf[4:8])),
	}

	offset := 8

	// Determine if this request uses flexible encoding (request header v2)
	flexible := protocol.IsFlexibleRequestHeader(header.APIKey, header.APIVersion)

	// Parse clientId - always uses int16 length (nullable string) in both header v1 and v2.
	// Despite what one might expect, request header v2 does NOT use compact string for clientId.
	// See: https://ivanyu.me/blog/2024/09/08/kafka-protocol-practical-guide/
	if len(buf) < offset+2 {
		return nil, 0, errors.New("request too short for clientId length")
	}
	clientIDLen := int16(binary.BigEndian.Uint16(buf[offset : offset+2]))
	offset += 2

	if clientIDLen < -1 {
		return nil, 0, fmt.Errorf("invalid clientId length: %d", clientIDLen)
	}
	if clientIDLen > 0 {
		if len(buf) < offset+int(clientIDLen) {
			return nil, 0, errors.New("request too short for clientId")
		}
		header.ClientID = string(buf[offset : offset+int(clientIDLen)])
		offset += int(clientIDLen)

		// Extract zone_id from clientId per spec section 7.1
		header.ZoneID = parseZoneID(header.ClientID)
	}

	// Check if we need to parse tagged fields.
	// - Flexible APIs (header v2) always have tagged fields
	// - ApiVersions v3+ is special: it uses int16 clientId but STILL has tagged fields
	hasTaggedFields := flexible || (header.APIKey == 18 && header.APIVersion >= 3)

	if hasTaggedFields {
		// Parse tagged fields after clientId
		if len(buf) < offset+1 {
			return nil, 0, errors.New("request too short for header tags")
		}
		numTags, bytesRead := readUvarint(buf[offset:])
		offset += bytesRead
		// Skip each tag
		for i := uint64(0); i < numTags; i++ {
			if len(buf) <= offset {
				return nil, 0, errors.New("request too short for tag key")
			}
			_, bytesRead := readUvarint(buf[offset:]) // tag key
			offset += bytesRead
			if len(buf) <= offset {
				return nil, 0, errors.New("request too short for tag length")
			}
			tagLen, bytesRead := readUvarint(buf[offset:]) // tag length
			offset += bytesRead
			if len(buf) < offset+int(tagLen) {
				return nil, 0, errors.New("request too short for tag data")
			}
			offset += int(tagLen) // skip tag data
		}
	}

	return header, offset, nil
}

// readUvarint reads an unsigned varint from the buffer.
// Returns the value and the number of bytes consumed.
func readUvarint(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
		if s >= 64 {
			return 0, i + 1 // Overflow, return what we have
		}
	}
	return x, len(buf)
}

// writeResponse writes a Kafka response with the length prefix.
func (s *Server) writeResponse(w io.Writer, response []byte) error {
	// Write 4-byte length prefix
	var lengthBuf [4]byte
	binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(response)))

	if _, err := w.Write(lengthBuf[:]); err != nil {
		return fmt.Errorf("failed to write length prefix: %w", err)
	}

	if _, err := w.Write(response); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	return nil
}

// monitorConnection watches for connection closure while a handler is running.
// It uses poll syscall to detect when the remote end closes the connection,
// without reading any data (which could consume pipelined requests).
// When closure is detected, it cancels the request context so the handler can exit early.
func (s *Server) monitorConnection(conn net.Conn, ctx context.Context, cancel context.CancelFunc, done chan struct{}) {
	defer close(done)

	// Get the underlying file descriptor for polling
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		// Not a TCP connection, can't monitor
		<-ctx.Done()
		return
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		// Can't get raw conn, fall back to waiting for context
		<-ctx.Done()
		return
	}

	var fd int
	if err := rawConn.Control(func(fdPtr uintptr) {
		fd = int(fdPtr)
	}); err != nil {
		<-ctx.Done()
		return
	}

	// Poll for connection close using POLLRDHUP which specifically detects
	// when the remote end has shutdown the connection (sent FIN).
	pollFds := []unix.PollFd{{
		Fd:     int32(fd),
		Events: unix.POLLHUP | unix.POLLERR,
	}}

	for {
		select {
		case <-ctx.Done():
			// Handler finished or was cancelled externally
			return
		default:
		}

		// Poll with 100ms timeout
		n, err := unix.Poll(pollFds, 100)
		if err != nil {
			if err == unix.EINTR {
				continue // Interrupted, retry
			}
			// Poll error, cancel context
			cancel()
			return
		}

		if n > 0 && pollFds[0].Revents != 0 {
			// POLLRDHUP, POLLHUP, or POLLERR all indicate connection issues
			cancel()
			return
		}
	}
}

// EncodeResponseHeader creates a Kafka response header.
// For most APIs, this is just the correlation ID.
func EncodeResponseHeader(correlationID int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(correlationID))
	return buf
}

// APIKey returns the name of a Kafka API by its key.
func APIKey(key int16) string {
	return kmsg.Key(key).Name()
}

// parseZoneID extracts zone_id from a Kafka client.id string.
// Per spec section 7.1, client.id may contain comma-separated k=v pairs.
// Example: "zone_id=us-east-1a,app=myservice"
// Returns empty string if zone_id is not found.
func parseZoneID(clientID string) string {
	if clientID == "" {
		return ""
	}

	for _, part := range strings.Split(clientID, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "zone_id=") {
			return strings.TrimPrefix(part, "zone_id=")
		}
	}

	return ""
}
