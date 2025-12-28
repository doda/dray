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
	"time"

	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ErrServerClosed is returned when operations are attempted on a closed server.
var ErrServerClosed = errors.New("server closed")

// Handler processes Kafka API requests and returns responses.
type Handler interface {
	// HandleRequest processes a request and returns the response bytes.
	// The response should NOT include the length prefix - that will be added by the server.
	HandleRequest(ctx context.Context, header *RequestHeader, payload []byte) ([]byte, error)
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

// zoneContextKey is the context key for storing the client's zone_id.
type zoneContextKey struct{}

// ZoneIDFromContext retrieves the zone_id from context.
// Returns empty string if not set.
func ZoneIDFromContext(ctx context.Context) string {
	if v := ctx.Value(zoneContextKey{}); v != nil {
		return v.(string)
	}
	return ""
}

// WithZoneID returns a new context with the given zone_id.
func WithZoneID(ctx context.Context, zoneID string) context.Context {
	return context.WithValue(ctx, zoneContextKey{}, zoneID)
}

// Config holds the TCP server configuration.
type Config struct {
	ListenAddr     string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxRequestSize int32
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

	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	closed   atomic.Bool
	connWg   sync.WaitGroup
	connID   atomic.Int64
}

// New creates a new Server with the given configuration and handler.
func New(cfg Config, handler Handler, logger *logging.Logger) *Server {
	if logger == nil {
		logger = logging.DefaultLogger()
	}
	return &Server{
		cfg:     cfg,
		handler: handler,
		logger:  logger,
		conns:   make(map[net.Conn]struct{}),
	}
}

// ListenAndServe starts the server on the configured address.
func (s *Server) ListenAndServe() error {
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
			if s.closed.Load() {
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

// Close shuts down the server gracefully.
func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrServerClosed
	}

	s.mu.Lock()
	if s.listener != nil {
		s.listener.Close()
	}
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()

	s.connWg.Wait()
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.connWg.Done()
	defer conn.Close()

	connID := s.connID.Add(1)
	remoteAddr := conn.RemoteAddr().String()

	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	logger := s.logger.With(map[string]any{
		"connId":     connID,
		"remoteAddr": remoteAddr,
	})
	logger.Debug("connection accepted")

	for {
		if s.closed.Load() {
			return
		}

		if s.cfg.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		}

		header, payload, err := s.readRequest(conn)
		if err != nil {
			if err == io.EOF || s.closed.Load() {
				logger.Debug("connection closed")
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Debug("read timeout")
				return
			}
			logger.Warnf("read error", map[string]any{"error": err.Error()})
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

		ctx := context.Background()
		ctx = logging.WithLoggerCtx(ctx, reqLogger)
		// Store zone_id in context for handlers (per spec 7.1)
		if header.ZoneID != "" {
			ctx = WithZoneID(ctx, header.ZoneID)
		}

		response, err := s.handler.HandleRequest(ctx, header, payload)
		if err != nil {
			reqLogger.Errorf("handler error", map[string]any{"error": err.Error()})
			return
		}

		if s.cfg.WriteTimeout > 0 {
			conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		}

		if err := s.writeResponse(conn, response); err != nil {
			reqLogger.Warnf("write error", map[string]any{"error": err.Error()})
			return
		}

		reqLogger.Debug("response sent")
	}
}

// readRequest reads a Kafka request from the connection.
// Returns the parsed header and the payload (body after header).
func (s *Server) readRequest(r io.Reader) (*RequestHeader, []byte, error) {
	// Read 4-byte length prefix (big-endian)
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, nil, err
	}
	length := int32(binary.BigEndian.Uint32(lengthBuf[:]))

	if length < 0 || length > s.cfg.MaxRequestSize {
		return nil, nil, fmt.Errorf("invalid request size: %d", length)
	}

	// Read the full request
	requestBuf := make([]byte, length)
	if _, err := io.ReadFull(r, requestBuf); err != nil {
		return nil, nil, fmt.Errorf("failed to read request body: %w", err)
	}

	// Parse the request header
	header, headerLen, err := parseRequestHeader(requestBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse request header: %w", err)
	}

	// Payload is everything after the header
	payload := requestBuf[headerLen:]
	return header, payload, nil
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
	flexible := isFlexibleRequest(header.APIKey, header.APIVersion)

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

// isFlexibleRequest returns true if the given API key and version uses flexible encoding
// in the REQUEST HEADER (not the body). This is important because ApiVersions is special-cased
// to always use the v1 request header format (non-flexible) even for v3+ requests.
// Based on Kafka protocol specification for request header versions.
func isFlexibleRequest(apiKey, version int16) bool {
	// ApiVersions is special: it always uses the v1 request header format (non-flexible)
	// because the broker needs to understand the request to negotiate versions.
	// The REQUEST BODY still uses flexible encoding for v3+, but the HEADER does not.
	if apiKey == 18 { // ApiVersions
		return false // Always use non-flexible header
	}

	// This maps API keys to the version at which their HEADER becomes flexible.
	// See: https://kafka.apache.org/protocol.html#protocol_api_keys
	switch apiKey {
	case 0: // Produce
		return version >= 9
	case 1: // Fetch
		return version >= 12
	case 2: // ListOffsets
		return version >= 6
	case 3: // Metadata
		return version >= 9
	case 8: // OffsetCommit
		return version >= 8
	case 9: // OffsetFetch
		return version >= 6
	case 10: // FindCoordinator
		return version >= 3
	case 11: // JoinGroup
		return version >= 6
	case 12: // Heartbeat
		return version >= 4
	case 13: // LeaveGroup
		return version >= 4
	case 14: // SyncGroup
		return version >= 4
	case 15: // DescribeGroups
		return version >= 5
	case 16: // ListGroups
		return version >= 3
	case 19: // CreateTopics
		return version >= 5
	case 20: // DeleteTopics
		return version >= 4
	case 32: // DescribeConfigs
		return version >= 4
	case 42: // DeleteGroups
		return version >= 2
	case 44: // IncrementalAlterConfigs
		return version >= 1
	case 60: // DescribeCluster
		return version >= 0 // All versions are flexible
	case 68: // ConsumerGroupHeartbeat
		return version >= 0 // All versions are flexible
	case 69: // ConsumerGroupDescribe
		return version >= 0 // All versions are flexible
	default:
		return false
	}
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
