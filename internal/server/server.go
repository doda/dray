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

		// Parse zone_id from client.id per spec 7.1
		zoneID := ParseZoneID(header.ClientID)

		reqLogger := logger.With(map[string]any{
			"apiKey":        header.APIKey,
			"apiVersion":    header.APIVersion,
			"correlationId": header.CorrelationID,
			"clientId":      header.ClientID,
			"zoneId":        zoneID,
		})
		reqLogger.Debug("request received")

		ctx := context.Background()
		ctx = logging.WithLoggerCtx(ctx, reqLogger)
		ctx = WithClientID(ctx, header.ClientID)
		ctx = WithZoneID(ctx, zoneID)

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

	// clientId is a nullable string: int16 length followed by bytes
	// -1 means null, 0 means empty string, >0 means that many bytes
	// Other negative values are invalid per Kafka protocol
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
	}
	// clientIDLen == -1 (null) or 0 (empty): header.ClientID remains ""

	return header, offset, nil
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
