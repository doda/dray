package metrics

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server provides an HTTP server for Prometheus metrics scraping.
// It serves the /metrics endpoint with all registered Prometheus metrics.
type Server struct {
	mu        sync.RWMutex
	addr      string
	boundAddr string
	server    *http.Server
	registry  prometheus.Gatherer
}

// NewServer creates a new metrics server that listens on the given address.
// Use addr ":9090" for the default metrics port.
// Uses the default Prometheus registry.
func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		registry: nil, // nil means use default registry
	}
}

// NewServerWithRegistry creates a new metrics server with a custom registry.
// Useful for testing to avoid conflicts with the default registry.
func NewServerWithRegistry(addr string, gatherer prometheus.Gatherer) *Server {
	return &Server{
		addr:     addr,
		registry: gatherer,
	}
}

// Start starts the HTTP server for metrics.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	if s.registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	} else {
		mux.Handle("/metrics", promhttp.Handler())
	}

	s.server = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.boundAddr = ln.Addr().String()
	s.mu.Unlock()

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			// Log error but don't fail - metrics are best-effort
		}
	}()

	return nil
}

// Addr returns the actual bound address of the server.
// Returns the configured address if the server hasn't started yet.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.boundAddr != "" {
		return s.boundAddr
	}
	return s.addr
}

// Close shuts down the metrics server.
func (s *Server) Close() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}
