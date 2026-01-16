// Package server implements the TCP server for the Kafka wire protocol.
package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dray-io/dray/internal/logging"
)

// ReadinessChecker is an interface for components that can report their readiness.
// Each component (metadata store, object store, compactor) implements this to
// participate in readiness checks.
type ReadinessChecker interface {
	// Name returns the name of the component for display in health status.
	Name() string

	// CheckReady performs a health check and returns nil if the component is ready,
	// or an error describing why it's not ready.
	CheckReady(ctx context.Context) error
}

// HealthServer provides HTTP endpoints for health checks.
// It serves /healthz for liveness probes and /readyz for readiness probes.
type HealthServer struct {
	mu               sync.RWMutex
	addr             string
	boundAddr        string
	server           *http.Server
	logger           *logging.Logger
	shutDown         atomic.Bool
	goroutines       map[string]*goroutineStatus
	readinessChecks  []ReadinessChecker
	readinessTimeout time.Duration
	extraHandlers    map[string]http.Handler
}

// goroutineStatus tracks whether a critical goroutine is running.
type goroutineStatus struct {
	running   bool
	lastCheck time.Time
}

// HealthStatus represents the health check response.
type HealthStatus struct {
	Status     string                 `json:"status"`
	Goroutines map[string]bool        `json:"goroutines,omitempty"`
	Checks     map[string]CheckResult `json:"checks,omitempty"`
}

// CheckResult represents the result of a single health check.
type CheckResult struct {
	Healthy bool   `json:"healthy"`
	Message string `json:"message,omitempty"`
}

// DefaultReadinessTimeout is the default timeout for readiness checks.
const DefaultReadinessTimeout = 5 * time.Second

// NewHealthServer creates a new HealthServer.
func NewHealthServer(addr string, logger *logging.Logger) *HealthServer {
	if logger == nil {
		logger = logging.DefaultLogger()
	}
	return &HealthServer{
		addr:             addr,
		logger:           logger,
		goroutines:       make(map[string]*goroutineStatus),
		readinessChecks:  make([]ReadinessChecker, 0),
		readinessTimeout: DefaultReadinessTimeout,
		extraHandlers:    make(map[string]http.Handler),
	}
}

// RegisterHandler registers an extra HTTP handler to be served alongside health endpoints.
// Call before Start so the handler is mounted on the server mux.
func (h *HealthServer) RegisterHandler(pattern string, handler http.Handler) {
	if pattern == "" || handler == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.extraHandlers[pattern] = handler
}

// RegisterReadinessCheck registers a component for readiness checking.
// The component will be checked on each /readyz request.
func (h *HealthServer) RegisterReadinessCheck(checker ReadinessChecker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.readinessChecks = append(h.readinessChecks, checker)
}

// SetReadinessTimeout sets the timeout for individual readiness checks.
func (h *HealthServer) SetReadinessTimeout(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.readinessTimeout = d
}

// RegisterGoroutine registers a critical goroutine for health checking.
// Call this when a critical goroutine starts.
func (h *HealthServer) RegisterGoroutine(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.goroutines[name] = &goroutineStatus{
		running:   true,
		lastCheck: time.Now(),
	}
}

// UpdateGoroutine updates the last check time for a goroutine.
// Call this periodically from the goroutine to indicate it's still running.
func (h *HealthServer) UpdateGoroutine(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if status, ok := h.goroutines[name]; ok {
		status.lastCheck = time.Now()
	}
}

// UnregisterGoroutine marks a goroutine as stopped.
// Call this when a critical goroutine exits.
func (h *HealthServer) UnregisterGoroutine(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if status, ok := h.goroutines[name]; ok {
		status.running = false
	}
}

// SetShuttingDown marks the server as shutting down.
// After this is called, /healthz will return 503.
func (h *HealthServer) SetShuttingDown() {
	h.shutDown.Store(true)
}

// IsShuttingDown returns true if the server is shutting down.
func (h *HealthServer) IsShuttingDown() bool {
	return h.shutDown.Load()
}

// Start starts the HTTP health server.
func (h *HealthServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/readyz", h.handleReadyz)
	h.mu.RLock()
	extraHandlers := make(map[string]http.Handler, len(h.extraHandlers))
	for pattern, handler := range h.extraHandlers {
		extraHandlers[pattern] = handler
	}
	h.mu.RUnlock()
	for pattern, handler := range extraHandlers {
		mux.Handle(pattern, handler)
	}
	// Expose pprof endpoints for profiling.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	h.server = &http.Server{
		Addr:         h.addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second, // Longer to accommodate readiness checks
	}

	ln, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}

	h.mu.Lock()
	h.boundAddr = ln.Addr().String()
	h.mu.Unlock()

	h.logger.Infof("health server listening", map[string]any{"addr": ln.Addr().String()})

	go func() {
		if err := h.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			h.logger.Errorf("health server error", map[string]any{"error": err.Error()})
		}
	}()

	return nil
}

// Addr returns the actual bound address of the server.
// Returns the configured address if the server hasn't started yet.
func (h *HealthServer) Addr() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.boundAddr != "" {
		return h.boundAddr
	}
	return h.addr
}

// Close shuts down the health server.
func (h *HealthServer) Close() error {
	if h.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return h.server.Shutdown(ctx)
}

// handleHealthz handles the /healthz liveness endpoint.
// Returns 200 OK if the broker is alive and not shutting down.
// Returns 503 if shutting down or critical goroutines are not running.
func (h *HealthServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := h.checkLiveness()

	w.Header().Set("Content-Type", "application/json")

	if status.Status != "ok" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if r.Method != http.MethodHead {
		json.NewEncoder(w).Encode(status)
	}
}

// checkLiveness performs the liveness health check.
func (h *HealthServer) checkLiveness() HealthStatus {
	status := HealthStatus{
		Status:     "ok",
		Goroutines: make(map[string]bool),
		Checks:     make(map[string]CheckResult),
	}

	// Check if server is shutting down
	if h.shutDown.Load() {
		status.Status = "shutting_down"
		status.Checks["shutdown"] = CheckResult{
			Healthy: false,
			Message: "broker is shutting down",
		}
		return status
	}

	status.Checks["shutdown"] = CheckResult{
		Healthy: true,
		Message: "broker is running",
	}

	// Check critical goroutines
	h.mu.RLock()
	defer h.mu.RUnlock()

	allGoroutinesOK := true
	for name, gs := range h.goroutines {
		isHealthy := gs.running && time.Since(gs.lastCheck) < 30*time.Second
		status.Goroutines[name] = isHealthy
		if !isHealthy {
			allGoroutinesOK = false
		}
	}

	if !allGoroutinesOK {
		status.Status = "degraded"
		status.Checks["goroutines"] = CheckResult{
			Healthy: false,
			Message: "one or more critical goroutines are not running",
		}
	} else if len(h.goroutines) > 0 {
		status.Checks["goroutines"] = CheckResult{
			Healthy: true,
			Message: "all critical goroutines are running",
		}
	}

	return status
}

// CheckHealth returns the current health status without making an HTTP request.
// Useful for internal health checks.
func (h *HealthServer) CheckHealth() HealthStatus {
	return h.checkLiveness()
}

// handleReadyz handles the /readyz readiness endpoint.
// Returns 200 OK if all dependencies are healthy.
// Returns 503 if the server is shutting down or any dependency check fails.
func (h *HealthServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	status := h.checkReadiness(ctx)

	w.Header().Set("Content-Type", "application/json")

	if status.Status != "ok" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if r.Method != http.MethodHead {
		json.NewEncoder(w).Encode(status)
	}
}

// checkReadiness performs all readiness checks.
func (h *HealthServer) checkReadiness(ctx context.Context) HealthStatus {
	status := HealthStatus{
		Status: "ok",
		Checks: make(map[string]CheckResult),
	}

	// Check if server is shutting down
	if h.shutDown.Load() {
		status.Status = "shutting_down"
		status.Checks["shutdown"] = CheckResult{
			Healthy: false,
			Message: "broker is shutting down",
		}
		return status
	}

	status.Checks["shutdown"] = CheckResult{
		Healthy: true,
		Message: "broker is running",
	}

	// Run all registered readiness checks
	h.mu.RLock()
	checks := make([]ReadinessChecker, len(h.readinessChecks))
	copy(checks, h.readinessChecks)
	timeout := h.readinessTimeout
	h.mu.RUnlock()

	for _, checker := range checks {
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := checker.CheckReady(checkCtx)
		cancel()

		if err != nil {
			status.Status = "not_ready"
			status.Checks[checker.Name()] = CheckResult{
				Healthy: false,
				Message: err.Error(),
			}
		} else {
			status.Checks[checker.Name()] = CheckResult{
				Healthy: true,
				Message: "healthy",
			}
		}
	}

	return status
}

// CheckReadiness returns the current readiness status without making an HTTP request.
// Useful for internal readiness checks.
func (h *HealthServer) CheckReadiness(ctx context.Context) HealthStatus {
	return h.checkReadiness(ctx)
}
