package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthServer_Healthz_OK(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", status.Status)
	}
}

func TestHealthServer_Healthz_ShuttingDown(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.SetShuttingDown()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "shutting_down" {
		t.Errorf("expected status 'shutting_down', got %q", status.Status)
	}

	if check, ok := status.Checks["shutdown"]; !ok || check.Healthy {
		t.Error("expected shutdown check to be unhealthy")
	}
}

func TestHealthServer_Healthz_GoroutinesHealthy(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.RegisterGoroutine("tcp-acceptor")
	h.RegisterGoroutine("conn-handler")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", status.Status)
	}

	if len(status.Goroutines) != 2 {
		t.Errorf("expected 2 goroutines, got %d", len(status.Goroutines))
	}

	for name, running := range status.Goroutines {
		if !running {
			t.Errorf("goroutine %s should be running", name)
		}
	}
}

func TestHealthServer_Healthz_GoroutineStopped(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.RegisterGoroutine("tcp-acceptor")
	h.UnregisterGoroutine("tcp-acceptor")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "degraded" {
		t.Errorf("expected status 'degraded', got %q", status.Status)
	}

	if status.Goroutines["tcp-acceptor"] != false {
		t.Error("tcp-acceptor goroutine should show as not running")
	}
}

func TestHealthServer_Healthz_MethodNotAllowed(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHealthServer_Healthz_HeadMethod(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodHead, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	// HEAD should not have a body
	if w.Body.Len() > 0 {
		t.Error("HEAD response should not have a body")
	}
}

func TestHealthServer_UpdateGoroutine(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.RegisterGoroutine("worker")

	// Get initial time
	h.mu.RLock()
	initialTime := h.goroutines["worker"].lastCheck
	h.mu.RUnlock()

	// Wait a bit and update
	time.Sleep(10 * time.Millisecond)
	h.UpdateGoroutine("worker")

	// Check time was updated
	h.mu.RLock()
	updatedTime := h.goroutines["worker"].lastCheck
	h.mu.RUnlock()

	if !updatedTime.After(initialTime) {
		t.Error("UpdateGoroutine should update lastCheck time")
	}
}

func TestHealthServer_CheckHealth(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.RegisterGoroutine("worker")

	status := h.CheckHealth()

	if status.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", status.Status)
	}

	if !status.Goroutines["worker"] {
		t.Error("worker goroutine should be healthy")
	}
}

func TestHealthServer_IsShuttingDown(t *testing.T) {
	h := NewHealthServer(":0", nil)

	if h.IsShuttingDown() {
		t.Error("should not be shutting down initially")
	}

	h.SetShuttingDown()

	if !h.IsShuttingDown() {
		t.Error("should be shutting down after SetShuttingDown")
	}
}

func TestHealthServer_StartAndClose(t *testing.T) {
	h := NewHealthServer("127.0.0.1:0", nil)

	if err := h.Start(); err != nil {
		t.Fatalf("failed to start health server: %v", err)
	}

	// Give the server time to start
	time.Sleep(50 * time.Millisecond)

	// Make a request to verify it's running
	resp, err := http.Get("http://" + h.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("failed to make request: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Close the server
	if err := h.Close(); err != nil {
		t.Errorf("failed to close health server: %v", err)
	}
}

func TestHealthServer_GoroutineStale(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.RegisterGoroutine("stale-worker")

	// Manually set the last check to more than 30 seconds ago
	h.mu.Lock()
	h.goroutines["stale-worker"].lastCheck = time.Now().Add(-31 * time.Second)
	h.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "degraded" {
		t.Errorf("expected status 'degraded', got %q", status.Status)
	}

	if status.Goroutines["stale-worker"] != false {
		t.Error("stale goroutine should show as not running")
	}
}

func TestHealthServer_ContentType(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.handleHealthz(w, req)

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got %q", contentType)
	}
}

func TestHealthServer_CloseWithoutStart(t *testing.T) {
	h := NewHealthServer(":0", nil)

	// Close should be safe even if not started
	if err := h.Close(); err != nil {
		t.Errorf("Close() without Start() should not error: %v", err)
	}
}
