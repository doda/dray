package metrics

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// testMetricsOnce ensures we only create test metrics once to avoid duplicate registration
var testMetricsOnce sync.Once
var testMetrics *ProduceMetrics

func getTestMetrics() *ProduceMetrics {
	testMetricsOnce.Do(func() {
		testMetrics = NewProduceMetrics()
	})
	return testMetrics
}

func TestNewServer(t *testing.T) {
	s := NewServer(":0")
	if s.addr != ":0" {
		t.Errorf("addr = %q, want %q", s.addr, ":0")
	}
}

func TestServer_StartAndClose(t *testing.T) {
	s := NewServer(":0")
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer s.Close()

	// Verify we got a bound address
	addr := s.Addr()
	if !strings.Contains(addr, ":") {
		t.Errorf("Addr() = %q, expected host:port format", addr)
	}
}

func TestServer_MetricsEndpoint(t *testing.T) {
	// Get test metrics (only created once)
	m := getTestMetrics()
	m.RecordSuccess(0.005)
	m.RecordSuccess(0.010)
	m.RecordFailure(0.050)

	// Start server
	s := NewServer(":0")
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer s.Close()

	// Give server time to start
	time.Sleep(10 * time.Millisecond)

	// Fetch metrics
	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	bodyStr := string(body)

	// Check for our custom metrics
	if !strings.Contains(bodyStr, "dray_produce_latency_seconds") {
		t.Error("expected dray_produce_latency_seconds in metrics output")
	}
	if !strings.Contains(bodyStr, "dray_produce_requests_total") {
		t.Error("expected dray_produce_requests_total in metrics output")
	}

	// Check for status labels
	if !strings.Contains(bodyStr, `status="success"`) {
		t.Error("expected status=success label in metrics output")
	}
	if !strings.Contains(bodyStr, `status="failure"`) {
		t.Error("expected status=failure label in metrics output")
	}
}

func TestServer_MetricsEndpointFormat(t *testing.T) {
	// Get test metrics (only created once)
	m := getTestMetrics()
	m.RecordSuccess(0.001)

	s := NewServer(":0")
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer s.Close()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	// Verify content type is prometheus format
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("Content-Type = %q, expected text/plain", contentType)
	}
}

func TestServerWithCustomRegistry(t *testing.T) {
	// Use a custom registry for isolation
	reg := prometheus.NewRegistry()
	m := NewProduceMetricsWithRegistry(reg)
	m.RecordSuccess(0.002)
	m.RecordFailure(0.008)

	// Create server with custom registry
	s := NewServerWithRegistry(":0", reg)
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer s.Close()

	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	bodyStr := string(body)

	// Verify custom metrics are present
	if !strings.Contains(bodyStr, "dray_produce_latency_seconds") {
		t.Error("expected dray_produce_latency_seconds in metrics output")
	}
}

func TestServer_Close(t *testing.T) {
	s := NewServer(":0")
	if err := s.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	addr := s.Addr()

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Give server time to close
	time.Sleep(10 * time.Millisecond)

	// Verify server is closed
	_, err := http.Get("http://" + addr + "/metrics")
	if err == nil {
		t.Error("expected error after server close")
	}
}

func TestServer_CloseWithoutStart(t *testing.T) {
	s := NewServer(":0")
	// Should not panic or error
	if err := s.Close(); err != nil {
		t.Errorf("Close on unstarted server returned error: %v", err)
	}
}
