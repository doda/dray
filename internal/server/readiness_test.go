package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/objectstore"
)

func TestHealthServer_Readyz_OK(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

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

func TestHealthServer_Readyz_ShuttingDown(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.SetShuttingDown()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

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

func TestHealthServer_Readyz_WithHealthyCheck(t *testing.T) {
	h := NewHealthServer(":0", nil)

	// Register a healthy checker
	checker := NewFuncChecker("test_component", func(ctx context.Context) error {
		return nil
	})
	h.RegisterReadinessCheck(checker)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

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

	check, ok := status.Checks["test_component"]
	if !ok {
		t.Fatal("expected test_component check to be present")
	}
	if !check.Healthy {
		t.Error("expected test_component check to be healthy")
	}
	if check.Message != "healthy" {
		t.Errorf("expected message 'healthy', got %q", check.Message)
	}
}

func TestHealthServer_Readyz_WithUnhealthyCheck(t *testing.T) {
	h := NewHealthServer(":0", nil)

	// Register an unhealthy checker
	checker := NewFuncChecker("failing_component", func(ctx context.Context) error {
		return errors.New("connection refused")
	})
	h.RegisterReadinessCheck(checker)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", status.Status)
	}

	check, ok := status.Checks["failing_component"]
	if !ok {
		t.Fatal("expected failing_component check to be present")
	}
	if check.Healthy {
		t.Error("expected failing_component check to be unhealthy")
	}
	if check.Message != "connection refused" {
		t.Errorf("expected message 'connection refused', got %q", check.Message)
	}
}

func TestHealthServer_Readyz_MultipleChecks(t *testing.T) {
	h := NewHealthServer(":0", nil)

	// Register multiple checkers - one healthy, one unhealthy
	h.RegisterReadinessCheck(NewFuncChecker("healthy_component", func(ctx context.Context) error {
		return nil
	}))
	h.RegisterReadinessCheck(NewFuncChecker("unhealthy_component", func(ctx context.Context) error {
		return errors.New("service unavailable")
	}))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", status.Status)
	}

	// Check healthy component
	check, ok := status.Checks["healthy_component"]
	if !ok {
		t.Fatal("expected healthy_component check to be present")
	}
	if !check.Healthy {
		t.Error("expected healthy_component check to be healthy")
	}

	// Check unhealthy component
	check, ok = status.Checks["unhealthy_component"]
	if !ok {
		t.Fatal("expected unhealthy_component check to be present")
	}
	if check.Healthy {
		t.Error("expected unhealthy_component check to be unhealthy")
	}
}

func TestHealthServer_Readyz_MethodNotAllowed(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodPost, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHealthServer_Readyz_HeadMethod(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodHead, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	// HEAD should not have a body
	if w.Body.Len() > 0 {
		t.Error("HEAD response should not have a body")
	}
}

func TestHealthServer_Readyz_Timeout(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.SetReadinessTimeout(50 * time.Millisecond)

	// Register a slow checker that will timeout
	checker := NewFuncChecker("slow_component", func(ctx context.Context) error {
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	h.RegisterReadinessCheck(checker)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", status.Status)
	}

	check, ok := status.Checks["slow_component"]
	if !ok {
		t.Fatal("expected slow_component check to be present")
	}
	if check.Healthy {
		t.Error("expected slow_component check to be unhealthy due to timeout")
	}
}

func TestHealthServer_Readyz_ContentType(t *testing.T) {
	h := NewHealthServer(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got %q", contentType)
	}
}

func TestHealthServer_CheckReadiness(t *testing.T) {
	h := NewHealthServer(":0", nil)
	h.RegisterReadinessCheck(NewFuncChecker("component", func(ctx context.Context) error {
		return nil
	}))

	status := h.CheckReadiness(context.Background())

	if status.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", status.Status)
	}

	if check, ok := status.Checks["component"]; !ok || !check.Healthy {
		t.Error("expected component check to be healthy")
	}
}

func TestHealthServer_StartWithReadyz(t *testing.T) {
	h := NewHealthServer("127.0.0.1:0", nil)
	h.RegisterReadinessCheck(NewFuncChecker("test", func(ctx context.Context) error {
		return nil
	}))

	if err := h.Start(); err != nil {
		t.Fatalf("failed to start health server: %v", err)
	}
	defer h.Close()

	// Give the server time to start
	time.Sleep(50 * time.Millisecond)

	// Make a request to /readyz
	resp, err := http.Get("http://" + h.Addr() + "/readyz")
	if err != nil {
		t.Fatalf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var status HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", status.Status)
	}
}

func TestCompactorChecker_Healthy(t *testing.T) {
	checker := NewCompactorChecker(func() bool { return true })

	if err := checker.CheckReady(context.Background()); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if checker.Name() != "compactor" {
		t.Errorf("expected name 'compactor', got %q", checker.Name())
	}
}

func TestCompactorChecker_Unhealthy(t *testing.T) {
	checker := NewCompactorChecker(func() bool { return false })

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for unhealthy compactor")
	}
	if err.Error() != "compactor is not running" {
		t.Errorf("expected 'compactor is not running', got %q", err.Error())
	}
}

func TestCompactorChecker_NotConfigured(t *testing.T) {
	checker := NewCompactorChecker(nil)

	if err := checker.CheckReady(context.Background()); err != nil {
		t.Errorf("expected no error when compactor not configured, got: %v", err)
	}
}

func TestFuncChecker_NilFunc(t *testing.T) {
	checker := NewFuncChecker("test", nil)

	if err := checker.CheckReady(context.Background()); err != nil {
		t.Errorf("expected no error for nil func, got: %v", err)
	}

	if checker.Name() != "test" {
		t.Errorf("expected name 'test', got %q", checker.Name())
	}
}

func TestMetadataStoreChecker_NilStore(t *testing.T) {
	checker := NewMetadataStoreChecker(nil)

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for nil store")
	}
	if err.Error() != "metadata store not configured" {
		t.Errorf("expected 'metadata store not configured', got %q", err.Error())
	}

	if checker.Name() != "metadata_store" {
		t.Errorf("expected name 'metadata_store', got %q", checker.Name())
	}
}

func TestObjectStoreChecker_NilStore(t *testing.T) {
	checker := NewObjectStoreChecker(nil)

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for nil store")
	}
	if err.Error() != "object store not configured" {
		t.Errorf("expected 'object store not configured', got %q", err.Error())
	}

	if checker.Name() != "object_store" {
		t.Errorf("expected name 'object_store', got %q", checker.Name())
	}
}

func TestObjectStoreHeadChecker_NilStore(t *testing.T) {
	checker := NewObjectStoreHeadChecker(nil)

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for nil store")
	}
	if err.Error() != "object store not configured" {
		t.Errorf("expected 'object store not configured', got %q", err.Error())
	}

	if checker.Name() != "object_store" {
		t.Errorf("expected name 'object_store', got %q", checker.Name())
	}
}

// TestMetadataStoreChecker_WithMockStore tests the MetadataStoreChecker with a real mock store.
func TestMetadataStoreChecker_WithMockStore(t *testing.T) {
	store := metadata.NewMockStore()
	defer store.Close()

	checker := NewMetadataStoreChecker(store)

	err := checker.CheckReady(context.Background())
	if err != nil {
		t.Errorf("expected no error for healthy store, got: %v", err)
	}
}

// TestMetadataStoreChecker_ClosedStore tests that a closed store is detected as unhealthy.
func TestMetadataStoreChecker_ClosedStore(t *testing.T) {
	store := metadata.NewMockStore()
	store.Close() // Close it immediately

	checker := NewMetadataStoreChecker(store)

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for closed store")
	}
	if !errors.Is(err, metadata.ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got: %v", err)
	}
}

// mockObjectStore is a simple mock object store for testing.
type mockObjectStore struct {
	getErr  error
	listErr error
}

func (m *mockObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return nil
}

func (m *mockObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return nil
}

func (m *mockObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return nil, objectstore.ErrNotFound
}

func (m *mockObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	return nil, objectstore.ErrNotFound
}

func (m *mockObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	return objectstore.ObjectMeta{}, objectstore.ErrNotFound
}

func (m *mockObjectStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (m *mockObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return nil, nil // Empty list is OK
}

func (m *mockObjectStore) Close() error {
	return nil
}

// TestObjectStoreChecker_HealthyStore tests the checker with a healthy mock store.
func TestObjectStoreChecker_HealthyStore(t *testing.T) {
	store := &mockObjectStore{}
	checker := NewObjectStoreChecker(store)

	err := checker.CheckReady(context.Background())
	if err != nil {
		t.Errorf("expected no error for healthy store, got: %v", err)
	}
}

// TestObjectStoreChecker_UnhealthyStore tests the checker with a store that returns errors.
func TestObjectStoreChecker_UnhealthyStore(t *testing.T) {
	store := &mockObjectStore{
		listErr: errors.New("connection refused"),
	}
	checker := NewObjectStoreChecker(store)

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for unhealthy store")
	}
}

// TestObjectStoreChecker_BucketNotFound tests that bucket not found is reported as unhealthy.
func TestObjectStoreChecker_BucketNotFound(t *testing.T) {
	store := &mockObjectStore{
		listErr: objectstore.ErrBucketNotFound,
	}
	checker := NewObjectStoreChecker(store)

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for bucket not found")
	}
}

// TestObjectStoreHeadChecker_HealthyStore tests the head checker with a healthy store.
func TestObjectStoreHeadChecker_HealthyStore(t *testing.T) {
	store := &mockObjectStore{} // Default returns ErrNotFound which means store is reachable
	checker := NewObjectStoreHeadChecker(store)

	err := checker.CheckReady(context.Background())
	if err != nil {
		t.Errorf("expected no error for healthy store, got: %v", err)
	}
}

// TestObjectStoreHeadChecker_ConnectionError tests that connection errors are detected.
func TestObjectStoreHeadChecker_ConnectionError(t *testing.T) {
	store := &mockObjectStore{
		getErr: errors.New("connection refused"),
	}
	checker := NewObjectStoreHeadChecker(store)

	err := checker.CheckReady(context.Background())
	if err == nil {
		t.Error("expected error for connection refused")
	}
}

// TestReadyz_AllDependenciesHealthy tests the full readiness flow with all dependencies.
func TestReadyz_AllDependenciesHealthy(t *testing.T) {
	h := NewHealthServer("127.0.0.1:0", nil)

	// Register all dependency checkers
	metaStore := metadata.NewMockStore()
	defer metaStore.Close()
	h.RegisterReadinessCheck(NewMetadataStoreChecker(metaStore))

	objStore := &mockObjectStore{}
	h.RegisterReadinessCheck(NewObjectStoreHeadChecker(objStore))

	h.RegisterReadinessCheck(NewCompactorChecker(func() bool { return true }))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

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

	// Verify all checks are present and healthy
	for _, name := range []string{"metadata_store", "object_store", "compactor"} {
		check, ok := status.Checks[name]
		if !ok {
			t.Errorf("expected %s check to be present", name)
			continue
		}
		if !check.Healthy {
			t.Errorf("expected %s check to be healthy", name)
		}
	}
}

// TestReadyz_MetadataStoreUnhealthy tests that metadata store failure causes not_ready.
func TestReadyz_MetadataStoreUnhealthy(t *testing.T) {
	h := NewHealthServer(":0", nil)

	// Register a closed metadata store
	metaStore := metadata.NewMockStore()
	metaStore.Close()
	h.RegisterReadinessCheck(NewMetadataStoreChecker(metaStore))

	// Object store is healthy
	objStore := &mockObjectStore{}
	h.RegisterReadinessCheck(NewObjectStoreHeadChecker(objStore))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", status.Status)
	}

	// Metadata store should be unhealthy
	if check, ok := status.Checks["metadata_store"]; ok {
		if check.Healthy {
			t.Error("expected metadata_store check to be unhealthy")
		}
	} else {
		t.Error("expected metadata_store check to be present")
	}

	// Object store should still be healthy
	if check, ok := status.Checks["object_store"]; ok {
		if !check.Healthy {
			t.Error("expected object_store check to be healthy")
		}
	}
}

// TestReadyz_ObjectStoreUnhealthy tests that object store failure causes not_ready.
func TestReadyz_ObjectStoreUnhealthy(t *testing.T) {
	h := NewHealthServer(":0", nil)

	// Metadata store is healthy
	metaStore := metadata.NewMockStore()
	defer metaStore.Close()
	h.RegisterReadinessCheck(NewMetadataStoreChecker(metaStore))

	// Object store is unhealthy - connection error
	objStore := &mockObjectStore{
		getErr: errors.New("connection refused"),
	}
	h.RegisterReadinessCheck(NewObjectStoreHeadChecker(objStore))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	h.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var status HealthStatus
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", status.Status)
	}

	// Metadata store should still be healthy
	if check, ok := status.Checks["metadata_store"]; ok {
		if !check.Healthy {
			t.Error("expected metadata_store check to be healthy")
		}
	}

	// Object store should be unhealthy
	if check, ok := status.Checks["object_store"]; ok {
		if check.Healthy {
			t.Error("expected object_store check to be unhealthy")
		}
	} else {
		t.Error("expected object_store check to be present")
	}
}
