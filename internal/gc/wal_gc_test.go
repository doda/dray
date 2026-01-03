package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
)

// mockObjectStore implements objectstore.Store for testing.
type mockObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	closed  bool
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		objects: make(map[string][]byte),
	}
}

func (s *mockObjectStore) Put(_ context.Context, key string, reader io.Reader, _ int64, _ string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()
	return nil
}

func (s *mockObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, _ objectstore.PutOptions) error {
	return s.Put(ctx, key, reader, size, contentType)
}

func (s *mockObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *mockObjectStore) GetRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if end == -1 || end >= int64(len(data)) {
		end = int64(len(data) - 1)
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (s *mockObjectStore) Head(_ context.Context, key string) (objectstore.ObjectMeta, error) {
	s.mu.Lock()
	data, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (s *mockObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}

func (s *mockObjectStore) List(_ context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var keys []objectstore.ObjectMeta
	for k, v := range s.objects {
		if len(prefix) == 0 || (len(k) >= len(prefix) && k[:len(prefix)] == prefix) {
			keys = append(keys, objectstore.ObjectMeta{Key: k, Size: int64(len(v))})
		}
	}
	return keys, nil
}

func (s *mockObjectStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *mockObjectStore) hasObject(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key]
	return ok
}

var _ objectstore.Store = (*mockObjectStore)(nil)

func TestWALGCWorker_ScanOnce_DeletesEligibleObjects(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a GC record that's eligible for deletion (deleteAfterMs in the past)
	gcRecord := WALGCRecord{
		Path:          testWALPath(0, "test-wal-001"),
		DeleteAfterMs: time.Now().Add(-1 * time.Hour).UnixMilli(),
		CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
		SizeBytes:     1024,
	}
	gcRecordBytes, err := json.Marshal(gcRecord)
	if err != nil {
		t.Fatal(err)
	}

	// Write the WAL object to object store
	objStore.Put(ctx, gcRecord.Path, bytes.NewReader([]byte("test-wal-data")), 13, "application/octet-stream")

	// Create the GC marker
	gcKey := keys.WALGCKeyPath(0, "test-wal-001")
	metaStore.Put(ctx, gcKey, gcRecordBytes)

	// Create the worker and run a single scan
	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 1, BatchSize: 100})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the object was deleted
	if objStore.hasObject(gcRecord.Path) {
		t.Error("WAL object should have been deleted from object storage")
	}

	// Verify the GC marker was deleted
	result, err := metaStore.Get(ctx, gcKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("GC marker should have been deleted from metadata")
	}
}

func TestWALGCWorker_ScanOnce_SkipsNonEligibleObjects(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a GC record that's NOT eligible for deletion (deleteAfterMs in the future)
	gcRecord := WALGCRecord{
		Path:          testWALPath(0, "test-wal-002"),
		DeleteAfterMs: time.Now().Add(1 * time.Hour).UnixMilli(),
		CreatedAt:     time.Now().Add(-10 * time.Minute).UnixMilli(),
		SizeBytes:     2048,
	}
	gcRecordBytes, err := json.Marshal(gcRecord)
	if err != nil {
		t.Fatal(err)
	}

	// Write the WAL object to object store
	objStore.Put(ctx, gcRecord.Path, bytes.NewReader([]byte("test-wal-data-2")), 15, "application/octet-stream")

	// Create the GC marker
	gcKey := keys.WALGCKeyPath(0, "test-wal-002")
	metaStore.Put(ctx, gcKey, gcRecordBytes)

	// Create the worker and run a single scan
	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 1, BatchSize: 100})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the object was NOT deleted
	if !objStore.hasObject(gcRecord.Path) {
		t.Error("WAL object should NOT have been deleted (grace period not passed)")
	}

	// Verify the GC marker still exists
	result, err := metaStore.Get(ctx, gcKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("GC marker should still exist (grace period not passed)")
	}
}

func TestWALGCWorker_ScanOnce_MultipleDomains(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create GC records in multiple domains
	domains := []int{0, 1, 2}
	for _, domain := range domains {
		gcRecord := WALGCRecord{
			Path:          testWALPath(domain, "test-wal"),
			DeleteAfterMs: time.Now().Add(-30 * time.Minute).UnixMilli(),
			CreatedAt:     time.Now().Add(-1 * time.Hour).UnixMilli(),
			SizeBytes:     512,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		objStore.Put(ctx, gcRecord.Path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		gcKey := keys.WALGCKeyPath(domain, "test-wal")
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	// Create the worker and run a single scan
	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 3, BatchSize: 100})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify all objects and markers were deleted
	for _, domain := range domains {
		path := testWALPath(domain, "test-wal")
		if objStore.hasObject(path) {
			t.Errorf("WAL object in domain %d should have been deleted", domain)
		}

		gcKey := keys.WALGCKeyPath(domain, "test-wal")
		result, _ := metaStore.Get(ctx, gcKey)
		if result.Exists {
			t.Errorf("GC marker in domain %d should have been deleted", domain)
		}
	}
}

func TestWALGCWorker_ScanOnce_HandlesAlreadyDeletedObject(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a GC record pointing to an object that doesn't exist
	gcRecord := WALGCRecord{
		Path:          testWALPath(0, "missing-wal"),
		DeleteAfterMs: time.Now().Add(-1 * time.Hour).UnixMilli(),
		CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
		SizeBytes:     1024,
	}
	gcRecordBytes, _ := json.Marshal(gcRecord)
	gcKey := keys.WALGCKeyPath(0, "missing-wal")
	metaStore.Put(ctx, gcKey, gcRecordBytes)

	// Create the worker and run a single scan
	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 1, BatchSize: 100})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the GC marker was still deleted
	result, err := metaStore.Get(ctx, gcKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("GC marker should have been deleted even if object was already gone")
	}
}

func TestWALGCWorker_ProcessEligible(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create 3 eligible and 2 non-eligible GC records
	for i := 0; i < 5; i++ {
		walID := "test-wal-" + string(rune('a'+i))
		path := testWALPath(0, walID)

		var deleteAfterMs int64
		if i < 3 {
			deleteAfterMs = time.Now().Add(-1 * time.Hour).UnixMilli() // Eligible
		} else {
			deleteAfterMs = time.Now().Add(1 * time.Hour).UnixMilli() // Not eligible
		}

		gcRecord := WALGCRecord{
			Path:          path,
			DeleteAfterMs: deleteAfterMs,
			CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
			SizeBytes:     100,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		objStore.Put(ctx, path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		gcKey := keys.WALGCKeyPath(0, walID)
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 1, BatchSize: 100})
	deleted, err := worker.ProcessEligible(ctx, 0)
	if err != nil {
		t.Fatalf("ProcessEligible failed: %v", err)
	}

	if deleted != 3 {
		t.Errorf("Expected 3 deleted records, got %d", deleted)
	}

	// Verify eligible ones were deleted
	for i := 0; i < 3; i++ {
		walID := "test-wal-" + string(rune('a'+i))
		gcKey := keys.WALGCKeyPath(0, walID)
		result, _ := metaStore.Get(ctx, gcKey)
		if result.Exists {
			t.Errorf("GC marker for %s should have been deleted", walID)
		}
	}

	// Verify non-eligible ones still exist
	for i := 3; i < 5; i++ {
		walID := "test-wal-" + string(rune('a'+i))
		gcKey := keys.WALGCKeyPath(0, walID)
		result, _ := metaStore.Get(ctx, gcKey)
		if !result.Exists {
			t.Errorf("GC marker for %s should NOT have been deleted", walID)
		}
	}
}

func TestWALGCWorker_GetPendingCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create records in different domains
	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			walID := "wal-" + string(rune('0'+i)) + "-" + string(rune('a'+j))
			gcRecord := WALGCRecord{
				Path:          testWALPath(i, walID),
				DeleteAfterMs: time.Now().Add(-1 * time.Hour).UnixMilli(),
				CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
				SizeBytes:     100,
			}
			gcRecordBytes, _ := json.Marshal(gcRecord)
			gcKey := keys.WALGCKeyPath(i, walID)
			metaStore.Put(ctx, gcKey, gcRecordBytes)
		}
	}

	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 3, BatchSize: 100})
	count, err := worker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("GetPendingCount failed: %v", err)
	}

	if count != 6 {
		t.Errorf("Expected 6 pending GC records, got %d", count)
	}
}

func TestWALGCWorker_GetEligibleCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create 3 eligible and 2 non-eligible records
	for i := 0; i < 5; i++ {
		walID := "wal-" + string(rune('a'+i))
		var deleteAfterMs int64
		if i < 3 {
			deleteAfterMs = time.Now().Add(-1 * time.Hour).UnixMilli()
		} else {
			deleteAfterMs = time.Now().Add(1 * time.Hour).UnixMilli()
		}
		gcRecord := WALGCRecord{
			Path:          testWALPath(0, walID),
			DeleteAfterMs: deleteAfterMs,
			CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
			SizeBytes:     100,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		gcKey := keys.WALGCKeyPath(0, walID)
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 1, BatchSize: 100})
	count, err := worker.GetEligibleCount(ctx)
	if err != nil {
		t.Fatalf("GetEligibleCount failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 eligible GC records, got %d", count)
	}
}

func TestWALGCWorker_StartStop(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{
		ScanIntervalMs: 100,
		NumDomains:     1,
		BatchSize:      100,
	})

	// Start should be idempotent
	worker.Start()
	worker.Start() // Second call should be no-op

	// Give it a moment to run
	time.Sleep(50 * time.Millisecond)

	// Stop should be idempotent
	worker.Stop()
	worker.Stop() // Second call should be no-op
}

func TestWALGCWorker_DefaultConfig(t *testing.T) {
	cfg := DefaultWALGCWorkerConfig()
	if cfg.ScanIntervalMs <= 0 {
		t.Error("ScanIntervalMs should be positive")
	}
	if cfg.NumDomains <= 0 {
		t.Error("NumDomains should be positive")
	}
	if cfg.BatchSize <= 0 {
		t.Error("BatchSize should be positive")
	}
}

func TestWALGCWorker_ConfigDefaults(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	// Create worker with zero config values - should use defaults
	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{})

	if worker.config.ScanIntervalMs <= 0 {
		t.Error("ScanIntervalMs should have been set to default")
	}
	if worker.config.NumDomains <= 0 {
		t.Error("NumDomains should have been set to default")
	}
	if worker.config.BatchSize <= 0 {
		t.Error("BatchSize should have been set to default")
	}
}

func TestWALGCWorker_IntegrationWithWALRefCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Simulate what happens when compaction decrements a WAL refcount to 0
	// The compaction process creates a GC marker at /dray/v1/wal/gc/<metaDomain>/<walId>
	walID := "wal-compacted-001"
	walPath := testWALPath(0, walID)

	// Object exists
	objStore.Put(ctx, walPath, bytes.NewReader([]byte("original-wal-content")), 20, "application/octet-stream")

	// GC marker created by compaction swap (with a short grace period for testing)
	gcRecord := WALGCRecord{
		Path:          walPath,
		DeleteAfterMs: time.Now().Add(-1 * time.Second).UnixMilli(), // Already eligible
		CreatedAt:     time.Now().Add(-1 * time.Hour).UnixMilli(),
		SizeBytes:     20,
	}
	gcRecordBytes, _ := json.Marshal(gcRecord)
	gcKey := keys.WALGCKeyPath(0, walID)
	metaStore.Put(ctx, gcKey, gcRecordBytes)

	// GC worker scans and deletes
	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{NumDomains: 1, BatchSize: 100})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Object should be gone
	if objStore.hasObject(walPath) {
		t.Error("WAL object should have been deleted by GC")
	}

	// Marker should be gone
	result, _ := metaStore.Get(ctx, gcKey)
	if result.Exists {
		t.Error("GC marker should have been deleted")
	}
}

func TestWALGCWorker_Pagination_EarlyEntriesNotEligible(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Use a small batch size (3) to force multiple pages
	batchSize := 3
	now := time.Now()

	// Create 10 records total:
	// - Records 0-4 (first two pages, 5 records) are NOT eligible (deleteAfterMs in future)
	// - Records 5-9 (later pages) ARE eligible (deleteAfterMs in past)
	// Without pagination, only the first 3 records would be scanned and none deleted.
	// With pagination, all 10 are scanned and records 5-9 are deleted.
	for i := 0; i < 10; i++ {
		// Use zero-padded IDs to ensure lexicographic ordering matches numeric ordering
		walID := "wal-" + string(rune('0'+i/10)) + string(rune('0'+i%10))
		path := testWALPath(0, walID)

		var deleteAfterMs int64
		if i < 5 {
			// Not yet eligible (grace period not passed)
			deleteAfterMs = now.Add(1 * time.Hour).UnixMilli()
		} else {
			// Eligible for deletion
			deleteAfterMs = now.Add(-1 * time.Hour).UnixMilli()
		}

		gcRecord := WALGCRecord{
			Path:          path,
			DeleteAfterMs: deleteAfterMs,
			CreatedAt:     now.Add(-2 * time.Hour).UnixMilli(),
			SizeBytes:     100,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		objStore.Put(ctx, path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		gcKey := keys.WALGCKeyPath(0, walID)
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	// Create worker with small BatchSize to force pagination
	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{
		NumDomains: 1,
		BatchSize:  batchSize,
	})

	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify that eligible records (5-9) were deleted
	for i := 5; i < 10; i++ {
		walID := "wal-" + string(rune('0'+i/10)) + string(rune('0'+i%10))
		gcKey := keys.WALGCKeyPath(0, walID)
		result, _ := metaStore.Get(ctx, gcKey)
		if result.Exists {
			t.Errorf("GC marker for %s should have been deleted (was eligible)", walID)
		}
		path := testWALPath(0, walID)
		if objStore.hasObject(path) {
			t.Errorf("Object for %s should have been deleted", walID)
		}
	}

	// Verify that non-eligible records (0-4) still exist
	for i := 0; i < 5; i++ {
		walID := "wal-" + string(rune('0'+i/10)) + string(rune('0'+i%10))
		gcKey := keys.WALGCKeyPath(0, walID)
		result, _ := metaStore.Get(ctx, gcKey)
		if !result.Exists {
			t.Errorf("GC marker for %s should NOT have been deleted (not yet eligible)", walID)
		}
		path := testWALPath(0, walID)
		if !objStore.hasObject(path) {
			t.Errorf("Object for %s should NOT have been deleted", walID)
		}
	}
}

func TestWALGCWorker_Pagination_AllPagesProcessed(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Use a batch size of 2 to force many pages
	batchSize := 2
	now := time.Now()

	// Create 7 eligible records - should require 4 pages (2+2+2+1)
	for i := 0; i < 7; i++ {
		walID := "wal-" + string(rune('a'+i))
		path := testWALPath(0, walID)

		gcRecord := WALGCRecord{
			Path:          path,
			DeleteAfterMs: now.Add(-1 * time.Hour).UnixMilli(),
			CreatedAt:     now.Add(-2 * time.Hour).UnixMilli(),
			SizeBytes:     100,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		objStore.Put(ctx, path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		gcKey := keys.WALGCKeyPath(0, walID)
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	worker := NewWALGCWorker(metaStore, objStore, WALGCWorkerConfig{
		NumDomains: 1,
		BatchSize:  batchSize,
	})

	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify all 7 records were deleted
	for i := 0; i < 7; i++ {
		walID := "wal-" + string(rune('a'+i))
		gcKey := keys.WALGCKeyPath(0, walID)
		result, _ := metaStore.Get(ctx, gcKey)
		if result.Exists {
			t.Errorf("GC marker for %s should have been deleted", walID)
		}
	}

	// Verify no pending records remain
	pending, err := worker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("GetPendingCount failed: %v", err)
	}
	if pending != 0 {
		t.Errorf("Expected 0 pending records, got %d", pending)
	}
}
