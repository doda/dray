package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/wal"
)

const (
	testWALDate = "2025/01/02"
	testWALZone = "zone-a"
)

func testWALPath(domain int, walID string) string {
	return fmt.Sprintf("wal/v1/zone=%s/domain=%d/date=%s/%s.wo", testWALZone, domain, testWALDate, walID)
}

func TestWALOrphanGCWorker_ScanOnce_DeletesOrphanedObjects(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a staging marker that's orphaned (older than TTL)
	orphanTTLMs := int64(1000) // 1 second TTL for testing
	marker := wal.StagingMarker{
		Path:      testWALPath(0, "orphaned-wal-001"),
		CreatedAt: time.Now().Add(-2 * time.Hour).UnixMilli(), // Created 2 hours ago
		SizeBytes: 1024,
	}
	markerBytes, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}

	// Write the WAL object to object store
	objStore.Put(ctx, marker.Path, bytes.NewReader([]byte("orphaned-wal-data")), 17, "application/octet-stream")

	// Create the staging marker
	stagingKey := keys.WALStagingKeyPath(0, "orphaned-wal-001")
	metaStore.Put(ctx, stagingKey, markerBytes)

	// Create the worker with short TTL and run a single scan
	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: orphanTTLMs,
		NumDomains:  1,
		BatchSize:   100,
	})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the object was deleted
	if objStore.hasObject(marker.Path) {
		t.Error("Orphaned WAL object should have been deleted from object storage")
	}

	// Verify the staging marker was deleted
	result, err := metaStore.Get(ctx, stagingKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("Staging marker should have been deleted from metadata")
	}
}

func TestWALOrphanGCWorker_ScanOnce_SkipsRecentMarkers(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a staging marker that's NOT orphaned (created recently)
	orphanTTLMs := int64(3600000) // 1 hour TTL
	marker := wal.StagingMarker{
		Path:      testWALPath(0, "recent-wal-001"),
		CreatedAt: time.Now().Add(-10 * time.Minute).UnixMilli(), // Created 10 minutes ago
		SizeBytes: 2048,
	}
	markerBytes, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}

	// Write the WAL object to object store
	objStore.Put(ctx, marker.Path, bytes.NewReader([]byte("recent-wal-data")), 15, "application/octet-stream")

	// Create the staging marker
	stagingKey := keys.WALStagingKeyPath(0, "recent-wal-001")
	metaStore.Put(ctx, stagingKey, markerBytes)

	// Create the worker and run a single scan
	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: orphanTTLMs,
		NumDomains:  1,
		BatchSize:   100,
	})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the object was NOT deleted
	if !objStore.hasObject(marker.Path) {
		t.Error("WAL object should NOT have been deleted (TTL not passed)")
	}

	// Verify the staging marker still exists
	result, err := metaStore.Get(ctx, stagingKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("Staging marker should still exist (TTL not passed)")
	}
}

func TestWALOrphanGCWorker_ScanOnce_MultipleDomains(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	orphanTTLMs := int64(1000) // 1 second TTL

	// Create staging markers in multiple domains
	domains := []int{0, 1, 2}
	for _, domain := range domains {
		walID := "orphan-wal-" + string(rune('a'+domain))
		path := testWALPath(domain, walID)

		marker := wal.StagingMarker{
			Path:      path,
			CreatedAt: time.Now().Add(-1 * time.Hour).UnixMilli(),
			SizeBytes: 512,
		}
		markerBytes, _ := json.Marshal(marker)
		objStore.Put(ctx, path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		stagingKey := keys.WALStagingKeyPath(domain, walID)
		metaStore.Put(ctx, stagingKey, markerBytes)
	}

	// Create the worker and run a single scan
	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: orphanTTLMs,
		NumDomains:  3,
		BatchSize:   100,
	})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify all objects and markers were deleted
	for _, domain := range domains {
		walID := "orphan-wal-" + string(rune('a'+domain))
		path := testWALPath(domain, walID)
		if objStore.hasObject(path) {
			t.Errorf("WAL object in domain %d should have been deleted", domain)
		}

		stagingKey := keys.WALStagingKeyPath(domain, walID)
		result, _ := metaStore.Get(ctx, stagingKey)
		if result.Exists {
			t.Errorf("Staging marker in domain %d should have been deleted", domain)
		}
	}
}

func TestWALOrphanGCWorker_ScanOnce_HandlesAlreadyDeletedObject(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a staging marker pointing to an object that doesn't exist
	orphanTTLMs := int64(1000)
	marker := wal.StagingMarker{
		Path:      testWALPath(0, "missing-wal"),
		CreatedAt: time.Now().Add(-1 * time.Hour).UnixMilli(),
		SizeBytes: 1024,
	}
	markerBytes, _ := json.Marshal(marker)
	stagingKey := keys.WALStagingKeyPath(0, "missing-wal")
	metaStore.Put(ctx, stagingKey, markerBytes)

	// Create the worker and run a single scan
	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: orphanTTLMs,
		NumDomains:  1,
		BatchSize:   100,
	})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the staging marker was still deleted
	result, err := metaStore.Get(ctx, stagingKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("Staging marker should have been deleted even if object was already gone")
	}
}

func TestWALOrphanGCWorker_ProcessOrphans(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	orphanTTLMs := int64(30 * 60 * 1000) // 30 minutes

	// Create 3 orphaned and 2 non-orphaned staging markers
	for i := 0; i < 5; i++ {
		walID := "test-wal-" + string(rune('a'+i))
		path := testWALPath(0, walID)

		var createdAt int64
		if i < 3 {
			createdAt = time.Now().Add(-1 * time.Hour).UnixMilli() // Orphaned (1 hour old)
		} else {
			createdAt = time.Now().Add(-10 * time.Minute).UnixMilli() // Not orphaned (10 min old)
		}

		marker := wal.StagingMarker{
			Path:      path,
			CreatedAt: createdAt,
			SizeBytes: 100,
		}
		markerBytes, _ := json.Marshal(marker)
		objStore.Put(ctx, path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		stagingKey := keys.WALStagingKeyPath(0, walID)
		metaStore.Put(ctx, stagingKey, markerBytes)
	}

	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: orphanTTLMs,
		NumDomains:  1,
		BatchSize:   100,
	})
	deleted, err := worker.ProcessOrphans(ctx, 0)
	if err != nil {
		t.Fatalf("ProcessOrphans failed: %v", err)
	}

	if deleted != 3 {
		t.Errorf("Expected 3 deleted orphans, got %d", deleted)
	}

	// Verify orphaned ones were deleted
	for i := 0; i < 3; i++ {
		walID := "test-wal-" + string(rune('a'+i))
		stagingKey := keys.WALStagingKeyPath(0, walID)
		result, _ := metaStore.Get(ctx, stagingKey)
		if result.Exists {
			t.Errorf("Staging marker for %s should have been deleted", walID)
		}
	}

	// Verify non-orphaned ones still exist
	for i := 3; i < 5; i++ {
		walID := "test-wal-" + string(rune('a'+i))
		stagingKey := keys.WALStagingKeyPath(0, walID)
		result, _ := metaStore.Get(ctx, stagingKey)
		if !result.Exists {
			t.Errorf("Staging marker for %s should NOT have been deleted", walID)
		}
	}
}

func TestWALOrphanGCWorker_GetStagingCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create staging markers in different domains
	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			walID := "wal-" + string(rune('0'+i)) + "-" + string(rune('a'+j))
			marker := wal.StagingMarker{
				Path:      testWALPath(i, walID),
				CreatedAt: time.Now().Add(-1 * time.Hour).UnixMilli(),
				SizeBytes: 100,
			}
			markerBytes, _ := json.Marshal(marker)
			stagingKey := keys.WALStagingKeyPath(i, walID)
			metaStore.Put(ctx, stagingKey, markerBytes)
		}
	}

	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		NumDomains: 3,
		BatchSize:  100,
	})
	count, err := worker.GetStagingCount(ctx)
	if err != nil {
		t.Fatalf("GetStagingCount failed: %v", err)
	}

	if count != 6 {
		t.Errorf("Expected 6 staging markers, got %d", count)
	}
}

func TestWALOrphanGCWorker_GetOrphanCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	orphanTTLMs := int64(30 * 60 * 1000) // 30 minutes

	// Create 3 orphaned and 2 non-orphaned staging markers
	for i := 0; i < 5; i++ {
		walID := "wal-" + string(rune('a'+i))
		var createdAt int64
		if i < 3 {
			createdAt = time.Now().Add(-1 * time.Hour).UnixMilli() // Orphaned
		} else {
			createdAt = time.Now().Add(-10 * time.Minute).UnixMilli() // Not orphaned
		}
		marker := wal.StagingMarker{
			Path:      testWALPath(0, walID),
			CreatedAt: createdAt,
			SizeBytes: 100,
		}
		markerBytes, _ := json.Marshal(marker)
		stagingKey := keys.WALStagingKeyPath(0, walID)
		metaStore.Put(ctx, stagingKey, markerBytes)
	}

	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: orphanTTLMs,
		NumDomains:  1,
		BatchSize:   100,
	})
	count, err := worker.GetOrphanCount(ctx)
	if err != nil {
		t.Fatalf("GetOrphanCount failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 orphaned markers, got %d", count)
	}
}

func TestWALOrphanGCWorker_StartStop(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		ScanIntervalMs: 100,
		OrphanTTLMs:    1000,
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

func TestWALOrphanGCWorker_DefaultConfig(t *testing.T) {
	cfg := DefaultWALOrphanGCWorkerConfig()
	if cfg.ScanIntervalMs <= 0 {
		t.Error("ScanIntervalMs should be positive")
	}
	if cfg.OrphanTTLMs <= 0 {
		t.Error("OrphanTTLMs should be positive")
	}
	if cfg.NumDomains <= 0 {
		t.Error("NumDomains should be positive")
	}
	if cfg.BatchSize <= 0 {
		t.Error("BatchSize should be positive")
	}

	// Verify 24 hour default per spec
	expectedTTL := int64(86400000) // 24 hours in ms
	if cfg.OrphanTTLMs != expectedTTL {
		t.Errorf("OrphanTTLMs should default to 24 hours (%d), got %d", expectedTTL, cfg.OrphanTTLMs)
	}
}

func TestWALOrphanGCWorker_ConfigDefaults(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	// Create worker with zero config values - should use defaults
	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{})

	if worker.config.ScanIntervalMs <= 0 {
		t.Error("ScanIntervalMs should have been set to default")
	}
	if worker.config.OrphanTTLMs <= 0 {
		t.Error("OrphanTTLMs should have been set to default")
	}
	if worker.config.NumDomains <= 0 {
		t.Error("NumDomains should have been set to default")
	}
	if worker.config.BatchSize <= 0 {
		t.Error("BatchSize should have been set to default")
	}
}

func TestWALOrphanGCWorker_IntegrationWithStagingWriter(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Simulate what happens when a broker crashes after writing WAL but before commit:
	// 1. Staging marker is created
	// 2. WAL object is written to object storage
	// 3. Broker crashes - commit transaction never completes
	// 4. Staging marker remains, indicating an orphaned WAL

	walID := "crash-orphan-001"
	walPath := testWALPath(0, walID)

	// Object was written before crash
	objStore.Put(ctx, walPath, bytes.NewReader([]byte("wal-content-before-crash")), 24, "application/octet-stream")

	// Staging marker was created before crash (old timestamp to simulate age)
	marker := wal.StagingMarker{
		Path:      walPath,
		CreatedAt: time.Now().Add(-2 * time.Hour).UnixMilli(), // 2 hours ago
		SizeBytes: 24,
	}
	markerBytes, _ := json.Marshal(marker)
	stagingKey := keys.WALStagingKeyPath(0, walID)
	metaStore.Put(ctx, stagingKey, markerBytes)

	// Orphan GC worker scans and cleans up
	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: 3600000, // 1 hour TTL
		NumDomains:  1,
		BatchSize:   100,
	})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Object should be gone
	if objStore.hasObject(walPath) {
		t.Error("Orphaned WAL object should have been deleted by GC")
	}

	// Staging marker should be gone
	result, _ := metaStore.Get(ctx, stagingKey)
	if result.Exists {
		t.Error("Staging marker should have been deleted")
	}
}

func TestWALOrphanGCWorker_DoesNotDeleteCommittedWALs(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// When a WAL is successfully committed:
	// 1. Staging marker is deleted in the commit transaction
	// 2. WAL object record is created with refcount
	// 3. Only the orphan GC should not touch committed WALs (no staging marker exists)

	walID := "committed-wal-001"
	walPath := testWALPath(0, walID)

	// Object was written and committed
	objStore.Put(ctx, walPath, bytes.NewReader([]byte("committed-wal-content")), 21, "application/octet-stream")

	// Staging marker was deleted in commit - so no staging key exists

	// Create an old WAL object record (simulating normal GC flow)
	walObjKey := keys.WALObjectKeyPath(0, walID)
	metaStore.Put(ctx, walObjKey, []byte(`{"path":"`+testWALPath(0, "committed-wal-001")+`","refCount":1}`))

	// Orphan GC worker scans
	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: 1000, // Short TTL
		NumDomains:  1,
		BatchSize:   100,
	})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Object should still exist (it's committed, not orphaned)
	if !objStore.hasObject(walPath) {
		t.Error("Committed WAL object should NOT have been deleted (no staging marker = not orphaned)")
	}

	// WAL object record should still exist
	result, _ := metaStore.Get(ctx, walObjKey)
	if !result.Exists {
		t.Error("WAL object record should still exist")
	}
}

func TestWALOrphanGCWorker_MixedOrphansAndRecent(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	orphanTTLMs := int64(30 * 60 * 1000) // 30 minutes

	// Create a mix: some orphaned, some recent
	testCases := []struct {
		walID      string
		createdAgo time.Duration
		isOrphaned bool
	}{
		{"old-orphan-1", 2 * time.Hour, true},
		{"recent-1", 5 * time.Minute, false},
		{"old-orphan-2", 45 * time.Minute, true},
		{"recent-2", 10 * time.Minute, false},
		{"old-orphan-3", 1 * time.Hour, true},
	}

	for _, tc := range testCases {
		path := testWALPath(0, tc.walID)
		marker := wal.StagingMarker{
			Path:      path,
			CreatedAt: time.Now().Add(-tc.createdAgo).UnixMilli(),
			SizeBytes: 100,
		}
		markerBytes, _ := json.Marshal(marker)
		objStore.Put(ctx, path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		stagingKey := keys.WALStagingKeyPath(0, tc.walID)
		metaStore.Put(ctx, stagingKey, markerBytes)
	}

	worker := NewWALOrphanGCWorker(metaStore, objStore, WALOrphanGCWorkerConfig{
		OrphanTTLMs: orphanTTLMs,
		NumDomains:  1,
		BatchSize:   100,
	})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify expected outcomes
	for _, tc := range testCases {
		path := testWALPath(0, tc.walID)
		stagingKey := keys.WALStagingKeyPath(0, tc.walID)

		objExists := objStore.hasObject(path)
		result, _ := metaStore.Get(ctx, stagingKey)
		markerExists := result.Exists

		if tc.isOrphaned {
			if objExists {
				t.Errorf("%s: object should have been deleted (orphaned)", tc.walID)
			}
			if markerExists {
				t.Errorf("%s: staging marker should have been deleted (orphaned)", tc.walID)
			}
		} else {
			if !objExists {
				t.Errorf("%s: object should NOT have been deleted (recent)", tc.walID)
			}
			if !markerExists {
				t.Errorf("%s: staging marker should NOT have been deleted (recent)", tc.walID)
			}
		}
	}
}
