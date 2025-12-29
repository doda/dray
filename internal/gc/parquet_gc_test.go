package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

func TestParquetGCWorker_ScanOnce_DeletesEligibleObjects(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a GC record that's eligible for deletion (deleteAfterMs in the past)
	gcRecord := ParquetGCRecord{
		Path:          "streams/stream-001/parquet/parquet-001.parquet",
		DeleteAfterMs: time.Now().Add(-1 * time.Hour).UnixMilli(),
		CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
		SizeBytes:     1024000,
		StreamID:      "stream-001",
		JobID:         "job-123",
	}
	gcRecordBytes, err := json.Marshal(gcRecord)
	if err != nil {
		t.Fatal(err)
	}

	// Write the Parquet object to object store
	objStore.Put(ctx, gcRecord.Path, bytes.NewReader([]byte("test-parquet-data")), 17, "application/octet-stream")

	// Create the GC marker
	gcKey := keys.ParquetGCKeyPath("stream-001", "parquet-001")
	metaStore.Put(ctx, gcKey, gcRecordBytes)

	// Create the worker and run a single scan
	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the object was deleted
	if objStore.hasObject(gcRecord.Path) {
		t.Error("Parquet object should have been deleted from object storage")
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

func TestParquetGCWorker_ScanOnce_SkipsNonEligibleObjects(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a GC record that's NOT eligible (deleteAfterMs in the future)
	gcRecord := ParquetGCRecord{
		Path:          "streams/stream-002/parquet/parquet-002.parquet",
		DeleteAfterMs: time.Now().Add(1 * time.Hour).UnixMilli(),
		CreatedAt:     time.Now().Add(-10 * time.Minute).UnixMilli(),
		SizeBytes:     2048000,
		StreamID:      "stream-002",
	}
	gcRecordBytes, err := json.Marshal(gcRecord)
	if err != nil {
		t.Fatal(err)
	}

	// Write the Parquet object to object store
	objStore.Put(ctx, gcRecord.Path, bytes.NewReader([]byte("test-parquet-data-2")), 19, "application/octet-stream")

	// Create the GC marker
	gcKey := keys.ParquetGCKeyPath("stream-002", "parquet-002")
	metaStore.Put(ctx, gcKey, gcRecordBytes)

	// Create the worker and run a single scan
	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify the object was NOT deleted
	if !objStore.hasObject(gcRecord.Path) {
		t.Error("Parquet object should NOT have been deleted (grace period not passed)")
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

func TestParquetGCWorker_ScanOnce_MultipleStreams(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create GC records in multiple streams
	streams := []string{"stream-a", "stream-b", "stream-c"}
	for _, streamID := range streams {
		gcRecord := ParquetGCRecord{
			Path:          "streams/" + streamID + "/parquet/test.parquet",
			DeleteAfterMs: time.Now().Add(-30 * time.Minute).UnixMilli(),
			CreatedAt:     time.Now().Add(-1 * time.Hour).UnixMilli(),
			SizeBytes:     512000,
			StreamID:      streamID,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		objStore.Put(ctx, gcRecord.Path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		gcKey := keys.ParquetGCKeyPath(streamID, "test")
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	// Create the worker and run a single scan
	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	err := worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify all objects and markers were deleted
	for _, streamID := range streams {
		path := "streams/" + streamID + "/parquet/test.parquet"
		if objStore.hasObject(path) {
			t.Errorf("Parquet object for stream %s should have been deleted", streamID)
		}

		gcKey := keys.ParquetGCKeyPath(streamID, "test")
		result, _ := metaStore.Get(ctx, gcKey)
		if result.Exists {
			t.Errorf("GC marker for stream %s should have been deleted", streamID)
		}
	}
}

func TestParquetGCWorker_ScanOnce_HandlesAlreadyDeletedObject(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create a GC record pointing to an object that doesn't exist
	gcRecord := ParquetGCRecord{
		Path:          "streams/stream-001/parquet/missing.parquet",
		DeleteAfterMs: time.Now().Add(-1 * time.Hour).UnixMilli(),
		CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
		SizeBytes:     1024000,
		StreamID:      "stream-001",
	}
	gcRecordBytes, _ := json.Marshal(gcRecord)
	gcKey := keys.ParquetGCKeyPath("stream-001", "missing")
	metaStore.Put(ctx, gcKey, gcRecordBytes)

	// Create the worker and run a single scan
	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
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

func TestParquetGCWorker_ProcessEligible(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()
	streamID := "test-stream"

	// Create 3 eligible and 2 non-eligible GC records
	for i := 0; i < 5; i++ {
		parquetID := "parquet-" + string(rune('a'+i))
		path := "streams/" + streamID + "/parquet/" + parquetID + ".parquet"

		var deleteAfterMs int64
		if i < 3 {
			deleteAfterMs = time.Now().Add(-1 * time.Hour).UnixMilli() // Eligible
		} else {
			deleteAfterMs = time.Now().Add(1 * time.Hour).UnixMilli() // Not eligible
		}

		gcRecord := ParquetGCRecord{
			Path:          path,
			DeleteAfterMs: deleteAfterMs,
			CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
			SizeBytes:     100000,
			StreamID:      streamID,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		objStore.Put(ctx, path, bytes.NewReader([]byte("data")), 4, "application/octet-stream")
		gcKey := keys.ParquetGCKeyPath(streamID, parquetID)
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	deleted, err := worker.ProcessEligible(ctx, streamID)
	if err != nil {
		t.Fatalf("ProcessEligible failed: %v", err)
	}

	if deleted != 3 {
		t.Errorf("Expected 3 deleted records, got %d", deleted)
	}

	// Verify eligible ones were deleted
	for i := 0; i < 3; i++ {
		parquetID := "parquet-" + string(rune('a'+i))
		gcKey := keys.ParquetGCKeyPath(streamID, parquetID)
		result, _ := metaStore.Get(ctx, gcKey)
		if result.Exists {
			t.Errorf("GC marker for %s should have been deleted", parquetID)
		}
	}

	// Verify non-eligible ones still exist
	for i := 3; i < 5; i++ {
		parquetID := "parquet-" + string(rune('a'+i))
		gcKey := keys.ParquetGCKeyPath(streamID, parquetID)
		result, _ := metaStore.Get(ctx, gcKey)
		if !result.Exists {
			t.Errorf("GC marker for %s should NOT have been deleted", parquetID)
		}
	}
}

func TestParquetGCWorker_GetPendingCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create records in different streams
	streams := []string{"stream-1", "stream-2", "stream-3"}
	for _, streamID := range streams {
		for j := 0; j < 2; j++ {
			parquetID := "parquet-" + string(rune('a'+j))
			gcRecord := ParquetGCRecord{
				Path:          "streams/" + streamID + "/parquet/" + parquetID + ".parquet",
				DeleteAfterMs: time.Now().Add(-1 * time.Hour).UnixMilli(),
				CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
				SizeBytes:     100000,
				StreamID:      streamID,
			}
			gcRecordBytes, _ := json.Marshal(gcRecord)
			gcKey := keys.ParquetGCKeyPath(streamID, parquetID)
			metaStore.Put(ctx, gcKey, gcRecordBytes)
		}
	}

	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	count, err := worker.GetPendingCount(ctx)
	if err != nil {
		t.Fatalf("GetPendingCount failed: %v", err)
	}

	if count != 6 {
		t.Errorf("Expected 6 pending GC records, got %d", count)
	}
}

func TestParquetGCWorker_GetEligibleCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Create 3 eligible and 2 non-eligible records
	streamID := "test-stream"
	for i := 0; i < 5; i++ {
		parquetID := "parquet-" + string(rune('a'+i))
		var deleteAfterMs int64
		if i < 3 {
			deleteAfterMs = time.Now().Add(-1 * time.Hour).UnixMilli()
		} else {
			deleteAfterMs = time.Now().Add(1 * time.Hour).UnixMilli()
		}
		gcRecord := ParquetGCRecord{
			Path:          "streams/" + streamID + "/parquet/" + parquetID + ".parquet",
			DeleteAfterMs: deleteAfterMs,
			CreatedAt:     time.Now().Add(-2 * time.Hour).UnixMilli(),
			SizeBytes:     100000,
			StreamID:      streamID,
		}
		gcRecordBytes, _ := json.Marshal(gcRecord)
		gcKey := keys.ParquetGCKeyPath(streamID, parquetID)
		metaStore.Put(ctx, gcKey, gcRecordBytes)
	}

	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	count, err := worker.GetEligibleCount(ctx)
	if err != nil {
		t.Fatalf("GetEligibleCount failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 eligible GC records, got %d", count)
	}
}

func TestParquetGCWorker_StartStop(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{
		ScanIntervalMs: 100,
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

func TestParquetGCWorker_DefaultConfig(t *testing.T) {
	cfg := DefaultParquetGCWorkerConfig()
	if cfg.ScanIntervalMs <= 0 {
		t.Error("ScanIntervalMs should be positive")
	}
	if cfg.GracePeriodMs <= 0 {
		t.Error("GracePeriodMs should be positive")
	}
	if cfg.BatchSize <= 0 {
		t.Error("BatchSize should be positive")
	}
}

func TestParquetGCWorker_ConfigDefaults(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()

	// Create worker with zero config values - should use defaults
	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{})

	if worker.config.ScanIntervalMs <= 0 {
		t.Error("ScanIntervalMs should have been set to default")
	}
	if worker.config.GracePeriodMs <= 0 {
		t.Error("GracePeriodMs should have been set to default")
	}
	if worker.config.BatchSize <= 0 {
		t.Error("BatchSize should have been set to default")
	}
}

func TestScheduleParquetGC(t *testing.T) {
	metaStore := metadata.NewMockStore()
	ctx := context.Background()

	record := ParquetGCRecord{
		Path:          "streams/stream-001/parquet/parquet-file.parquet",
		DeleteAfterMs: time.Now().Add(10 * time.Minute).UnixMilli(),
		CreatedAt:     time.Now().UnixMilli(),
		SizeBytes:     1024000,
		StreamID:      "stream-001",
		JobID:         "job-456",
	}

	err := ScheduleParquetGC(ctx, metaStore, record)
	if err != nil {
		t.Fatalf("ScheduleParquetGC failed: %v", err)
	}

	// Verify the GC marker was created
	gcKey := keys.ParquetGCKeyPath("stream-001", "parquet-file")
	result, err := metaStore.Get(ctx, gcKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("GC marker should have been created")
	}

	// Verify the record content
	var storedRecord ParquetGCRecord
	if err := json.Unmarshal(result.Value, &storedRecord); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if storedRecord.Path != record.Path {
		t.Errorf("Path mismatch: got %s, want %s", storedRecord.Path, record.Path)
	}
	if storedRecord.JobID != record.JobID {
		t.Errorf("JobID mismatch: got %s, want %s", storedRecord.JobID, record.JobID)
	}
}

func TestExtractParquetID(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"streams/stream-001/parquet/file-123.parquet", "file-123"},
		{"path/to/my-file.parquet", "my-file"},
		{"simple.parquet", "simple"},
		{"no-extension", "no-extension"},
		{"/absolute/path/data.parquet", "data"},
		{"", ""},
	}

	for _, tt := range tests {
		result := extractParquetID(tt.path)
		if result != tt.expected {
			t.Errorf("extractParquetID(%q) = %q, want %q", tt.path, result, tt.expected)
		}
	}
}

func TestParquetGCWorker_IntegrationWithCompaction(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	// Simulate what happens when re-compaction replaces old Parquet files
	streamID := "stream-001"
	parquetPath := "streams/" + streamID + "/parquet/old-parquet-001.parquet"

	// Original Parquet file exists
	objStore.Put(ctx, parquetPath, bytes.NewReader([]byte("original-parquet-content")), 24, "application/octet-stream")

	// GC marker created by compaction swap (with a short grace period for testing)
	gcRecord := ParquetGCRecord{
		Path:          parquetPath,
		DeleteAfterMs: time.Now().Add(-1 * time.Second).UnixMilli(), // Already eligible
		CreatedAt:     time.Now().Add(-1 * time.Hour).UnixMilli(),
		SizeBytes:     24,
		StreamID:      streamID,
		JobID:         "recompaction-job-001",
	}

	err := ScheduleParquetGC(ctx, metaStore, gcRecord)
	if err != nil {
		t.Fatalf("ScheduleParquetGC failed: %v", err)
	}

	// GC worker scans and deletes
	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Object should be gone
	if objStore.hasObject(parquetPath) {
		t.Error("Parquet object should have been deleted by GC")
	}

	// Marker should be gone
	gcKey := keys.ParquetGCKeyPath(streamID, "old-parquet-001")
	result, _ := metaStore.Get(ctx, gcKey)
	if result.Exists {
		t.Error("GC marker should have been deleted")
	}
}

func TestParquetGCWorker_SkipsIcebergReferencedFile(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	streamID := "stream-iceberg"
	parquetPath := "streams/" + streamID + "/parquet/iceberg-file.parquet"

	objStore.Put(ctx, parquetPath, bytes.NewReader([]byte("iceberg-data")), 12, "application/octet-stream")

	gcRecord := ParquetGCRecord{
		Path:          parquetPath,
		DeleteAfterMs: time.Now().Add(-1 * time.Second).UnixMilli(),
		CreatedAt:     time.Now().Add(-1 * time.Hour).UnixMilli(),
		SizeBytes:     12,
		StreamID:      streamID,
		IcebergEnabled: true,
	}

	if err := ScheduleParquetGC(ctx, metaStore, gcRecord); err != nil {
		t.Fatalf("ScheduleParquetGC failed: %v", err)
	}

	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	if !objStore.hasObject(parquetPath) {
		t.Error("Parquet object should NOT be deleted before Iceberg removal")
	}

	gcKey := keys.ParquetGCKeyPath(streamID, "iceberg-file")
	result, _ := metaStore.Get(ctx, gcKey)
	if !result.Exists {
		t.Error("GC marker should still exist before Iceberg removal")
	}

	if err := ConfirmParquetIcebergRemoval(ctx, metaStore, streamID, parquetPath); err != nil {
		t.Fatalf("ConfirmParquetIcebergRemoval failed: %v", err)
	}

	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed after confirm: %v", err)
	}

	if objStore.hasObject(parquetPath) {
		t.Error("Parquet object should be deleted after Iceberg removal")
	}

	result, _ = metaStore.Get(ctx, gcKey)
	if result.Exists {
		t.Error("GC marker should be deleted after Iceberg removal")
	}
}

func TestParquetGCWorker_GracePeriodRespected(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	ctx := context.Background()

	streamID := "stream-001"
	parquetPath := "streams/" + streamID + "/parquet/recent.parquet"

	// Create object
	objStore.Put(ctx, parquetPath, bytes.NewReader([]byte("data")), 4, "application/octet-stream")

	// GC marker with future deleteAfterMs
	gcRecord := ParquetGCRecord{
		Path:          parquetPath,
		DeleteAfterMs: time.Now().Add(5 * time.Minute).UnixMilli(), // Not yet eligible
		CreatedAt:     time.Now().UnixMilli(),
		SizeBytes:     4,
		StreamID:      streamID,
	}

	err := ScheduleParquetGC(ctx, metaStore, gcRecord)
	if err != nil {
		t.Fatalf("ScheduleParquetGC failed: %v", err)
	}

	// Run GC - should not delete yet
	worker := NewParquetGCWorker(metaStore, objStore, ParquetGCWorkerConfig{BatchSize: 100})
	err = worker.ScanOnce(ctx)
	if err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Object should still exist
	if !objStore.hasObject(parquetPath) {
		t.Error("Parquet object should NOT have been deleted (grace period not passed)")
	}

	// Marker should still exist
	gcKey := keys.ParquetGCKeyPath(streamID, "recent")
	result, _ := metaStore.Get(ctx, gcKey)
	if !result.Exists {
		t.Error("GC marker should still exist (grace period not passed)")
	}
}
