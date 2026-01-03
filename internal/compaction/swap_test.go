package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/produce"
)

type conflictOnceStore struct {
	metadata.MetadataStore
	mu                 sync.Mutex
	conflictsRemaining int
}

func (s *conflictOnceStore) Txn(ctx context.Context, scopeKey string, fn func(metadata.Txn) error) error {
	s.mu.Lock()
	if s.conflictsRemaining > 0 {
		s.conflictsRemaining--
		s.mu.Unlock()
		return metadata.ErrTxnConflict
	}
	s.mu.Unlock()
	return s.MetadataStore.Txn(ctx, scopeKey, fn)
}

func TestIndexSwapper_Swap_SingleWALEntry(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-1"
	metaDomain := 1
	walID := "wal-001"

	// Create a WAL index entry
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1000,
		CreatedAtMs:    time.Now().UnixMilli(),
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		MessageCount:   100,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          walID,
		WalPath:        "/wal/domain-1/wal-001.wal",
		ChunkOffset:    0,
		ChunkLength:    1000,
	}

	walEntryBytes, _ := json.Marshal(walEntry)
	walIndexKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walIndexKey, walEntryBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)
	walRecord := produce.WALObjectRecord{
		Path:      walEntry.WalPath,
		RefCount:  1,
		CreatedAt: time.Now().UnixMilli(),
		SizeBytes: 1000,
	}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	// Create Parquet entry to replace
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/stream-1/compact-001.parquet",
		ParquetSizeBytes: 500,
	}

	// Execute swap
	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{walIndexKey},
		ParquetEntry: parquetEntry,
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	// Verify old WAL entry was deleted
	walResult, _ := meta.Get(ctx, walIndexKey)
	if walResult.Exists {
		t.Error("WAL index entry should be deleted")
	}

	// Verify new Parquet entry exists
	parquetResult, err := meta.Get(ctx, result.NewIndexKey)
	if err != nil {
		t.Fatalf("Failed to get Parquet entry: %v", err)
	}
	if !parquetResult.Exists {
		t.Fatal("Parquet index entry should exist")
	}

	var storedEntry index.IndexEntry
	if err := json.Unmarshal(parquetResult.Value, &storedEntry); err != nil {
		t.Fatalf("Failed to unmarshal Parquet entry: %v", err)
	}

	if storedEntry.FileType != index.FileTypeParquet {
		t.Errorf("Expected FileType PARQUET, got %s", storedEntry.FileType)
	}
	if storedEntry.StartOffset != 0 {
		t.Errorf("Expected StartOffset 0, got %d", storedEntry.StartOffset)
	}
	if storedEntry.EndOffset != 100 {
		t.Errorf("Expected EndOffset 100, got %d", storedEntry.EndOffset)
	}
	if storedEntry.CumulativeSize != 500 { // Should be ParquetSizeBytes since no previous entry
		t.Errorf("Expected CumulativeSize 500, got %d", storedEntry.CumulativeSize)
	}

	// Verify WAL object refcount was decremented and moved to GC
	walObjResult, _ := meta.Get(ctx, walObjectKey)
	if walObjResult.Exists {
		t.Error("WAL object record should be deleted (moved to GC)")
	}

	gcKey := keys.WALGCKeyPath(metaDomain, walID)
	gcResult, _ := meta.Get(ctx, gcKey)
	if !gcResult.Exists {
		t.Error("WAL GC record should exist")
	}

	if len(result.WALObjectsReadyForGC) != 1 {
		t.Errorf("Expected 1 WAL object ready for GC, got %d", len(result.WALObjectsReadyForGC))
	}
}

func TestIndexSwapper_Swap_RetryPreservesRefcount(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	store := &conflictOnceStore{MetadataStore: meta, conflictsRemaining: 1}
	swapper := NewIndexSwapper(store)

	streamID := "test-stream-retry"
	metaDomain := 2
	walID := "wal-retry-001"

	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      10,
		CumulativeSize: 100,
		CreatedAtMs:    time.Now().UnixMilli(),
		FileType:       index.FileTypeWAL,
		RecordCount:    10,
		MessageCount:   10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          walID,
		WalPath:        "/wal/domain-2/wal-retry-001.wal",
		ChunkOffset:    0,
		ChunkLength:    100,
	}

	walEntryBytes, _ := json.Marshal(walEntry)
	walIndexKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walIndexKey, walEntryBytes)

	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)
	walRecord := produce.WALObjectRecord{
		Path:      walEntry.WalPath,
		RefCount:  2,
		CreatedAt: time.Now().UnixMilli(),
		SizeBytes: 100,
	}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        10,
		FileType:         index.FileTypeParquet,
		RecordCount:      10,
		MessageCount:     10,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/stream-retry/compact-001.parquet",
		ParquetSizeBytes: 50,
	}

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{walIndexKey},
		ParquetEntry: parquetEntry,
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("Swap failed after retry: %v", err)
	}

	walResult, _ := meta.Get(ctx, walIndexKey)
	if walResult.Exists {
		t.Error("WAL index entry should be deleted after swap")
	}

	walObjResult, _ := meta.Get(ctx, walObjectKey)
	if !walObjResult.Exists {
		t.Fatal("WAL object record should still exist with refcount 1")
	}

	var record produce.WALObjectRecord
	if err := json.Unmarshal(walObjResult.Value, &record); err != nil {
		t.Fatalf("Failed to unmarshal WAL object record: %v", err)
	}
	if record.RefCount != 1 {
		t.Errorf("Expected WAL refcount 1 after retry swap, got %d", record.RefCount)
	}
}

func TestIndexSwapper_Swap_MultipleWALEntries(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-2"
	metaDomain := 1

	// Create multiple contiguous WAL entries
	walEntries := []index.IndexEntry{
		{
			StreamID:       streamID,
			StartOffset:    0,
			EndOffset:      50,
			CumulativeSize: 500,
			FileType:       index.FileTypeWAL,
			RecordCount:    50,
			MinTimestampMs: 1000,
			MaxTimestampMs: 1500,
			WalID:          "wal-001",
			WalPath:        "/wal/wal-001.wal",
		},
		{
			StreamID:       streamID,
			StartOffset:    50,
			EndOffset:      100,
			CumulativeSize: 1000,
			FileType:       index.FileTypeWAL,
			RecordCount:    50,
			MinTimestampMs: 1500,
			MaxTimestampMs: 2000,
			WalID:          "wal-002",
			WalPath:        "/wal/wal-002.wal",
		},
		{
			StreamID:       streamID,
			StartOffset:    100,
			EndOffset:      150,
			CumulativeSize: 1500,
			FileType:       index.FileTypeWAL,
			RecordCount:    50,
			MinTimestampMs: 2000,
			MaxTimestampMs: 2500,
			WalID:          "wal-003",
			WalPath:        "/wal/wal-003.wal",
		},
	}

	var walIndexKeys []string
	for _, entry := range walEntries {
		entryBytes, _ := json.Marshal(entry)
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		meta.Put(ctx, key, entryBytes)
		walIndexKeys = append(walIndexKeys, key)

		// Create WAL object record with refcount > 1 to test partial decrement
		walObjectKey := keys.WALObjectKeyPath(metaDomain, entry.WalID)
		walRecord := produce.WALObjectRecord{
			Path:      entry.WalPath,
			RefCount:  2, // Multiple streams reference this WAL
			CreatedAt: time.Now().UnixMilli(),
			SizeBytes: 500,
		}
		walRecordBytes, _ := json.Marshal(walRecord)
		meta.Put(ctx, walObjectKey, walRecordBytes)
	}

	// Create Parquet entry covering all three
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        150,
		FileType:         index.FileTypeParquet,
		RecordCount:      150,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2500,
		ParquetPath:      "/parquet/compact.parquet",
		ParquetSizeBytes: 800,
	}

	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	// Verify all old entries deleted
	for _, key := range walIndexKeys {
		r, _ := meta.Get(ctx, key)
		if r.Exists {
			t.Errorf("WAL entry %s should be deleted", key)
		}
	}

	// Verify new Parquet entry
	parquetResult, _ := meta.Get(ctx, result.NewIndexKey)
	if !parquetResult.Exists {
		t.Fatal("Parquet entry should exist")
	}

	var stored index.IndexEntry
	json.Unmarshal(parquetResult.Value, &stored)

	if stored.StartOffset != 0 || stored.EndOffset != 150 {
		t.Errorf("Unexpected offset range: %d-%d", stored.StartOffset, stored.EndOffset)
	}

	// Verify WAL refcounts decremented but not GC'd (refcount was 2)
	for _, entry := range walEntries {
		walObjectKey := keys.WALObjectKeyPath(metaDomain, entry.WalID)
		r, _ := meta.Get(ctx, walObjectKey)
		if !r.Exists {
			t.Errorf("WAL object %s should still exist (refcount was 2)", entry.WalID)
			continue
		}

		var record produce.WALObjectRecord
		json.Unmarshal(r.Value, &record)
		if record.RefCount != 1 {
			t.Errorf("WAL %s: expected refcount 1, got %d", entry.WalID, record.RefCount)
		}
	}

	if len(result.DecrementedWALObjects) != 3 {
		t.Errorf("Expected 3 decremented WAL objects, got %d", len(result.DecrementedWALObjects))
	}
	if len(result.WALObjectsReadyForGC) != 0 {
		t.Errorf("Expected 0 WAL objects ready for GC, got %d", len(result.WALObjectsReadyForGC))
	}
}

func TestIndexSwapper_Swap_DoesNotScheduleParquetGCBeforeDone(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-parquet-gc"

	oldParquet := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/test-stream-parquet-gc/old-001.parquet",
		ParquetSizeBytes: 400,
	}
	oldParquetBytes, _ := json.Marshal(oldParquet)
	oldParquetKey, _ := keys.OffsetIndexKeyPath(streamID, oldParquet.EndOffset, int64(oldParquet.ParquetSizeBytes))
	meta.Put(ctx, oldParquetKey, oldParquetBytes)

	newParquet := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/test-stream-parquet-gc/new-002.parquet",
		ParquetSizeBytes: 300,
	}

	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:         streamID,
		ParquetIndexKeys: []string{oldParquetKey},
		ParquetEntry:     newParquet,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	if len(result.ParquetGCCandidates) != 1 {
		t.Fatalf("expected 1 Parquet GC candidate, got %d", len(result.ParquetGCCandidates))
	}
	if result.ParquetGCCandidates[0].Path != oldParquet.ParquetPath {
		t.Fatalf("expected Parquet GC candidate %s, got %s", oldParquet.ParquetPath, result.ParquetGCCandidates[0].Path)
	}
	if result.ParquetGCGracePeriodMs <= 0 {
		t.Fatalf("expected Parquet GC grace period to be set, got %d", result.ParquetGCGracePeriodMs)
	}

	gcKey := keys.ParquetGCKeyPath(streamID, "old-001")
	gcResult, _ := meta.Get(ctx, gcKey)
	if gcResult.Exists {
		t.Error("Parquet GC record should not be created before job reaches DONE")
	}
}

func TestIndexSwapper_Swap_CumulativeSizeWithPreviousEntry(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-3"

	// Create a previous entry that should not be affected
	prevEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1000,
		FileType:       index.FileTypeParquet, // Already compacted
		RecordCount:    100,
	}
	prevBytes, _ := json.Marshal(prevEntry)
	prevKey, _ := keys.OffsetIndexKeyPath(streamID, prevEntry.EndOffset, prevEntry.CumulativeSize)
	meta.Put(ctx, prevKey, prevBytes)

	// Create a WAL entry after the previous one
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    100,
		EndOffset:      200,
		CumulativeSize: 2000,
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		WalID:          "wal-after",
	}
	walBytes, _ := json.Marshal(walEntry)
	walKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walKey, walBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(0, walEntry.WalID)
	walRecord := produce.WALObjectRecord{RefCount: 1}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	// Create Parquet entry to replace
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      100,
		EndOffset:        200,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		ParquetSizeBytes: 600,
	}

	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{walKey},
		ParquetEntry: parquetEntry,
		MetaDomain:   0,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	// Verify cumulative size is based on previous entry
	parquetResult, _ := meta.Get(ctx, result.NewIndexKey)
	var stored index.IndexEntry
	json.Unmarshal(parquetResult.Value, &stored)

	expectedCumulativeSize := prevEntry.CumulativeSize + int64(parquetEntry.ParquetSizeBytes)
	if stored.CumulativeSize != expectedCumulativeSize {
		t.Errorf("Expected CumulativeSize %d (prev %d + new %d), got %d",
			expectedCumulativeSize, prevEntry.CumulativeSize, parquetEntry.ParquetSizeBytes, stored.CumulativeSize)
	}

	// Verify previous entry is unchanged
	prevResult, _ := meta.Get(ctx, prevKey)
	if !prevResult.Exists {
		t.Fatal("Previous entry should still exist")
	}
}

func TestIndexSwapper_Swap_OffsetMismatch(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-4"

	walEntry := index.IndexEntry{
		StreamID:    streamID,
		StartOffset: 0,
		EndOffset:   100,
		FileType:    index.FileTypeWAL,
	}
	walBytes, _ := json.Marshal(walEntry)
	walKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, 1000)
	meta.Put(ctx, walKey, walBytes)

	// Parquet entry with mismatched offsets
	parquetEntry := index.IndexEntry{
		StreamID:    streamID,
		StartOffset: 0,
		EndOffset:   50, // Different from WAL entry!
		FileType:    index.FileTypeParquet,
	}

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{walKey},
		ParquetEntry: parquetEntry,
	})

	if err == nil {
		t.Fatal("Expected error for offset mismatch")
	}
	if err.Error() == "" || !containsString(err.Error(), "mismatch") {
		t.Errorf("Expected offset mismatch error, got: %v", err)
	}
}

func TestIndexSwapper_Swap_GapInWALEntries(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-5"

	// Create non-contiguous WAL entries (gap between 50 and 60)
	entry1 := index.IndexEntry{
		StreamID:    streamID,
		StartOffset: 0,
		EndOffset:   50,
		FileType:    index.FileTypeWAL,
	}
	entry2 := index.IndexEntry{
		StreamID:    streamID,
		StartOffset: 60, // Gap!
		EndOffset:   100,
		FileType:    index.FileTypeWAL,
	}

	entry1Bytes, _ := json.Marshal(entry1)
	entry2Bytes, _ := json.Marshal(entry2)
	key1, _ := keys.OffsetIndexKeyPath(streamID, entry1.EndOffset, 500)
	key2, _ := keys.OffsetIndexKeyPath(streamID, entry2.EndOffset, 1000)
	meta.Put(ctx, key1, entry1Bytes)
	meta.Put(ctx, key2, entry2Bytes)

	parquetEntry := index.IndexEntry{
		StreamID:    streamID,
		StartOffset: 0,
		EndOffset:   100,
		FileType:    index.FileTypeParquet,
	}

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{key1, key2},
		ParquetEntry: parquetEntry,
	})

	if err == nil {
		t.Fatal("Expected error for gap in WAL entries")
	}
}

func TestIndexSwapper_Swap_EmptyRequest(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     "test-stream",
		WALIndexKeys: []string{}, // Empty!
		ParquetEntry: index.IndexEntry{},
	})

	if err != ErrNoEntriesToSwap {
		t.Errorf("Expected ErrNoEntriesToSwap, got: %v", err)
	}
}

func TestIndexSwapper_Swap_InvalidStreamID(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     "", // Empty!
		WALIndexKeys: []string{"key"},
		ParquetEntry: index.IndexEntry{},
	})

	if err != ErrInvalidStreamID {
		t.Errorf("Expected ErrInvalidStreamID, got: %v", err)
	}
}

func TestIndexSwapper_Swap_WALEntryNotFound(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     "test-stream",
		WALIndexKeys: []string{"/nonexistent/key"},
		ParquetEntry: index.IndexEntry{},
	})

	if err == nil {
		t.Fatal("Expected error for nonexistent WAL entry")
	}
}

func TestIndexSwapper_Swap_NotWALEntry(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream"

	// Create a Parquet entry instead of WAL
	entry := index.IndexEntry{
		StreamID:    streamID,
		StartOffset: 0,
		EndOffset:   100,
		FileType:    index.FileTypeParquet, // Not WAL!
	}
	entryBytes, _ := json.Marshal(entry)
	key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, 1000)
	meta.Put(ctx, key, entryBytes)

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{key},
		ParquetEntry: index.IndexEntry{StartOffset: 0, EndOffset: 100},
	})

	if err == nil {
		t.Fatal("Expected error for non-WAL entry")
	}
}

func TestIndexSwapper_SwapFromJob(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-job"

	// Create WAL entries within job's source range
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1000,
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-job-001",
	}
	walBytes, _ := json.Marshal(walEntry)
	walKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walKey, walBytes)

	// Create WAL object record
	metaDomain := 7
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walEntry.WalID)
	walRecord := produce.WALObjectRecord{RefCount: 1}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	job := &Job{
		StreamID:          streamID,
		SourceStartOffset: 0,
		SourceEndOffset:   100,
	}

	result, err := swapper.SwapFromJob(ctx, job, "/parquet/job-output.parquet", 500, 100, metaDomain)
	if err != nil {
		t.Fatalf("SwapFromJob failed: %v", err)
	}

	if result.NewIndexKey == "" {
		t.Error("Expected non-empty NewIndexKey")
	}

	// Verify the old WAL entry was deleted
	walResult, _ := meta.Get(ctx, walKey)
	if walResult.Exists {
		t.Error("WAL entry should be deleted")
	}

	// Verify new Parquet entry
	parquetResult, _ := meta.Get(ctx, result.NewIndexKey)
	if !parquetResult.Exists {
		t.Fatal("Parquet entry should exist")
	}

	var stored index.IndexEntry
	json.Unmarshal(parquetResult.Value, &stored)

	if stored.FileType != index.FileTypeParquet {
		t.Errorf("Expected PARQUET type, got %s", stored.FileType)
	}
	if stored.ParquetPath != "/parquet/job-output.parquet" {
		t.Errorf("Expected path /parquet/job-output.parquet, got %s", stored.ParquetPath)
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestIndexSwapper_Swap_ParquetRewrite_IcebergEnabled(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-iceberg-rewrite"

	oldParquet := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/test-stream-iceberg-rewrite/old-parquet.parquet",
		ParquetSizeBytes: 400,
	}
	oldParquetBytes, _ := json.Marshal(oldParquet)
	oldParquetKey, _ := keys.OffsetIndexKeyPath(streamID, oldParquet.EndOffset, int64(oldParquet.ParquetSizeBytes))
	meta.Put(ctx, oldParquetKey, oldParquetBytes)

	newParquet := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/test-stream-iceberg-rewrite/new-parquet.parquet",
		ParquetSizeBytes: 300,
	}

	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:         streamID,
		ParquetIndexKeys: []string{oldParquetKey},
		ParquetEntry:     newParquet,
		IcebergEnabled:   true, // Iceberg is enabled for this stream
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	if len(result.ParquetGCCandidates) != 1 {
		t.Fatalf("expected 1 Parquet GC candidate, got %d", len(result.ParquetGCCandidates))
	}

	candidate := result.ParquetGCCandidates[0]
	if candidate.Path != oldParquet.ParquetPath {
		t.Errorf("expected GC candidate path %s, got %s", oldParquet.ParquetPath, candidate.Path)
	}
	if !candidate.IcebergEnabled {
		t.Error("expected GC candidate IcebergEnabled to be true when swap has IcebergEnabled=true")
	}
	if candidate.IcebergRemovalConfirmed {
		t.Error("expected GC candidate IcebergRemovalConfirmed to be false initially")
	}
}

func TestIndexSwapper_Swap_ParquetRewrite_IcebergDisabled(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-no-iceberg-rewrite"

	oldParquet := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/test-stream-no-iceberg-rewrite/old-parquet.parquet",
		ParquetSizeBytes: 400,
	}
	oldParquetBytes, _ := json.Marshal(oldParquet)
	oldParquetKey, _ := keys.OffsetIndexKeyPath(streamID, oldParquet.EndOffset, int64(oldParquet.ParquetSizeBytes))
	meta.Put(ctx, oldParquetKey, oldParquetBytes)

	newParquet := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/test-stream-no-iceberg-rewrite/new-parquet.parquet",
		ParquetSizeBytes: 300,
	}

	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:         streamID,
		ParquetIndexKeys: []string{oldParquetKey},
		ParquetEntry:     newParquet,
		IcebergEnabled:   false, // Iceberg is NOT enabled for this stream
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	if len(result.ParquetGCCandidates) != 1 {
		t.Fatalf("expected 1 Parquet GC candidate, got %d", len(result.ParquetGCCandidates))
	}

	candidate := result.ParquetGCCandidates[0]
	if candidate.IcebergEnabled {
		t.Error("expected GC candidate IcebergEnabled to be false when swap has IcebergEnabled=false")
	}
}

// listCallTrackingStore wraps a MetadataStore and records all List calls
// to verify that the implementation uses bounded queries.
type listCallTrackingStore struct {
	metadata.MetadataStore
	mu        sync.Mutex
	listCalls []listCallRecord
}

type listCallRecord struct {
	StartKey    string
	EndKey      string
	Limit       int
	ResultCount int
}

func (s *listCallTrackingStore) List(ctx context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	result, err := s.MetadataStore.List(ctx, startKey, endKey, limit)
	s.mu.Lock()
	s.listCalls = append(s.listCalls, listCallRecord{
		StartKey:    startKey,
		EndKey:      endKey,
		Limit:       limit,
		ResultCount: len(result),
	})
	s.mu.Unlock()
	return result, err
}

func (s *listCallTrackingStore) getListCalls() []listCallRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]listCallRecord{}, s.listCalls...)
}

// TestIndexSwapper_BoundedListCalls verifies that the swapper uses targeted range queries
// instead of O(N) full index scans even with large indexes.
func TestIndexSwapper_BoundedListCalls(t *testing.T) {
	ctx := context.Background()
	baseMeta := metadata.NewMockStore()
	meta := &listCallTrackingStore{MetadataStore: baseMeta}
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-bounded-list"
	metaDomain := 1

	// Create a large number of entries to simulate a large index.
	// This is to verify that the swapper doesn't scan all entries.
	numEntries := 1000
	entrySize := 100

	for i := 0; i < numEntries; i++ {
		startOffset := int64(i * 100)
		endOffset := int64((i + 1) * 100)
		cumulativeSize := int64((i + 1) * entrySize)

		entry := index.IndexEntry{
			StreamID:       streamID,
			StartOffset:    startOffset,
			EndOffset:      endOffset,
			CumulativeSize: cumulativeSize,
			FileType:       index.FileTypeWAL,
			RecordCount:    100,
			MessageCount:   100,
			MinTimestampMs: 1000 + int64(i*1000),
			MaxTimestampMs: 2000 + int64(i*1000),
			WalID:          fmt.Sprintf("wal-%04d", i),
			WalPath:        fmt.Sprintf("/wal/domain-1/wal-%04d.wal", i),
			ChunkOffset:    0,
			ChunkLength:    uint32(entrySize),
		}
		entryBytes, _ := json.Marshal(entry)
		entryKey, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		baseMeta.Put(ctx, entryKey, entryBytes)

		// Create WAL object record
		walObjectKey := keys.WALObjectKeyPath(metaDomain, entry.WalID)
		walRecord := produce.WALObjectRecord{
			Path:      entry.WalPath,
			RefCount:  1,
			CreatedAt: time.Now().UnixMilli(),
			SizeBytes: int64(entrySize),
		}
		walRecordBytes, _ := json.Marshal(walRecord)
		baseMeta.Put(ctx, walObjectKey, walRecordBytes)
	}

	// Swap a small range in the middle of the index (entries 500-502, covering 3 entries).
	// This tests that we don't scan all 1000 entries.
	swapStartOffset := int64(50000) // Start of entry 500
	swapEndOffset := int64(50300)   // End of entry 502

	job := &Job{
		StreamID:          streamID,
		SourceStartOffset: swapStartOffset,
		SourceEndOffset:   swapEndOffset,
	}

	_, err := swapper.SwapFromJob(ctx, job, "/parquet/test.parquet", 300, 300, metaDomain)
	if err != nil {
		t.Fatalf("SwapFromJob failed: %v", err)
	}

	// Check that List calls were bounded
	calls := meta.getListCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one List call")
	}

	// Total results from all List calls should be much less than numEntries
	totalResults := 0
	for _, call := range calls {
		totalResults += call.ResultCount
	}

	// We should only have scanned a few entries (3 WAL entries in range + 1 for cumulative size),
	// not all 1000 entries.
	maxExpectedResults := 10 // Very generous bound
	if totalResults > maxExpectedResults {
		t.Errorf("List calls scanned too many entries: got %d results, expected at most %d (full index has %d entries)",
			totalResults, maxExpectedResults, numEntries)
		for i, call := range calls {
			t.Logf("List call %d: startKey=%s, endKey=%s, limit=%d, results=%d",
				i, call.StartKey, call.EndKey, call.Limit, call.ResultCount)
		}
	}
}

// TestIndexSwapper_Swap_BoundedCumulativeSizeLookup verifies that getPreviousCumulativeSize
// uses a targeted query instead of scanning from offset 0.
func TestIndexSwapper_Swap_BoundedCumulativeSizeLookup(t *testing.T) {
	ctx := context.Background()
	baseMeta := metadata.NewMockStore()
	meta := &listCallTrackingStore{MetadataStore: baseMeta}
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-cumulative-bounded"

	// Create 100 existing Parquet entries
	numEntries := 100
	entrySize := 1000

	for i := 0; i < numEntries; i++ {
		startOffset := int64(i * 100)
		endOffset := int64((i + 1) * 100)
		cumulativeSize := int64((i + 1) * entrySize)

		entry := index.IndexEntry{
			StreamID:         streamID,
			StartOffset:      startOffset,
			EndOffset:        endOffset,
			CumulativeSize:   cumulativeSize,
			FileType:         index.FileTypeParquet,
			RecordCount:      100,
			ParquetPath:      fmt.Sprintf("/parquet/%04d.parquet", i),
			ParquetSizeBytes: uint64(entrySize),
		}
		entryBytes, _ := json.Marshal(entry)
		entryKey, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		baseMeta.Put(ctx, entryKey, entryBytes)
	}

	// Add a WAL entry at the end (entry 100)
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    10000,
		EndOffset:      10100,
		CumulativeSize: 101000,
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		WalID:          "wal-last",
	}
	walBytes, _ := json.Marshal(walEntry)
	walKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	baseMeta.Put(ctx, walKey, walBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(0, walEntry.WalID)
	walRecord := produce.WALObjectRecord{RefCount: 1}
	walRecordBytes, _ := json.Marshal(walRecord)
	baseMeta.Put(ctx, walObjectKey, walRecordBytes)

	// Create Parquet entry to replace
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      10000,
		EndOffset:        10100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		ParquetSizeBytes: 500,
	}

	_, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{walKey},
		ParquetEntry: parquetEntry,
		MetaDomain:   0,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	// Check that the cumulative size lookup was bounded
	calls := meta.getListCalls()
	if len(calls) == 0 {
		t.Fatal("expected at least one List call for cumulative size lookup")
	}

	// The getPreviousCumulativeSize call should have used limit=1 and targeted the specific offset.
	// It should not have scanned all 100 previous entries.
	totalResults := 0
	for _, call := range calls {
		totalResults += call.ResultCount
	}

	// We should only have fetched 1 entry for the cumulative size lookup
	maxExpectedResults := 2 // 1 for cumulative size, potentially 0 if not needed
	if totalResults > maxExpectedResults {
		t.Errorf("List calls scanned too many entries: got %d results, expected at most %d (full index has %d entries)",
			totalResults, maxExpectedResults, numEntries+1)
		for i, call := range calls {
			t.Logf("List call %d: startKey=%s, endKey=%s, limit=%d, results=%d",
				i, call.StartKey, call.EndKey, call.Limit, call.ResultCount)
		}
	}

	// Verify that at least one call used limit=1
	hasLimitedCall := false
	for _, call := range calls {
		if call.Limit == 1 {
			hasLimitedCall = true
			break
		}
	}
	if !hasLimitedCall {
		t.Error("expected at least one List call with limit=1 for cumulative size lookup")
	}
}

// TestIndexSwapper_Swap_ParquetID_AutoGenerated verifies that ParquetID is automatically
// generated when not provided per SPEC 6.3.3.
func TestIndexSwapper_Swap_ParquetID_AutoGenerated(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-parquet-id-auto"
	metaDomain := 1
	walID := "wal-auto-parquet-id"

	// Create a WAL index entry
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1000,
		CreatedAtMs:    time.Now().UnixMilli(),
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		MessageCount:   100,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          walID,
		WalPath:        "/wal/domain-1/wal-auto-parquet-id.wal",
		ChunkOffset:    0,
		ChunkLength:    1000,
	}

	walEntryBytes, _ := json.Marshal(walEntry)
	walIndexKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walIndexKey, walEntryBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)
	walRecord := produce.WALObjectRecord{
		Path:      walEntry.WalPath,
		RefCount:  1,
		CreatedAt: time.Now().UnixMilli(),
		SizeBytes: 1000,
	}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	// Create Parquet entry WITHOUT ParquetID - it should be auto-generated
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/auto-generated-id.parquet",
		ParquetSizeBytes: 500,
		// ParquetID not set - should be auto-generated
	}

	// Execute swap
	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{walIndexKey},
		ParquetEntry: parquetEntry,
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	// Verify new Parquet entry has a ParquetID
	parquetResult, err := meta.Get(ctx, result.NewIndexKey)
	if err != nil {
		t.Fatalf("Failed to get Parquet entry: %v", err)
	}
	if !parquetResult.Exists {
		t.Fatal("Parquet index entry should exist")
	}

	var storedEntry index.IndexEntry
	if err := json.Unmarshal(parquetResult.Value, &storedEntry); err != nil {
		t.Fatalf("Failed to unmarshal Parquet entry: %v", err)
	}

	if storedEntry.ParquetID == "" {
		t.Error("ParquetID should be auto-generated when not provided")
	}

	// Verify ParquetID looks like a UUID (36 chars with dashes)
	if len(storedEntry.ParquetID) != 36 {
		t.Errorf("ParquetID should be a UUID (36 chars), got: %s (len=%d)", storedEntry.ParquetID, len(storedEntry.ParquetID))
	}
}

// TestIndexSwapper_Swap_ParquetID_Provided verifies that a provided ParquetID is used
// per SPEC 6.3.3.
func TestIndexSwapper_Swap_ParquetID_Provided(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-parquet-id-provided"
	metaDomain := 1
	walID := "wal-provided-parquet-id"
	expectedParquetID := "custom-parquet-id-12345"

	// Create a WAL index entry
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      50,
		CumulativeSize: 500,
		CreatedAtMs:    time.Now().UnixMilli(),
		FileType:       index.FileTypeWAL,
		RecordCount:    50,
		MessageCount:   50,
		MinTimestampMs: 1000,
		MaxTimestampMs: 1500,
		WalID:          walID,
		WalPath:        "/wal/domain-1/wal-provided-parquet-id.wal",
		ChunkOffset:    0,
		ChunkLength:    500,
	}

	walEntryBytes, _ := json.Marshal(walEntry)
	walIndexKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walIndexKey, walEntryBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)
	walRecord := produce.WALObjectRecord{
		Path:      walEntry.WalPath,
		RefCount:  1,
		CreatedAt: time.Now().UnixMilli(),
		SizeBytes: 500,
	}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	// Create Parquet entry
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        50,
		FileType:         index.FileTypeParquet,
		RecordCount:      50,
		MessageCount:     50,
		MinTimestampMs:   1000,
		MaxTimestampMs:   1500,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      "/parquet/provided-id.parquet",
		ParquetSizeBytes: 300,
	}

	// Execute swap with explicit ParquetID
	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: []string{walIndexKey},
		ParquetEntry: parquetEntry,
		ParquetID:    expectedParquetID,
		MetaDomain:   metaDomain,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	// Verify new Parquet entry has the provided ParquetID
	parquetResult, err := meta.Get(ctx, result.NewIndexKey)
	if err != nil {
		t.Fatalf("Failed to get Parquet entry: %v", err)
	}

	var storedEntry index.IndexEntry
	if err := json.Unmarshal(parquetResult.Value, &storedEntry); err != nil {
		t.Fatalf("Failed to unmarshal Parquet entry: %v", err)
	}

	if storedEntry.ParquetID != expectedParquetID {
		t.Errorf("Expected ParquetID %s, got %s", expectedParquetID, storedEntry.ParquetID)
	}
}

// TestIndexSwapper_Swap_IcebergDataFileID verifies that IcebergDataFileID is set
// when provided per SPEC 6.3.3.
func TestIndexSwapper_Swap_IcebergDataFileID(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-iceberg-file-id"
	metaDomain := 1
	walID := "wal-iceberg-file-id"
	expectedIcebergDataFileID := "s3://bucket/iceberg/data/file-001.parquet"

	// Create a WAL index entry
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1000,
		CreatedAtMs:    time.Now().UnixMilli(),
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		MessageCount:   100,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          walID,
		WalPath:        "/wal/domain-1/wal-iceberg-file-id.wal",
		ChunkOffset:    0,
		ChunkLength:    1000,
	}

	walEntryBytes, _ := json.Marshal(walEntry)
	walIndexKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walIndexKey, walEntryBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)
	walRecord := produce.WALObjectRecord{
		Path:      walEntry.WalPath,
		RefCount:  1,
		CreatedAt: time.Now().UnixMilli(),
		SizeBytes: 1000,
	}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	// Create Parquet entry
	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      0,
		EndOffset:        100,
		FileType:         index.FileTypeParquet,
		RecordCount:      100,
		MessageCount:     100,
		MinTimestampMs:   1000,
		MaxTimestampMs:   2000,
		CreatedAtMs:      time.Now().UnixMilli(),
		ParquetPath:      expectedIcebergDataFileID,
		ParquetSizeBytes: 500,
	}

	// Execute swap with IcebergDataFileID
	result, err := swapper.Swap(ctx, SwapRequest{
		StreamID:          streamID,
		WALIndexKeys:      []string{walIndexKey},
		ParquetEntry:      parquetEntry,
		IcebergDataFileID: expectedIcebergDataFileID,
		MetaDomain:        metaDomain,
	})
	if err != nil {
		t.Fatalf("Swap failed: %v", err)
	}

	// Verify new Parquet entry has the IcebergDataFileID
	parquetResult, err := meta.Get(ctx, result.NewIndexKey)
	if err != nil {
		t.Fatalf("Failed to get Parquet entry: %v", err)
	}

	var storedEntry index.IndexEntry
	if err := json.Unmarshal(parquetResult.Value, &storedEntry); err != nil {
		t.Fatalf("Failed to unmarshal Parquet entry: %v", err)
	}

	if storedEntry.IcebergDataFileID != expectedIcebergDataFileID {
		t.Errorf("Expected IcebergDataFileID %s, got %s", expectedIcebergDataFileID, storedEntry.IcebergDataFileID)
	}

	// ParquetID should also be auto-generated
	if storedEntry.ParquetID == "" {
		t.Error("ParquetID should be auto-generated")
	}
}

// TestIndexSwapper_SwapFromJob_PopulatesParquetID verifies that SwapFromJob
// generates a ParquetID per SPEC 6.3.3.
func TestIndexSwapper_SwapFromJob_PopulatesParquetID(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-job-parquet-id"
	metaDomain := 1

	// Create WAL entry within job's source range
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1000,
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-job-parquet-id",
	}
	walBytes, _ := json.Marshal(walEntry)
	walKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walKey, walBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walEntry.WalID)
	walRecord := produce.WALObjectRecord{RefCount: 1}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	job := &Job{
		StreamID:          streamID,
		SourceStartOffset: 0,
		SourceEndOffset:   100,
	}

	result, err := swapper.SwapFromJob(ctx, job, "/parquet/job-parquet-id.parquet", 500, 100, metaDomain)
	if err != nil {
		t.Fatalf("SwapFromJob failed: %v", err)
	}

	// Verify new Parquet entry has a ParquetID
	parquetResult, err := meta.Get(ctx, result.NewIndexKey)
	if err != nil {
		t.Fatalf("Failed to get Parquet entry: %v", err)
	}

	var storedEntry index.IndexEntry
	if err := json.Unmarshal(parquetResult.Value, &storedEntry); err != nil {
		t.Fatalf("Failed to unmarshal Parquet entry: %v", err)
	}

	if storedEntry.ParquetID == "" {
		t.Error("ParquetID should be auto-generated by SwapFromJob")
	}

	// Verify ParquetID looks like a UUID
	if len(storedEntry.ParquetID) != 36 {
		t.Errorf("ParquetID should be a UUID, got: %s", storedEntry.ParquetID)
	}
}

// TestIndexSwapper_SwapFromJob_UsesJobIcebergDataFileID verifies that SwapFromJob
// uses the IcebergDataFileID from the job when Iceberg commit has succeeded.
func TestIndexSwapper_SwapFromJob_UsesJobIcebergDataFileID(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()
	swapper := NewIndexSwapper(meta)

	streamID := "test-stream-job-iceberg-id"
	metaDomain := 1
	expectedIcebergDataFileID := "s3://bucket/iceberg/from-job.parquet"

	// Create WAL entry within job's source range
	walEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      100,
		CumulativeSize: 1000,
		FileType:       index.FileTypeWAL,
		RecordCount:    100,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		WalID:          "wal-job-iceberg-id",
	}
	walBytes, _ := json.Marshal(walEntry)
	walKey, _ := keys.OffsetIndexKeyPath(streamID, walEntry.EndOffset, walEntry.CumulativeSize)
	meta.Put(ctx, walKey, walBytes)

	// Create WAL object record
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walEntry.WalID)
	walRecord := produce.WALObjectRecord{RefCount: 1}
	walRecordBytes, _ := json.Marshal(walRecord)
	meta.Put(ctx, walObjectKey, walRecordBytes)

	// Create a job with IcebergDataFileID set (simulating post-Iceberg commit state)
	job := &Job{
		StreamID:          streamID,
		SourceStartOffset: 0,
		SourceEndOffset:   100,
		IcebergDataFileID: expectedIcebergDataFileID, // Set by MarkIcebergCommitted
	}

	result, err := swapper.SwapFromJob(ctx, job, expectedIcebergDataFileID, 500, 100, metaDomain)
	if err != nil {
		t.Fatalf("SwapFromJob failed: %v", err)
	}

	// Verify new Parquet entry has the IcebergDataFileID from the job
	parquetResult, err := meta.Get(ctx, result.NewIndexKey)
	if err != nil {
		t.Fatalf("Failed to get Parquet entry: %v", err)
	}

	var storedEntry index.IndexEntry
	if err := json.Unmarshal(parquetResult.Value, &storedEntry); err != nil {
		t.Fatalf("Failed to unmarshal Parquet entry: %v", err)
	}

	if storedEntry.IcebergDataFileID != expectedIcebergDataFileID {
		t.Errorf("Expected IcebergDataFileID %s, got %s", expectedIcebergDataFileID, storedEntry.IcebergDataFileID)
	}
}
