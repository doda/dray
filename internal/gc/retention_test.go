package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/topics"
)

func TestRetentionWorker_EnforceRetentionMs(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	// Create a topic with a short retention period
	now := time.Now().UnixMilli()
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigRetentionMs: "60000", // 1 minute retention
		},
		NowMs: now,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Get the stream ID for partition 0
	partitions, err := topicStore.ListPartitions(ctx, "test-topic")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	streamID := partitions[0].StreamID

	// Create the stream with HWM
	hwmKey := keys.HwmKeyPath(streamID)
	if _, err := metaStore.Put(ctx, hwmKey, index.EncodeHWM(100)); err != nil {
		t.Fatalf("failed to create HWM: %v", err)
	}

	// Create some old index entries (2 minutes old)
	oldTimestamp := now - 120000 // 2 minutes ago
	oldEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      50,
		CumulativeSize: 1000,
		FileType:       index.FileTypeParquet,
		MinTimestampMs: oldTimestamp,
		MaxTimestampMs: oldTimestamp + 1000,
		ParquetPath:    "old-parquet.parquet",
		ParquetSizeBytes: 1000,
		CreatedAtMs:    oldTimestamp,
	}
	oldKey, _ := keys.OffsetIndexKeyPath(streamID, oldEntry.EndOffset, oldEntry.CumulativeSize)
	oldBytes, _ := json.Marshal(oldEntry)
	if _, err := metaStore.Put(ctx, oldKey, oldBytes); err != nil {
		t.Fatalf("failed to create old entry: %v", err)
	}

	// Create a new index entry (30 seconds old - within retention)
	newTimestamp := now - 30000 // 30 seconds ago
	newEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    50,
		EndOffset:      100,
		CumulativeSize: 2000,
		FileType:       index.FileTypeParquet,
		MinTimestampMs: newTimestamp,
		MaxTimestampMs: newTimestamp + 1000,
		ParquetPath:    "new-parquet.parquet",
		ParquetSizeBytes: 1000,
		CreatedAtMs:    newTimestamp,
	}
	newKey, _ := keys.OffsetIndexKeyPath(streamID, newEntry.EndOffset, newEntry.CumulativeSize)
	newBytes, _ := json.Marshal(newEntry)
	if _, err := metaStore.Put(ctx, newKey, newBytes); err != nil {
		t.Fatalf("failed to create new entry: %v", err)
	}

	// Create the Parquet files in object store
	oldData := []byte("old data")
	if err := objStore.Put(ctx, "old-parquet.parquet", bytes.NewReader(oldData), int64(len(oldData)), "application/octet-stream"); err != nil {
		t.Fatalf("failed to create old parquet: %v", err)
	}
	newData := []byte("new data")
	if err := objStore.Put(ctx, "new-parquet.parquet", bytes.NewReader(newData), int64(len(newData)), "application/octet-stream"); err != nil {
		t.Fatalf("failed to create new parquet: %v", err)
	}

	// Create and run the retention worker
	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains:    4,
		GracePeriodMs: 0, // No grace period for testing
	})

	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify old entry was deleted
	oldResult, err := metaStore.Get(ctx, oldKey)
	if err != nil {
		t.Fatalf("failed to check old entry: %v", err)
	}
	if oldResult.Exists {
		t.Error("old entry should have been deleted by retention enforcement")
	}

	// Verify new entry was kept
	newResult, err := metaStore.Get(ctx, newKey)
	if err != nil {
		t.Fatalf("failed to check new entry: %v", err)
	}
	if !newResult.Exists {
		t.Error("new entry should have been kept (within retention)")
	}

	// Check that Parquet GC record was created for old file
	gcPrefix := keys.ParquetGCPrefix
	gcRecords, err := metaStore.List(ctx, gcPrefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list GC records: %v", err)
	}
	if len(gcRecords) == 0 {
		t.Error("expected Parquet GC record to be created for old file")
	}
}

func TestRetentionWorker_EnforceRetentionBytes(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	// Create a topic with a small byte limit
	now := time.Now().UnixMilli()
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic-bytes",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigRetentionMs:    "-1",   // Unlimited time
			topics.ConfigRetentionBytes: "1500", // Only 1500 bytes allowed
		},
		NowMs: now,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	partitions, err := topicStore.ListPartitions(ctx, "test-topic-bytes")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	streamID := partitions[0].StreamID

	// Create the stream with HWM
	hwmKey := keys.HwmKeyPath(streamID)
	if _, err := metaStore.Put(ctx, hwmKey, index.EncodeHWM(150)); err != nil {
		t.Fatalf("failed to create HWM: %v", err)
	}

	// Create three entries: 500 bytes each, total 1500 bytes
	// With limit of 1500, the oldest should be deleted to stay under limit
	entries := []index.IndexEntry{
		{
			StreamID:       streamID,
			StartOffset:    0,
			EndOffset:      50,
			CumulativeSize: 500,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 3000,
			MaxTimestampMs: now - 2500,
			ParquetPath:    "parquet-1.parquet",
			ParquetSizeBytes: 500,
			CreatedAtMs:    now - 3000,
		},
		{
			StreamID:       streamID,
			StartOffset:    50,
			EndOffset:      100,
			CumulativeSize: 1000,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 2000,
			MaxTimestampMs: now - 1500,
			ParquetPath:    "parquet-2.parquet",
			ParquetSizeBytes: 500,
			CreatedAtMs:    now - 2000,
		},
		{
			StreamID:       streamID,
			StartOffset:    100,
			EndOffset:      150,
			CumulativeSize: 2000, // Total exceeds 1500 limit
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 1000,
			MaxTimestampMs: now - 500,
			ParquetPath:    "parquet-3.parquet",
			ParquetSizeBytes: 1000,
			CreatedAtMs:    now - 1000,
		},
	}

	entryKeys := make([]string, len(entries))
	for i, entry := range entries {
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		entryKeys[i] = key
		data, _ := json.Marshal(entry)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to create entry %d: %v", i, err)
		}
		pdata := make([]byte, entry.ParquetSizeBytes)
		if err := objStore.Put(ctx, entry.ParquetPath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
			t.Fatalf("failed to create parquet %d: %v", i, err)
		}
	}

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains:    4,
		GracePeriodMs: 0,
	})

	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify first entry was deleted (to get under 1500 byte limit)
	firstResult, err := metaStore.Get(ctx, entryKeys[0])
	if err != nil {
		t.Fatalf("failed to check first entry: %v", err)
	}
	if firstResult.Exists {
		t.Error("first entry should have been deleted to meet byte limit")
	}

	// Verify later entries were kept
	for i := 1; i < len(entryKeys); i++ {
		result, err := metaStore.Get(ctx, entryKeys[i])
		if err != nil {
			t.Fatalf("failed to check entry %d: %v", i, err)
		}
		if !result.Exists {
			t.Errorf("entry %d should have been kept", i)
		}
	}
}

func TestRetentionWorker_KeepsAtLeastOneEntry(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	// Create a topic with very aggressive retention (everything would be deleted)
	now := time.Now().UnixMilli()
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic-keep-one",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigRetentionMs:    "1",  // 1ms retention (everything is old)
			topics.ConfigRetentionBytes: "10", // 10 bytes limit (everything exceeds)
		},
		NowMs: now,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	partitions, err := topicStore.ListPartitions(ctx, "test-topic-keep-one")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	streamID := partitions[0].StreamID

	hwmKey := keys.HwmKeyPath(streamID)
	if _, err := metaStore.Put(ctx, hwmKey, index.EncodeHWM(100)); err != nil {
		t.Fatalf("failed to create HWM: %v", err)
	}

	// Create two entries - both should be eligible for deletion, but at least one must be kept
	entries := []index.IndexEntry{
		{
			StreamID:       streamID,
			StartOffset:    0,
			EndOffset:      50,
			CumulativeSize: 1000,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 10000,
			MaxTimestampMs: now - 9000,
			ParquetPath:    "keep-one-1.parquet",
			ParquetSizeBytes: 1000,
			CreatedAtMs:    now - 10000,
		},
		{
			StreamID:       streamID,
			StartOffset:    50,
			EndOffset:      100,
			CumulativeSize: 2000,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 8000,
			MaxTimestampMs: now - 7000,
			ParquetPath:    "keep-one-2.parquet",
			ParquetSizeBytes: 1000,
			CreatedAtMs:    now - 8000,
		},
	}

	entryKeys := make([]string, len(entries))
	for i, entry := range entries {
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		entryKeys[i] = key
		data, _ := json.Marshal(entry)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to create entry %d: %v", i, err)
		}
	}

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains:    4,
		GracePeriodMs: 0,
	})

	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Count how many entries remain
	remaining := 0
	for _, key := range entryKeys {
		result, err := metaStore.Get(ctx, key)
		if err != nil {
			t.Fatalf("failed to check entry: %v", err)
		}
		if result.Exists {
			remaining++
		}
	}

	if remaining < 1 {
		t.Error("at least one entry should have been kept")
	}
}

func TestRetentionWorker_UnlimitedRetention(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	// Create a topic with unlimited retention
	now := time.Now().UnixMilli()
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic-unlimited",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigRetentionMs:    "-1", // Unlimited
			topics.ConfigRetentionBytes: "-1", // Unlimited
		},
		NowMs: now,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	partitions, err := topicStore.ListPartitions(ctx, "test-topic-unlimited")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	streamID := partitions[0].StreamID

	hwmKey := keys.HwmKeyPath(streamID)
	if _, err := metaStore.Put(ctx, hwmKey, index.EncodeHWM(50)); err != nil {
		t.Fatalf("failed to create HWM: %v", err)
	}

	// Create a very old entry
	entry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      50,
		CumulativeSize: 1000000,
		FileType:       index.FileTypeParquet,
		MinTimestampMs: now - 86400000*365, // 1 year old
		MaxTimestampMs: now - 86400000*365,
		ParquetPath:    "unlimited.parquet",
		ParquetSizeBytes: 1000000,
		CreatedAtMs:    now - 86400000*365,
	}
	key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
	data, _ := json.Marshal(entry)
	if _, err := metaStore.Put(ctx, key, data); err != nil {
		t.Fatalf("failed to create entry: %v", err)
	}

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains:    4,
		GracePeriodMs: 0,
	})

	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify entry was kept (unlimited retention)
	result, err := metaStore.Get(ctx, key)
	if err != nil {
		t.Fatalf("failed to check entry: %v", err)
	}
	if !result.Exists {
		t.Error("entry should have been kept with unlimited retention")
	}
}

func TestRetentionWorker_WALEntryWithRefCount(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic-wal",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigRetentionMs: "60000",
		},
		NowMs: now,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	partitions, err := topicStore.ListPartitions(ctx, "test-topic-wal")
	if err != nil {
		t.Fatalf("ListPartitions failed: %v", err)
	}
	streamID := partitions[0].StreamID

	hwmKey := keys.HwmKeyPath(streamID)
	if _, err := metaStore.Put(ctx, hwmKey, index.EncodeHWM(100)); err != nil {
		t.Fatalf("failed to create HWM: %v", err)
	}

	// Create a WAL object record
	walID := "test-wal-id"
	walPath := "wal/test.wal"
	metaDomain := int(metadata.CalculateMetaDomain(streamID, 4))
	walObjKey := keys.WALObjectKeyPath(metaDomain, walID)
	walRecord := walObjectRecord{
		Path:      walPath,
		RefCount:  1,
		CreatedAt: now - 120000,
		SizeBytes: 1000,
	}
	walBytes, _ := json.Marshal(walRecord)
	if _, err := metaStore.Put(ctx, walObjKey, walBytes); err != nil {
		t.Fatalf("failed to create WAL record: %v", err)
	}

	// Create a WAL file in object store
	walData := []byte("wal data")
	if err := objStore.Put(ctx, walPath, bytes.NewReader(walData), int64(len(walData)), "application/octet-stream"); err != nil {
		t.Fatalf("failed to create WAL file: %v", err)
	}

	// Create old WAL index entry
	oldEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      50,
		CumulativeSize: 500,
		FileType:       index.FileTypeWAL,
		MinTimestampMs: now - 120000,
		MaxTimestampMs: now - 110000,
		WalID:          walID,
		WalPath:        walPath,
		ChunkOffset:    0,
		ChunkLength:    500,
		CreatedAtMs:    now - 120000,
	}
	oldKey, _ := keys.OffsetIndexKeyPath(streamID, oldEntry.EndOffset, oldEntry.CumulativeSize)
	oldBytes, _ := json.Marshal(oldEntry)
	if _, err := metaStore.Put(ctx, oldKey, oldBytes); err != nil {
		t.Fatalf("failed to create old entry: %v", err)
	}

	// Create new entry to keep
	newEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    50,
		EndOffset:      100,
		CumulativeSize: 1000,
		FileType:       index.FileTypeParquet,
		MinTimestampMs: now - 30000,
		MaxTimestampMs: now - 20000,
		ParquetPath:    "new.parquet",
		ParquetSizeBytes: 500,
		CreatedAtMs:    now - 30000,
	}
	newKey, _ := keys.OffsetIndexKeyPath(streamID, newEntry.EndOffset, newEntry.CumulativeSize)
	newBytes, _ := json.Marshal(newEntry)
	if _, err := metaStore.Put(ctx, newKey, newBytes); err != nil {
		t.Fatalf("failed to create new entry: %v", err)
	}

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains:    4,
		GracePeriodMs: 0,
	})

	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Verify old WAL entry was deleted
	oldResult, err := metaStore.Get(ctx, oldKey)
	if err != nil {
		t.Fatalf("failed to check old entry: %v", err)
	}
	if oldResult.Exists {
		t.Error("old WAL entry should have been deleted")
	}

	// Verify WAL object record was deleted (refcount reached 0)
	walResult, err := metaStore.Get(ctx, walObjKey)
	if err != nil {
		t.Fatalf("failed to check WAL object record: %v", err)
	}
	if walResult.Exists {
		t.Error("WAL object record should have been deleted (refcount=0)")
	}

	// Verify WAL GC record was created
	gcKey := keys.WALGCKeyPath(metaDomain, walID)
	gcResult, err := metaStore.Get(ctx, gcKey)
	if err != nil {
		t.Fatalf("failed to check GC record: %v", err)
	}
	if !gcResult.Exists {
		t.Error("WAL GC record should have been created")
	}
}

func TestRetentionWorker_GetStreamRetentionStats(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	streamID := "test-stream-stats"

	// Create some entries
	entries := []index.IndexEntry{
		{
			StreamID:       streamID,
			StartOffset:    0,
			EndOffset:      50,
			CumulativeSize: 1000,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 60000,
			MaxTimestampMs: now - 50000,
		},
		{
			StreamID:       streamID,
			StartOffset:    50,
			EndOffset:      100,
			CumulativeSize: 2500,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 40000,
			MaxTimestampMs: now - 30000,
		},
		{
			StreamID:       streamID,
			StartOffset:    100,
			EndOffset:      150,
			CumulativeSize: 4000,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: now - 20000,
			MaxTimestampMs: now - 10000,
		},
	}

	for _, entry := range entries {
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		data, _ := json.Marshal(entry)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to create entry: %v", err)
		}
	}

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains: 4,
	})

	stats, err := worker.GetStreamRetentionStats(ctx, streamID)
	if err != nil {
		t.Fatalf("GetStreamRetentionStats failed: %v", err)
	}

	if stats.StreamID != streamID {
		t.Errorf("StreamID = %q, want %q", stats.StreamID, streamID)
	}
	if stats.EntryCount != 3 {
		t.Errorf("EntryCount = %d, want 3", stats.EntryCount)
	}
	if stats.OldestTimestamp != now-60000 {
		t.Errorf("OldestTimestamp = %d, want %d", stats.OldestTimestamp, now-60000)
	}
	if stats.NewestTimestamp != now-10000 {
		t.Errorf("NewestTimestamp = %d, want %d", stats.NewestTimestamp, now-10000)
	}
	if stats.TotalBytes != 4000 {
		t.Errorf("TotalBytes = %d, want 4000", stats.TotalBytes)
	}
}

func TestRetentionWorker_StartStop(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		ScanIntervalMs: 100,
		NumDomains:     4,
	})

	// Start the worker
	worker.Start()

	// Start again should be no-op
	worker.Start()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Stop the worker
	worker.Stop()

	// Stop again should be no-op
	worker.Stop()
}

func TestRetentionWorker_DefaultConfig(t *testing.T) {
	config := DefaultRetentionWorkerConfig()

	if config.ScanIntervalMs != 300000 {
		t.Errorf("ScanIntervalMs = %d, want 300000", config.ScanIntervalMs)
	}
	if config.NumDomains != 16 {
		t.Errorf("NumDomains = %d, want 16", config.NumDomains)
	}
	if config.GracePeriodMs != 600000 {
		t.Errorf("GracePeriodMs = %d, want 600000", config.GracePeriodMs)
	}
}

func TestRetentionWorker_EnforceStream(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	now := time.Now().UnixMilli()
	streamID := "manual-enforce-stream"

	hwmKey := keys.HwmKeyPath(streamID)
	if _, err := metaStore.Put(ctx, hwmKey, index.EncodeHWM(100)); err != nil {
		t.Fatalf("failed to create HWM: %v", err)
	}

	// Create old entry
	oldEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    0,
		EndOffset:      50,
		CumulativeSize: 1000,
		FileType:       index.FileTypeParquet,
		MinTimestampMs: now - 120000,
		MaxTimestampMs: now - 110000,
		ParquetPath:    "manual-old.parquet",
		ParquetSizeBytes: 1000,
		CreatedAtMs:    now - 120000,
	}
	oldKey, _ := keys.OffsetIndexKeyPath(streamID, oldEntry.EndOffset, oldEntry.CumulativeSize)
	oldBytes, _ := json.Marshal(oldEntry)
	if _, err := metaStore.Put(ctx, oldKey, oldBytes); err != nil {
		t.Fatalf("failed to create old entry: %v", err)
	}

	// Create new entry
	newEntry := index.IndexEntry{
		StreamID:       streamID,
		StartOffset:    50,
		EndOffset:      100,
		CumulativeSize: 2000,
		FileType:       index.FileTypeParquet,
		MinTimestampMs: now - 30000,
		MaxTimestampMs: now - 20000,
		ParquetPath:    "manual-new.parquet",
		ParquetSizeBytes: 1000,
		CreatedAtMs:    now - 30000,
	}
	newKey, _ := keys.OffsetIndexKeyPath(streamID, newEntry.EndOffset, newEntry.CumulativeSize)
	newBytes, _ := json.Marshal(newEntry)
	if _, err := metaStore.Put(ctx, newKey, newBytes); err != nil {
		t.Fatalf("failed to create new entry: %v", err)
	}

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains:    4,
		GracePeriodMs: 0,
	})

	// Manually enforce with 1 minute retention
	if err := worker.EnforceStream(ctx, streamID, 60000, -1); err != nil {
		t.Fatalf("EnforceStream failed: %v", err)
	}

	// Verify old entry was deleted
	oldResult, err := metaStore.Get(ctx, oldKey)
	if err != nil {
		t.Fatalf("failed to check old entry: %v", err)
	}
	if oldResult.Exists {
		t.Error("old entry should have been deleted")
	}

	// Verify new entry was kept
	newResult, err := metaStore.Get(ctx, newKey)
	if err != nil {
		t.Fatalf("failed to check new entry: %v", err)
	}
	if !newResult.Exists {
		t.Error("new entry should have been kept")
	}
}

func TestRetentionWorker_MultipleTopics(t *testing.T) {
	metaStore := metadata.NewMockStore()
	objStore := newMockObjectStore()
	topicStore := topics.NewStore(metaStore)
	ctx := context.Background()

	now := time.Now().UnixMilli()

	// Create two topics with different retention settings
	topics1, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "topic-short-retention",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigRetentionMs: "60000", // 1 minute
		},
		NowMs: now,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	topics2, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "topic-long-retention",
		PartitionCount: 1,
		Config: map[string]string{
			topics.ConfigRetentionMs: strconv.FormatInt(86400000, 10), // 1 day
		},
		NowMs: now,
	})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	partitions1, _ := topicStore.ListPartitions(ctx, "topic-short-retention")
	partitions2, _ := topicStore.ListPartitions(ctx, "topic-long-retention")
	stream1 := partitions1[0].StreamID
	stream2 := partitions2[0].StreamID

	// Create HWMs
	if _, err := metaStore.Put(ctx, keys.HwmKeyPath(stream1), index.EncodeHWM(50)); err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.Put(ctx, keys.HwmKeyPath(stream2), index.EncodeHWM(50)); err != nil {
		t.Fatal(err)
	}

	// Create old entries for both streams (2 minutes old)
	oldTimestamp := now - 120000
	for i, streamID := range []string{stream1, stream2} {
		entry := index.IndexEntry{
			StreamID:       streamID,
			StartOffset:    0,
			EndOffset:      25,
			CumulativeSize: 500,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: oldTimestamp,
			MaxTimestampMs: oldTimestamp + 1000,
			ParquetPath:    "multi-old-" + strconv.Itoa(i) + ".parquet",
			ParquetSizeBytes: 500,
			CreatedAtMs:    oldTimestamp,
		}
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		data, _ := json.Marshal(entry)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to create entry: %v", err)
		}
	}

	// Create new entries for both streams (30 seconds old - within short retention)
	newTimestamp := now - 30000
	for i, streamID := range []string{stream1, stream2} {
		entry := index.IndexEntry{
			StreamID:       streamID,
			StartOffset:    25,
			EndOffset:      50,
			CumulativeSize: 1000,
			FileType:       index.FileTypeParquet,
			MinTimestampMs: newTimestamp,
			MaxTimestampMs: newTimestamp + 1000,
			ParquetPath:    "multi-new-" + strconv.Itoa(i) + ".parquet",
			ParquetSizeBytes: 500,
			CreatedAtMs:    newTimestamp,
		}
		key, _ := keys.OffsetIndexKeyPath(streamID, entry.EndOffset, entry.CumulativeSize)
		data, _ := json.Marshal(entry)
		if _, err := metaStore.Put(ctx, key, data); err != nil {
			t.Fatalf("failed to create new entry: %v", err)
		}
	}

	_ = topics1
	_ = topics2

	worker := NewRetentionWorker(metaStore, objStore, topicStore, RetentionWorkerConfig{
		NumDomains:    4,
		GracePeriodMs: 0,
	})

	if err := worker.ScanOnce(ctx); err != nil {
		t.Fatalf("ScanOnce failed: %v", err)
	}

	// Stream1 (short retention - 1 minute): old entry deleted, new entry kept
	prefix1 := keys.OffsetIndexPrefix(stream1)
	kvs1, _ := metaStore.List(ctx, prefix1, "", 0)
	if len(kvs1) != 1 {
		t.Errorf("stream1 should have 1 entry (old deleted, new kept), got %d", len(kvs1))
	}

	// Stream2 (long retention - 1 day): both entries should be kept
	prefix2 := keys.OffsetIndexPrefix(stream2)
	kvs2, _ := metaStore.List(ctx, prefix2, "", 0)
	if len(kvs2) != 2 {
		t.Errorf("stream2 should keep both entries (long retention), got %d", len(kvs2))
	}
}
