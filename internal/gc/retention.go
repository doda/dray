// Package gc implements garbage collection for WAL and Parquet objects.
// This file implements retention enforcement based on retention.ms and retention.bytes.
package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/topics"
)

// RetentionWorkerConfig configures the retention enforcement worker.
type RetentionWorkerConfig struct {
	// ScanIntervalMs is the interval between retention scans in milliseconds.
	// Default: 300000 (5 minutes)
	ScanIntervalMs int64

	// NumDomains is the number of metadata domains.
	// Used for WAL GC record placement.
	// Default: 16
	NumDomains int

	// GracePeriodMs is the grace period before deleted data is actually removed.
	// Default: 600000 (10 minutes)
	GracePeriodMs int64

	// IcebergEnabled indicates whether Iceberg metadata is maintained for streams.
	// When true, retention will not schedule Parquet deletes to avoid orphaning Iceberg metadata.
	IcebergEnabled bool
}

// DefaultRetentionWorkerConfig returns default configuration.
func DefaultRetentionWorkerConfig() RetentionWorkerConfig {
	return RetentionWorkerConfig{
		ScanIntervalMs: 300000,
		NumDomains:     16,
		GracePeriodMs:  600000,
	}
}

// RetentionWorker enforces retention.ms and retention.bytes policies on streams.
// It periodically scans all streams and deletes data that exceeds retention limits.
type RetentionWorker struct {
	meta       metadata.MetadataStore
	obj        objectstore.Store
	topicStore *topics.Store
	config     RetentionWorkerConfig

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewRetentionWorker creates a new retention enforcement worker.
func NewRetentionWorker(
	meta metadata.MetadataStore,
	obj objectstore.Store,
	topicStore *topics.Store,
	config RetentionWorkerConfig,
) *RetentionWorker {
	if config.ScanIntervalMs <= 0 {
		config.ScanIntervalMs = 300000
	}
	if config.NumDomains <= 0 {
		config.NumDomains = 16
	}
	if config.GracePeriodMs <= 0 {
		config.GracePeriodMs = 600000
	}
	return &RetentionWorker{
		meta:       meta,
		obj:        obj,
		topicStore: topicStore,
		config:     config,
	}
}

// Start begins the retention worker background loop.
func (w *RetentionWorker) Start() {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.mu.Unlock()

	go w.run()
}

// Stop stops the retention worker and waits for completion.
func (w *RetentionWorker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	close(w.stopCh)
	w.mu.Unlock()

	<-w.doneCh

	w.mu.Lock()
	w.running = false
	w.mu.Unlock()
}

// run is the main worker loop.
func (w *RetentionWorker) run() {
	defer close(w.doneCh)

	ticker := time.NewTicker(time.Duration(w.config.ScanIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	ctx := context.Background()
	w.scan(ctx)

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

// scan scans all topics and enforces retention policies.
func (w *RetentionWorker) scan(ctx context.Context) {
	topicList, err := w.topicStore.ListTopics(ctx)
	if err != nil {
		return
	}

	for _, topic := range topicList {
		select {
		case <-w.stopCh:
			return
		default:
		}

		if err := w.enforceTopicRetention(ctx, topic); err != nil {
			continue
		}
	}
}

// ScanOnce performs a single retention scan synchronously.
func (w *RetentionWorker) ScanOnce(ctx context.Context) error {
	topicList, err := w.topicStore.ListTopics(ctx)
	if err != nil {
		return err
	}

	var lastErr error
	for _, topic := range topicList {
		if err := w.enforceTopicRetention(ctx, topic); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// enforceTopicRetention enforces retention on a single topic.
func (w *RetentionWorker) enforceTopicRetention(ctx context.Context, topic topics.TopicMeta) error {
	config := topics.MergeWithDefaults(topic.Config)

	retentionMs, err := topics.GetRetentionMs(config)
	if err != nil {
		return err
	}

	retentionBytes, err := topics.GetRetentionBytes(config)
	if err != nil {
		return err
	}

	partitions, err := w.topicStore.ListPartitions(ctx, topic.Name)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		if err := w.enforceStreamRetention(ctx, partition.StreamID, topic.Name, retentionMs, retentionBytes); err != nil {
			continue
		}
	}

	return nil
}

// enforceStreamRetention enforces retention on a single stream.
func (w *RetentionWorker) enforceStreamRetention(
	ctx context.Context,
	streamID string,
	topicName string,
	retentionMs int64,
	retentionBytes int64,
) error {
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return err
	}

	if len(kvs) == 0 {
		return nil
	}

	entries := make([]indexEntryWithKey, 0, len(kvs))
	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			continue
		}
		entries = append(entries, indexEntryWithKey{
			key:   kv.Key,
			entry: entry,
		})
	}

	if len(entries) == 0 {
		return nil
	}

	now := time.Now().UnixMilli()
	deleteAfterMs := now + w.config.GracePeriodMs

	var toDelete []indexEntryWithKey

	// Enforce retention.ms: delete entries older than retention limit
	if retentionMs > 0 {
		cutoffMs := now - retentionMs
		toDelete = append(toDelete, w.findEntriesOlderThan(entries, cutoffMs)...)
	}

	// Enforce retention.bytes: delete entries beyond size limit
	if retentionBytes > 0 {
		toDelete = append(toDelete, w.findEntriesBeyondSizeLimit(entries, retentionBytes, toDelete)...)
	}

	// Deduplicate entries to delete
	toDelete = deduplicateEntries(toDelete)

	// Must keep at least one entry (the most recent one)
	if len(toDelete) >= len(entries) && len(entries) > 0 {
		toDelete = toDelete[:len(entries)-1]
	}

	for _, e := range toDelete {
		if err := w.deleteIndexEntry(ctx, e.key, e.entry, deleteAfterMs); err != nil {
			continue
		}
	}

	return nil
}

// indexEntryWithKey pairs an index entry with its metadata key.
type indexEntryWithKey struct {
	key   string
	entry index.IndexEntry
}

// findEntriesOlderThan returns entries with MaxTimestampMs before cutoff.
func (w *RetentionWorker) findEntriesOlderThan(entries []indexEntryWithKey, cutoffMs int64) []indexEntryWithKey {
	var result []indexEntryWithKey
	for _, e := range entries {
		if e.entry.MaxTimestampMs < cutoffMs {
			result = append(result, e)
		}
	}
	return result
}

// findEntriesBeyondSizeLimit returns entries that push total size beyond limit.
// Entries are deleted from oldest (smallest offset) first.
func (w *RetentionWorker) findEntriesBeyondSizeLimit(entries []indexEntryWithKey, limitBytes int64, alreadyDeleted []indexEntryWithKey) []indexEntryWithKey {
	if len(entries) == 0 {
		return nil
	}

	alreadyDeletedSet := make(map[string]struct{}, len(alreadyDeleted))
	for _, e := range alreadyDeleted {
		alreadyDeletedSet[e.key] = struct{}{}
	}

	var totalSize int64
	for _, e := range entries {
		if _, deleted := alreadyDeletedSet[e.key]; deleted {
			continue
		}
		totalSize += entrySizeBytes(e.entry)
	}

	if totalSize <= limitBytes {
		return nil
	}

	bytesToDelete := totalSize - limitBytes
	var result []indexEntryWithKey
	var deletedBytes int64

	// Delete from oldest (first) entries
	for _, e := range entries {
		if _, deleted := alreadyDeletedSet[e.key]; deleted {
			continue
		}
		if deletedBytes >= bytesToDelete {
			break
		}

		result = append(result, e)
		deletedBytes += entrySizeBytes(e.entry)
	}

	return result
}

// deleteIndexEntry deletes an index entry and schedules its data for GC atomically.
// For WAL entries, the index deletion and refcount decrement are performed in a
// single transaction to prevent refcount leaks if one operation fails.
func (w *RetentionWorker) deleteIndexEntry(
	ctx context.Context,
	key string,
	entry index.IndexEntry,
	deleteAfterMs int64,
) error {
	if entry.FileType == index.FileTypeWAL {
		return w.deleteWALIndexEntryAtomically(ctx, key, entry, deleteAfterMs)
	}

	if entry.FileType == index.FileTypeParquet {
		return w.deleteParquetIndexEntry(ctx, key, entry, deleteAfterMs)
	}

	// Unknown file type, just delete the index entry
	return w.meta.Delete(ctx, key)
}

// deleteWALIndexEntryAtomically deletes a WAL index entry and decrements the WAL
// refcount in a single atomic transaction. This prevents refcount leaks that could
// occur if the operations were performed separately.
func (w *RetentionWorker) deleteWALIndexEntryAtomically(
	ctx context.Context,
	key string,
	entry index.IndexEntry,
	deleteAfterMs int64,
) error {
	if entry.WalID == "" {
		// No WAL ID, just delete the index entry
		return w.meta.Delete(ctx, key)
	}

	metaDomain := int(metadata.CalculateMetaDomain(entry.StreamID, w.config.NumDomains))

	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := w.meta.Txn(ctx, key, func(txn metadata.Txn) error {
			// Delete the index entry (unconditional - idempotent if already deleted)
			txn.Delete(key)

			// Decrement WAL refcount within the same transaction
			if err := w.decrementWALRefCountInTxn(txn, metaDomain, entry.WalID, entry.WalPath, deleteAfterMs); err != nil {
				return err
			}

			return nil
		})

		if err == nil {
			return nil
		}

		// Retry on version mismatch or transaction conflict
		if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
			if attempt < maxRetries-1 {
				continue
			}
		}

		return fmt.Errorf("delete WAL index entry %s: %w", key, err)
	}

	return nil
}

// deleteParquetIndexEntry deletes a Parquet index entry and schedules GC if needed.
func (w *RetentionWorker) deleteParquetIndexEntry(
	ctx context.Context,
	key string,
	entry index.IndexEntry,
	deleteAfterMs int64,
) error {
	// Delete the index entry first
	if err := w.meta.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete index entry %s: %w", key, err)
	}

	// Schedule GC for the Parquet file when Iceberg is not enabled
	if w.config.IcebergEnabled {
		return nil
	}

	gcRecord := ParquetGCRecord{
		Path:                    entry.ParquetPath,
		DeleteAfterMs:           deleteAfterMs,
		CreatedAt:               entry.CreatedAtMs,
		SizeBytes:               int64(entry.ParquetSizeBytes),
		StreamID:                entry.StreamID,
		IcebergEnabled:          false,
		IcebergRemovalConfirmed: true,
	}
	if err := ScheduleParquetGC(ctx, w.meta, gcRecord); err != nil {
		return fmt.Errorf("schedule Parquet GC: %w", err)
	}

	return nil
}

// decrementWALRefCountInTxn decrements the WAL refcount within a transaction.
// If the refcount reaches zero, it schedules the WAL for GC. This method is
// designed to be called from within a transaction to ensure atomicity with
// other operations (like index entry deletion).
func (w *RetentionWorker) decrementWALRefCountInTxn(
	txn metadata.Txn,
	metaDomain int,
	walID string,
	walPath string,
	deleteAfterMs int64,
) error {
	walObjectKey := keys.WALObjectKeyPath(metaDomain, walID)

	value, version, err := txn.Get(walObjectKey)
	if err != nil {
		if errors.Is(err, metadata.ErrKeyNotFound) {
			// WAL object record doesn't exist - may already be deleted or GC'd.
			// This is idempotent - we can proceed without decrementing.
			return nil
		}
		return fmt.Errorf("get WAL object record: %w", err)
	}

	var record walObjectRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return fmt.Errorf("unmarshal WAL object record: %w", err)
	}

	if record.RefCount <= 1 {
		// Move to GC queue - refcount is reaching zero
		gcKey := keys.WALGCKeyPath(metaDomain, walID)
		gcRecord := WALGCRecord{
			Path:          walPath,
			DeleteAfterMs: deleteAfterMs,
			CreatedAt:     record.CreatedAt,
			SizeBytes:     record.SizeBytes,
		}
		gcRecordBytes, err := json.Marshal(gcRecord)
		if err != nil {
			return fmt.Errorf("marshal GC record: %w", err)
		}
		txn.DeleteWithVersion(walObjectKey, version)
		txn.Put(gcKey, gcRecordBytes)
		return nil
	}

	// Decrement refcount
	record.RefCount--
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal updated record: %w", err)
	}
	txn.PutWithVersion(walObjectKey, recordBytes, version)
	return nil
}

// walObjectRecord mirrors produce.WALObjectRecord to avoid import cycle.
type walObjectRecord struct {
	Path      string `json:"path"`
	RefCount  int32  `json:"refCount"`
	CreatedAt int64  `json:"createdAt"`
	SizeBytes int64  `json:"sizeBytes"`
}

// deduplicateEntries removes duplicate entries based on key.
func deduplicateEntries(entries []indexEntryWithKey) []indexEntryWithKey {
	seen := make(map[string]bool)
	result := make([]indexEntryWithKey, 0, len(entries))
	for _, e := range entries {
		if !seen[e.key] {
			seen[e.key] = true
			result = append(result, e)
		}
	}
	return result
}

func entrySizeBytes(entry index.IndexEntry) int64 {
	if entry.FileType == index.FileTypeWAL {
		return int64(entry.ChunkLength)
	}
	return int64(entry.ParquetSizeBytes)
}

// GetStreamRetentionStats returns retention statistics for a stream.
// This is useful for monitoring and debugging.
type StreamRetentionStats struct {
	StreamID        string
	OldestTimestamp int64
	NewestTimestamp int64
	TotalBytes      int64
	EntryCount      int
}

// GetStreamRetentionStats retrieves retention stats for a stream.
func (w *RetentionWorker) GetStreamRetentionStats(ctx context.Context, streamID string) (*StreamRetentionStats, error) {
	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, err
	}

	stats := &StreamRetentionStats{
		StreamID:   streamID,
		EntryCount: len(kvs),
	}

	for i, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			continue
		}

		if i == 0 || entry.MinTimestampMs < stats.OldestTimestamp {
			stats.OldestTimestamp = entry.MinTimestampMs
		}
		if entry.MaxTimestampMs > stats.NewestTimestamp {
			stats.NewestTimestamp = entry.MaxTimestampMs
		}
		stats.TotalBytes = entry.CumulativeSize
	}

	return stats, nil
}

// EnforceStream allows manual retention enforcement on a specific stream.
func (w *RetentionWorker) EnforceStream(ctx context.Context, streamID string, retentionMs, retentionBytes int64) error {
	return w.enforceStreamRetention(ctx, streamID, "", retentionMs, retentionBytes)
}
