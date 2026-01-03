package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
)

// ParquetGCRecord is stored at /dray/v1/parquet/gc/<streamId>/<parquetId> when a
// Parquet file is ready for garbage collection (after compaction rewrite).
type ParquetGCRecord struct {
	// Path is the object storage path of the Parquet file.
	Path string `json:"path"`

	// DeleteAfterMs is the timestamp (unix milliseconds) after which
	// the Parquet file can be safely deleted.
	DeleteAfterMs int64 `json:"deleteAfterMs"`

	// CreatedAt is when the Parquet file was originally created (unix milliseconds).
	CreatedAt int64 `json:"createdAt"`

	// SizeBytes is the size of the Parquet file in bytes.
	SizeBytes int64 `json:"sizeBytes"`

	// StreamID is the stream this Parquet file belongs to.
	StreamID string `json:"streamId"`

	// JobID is the compaction job ID that replaced this Parquet file.
	JobID string `json:"jobId,omitempty"`

	// IcebergEnabled indicates whether the stream was configured to write Iceberg metadata.
	IcebergEnabled bool `json:"icebergEnabled,omitempty"`

	// IcebergRemovalConfirmed indicates Iceberg metadata no longer references this file.
	IcebergRemovalConfirmed bool `json:"icebergRemovalConfirmed,omitempty"`
}

// ParquetGCWorkerConfig configures the Parquet GC worker.
type ParquetGCWorkerConfig struct {
	// ScanIntervalMs is the interval between GC scans in milliseconds.
	// Default: 60000 (1 minute)
	ScanIntervalMs int64

	// GracePeriodMs is the grace period before a Parquet file is deleted.
	// This allows in-flight reads to complete before deletion.
	// Default: 600000 (10 minutes)
	GracePeriodMs int64

	// BatchSize is the maximum number of GC records to process per scan.
	// Default: 100
	BatchSize int
}

// DefaultParquetGCWorkerConfig returns a default configuration.
func DefaultParquetGCWorkerConfig() ParquetGCWorkerConfig {
	return ParquetGCWorkerConfig{
		ScanIntervalMs: 60000,  // 1 minute
		GracePeriodMs:  600000, // 10 minutes
		BatchSize:      100,
	}
}

// ParquetGCWorker scans for Parquet files marked for garbage collection
// and deletes them after their grace period has passed.
//
// When a compaction job rewrites Parquet files (re-compaction), the old
// Parquet files are tracked in the job state and scheduled for deletion
// after the job reaches DONE. This worker processes those GC markers.
type ParquetGCWorker struct {
	meta   metadata.MetadataStore
	obj    objectstore.Store
	config ParquetGCWorkerConfig

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewParquetGCWorker creates a new Parquet GC worker.
func NewParquetGCWorker(meta metadata.MetadataStore, obj objectstore.Store, config ParquetGCWorkerConfig) *ParquetGCWorker {
	if config.ScanIntervalMs <= 0 {
		config.ScanIntervalMs = 60000
	}
	if config.GracePeriodMs <= 0 {
		config.GracePeriodMs = 600000
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	return &ParquetGCWorker{
		meta:   meta,
		obj:    obj,
		config: config,
	}
}

// Start begins the GC worker background loop.
func (w *ParquetGCWorker) Start() {
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

// Stop stops the GC worker and waits for it to complete.
func (w *ParquetGCWorker) Stop() {
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
func (w *ParquetGCWorker) run() {
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

// scan scans all Parquet GC markers for eligible deletions.
func (w *ParquetGCWorker) scan(ctx context.Context) {
	prefix := keys.ParquetGCAllPrefix()
	endKey := prefix + "~"
	startKey := prefix

	now := time.Now().UnixMilli()

	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		kvs, err := w.meta.List(ctx, startKey, endKey, w.config.BatchSize)
		if err != nil {
			return
		}
		if len(kvs) == 0 {
			return
		}

		for _, kv := range kvs {
			select {
			case <-w.stopCh:
				return
			default:
			}

			if _, err := w.processGCRecord(ctx, kv.Key, kv.Value, now); err != nil {
				continue
			}
		}

		if len(kvs) < w.config.BatchSize {
			return
		}

		startKey = kvs[len(kvs)-1].Key + "\x00"
	}
}

// processGCRecord processes a single GC record.
// Returns (true, nil) if the record was deleted, (false, nil) if not eligible,
// or (false, err) on error.
func (w *ParquetGCWorker) processGCRecord(ctx context.Context, gcKey string, value []byte, nowMs int64) (bool, error) {
	var record ParquetGCRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return false, fmt.Errorf("unmarshal Parquet GC record: %w", err)
	}

	if nowMs < record.DeleteAfterMs {
		return false, nil
	}

	if record.IcebergEnabled && !record.IcebergRemovalConfirmed {
		return false, nil
	}

	if err := w.obj.Delete(ctx, record.Path); err != nil {
		if !errors.Is(err, objectstore.ErrNotFound) {
			return false, fmt.Errorf("delete Parquet object %s: %w", record.Path, err)
		}
	}

	if err := w.meta.Delete(ctx, gcKey); err != nil {
		return false, fmt.Errorf("delete GC marker %s: %w", gcKey, err)
	}

	return true, nil
}

// ScanOnce performs a single GC scan synchronously.
func (w *ParquetGCWorker) ScanOnce(ctx context.Context) error {
	prefix := keys.ParquetGCAllPrefix()
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return fmt.Errorf("list Parquet GC markers: %w", err)
	}

	now := time.Now().UnixMilli()

	for _, kv := range kvs {
		if _, err := w.processGCRecord(ctx, kv.Key, kv.Value, now); err != nil {
			continue
		}
	}

	return nil
}

// ProcessEligible processes all eligible Parquet GC records for a stream.
// Returns the number of records deleted and any error encountered.
func (w *ParquetGCWorker) ProcessEligible(ctx context.Context, streamID string) (int, error) {
	prefix := keys.ParquetGCStreamPrefix(streamID)
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return 0, fmt.Errorf("list Parquet GC markers: %w", err)
	}

	now := time.Now().UnixMilli()
	deleted := 0

	for _, kv := range kvs {
		wasDeleted, err := w.processGCRecord(ctx, kv.Key, kv.Value, now)
		if err != nil {
			continue
		}
		if wasDeleted {
			deleted++
		}
	}

	return deleted, nil
}

// GetPendingCount returns the number of pending Parquet GC records.
func (w *ParquetGCWorker) GetPendingCount(ctx context.Context) (int, error) {
	prefix := keys.ParquetGCAllPrefix()
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return 0, err
	}
	return len(kvs), nil
}

// GetEligibleCount returns the number of Parquet GC records eligible for deletion now.
func (w *ParquetGCWorker) GetEligibleCount(ctx context.Context) (int, error) {
	now := time.Now().UnixMilli()
	eligible := 0

	prefix := keys.ParquetGCAllPrefix()
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return 0, err
	}

	for _, kv := range kvs {
		var record ParquetGCRecord
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			continue
		}
		if now >= record.DeleteAfterMs {
			eligible++
		}
	}
	return eligible, nil
}

// ScheduleParquetGC schedules a Parquet file for garbage collection.
// This is called by the compaction worker when re-compacting Parquet files.
func ScheduleParquetGC(ctx context.Context, meta metadata.MetadataStore, record ParquetGCRecord) error {
	parquetID := extractParquetID(record.Path)
	if parquetID == "" {
		return fmt.Errorf("could not extract parquet ID from path: %s", record.Path)
	}

	gcKey := keys.ParquetGCKeyPath(record.StreamID, parquetID)

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal Parquet GC record: %w", err)
	}

	_, err = meta.Put(ctx, gcKey, recordBytes)
	if err != nil {
		return fmt.Errorf("write Parquet GC marker: %w", err)
	}

	return nil
}

// extractParquetID extracts a unique ID from a Parquet file path.
// The path format is typically: streams/<streamId>/parquet/<filename>.parquet
func extractParquetID(path string) string {
	// Extract the filename without extension as the ID
	lastSlash := -1
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			lastSlash = i
			break
		}
	}

	filename := path
	if lastSlash >= 0 {
		filename = path[lastSlash+1:]
	}

	// Remove .parquet extension if present
	if len(filename) > 8 && filename[len(filename)-8:] == ".parquet" {
		filename = filename[:len(filename)-8]
	}

	if filename == "" {
		return path // Fallback to full path
	}

	return filename
}

// ConfirmParquetIcebergRemoval marks a Parquet GC record as having been removed from
// Iceberg metadata. This is called after a successful ReplaceFiles commit to the Iceberg
// catalog. Once confirmed, the Parquet file becomes eligible for deletion by the GC worker.
func ConfirmParquetIcebergRemoval(ctx context.Context, meta metadata.MetadataStore, streamID, parquetPath string) error {
	parquetID := extractParquetID(parquetPath)
	if parquetID == "" {
		return fmt.Errorf("could not extract parquet ID from path: %s", parquetPath)
	}

	gcKey := keys.ParquetGCKeyPath(streamID, parquetID)

	result, err := meta.Get(ctx, gcKey)
	if err != nil {
		return fmt.Errorf("get Parquet GC record: %w", err)
	}
	if !result.Exists {
		return nil
	}

	var record ParquetGCRecord
	if err := json.Unmarshal(result.Value, &record); err != nil {
		return fmt.Errorf("unmarshal Parquet GC record: %w", err)
	}

	if record.IcebergRemovalConfirmed {
		return nil
	}

	record.IcebergRemovalConfirmed = true

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal Parquet GC record: %w", err)
	}

	_, err = meta.Put(ctx, gcKey, recordBytes, metadata.WithExpectedVersion(result.Version))
	if err != nil {
		if errors.Is(err, metadata.ErrVersionMismatch) {
			return ConfirmParquetIcebergRemoval(ctx, meta, streamID, parquetPath)
		}
		return fmt.Errorf("update Parquet GC record: %w", err)
	}

	return nil
}
