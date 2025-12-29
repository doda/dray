// Package gc implements garbage collection for WAL and Parquet objects.
// This file implements the WAL GC worker that deletes objects whose
// refcount has reached zero.
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

// WALGCRecord is stored at /dray/v1/wal/gc/<metaDomain>/<walId> when a WAL object
// is ready for garbage collection.
type WALGCRecord struct {
	Path          string `json:"path"`
	DeleteAfterMs int64  `json:"deleteAfterMs"`
	CreatedAt     int64  `json:"createdAt"`
	SizeBytes     int64  `json:"sizeBytes"`
}

// WALGCWorkerConfig configures the WAL GC worker.
type WALGCWorkerConfig struct {
	// ScanIntervalMs is the interval between GC scans in milliseconds.
	// Default: 60000 (1 minute)
	ScanIntervalMs int64

	// NumDomains is the number of metadata domains to scan.
	// Default: 16
	NumDomains int

	// BatchSize is the maximum number of GC records to process per scan.
	// Default: 100
	BatchSize int
}

// DefaultWALGCWorkerConfig returns a default configuration.
func DefaultWALGCWorkerConfig() WALGCWorkerConfig {
	return WALGCWorkerConfig{
		ScanIntervalMs: 60000,
		NumDomains:     16,
		BatchSize:      100,
	}
}

// WALGCWorker scans for WAL objects marked for garbage collection
// and deletes them after their deleteAfterMs grace period has passed.
type WALGCWorker struct {
	meta   metadata.MetadataStore
	obj    objectstore.Store
	config WALGCWorkerConfig

	mu       sync.Mutex
	running  bool
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewWALGCWorker creates a new WAL GC worker.
func NewWALGCWorker(meta metadata.MetadataStore, obj objectstore.Store, config WALGCWorkerConfig) *WALGCWorker {
	if config.ScanIntervalMs <= 0 {
		config.ScanIntervalMs = 60000
	}
	if config.NumDomains <= 0 {
		config.NumDomains = 16
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	return &WALGCWorker{
		meta:   meta,
		obj:    obj,
		config: config,
	}
}

// Start begins the GC worker background loop.
// The worker will periodically scan for eligible WAL objects and delete them.
func (w *WALGCWorker) Start() {
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
func (w *WALGCWorker) Stop() {
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
func (w *WALGCWorker) run() {
	defer close(w.doneCh)

	ticker := time.NewTicker(time.Duration(w.config.ScanIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	// Run one scan immediately on start
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

// scan scans all metadata domains for GC-eligible WAL objects.
func (w *WALGCWorker) scan(ctx context.Context) {
	for domain := 0; domain < w.config.NumDomains; domain++ {
		select {
		case <-w.stopCh:
			return
		default:
		}
		if err := w.scanDomain(ctx, domain); err != nil {
			// Log error but continue with other domains
			continue
		}
	}
}

// scanDomain scans a single metadata domain for GC-eligible WAL objects.
func (w *WALGCWorker) scanDomain(ctx context.Context, metaDomain int) error {
	prefix := keys.WALGCDomainPrefix(metaDomain)
	kvs, err := w.meta.List(ctx, prefix, "", w.config.BatchSize)
	if err != nil {
		return fmt.Errorf("list GC markers for domain %d: %w", metaDomain, err)
	}

	now := time.Now().UnixMilli()

	for _, kv := range kvs {
		select {
		case <-w.stopCh:
			return nil
		default:
		}

		if _, err := w.processGCRecord(ctx, kv.Key, kv.Value, now); err != nil {
			// Log error but continue processing other records
			continue
		}
	}

	return nil
}

// processGCRecord processes a single GC record.
// Returns (true, nil) if the record was deleted, (false, nil) if not eligible,
// or (false, err) on error.
func (w *WALGCWorker) processGCRecord(ctx context.Context, gcKey string, value []byte, nowMs int64) (bool, error) {
	var record WALGCRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return false, fmt.Errorf("unmarshal GC record: %w", err)
	}

	// Check if the grace period has passed
	if nowMs < record.DeleteAfterMs {
		// Not yet eligible for deletion
		return false, nil
	}

	// Delete the object from object storage
	if err := w.obj.Delete(ctx, record.Path); err != nil {
		// If the object is already gone, that's fine
		if !errors.Is(err, objectstore.ErrNotFound) {
			return false, fmt.Errorf("delete object %s: %w", record.Path, err)
		}
	}

	// Delete the GC marker from metadata
	if err := w.meta.Delete(ctx, gcKey); err != nil {
		return false, fmt.Errorf("delete GC marker %s: %w", gcKey, err)
	}

	return true, nil
}

// ScanOnce performs a single GC scan synchronously.
// This is useful for testing or manual GC triggers.
func (w *WALGCWorker) ScanOnce(ctx context.Context) error {
	var lastErr error
	for domain := 0; domain < w.config.NumDomains; domain++ {
		if err := w.scanDomain(ctx, domain); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ProcessEligible processes all eligible GC records in a single domain.
// Returns the number of records deleted and any error encountered.
func (w *WALGCWorker) ProcessEligible(ctx context.Context, metaDomain int) (int, error) {
	prefix := keys.WALGCDomainPrefix(metaDomain)
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return 0, fmt.Errorf("list GC markers: %w", err)
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

// GetPendingCount returns the number of pending GC records across all domains.
func (w *WALGCWorker) GetPendingCount(ctx context.Context) (int, error) {
	total := 0
	for domain := 0; domain < w.config.NumDomains; domain++ {
		prefix := keys.WALGCDomainPrefix(domain)
		kvs, err := w.meta.List(ctx, prefix, "", 0)
		if err != nil {
			return 0, err
		}
		total += len(kvs)
	}
	return total, nil
}

// GetEligibleCount returns the number of GC records eligible for deletion now.
func (w *WALGCWorker) GetEligibleCount(ctx context.Context) (int, error) {
	now := time.Now().UnixMilli()
	eligible := 0

	for domain := 0; domain < w.config.NumDomains; domain++ {
		prefix := keys.WALGCDomainPrefix(domain)
		kvs, err := w.meta.List(ctx, prefix, "", 0)
		if err != nil {
			return 0, err
		}

		for _, kv := range kvs {
			var record WALGCRecord
			if err := json.Unmarshal(kv.Value, &record); err != nil {
				continue
			}
			if now >= record.DeleteAfterMs {
				eligible++
			}
		}
	}
	return eligible, nil
}
