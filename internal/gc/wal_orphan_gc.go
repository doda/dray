package gc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/wal"
)

// WALOrphanGCWorkerConfig configures the WAL orphan GC worker.
type WALOrphanGCWorkerConfig struct {
	// ScanIntervalMs is the interval between orphan scans in milliseconds.
	// Default: 60000 (1 minute)
	ScanIntervalMs int64

	// OrphanTTLMs is the time after which a staging marker is considered orphaned.
	// Staging markers older than this will have their WAL objects deleted.
	// Per spec section 9.7, default is 24 hours (86400000ms).
	// Default: 86400000 (24 hours)
	OrphanTTLMs int64

	// NumDomains is the number of metadata domains to scan.
	// Default: 16
	NumDomains int

	// BatchSize is the maximum number of staging markers to fetch per list call.
	// Scans will paginate until all markers in a domain are processed.
	// Default: 100
	BatchSize int
}

// DefaultWALOrphanGCWorkerConfig returns a default configuration.
func DefaultWALOrphanGCWorkerConfig() WALOrphanGCWorkerConfig {
	return WALOrphanGCWorkerConfig{
		ScanIntervalMs: 60000,
		OrphanTTLMs:    86400000, // 24 hours per spec
		NumDomains:     16,
		BatchSize:      100,
	}
}

// WALOrphanGCWorker scans for orphaned WAL objects (staging markers that
// persist beyond wal.orphan_ttl) and deletes them.
//
// Per spec section 9.7, orphaned WAL objects occur when:
// - A WAL object is written to object storage
// - The staging marker is created in metadata
// - The broker crashes before the commit transaction completes
//
// The staging marker remains, indicating an orphaned WAL that can be safely
// deleted after the orphan TTL passes.
type WALOrphanGCWorker struct {
	meta   metadata.MetadataStore
	obj    objectstore.Store
	config WALOrphanGCWorkerConfig

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewWALOrphanGCWorker creates a new WAL orphan GC worker.
func NewWALOrphanGCWorker(meta metadata.MetadataStore, obj objectstore.Store, config WALOrphanGCWorkerConfig) *WALOrphanGCWorker {
	if config.ScanIntervalMs <= 0 {
		config.ScanIntervalMs = 60000
	}
	if config.OrphanTTLMs <= 0 {
		config.OrphanTTLMs = 86400000 // 24 hours
	}
	if config.NumDomains <= 0 {
		config.NumDomains = 16
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	return &WALOrphanGCWorker{
		meta:   meta,
		obj:    obj,
		config: config,
	}
}

// Start begins the orphan GC worker background loop.
func (w *WALOrphanGCWorker) Start() {
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

// Stop stops the orphan GC worker and waits for it to complete.
func (w *WALOrphanGCWorker) Stop() {
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
func (w *WALOrphanGCWorker) run() {
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

// scan scans all metadata domains for orphaned staging markers.
func (w *WALOrphanGCWorker) scan(ctx context.Context) {
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

// scanDomain scans a single metadata domain for orphaned staging markers.
func (w *WALOrphanGCWorker) scanDomain(ctx context.Context, metaDomain int) error {
	prefix := keys.WALStagingDomainPrefix(metaDomain)
	endKey := prefix + "~"
	startKey := prefix

	now := time.Now().UnixMilli()

	for {
		kvs, err := w.meta.List(ctx, startKey, endKey, w.config.BatchSize)
		if err != nil {
			return fmt.Errorf("list staging markers for domain %d: %w", metaDomain, err)
		}
		if len(kvs) == 0 {
			return nil
		}

		for _, kv := range kvs {
			select {
			case <-w.stopCh:
				return nil
			default:
			}

			if _, err := w.processStagingMarker(ctx, kv.Key, kv.Value, now); err != nil {
				// Log error but continue processing other markers
				continue
			}
		}

		if len(kvs) < w.config.BatchSize {
			return nil
		}

		startKey = kvs[len(kvs)-1].Key + "\x00"
	}
}

// processStagingMarker processes a single staging marker.
// Returns (true, nil) if the orphan was deleted, (false, nil) if not orphaned yet,
// or (false, err) on error.
func (w *WALOrphanGCWorker) processStagingMarker(ctx context.Context, stagingKey string, value []byte, nowMs int64) (bool, error) {
	marker, err := wal.ParseStagingMarker(value)
	if err != nil {
		return false, fmt.Errorf("parse staging marker: %w", err)
	}

	// Check if the orphan TTL has passed
	orphanAfter := marker.CreatedAt + w.config.OrphanTTLMs
	if nowMs < orphanAfter {
		// Not yet orphaned
		return false, nil
	}

	// Delete the orphaned WAL object from object storage
	if err := w.obj.Delete(ctx, marker.Path); err != nil {
		// If the object is already gone, that's fine
		if !errors.Is(err, objectstore.ErrNotFound) {
			return false, fmt.Errorf("delete orphaned WAL object %s: %w", marker.Path, err)
		}
	}

	// Delete the staging marker from metadata
	if err := w.meta.Delete(ctx, stagingKey); err != nil {
		return false, fmt.Errorf("delete staging marker %s: %w", stagingKey, err)
	}

	return true, nil
}

// ScanOnce performs a single orphan scan synchronously.
// This is useful for testing or manual cleanup triggers.
func (w *WALOrphanGCWorker) ScanOnce(ctx context.Context) error {
	var lastErr error
	for domain := 0; domain < w.config.NumDomains; domain++ {
		if err := w.scanDomain(ctx, domain); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ProcessOrphans processes all orphaned staging markers in a single domain.
// Returns the number of orphans deleted and any error encountered.
func (w *WALOrphanGCWorker) ProcessOrphans(ctx context.Context, metaDomain int) (int, error) {
	prefix := keys.WALStagingDomainPrefix(metaDomain)
	kvs, err := w.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return 0, fmt.Errorf("list staging markers: %w", err)
	}

	now := time.Now().UnixMilli()
	deleted := 0

	for _, kv := range kvs {
		wasDeleted, err := w.processStagingMarker(ctx, kv.Key, kv.Value, now)
		if err != nil {
			continue
		}
		if wasDeleted {
			deleted++
		}
	}

	return deleted, nil
}

// GetStagingCount returns the number of staging markers across all domains.
func (w *WALOrphanGCWorker) GetStagingCount(ctx context.Context) (int, error) {
	total := 0
	for domain := 0; domain < w.config.NumDomains; domain++ {
		prefix := keys.WALStagingDomainPrefix(domain)
		kvs, err := w.meta.List(ctx, prefix, "", 0)
		if err != nil {
			return 0, err
		}
		total += len(kvs)
	}
	return total, nil
}

// GetOrphanCount returns the number of orphaned staging markers (older than TTL).
func (w *WALOrphanGCWorker) GetOrphanCount(ctx context.Context) (int, error) {
	now := time.Now().UnixMilli()
	orphaned := 0

	for domain := 0; domain < w.config.NumDomains; domain++ {
		prefix := keys.WALStagingDomainPrefix(domain)
		kvs, err := w.meta.List(ctx, prefix, "", 0)
		if err != nil {
			return 0, err
		}

		for _, kv := range kvs {
			marker, err := wal.ParseStagingMarker(kv.Value)
			if err != nil {
				continue
			}
			orphanAfter := marker.CreatedAt + w.config.OrphanTTLMs
			if now >= orphanAfter {
				orphaned++
			}
		}
	}
	return orphaned, nil
}
