package index

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// Index cache watcher backoff configuration defaults.
const (
	defaultIndexCacheInitialBackoff = 100 * time.Millisecond
	defaultIndexCacheMaxBackoff     = 30 * time.Second
	defaultIndexCacheBackoffFactor  = 2.0
)

// CachedIndexEntry represents a cached index entry with revision stamping.
// The Version field is used to ensure session monotonicity - cached entries
// are only valid if their version is consistent with the metadata store.
type CachedIndexEntry struct {
	Entry   IndexEntry
	Version metadata.Version
}

// streamCache holds cached index entries for a single stream.
// Entries are stored in a map keyed by their index key for fast lookup.
type streamCache struct {
	entries map[string]CachedIndexEntry // indexKey -> cached entry
	size    int64                       // approximate memory size in bytes
}

// IndexCacheConfig configures the IndexCache.
type IndexCacheConfig struct {
	// MaxMemoryBytes is the maximum total memory across all stream caches.
	// When exceeded, entries are evicted starting from streams with the
	// oldest access times. Default: 64MB.
	MaxMemoryBytes int64

	// MaxEntriesPerStream limits entries cached per stream.
	// Default: 1000.
	MaxEntriesPerStream int

	// InitialBackoff is the initial delay for watcher retry on errors.
	// Default: 100ms.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay for watcher retry on errors.
	// Default: 30s.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier for exponential backoff.
	// Default: 2.0.
	BackoffFactor float64
}

// DefaultIndexCacheConfig returns sensible defaults for IndexCache.
func DefaultIndexCacheConfig() IndexCacheConfig {
	return IndexCacheConfig{
		MaxMemoryBytes:      64 * 1024 * 1024, // 64MB
		MaxEntriesPerStream: 1000,
		InitialBackoff:      defaultIndexCacheInitialBackoff,
		MaxBackoff:          defaultIndexCacheMaxBackoff,
		BackoffFactor:       defaultIndexCacheBackoffFactor,
	}
}

// IndexCache caches recent index entries with revision stamping for session monotonicity.
// It subscribes to metadata notifications to invalidate entries when the underlying
// index data changes.
//
// The cache provides:
//   - Fast lookup of index entries by key or offset range
//   - Memory bounding with LRU-style eviction
//   - Automatic invalidation via Oxia notifications
//   - Session monotonicity via revision stamping
//
// Per SPEC section 10.2, the cache is ephemeral, bounded, non-authoritative,
// and invalidated by Oxia notifications.
type IndexCache struct {
	store  metadata.MetadataStore
	config IndexCacheConfig

	mu            sync.RWMutex
	streams       map[string]*streamCache // streamID -> stream cache
	totalSize     int64                   // total memory usage across all streams
	lastAccessSeq int64                   // monotonic sequence for LRU tracking
	accessTimes   map[string]int64        // streamID -> last access sequence

	// Background notification watcher state
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closed    bool
	wg        sync.WaitGroup
}

// NewIndexCache creates a new IndexCache backed by the given metadata store.
// The cache automatically starts watching for notifications to invalidate
// stale entries. Call Close() when the cache is no longer needed.
func NewIndexCache(store metadata.MetadataStore, config IndexCacheConfig) *IndexCache {
	defaults := DefaultIndexCacheConfig()
	if config.MaxMemoryBytes <= 0 {
		config.MaxMemoryBytes = defaults.MaxMemoryBytes
	}
	if config.MaxEntriesPerStream <= 0 {
		config.MaxEntriesPerStream = defaults.MaxEntriesPerStream
	}
	if config.InitialBackoff <= 0 {
		config.InitialBackoff = defaults.InitialBackoff
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = defaults.MaxBackoff
	}
	if config.BackoffFactor <= 1.0 {
		config.BackoffFactor = defaults.BackoffFactor
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &IndexCache{
		store:       store,
		config:      config,
		streams:     make(map[string]*streamCache),
		accessTimes: make(map[string]int64),
		ctx:         ctx,
		cancel:      cancel,
	}
	c.wg.Add(1)
	go c.watchNotifications()
	return c
}

// Get retrieves a cached index entry by its key.
// Returns the entry, its version, and whether it was found in cache.
// This does not fetch from the store - use GetOrFetch for that behavior.
func (c *IndexCache) Get(streamID, indexKey string) (*IndexEntry, metadata.Version, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sc, ok := c.streams[streamID]
	if !ok {
		return nil, 0, false
	}

	cached, ok := sc.entries[indexKey]
	if !ok {
		return nil, 0, false
	}

	return &cached.Entry, cached.Version, true
}

// GetOrFetch retrieves a cached entry or fetches from the store on cache miss.
// Updates the cache on successful fetch.
func (c *IndexCache) GetOrFetch(ctx context.Context, streamID, indexKey string, sm *StreamManager) (*IndexEntry, metadata.Version, error) {
	// Check cache first
	if entry, version, ok := c.Get(streamID, indexKey); ok {
		c.recordAccess(streamID)
		return entry, version, nil
	}

	// Cache miss - fetch from store
	entry, version, err := sm.GetIndexEntry(ctx, indexKey)
	if err != nil {
		return nil, 0, err
	}

	// Update cache
	c.Put(streamID, indexKey, entry, version)

	return entry, version, nil
}

// Put adds or updates an entry in the cache.
// The entry is only updated if the new version is >= the cached version
// to maintain monotonicity.
func (c *IndexCache) Put(streamID, indexKey string, entry *IndexEntry, version metadata.Version) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.putLocked(streamID, indexKey, entry, version)
}

// putLocked adds an entry (caller must hold write lock).
func (c *IndexCache) putLocked(streamID, indexKey string, entry *IndexEntry, version metadata.Version) {
	sc, ok := c.streams[streamID]
	if !ok {
		sc = &streamCache{
			entries: make(map[string]CachedIndexEntry),
		}
		c.streams[streamID] = sc
	}

	// Check monotonicity - only update if version is newer or entry doesn't exist
	if existing, ok := sc.entries[indexKey]; ok {
		if version < existing.Version {
			return // Don't update with older version
		}
		// Subtract old entry size
		c.totalSize -= estimateEntrySize(&existing.Entry)
		sc.size -= estimateEntrySize(&existing.Entry)
	}

	// Add new entry
	entrySize := estimateEntrySize(entry)
	sc.entries[indexKey] = CachedIndexEntry{
		Entry:   *entry,
		Version: version,
	}
	sc.size += entrySize
	c.totalSize += entrySize

	// Track access
	c.recordAccessLocked(streamID)

	// Enforce per-stream limit
	for len(sc.entries) > c.config.MaxEntriesPerStream {
		c.evictOldestEntryFromStream(sc)
	}

	// Enforce memory limit
	c.evictIfNeeded()
}

// PutBatch adds multiple entries for a stream efficiently.
func (c *IndexCache) PutBatch(streamID string, entries []CachedIndexEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, ce := range entries {
		key, err := keys.OffsetIndexKeyPath(streamID, ce.Entry.EndOffset, ce.Entry.CumulativeSize)
		if err != nil {
			continue
		}
		c.putLocked(streamID, key, &ce.Entry, ce.Version)
	}
}

// GetEntriesInRange returns cached entries for a stream where endOffset > fetchOffset.
// This is useful for serving fetch requests from cache.
// Returns entries sorted by offset, or nil if cache cannot satisfy the request.
func (c *IndexCache) GetEntriesInRange(streamID string, fetchOffset int64, limit int) []CachedIndexEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sc, ok := c.streams[streamID]
	if !ok {
		return nil
	}

	var result []CachedIndexEntry
	for _, ce := range sc.entries {
		if ce.Entry.EndOffset > fetchOffset {
			result = append(result, ce)
		}
	}

	if len(result) == 0 {
		return nil
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Entry.EndOffset < result[j].Entry.EndOffset
	})

	if result[0].Entry.StartOffset > fetchOffset {
		return nil
	}

	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}

	return result
}

// InvalidateStream removes all cached entries for a stream.
func (c *IndexCache) InvalidateStream(streamID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if sc, ok := c.streams[streamID]; ok {
		c.totalSize -= sc.size
		delete(c.streams, streamID)
		delete(c.accessTimes, streamID)
	}
}

// InvalidateEntry removes a specific entry from the cache.
func (c *IndexCache) InvalidateEntry(streamID, indexKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sc, ok := c.streams[streamID]
	if !ok {
		return
	}

	if ce, ok := sc.entries[indexKey]; ok {
		size := estimateEntrySize(&ce.Entry)
		sc.size -= size
		c.totalSize -= size
		delete(sc.entries, indexKey)
	}
}

// InvalidateAll removes all entries from the cache.
func (c *IndexCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.streams = make(map[string]*streamCache)
	c.accessTimes = make(map[string]int64)
	c.totalSize = 0
}

// Stats returns cache statistics.
func (c *IndexCache) Stats() IndexCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalEntries := 0
	for _, sc := range c.streams {
		totalEntries += len(sc.entries)
	}

	return IndexCacheStats{
		StreamCount:    len(c.streams),
		TotalEntries:   totalEntries,
		TotalSizeBytes: c.totalSize,
		MaxSizeBytes:   c.config.MaxMemoryBytes,
	}
}

// IndexCacheStats contains cache statistics.
type IndexCacheStats struct {
	StreamCount    int
	TotalEntries   int
	TotalSizeBytes int64
	MaxSizeBytes   int64
}

// Close stops the notification watcher and releases resources.
func (c *IndexCache) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancel()
	})
	c.wg.Wait()
	return nil
}

// recordAccess updates the access time for a stream (caller must not hold lock).
func (c *IndexCache) recordAccess(streamID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recordAccessLocked(streamID)
}

// recordAccessLocked updates access time (caller must hold write lock).
func (c *IndexCache) recordAccessLocked(streamID string) {
	c.lastAccessSeq++
	c.accessTimes[streamID] = c.lastAccessSeq
}

// evictIfNeeded removes entries to stay within memory limit.
// Caller must hold write lock.
func (c *IndexCache) evictIfNeeded() {
	for c.totalSize > c.config.MaxMemoryBytes && len(c.streams) > 0 {
		// Find the least recently accessed stream
		var oldestStream string
		var oldestSeq int64 = -1
		for streamID, seq := range c.accessTimes {
			if oldestSeq < 0 || seq < oldestSeq {
				oldestSeq = seq
				oldestStream = streamID
			}
		}

		if oldestStream == "" {
			break
		}

		// Evict entries from this stream
		if sc, ok := c.streams[oldestStream]; ok {
			if len(sc.entries) == 0 {
				delete(c.streams, oldestStream)
				delete(c.accessTimes, oldestStream)
				continue
			}
			c.evictOldestEntryFromStream(sc)
			if len(sc.entries) == 0 {
				delete(c.streams, oldestStream)
				delete(c.accessTimes, oldestStream)
			}
		}
	}
}

// evictOldestEntryFromStream removes the entry with the smallest offset.
// Caller must hold write lock.
func (c *IndexCache) evictOldestEntryFromStream(sc *streamCache) {
	var oldestKey string
	var oldestOffset int64 = -1
	var oldestEntry CachedIndexEntry

	for key, ce := range sc.entries {
		if oldestOffset < 0 || ce.Entry.StartOffset < oldestOffset {
			oldestOffset = ce.Entry.StartOffset
			oldestKey = key
			oldestEntry = ce
		}
	}

	if oldestKey != "" {
		size := estimateEntrySize(&oldestEntry.Entry)
		sc.size -= size
		c.totalSize -= size
		delete(sc.entries, oldestKey)
	}
}

// estimateEntrySize returns an approximate memory footprint for an IndexEntry.
func estimateEntrySize(e *IndexEntry) int64 {
	// Base struct size + string lengths + slice overhead
	size := int64(200) // Base struct overhead estimate
	size += int64(len(e.StreamID))
	size += int64(len(e.WalID))
	size += int64(len(e.WalPath))
	size += int64(len(e.ParquetID))
	size += int64(len(e.ParquetPath))
	size += int64(len(e.IcebergDataFileID))
	size += int64(len(e.BatchIndex) * 40) // BatchIndexEntry size estimate
	return size
}

// watchNotifications listens for metadata notifications and invalidates
// cache entries when offset-index keys are modified.
func (c *IndexCache) watchNotifications() {
	defer c.wg.Done()

	logger := logging.Global().With(map[string]any{
		"component": "index_cache",
	})
	backoff := c.config.InitialBackoff

	for {
		// Check if we're closed before starting
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		// Get notification stream
		stream, err := c.store.Notifications(c.ctx)
		if err != nil {
			// Context cancelled - we're shutting down
			if c.ctx.Err() != nil {
				return
			}

			logger.Warnf("notification stream connection failed", map[string]any{
				"error":   err.Error(),
				"backoff": backoff.String(),
			})

			// Invalidate all on error and wait before retry
			c.InvalidateAll()

			select {
			case <-c.ctx.Done():
				return
			case <-time.After(backoff):
			}

			// Exponential backoff capped at maxBackoff
			backoff = time.Duration(float64(backoff) * c.config.BackoffFactor)
			if backoff > c.config.MaxBackoff {
				backoff = c.config.MaxBackoff
			}
			continue
		}

		// Reset backoff on successful connection
		backoff = c.config.InitialBackoff

		// Process notifications until error or cancellation
		c.processNotifications(stream)
		stream.Close()

		// Check if we should exit
		if c.ctx.Err() != nil {
			return
		}

		// Invalidate all after stream disconnect to ensure consistency
		logger.Warnf("notification stream disconnected", nil)
		c.InvalidateAll()
	}
}

// processNotifications handles incoming notifications from the stream.
func (c *IndexCache) processNotifications(stream metadata.NotificationStream) {
	for {
		notification, err := stream.Next(c.ctx)
		if err != nil {
			// Stream error or context cancelled
			return
		}

		// Check if this is an offset-index key
		streamID, endOffset, _, err := extractOffsetIndexKeyInfo(notification.Key)
		if err != nil {
			continue // Not an offset-index key
		}

		if notification.Deleted {
			// Index entry was deleted - invalidate from cache
			c.InvalidateEntry(streamID, notification.Key)
		} else {
			// Index entry was updated - update cache with new value
			var entry IndexEntry
			if err := json.Unmarshal(notification.Value, &entry); err == nil {
				c.Put(streamID, notification.Key, &entry, notification.Version)
			} else {
				// Can't decode - invalidate to force re-fetch
				c.InvalidateEntry(streamID, notification.Key)
			}
			// Update used to update HWM tracking, but we don't have direct access
			// The fetch path will re-check HWM as needed
			_ = endOffset // Used for potential optimization
		}
	}
}

// extractOffsetIndexKeyInfo extracts streamID, endOffset, and cumulativeSize from an index key.
// Returns an error if the key is not an offset-index key.
func extractOffsetIndexKeyInfo(key string) (streamID string, endOffset, cumulativeSize int64, err error) {
	// Offset index key format: /dray/v1/streams/<streamId>/offset-index/<offsetEndZ>/<cumulativeSizeZ>
	if !strings.HasPrefix(key, keys.StreamsPrefix+"/") {
		err = ErrStreamNotFound
		return
	}

	// Parse using keys package
	parsed, err := keys.ParseOffsetIndexKey(key)
	if err != nil {
		return "", 0, 0, err
	}
	return parsed.StreamID, parsed.OffsetEnd, parsed.CumulativeSize, nil
}
