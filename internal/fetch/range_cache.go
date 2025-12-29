package fetch

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// RangeCacheConfig configures the ObjectRangeCache.
type RangeCacheConfig struct {
	// MaxMemoryBytes is the maximum total memory for cached WAL data.
	// When exceeded, entries are evicted in LRU order.
	// Default: 128MB.
	MaxMemoryBytes int64

	// MaxEntriesPerWAL limits cached ranges per WAL object.
	// Default: 100.
	MaxEntriesPerWAL int
}

// DefaultRangeCacheConfig returns sensible defaults for ObjectRangeCache.
func DefaultRangeCacheConfig() RangeCacheConfig {
	return RangeCacheConfig{
		MaxMemoryBytes:   128 * 1024 * 1024, // 128MB
		MaxEntriesPerWAL: 100,
	}
}

// rangeKey uniquely identifies a cached byte range within a WAL object.
type rangeKey struct {
	walPath   string
	startByte int64
	endByte   int64
}

// String returns a string representation of the range key.
func (k rangeKey) String() string {
	return fmt.Sprintf("%s[%d:%d]", k.walPath, k.startByte, k.endByte)
}

// cachedRange holds cached WAL byte range data.
type cachedRange struct {
	key       rangeKey
	data      []byte
	accessSeq int64 // LRU tracking: higher = more recently used
}

// ObjectRangeCache caches recent WAL byte ranges to avoid repeated
// object storage reads for hot partitions.
//
// The cache provides:
//   - Fast lookup of WAL chunk data by path and byte range
//   - Memory bounding with LRU eviction
//   - Automatic invalidation via metadata notifications (when WAL objects are GC'd)
//
// Per SPEC section 10.2, the cache is ephemeral, bounded, non-authoritative,
// and invalidated by Oxia notifications.
type ObjectRangeCache struct {
	config RangeCacheConfig

	mu            sync.RWMutex
	ranges        map[rangeKey]*cachedRange // all cached ranges
	walRanges     map[string][]rangeKey     // walPath -> range keys for eviction
	walIDRanges   map[string][]rangeKey     // walID -> range keys for invalidation
	totalSize     int64                     // total memory usage
	lastAccessSeq int64                     // monotonic sequence for LRU tracking

	// Notification watcher state
	store     metadata.MetadataStore
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closed    bool
	wg        sync.WaitGroup
}

// NewObjectRangeCache creates a new ObjectRangeCache.
// If store is non-nil, the cache automatically watches for WAL GC notifications
// to invalidate stale entries. Call Close() when the cache is no longer needed.
func NewObjectRangeCache(store metadata.MetadataStore, config RangeCacheConfig) *ObjectRangeCache {
	if config.MaxMemoryBytes <= 0 {
		config.MaxMemoryBytes = DefaultRangeCacheConfig().MaxMemoryBytes
	}
	if config.MaxEntriesPerWAL <= 0 {
		config.MaxEntriesPerWAL = DefaultRangeCacheConfig().MaxEntriesPerWAL
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &ObjectRangeCache{
		config:    config,
		ranges:    make(map[rangeKey]*cachedRange),
		walRanges: make(map[string][]rangeKey),
		walIDRanges: make(map[string][]rangeKey),
		store:     store,
		ctx:       ctx,
		cancel:    cancel,
	}

	if store != nil {
		c.wg.Add(1)
		go c.watchNotifications()
	}

	return c
}

// Get retrieves cached WAL data for the given path and byte range.
// Returns the data and true if found, nil and false otherwise.
// An exact range match is required.
func (c *ObjectRangeCache) Get(walPath string, startByte, endByte int64) ([]byte, bool) {
	key := rangeKey{walPath: walPath, startByte: startByte, endByte: endByte}

	c.mu.Lock()
	defer c.mu.Unlock()

	cached, ok := c.ranges[key]
	if !ok {
		return nil, false
	}

	// Update access time for LRU
	c.lastAccessSeq++
	cached.accessSeq = c.lastAccessSeq

	// Return a copy to avoid data races
	dataCopy := make([]byte, len(cached.data))
	copy(dataCopy, cached.data)
	return dataCopy, true
}

// Put adds or updates a cached byte range.
// If the cache is at capacity, older entries are evicted.
func (c *ObjectRangeCache) Put(walPath string, startByte, endByte int64, data []byte) {
	key := rangeKey{walPath: walPath, startByte: startByte, endByte: endByte}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.putLocked(key, data)
}

// putLocked adds an entry (caller must hold write lock).
func (c *ObjectRangeCache) putLocked(key rangeKey, data []byte) {
	// If entry already exists, update it
	if existing, ok := c.ranges[key]; ok {
		c.totalSize -= int64(len(existing.data))
		c.lastAccessSeq++
		existing.data = make([]byte, len(data))
		copy(existing.data, data)
		existing.accessSeq = c.lastAccessSeq
		c.totalSize += int64(len(data))
		c.evictIfNeeded()
		return
	}

	// Store a copy of the data
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	c.lastAccessSeq++
	cached := &cachedRange{
		key:       key,
		data:      dataCopy,
		accessSeq: c.lastAccessSeq,
	}
	c.ranges[key] = cached
	c.walRanges[key.walPath] = append(c.walRanges[key.walPath], key)
	if walID := walIDFromPath(key.walPath); walID != "" {
		c.walIDRanges[walID] = append(c.walIDRanges[walID], key)
	}
	c.totalSize += int64(len(data))

	// Enforce per-WAL limit
	c.enforcePerWALLimit(key.walPath)

	// Enforce memory limit
	c.evictIfNeeded()
}

// enforcePerWALLimit removes oldest ranges from a WAL if it exceeds the limit.
// Caller must hold write lock.
func (c *ObjectRangeCache) enforcePerWALLimit(walPath string) {
	keys := c.walRanges[walPath]
	for len(keys) > c.config.MaxEntriesPerWAL {
		// Find and remove the oldest entry for this WAL
		var oldestIdx int
		var oldestSeq int64 = -1
		for i, k := range keys {
			if cached, ok := c.ranges[k]; ok {
				if oldestSeq < 0 || cached.accessSeq < oldestSeq {
					oldestSeq = cached.accessSeq
					oldestIdx = i
				}
			}
		}

		if oldestSeq < 0 {
			break
		}

		// Remove the oldest entry
		oldKey := keys[oldestIdx]
		if cached, ok := c.ranges[oldKey]; ok {
			c.totalSize -= int64(len(cached.data))
			delete(c.ranges, oldKey)
		}
		keys = append(keys[:oldestIdx], keys[oldestIdx+1:]...)
	}
	c.walRanges[walPath] = keys
}

// evictIfNeeded removes entries to stay within memory limit using LRU policy.
// Caller must hold write lock.
func (c *ObjectRangeCache) evictIfNeeded() {
	for c.totalSize > c.config.MaxMemoryBytes && len(c.ranges) > 0 {
		// Find the least recently used entry globally
		var oldestKey rangeKey
		var oldestSeq int64 = -1
		for key, cached := range c.ranges {
			if oldestSeq < 0 || cached.accessSeq < oldestSeq {
				oldestSeq = cached.accessSeq
				oldestKey = key
			}
		}

		if oldestSeq < 0 {
			break
		}

		// Remove the entry
		c.removeLocked(oldestKey)
	}
}

// removeLocked removes a specific entry from the cache.
// Caller must hold write lock.
func (c *ObjectRangeCache) removeLocked(key rangeKey) {
	cached, ok := c.ranges[key]
	if !ok {
		return
	}

	c.totalSize -= int64(len(cached.data))
	delete(c.ranges, key)

	// Remove from walRanges index
	keys := c.walRanges[key.walPath]
	for i, k := range keys {
		if k == key {
			c.walRanges[key.walPath] = append(keys[:i], keys[i+1:]...)
			break
		}
	}
	if len(c.walRanges[key.walPath]) == 0 {
		delete(c.walRanges, key.walPath)
	}

	if walID := walIDFromPath(key.walPath); walID != "" {
		keys := c.walIDRanges[walID]
		for i, k := range keys {
			if k == key {
				c.walIDRanges[walID] = append(keys[:i], keys[i+1:]...)
				break
			}
		}
		if len(c.walIDRanges[walID]) == 0 {
			delete(c.walIDRanges, walID)
		}
	}
}

// InvalidateWAL removes all cached ranges for a WAL object.
func (c *ObjectRangeCache) InvalidateWAL(walPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keys := c.walRanges[walPath]
	for _, key := range keys {
		c.removeLocked(key)
	}
	delete(c.walRanges, walPath)
}

// InvalidateWALID removes all cached ranges for a WAL object by ID.
func (c *ObjectRangeCache) InvalidateWALID(walID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	keys := append([]rangeKey(nil), c.walIDRanges[walID]...)
	for _, key := range keys {
		c.removeLocked(key)
	}
	delete(c.walIDRanges, walID)
}

// InvalidateAll removes all cached entries.
func (c *ObjectRangeCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ranges = make(map[rangeKey]*cachedRange)
	c.walRanges = make(map[string][]rangeKey)
	c.walIDRanges = make(map[string][]rangeKey)
	c.totalSize = 0
}

// Stats returns cache statistics.
func (c *ObjectRangeCache) Stats() RangeCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return RangeCacheStats{
		WALCount:       len(c.walRanges),
		RangeCount:     len(c.ranges),
		TotalSizeBytes: c.totalSize,
		MaxSizeBytes:   c.config.MaxMemoryBytes,
	}
}

// RangeCacheStats contains cache statistics.
type RangeCacheStats struct {
	WALCount       int
	RangeCount     int
	TotalSizeBytes int64
	MaxSizeBytes   int64
}

// Close stops the notification watcher and releases resources.
func (c *ObjectRangeCache) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.cancel()
	})
	c.wg.Wait()
	return nil
}

// watchNotifications listens for metadata notifications and invalidates
// cache entries when WAL objects are deleted (via GC).
func (c *ObjectRangeCache) watchNotifications() {
	defer c.wg.Done()

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
			// Invalidate all on error and retry
			c.InvalidateAll()
			continue
		}

		// Process notifications until error or cancellation
		c.processNotifications(stream)
		stream.Close()

		// Check if we should exit
		if c.ctx.Err() != nil {
			return
		}

		// Invalidate all after stream disconnect to ensure consistency
		c.InvalidateAll()
	}
}

// processNotifications handles incoming notifications from the stream.
func (c *ObjectRangeCache) processNotifications(stream metadata.NotificationStream) {
	for {
		notification, err := stream.Next(c.ctx)
		if err != nil {
			// Stream error or context cancelled
			return
		}

		// Check if this is a WAL GC notification (WAL object being deleted)
		// GC keys: /dray/v1/wal/gc/<metaDomain>/<walId>
		// WAL object keys: /dray/v1/wal/objects/<metaDomain>/<walId>
		if notification.Deleted {
			walID := extractWALIDFromNotification(notification.Key)
			if walID != "" {
				c.InvalidateWALID(walID)
			}
		}
	}
}

// extractWALIDFromNotification extracts the WAL object ID from a notification key.
// Returns empty string if the key is not a WAL-related key.
func extractWALIDFromNotification(key string) string {
	// Handle WAL object deletion notifications
	// Key format: /dray/v1/wal/objects/<metaDomain>/<walId>
	if strings.HasPrefix(key, keys.WALObjectsPrefix+"/") {
		suffix := strings.TrimPrefix(key, keys.WALObjectsPrefix+"/")
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) == 2 {
			return parts[1]
		}
	}

	// Handle WAL GC marker notifications
	// Key format: /dray/v1/wal/gc/<metaDomain>/<walId>
	if strings.HasPrefix(key, keys.WALGCPrefix+"/") {
		suffix := strings.TrimPrefix(key, keys.WALGCPrefix+"/")
		parts := strings.SplitN(suffix, "/", 2)
		if len(parts) == 2 {
			return parts[1]
		}
	}

	return ""
}

func walIDFromPath(walPath string) string {
	if walPath == "" {
		return ""
	}
	parts := strings.Split(walPath, "/")
	last := parts[len(parts)-1]
	if last == "" {
		return ""
	}
	switch {
	case strings.HasSuffix(last, ".wo"):
		return strings.TrimSuffix(last, ".wo")
	case strings.HasSuffix(last, ".wal"):
		return strings.TrimSuffix(last, ".wal")
	default:
		return ""
	}
}
