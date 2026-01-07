package index

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// HWM cache watcher backoff configuration defaults.
const (
	defaultHWMCacheInitialBackoff = 100 * time.Millisecond
	defaultHWMCacheMaxBackoff     = 30 * time.Second
	defaultHWMCacheBackoffFactor  = 2.0
)

// CachedHWM represents a cached high watermark value with revision stamping.
type CachedHWM struct {
	// HWM is the high watermark value.
	HWM int64
	// Version is the metadata store version for this hwm.
	// Used for session monotonicity - cached entries are only valid
	// if the version matches the current store version.
	Version metadata.Version
}

// HWMCache caches high watermark values with revision stamping.
// It subscribes to metadata notifications to invalidate entries when
// the underlying hwm value changes, ensuring read-your-writes consistency.
//
// The cache guarantees session monotonicity: a cached value is only used
// if its version is >= the version at the time of the original read.
type HWMCache struct {
	store metadata.MetadataStore

	mu    sync.RWMutex
	cache map[string]CachedHWM // streamID -> CachedHWM

	// Background notification watcher state
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closed    bool
	wg        sync.WaitGroup

	// Backoff configuration for watch loop errors.
	initialBackoff time.Duration
	maxBackoff     time.Duration
	backoffFactor  float64
}

// HWMCacheOption configures an HWMCache.
type HWMCacheOption func(*HWMCache)

// WithHWMBackoff configures backoff parameters for the HWM watch loop.
func WithHWMBackoff(initial, max time.Duration, factor float64) HWMCacheOption {
	return func(c *HWMCache) {
		if initial > 0 {
			c.initialBackoff = initial
		}
		if max > 0 {
			c.maxBackoff = max
		}
		if factor > 1.0 {
			c.backoffFactor = factor
		}
	}
}

// NewHWMCache creates a new HWM cache backed by the given metadata store.
// The cache automatically starts watching for notifications to invalidate
// stale entries. Call Close() when the cache is no longer needed.
func NewHWMCache(store metadata.MetadataStore, opts ...HWMCacheOption) *HWMCache {
	ctx, cancel := context.WithCancel(context.Background())
	c := &HWMCache{
		store:          store,
		cache:          make(map[string]CachedHWM),
		ctx:            ctx,
		cancel:         cancel,
		initialBackoff: defaultHWMCacheInitialBackoff,
		maxBackoff:     defaultHWMCacheMaxBackoff,
		backoffFactor:  defaultHWMCacheBackoffFactor,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.wg.Add(1)
	go c.watchNotifications()
	return c
}

// Get retrieves the high watermark for a stream, using the cache if available.
// Returns the hwm value, version, and any error.
//
// If the cache has a valid entry (version matches or is newer), it returns
// the cached value. Otherwise, it fetches from the store and caches the result.
func (c *HWMCache) Get(ctx context.Context, streamID string) (int64, metadata.Version, error) {
	// Check cache first
	c.mu.RLock()
	if cached, ok := c.cache[streamID]; ok {
		c.mu.RUnlock()
		return cached.HWM, cached.Version, nil
	}
	c.mu.RUnlock()

	// Cache miss - fetch from store
	hwmKey := keys.HwmKeyPath(streamID)
	result, err := c.store.Get(ctx, hwmKey)
	if err != nil {
		return 0, 0, err
	}
	if !result.Exists {
		return 0, 0, ErrStreamNotFound
	}

	hwm, err := DecodeHWM(result.Value)
	if err != nil {
		return 0, 0, err
	}

	// Update cache
	c.mu.Lock()
	c.cache[streamID] = CachedHWM{HWM: hwm, Version: result.Version}
	c.mu.Unlock()

	return hwm, result.Version, nil
}

// GetIfCached returns the cached hwm if available, without fetching from store.
// Returns (hwm, version, true) if cached, or (0, 0, false) if not cached.
func (c *HWMCache) GetIfCached(streamID string) (int64, metadata.Version, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if cached, ok := c.cache[streamID]; ok {
		return cached.HWM, cached.Version, true
	}
	return 0, 0, false
}

// Put updates the cache with a known hwm value and version.
// This should be called after a successful write to keep the cache consistent.
// The entry is only updated if the new version is >= the cached version
// to maintain monotonicity.
func (c *HWMCache) Put(streamID string, hwm int64, version metadata.Version) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Only update if version is newer or entry doesn't exist
	if existing, ok := c.cache[streamID]; !ok || version >= existing.Version {
		c.cache[streamID] = CachedHWM{HWM: hwm, Version: version}
	}
}

// Invalidate removes a specific stream's hwm from the cache.
// This is typically called when a notification indicates the hwm has changed.
func (c *HWMCache) Invalidate(streamID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, streamID)
}

// InvalidateAll removes all entries from the cache.
// This is useful when the notification stream is restarted and we need
// to ensure consistency.
func (c *HWMCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]CachedHWM)
}

// Size returns the number of entries in the cache.
func (c *HWMCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// Close stops the notification watcher and releases resources.
func (c *HWMCache) Close() error {
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
// cache entries when hwm keys are modified.
func (c *HWMCache) watchNotifications() {
	defer c.wg.Done()
	logger := logging.Global().With(map[string]any{
		"component": "hwm_cache",
	})
	var pendingRefresh []string
	var disconnected bool
	backoff := c.initialBackoff

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
			backoff = time.Duration(float64(backoff) * c.backoffFactor)
			if backoff > c.maxBackoff {
				backoff = c.maxBackoff
			}
			continue
		}

		// Reset backoff on successful connection
		backoff = c.initialBackoff

		if disconnected {
			logger.Infof("notification stream reconnected", map[string]any{
				"cachedStreams": len(pendingRefresh),
			})
			refreshed, failed := c.refreshCachedHWMs(pendingRefresh)
			if refreshed+failed > 0 {
				logger.Infof("hwm cache refreshed after reconnect", map[string]any{
					"refreshed": refreshed,
					"failed":    failed,
				})
			}
			// Variables are reset in the error path below (lines 277-278)
		}

		// Process notifications until error or cancellation
		err = c.processNotifications(stream)
		stream.Close()

		// Check if we should exit
		if c.ctx.Err() != nil {
			return
		}

		// Invalidate all after stream disconnect to ensure consistency
		if err != nil {
			logger.Warnf("notification stream disconnected", map[string]any{
				"error": err.Error(),
			})
		}
		pendingRefresh = c.cachedStreamIDs()
		disconnected = true
		c.InvalidateAll()
	}
}

// processNotifications handles incoming notifications from the stream.
func (c *HWMCache) processNotifications(stream metadata.NotificationStream) error {
	for {
		notification, err := stream.Next(c.ctx)
		if err != nil {
			// Stream error or context cancelled
			return err
		}

		// Check if this is an hwm key
		streamID := extractStreamIDFromHWMKey(notification.Key)
		if streamID == "" {
			continue
		}

		if notification.Deleted {
			// hwm was deleted - invalidate cache
			c.Invalidate(streamID)
		} else {
			// hwm was updated - update cache with new value if we can decode it
			hwm, err := DecodeHWM(notification.Value)
			if err == nil {
				c.Put(streamID, hwm, notification.Version)
			} else {
				// Can't decode - invalidate to force re-fetch
				c.Invalidate(streamID)
			}
		}
	}
}

func (c *HWMCache) cachedStreamIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	streamIDs := make([]string, 0, len(c.cache))
	for streamID := range c.cache {
		streamIDs = append(streamIDs, streamID)
	}
	return streamIDs
}

func (c *HWMCache) refreshCachedHWMs(streamIDs []string) (int, int) {
	if len(streamIDs) == 0 {
		return 0, 0
	}

	refreshed := 0
	failed := 0
	for _, streamID := range streamIDs {
		hwmKey := keys.HwmKeyPath(streamID)
		result, err := c.store.Get(c.ctx, hwmKey)
		if err != nil {
			failed++
			continue
		}
		if !result.Exists {
			failed++
			continue
		}
		hwm, err := DecodeHWM(result.Value)
		if err != nil {
			failed++
			continue
		}
		c.Put(streamID, hwm, result.Version)
		refreshed++
	}
	return refreshed, failed
}

// extractStreamIDFromHWMKey extracts the stream ID from an hwm key.
// Returns empty string if the key is not an hwm key.
//
// HWM key format: /dray/v1/streams/<streamId>/hwm
func extractStreamIDFromHWMKey(key string) string {
	const hwmSuffix = "/hwm"
	if !strings.HasSuffix(key, hwmSuffix) {
		return ""
	}
	if !strings.HasPrefix(key, keys.StreamsPrefix+"/") {
		return ""
	}

	// Remove prefix and suffix to get stream ID
	remaining := strings.TrimPrefix(key, keys.StreamsPrefix+"/")
	remaining = strings.TrimSuffix(remaining, hwmSuffix)

	// Remaining should be just the stream ID (no slashes)
	if strings.Contains(remaining, "/") {
		return ""
	}

	return remaining
}
