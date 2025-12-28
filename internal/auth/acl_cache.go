package auth

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// ACLCache provides a cached view of ACLs with notification-based invalidation.
type ACLCache struct {
	store  *ACLStore
	meta   metadata.MetadataStore
	mu     sync.RWMutex
	acls   map[string]*ACLEntry // key -> entry
	loaded bool
	cancel context.CancelFunc
	done   chan struct{}
}

// NewACLCache creates a new ACL cache wrapping the given store.
func NewACLCache(store *ACLStore, meta metadata.MetadataStore) *ACLCache {
	return &ACLCache{
		store: store,
		meta:  meta,
		acls:  make(map[string]*ACLEntry),
		done:  make(chan struct{}),
	}
}

// Start begins watching for ACL changes.
func (c *ACLCache) Start(ctx context.Context) error {
	// Load all ACLs initially
	if err := c.reload(ctx); err != nil {
		return err
	}

	// Start notification watcher
	watchCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	stream, err := c.meta.Notifications(watchCtx)
	if err != nil {
		cancel()
		return err
	}

	go c.watchLoop(watchCtx, stream)

	return nil
}

// Stop stops the cache watcher.
func (c *ACLCache) Stop() {
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
}

// reload loads all ACLs from the store.
func (c *ACLCache) reload(ctx context.Context) error {
	entries, err := c.store.ListACLs(ctx, nil)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.acls = make(map[string]*ACLEntry, len(entries))
	for _, entry := range entries {
		key := aclKeyPath(entry)
		c.acls[key] = entry
	}
	c.loaded = true

	return nil
}

// watchLoop processes notifications and invalidates cache entries.
func (c *ACLCache) watchLoop(ctx context.Context, stream metadata.NotificationStream) {
	defer close(c.done)
	defer stream.Close()

	for {
		notification, err := stream.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		// Only process ACL key changes
		if !strings.HasPrefix(notification.Key, keys.ACLsPrefix+"/") {
			continue
		}

		c.mu.Lock()
		if notification.Deleted {
			delete(c.acls, notification.Key)
		} else {
			var entry ACLEntry
			if err := json.Unmarshal(notification.Value, &entry); err == nil {
				c.acls[notification.Key] = &entry
			}
		}
		c.mu.Unlock()
	}
}

// GetACLs returns all cached ACLs matching the filter.
func (c *ACLCache) GetACLs(filter *ACLFilter) []*ACLEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []*ACLEntry
	for _, entry := range c.acls {
		if filter == nil || filter.Matches(entry) {
			entryCopy := *entry
			result = append(result, &entryCopy)
		}
	}

	return result
}

// Authorize checks if the given action is allowed.
// Returns true if explicitly allowed, false if explicitly denied or no matching rule.
// Deny rules take precedence over allow rules.
func (c *ACLCache) Authorize(resourceType ResourceType, resourceName, principal, host string, operation Operation) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var allowMatches []*ACLEntry
	var denyMatches []*ACLEntry

	for _, entry := range c.acls {
		if !c.matchesRequest(entry, resourceType, resourceName, principal, host, operation) {
			continue
		}

		if entry.Permission == PermissionDeny {
			denyMatches = append(denyMatches, entry)
		} else if entry.Permission == PermissionAllow {
			allowMatches = append(allowMatches, entry)
		}
	}

	// Deny takes precedence
	if len(denyMatches) > 0 {
		return false
	}

	// Allow if any allow rule matches
	return len(allowMatches) > 0
}

// matchesRequest checks if an ACL entry matches the given request.
func (c *ACLCache) matchesRequest(entry *ACLEntry, resourceType ResourceType, resourceName, principal, host string, operation Operation) bool {
	// Check resource type
	if entry.ResourceType != resourceType {
		return false
	}

	// Check resource name with pattern matching
	switch entry.PatternType {
	case PatternTypeLiteral:
		if entry.ResourceName != resourceName && entry.ResourceName != "*" {
			return false
		}
	case PatternTypePrefixed:
		if !strings.HasPrefix(resourceName, entry.ResourceName) {
			return false
		}
	default:
		return false
	}

	// Check principal
	if entry.Principal != principal && entry.Principal != "User:*" {
		return false
	}

	// Check host
	if entry.Host != host && entry.Host != "*" {
		return false
	}

	// Check operation
	if entry.Operation != operation && entry.Operation != OperationAll {
		return false
	}

	return true
}

// IsLoaded returns true if the cache has been loaded.
func (c *ACLCache) IsLoaded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loaded
}

// Count returns the number of cached ACL entries.
func (c *ACLCache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.acls)
}

// Invalidate forces a full reload of the cache.
func (c *ACLCache) Invalidate(ctx context.Context) error {
	return c.reload(ctx)
}
