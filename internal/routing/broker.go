// Package routing implements zone-aware routing and broker registration.
package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// BrokerInfo holds information about a registered broker.
type BrokerInfo struct {
	// BrokerID is the unique identifier for this broker.
	BrokerID string `json:"brokerId"`

	// NodeID is the numeric ID used in Kafka protocol responses.
	NodeID int32 `json:"nodeId"`

	// ZoneID is the availability zone where this broker runs.
	ZoneID string `json:"zoneId"`

	// AdvertisedListeners is the list of host:port addresses clients use to connect.
	AdvertisedListeners []string `json:"advertisedListeners"`

	// StartedAt is the Unix timestamp (milliseconds) when the broker started.
	StartedAt int64 `json:"startedAt"`

	// BuildInfo contains version and build metadata.
	BuildInfo BuildInfo `json:"buildInfo"`
}

// BuildInfo contains broker version and build metadata.
type BuildInfo struct {
	// Version is the Dray version string.
	Version string `json:"version"`

	// GitCommit is the git commit hash at build time.
	GitCommit string `json:"gitCommit"`

	// BuildTime is when the binary was built.
	BuildTime string `json:"buildTime"`
}

// Host returns the hostname from the first advertised listener.
func (b *BrokerInfo) Host() string {
	if len(b.AdvertisedListeners) == 0 {
		return ""
	}
	host, _, err := net.SplitHostPort(b.AdvertisedListeners[0])
	if err != nil {
		return b.AdvertisedListeners[0]
	}
	return host
}

// Port returns the port from the first advertised listener.
func (b *BrokerInfo) Port() int32 {
	if len(b.AdvertisedListeners) == 0 {
		return 0
	}
	_, portStr, err := net.SplitHostPort(b.AdvertisedListeners[0])
	if err != nil {
		return 0
	}
	port, _ := strconv.ParseInt(portStr, 10, 32)
	return int32(port)
}

// RegistryConfig configures the broker registry.
type RegistryConfig struct {
	// ClusterID is the identifier for this Dray cluster.
	ClusterID string

	// BrokerID is the unique identifier for this broker.
	BrokerID string

	// NodeID is the numeric Kafka node ID for this broker.
	NodeID int32

	// ZoneID is the availability zone for this broker.
	ZoneID string

	// AdvertisedListeners are the host:port addresses advertised to clients.
	AdvertisedListeners []string

	// BuildInfo contains version and build metadata.
	BuildInfo BuildInfo

	// Logger for registration events.
	Logger *logging.Logger
}

// Registry manages broker registration and discovery using Oxia ephemeral keys.
// Brokers register themselves on startup, and the registration is automatically
// cleaned up when the broker's session expires (e.g., due to crash or shutdown).
type Registry struct {
	store  metadata.MetadataStore
	config RegistryConfig
	logger *logging.Logger

	mu         sync.RWMutex
	registered bool
	startedAt  int64
}

// NewRegistry creates a new broker registry.
func NewRegistry(store metadata.MetadataStore, config RegistryConfig) *Registry {
	logger := config.Logger
	if logger == nil {
		logger = logging.DefaultLogger()
	}

	return &Registry{
		store:     store,
		config:    config,
		logger:    logger,
		startedAt: time.Now().UnixMilli(),
	}
}

// Register registers this broker with an ephemeral key.
// The key will be automatically deleted when the session ends.
func (r *Registry) Register(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	info := BrokerInfo{
		BrokerID:            r.config.BrokerID,
		NodeID:              r.config.NodeID,
		ZoneID:              r.config.ZoneID,
		AdvertisedListeners: r.config.AdvertisedListeners,
		StartedAt:           r.startedAt,
		BuildInfo:           r.config.BuildInfo,
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal broker info: %w", err)
	}

	key := keys.BrokerKeyPath(r.config.ClusterID, r.config.BrokerID)

	_, err = r.store.PutEphemeral(ctx, key, data)
	if err != nil {
		return fmt.Errorf("failed to register broker: %w", err)
	}

	r.registered = true
	r.logger.Infof("broker registered", map[string]any{
		"brokerId":  r.config.BrokerID,
		"nodeId":    r.config.NodeID,
		"zoneId":    r.config.ZoneID,
		"listeners": r.config.AdvertisedListeners,
		"key":       key,
	})

	return nil
}

// Deregister explicitly removes the broker registration.
// This is optional since ephemeral keys are automatically cleaned up on session end.
func (r *Registry) Deregister(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.registered {
		return nil
	}

	key := keys.BrokerKeyPath(r.config.ClusterID, r.config.BrokerID)

	if err := r.store.Delete(ctx, key); err != nil {
		return fmt.Errorf("failed to deregister broker: %w", err)
	}

	r.registered = false
	r.logger.Infof("broker deregistered", map[string]any{
		"brokerId": r.config.BrokerID,
		"key":      key,
	})

	return nil
}

// ListBrokers returns all registered brokers in the cluster.
// If zoneID is non-empty, filters to return only brokers in that zone.
func (r *Registry) ListBrokers(ctx context.Context, zoneID string) ([]BrokerInfo, error) {
	prefix := keys.BrokersPrefix(r.config.ClusterID)

	kvs, err := r.store.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("failed to list brokers: %w", err)
	}

	var brokers []BrokerInfo
	for _, kv := range kvs {
		var info BrokerInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			r.logger.Warnf("failed to unmarshal broker info", map[string]any{
				"key":   kv.Key,
				"error": err.Error(),
			})
			continue
		}

		if zoneID != "" && info.ZoneID != zoneID {
			continue
		}

		brokers = append(brokers, info)
	}

	return brokers, nil
}

// GetBroker retrieves information about a specific broker.
func (r *Registry) GetBroker(ctx context.Context, brokerID string) (BrokerInfo, bool, error) {
	key := keys.BrokerKeyPath(r.config.ClusterID, brokerID)

	result, err := r.store.Get(ctx, key)
	if err != nil {
		return BrokerInfo{}, false, fmt.Errorf("failed to get broker: %w", err)
	}

	if !result.Exists {
		return BrokerInfo{}, false, nil
	}

	var info BrokerInfo
	if err := json.Unmarshal(result.Value, &info); err != nil {
		return BrokerInfo{}, false, fmt.Errorf("failed to unmarshal broker info: %w", err)
	}

	return info, true, nil
}

// IsRegistered returns whether this broker is currently registered.
func (r *Registry) IsRegistered() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registered
}

// BrokerInfo returns the current broker's information.
func (r *Registry) BrokerInfo() BrokerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return BrokerInfo{
		BrokerID:            r.config.BrokerID,
		NodeID:              r.config.NodeID,
		ZoneID:              r.config.ZoneID,
		AdvertisedListeners: r.config.AdvertisedListeners,
		StartedAt:           r.startedAt,
		BuildInfo:           r.config.BuildInfo,
	}
}

// LocalBrokerID returns this broker's ID.
func (r *Registry) LocalBrokerID() string {
	return r.config.BrokerID
}

// ClusterID returns the cluster ID this registry is associated with.
func (r *Registry) ClusterID() string {
	return r.config.ClusterID
}

// ParseZoneID extracts zone_id from a Kafka client.id string.
// Per spec section 7.1, client.id may contain comma-separated k=v pairs.
// Example: "zone_id=us-east-1a,app=myservice"
func ParseZoneID(clientID string) string {
	if clientID == "" {
		return ""
	}

	for _, part := range strings.Split(clientID, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "zone_id=") {
			return strings.TrimPrefix(part, "zone_id=")
		}
	}

	return ""
}
