package routing

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
)

// AffinityMapper computes deterministic partition affinity using rendezvous hashing.
// For each (zoneId, streamId) pair, it selects a consistent affinity broker that
// will be advertised as the partition leader for client routing purposes.
//
// Any broker can still serve requests (Dray is leaderless), but affinity helps with:
//   - Cache locality (same broker handles similar requests)
//   - Reduced hot-spotting (partitions spread evenly)
//   - Minimal reassignment when brokers join/leave (rendezvous hash property)
type AffinityMapper struct {
	registry *Registry
	mu       sync.RWMutex
	cache    map[affinityCacheKey]int32
}

type affinityCacheKey struct {
	zoneID   string
	streamID string
}

// NewAffinityMapper creates a new affinity mapper using the given broker registry.
func NewAffinityMapper(registry *Registry) *AffinityMapper {
	return &AffinityMapper{
		registry: registry,
		cache:    make(map[affinityCacheKey]int32),
	}
}

// GetAffinityBroker returns the affinity broker NodeID for the given zone and stream.
// If zoneID is empty or no brokers exist in the zone, falls back to all available brokers.
// Returns -1 if no brokers are available.
func (m *AffinityMapper) GetAffinityBroker(ctx context.Context, zoneID, streamID string) (int32, error) {
	// Try to get zone-specific brokers first
	brokers, err := m.registry.ListBrokers(ctx, zoneID)
	if err != nil {
		return -1, err
	}

	// Fall back to all brokers if zone has none
	if len(brokers) == 0 && zoneID != "" {
		brokers, err = m.registry.ListBrokers(ctx, "")
		if err != nil {
			return -1, err
		}
	}

	if len(brokers) == 0 {
		return -1, nil
	}

	// Use rendezvous hashing to select broker
	nodeID := RendezvousHash(brokers, streamID)
	return nodeID, nil
}

// GetAffinityBrokerFromList returns the affinity broker from a provided list of brokers.
// This is useful when the caller already has a broker list and wants consistent hashing.
// Returns -1 if the broker list is empty.
func GetAffinityBrokerFromList(brokers []BrokerInfo, streamID string) int32 {
	if len(brokers) == 0 {
		return -1
	}
	return RendezvousHash(brokers, streamID)
}

// RendezvousHash implements highest random weight (HRW) hashing to select a broker.
// It deterministically picks the broker with the highest hash score for a given key.
//
// Properties:
//   - Consistent: same (brokers, key) always returns same result
//   - Minimal disruption: adding/removing a broker only affects keys mapped to that broker
//   - Even distribution: keys are spread uniformly across brokers
//
// The algorithm:
//  1. For each broker, compute hash(brokerID + key)
//  2. Return broker with highest hash value
func RendezvousHash(brokers []BrokerInfo, key string) int32 {
	if len(brokers) == 0 {
		return -1
	}

	if len(brokers) == 1 {
		return brokers[0].NodeID
	}

	var maxScore uint64
	var selectedNodeID int32 = -1

	for _, broker := range brokers {
		score := computeScore(broker.BrokerID, key)
		if selectedNodeID == -1 || score > maxScore {
			maxScore = score
			selectedNodeID = broker.NodeID
		}
	}

	return selectedNodeID
}

// computeScore generates a deterministic score for a broker-key pair.
// Uses FNV-1a hash which is fast and has good distribution.
func computeScore(brokerID, key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(brokerID))
	h.Write([]byte{0}) // separator
	h.Write([]byte(key))
	return h.Sum64()
}

// ClearCache clears the affinity cache. Call this when broker membership changes.
func (m *AffinityMapper) ClearCache() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = make(map[affinityCacheKey]int32)
}

// GetAffinityBrokerCached returns the cached affinity broker if available,
// otherwise computes and caches the result.
func (m *AffinityMapper) GetAffinityBrokerCached(ctx context.Context, zoneID, streamID string) (int32, error) {
	key := affinityCacheKey{zoneID: zoneID, streamID: streamID}

	// Check cache first
	m.mu.RLock()
	if nodeID, ok := m.cache[key]; ok {
		m.mu.RUnlock()
		return nodeID, nil
	}
	m.mu.RUnlock()

	// Compute and cache
	nodeID, err := m.GetAffinityBroker(ctx, zoneID, streamID)
	if err != nil {
		return -1, err
	}

	m.mu.Lock()
	m.cache[key] = nodeID
	m.mu.Unlock()

	return nodeID, nil
}

// GetAffinityBrokersForPartitions returns affinity broker NodeIDs for multiple stream IDs.
// This is useful when building metadata responses for multiple partitions.
func (m *AffinityMapper) GetAffinityBrokersForPartitions(ctx context.Context, zoneID string, streamIDs []string) (map[string]int32, error) {
	result := make(map[string]int32, len(streamIDs))

	// Get brokers once
	brokers, err := m.registry.ListBrokers(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	// Fall back to all brokers if zone has none
	if len(brokers) == 0 && zoneID != "" {
		brokers, err = m.registry.ListBrokers(ctx, "")
		if err != nil {
			return nil, err
		}
	}

	for _, streamID := range streamIDs {
		if len(brokers) == 0 {
			result[streamID] = -1
		} else {
			result[streamID] = RendezvousHash(brokers, streamID)
		}
	}

	return result, nil
}

// PartitionLeaderSelector defines the interface for selecting partition leaders.
type PartitionLeaderSelector interface {
	// SelectLeader returns the NodeID of the broker that should be the leader for
	// the given stream in the specified zone. Returns -1 if no broker is available.
	SelectLeader(ctx context.Context, zoneID, streamID string) (int32, error)
}

// Ensure AffinityMapper implements PartitionLeaderSelector.
var _ PartitionLeaderSelector = (*AffinityMapper)(nil)

// SelectLeader implements PartitionLeaderSelector.
func (m *AffinityMapper) SelectLeader(ctx context.Context, zoneID, streamID string) (int32, error) {
	return m.GetAffinityBroker(ctx, zoneID, streamID)
}

// StaticAffinityMapper provides affinity mapping from a static broker list.
// Useful for tests or when broker list doesn't change frequently.
type StaticAffinityMapper struct {
	brokers []BrokerInfo
}

// NewStaticAffinityMapper creates an affinity mapper with a fixed broker list.
func NewStaticAffinityMapper(brokers []BrokerInfo) *StaticAffinityMapper {
	return &StaticAffinityMapper{brokers: brokers}
}

// SelectLeader implements PartitionLeaderSelector for static broker list.
func (m *StaticAffinityMapper) SelectLeader(_ context.Context, zoneID, streamID string) (int32, error) {
	brokers := m.brokers
	if zoneID != "" {
		// Filter to zone-specific brokers
		var zoneBrokers []BrokerInfo
		for _, b := range m.brokers {
			if b.ZoneID == zoneID {
				zoneBrokers = append(zoneBrokers, b)
			}
		}
		if len(zoneBrokers) > 0 {
			brokers = zoneBrokers
		}
	}
	return RendezvousHash(brokers, streamID), nil
}

// Ensure StaticAffinityMapper implements PartitionLeaderSelector.
var _ PartitionLeaderSelector = (*StaticAffinityMapper)(nil)

// RendezvousHashUint64 is a variant that accepts a uint64 key directly.
// Useful when the key is already numeric (e.g., partition number).
func RendezvousHashUint64(brokers []BrokerInfo, key uint64) int32 {
	if len(brokers) == 0 {
		return -1
	}

	if len(brokers) == 1 {
		return brokers[0].NodeID
	}

	var keyBytes [8]byte
	binary.BigEndian.PutUint64(keyBytes[:], key)

	var maxScore uint64
	var selectedNodeID int32 = -1

	for _, broker := range brokers {
		h := fnv.New64a()
		h.Write([]byte(broker.BrokerID))
		h.Write([]byte{0})
		h.Write(keyBytes[:])
		score := h.Sum64()

		if selectedNodeID == -1 || score > maxScore {
			maxScore = score
			selectedNodeID = broker.NodeID
		}
	}

	return selectedNodeID
}
