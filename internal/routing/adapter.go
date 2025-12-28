package routing

import (
	"context"
	"fmt"

	"github.com/dray-io/dray/internal/protocol"
)

// RegistryListerAdapter adapts a Registry to the protocol.BrokerLister interface.
// This allows the Registry to be used directly with the metadata and
// describe-cluster handlers.
type RegistryListerAdapter struct {
	registry *Registry
}

// NewRegistryListerAdapter creates a new adapter for the given registry.
func NewRegistryListerAdapter(registry *Registry) *RegistryListerAdapter {
	return &RegistryListerAdapter{registry: registry}
}

// ListBrokers returns all registered brokers, optionally filtered by zone.
// Converts routing.BrokerInfo to protocol.BrokerInfo.
func (a *RegistryListerAdapter) ListBrokers(ctx context.Context, zoneID string) ([]protocol.BrokerInfo, error) {
	brokers, err := a.registry.ListBrokers(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	result := make([]protocol.BrokerInfo, len(brokers))
	for i, b := range brokers {
		result[i] = protocol.BrokerInfo{
			NodeID: b.NodeID,
			Host:   b.Host(),
			Port:   b.Port(),
			Rack:   b.ZoneID,
		}
	}
	return result, nil
}

// GetPartitionLeader returns the affinity broker for the given partition using
// rendezvous hashing. This provides deterministic broker assignment for partitions
// that minimizes reassignment when brokers join/leave.
//
// Parameters:
//   - zoneID: if non-empty, selects from brokers in this zone first
//   - topic: the topic name
//   - partition: the partition number
//
// Returns the NodeID of the affinity broker, or -1 if no brokers are available.
func (a *RegistryListerAdapter) GetPartitionLeader(ctx context.Context, zoneID, topic string, partition int32) (int32, error) {
	brokers, err := a.registry.ListBrokers(ctx, zoneID)
	if err != nil {
		return -1, err
	}

	// Fall back to all brokers if zone has none
	if len(brokers) == 0 && zoneID != "" {
		brokers, err = a.registry.ListBrokers(ctx, "")
		if err != nil {
			return -1, err
		}
	}

	if len(brokers) == 0 {
		return -1, nil
	}

	// Use topic-partition as the stream ID for rendezvous hashing
	streamID := fmt.Sprintf("%s-%d", topic, partition)
	return RendezvousHash(brokers, streamID), nil
}

// Ensure RegistryListerAdapter implements protocol.BrokerLister.
var _ protocol.BrokerLister = (*RegistryListerAdapter)(nil)

// Ensure RegistryListerAdapter implements protocol.PartitionLeaderSelector.
var _ protocol.PartitionLeaderSelector = (*RegistryListerAdapter)(nil)

// AffinityListerAdapter extends RegistryListerAdapter with affinity-aware leader selection.
// It implements both protocol.BrokerLister and protocol.PartitionLeaderSelector.
type AffinityListerAdapter struct {
	*RegistryListerAdapter
}

// NewAffinityListerAdapter creates an adapter that provides affinity-based partition
// leader selection using rendezvous hashing.
func NewAffinityListerAdapter(registry *Registry) *AffinityListerAdapter {
	return &AffinityListerAdapter{
		RegistryListerAdapter: NewRegistryListerAdapter(registry),
	}
}

// Ensure AffinityListerAdapter implements protocol.BrokerLister.
var _ protocol.BrokerLister = (*AffinityListerAdapter)(nil)

// Ensure AffinityListerAdapter implements protocol.PartitionLeaderSelector.
var _ protocol.PartitionLeaderSelector = (*AffinityListerAdapter)(nil)
