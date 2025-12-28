package routing

import (
	"context"

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

// Ensure RegistryListerAdapter implements protocol.BrokerLister.
var _ protocol.BrokerLister = (*RegistryListerAdapter)(nil)
