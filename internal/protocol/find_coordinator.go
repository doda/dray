package protocol

import (
	"context"
	"hash/fnv"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes used in FindCoordinator responses.
const (
	errTransactorCoordinatorLoad int16 = 25
)

// CoordinatorType represents the type of coordinator being requested.
type CoordinatorType int8

const (
	// CoordinatorTypeGroup indicates a group coordinator (for consumer groups).
	CoordinatorTypeGroup CoordinatorType = 0
	// CoordinatorTypeTransaction indicates a transaction coordinator.
	CoordinatorTypeTransaction CoordinatorType = 1
)

// FindCoordinatorHandlerConfig configures the FindCoordinator handler.
type FindCoordinatorHandlerConfig struct {
	// LocalBroker is the fallback broker when no other brokers are available.
	LocalBroker BrokerInfo
	// BrokerLister provides access to registered brokers.
	BrokerLister BrokerLister
}

// FindCoordinatorHandler handles FindCoordinator (key 10) requests.
// In Dray's leaderless architecture, any broker can act as coordinator since
// all state is stored in Oxia. This handler implements the "virtual coordinator"
// model described in the spec section 12.2.
type FindCoordinatorHandler struct {
	cfg FindCoordinatorHandlerConfig
}

// NewFindCoordinatorHandler creates a new FindCoordinator handler.
func NewFindCoordinatorHandler(cfg FindCoordinatorHandlerConfig) *FindCoordinatorHandler {
	return &FindCoordinatorHandler{cfg: cfg}
}

// Handle processes a FindCoordinator request.
// The zoneID parameter is extracted from the client's client.id per spec 7.1.
func (h *FindCoordinatorHandler) Handle(ctx context.Context, version int16, req *kmsg.FindCoordinatorRequest, zoneID string) *kmsg.FindCoordinatorResponse {
	resp := kmsg.NewPtrFindCoordinatorResponse()
	resp.SetVersion(version)

	// v4+ supports batched lookups with multiple coordinator keys.
	// v0-v3 uses the single Key field.
	if version >= 4 {
		return h.handleBatched(ctx, version, req, zoneID, resp)
	}
	return h.handleSingle(ctx, version, req, zoneID, resp)
}

// handleSingle handles v0-v3 requests with a single coordinator key.
func (h *FindCoordinatorHandler) handleSingle(ctx context.Context, version int16, req *kmsg.FindCoordinatorRequest, zoneID string, resp *kmsg.FindCoordinatorResponse) *kmsg.FindCoordinatorResponse {
	coordinatorType := CoordinatorType(req.CoordinatorType)

	// Reject transaction coordinators per spec 14.3
	if coordinatorType == CoordinatorTypeTransaction {
		resp.ErrorCode = errTransactorCoordinatorLoad
		if version >= 1 {
			msg := "transaction coordinators are not supported"
			resp.ErrorMessage = &msg
		}
		resp.NodeID = -1
		resp.Host = ""
		resp.Port = 0
		return resp
	}

	// Get the coordinator broker
	broker := h.selectCoordinator(ctx, req.CoordinatorKey, zoneID)

	resp.ErrorCode = 0
	resp.NodeID = broker.NodeID
	resp.Host = broker.Host
	resp.Port = broker.Port

	return resp
}

// handleBatched handles v4+ requests with multiple coordinator keys.
func (h *FindCoordinatorHandler) handleBatched(ctx context.Context, version int16, req *kmsg.FindCoordinatorRequest, zoneID string, resp *kmsg.FindCoordinatorResponse) *kmsg.FindCoordinatorResponse {
	coordinatorType := CoordinatorType(req.CoordinatorType)

	for _, key := range req.CoordinatorKeys {
		coordinator := kmsg.NewFindCoordinatorResponseCoordinator()
		coordinator.Key = key

		// Reject transaction coordinators per spec 14.3
		if coordinatorType == CoordinatorTypeTransaction {
			coordinator.ErrorCode = errTransactorCoordinatorLoad
			msg := "transaction coordinators are not supported"
			coordinator.ErrorMessage = &msg
			coordinator.NodeID = -1
			coordinator.Host = ""
			coordinator.Port = 0
		} else {
			broker := h.selectCoordinator(ctx, key, zoneID)
			coordinator.ErrorCode = 0
			coordinator.NodeID = broker.NodeID
			coordinator.Host = broker.Host
			coordinator.Port = broker.Port
		}

		resp.Coordinators = append(resp.Coordinators, coordinator)
	}

	return resp
}

// selectCoordinator selects a coordinator broker for the given key.
// It uses zone-aware routing to prefer brokers in the client's zone,
// and uses deterministic hashing to ensure consistency.
func (h *FindCoordinatorHandler) selectCoordinator(ctx context.Context, key string, zoneID string) BrokerInfo {
	// Get brokers from the specified zone
	brokers := h.getBrokers(ctx, zoneID)
	if len(brokers) == 0 {
		return h.cfg.LocalBroker
	}

	// Use deterministic hash to select a broker for consistency.
	// The same key always maps to the same broker (within the same broker set).
	idx := hashToIndex(key, len(brokers))
	return brokers[idx]
}

// getBrokers retrieves the broker list, applying zone filtering with fallback.
func (h *FindCoordinatorHandler) getBrokers(ctx context.Context, zoneID string) []BrokerInfo {
	if h.cfg.BrokerLister == nil {
		return []BrokerInfo{h.cfg.LocalBroker}
	}

	// Try to get brokers from the specified zone first
	if zoneID != "" {
		brokers, err := h.cfg.BrokerLister.ListBrokers(ctx, zoneID)
		if err == nil && len(brokers) > 0 {
			return brokers
		}
	}

	// Fall back to all brokers if zone filtering returns empty or fails
	brokers, err := h.cfg.BrokerLister.ListBrokers(ctx, "")
	if err != nil || len(brokers) == 0 {
		return []BrokerInfo{h.cfg.LocalBroker}
	}
	return brokers
}

// hashToIndex uses FNV-1a hash to deterministically map a key to an index.
// This ensures the same key always selects the same broker for consistency.
func hashToIndex(key string, count int) int {
	if count <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(count))
}
