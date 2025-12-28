package protocol

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeClusterHandlerConfig configures the DescribeCluster handler.
type DescribeClusterHandlerConfig struct {
	ClusterID    string
	ControllerID int32
	LocalBroker  BrokerInfo
	BrokerLister BrokerLister
}

// DescribeClusterHandler handles DescribeCluster (key 60) requests.
type DescribeClusterHandler struct {
	cfg DescribeClusterHandlerConfig
}

// NewDescribeClusterHandler creates a new DescribeCluster handler.
func NewDescribeClusterHandler(cfg DescribeClusterHandlerConfig) *DescribeClusterHandler {
	return &DescribeClusterHandler{
		cfg: cfg,
	}
}

// Handle processes a DescribeCluster request.
func (h *DescribeClusterHandler) Handle(ctx context.Context, version int16, req *kmsg.DescribeClusterRequest) *kmsg.DescribeClusterResponse {
	resp := kmsg.NewPtrDescribeClusterResponse()
	resp.SetVersion(version)

	resp.ThrottleMillis = 0
	resp.ErrorCode = 0
	resp.ClusterID = h.cfg.ClusterID

	// For Dray's leaderless architecture, any broker can be the controller.
	// We return the configured ControllerID which is typically set to the local broker.
	resp.ControllerID = h.cfg.ControllerID

	// Get broker list
	brokers := h.getBrokers(ctx)
	for _, b := range brokers {
		broker := kmsg.NewDescribeClusterResponseBroker()
		broker.NodeID = b.NodeID
		broker.Host = b.Host
		broker.Port = b.Port
		if b.Rack != "" {
			broker.Rack = &b.Rack
		}
		resp.Brokers = append(resp.Brokers, broker)
	}

	// For v1+, set EndpointType (1=brokers, 2=controllers)
	// Since Dray is leaderless and all brokers can handle requests,
	// we return type 1 (brokers) by default
	if version >= 1 {
		resp.EndpointType = 1
	}

	// ClusterAuthorizedOperations - if requested and version supports it
	// Default to -2147483648 (0x80000000) which means "not available"
	// When IncludeClusterAuthorizedOperations is true, we could return
	// the operations the caller is authorized to perform.
	// For now, we return the default indicating not supported.
	resp.ClusterAuthorizedOperations = -2147483648

	return resp
}

// getBrokers retrieves the broker list from the broker lister or falls back to local broker.
func (h *DescribeClusterHandler) getBrokers(ctx context.Context) []BrokerInfo {
	if h.cfg.BrokerLister == nil {
		return []BrokerInfo{h.cfg.LocalBroker}
	}

	brokers, err := h.cfg.BrokerLister.ListBrokers(ctx, "")
	if err != nil || len(brokers) == 0 {
		return []BrokerInfo{h.cfg.LocalBroker}
	}
	return brokers
}
