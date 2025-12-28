package protocol

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDescribeClusterHandler_ReturnsClusterID(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster-123",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	if resp.ClusterID != "test-cluster-123" {
		t.Errorf("expected cluster ID 'test-cluster-123', got '%s'", resp.ClusterID)
	}
}

func TestDescribeClusterHandler_ReturnsControllerID(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "my-cluster",
		ControllerID: 42,
		LocalBroker: BrokerInfo{
			NodeID: 42,
			Host:   "broker-42.example.com",
			Port:   9092,
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	// In Dray's leaderless architecture, any broker can be the controller.
	// The ControllerID should match what's configured.
	if resp.ControllerID != 42 {
		t.Errorf("expected controller ID 42, got %d", resp.ControllerID)
	}
}

func TestDescribeClusterHandler_ReturnsBrokerList(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
			Rack:   "rack-a",
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}

	broker := resp.Brokers[0]
	if broker.NodeID != 1 {
		t.Errorf("expected broker NodeID 1, got %d", broker.NodeID)
	}
	if broker.Host != "localhost" {
		t.Errorf("expected broker host 'localhost', got '%s'", broker.Host)
	}
	if broker.Port != 9092 {
		t.Errorf("expected broker port 9092, got %d", broker.Port)
	}
	if broker.Rack == nil || *broker.Rack != "rack-a" {
		t.Errorf("expected broker rack 'rack-a', got %v", broker.Rack)
	}
}

func TestDescribeClusterHandler_BrokerWithoutRack(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
			Rack:   "", // No rack
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}

	broker := resp.Brokers[0]
	if broker.Rack != nil {
		t.Errorf("expected broker rack to be nil, got '%s'", *broker.Rack)
	}
}

func TestDescribeClusterHandler_MultipleBrokers(t *testing.T) {
	ctx := context.Background()

	mockLister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker-1.example.com", Port: 9092, Rack: "us-east-1a"},
			{NodeID: 2, Host: "broker-2.example.com", Port: 9092, Rack: "us-east-1b"},
			{NodeID: 3, Host: "broker-3.example.com", Port: 9092, Rack: "us-east-1c"},
		},
	}

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "prod-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "broker-1.example.com",
			Port:   9092,
		},
		BrokerLister: mockLister,
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	if len(resp.Brokers) != 3 {
		t.Fatalf("expected 3 brokers, got %d", len(resp.Brokers))
	}

	// Verify all brokers are present
	nodeIDs := make(map[int32]bool)
	for _, b := range resp.Brokers {
		nodeIDs[b.NodeID] = true
	}

	if !nodeIDs[1] || !nodeIDs[2] || !nodeIDs[3] {
		t.Errorf("expected node IDs 1, 2, 3 but got %v", nodeIDs)
	}
}

func TestDescribeClusterHandler_V0Response(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	// V0 should set basic fields
	if resp.ClusterID != "test-cluster" {
		t.Errorf("expected cluster ID 'test-cluster', got '%s'", resp.ClusterID)
	}
	if resp.ControllerID != 1 {
		t.Errorf("expected controller ID 1, got %d", resp.ControllerID)
	}

	// Verify throttle is 0
	if resp.ThrottleMillis != 0 {
		t.Errorf("expected ThrottleMillis 0, got %d", resp.ThrottleMillis)
	}
}

func TestDescribeClusterHandler_V1Response(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 1, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	// V1+ should set EndpointType (1=brokers)
	if resp.EndpointType != 1 {
		t.Errorf("expected EndpointType 1 (brokers), got %d", resp.EndpointType)
	}
}

func TestDescribeClusterHandler_ClusterAuthorizedOperations(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	req.IncludeClusterAuthorizedOperations = true
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	// When ACL support isn't implemented, the default value indicates "not available"
	// The default is -2147483648 (0x80000000)
	if resp.ClusterAuthorizedOperations != -2147483648 {
		t.Errorf("expected ClusterAuthorizedOperations to be -2147483648 (not available), got %d", resp.ClusterAuthorizedOperations)
	}
}

func TestDescribeClusterHandler_EmptyBrokerLister(t *testing.T) {
	ctx := context.Background()

	// Broker lister that returns no brokers
	mockLister := &mockBrokerLister{
		brokers: []BrokerInfo{},
	}

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "fallback-host",
			Port:   9092,
		},
		BrokerLister: mockLister,
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	// Should fall back to local broker
	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker (fallback), got %d", len(resp.Brokers))
	}

	if resp.Brokers[0].Host != "fallback-host" {
		t.Errorf("expected fallback broker host 'fallback-host', got '%s'", resp.Brokers[0].Host)
	}
}

func TestDescribeClusterHandler_NilBrokerLister(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "local-host",
			Port:   9092,
		},
		BrokerLister: nil, // No broker lister
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	// Should use local broker
	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}

	if resp.Brokers[0].Host != "local-host" {
		t.Errorf("expected local broker host 'local-host', got '%s'", resp.Brokers[0].Host)
	}
}

func TestDescribeClusterHandler_ErrorMessageIsNilOnSuccess(t *testing.T) {
	ctx := context.Background()

	handler := NewDescribeClusterHandler(DescribeClusterHandlerConfig{
		ClusterID:    "test-cluster",
		ControllerID: 1,
		LocalBroker: BrokerInfo{
			NodeID: 1,
			Host:   "localhost",
			Port:   9092,
		},
	})

	req := kmsg.NewPtrDescribeClusterRequest()
	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.ErrorCode)
	}

	// On success, ErrorMessage should remain nil
	if resp.ErrorMessage != nil {
		t.Errorf("expected nil error message on success, got '%s'", *resp.ErrorMessage)
	}
}
