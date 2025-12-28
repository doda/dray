package protocol

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestFindCoordinatorHandler_GroupCoordinator(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"},
			{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-c"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"zone-a": {{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"}},
			"zone-b": {{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"}},
			"zone-c": {{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-c"}},
		},
	}

	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: lister,
	})

	// Test v3 request
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(3)
	req.CoordinatorKey = "test-group"
	req.CoordinatorType = 0 // Group coordinator

	resp := handler.Handle(context.Background(), 3, req, "")

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if resp.NodeID < 0 {
		t.Errorf("expected valid node ID, got %d", resp.NodeID)
	}
	if resp.Host == "" {
		t.Error("expected host to be set")
	}
	if resp.Port <= 0 {
		t.Errorf("expected valid port, got %d", resp.Port)
	}
}

func TestFindCoordinatorHandler_ZoneFiltering(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"},
			{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-c"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"zone-a": {{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"}},
			"zone-b": {{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"}},
			"zone-c": {{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-c"}},
		},
	}

	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: lister,
	})

	// Request with zone-a should return broker in zone-a
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(3)
	req.CoordinatorKey = "test-group"
	req.CoordinatorType = 0

	resp := handler.Handle(context.Background(), 3, req, "zone-a")

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if resp.NodeID != 1 {
		t.Errorf("expected broker 1 from zone-a, got %d", resp.NodeID)
	}
	if resp.Host != "broker1" {
		t.Errorf("expected host broker1, got %s", resp.Host)
	}

	// Request with zone-b should return broker in zone-b
	resp = handler.Handle(context.Background(), 3, req, "zone-b")

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if resp.NodeID != 2 {
		t.Errorf("expected broker 2 from zone-b, got %d", resp.NodeID)
	}
	if resp.Host != "broker2" {
		t.Errorf("expected host broker2, got %s", resp.Host)
	}
}

func TestFindCoordinatorHandler_ZoneFallback(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"zone-a": {{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"}},
			"zone-b": {{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"}},
			// zone-c has no brokers
		},
	}

	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: lister,
	})

	// Request with non-existent zone should fall back to any broker
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(3)
	req.CoordinatorKey = "test-group"
	req.CoordinatorType = 0

	resp := handler.Handle(context.Background(), 3, req, "zone-c")

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	// Should get a broker from the all-brokers list
	if resp.NodeID != 1 && resp.NodeID != 2 {
		t.Errorf("expected broker 1 or 2 from fallback, got %d", resp.NodeID)
	}
}

func TestFindCoordinatorHandler_DeterministicHash(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"},
			{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-c"},
		},
		zoneBrokers: make(map[string][]BrokerInfo),
	}

	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: lister,
	})

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(3)
	req.CoordinatorKey = "consistent-group"
	req.CoordinatorType = 0

	// Call multiple times and verify we get the same result
	firstResp := handler.Handle(context.Background(), 3, req, "")
	firstNodeID := firstResp.NodeID

	for i := 0; i < 10; i++ {
		resp := handler.Handle(context.Background(), 3, req, "")
		if resp.NodeID != firstNodeID {
			t.Errorf("iteration %d: expected consistent node ID %d, got %d", i, firstNodeID, resp.NodeID)
		}
	}

	// Different keys should potentially map to different brokers
	// (though with only 3 brokers some collisions are expected)
	keys := []string{"group-a", "group-b", "group-c", "group-d", "group-e"}
	results := make(map[int32]int)
	for _, key := range keys {
		req.CoordinatorKey = key
		resp := handler.Handle(context.Background(), 3, req, "")
		results[resp.NodeID]++
	}

	// With 5 different keys across 3 brokers, we should see at least 2 different brokers
	if len(results) < 2 {
		t.Errorf("expected hash to distribute across multiple brokers, got distribution: %v", results)
	}
}

func TestFindCoordinatorHandler_TransactionRejection(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: nil,
	})

	// Test v3 transaction request
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(3)
	req.CoordinatorKey = "tx-id"
	req.CoordinatorType = 1 // Transaction coordinator

	resp := handler.Handle(context.Background(), 3, req, "")

	if resp.ErrorCode != errTransactorCoordinatorLoad {
		t.Errorf("expected error code %d for transaction coordinator, got %d", errTransactorCoordinatorLoad, resp.ErrorCode)
	}
	if resp.NodeID != -1 {
		t.Errorf("expected node ID -1 for rejected request, got %d", resp.NodeID)
	}
	if resp.ErrorMessage == nil || *resp.ErrorMessage == "" {
		t.Error("expected error message for v1+ request")
	}
}

func TestFindCoordinatorHandler_BatchedRequest(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-b"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"zone-a": {{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"}},
		},
	}

	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: lister,
	})

	// Test v4 batched request
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(4)
	req.CoordinatorType = 0 // Group coordinator
	req.CoordinatorKeys = []string{"group-1", "group-2", "group-3"}

	resp := handler.Handle(context.Background(), 4, req, "zone-a")

	if len(resp.Coordinators) != 3 {
		t.Errorf("expected 3 coordinators, got %d", len(resp.Coordinators))
	}

	for i, coord := range resp.Coordinators {
		if coord.ErrorCode != 0 {
			t.Errorf("coordinator %d: expected no error, got %d", i, coord.ErrorCode)
		}
		if coord.NodeID != 1 {
			t.Errorf("coordinator %d: expected node 1 from zone-a, got %d", i, coord.NodeID)
		}
		if coord.Host != "broker1" {
			t.Errorf("coordinator %d: expected host broker1, got %s", i, coord.Host)
		}
	}
}

func TestFindCoordinatorHandler_BatchedTransactionRejection(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: nil,
	})

	// Test v4 batched transaction request
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(4)
	req.CoordinatorType = 1 // Transaction coordinator
	req.CoordinatorKeys = []string{"tx-1", "tx-2"}

	resp := handler.Handle(context.Background(), 4, req, "")

	if len(resp.Coordinators) != 2 {
		t.Errorf("expected 2 coordinators, got %d", len(resp.Coordinators))
	}

	for i, coord := range resp.Coordinators {
		if coord.ErrorCode != errTransactorCoordinatorLoad {
			t.Errorf("coordinator %d: expected error code %d, got %d", i, errTransactorCoordinatorLoad, coord.ErrorCode)
		}
		if coord.NodeID != -1 {
			t.Errorf("coordinator %d: expected node ID -1, got %d", i, coord.NodeID)
		}
		if coord.ErrorMessage == nil {
			t.Errorf("coordinator %d: expected error message", i)
		}
	}
}

func TestFindCoordinatorHandler_NoBrokerLister(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 99, Host: "local-host", Port: 9999, Rack: "local-zone"}
	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: nil,
	})

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(3)
	req.CoordinatorKey = "test-group"
	req.CoordinatorType = 0

	resp := handler.Handle(context.Background(), 3, req, "any-zone")

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if resp.NodeID != 99 {
		t.Errorf("expected local broker node ID 99, got %d", resp.NodeID)
	}
	if resp.Host != "local-host" {
		t.Errorf("expected local broker host, got %s", resp.Host)
	}
	if resp.Port != 9999 {
		t.Errorf("expected local broker port 9999, got %d", resp.Port)
	}
}

func TestFindCoordinatorHandler_V0Request(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092}
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092},
		},
	}

	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: lister,
	})

	// Test v0 request (no CoordinatorType field, defaults to group)
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(0)
	req.CoordinatorKey = "test-group"

	resp := handler.Handle(context.Background(), 0, req, "")

	if resp.ErrorCode != 0 {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if resp.NodeID != 1 {
		t.Errorf("expected node ID 1, got %d", resp.NodeID)
	}
}

func TestHashToIndex(t *testing.T) {
	tests := []struct {
		key   string
		count int
	}{
		{"test-group", 3},
		{"another-group", 5},
		{"", 1},
		{"group", 10},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			idx := hashToIndex(tt.key, tt.count)
			if idx < 0 || idx >= tt.count {
				t.Errorf("hashToIndex(%q, %d) = %d, want 0 <= idx < %d", tt.key, tt.count, idx, tt.count)
			}

			// Verify determinism
			for i := 0; i < 100; i++ {
				if got := hashToIndex(tt.key, tt.count); got != idx {
					t.Errorf("hashToIndex is not deterministic: got %d, want %d", got, idx)
				}
			}
		})
	}
}

func TestHashToIndex_ZeroCount(t *testing.T) {
	idx := hashToIndex("test", 0)
	if idx != 0 {
		t.Errorf("hashToIndex with count 0 should return 0, got %d", idx)
	}
}

func TestFindCoordinatorHandler_MultipleZoneBrokers(t *testing.T) {
	localBroker := BrokerInfo{NodeID: 1, Host: "localhost", Port: 9092, Rack: "zone-a"}
	lister := &mockBrokerLister{
		brokers: []BrokerInfo{
			{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
			{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-a"},
			{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-a"},
			{NodeID: 4, Host: "broker4", Port: 9092, Rack: "zone-b"},
		},
		zoneBrokers: map[string][]BrokerInfo{
			"zone-a": {
				{NodeID: 1, Host: "broker1", Port: 9092, Rack: "zone-a"},
				{NodeID: 2, Host: "broker2", Port: 9092, Rack: "zone-a"},
				{NodeID: 3, Host: "broker3", Port: 9092, Rack: "zone-a"},
			},
			"zone-b": {
				{NodeID: 4, Host: "broker4", Port: 9092, Rack: "zone-b"},
			},
		},
	}

	handler := NewFindCoordinatorHandler(FindCoordinatorHandlerConfig{
		LocalBroker:  localBroker,
		BrokerLister: lister,
	})

	// Test that different groups can map to different brokers within the same zone
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.SetVersion(3)
	req.CoordinatorType = 0

	results := make(map[int32]int)
	keys := []string{"group-1", "group-2", "group-3", "group-4", "group-5", "group-6", "group-7", "group-8", "group-9", "group-10"}
	for _, key := range keys {
		req.CoordinatorKey = key
		resp := handler.Handle(context.Background(), 3, req, "zone-a")
		if resp.ErrorCode != 0 {
			t.Errorf("key %s: expected no error, got %d", key, resp.ErrorCode)
		}
		// Should be one of the zone-a brokers (1, 2, or 3)
		if resp.NodeID < 1 || resp.NodeID > 3 {
			t.Errorf("key %s: expected broker from zone-a (1-3), got %d", key, resp.NodeID)
		}
		results[resp.NodeID]++
	}

	// With 10 keys across 3 brokers, we should see distribution
	if len(results) < 2 {
		t.Logf("distribution across zone-a brokers: %v", results)
	}
}
