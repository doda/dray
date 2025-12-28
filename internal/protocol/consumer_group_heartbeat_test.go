package protocol

import (
	"context"
	"testing"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestConsumerGroupHeartbeatHandler_JoinGroup(t *testing.T) {
	ctx := context.Background()

	t.Run("success - new member joins", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "test-group"
		req.MemberID = "" // Empty means new member
		req.MemberEpoch = 0
		req.SubscribedTopicNames = []string{"topic1", "topic2"}

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBNone {
			t.Errorf("expected error code %d, got %d", errCGHBNone, resp.ErrorCode)
		}
		if resp.MemberID == nil || *resp.MemberID == "" {
			t.Error("expected non-empty member ID")
		}
		if resp.MemberEpoch != 1 {
			t.Errorf("expected member epoch 1, got %d", resp.MemberEpoch)
		}
		if resp.HeartbeatIntervalMillis != defaultHeartbeatIntervalMs {
			t.Errorf("expected heartbeat interval %d, got %d", defaultHeartbeatIntervalMs, resp.HeartbeatIntervalMillis)
		}

		// Verify group was created
		groupType, err := store.GetGroupType(ctx, "test-group")
		if err != nil {
			t.Errorf("failed to get group type: %v", err)
		}
		if groupType != groups.GroupTypeConsumer {
			t.Errorf("expected group type consumer, got %s", groupType)
		}

		// Verify member was added
		member, err := store.GetMember(ctx, "test-group", *resp.MemberID)
		if err != nil {
			t.Errorf("failed to get member: %v", err)
		}
		if member.MemberEpoch != 1 {
			t.Errorf("expected member epoch 1, got %d", member.MemberEpoch)
		}
	})

	t.Run("success - join with member epoch zero and member ID set", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "test-group"
		req.MemberID = "client-provided-id"
		req.MemberEpoch = 0
		req.SubscribedTopicNames = []string{"topic1"}

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBNone {
			t.Errorf("expected error code %d, got %d", errCGHBNone, resp.ErrorCode)
		}
		if resp.MemberID == nil || *resp.MemberID == "" {
			t.Error("expected non-empty member ID")
		}
		if resp.MemberEpoch != 1 {
			t.Errorf("expected member epoch 1, got %d", resp.MemberEpoch)
		}
	})

	t.Run("error - empty group ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = ""

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBInvalidGroupID {
			t.Errorf("expected error code %d, got %d", errCGHBInvalidGroupID, resp.ErrorCode)
		}
	})

	t.Run("error - group is classic type", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// Create classic group first
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "classic-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        1000,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "classic-group"
		req.MemberID = ""
		req.MemberEpoch = 0

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBInvalidGroupID {
			t.Errorf("expected error code %d, got %d", errCGHBInvalidGroupID, resp.ErrorCode)
		}
	})

	t.Run("success - static member rejoins", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		instanceID := "my-instance"

		// First join
		req1 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req1.Group = "test-group"
		req1.MemberID = ""
		req1.MemberEpoch = 0
		req1.InstanceID = &instanceID
		req1.SubscribedTopicNames = []string{"topic1"}

		resp1 := handler.Handle(ctx, 0, req1)
		if resp1.ErrorCode != errCGHBNone {
			t.Fatalf("first join failed: %d", resp1.ErrorCode)
		}

		firstMemberID := *resp1.MemberID

		// Rejoin with same instance ID (simulating restart)
		req2 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req2.Group = "test-group"
		req2.MemberID = ""
		req2.MemberEpoch = 0
		req2.InstanceID = &instanceID
		req2.SubscribedTopicNames = []string{"topic1"}

		resp2 := handler.Handle(ctx, 0, req2)
		if resp2.ErrorCode != errCGHBNone {
			t.Fatalf("rejoin failed: %d", resp2.ErrorCode)
		}

		// Should get same member ID
		if *resp2.MemberID != firstMemberID {
			t.Errorf("expected same member ID %s, got %s", firstMemberID, *resp2.MemberID)
		}

		// Epoch should be incremented
		if resp2.MemberEpoch != 2 {
			t.Errorf("expected member epoch 2, got %d", resp2.MemberEpoch)
		}
	})

	t.Run("error - unsupported assignor", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		badAssignor := "bad-assignor"
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "test-group"
		req.MemberID = ""
		req.MemberEpoch = 0
		req.ServerAssignor = &badAssignor

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBUnsupportedAssignor {
			t.Errorf("expected error code %d, got %d", errCGHBUnsupportedAssignor, resp.ErrorCode)
		}
	})
}

func TestConsumerGroupHeartbeatHandler_Heartbeat(t *testing.T) {
	ctx := context.Background()

	t.Run("success - regular heartbeat", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// First join to get a member
		joinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		joinReq.Group = "test-group"
		joinReq.MemberID = ""
		joinReq.MemberEpoch = 0
		joinReq.SubscribedTopicNames = []string{"topic1"}

		joinResp := handler.Handle(ctx, 0, joinReq)
		if joinResp.ErrorCode != errCGHBNone {
			t.Fatalf("join failed: %d", joinResp.ErrorCode)
		}

		memberID := *joinResp.MemberID

		// Send heartbeat
		hbReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		hbReq.Group = "test-group"
		hbReq.MemberID = memberID
		hbReq.MemberEpoch = 1

		hbResp := handler.Handle(ctx, 0, hbReq)

		if hbResp.ErrorCode != errCGHBNone {
			t.Errorf("expected error code %d, got %d", errCGHBNone, hbResp.ErrorCode)
		}
		if *hbResp.MemberID != memberID {
			t.Errorf("expected member ID %s, got %s", memberID, *hbResp.MemberID)
		}
		if hbResp.MemberEpoch != 1 {
			t.Errorf("expected epoch 1, got %d", hbResp.MemberEpoch)
		}
	})

	t.Run("error - unknown member ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// Create group first
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        1000,
		})

		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "test-group"
		req.MemberID = "nonexistent-member"
		req.MemberEpoch = 1

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBUnknownMemberID {
			t.Errorf("expected error code %d, got %d", errCGHBUnknownMemberID, resp.ErrorCode)
		}
	})

	t.Run("error - fenced member epoch", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// First join
		joinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		joinReq.Group = "test-group"
		joinReq.MemberID = ""
		joinReq.MemberEpoch = 0
		joinReq.SubscribedTopicNames = []string{"topic1"}

		joinResp := handler.Handle(ctx, 0, joinReq)
		if joinResp.ErrorCode != errCGHBNone {
			t.Fatalf("join failed: %d", joinResp.ErrorCode)
		}

		memberID := *joinResp.MemberID

		// Send heartbeat with wrong epoch
		hbReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		hbReq.Group = "test-group"
		hbReq.MemberID = memberID
		hbReq.MemberEpoch = 999 // Wrong epoch

		hbResp := handler.Handle(ctx, 0, hbReq)

		if hbResp.ErrorCode != errCGHBFencedMemberEpoch {
			t.Errorf("expected error code %d, got %d", errCGHBFencedMemberEpoch, hbResp.ErrorCode)
		}
		// Response should contain current epoch
		if hbResp.MemberEpoch != 1 {
			t.Errorf("expected epoch 1 in response, got %d", hbResp.MemberEpoch)
		}
	})

	t.Run("success - subscription change bumps epoch", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// First join
		joinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		joinReq.Group = "test-group"
		joinReq.MemberID = ""
		joinReq.MemberEpoch = 0
		joinReq.SubscribedTopicNames = []string{"topic1"}

		joinResp := handler.Handle(ctx, 0, joinReq)
		if joinResp.ErrorCode != errCGHBNone {
			t.Fatalf("join failed: %d", joinResp.ErrorCode)
		}

		memberID := *joinResp.MemberID

		// Send heartbeat with different subscription
		hbReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		hbReq.Group = "test-group"
		hbReq.MemberID = memberID
		hbReq.MemberEpoch = 1
		hbReq.SubscribedTopicNames = []string{"topic1", "topic2"} // Changed!

		hbResp := handler.Handle(ctx, 0, hbReq)

		if hbResp.ErrorCode != errCGHBNone {
			t.Errorf("expected error code %d, got %d", errCGHBNone, hbResp.ErrorCode)
		}
		// Epoch should be bumped due to subscription change
		if hbResp.MemberEpoch != 2 {
			t.Errorf("expected epoch 2 after subscription change, got %d", hbResp.MemberEpoch)
		}
	})
}

func TestConsumerGroupHeartbeatHandler_LeaveGroup(t *testing.T) {
	ctx := context.Background()

	t.Run("success - member leaves", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// First join
		joinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		joinReq.Group = "test-group"
		joinReq.MemberID = ""
		joinReq.MemberEpoch = 0
		joinReq.SubscribedTopicNames = []string{"topic1"}

		joinResp := handler.Handle(ctx, 0, joinReq)
		if joinResp.ErrorCode != errCGHBNone {
			t.Fatalf("join failed: %d", joinResp.ErrorCode)
		}

		memberID := *joinResp.MemberID

		// Leave group (MemberEpoch == -1)
		leaveReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		leaveReq.Group = "test-group"
		leaveReq.MemberID = memberID
		leaveReq.MemberEpoch = -1

		leaveResp := handler.Handle(ctx, 0, leaveReq)

		if leaveResp.ErrorCode != errCGHBNone {
			t.Errorf("expected error code %d, got %d", errCGHBNone, leaveResp.ErrorCode)
		}
		if leaveResp.MemberEpoch != -1 {
			t.Errorf("expected epoch -1, got %d", leaveResp.MemberEpoch)
		}

		// Verify member was removed
		_, err := store.GetMember(ctx, "test-group", memberID)
		if err == nil {
			t.Error("expected member to be removed")
		}
	})

	t.Run("success - leave unknown member", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// Create group
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        1000,
		})

		// Leave with unknown member ID should succeed
		leaveReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		leaveReq.Group = "test-group"
		leaveReq.MemberID = "unknown-member"
		leaveReq.MemberEpoch = -1

		leaveResp := handler.Handle(ctx, 0, leaveReq)

		if leaveResp.ErrorCode != errCGHBNone {
			t.Errorf("expected error code %d, got %d", errCGHBNone, leaveResp.ErrorCode)
		}
	})

	t.Run("error - leave without member ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// Create group
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        1000,
		})

		leaveReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		leaveReq.Group = "test-group"
		leaveReq.MemberID = "" // No member ID
		leaveReq.MemberEpoch = -1

		leaveResp := handler.Handle(ctx, 0, leaveReq)

		if leaveResp.ErrorCode != errCGHBUnknownMemberID {
			t.Errorf("expected error code %d, got %d", errCGHBUnknownMemberID, leaveResp.ErrorCode)
		}
	})

	t.Run("success - group becomes empty after leave", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		// First join
		joinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		joinReq.Group = "test-group"
		joinReq.MemberID = ""
		joinReq.MemberEpoch = 0
		joinReq.SubscribedTopicNames = []string{"topic1"}

		joinResp := handler.Handle(ctx, 0, joinReq)
		if joinResp.ErrorCode != errCGHBNone {
			t.Fatalf("join failed: %d", joinResp.ErrorCode)
		}

		memberID := *joinResp.MemberID

		// Leave group
		leaveReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		leaveReq.Group = "test-group"
		leaveReq.MemberID = memberID
		leaveReq.MemberEpoch = -1

		leaveResp := handler.Handle(ctx, 0, leaveReq)
		if leaveResp.ErrorCode != errCGHBNone {
			t.Fatalf("leave failed: %d", leaveResp.ErrorCode)
		}

		// Verify group state is Empty
		state, err := store.GetGroupState(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to get group state: %v", err)
		}
		if state.State != groups.GroupStateEmpty {
			t.Errorf("expected state Empty, got %s", state.State)
		}
	})
}

func TestConsumerGroupHeartbeatHandler_WithLeaseManager(t *testing.T) {
	ctx := context.Background()

	t.Run("error - not coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewConsumerGroupHeartbeatHandler(store, leaseManager, nil)

		// Simulate another broker holding the lease
		_, _ = metaStore.Put(ctx, "/dray/v1/groups/test-group/lease", []byte(`{"brokerID":"broker-2","acquiredAt":0}`))

		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "test-group"
		req.MemberID = ""
		req.MemberEpoch = 0

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBNotCoordinator {
			t.Errorf("expected error code %d, got %d", errCGHBNotCoordinator, resp.ErrorCode)
		}
	})

	t.Run("success - with lease", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewConsumerGroupHeartbeatHandler(store, leaseManager, nil)

		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "test-group"
		req.MemberID = ""
		req.MemberEpoch = 0
		req.SubscribedTopicNames = []string{"topic1"}

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBNone {
			t.Errorf("expected error code %d, got %d", errCGHBNone, resp.ErrorCode)
		}
		if resp.MemberID == nil || *resp.MemberID == "" {
			t.Error("expected non-empty member ID")
		}
	})
}

func TestConsumerGroupHeartbeatHandler_RackAwareness(t *testing.T) {
	ctx := context.Background()

	t.Run("success - rack ID stored", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

		rackID := "us-east-1a"
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = "test-group"
		req.MemberID = ""
		req.MemberEpoch = 0
		req.RackID = &rackID
		req.SubscribedTopicNames = []string{"topic1"}

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errCGHBNone {
			t.Fatalf("join failed: %d", resp.ErrorCode)
		}

		// Verify rack ID was stored
		member, err := store.GetMember(ctx, "test-group", *resp.MemberID)
		if err != nil {
			t.Fatalf("failed to get member: %v", err)
		}
		if member.RackID != rackID {
			t.Errorf("expected rack ID %s, got %s", rackID, member.RackID)
		}
	})
}

func TestStringSlicesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		expected bool
	}{
		{"both nil", nil, nil, true},
		{"both empty", []string{}, []string{}, true},
		{"same elements", []string{"a", "b"}, []string{"a", "b"}, true},
		{"different lengths", []string{"a"}, []string{"a", "b"}, false},
		{"different elements", []string{"a", "b"}, []string{"a", "c"}, false},
		{"different order", []string{"a", "b"}, []string{"b", "a"}, false},
		{"one nil", nil, []string{"a"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stringSlicesEqual(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("stringSlicesEqual(%v, %v) = %v, expected %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}
