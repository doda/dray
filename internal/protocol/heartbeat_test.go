package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHeartbeatHandler_Handle(t *testing.T) {
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	t.Run("success - updates heartbeat time", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		// Create group and member
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		// Transition to stable state with generation 1
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs - 5000, // Old heartbeat
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		// Send heartbeat
		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatNone {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}

		// Verify heartbeat was updated
		member, err := store.GetMember(ctx, "test-group", "member-1")
		if err != nil {
			t.Fatalf("get member: %v", err)
		}
		if member.LastHeartbeatMs <= nowMs-5000 {
			t.Error("heartbeat time was not updated")
		}
	})

	t.Run("error - empty group ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = ""
		req.Generation = 1
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatInvalidGroupID {
			t.Errorf("expected INVALID_GROUP_ID (24), got %d", resp.ErrorCode)
		}
	})

	t.Run("error - empty member ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = ""

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatUnknownMemberID {
			t.Errorf("expected UNKNOWN_MEMBER_ID (25), got %d", resp.ErrorCode)
		}
	})

	t.Run("error - group not found", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "nonexistent-group"
		req.Generation = 1
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatCoordinatorNotAvail {
			t.Errorf("expected COORDINATOR_NOT_AVAILABLE (15), got %d", resp.ErrorCode)
		}
	})

	t.Run("error - member not found", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		// Create group in stable state
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "nonexistent-member"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatUnknownMemberID {
			t.Errorf("expected UNKNOWN_MEMBER_ID (25), got %d", resp.ErrorCode)
		}
	})

	t.Run("error - generation mismatch", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		// Create group with generation 1
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 99 // Wrong generation
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatIllegalGeneration {
			t.Errorf("expected ILLEGAL_GENERATION (22), got %d", resp.ErrorCode)
		}
	})

	t.Run("error - rebalance in progress", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		// Create group in PreparingRebalance state
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStatePreparingRebalance,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatRebalanceInProgress {
			t.Errorf("expected REBALANCE_IN_PROGRESS (27), got %d", resp.ErrorCode)
		}
	})

	t.Run("error - dead group", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		// Create group in Dead state
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "test-group",
			ToState: groups.GroupStateDead,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 0
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatUnknownMemberID {
			t.Errorf("expected UNKNOWN_MEMBER_ID (25), got %d", resp.ErrorCode)
		}
	})

	t.Run("success - empty state allowed", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		// Create group in Empty state (generation 0)
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 0
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatNone {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
	})

	t.Run("success - completingRebalance state allowed", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateCompletingRebalance,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatNone {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
	})

	t.Run("v3+ instance ID validation - success", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		instanceID := "instance-1"
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:         "test-group",
			MemberID:        "member-1",
			ClientID:        "client-1",
			GroupInstanceID: instanceID,
			SessionTimeoutMs: 30000,
			NowMs:           nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"
		req.InstanceID = &instanceID

		resp := handler.Handle(ctx, 3, req)

		if resp.ErrorCode != errHeartbeatNone {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
	})

	t.Run("v3+ instance ID validation - fenced", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:         "test-group",
			MemberID:        "member-1",
			ClientID:        "client-1",
			GroupInstanceID: "instance-1",
			SessionTimeoutMs: 30000,
			NowMs:           nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		wrongInstanceID := "wrong-instance"
		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"
		req.InstanceID = &wrongInstanceID

		resp := handler.Handle(ctx, 3, req)

		if resp.ErrorCode != errHeartbeatFencedInstanceID {
			t.Errorf("expected FENCED_INSTANCE_ID (82), got %d", resp.ErrorCode)
		}
	})

	t.Run("version response formatting", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewHeartbeatHandler(store, nil)

		// Setup group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		for _, version := range []int16{0, 1, 2, 3, 4, 5} {
			req := kmsg.NewPtrHeartbeatRequest()
			req.Group = "test-group"
			req.Generation = 1
			req.MemberID = "member-1"

			resp := handler.Handle(ctx, version, req)

			if resp.GetVersion() != version {
				t.Errorf("version %d: expected version %d, got %d", version, version, resp.GetVersion())
			}
			if resp.ErrorCode != errHeartbeatNone {
				t.Errorf("version %d: expected no error, got %d", version, resp.ErrorCode)
			}
		}
	})
}

func TestHeartbeatHandler_WithLeaseManager(t *testing.T) {
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	t.Run("not coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewHeartbeatHandler(store, leaseManager)

		// Simulate another broker holding the lease
		_, _ = metaStore.Put(ctx, "/dray/v1/groups/test-group/lease", []byte(`{"brokerID":"broker-2","acquiredAt":0}`))

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatNotCoordinator {
			t.Errorf("expected NOT_COORDINATOR (16), got %d", resp.ErrorCode)
		}
	})

	t.Run("is coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewHeartbeatHandler(store, leaseManager)

		// Create group and member
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 5, req)

		if resp.ErrorCode != errHeartbeatNone {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
	})
}

// newTestGroupStore creates a test group store with a mock metadata store.
func newTestGroupStore() *groups.Store {
	return groups.NewStore(metadata.NewMockStore())
}
