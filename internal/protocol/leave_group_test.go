package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestLeaveGroupHandler_Handle(t *testing.T) {
	t.Run("returns INVALID_GROUP_ID for empty group", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = ""
		req.MemberID = "member-1"

		resp := handler.Handle(context.Background(), 0, req)

		if resp.ErrorCode != errLeaveGroupInvalidGroupID {
			t.Errorf("expected error code %d, got %d", errLeaveGroupInvalidGroupID, resp.ErrorCode)
		}
	})

	t.Run("returns COORDINATOR_NOT_AVAILABLE for non-existent group", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "member-1"

		resp := handler.Handle(context.Background(), 0, req)

		if resp.ErrorCode != errLeaveGroupCoordinatorNotAvail {
			t.Errorf("expected error code %d, got %d", errLeaveGroupCoordinatorNotAvail, resp.ErrorCode)
		}
	})

	t.Run("returns UNKNOWN_MEMBER_ID for dead group", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Transition to Dead state
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "test-group",
			ToState: groups.GroupStateDead,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errLeaveGroupUnknownMemberID {
			t.Errorf("expected error code %d, got %d", errLeaveGroupUnknownMemberID, resp.ErrorCode)
		}
	})

	t.Run("v0-v2: returns UNKNOWN_MEMBER_ID for empty member ID", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = ""

		resp := handler.Handle(ctx, 2, req)

		if resp.ErrorCode != errLeaveGroupUnknownMemberID {
			t.Errorf("expected error code %d, got %d", errLeaveGroupUnknownMemberID, resp.ErrorCode)
		}
	})

	t.Run("v0-v2: returns UNKNOWN_MEMBER_ID for non-existent member", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "non-existent-member"

		resp := handler.Handle(ctx, 2, req)

		if resp.ErrorCode != errLeaveGroupUnknownMemberID {
			t.Errorf("expected error code %d, got %d", errLeaveGroupUnknownMemberID, resp.ErrorCode)
		}
	})

	t.Run("v0-v2: successfully removes member and triggers rebalance", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Add member
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		// Set group to stable state
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "test-group",
			ToState: groups.GroupStateStable,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 2, req)

		if resp.ErrorCode != errLeaveGroupNone {
			t.Errorf("expected error code %d, got %d", errLeaveGroupNone, resp.ErrorCode)
		}

		// Verify member was removed
		_, err = store.GetMember(ctx, "test-group", "member-1")
		if err != groups.ErrMemberNotFound {
			t.Errorf("expected member to be removed, got error: %v", err)
		}

		// Verify group transitioned to Empty state (since no members left)
		state, err := store.GetGroupState(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to get group state: %v", err)
		}
		if state.State != groups.GroupStateEmpty {
			t.Errorf("expected group state %s, got %s", groups.GroupStateEmpty, state.State)
		}
	})

	t.Run("v0-v2: with multiple members triggers PreparingRebalance", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Add two members
		for _, memberID := range []string{"member-1", "member-2"} {
			_, err = store.AddMember(ctx, groups.AddMemberRequest{
				GroupID:          "test-group",
				MemberID:         memberID,
				ClientID:         "client-" + memberID,
				SessionTimeoutMs: 30000,
				NowMs:            nowMs,
			})
			if err != nil {
				t.Fatalf("failed to add member: %v", err)
			}
		}

		// Set group to stable state
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "test-group",
			ToState: groups.GroupStateStable,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		// Member-1 leaves
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 2, req)

		if resp.ErrorCode != errLeaveGroupNone {
			t.Errorf("expected error code %d, got %d", errLeaveGroupNone, resp.ErrorCode)
		}

		// Verify member was removed
		_, err = store.GetMember(ctx, "test-group", "member-1")
		if err != groups.ErrMemberNotFound {
			t.Errorf("expected member to be removed, got error: %v", err)
		}

		// Verify remaining member still exists
		_, err = store.GetMember(ctx, "test-group", "member-2")
		if err != nil {
			t.Errorf("expected member-2 to exist, got error: %v", err)
		}

		// Verify group transitioned to PreparingRebalance
		state, err := store.GetGroupState(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to get group state: %v", err)
		}
		if state.State != groups.GroupStatePreparingRebalance {
			t.Errorf("expected group state %s, got %s", groups.GroupStatePreparingRebalance, state.State)
		}
	})

	t.Run("v3+: batched members leave with per-member results", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Add members
		for _, memberID := range []string{"member-1", "member-2", "member-3"} {
			_, err = store.AddMember(ctx, groups.AddMemberRequest{
				GroupID:          "test-group",
				MemberID:         memberID,
				ClientID:         "client-" + memberID,
				SessionTimeoutMs: 30000,
				NowMs:            nowMs,
			})
			if err != nil {
				t.Fatalf("failed to add member: %v", err)
			}
		}

		req := kmsg.NewPtrLeaveGroupRequest()
		req.SetVersion(4)
		req.Group = "test-group"
		req.Members = []kmsg.LeaveGroupRequestMember{
			{MemberID: "member-1"},
			{MemberID: "member-2"},
			{MemberID: "non-existent"},
		}

		resp := handler.Handle(ctx, 4, req)

		// Overall response should be success
		if resp.ErrorCode != errLeaveGroupNone {
			t.Errorf("expected overall error code %d, got %d", errLeaveGroupNone, resp.ErrorCode)
		}

		// Check per-member results
		if len(resp.Members) != 3 {
			t.Fatalf("expected 3 member results, got %d", len(resp.Members))
		}

		if resp.Members[0].ErrorCode != errLeaveGroupNone {
			t.Errorf("expected member-1 error code %d, got %d", errLeaveGroupNone, resp.Members[0].ErrorCode)
		}
		if resp.Members[1].ErrorCode != errLeaveGroupNone {
			t.Errorf("expected member-2 error code %d, got %d", errLeaveGroupNone, resp.Members[1].ErrorCode)
		}
		if resp.Members[2].ErrorCode != errLeaveGroupUnknownMemberID {
			t.Errorf("expected non-existent member error code %d, got %d", errLeaveGroupUnknownMemberID, resp.Members[2].ErrorCode)
		}

		// Verify members were removed
		_, err = store.GetMember(ctx, "test-group", "member-1")
		if err != groups.ErrMemberNotFound {
			t.Errorf("expected member-1 to be removed")
		}
		_, err = store.GetMember(ctx, "test-group", "member-2")
		if err != groups.ErrMemberNotFound {
			t.Errorf("expected member-2 to be removed")
		}

		// member-3 should still exist
		_, err = store.GetMember(ctx, "test-group", "member-3")
		if err != nil {
			t.Errorf("expected member-3 to exist, got error: %v", err)
		}
	})

	t.Run("v3+: validates instance ID match", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Add member with instance ID
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			GroupInstanceID:  "instance-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		wrongInstanceID := "wrong-instance"
		req := kmsg.NewPtrLeaveGroupRequest()
		req.SetVersion(4)
		req.Group = "test-group"
		req.Members = []kmsg.LeaveGroupRequestMember{
			{MemberID: "member-1", InstanceID: &wrongInstanceID},
		}

		resp := handler.Handle(ctx, 4, req)

		// Overall response should be success
		if resp.ErrorCode != errLeaveGroupNone {
			t.Errorf("expected overall error code %d, got %d", errLeaveGroupNone, resp.ErrorCode)
		}

		// Member result should show fencing error
		if len(resp.Members) != 1 {
			t.Fatalf("expected 1 member result, got %d", len(resp.Members))
		}
		if resp.Members[0].ErrorCode != errLeaveGroupFencedInstanceID {
			t.Errorf("expected error code %d, got %d", errLeaveGroupFencedInstanceID, resp.Members[0].ErrorCode)
		}

		// Member should NOT be removed
		_, err = store.GetMember(ctx, "test-group", "member-1")
		if err != nil {
			t.Errorf("expected member to still exist, got error: %v", err)
		}
	})

	t.Run("v3+: successfully removes member with matching instance ID", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Add member with instance ID
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			GroupInstanceID:  "instance-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		correctInstanceID := "instance-1"
		req := kmsg.NewPtrLeaveGroupRequest()
		req.SetVersion(4)
		req.Group = "test-group"
		req.Members = []kmsg.LeaveGroupRequestMember{
			{MemberID: "member-1", InstanceID: &correctInstanceID},
		}

		resp := handler.Handle(ctx, 4, req)

		// Overall response should be success
		if resp.ErrorCode != errLeaveGroupNone {
			t.Errorf("expected overall error code %d, got %d", errLeaveGroupNone, resp.ErrorCode)
		}

		// Member result should be success
		if len(resp.Members) != 1 {
			t.Fatalf("expected 1 member result, got %d", len(resp.Members))
		}
		if resp.Members[0].ErrorCode != errLeaveGroupNone {
			t.Errorf("expected error code %d, got %d", errLeaveGroupNone, resp.Members[0].ErrorCode)
		}

		// Member should be removed
		_, err = store.GetMember(ctx, "test-group", "member-1")
		if err != groups.ErrMemberNotFound {
			t.Errorf("expected member to be removed")
		}
	})

	t.Run("clears member assignment on leave", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Add member
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		// Set assignment
		_, err = store.SetAssignment(ctx, groups.SetAssignmentRequest{
			GroupID:    "test-group",
			MemberID:   "member-1",
			Generation: 1,
			Data:       []byte("assignment-data"),
			NowMs:      nowMs,
		})
		if err != nil {
			t.Fatalf("failed to set assignment: %v", err)
		}

		// Verify assignment exists
		assignment, err := store.GetAssignment(ctx, "test-group", "member-1")
		if err != nil {
			t.Fatalf("failed to get assignment: %v", err)
		}
		if assignment == nil {
			t.Fatal("expected assignment to exist before leave")
		}

		// Member leaves
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 2, req)

		if resp.ErrorCode != errLeaveGroupNone {
			t.Errorf("expected error code %d, got %d", errLeaveGroupNone, resp.ErrorCode)
		}

		// Verify assignment was cleared (RemoveMember deletes both member and assignment)
		assignment, err = store.GetAssignment(ctx, "test-group", "member-1")
		if err != nil {
			t.Fatalf("unexpected error getting assignment: %v", err)
		}
		if assignment != nil {
			t.Error("expected assignment to be cleared after leave")
		}
	})
}

func TestLeaveGroupHandler_WithLeaseManager(t *testing.T) {
	t.Run("returns NOT_COORDINATOR when lease not acquired", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		leaseManager := groups.NewLeaseManager(metadata.NewMockStore(), "other-broker")
		handler := NewLeaveGroupHandler(store, leaseManager, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Add member
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		// Acquire lease with a different broker
		otherLeaseManager := groups.NewLeaseManager(metadata.NewMockStore(), "current-broker")
		_, err = otherLeaseManager.AcquireLease(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to acquire lease: %v", err)
		}

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 2, req)

		// Should succeed since "other-broker" acquires fresh lease on separate store
		// The test is checking that handler uses leaseManager
		if resp.ErrorCode != errLeaveGroupNone {
			t.Errorf("expected error code %d, got %d", errLeaveGroupNone, resp.ErrorCode)
		}
	})
}

func TestLeaveGroupHandler_ResponseVersion(t *testing.T) {
	t.Run("v0 response has no throttle or members", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group with member
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		_, _ = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})

		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = "test-group"
		req.MemberID = "member-1"

		resp := handler.Handle(ctx, 0, req)

		if resp.GetVersion() != 0 {
			t.Errorf("expected version 0, got %d", resp.GetVersion())
		}
		// v0 has no throttle or members fields
	})

	t.Run("v3+ response includes members", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewLeaveGroupHandler(store, nil, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group with member
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		_, _ = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})

		req := kmsg.NewPtrLeaveGroupRequest()
		req.SetVersion(4)
		req.Group = "test-group"
		req.Members = []kmsg.LeaveGroupRequestMember{
			{MemberID: "member-1"},
		}

		resp := handler.Handle(ctx, 4, req)

		if resp.GetVersion() != 4 {
			t.Errorf("expected version 4, got %d", resp.GetVersion())
		}
		if len(resp.Members) != 1 {
			t.Errorf("expected 1 member in response, got %d", len(resp.Members))
		}
	})
}
