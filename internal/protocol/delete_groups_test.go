package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDeleteGroupsHandler_Handle(t *testing.T) {
	t.Run("returns INVALID_GROUP_ID for empty group", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{""}

		resp := handler.Handle(context.Background(), 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsInvalidGroupID {
			t.Errorf("expected error code %d, got %d", errDeleteGroupsInvalidGroupID, resp.Groups[0].ErrorCode)
		}
	})

	t.Run("returns GROUP_ID_NOT_FOUND for non-existent group", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"non-existent-group"}

		resp := handler.Handle(context.Background(), 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsGroupIDNotFound {
			t.Errorf("expected error code %d, got %d", errDeleteGroupsGroupIDNotFound, resp.Groups[0].ErrorCode)
		}
	})

	t.Run("deletes empty group successfully", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group (starts in Empty state)
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNone {
			t.Errorf("expected error code %d, got %d", errDeleteGroupsNone, resp.Groups[0].ErrorCode)
		}

		// Verify group is deleted
		exists, err := store.GroupExists(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to check group existence: %v", err)
		}
		if exists {
			t.Error("expected group to be deleted")
		}
	})

	t.Run("deletes Dead group successfully", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

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
			t.Fatalf("failed to transition to Dead state: %v", err)
		}

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNone {
			t.Errorf("expected error code %d, got %d", errDeleteGroupsNone, resp.Groups[0].ErrorCode)
		}
	})

	t.Run("returns NON_EMPTY_GROUP for group with active members", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

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
			ClientHost:       "host-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		// Transition to Stable state
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "test-group",
			ToState: groups.GroupStateStable,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNonEmptyGroup {
			t.Errorf("expected error code %d (NON_EMPTY_GROUP), got %d", errDeleteGroupsNonEmptyGroup, resp.Groups[0].ErrorCode)
		}

		// Verify group still exists
		exists, err := store.GroupExists(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to check group existence: %v", err)
		}
		if !exists {
			t.Error("expected group to still exist")
		}
	})

	t.Run("cleans up committed offsets on delete", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

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

		// Add committed offsets for the group
		offsetKey1 := keys.GroupOffsetKeyPath("test-group", "topic-1", 0)
		offsetKey2 := keys.GroupOffsetKeyPath("test-group", "topic-1", 1)
		offsetKey3 := keys.GroupOffsetKeyPath("test-group", "topic-2", 0)

		for _, key := range []string{offsetKey1, offsetKey2, offsetKey3} {
			_, err := metaStore.Put(ctx, key, []byte(`{"offset": 100}`))
			if err != nil {
				t.Fatalf("failed to put offset: %v", err)
			}
		}

		// Verify offsets exist
		offsetsPrefix := keys.GroupOffsetsPrefix("test-group")
		kvs, err := metaStore.List(ctx, offsetsPrefix, "", 0)
		if err != nil {
			t.Fatalf("failed to list offsets: %v", err)
		}
		if len(kvs) != 3 {
			t.Fatalf("expected 3 offsets, got %d", len(kvs))
		}

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNone {
			t.Errorf("expected error code %d, got %d", errDeleteGroupsNone, resp.Groups[0].ErrorCode)
		}

		// Verify offsets are deleted
		kvs, err = metaStore.List(ctx, offsetsPrefix, "", 0)
		if err != nil {
			t.Fatalf("failed to list offsets: %v", err)
		}
		if len(kvs) != 0 {
			t.Errorf("expected 0 offsets after delete, got %d", len(kvs))
		}
	})

	t.Run("deletes multiple groups", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create groups
		for _, groupID := range []string{"group-1", "group-2", "group-3"} {
			_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
				GroupID:      groupID,
				Type:         groups.GroupTypeClassic,
				ProtocolType: "consumer",
				NowMs:        nowMs,
			})
			if err != nil {
				t.Fatalf("failed to create group %s: %v", groupID, err)
			}
		}

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"group-1", "group-2", "group-3", "non-existent"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 4 {
			t.Fatalf("expected 4 groups in response, got %d", len(resp.Groups))
		}

		// Check group-1, group-2, group-3 succeeded
		for i := 0; i < 3; i++ {
			if resp.Groups[i].ErrorCode != errDeleteGroupsNone {
				t.Errorf("expected no error for %s, got %d", resp.Groups[i].Group, resp.Groups[i].ErrorCode)
			}
		}

		// Check non-existent failed
		if resp.Groups[3].ErrorCode != errDeleteGroupsGroupIDNotFound {
			t.Errorf("expected GROUP_ID_NOT_FOUND for non-existent, got %d", resp.Groups[3].ErrorCode)
		}

		// Verify all existing groups are deleted
		for _, groupID := range []string{"group-1", "group-2", "group-3"} {
			exists, err := store.GroupExists(ctx, groupID)
			if err != nil {
				t.Fatalf("failed to check group %s existence: %v", groupID, err)
			}
			if exists {
				t.Errorf("expected group %s to be deleted", groupID)
			}
		}
	})

	t.Run("handles empty request", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{}

		resp := handler.Handle(context.Background(), 0, req)

		if len(resp.Groups) != 0 {
			t.Errorf("expected 0 groups in response, got %d", len(resp.Groups))
		}
	})
}

func TestDeleteGroupsHandler_WithLeaseManager(t *testing.T) {
	t.Run("returns NOT_COORDINATOR when lease not acquired", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)

		// Create two lease managers that will contend
		otherLeaseManager := groups.NewLeaseManager(metaStore, "other-broker")
		leaseManager := groups.NewLeaseManager(metaStore, "this-broker")
		handler := NewDeleteGroupsHandler(store, leaseManager, metaStore)

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

		// Other broker acquires lease first
		_, err = otherLeaseManager.AcquireLease(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to acquire lease: %v", err)
		}

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNotCoordinator {
			t.Errorf("expected error code %d, got %d", errDeleteGroupsNotCoordinator, resp.Groups[0].ErrorCode)
		}

		// Verify group still exists
		exists, err := store.GroupExists(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to check group existence: %v", err)
		}
		if !exists {
			t.Error("expected group to still exist")
		}
	})

	t.Run("deletes group when lease is acquired", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "this-broker")
		handler := NewDeleteGroupsHandler(store, leaseManager, metaStore)

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

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNone {
			t.Errorf("expected error code %d, got %d", errDeleteGroupsNone, resp.Groups[0].ErrorCode)
		}

		// Verify group is deleted
		exists, err := store.GroupExists(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to check group existence: %v", err)
		}
		if exists {
			t.Error("expected group to be deleted")
		}
	})
}

func TestDeleteGroupsHandler_ResponseVersion(t *testing.T) {
	t.Run("v0 response has no throttle", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"non-existent"}

		resp := handler.Handle(context.Background(), 0, req)

		if resp.GetVersion() != 0 {
			t.Errorf("expected version 0, got %d", resp.GetVersion())
		}
	})

	t.Run("v1+ response includes throttle", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"non-existent"}

		resp := handler.Handle(context.Background(), 1, req)

		if resp.GetVersion() != 1 {
			t.Errorf("expected version 1, got %d", resp.GetVersion())
		}
		if resp.ThrottleMillis != 0 {
			t.Errorf("expected ThrottleMillis 0, got %d", resp.ThrottleMillis)
		}
	})

	t.Run("v2 response is flexible encoded", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.SetVersion(2)
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 2, req)

		if resp.GetVersion() != 2 {
			t.Errorf("expected version 2, got %d", resp.GetVersion())
		}
	})
}

func TestDeleteGroupsHandler_GroupStates(t *testing.T) {
	// Groups in these states should be deletable (no members)
	deletableStates := []groups.GroupStateName{
		groups.GroupStateEmpty,
		groups.GroupStateDead,
	}

	for _, state := range deletableStates {
		t.Run("deletes "+string(state)+" group", func(t *testing.T) {
			metaStore := metadata.NewMockStore()
			store := groups.NewStore(metaStore)
			handler := NewDeleteGroupsHandler(store, nil, metaStore)

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

			// Transition to target state
			_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
				GroupID: "test-group",
				ToState: state,
				NowMs:   nowMs,
			})
			if err != nil {
				t.Fatalf("failed to transition state: %v", err)
			}

			req := kmsg.NewPtrDeleteGroupsRequest()
			req.Groups = []string{"test-group"}

			resp := handler.Handle(ctx, 0, req)

			if len(resp.Groups) != 1 {
				t.Fatalf("expected 1 group, got %d", len(resp.Groups))
			}
			if resp.Groups[0].ErrorCode != errDeleteGroupsNone {
				t.Errorf("expected no error for %s state, got %d", state, resp.Groups[0].ErrorCode)
			}
		})
	}

	// Groups in these states with active members should not be deletable
	nonDeletableStates := []groups.GroupStateName{
		groups.GroupStateStable,
		groups.GroupStatePreparingRebalance,
		groups.GroupStateCompletingRebalance,
	}

	for _, state := range nonDeletableStates {
		t.Run("rejects "+string(state)+" group with members", func(t *testing.T) {
			metaStore := metadata.NewMockStore()
			store := groups.NewStore(metaStore)
			handler := NewDeleteGroupsHandler(store, nil, metaStore)

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
				ClientHost:       "host-1",
				SessionTimeoutMs: 30000,
				NowMs:            nowMs,
			})
			if err != nil {
				t.Fatalf("failed to add member: %v", err)
			}

			// Transition to target state
			_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
				GroupID: "test-group",
				ToState: state,
				NowMs:   nowMs,
			})
			if err != nil {
				t.Fatalf("failed to transition state: %v", err)
			}

			req := kmsg.NewPtrDeleteGroupsRequest()
			req.Groups = []string{"test-group"}

			resp := handler.Handle(ctx, 0, req)

			if len(resp.Groups) != 1 {
				t.Fatalf("expected 1 group, got %d", len(resp.Groups))
			}
			if resp.Groups[0].ErrorCode != errDeleteGroupsNonEmptyGroup {
				t.Errorf("expected NON_EMPTY_GROUP error for %s state with members, got %d", state, resp.Groups[0].ErrorCode)
			}
		})
	}
}

func TestDeleteGroupsHandler_MembersAndAssignments(t *testing.T) {
	t.Run("deletes members and assignments with group", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

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
		for i := 0; i < 3; i++ {
			memberID := "member-" + string(rune('0'+i))
			_, err = store.AddMember(ctx, groups.AddMemberRequest{
				GroupID:          "test-group",
				MemberID:         memberID,
				ClientID:         "client",
				ClientHost:       "host",
				SessionTimeoutMs: 30000,
				NowMs:            nowMs,
			})
			if err != nil {
				t.Fatalf("failed to add member: %v", err)
			}

			// Set assignment
			_, err = store.SetAssignment(ctx, groups.SetAssignmentRequest{
				GroupID:    "test-group",
				MemberID:   memberID,
				Generation: 1,
				Data:       []byte("assignment"),
				NowMs:      nowMs,
			})
			if err != nil {
				t.Fatalf("failed to set assignment: %v", err)
			}
		}

		// Transition to Empty state (simulating all members have left)
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:          "test-group",
			ToState:          groups.GroupStateEmpty,
			ClearAssignments: true,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		// Remove all members
		members, _ := store.ListMembers(ctx, "test-group")
		for _, m := range members {
			_ = store.RemoveMember(ctx, "test-group", m.MemberID)
		}

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNone {
			t.Errorf("expected no error, got %d", resp.Groups[0].ErrorCode)
		}

		// Verify group is fully deleted
		exists, err := store.GroupExists(ctx, "test-group")
		if err != nil {
			t.Fatalf("failed to check group existence: %v", err)
		}
		if exists {
			t.Error("expected group to be deleted")
		}
	})
}

func TestDeleteGroupsHandler_NilMetaStore(t *testing.T) {
	t.Run("works without metadata store for offset cleanup", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		// Pass nil for the metadata store used for offset cleanup
		handler := NewDeleteGroupsHandler(store, nil, nil)

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

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDeleteGroupsNone {
			t.Errorf("expected no error, got %d", resp.Groups[0].ErrorCode)
		}
	})
}

func TestDeleteGroupsHandler_GroupIDPreserved(t *testing.T) {
	t.Run("group ID is preserved in response", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		handler := NewDeleteGroupsHandler(store, nil, metaStore)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"group-a", "group-b", "group-c"}

		resp := handler.Handle(context.Background(), 0, req)

		if len(resp.Groups) != 3 {
			t.Fatalf("expected 3 groups, got %d", len(resp.Groups))
		}

		for i, expected := range []string{"group-a", "group-b", "group-c"} {
			if resp.Groups[i].Group != expected {
				t.Errorf("expected group ID %q at index %d, got %q", expected, i, resp.Groups[i].Group)
			}
		}
	})
}
