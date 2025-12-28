package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDescribeGroupsHandler_Handle(t *testing.T) {
	t.Run("returns INVALID_GROUP_ID for empty group", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{""}

		resp := handler.Handle(context.Background(), 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDescribeGroupsInvalidGroupID {
			t.Errorf("expected error code %d, got %d", errDescribeGroupsInvalidGroupID, resp.Groups[0].ErrorCode)
		}
	})

	t.Run("returns Dead state for non-existent group (v0-v5)", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"non-existent-group"}

		resp := handler.Handle(context.Background(), 4, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDescribeGroupsNone {
			t.Errorf("expected error code %d, got %d", errDescribeGroupsNone, resp.Groups[0].ErrorCode)
		}
		if resp.Groups[0].State != string(groups.GroupStateDead) {
			t.Errorf("expected state %s, got %s", groups.GroupStateDead, resp.Groups[0].State)
		}
	})

	t.Run("returns GROUP_ID_NOT_FOUND for non-existent group (v6+)", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.SetVersion(6)
		req.Groups = []string{"non-existent-group"}

		resp := handler.Handle(context.Background(), 6, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDescribeGroupsGroupIDNotFound {
			t.Errorf("expected error code %d, got %d", errDescribeGroupsGroupIDNotFound, resp.Groups[0].ErrorCode)
		}
		if resp.Groups[0].ErrorMessage == nil || *resp.Groups[0].ErrorMessage == "" {
			t.Error("expected error message for v6+")
		}
	})

	t.Run("returns group state and members", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

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
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			ClientHost:       "host-1",
			ProtocolType:     "consumer",
			SessionTimeoutMs: 30000,
			Metadata:         []byte("metadata-1"),
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-2",
			ClientID:         "client-2",
			ClientHost:       "host-2",
			ProtocolType:     "consumer",
			SessionTimeoutMs: 30000,
			Metadata:         []byte("metadata-2"),
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		// Transition to stable state
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:  "test-group",
			ToState:  groups.GroupStateStable,
			Protocol: "range",
			NowMs:    nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}

		group := resp.Groups[0]
		if group.ErrorCode != errDescribeGroupsNone {
			t.Errorf("expected error code %d, got %d", errDescribeGroupsNone, group.ErrorCode)
		}
		if group.Group != "test-group" {
			t.Errorf("expected group ID 'test-group', got %q", group.Group)
		}
		if group.State != string(groups.GroupStateStable) {
			t.Errorf("expected state %s, got %s", groups.GroupStateStable, group.State)
		}
		if group.ProtocolType != "consumer" {
			t.Errorf("expected protocol type 'consumer', got %q", group.ProtocolType)
		}
		if group.Protocol != "range" {
			t.Errorf("expected protocol 'range', got %q", group.Protocol)
		}
		if len(group.Members) != 2 {
			t.Errorf("expected 2 members, got %d", len(group.Members))
		}
	})

	t.Run("includes protocol type and generation", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

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

		// Increment generation and set protocol
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			Protocol:     "roundrobin",
			Leader:       "member-1",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		group := resp.Groups[0]
		if group.ErrorCode != errDescribeGroupsNone {
			t.Errorf("expected error code %d, got %d", errDescribeGroupsNone, group.ErrorCode)
		}
		if group.ProtocolType != "consumer" {
			t.Errorf("expected protocol type 'consumer', got %q", group.ProtocolType)
		}
		if group.Protocol != "roundrobin" {
			t.Errorf("expected protocol 'roundrobin', got %q", group.Protocol)
		}
	})

	t.Run("returns member assignments", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

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
			Metadata:         []byte("metadata"),
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		// Set assignment
		assignmentData := []byte{0x00, 0x01, 0x02, 0x03}
		_, err = store.SetAssignment(ctx, groups.SetAssignmentRequest{
			GroupID:    "test-group",
			MemberID:   "member-1",
			Generation: 1,
			Data:       assignmentData,
			NowMs:      nowMs,
		})
		if err != nil {
			t.Fatalf("failed to set assignment: %v", err)
		}

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		group := resp.Groups[0]
		if group.ErrorCode != errDescribeGroupsNone {
			t.Errorf("expected error code %d, got %d", errDescribeGroupsNone, group.ErrorCode)
		}
		if len(group.Members) != 1 {
			t.Fatalf("expected 1 member, got %d", len(group.Members))
		}

		member := group.Members[0]
		if member.MemberID != "member-1" {
			t.Errorf("expected member ID 'member-1', got %q", member.MemberID)
		}
		if member.ClientID != "client-1" {
			t.Errorf("expected client ID 'client-1', got %q", member.ClientID)
		}
		if member.ClientHost != "host-1" {
			t.Errorf("expected client host 'host-1', got %q", member.ClientHost)
		}
		if string(member.ProtocolMetadata) != "metadata" {
			t.Errorf("expected metadata 'metadata', got %q", string(member.ProtocolMetadata))
		}
		if string(member.MemberAssignment) != string(assignmentData) {
			t.Errorf("expected assignment %v, got %v", assignmentData, member.MemberAssignment)
		}
	})

	t.Run("describes multiple groups", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create two groups
		for _, groupID := range []string{"group-1", "group-2"} {
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

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"group-1", "group-2", "non-existent"}

		resp := handler.Handle(ctx, 4, req)

		if len(resp.Groups) != 3 {
			t.Fatalf("expected 3 groups in response, got %d", len(resp.Groups))
		}

		// Check group-1
		if resp.Groups[0].Group != "group-1" {
			t.Errorf("expected group ID 'group-1', got %q", resp.Groups[0].Group)
		}
		if resp.Groups[0].ErrorCode != errDescribeGroupsNone {
			t.Errorf("expected error code %d for group-1, got %d", errDescribeGroupsNone, resp.Groups[0].ErrorCode)
		}

		// Check group-2
		if resp.Groups[1].Group != "group-2" {
			t.Errorf("expected group ID 'group-2', got %q", resp.Groups[1].Group)
		}
		if resp.Groups[1].ErrorCode != errDescribeGroupsNone {
			t.Errorf("expected error code %d for group-2, got %d", errDescribeGroupsNone, resp.Groups[1].ErrorCode)
		}

		// Check non-existent (v4 returns Dead state)
		if resp.Groups[2].Group != "non-existent" {
			t.Errorf("expected group ID 'non-existent', got %q", resp.Groups[2].Group)
		}
		if resp.Groups[2].ErrorCode != errDescribeGroupsNone {
			t.Errorf("expected error code %d for non-existent, got %d", errDescribeGroupsNone, resp.Groups[2].ErrorCode)
		}
		if resp.Groups[2].State != string(groups.GroupStateDead) {
			t.Errorf("expected state %s for non-existent, got %s", groups.GroupStateDead, resp.Groups[2].State)
		}
	})

	t.Run("v4+ includes instance ID", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

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
			ClientHost:       "host-1",
			GroupInstanceID:  "instance-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.SetVersion(4)
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 4, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if len(resp.Groups[0].Members) != 1 {
			t.Fatalf("expected 1 member, got %d", len(resp.Groups[0].Members))
		}

		member := resp.Groups[0].Members[0]
		if member.InstanceID == nil {
			t.Error("expected instance ID to be set for v4+")
		} else if *member.InstanceID != "instance-1" {
			t.Errorf("expected instance ID 'instance-1', got %q", *member.InstanceID)
		}
	})

	t.Run("v3+ includes authorized operations when requested", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

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

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.SetVersion(3)
		req.Groups = []string{"test-group"}
		req.IncludeAuthorizedOperations = true

		resp := handler.Handle(ctx, 3, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}

		// Should have authorized operations set (READ | DESCRIBE = 0x500)
		if resp.Groups[0].AuthorizedOperations != 0x500 {
			t.Errorf("expected authorized operations 0x500, got 0x%x", resp.Groups[0].AuthorizedOperations)
		}
	})

	t.Run("v3+ uses default authorized operations when not requested", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

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

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.SetVersion(3)
		req.Groups = []string{"test-group"}
		req.IncludeAuthorizedOperations = false

		resp := handler.Handle(ctx, 3, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}

		// Should have default value
		if resp.Groups[0].AuthorizedOperations != -2147483648 {
			t.Errorf("expected default authorized operations -2147483648, got %d", resp.Groups[0].AuthorizedOperations)
		}
	})

	t.Run("handles empty request", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{}

		resp := handler.Handle(context.Background(), 0, req)

		if len(resp.Groups) != 0 {
			t.Errorf("expected 0 groups in response, got %d", len(resp.Groups))
		}
	})
}

func TestDescribeGroupsHandler_WithLeaseManager(t *testing.T) {
	t.Run("returns NOT_COORDINATOR when lease not acquired", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)

		// Create a lease manager with a different broker that will acquire the lease first
		otherLeaseManager := groups.NewLeaseManager(metaStore, "other-broker")

		// Use the same metaStore for both lease managers so they contend
		leaseManager := groups.NewLeaseManager(metaStore, "this-broker")
		handler := NewDescribeGroupsHandler(store, leaseManager)

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

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errDescribeGroupsNotCoordinator {
			t.Errorf("expected error code %d, got %d", errDescribeGroupsNotCoordinator, resp.Groups[0].ErrorCode)
		}
	})
}

func TestDescribeGroupsHandler_ResponseVersion(t *testing.T) {
	t.Run("v0 response is set correctly", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if resp.GetVersion() != 0 {
			t.Errorf("expected version 0, got %d", resp.GetVersion())
		}
	})

	t.Run("v5 response is flexible encoded", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewDescribeGroupsHandler(store, nil)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrDescribeGroupsRequest()
		req.SetVersion(5)
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 5, req)

		if resp.GetVersion() != 5 {
			t.Errorf("expected version 5, got %d", resp.GetVersion())
		}
	})
}

func TestDescribeGroupsHandler_AllGroupStates(t *testing.T) {
	states := []groups.GroupStateName{
		groups.GroupStateEmpty,
		groups.GroupStateStable,
		groups.GroupStatePreparingRebalance,
		groups.GroupStateCompletingRebalance,
		groups.GroupStateDead,
	}

	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			store := groups.NewStore(metadata.NewMockStore())
			handler := NewDescribeGroupsHandler(store, nil)

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

			// Transition to the target state
			_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
				GroupID: "test-group",
				ToState: state,
				NowMs:   nowMs,
			})
			if err != nil {
				t.Fatalf("failed to transition state: %v", err)
			}

			req := kmsg.NewPtrDescribeGroupsRequest()
			req.Groups = []string{"test-group"}

			resp := handler.Handle(ctx, 0, req)

			if len(resp.Groups) != 1 {
				t.Fatalf("expected 1 group, got %d", len(resp.Groups))
			}
			if resp.Groups[0].ErrorCode != errDescribeGroupsNone {
				t.Errorf("expected no error, got %d", resp.Groups[0].ErrorCode)
			}
			if resp.Groups[0].State != string(state) {
				t.Errorf("expected state %s, got %s", state, resp.Groups[0].State)
			}
		})
	}
}
