package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestListGroupsHandler_Handle(t *testing.T) {
	t.Run("returns empty list when no groups exist", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		req := kmsg.NewPtrListGroupsRequest()

		resp := handler.Handle(context.Background(), 0, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 0 {
			t.Errorf("expected 0 groups, got %d", len(resp.Groups))
		}
	})

	t.Run("returns all groups on this broker", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create multiple groups
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

		req := kmsg.NewPtrListGroupsRequest()

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 3 {
			t.Errorf("expected 3 groups, got %d", len(resp.Groups))
		}

		// Verify all groups are present
		groupIDs := make(map[string]bool)
		for _, g := range resp.Groups {
			groupIDs[g.Group] = true
		}
		for _, expected := range []string{"group-1", "group-2", "group-3"} {
			if !groupIDs[expected] {
				t.Errorf("expected group %s in response", expected)
			}
		}
	})

	t.Run("returns group IDs and protocol types", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group with specific protocol type
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		req := kmsg.NewPtrListGroupsRequest()

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}

		group := resp.Groups[0]
		if group.Group != "test-group" {
			t.Errorf("expected group ID 'test-group', got %q", group.Group)
		}
		if group.ProtocolType != "consumer" {
			t.Errorf("expected protocol type 'consumer', got %q", group.ProtocolType)
		}
	})

	t.Run("v4+ supports state filter", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create groups in different states
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "empty-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		_, err = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "stable-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		// Transition second group to stable
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "stable-group",
			ToState: groups.GroupStateStable,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		// Filter for only stable groups
		req := kmsg.NewPtrListGroupsRequest()
		req.SetVersion(4)
		req.StatesFilter = []string{"Stable"}

		resp := handler.Handle(ctx, 4, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].Group != "stable-group" {
			t.Errorf("expected group 'stable-group', got %q", resp.Groups[0].Group)
		}
	})

	t.Run("v4+ includes group state in response", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create group and transition to stable
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "test-group",
			ToState: groups.GroupStateStable,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("failed to transition state: %v", err)
		}

		req := kmsg.NewPtrListGroupsRequest()
		req.SetVersion(4)

		resp := handler.Handle(ctx, 4, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].GroupState != "Stable" {
			t.Errorf("expected group state 'Stable', got %q", resp.Groups[0].GroupState)
		}
	})

	t.Run("v4+ state filter with multiple states", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create groups in different states
		groupStates := []struct {
			id    string
			state groups.GroupStateName
		}{
			{"empty-group", groups.GroupStateEmpty},
			{"stable-group", groups.GroupStateStable},
			{"rebalancing-group", groups.GroupStatePreparingRebalance},
			{"completing-group", groups.GroupStateCompletingRebalance},
		}

		for _, gs := range groupStates {
			_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
				GroupID:      gs.id,
				Type:         groups.GroupTypeClassic,
				ProtocolType: "consumer",
				NowMs:        nowMs,
			})
			if err != nil {
				t.Fatalf("failed to create group %s: %v", gs.id, err)
			}

			if gs.state != groups.GroupStateEmpty {
				_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
					GroupID: gs.id,
					ToState: gs.state,
					NowMs:   nowMs,
				})
				if err != nil {
					t.Fatalf("failed to transition state for %s: %v", gs.id, err)
				}
			}
		}

		// Filter for stable and empty groups
		req := kmsg.NewPtrListGroupsRequest()
		req.SetVersion(4)
		req.StatesFilter = []string{"Empty", "Stable"}

		resp := handler.Handle(ctx, 4, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 2 {
			t.Errorf("expected 2 groups, got %d", len(resp.Groups))
		}

		groupIDs := make(map[string]bool)
		for _, g := range resp.Groups {
			groupIDs[g.Group] = true
		}
		if !groupIDs["empty-group"] {
			t.Error("expected 'empty-group' in response")
		}
		if !groupIDs["stable-group"] {
			t.Error("expected 'stable-group' in response")
		}
	})

	t.Run("v4+ empty state filter returns all groups", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create multiple groups
		for _, groupID := range []string{"group-1", "group-2"} {
			_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
				GroupID:      groupID,
				Type:         groups.GroupTypeClassic,
				ProtocolType: "consumer",
				NowMs:        nowMs,
			})
			if err != nil {
				t.Fatalf("failed to create group: %v", err)
			}
		}

		// No state filter
		req := kmsg.NewPtrListGroupsRequest()
		req.SetVersion(4)
		req.StatesFilter = []string{}

		resp := handler.Handle(ctx, 4, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 2 {
			t.Errorf("expected 2 groups, got %d", len(resp.Groups))
		}
	})

	t.Run("v0-v3 ignores state filter", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create groups in different states
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "empty-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "stable-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "stable-group",
			ToState: groups.GroupStateStable,
			NowMs:   nowMs,
		})

		// v3 request with state filter (should be ignored)
		req := kmsg.NewPtrListGroupsRequest()
		req.SetVersion(3)
		req.StatesFilter = []string{"Stable"} // This should be ignored for v3

		resp := handler.Handle(ctx, 3, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		// Both groups should be returned since state filter is ignored for v3
		if len(resp.Groups) != 2 {
			t.Errorf("expected 2 groups (state filter ignored for v3), got %d", len(resp.Groups))
		}
	})

	t.Run("response version is set correctly", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		for _, version := range []int16{0, 1, 2, 3, 4} {
			t.Run("version "+string(rune('0'+version)), func(t *testing.T) {
				req := kmsg.NewPtrListGroupsRequest()
				req.SetVersion(version)

				resp := handler.Handle(context.Background(), version, req)

				if resp.GetVersion() != version {
					t.Errorf("expected version %d, got %d", version, resp.GetVersion())
				}
			})
		}
	})

	t.Run("groups with different protocol types", func(t *testing.T) {
		store := groups.NewStore(metadata.NewMockStore())
		handler := NewListGroupsHandler(store)

		ctx := context.Background()
		nowMs := time.Now().UnixMilli()

		// Create groups with different protocol types
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "consumer-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "connect-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "connect",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrListGroupsRequest()

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errListGroupsNone {
			t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
		}
		if len(resp.Groups) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(resp.Groups))
		}

		// Build a map of group -> protocol type
		protocolTypes := make(map[string]string)
		for _, g := range resp.Groups {
			protocolTypes[g.Group] = g.ProtocolType
		}

		if protocolTypes["consumer-group"] != "consumer" {
			t.Errorf("expected protocol type 'consumer' for consumer-group, got %q", protocolTypes["consumer-group"])
		}
		if protocolTypes["connect-group"] != "connect" {
			t.Errorf("expected protocol type 'connect' for connect-group, got %q", protocolTypes["connect-group"])
		}
	})
}

func TestListGroupsHandler_AllGroupStates(t *testing.T) {
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
			handler := NewListGroupsHandler(store)

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

			// Filter for this state
			req := kmsg.NewPtrListGroupsRequest()
			req.SetVersion(4)
			req.StatesFilter = []string{string(state)}

			resp := handler.Handle(ctx, 4, req)

			if resp.ErrorCode != errListGroupsNone {
				t.Errorf("expected no error, got %d", resp.ErrorCode)
			}
			if len(resp.Groups) != 1 {
				t.Errorf("expected 1 group for state %s, got %d", state, len(resp.Groups))
			}
			if len(resp.Groups) > 0 && resp.Groups[0].GroupState != string(state) {
				t.Errorf("expected state %s, got %s", state, resp.Groups[0].GroupState)
			}
		})
	}
}

func TestListGroupsHandler_StateFilterNoMatch(t *testing.T) {
	store := groups.NewStore(metadata.NewMockStore())
	handler := NewListGroupsHandler(store)

	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	// Create an empty group
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      "empty-group",
		Type:         groups.GroupTypeClassic,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Filter for stable groups (none exist)
	req := kmsg.NewPtrListGroupsRequest()
	req.SetVersion(4)
	req.StatesFilter = []string{"Stable"}

	resp := handler.Handle(ctx, 4, req)

	if resp.ErrorCode != errListGroupsNone {
		t.Errorf("expected error code %d, got %d", errListGroupsNone, resp.ErrorCode)
	}
	if len(resp.Groups) != 0 {
		t.Errorf("expected 0 groups, got %d", len(resp.Groups))
	}
}
