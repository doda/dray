package groups

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

// mockClock implements Clock for testing.
type mockClock struct {
	now time.Time
}

func (m *mockClock) Now() time.Time { return m.now }

func (m *mockClock) Advance(d time.Duration) {
	m.now = m.now.Add(d)
}

func TestSessionSweeper_OnlyRunOnLeaseHolder(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add a member
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Transition to Stable state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// SweepGroup should fail because we don't hold the lease
	_, err = sweeper.SweepGroup(ctx, "test-group")
	if err != ErrLeaseNotHeld {
		t.Fatalf("expected ErrLeaseNotHeld, got %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !result.Acquired {
		t.Fatal("failed to acquire lease")
	}

	// Advance time past session timeout
	clock.Advance(31 * time.Second)

	// SweepGroup should succeed now
	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 member removed, got %d", removed)
	}
}

func TestSessionSweeper_CheckLastHeartbeatTime(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add members with different session timeouts
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 10000, // 10s timeout
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-1: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-2",
		ClientID:         "client-2",
		SessionTimeoutMs: 30000, // 30s timeout
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-2: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-3",
		ClientID:         "client-3",
		SessionTimeoutMs: 60000, // 60s timeout
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-3: %v", err)
	}

	// Transition to Stable state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil || !result.Acquired {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Advance time 15s - only member-1 should be expired
	clock.Advance(15 * time.Second)

	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 member removed, got %d", removed)
	}

	// Verify member-1 was removed
	members, err := store.ListMembers(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}

	memberIDs := make(map[string]bool)
	for _, m := range members {
		memberIDs[m.MemberID] = true
	}
	if memberIDs["member-1"] {
		t.Fatal("member-1 should have been removed")
	}
	if !memberIDs["member-2"] {
		t.Fatal("member-2 should still be present")
	}
	if !memberIDs["member-3"] {
		t.Fatal("member-3 should still be present")
	}

	// Advance time another 20s (total 35s) - member-2 should be expired
	clock.Advance(20 * time.Second)

	removed, err = sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 member removed, got %d", removed)
	}

	// Verify member-2 was removed
	members, err = store.ListMembers(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].MemberID != "member-3" {
		t.Fatalf("expected member-3, got %s", members[0].MemberID)
	}
}

func TestSessionSweeper_RemoveMembersExceedingTimeout(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add multiple members with the same timeout
	for i := 1; i <= 3; i++ {
		_, err = store.AddMember(ctx, AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-" + string(rune('0'+i)),
			ClientID:         "client-" + string(rune('0'+i)),
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member-%d: %v", i, err)
		}
	}

	// Transition to Stable state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil || !result.Acquired {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Advance time past timeout
	clock.Advance(31 * time.Second)

	// All members should be removed
	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 3 {
		t.Fatalf("expected 3 members removed, got %d", removed)
	}

	// Verify all members are removed
	members, err := store.ListMembers(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("expected 0 members, got %d", len(members))
	}
}

func TestSessionSweeper_TriggerRebalanceOnMemberRemoval(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add 2 members
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 10000, // 10s timeout
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-1: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-2",
		ClientID:         "client-2",
		SessionTimeoutMs: 60000, // 60s timeout
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-2: %v", err)
	}

	// Transition to Stable state with assignments
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Set assignments for both members
	_, err = store.SetAssignment(ctx, SetAssignmentRequest{
		GroupID:    "test-group",
		MemberID:   "member-1",
		Generation: 0,
		Data:       []byte("assignment-1"),
		NowMs:      nowMs,
	})
	if err != nil {
		t.Fatalf("failed to set assignment for member-1: %v", err)
	}

	_, err = store.SetAssignment(ctx, SetAssignmentRequest{
		GroupID:    "test-group",
		MemberID:   "member-2",
		Generation: 0,
		Data:       []byte("assignment-2"),
		NowMs:      nowMs,
	})
	if err != nil {
		t.Fatalf("failed to set assignment for member-2: %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil || !result.Acquired {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Advance time past member-1's timeout
	clock.Advance(15 * time.Second)

	// Sweep should remove member-1 and trigger rebalance
	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 member removed, got %d", removed)
	}

	// Verify group is in PreparingRebalance state (since member-2 remains)
	state, err := store.GetGroupState(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}
	if state.State != GroupStatePreparingRebalance {
		t.Fatalf("expected state PreparingRebalance, got %s", state.State)
	}

	// Assignments should be cleared
	assignments, err := store.ListAssignments(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to list assignments: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("expected 0 assignments, got %d", len(assignments))
	}
}

func TestSessionSweeper_TransitionToEmptyWhenAllMembersExpired(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add a single member
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-1: %v", err)
	}

	// Transition to Stable state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil || !result.Acquired {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Advance time past timeout
	clock.Advance(31 * time.Second)

	// Sweep should remove member-1 and transition to Empty
	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 member removed, got %d", removed)
	}

	// Verify group is in Empty state
	state, err := store.GetGroupState(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}
	if state.State != GroupStateEmpty {
		t.Fatalf("expected state Empty, got %s", state.State)
	}
}

func TestSessionSweeper_NoRemovalIfHeartbeatReceived(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add a member
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-1: %v", err)
	}

	// Transition to Stable state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil || !result.Acquired {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Advance time 20s (within timeout)
	clock.Advance(20 * time.Second)

	// Sweep should not remove member
	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 0 {
		t.Fatalf("expected 0 members removed, got %d", removed)
	}

	// Update heartbeat
	heartbeatTime := clock.Now().UnixMilli()
	err = store.UpdateMemberHeartbeat(ctx, "test-group", "member-1", heartbeatTime)
	if err != nil {
		t.Fatalf("failed to update heartbeat: %v", err)
	}

	// Advance time another 20s (40s from original, but only 20s from heartbeat)
	clock.Advance(20 * time.Second)

	// Sweep should still not remove member
	removed, err = sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 0 {
		t.Fatalf("expected 0 members removed, got %d", removed)
	}

	// Verify member still exists
	members, err := store.ListMembers(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
}

func TestSessionSweeper_SkipDeadGroups(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add a member
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member-1: %v", err)
	}

	// Transition to Dead state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateDead,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil || !result.Acquired {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Advance time past timeout
	clock.Advance(31 * time.Second)

	// Sweep should not remove members from Dead groups
	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 0 {
		t.Fatalf("expected 0 members removed from Dead group, got %d", removed)
	}
}

func TestSessionSweeper_Sweep(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	nowMs := clock.Now().UnixMilli()

	// Create group-1 with expired member
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "group-1",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 10000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group-1: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "group-1",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 10000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member to group-1: %v", err)
	}

	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "group-1",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition group-1: %v", err)
	}

	// Create group-2 with non-expired member
	_, err = store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "group-2",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 60000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group-2: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "group-2",
		MemberID:         "member-2",
		ClientID:         "client-2",
		SessionTimeoutMs: 60000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member to group-2: %v", err)
	}

	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "group-2",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition group-2: %v", err)
	}

	// Acquire leases for both groups
	_, err = leaseManager.AcquireLease(ctx, "group-1")
	if err != nil {
		t.Fatalf("failed to acquire lease for group-1: %v", err)
	}
	_, err = leaseManager.AcquireLease(ctx, "group-2")
	if err != nil {
		t.Fatalf("failed to acquire lease for group-2: %v", err)
	}

	// Advance time past group-1's timeout
	clock.Advance(15 * time.Second)

	// Sweep should check both groups, remove 1 member, and trigger 1 rebalance
	result, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.GroupsChecked != 2 {
		t.Fatalf("expected 2 groups checked, got %d", result.GroupsChecked)
	}
	if result.MembersRemoved != 1 {
		t.Fatalf("expected 1 member removed, got %d", result.MembersRemoved)
	}
	if result.RebalancesTriggered != 1 {
		t.Fatalf("expected 1 rebalance triggered, got %d", result.RebalancesTriggered)
	}

	// Verify group-1 is now Empty
	state, err := store.GetGroupState(ctx, "group-1")
	if err != nil {
		t.Fatalf("failed to get group-1 state: %v", err)
	}
	if state.State != GroupStateEmpty {
		t.Fatalf("expected group-1 state Empty, got %s", state.State)
	}

	// Verify group-2 is still Stable
	state, err = store.GetGroupState(ctx, "group-2")
	if err != nil {
		t.Fatalf("failed to get group-2 state: %v", err)
	}
	if state.State != GroupStateStable {
		t.Fatalf("expected group-2 state Stable, got %s", state.State)
	}
}

func TestSessionSweeper_NoSweepWithoutLeases(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group with expired member
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 10000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 10000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Advance time past timeout
	clock.Advance(15 * time.Second)

	// Sweep should check 0 groups (no leases held)
	result, err := sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.GroupsChecked != 0 {
		t.Fatalf("expected 0 groups checked, got %d", result.GroupsChecked)
	}
	if result.MembersRemoved != 0 {
		t.Fatalf("expected 0 members removed, got %d", result.MembersRemoved)
	}

	// Verify member still exists
	members, err := store.ListMembers(ctx, "test-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
}

func TestSessionSweeper_StartStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	meta := metadata.NewMockStore()
	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")

	// Create sweeper with a short interval for testing
	sweeper := NewSessionSweeper(store, leaseManager, 50*time.Millisecond)

	// Start the sweeper
	sweeper.Start(ctx)

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Stop the sweeper
	sweeper.Stop()

	// Starting again should work
	sweeper.Start(ctx)

	// Stopping again should work
	sweeper.Stop()
}

func TestSessionSweeper_ZeroSessionTimeout(t *testing.T) {
	ctx := context.Background()
	meta := metadata.NewMockStore()

	store := NewStore(meta)
	leaseManager := NewLeaseManager(meta, "broker-1")
	sweeper := NewSessionSweeper(store, leaseManager, time.Second)

	clock := &mockClock{now: time.Now()}
	sweeper.SetClock(clock)

	// Create a group
	nowMs := clock.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 0,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add a member with 0 session timeout
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "test-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 0,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Transition to Stable state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID: "test-group",
		ToState: GroupStateStable,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Acquire the lease
	result, err := leaseManager.AcquireLease(ctx, "test-group")
	if err != nil || !result.Acquired {
		t.Fatalf("failed to acquire lease: %v", err)
	}

	// Advance time
	clock.Advance(time.Hour)

	// Member with 0 timeout should not be expired
	removed, err := sweeper.SweepGroup(ctx, "test-group")
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if removed != 0 {
		t.Fatalf("expected 0 members removed (0 timeout = no expire), got %d", removed)
	}
}
