package groups

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

func TestStore_CreateGroup(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	state, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "test-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	if state.GroupID != "test-group" {
		t.Errorf("expected group id 'test-group', got %s", state.GroupID)
	}
	if state.Generation != 0 {
		t.Errorf("expected initial generation 0, got %d", state.Generation)
	}
	if state.State != GroupStateEmpty {
		t.Errorf("expected initial state 'Empty', got %s", state.State)
	}
	if state.ProtocolType != "consumer" {
		t.Errorf("expected protocol type 'consumer', got %s", state.ProtocolType)
	}
	if state.SessionTimeoutMs != 30000 {
		t.Errorf("expected session timeout 30000, got %d", state.SessionTimeoutMs)
	}
}

func TestStore_CreateGroupDuplicate(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "dup-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}

	_, err = store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "dup-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if !errors.Is(err, ErrGroupExists) {
		t.Errorf("expected ErrGroupExists, got %v", err)
	}
}

func TestStore_GetGroupType(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Test classic group
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "classic-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create classic group: %v", err)
	}

	groupType, err := store.GetGroupType(ctx, "classic-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != GroupTypeClassic {
		t.Errorf("expected 'classic', got %s", groupType)
	}

	// Test consumer group
	_, err = store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "consumer-group",
		Type:    GroupTypeConsumer,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	groupType, err = store.GetGroupType(ctx, "consumer-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != GroupTypeConsumer {
		t.Errorf("expected 'consumer', got %s", groupType)
	}
}

func TestStore_GetGroupTypeNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.GetGroupType(ctx, "nonexistent")
	if !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}

func TestStore_GetGroupState(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	nowMs := time.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID:          "state-group",
		Type:             GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 45000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	state, err := store.GetGroupState(ctx, "state-group")
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}

	if state.GroupID != "state-group" {
		t.Errorf("expected group id 'state-group', got %s", state.GroupID)
	}
	if state.State != GroupStateEmpty {
		t.Errorf("expected state 'Empty', got %s", state.State)
	}
	if state.Generation != 0 {
		t.Errorf("expected generation 0, got %d", state.Generation)
	}
	if state.CreatedAtMs != nowMs {
		t.Errorf("expected created_at %d, got %d", nowMs, state.CreatedAtMs)
	}
}

func TestStore_GetGroupStateNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.GetGroupState(ctx, "nonexistent")
	if !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}

func TestStore_UpdateGroupState(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "update-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	nowMs := time.Now().UnixMilli()
	updated, err := store.UpdateGroupState(ctx, UpdateGroupStateRequest{
		GroupID:    "update-group",
		State:      GroupStatePreparingRebalance,
		Generation: 1,
		Leader:     "member-1",
		Protocol:   "range",
		NowMs:      nowMs,
	})
	if err != nil {
		t.Fatalf("failed to update group state: %v", err)
	}

	if updated.State != GroupStatePreparingRebalance {
		t.Errorf("expected state 'PreparingRebalance', got %s", updated.State)
	}
	if updated.Generation != 1 {
		t.Errorf("expected generation 1, got %d", updated.Generation)
	}
	if updated.Leader != "member-1" {
		t.Errorf("expected leader 'member-1', got %s", updated.Leader)
	}
	if updated.Protocol != "range" {
		t.Errorf("expected protocol 'range', got %s", updated.Protocol)
	}
	if updated.UpdatedAtMs != nowMs {
		t.Errorf("expected updated_at %d, got %d", nowMs, updated.UpdatedAtMs)
	}
}

func TestStore_IncrementGeneration(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "gen-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Increment generation multiple times
	for i := 1; i <= 5; i++ {
		state, err := store.IncrementGeneration(ctx, "gen-group", time.Now().UnixMilli())
		if err != nil {
			t.Fatalf("failed to increment generation: %v", err)
		}
		if state.Generation != int32(i) {
			t.Errorf("expected generation %d, got %d", i, state.Generation)
		}
	}
}

func TestStore_DeleteGroup(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create group with members
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "delete-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add a member
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "delete-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 30000,
		NowMs:            time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Set assignment
	_, err = store.SetAssignment(ctx, SetAssignmentRequest{
		GroupID:    "delete-group",
		MemberID:   "member-1",
		Generation: 1,
		Data:       []byte("assignment-data"),
		NowMs:      time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to set assignment: %v", err)
	}

	// Delete the group
	err = store.DeleteGroup(ctx, "delete-group")
	if err != nil {
		t.Fatalf("failed to delete group: %v", err)
	}

	// Verify group no longer exists
	exists, err := store.GroupExists(ctx, "delete-group")
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if exists {
		t.Error("group should not exist after deletion")
	}

	// Verify members are gone
	members, err := store.ListMembers(ctx, "delete-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("expected 0 members after deletion, got %d", len(members))
	}
}

func TestStore_GroupExists(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	exists, err := store.GroupExists(ctx, "exists-group")
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if exists {
		t.Error("group should not exist yet")
	}

	_, err = store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "exists-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	exists, err = store.GroupExists(ctx, "exists-group")
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if !exists {
		t.Error("group should exist")
	}
}

func TestStore_AddMember(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "member-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	nowMs := time.Now().UnixMilli()
	member, err := store.AddMember(ctx, AddMemberRequest{
		GroupID:            "member-group",
		MemberID:           "member-1",
		ClientID:           "client-1",
		ClientHost:         "192.168.1.100",
		ProtocolType:       "consumer",
		SessionTimeoutMs:   30000,
		RebalanceTimeoutMs: 60000,
		GroupInstanceID:    "instance-1",
		Metadata:           []byte("metadata"),
		NowMs:              nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	if member.MemberID != "member-1" {
		t.Errorf("expected member id 'member-1', got %s", member.MemberID)
	}
	if member.GroupID != "member-group" {
		t.Errorf("expected group id 'member-group', got %s", member.GroupID)
	}
	if member.ClientID != "client-1" {
		t.Errorf("expected client id 'client-1', got %s", member.ClientID)
	}
	if member.ClientHost != "192.168.1.100" {
		t.Errorf("expected client host '192.168.1.100', got %s", member.ClientHost)
	}
	if member.SessionTimeoutMs != 30000 {
		t.Errorf("expected session timeout 30000, got %d", member.SessionTimeoutMs)
	}
	if member.RebalanceTimeoutMs != 60000 {
		t.Errorf("expected rebalance timeout 60000, got %d", member.RebalanceTimeoutMs)
	}
	if member.GroupInstanceID != "instance-1" {
		t.Errorf("expected group instance id 'instance-1', got %s", member.GroupInstanceID)
	}
	if member.JoinedAtMs != nowMs {
		t.Errorf("expected joined_at %d, got %d", nowMs, member.JoinedAtMs)
	}
}

func TestStore_AddMemberToNonexistentGroup(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.AddMember(ctx, AddMemberRequest{
		GroupID:  "nonexistent-group",
		MemberID: "member-1",
		NowMs:    time.Now().UnixMilli(),
	})
	if !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}

func TestStore_GetMember(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "get-member-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:          "get-member-group",
		MemberID:         "member-1",
		ClientID:         "client-1",
		SessionTimeoutMs: 30000,
		NowMs:            time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	member, err := store.GetMember(ctx, "get-member-group", "member-1")
	if err != nil {
		t.Fatalf("failed to get member: %v", err)
	}

	if member.MemberID != "member-1" {
		t.Errorf("expected member id 'member-1', got %s", member.MemberID)
	}
	if member.ClientID != "client-1" {
		t.Errorf("expected client id 'client-1', got %s", member.ClientID)
	}
}

func TestStore_GetMemberNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "member-not-found-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.GetMember(ctx, "member-not-found-group", "nonexistent")
	if !errors.Is(err, ErrMemberNotFound) {
		t.Errorf("expected ErrMemberNotFound, got %v", err)
	}
}

func TestStore_ListMembers(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "list-members-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add multiple members
	for i := 1; i <= 3; i++ {
		_, err = store.AddMember(ctx, AddMemberRequest{
			GroupID:  "list-members-group",
			MemberID: "member-" + string(rune('0'+i)),
			ClientID: "client-" + string(rune('0'+i)),
			NowMs:    time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to add member %d: %v", i, err)
		}
	}

	members, err := store.ListMembers(ctx, "list-members-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}

	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}

	seen := make(map[string]bool)
	for _, m := range members {
		seen[m.MemberID] = true
	}
	for i := 1; i <= 3; i++ {
		memberID := "member-" + string(rune('0'+i))
		if !seen[memberID] {
			t.Errorf("missing member %s", memberID)
		}
	}
}

func TestStore_UpdateMemberHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "heartbeat-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	oldNow := time.Now().UnixMilli()
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:  "heartbeat-group",
		MemberID: "member-1",
		NowMs:    oldNow,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	newNow := oldNow + 10000
	err = store.UpdateMemberHeartbeat(ctx, "heartbeat-group", "member-1", newNow)
	if err != nil {
		t.Fatalf("failed to update heartbeat: %v", err)
	}

	member, err := store.GetMember(ctx, "heartbeat-group", "member-1")
	if err != nil {
		t.Fatalf("failed to get member: %v", err)
	}

	if member.LastHeartbeatMs != newNow {
		t.Errorf("expected last heartbeat %d, got %d", newNow, member.LastHeartbeatMs)
	}
}

func TestStore_RemoveMember(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "remove-member-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:  "remove-member-group",
		MemberID: "member-1",
		NowMs:    time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Add an assignment
	_, err = store.SetAssignment(ctx, SetAssignmentRequest{
		GroupID:    "remove-member-group",
		MemberID:   "member-1",
		Generation: 1,
		Data:       []byte("data"),
		NowMs:      time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to set assignment: %v", err)
	}

	// Remove the member
	err = store.RemoveMember(ctx, "remove-member-group", "member-1")
	if err != nil {
		t.Fatalf("failed to remove member: %v", err)
	}

	// Verify member is gone
	_, err = store.GetMember(ctx, "remove-member-group", "member-1")
	if !errors.Is(err, ErrMemberNotFound) {
		t.Errorf("expected ErrMemberNotFound, got %v", err)
	}

	// Verify assignment is also gone
	assignment, err := store.GetAssignment(ctx, "remove-member-group", "member-1")
	if err != nil {
		t.Fatalf("failed to get assignment: %v", err)
	}
	if assignment != nil {
		t.Error("assignment should be nil after member removal")
	}
}

func TestStore_SetAssignment(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "assignment-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	nowMs := time.Now().UnixMilli()
	assignment, err := store.SetAssignment(ctx, SetAssignmentRequest{
		GroupID:    "assignment-group",
		MemberID:   "member-1",
		Generation: 5,
		Data:       []byte("assignment-data"),
		Partitions: []byte("partitions"),
		UserData:   []byte("user-data"),
		NowMs:      nowMs,
	})
	if err != nil {
		t.Fatalf("failed to set assignment: %v", err)
	}

	if assignment.MemberID != "member-1" {
		t.Errorf("expected member id 'member-1', got %s", assignment.MemberID)
	}
	if assignment.GroupID != "assignment-group" {
		t.Errorf("expected group id 'assignment-group', got %s", assignment.GroupID)
	}
	if assignment.Generation != 5 {
		t.Errorf("expected generation 5, got %d", assignment.Generation)
	}
	if string(assignment.Data) != "assignment-data" {
		t.Errorf("expected data 'assignment-data', got %s", string(assignment.Data))
	}
	if assignment.AssignedAt != nowMs {
		t.Errorf("expected assigned_at %d, got %d", nowMs, assignment.AssignedAt)
	}
}

func TestStore_GetAssignment(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "get-assignment-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.SetAssignment(ctx, SetAssignmentRequest{
		GroupID:    "get-assignment-group",
		MemberID:   "member-1",
		Generation: 3,
		Data:       []byte("test-data"),
		NowMs:      time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to set assignment: %v", err)
	}

	assignment, err := store.GetAssignment(ctx, "get-assignment-group", "member-1")
	if err != nil {
		t.Fatalf("failed to get assignment: %v", err)
	}

	if assignment == nil {
		t.Fatal("expected assignment, got nil")
	}
	if assignment.Generation != 3 {
		t.Errorf("expected generation 3, got %d", assignment.Generation)
	}
	if string(assignment.Data) != "test-data" {
		t.Errorf("expected data 'test-data', got %s", string(assignment.Data))
	}
}

func TestStore_GetAssignmentNotSet(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "no-assignment-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	assignment, err := store.GetAssignment(ctx, "no-assignment-group", "member-1")
	if err != nil {
		t.Fatalf("expected no error for missing assignment, got %v", err)
	}
	if assignment != nil {
		t.Error("expected nil assignment for unset member")
	}
}

func TestStore_ListAssignments(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "list-assignments-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Set multiple assignments
	for i := 1; i <= 3; i++ {
		_, err = store.SetAssignment(ctx, SetAssignmentRequest{
			GroupID:    "list-assignments-group",
			MemberID:   "member-" + string(rune('0'+i)),
			Generation: 1,
			Data:       []byte("data"),
			NowMs:      time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to set assignment %d: %v", i, err)
		}
	}

	assignments, err := store.ListAssignments(ctx, "list-assignments-group")
	if err != nil {
		t.Fatalf("failed to list assignments: %v", err)
	}

	if len(assignments) != 3 {
		t.Errorf("expected 3 assignments, got %d", len(assignments))
	}
}

func TestStore_SetAllAssignments(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "all-assignments-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	nowMs := time.Now().UnixMilli()
	err = store.SetAllAssignments(ctx, SetAllAssignmentsRequest{
		GroupID:    "all-assignments-group",
		Generation: 5,
		Assignments: map[string][]byte{
			"member-1": []byte("assignment-1"),
			"member-2": []byte("assignment-2"),
			"member-3": []byte("assignment-3"),
		},
		NowMs: nowMs,
	})
	if err != nil {
		t.Fatalf("failed to set all assignments: %v", err)
	}

	// Verify all assignments were set
	assignments, err := store.ListAssignments(ctx, "all-assignments-group")
	if err != nil {
		t.Fatalf("failed to list assignments: %v", err)
	}

	if len(assignments) != 3 {
		t.Errorf("expected 3 assignments, got %d", len(assignments))
	}

	for _, a := range assignments {
		if a.Generation != 5 {
			t.Errorf("expected generation 5, got %d", a.Generation)
		}
	}
}

func TestStore_ClearAllAssignments(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "clear-assignments-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Set some assignments
	err = store.SetAllAssignments(ctx, SetAllAssignmentsRequest{
		GroupID:    "clear-assignments-group",
		Generation: 1,
		Assignments: map[string][]byte{
			"member-1": []byte("a1"),
			"member-2": []byte("a2"),
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to set assignments: %v", err)
	}

	// Clear all
	err = store.ClearAllAssignments(ctx, "clear-assignments-group")
	if err != nil {
		t.Fatalf("failed to clear assignments: %v", err)
	}

	// Verify all are gone
	assignments, err := store.ListAssignments(ctx, "clear-assignments-group")
	if err != nil {
		t.Fatalf("failed to list assignments: %v", err)
	}

	if len(assignments) != 0 {
		t.Errorf("expected 0 assignments after clear, got %d", len(assignments))
	}
}

func TestStore_TransitionState(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "transition-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Transition from Empty to PreparingRebalance
	state, err := store.TransitionState(ctx, TransitionStateRequest{
		GroupID:      "transition-group",
		FromState:    GroupStateEmpty,
		ToState:      GroupStatePreparingRebalance,
		IncrementGen: true,
		NowMs:        time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	if state.State != GroupStatePreparingRebalance {
		t.Errorf("expected state PreparingRebalance, got %s", state.State)
	}
	if state.Generation != 1 {
		t.Errorf("expected generation 1, got %d", state.Generation)
	}

	// Transition to CompletingRebalance with leader
	state, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID:   "transition-group",
		FromState: GroupStatePreparingRebalance,
		ToState:   GroupStateCompletingRebalance,
		Leader:    "leader-member",
		Protocol:  "range",
		NowMs:     time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	if state.State != GroupStateCompletingRebalance {
		t.Errorf("expected state CompletingRebalance, got %s", state.State)
	}
	if state.Leader != "leader-member" {
		t.Errorf("expected leader 'leader-member', got %s", state.Leader)
	}
	if state.Protocol != "range" {
		t.Errorf("expected protocol 'range', got %s", state.Protocol)
	}
}

func TestStore_TransitionStateInvalidFromState(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "invalid-transition-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Try to transition from wrong state
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID:   "invalid-transition-group",
		FromState: GroupStateStable, // Actually in Empty state
		ToState:   GroupStatePreparingRebalance,
		NowMs:     time.Now().UnixMilli(),
	})
	if err == nil {
		t.Error("expected error for invalid from state")
	}
}

func TestStore_TransitionStateWithClearAssignments(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "clear-on-transition-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Set some assignments
	err = store.SetAllAssignments(ctx, SetAllAssignmentsRequest{
		GroupID:    "clear-on-transition-group",
		Generation: 1,
		Assignments: map[string][]byte{
			"member-1": []byte("a1"),
			"member-2": []byte("a2"),
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to set assignments: %v", err)
	}

	// Transition with clear assignments
	_, err = store.TransitionState(ctx, TransitionStateRequest{
		GroupID:          "clear-on-transition-group",
		ToState:          GroupStatePreparingRebalance,
		ClearAssignments: true,
		NowMs:            time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to transition: %v", err)
	}

	// Verify assignments were cleared
	assignments, err := store.ListAssignments(ctx, "clear-on-transition-group")
	if err != nil {
		t.Fatalf("failed to list assignments: %v", err)
	}
	if len(assignments) != 0 {
		t.Errorf("expected 0 assignments after transition, got %d", len(assignments))
	}
}

func TestStore_ListGroups(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create multiple groups
	for _, name := range []string{"group-a", "group-b", "group-c"} {
		_, err := store.CreateGroup(ctx, CreateGroupRequest{
			GroupID: name,
			Type:    GroupTypeClassic,
			NowMs:   time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create group %s: %v", name, err)
		}
	}

	groups, err := store.ListGroups(ctx)
	if err != nil {
		t.Fatalf("failed to list groups: %v", err)
	}

	if len(groups) != 3 {
		t.Errorf("expected 3 groups, got %d", len(groups))
	}

	seen := make(map[string]bool)
	for _, g := range groups {
		seen[g.GroupID] = true
	}
	for _, expected := range []string{"group-a", "group-b", "group-c"} {
		if !seen[expected] {
			t.Errorf("missing group %s", expected)
		}
	}
}

func TestStore_InvalidInputs(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Empty group ID for create
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if !errors.Is(err, ErrInvalidGroupID) {
		t.Errorf("expected ErrInvalidGroupID for empty group ID, got %v", err)
	}

	// Empty group ID for get type
	_, err = store.GetGroupType(ctx, "")
	if !errors.Is(err, ErrInvalidGroupID) {
		t.Errorf("expected ErrInvalidGroupID for empty group ID in GetGroupType, got %v", err)
	}

	// Empty group ID for get state
	_, err = store.GetGroupState(ctx, "")
	if !errors.Is(err, ErrInvalidGroupID) {
		t.Errorf("expected ErrInvalidGroupID for empty group ID in GetGroupState, got %v", err)
	}

	// Empty member ID for add member
	_, err = store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "test-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:  "test-group",
		MemberID: "",
		NowMs:    time.Now().UnixMilli(),
	})
	if !errors.Is(err, ErrInvalidMemberID) {
		t.Errorf("expected ErrInvalidMemberID for empty member ID, got %v", err)
	}

	// Empty group ID for add member
	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:  "",
		MemberID: "member-1",
		NowMs:    time.Now().UnixMilli(),
	})
	if !errors.Is(err, ErrInvalidGroupID) {
		t.Errorf("expected ErrInvalidGroupID for empty group ID in AddMember, got %v", err)
	}
}

func TestStore_DefaultGroupType(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create group without specifying type
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "default-type-group",
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	groupType, err := store.GetGroupType(ctx, "default-type-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != GroupTypeClassic {
		t.Errorf("expected default type 'classic', got %s", groupType)
	}
}

func TestStore_GetGroupStateWithVersion(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "version-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	state, version, err := store.GetGroupStateWithVersion(ctx, "version-group")
	if err != nil {
		t.Fatalf("failed to get group state with version: %v", err)
	}

	if state.GroupID != "version-group" {
		t.Errorf("expected group id 'version-group', got %s", state.GroupID)
	}
	if version <= 0 {
		t.Errorf("expected positive version, got %d", version)
	}
}

func TestStore_UpdateMember(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "update-member-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:  "update-member-group",
		MemberID: "member-1",
		ClientID: "client-1",
		Metadata: []byte("old-metadata"),
		NowMs:    time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	newNow := time.Now().UnixMilli() + 10000
	updated, err := store.UpdateMember(ctx, UpdateMemberRequest{
		GroupID:         "update-member-group",
		MemberID:        "member-1",
		LastHeartbeatMs: newNow,
		Metadata:        []byte("new-metadata"),
	})
	if err != nil {
		t.Fatalf("failed to update member: %v", err)
	}

	if updated.LastHeartbeatMs != newNow {
		t.Errorf("expected last heartbeat %d, got %d", newNow, updated.LastHeartbeatMs)
	}
	if string(updated.Metadata) != "new-metadata" {
		t.Errorf("expected metadata 'new-metadata', got %s", string(updated.Metadata))
	}
}

func TestStore_DeleteAssignment(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "delete-assignment-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.SetAssignment(ctx, SetAssignmentRequest{
		GroupID:    "delete-assignment-group",
		MemberID:   "member-1",
		Generation: 1,
		Data:       []byte("data"),
		NowMs:      time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to set assignment: %v", err)
	}

	err = store.DeleteAssignment(ctx, "delete-assignment-group", "member-1")
	if err != nil {
		t.Fatalf("failed to delete assignment: %v", err)
	}

	assignment, err := store.GetAssignment(ctx, "delete-assignment-group", "member-1")
	if err != nil {
		t.Fatalf("failed to get assignment: %v", err)
	}
	if assignment != nil {
		t.Error("assignment should be nil after deletion")
	}
}

// TestStore_ConvertGroupType tests converting a group from one protocol type to another.
func TestStore_ConvertGroupType(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create a classic group
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "convert-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Verify initial type
	groupType, err := store.GetGroupType(ctx, "convert-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != GroupTypeClassic {
		t.Errorf("expected classic type, got %s", groupType)
	}

	// Convert to consumer type
	err = store.ConvertGroupType(ctx, "convert-group", GroupTypeConsumer)
	if err != nil {
		t.Fatalf("failed to convert group type: %v", err)
	}

	// Verify new type
	groupType, err = store.GetGroupType(ctx, "convert-group")
	if err != nil {
		t.Fatalf("failed to get group type after conversion: %v", err)
	}
	if groupType != GroupTypeConsumer {
		t.Errorf("expected consumer type after conversion, got %s", groupType)
	}

	// Convert back to classic
	err = store.ConvertGroupType(ctx, "convert-group", GroupTypeClassic)
	if err != nil {
		t.Fatalf("failed to convert group type back: %v", err)
	}

	groupType, err = store.GetGroupType(ctx, "convert-group")
	if err != nil {
		t.Fatalf("failed to get group type after second conversion: %v", err)
	}
	if groupType != GroupTypeClassic {
		t.Errorf("expected classic type after second conversion, got %s", groupType)
	}
}

// TestStore_ConvertGroupTypeNotEmpty tests that conversion fails when group has members.
func TestStore_ConvertGroupTypeNotEmpty(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create a classic group with a member
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "non-empty-convert-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:  "non-empty-convert-group",
		MemberID: "member-1",
		ClientID: "client-1",
		NowMs:    time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Try to convert - should fail because group has members
	err = store.ConvertGroupType(ctx, "non-empty-convert-group", GroupTypeConsumer)
	if !errors.Is(err, ErrGroupNotEmpty) {
		t.Errorf("expected ErrGroupNotEmpty, got %v", err)
	}

	// Verify type is unchanged
	groupType, err := store.GetGroupType(ctx, "non-empty-convert-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != GroupTypeClassic {
		t.Errorf("expected classic type (unchanged), got %s", groupType)
	}
}

// TestStore_ConvertGroupTypeNotFound tests that conversion fails for nonexistent groups.
func TestStore_ConvertGroupTypeNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	err := store.ConvertGroupType(ctx, "nonexistent-group", GroupTypeConsumer)
	if !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}

// TestStore_ConvertGroupTypeInvalidID tests that conversion fails for empty group ID.
func TestStore_ConvertGroupTypeInvalidID(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	err := store.ConvertGroupType(ctx, "", GroupTypeConsumer)
	if !errors.Is(err, ErrInvalidGroupID) {
		t.Errorf("expected ErrInvalidGroupID, got %v", err)
	}
}

// TestStore_ConvertGroupTypeAfterMemberRemoval tests conversion after all members leave.
func TestStore_ConvertGroupTypeAfterMemberRemoval(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	// Create a classic group with a member
	_, err := store.CreateGroup(ctx, CreateGroupRequest{
		GroupID: "removable-member-group",
		Type:    GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, AddMemberRequest{
		GroupID:  "removable-member-group",
		MemberID: "member-1",
		NowMs:    time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Conversion should fail
	err = store.ConvertGroupType(ctx, "removable-member-group", GroupTypeConsumer)
	if !errors.Is(err, ErrGroupNotEmpty) {
		t.Errorf("expected ErrGroupNotEmpty, got %v", err)
	}

	// Remove the member
	err = store.RemoveMember(ctx, "removable-member-group", "member-1")
	if err != nil {
		t.Fatalf("failed to remove member: %v", err)
	}

	// Now conversion should succeed
	err = store.ConvertGroupType(ctx, "removable-member-group", GroupTypeConsumer)
	if err != nil {
		t.Fatalf("expected conversion to succeed after member removal, got %v", err)
	}

	// Verify new type
	groupType, err := store.GetGroupType(ctx, "removable-member-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != GroupTypeConsumer {
		t.Errorf("expected consumer type, got %s", groupType)
	}
}

// =============================================================================
// Committed Offset Tests
// =============================================================================

func TestStore_CommitOffset(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	offset, err := store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "test-group",
		Topic:           "test-topic",
		Partition:       0,
		Offset:          100,
		LeaderEpoch:     5,
		Metadata:        "test-metadata",
		RetentionTimeMs: -1,
		NowMs:           nowMs,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	if offset.GroupID != "test-group" {
		t.Errorf("expected group 'test-group', got %s", offset.GroupID)
	}
	if offset.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %s", offset.Topic)
	}
	if offset.Partition != 0 {
		t.Errorf("expected partition 0, got %d", offset.Partition)
	}
	if offset.Offset != 100 {
		t.Errorf("expected offset 100, got %d", offset.Offset)
	}
	if offset.LeaderEpoch != 5 {
		t.Errorf("expected leader epoch 5, got %d", offset.LeaderEpoch)
	}
	if offset.Metadata != "test-metadata" {
		t.Errorf("expected metadata 'test-metadata', got %s", offset.Metadata)
	}
	if offset.CommitTimestamp != nowMs {
		t.Errorf("expected commit timestamp %d, got %d", nowMs, offset.CommitTimestamp)
	}
	if offset.ExpireTimestamp != -1 {
		t.Errorf("expected no expiry (-1), got %d", offset.ExpireTimestamp)
	}
}

func TestStore_CommitOffsetWithRetention(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	offset, err := store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:         "retention-group",
		Topic:           "test-topic",
		Partition:       0,
		Offset:          50,
		LeaderEpoch:     -1,
		RetentionTimeMs: 3600000, // 1 hour
		NowMs:           nowMs,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	expectedExpiry := nowMs + 3600000
	if offset.ExpireTimestamp != expectedExpiry {
		t.Errorf("expected expiry %d, got %d", expectedExpiry, offset.ExpireTimestamp)
	}
}

func TestStore_GetCommittedOffset(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	// Commit an offset
	_, err := store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:   "get-offset-group",
		Topic:     "test-topic",
		Partition: 2,
		Offset:    200,
		NowMs:     nowMs,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Retrieve it
	offset, err := store.GetCommittedOffset(ctx, "get-offset-group", "test-topic", 2)
	if err != nil {
		t.Fatalf("failed to get committed offset: %v", err)
	}

	if offset == nil {
		t.Fatal("expected offset, got nil")
	}
	if offset.Offset != 200 {
		t.Errorf("expected offset 200, got %d", offset.Offset)
	}
}

func TestStore_GetCommittedOffsetNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	offset, err := store.GetCommittedOffset(ctx, "nonexistent-group", "nonexistent-topic", 0)
	if err != nil {
		t.Fatalf("expected no error for missing offset, got %v", err)
	}
	if offset != nil {
		t.Error("expected nil for missing offset")
	}
}

func TestStore_ListCommittedOffsets(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	// Commit multiple offsets
	for _, tp := range []struct{ topic string; partition int32 }{
		{"topic-a", 0},
		{"topic-a", 1},
		{"topic-b", 0},
	} {
		_, err := store.CommitOffset(ctx, CommitOffsetRequest{
			GroupID:   "list-offsets-group",
			Topic:     tp.topic,
			Partition: tp.partition,
			Offset:    100,
			NowMs:     nowMs,
		})
		if err != nil {
			t.Fatalf("failed to commit offset: %v", err)
		}
	}

	offsets, err := store.ListCommittedOffsets(ctx, "list-offsets-group")
	if err != nil {
		t.Fatalf("failed to list offsets: %v", err)
	}

	if len(offsets) != 3 {
		t.Errorf("expected 3 offsets, got %d", len(offsets))
	}
}

func TestStore_ListCommittedOffsetsForTopic(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	// Commit offsets for different topics
	for _, tp := range []struct{ topic string; partition int32 }{
		{"topic-a", 0},
		{"topic-a", 1},
		{"topic-a", 2},
		{"topic-b", 0},
	} {
		_, err := store.CommitOffset(ctx, CommitOffsetRequest{
			GroupID:   "topic-offsets-group",
			Topic:     tp.topic,
			Partition: tp.partition,
			Offset:    100,
			NowMs:     nowMs,
		})
		if err != nil {
			t.Fatalf("failed to commit offset: %v", err)
		}
	}

	// List only topic-a offsets
	offsets, err := store.ListCommittedOffsetsForTopic(ctx, "topic-offsets-group", "topic-a")
	if err != nil {
		t.Fatalf("failed to list topic offsets: %v", err)
	}

	if len(offsets) != 3 {
		t.Errorf("expected 3 offsets for topic-a, got %d", len(offsets))
	}

	for _, o := range offsets {
		if o.Topic != "topic-a" {
			t.Errorf("expected topic 'topic-a', got %s", o.Topic)
		}
	}
}

func TestStore_DeleteCommittedOffset(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	// Commit an offset
	_, err := store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:   "delete-offset-group",
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
		NowMs:     nowMs,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Delete it
	err = store.DeleteCommittedOffset(ctx, "delete-offset-group", "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to delete offset: %v", err)
	}

	// Verify it's gone
	offset, err := store.GetCommittedOffset(ctx, "delete-offset-group", "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset != nil {
		t.Error("expected nil after deletion")
	}
}

func TestStore_DeleteAllCommittedOffsets(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	// Commit multiple offsets
	for i := 0; i < 5; i++ {
		_, err := store.CommitOffset(ctx, CommitOffsetRequest{
			GroupID:   "delete-all-group",
			Topic:     "test-topic",
			Partition: int32(i),
			Offset:    int64(i * 100),
			NowMs:     nowMs,
		})
		if err != nil {
			t.Fatalf("failed to commit offset: %v", err)
		}
	}

	// Delete all
	err := store.DeleteAllCommittedOffsets(ctx, "delete-all-group")
	if err != nil {
		t.Fatalf("failed to delete all offsets: %v", err)
	}

	// Verify they're all gone
	offsets, err := store.ListCommittedOffsets(ctx, "delete-all-group")
	if err != nil {
		t.Fatalf("failed to list offsets: %v", err)
	}
	if len(offsets) != 0 {
		t.Errorf("expected 0 offsets after delete all, got %d", len(offsets))
	}
}

func TestStore_CommitOffsetInvalidGroupID(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())

	_, err := store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:   "",
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
		NowMs:     time.Now().UnixMilli(),
	})
	if !errors.Is(err, ErrInvalidGroupID) {
		t.Errorf("expected ErrInvalidGroupID, got %v", err)
	}
}

func TestStore_CommitOffsetOverwrite(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	// First commit
	_, err := store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:   "overwrite-group",
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
		Metadata:  "first",
		NowMs:     nowMs,
	})
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	// Second commit (overwrite)
	_, err = store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:   "overwrite-group",
		Topic:     "test-topic",
		Partition: 0,
		Offset:    200,
		Metadata:  "second",
		NowMs:     nowMs + 1000,
	})
	if err != nil {
		t.Fatalf("second commit failed: %v", err)
	}

	// Verify the second commit is the one stored
	offset, err := store.GetCommittedOffset(ctx, "overwrite-group", "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to get offset: %v", err)
	}
	if offset.Offset != 200 {
		t.Errorf("expected offset 200, got %d", offset.Offset)
	}
	if offset.Metadata != "second" {
		t.Errorf("expected metadata 'second', got %s", offset.Metadata)
	}
}

func TestStore_CommitOffsetsAtomic(t *testing.T) {
	ctx := context.Background()
	store := NewStore(metadata.NewMockStore())
	nowMs := time.Now().UnixMilli()

	offsets := []CommitOffsetRequest{
		{Topic: "topic-1", Partition: 0, Offset: 100, LeaderEpoch: 1},
		{Topic: "topic-1", Partition: 1, Offset: 200, LeaderEpoch: 2},
		{Topic: "topic-2", Partition: 0, Offset: 300, LeaderEpoch: 3},
	}

	committed, err := store.CommitOffsets(ctx, "atomic-group", offsets, nowMs)
	if err != nil {
		t.Fatalf("failed to commit offsets: %v", err)
	}

	if len(committed) != 3 {
		t.Fatalf("expected 3 committed offsets, got %d", len(committed))
	}

	// Verify all offsets were stored
	for i, req := range offsets {
		offset, err := store.GetCommittedOffset(ctx, "atomic-group", req.Topic, req.Partition)
		if err != nil {
			t.Fatalf("failed to get offset %d: %v", i, err)
		}
		if offset == nil {
			t.Fatalf("offset %d not found", i)
		}
		if offset.Offset != req.Offset {
			t.Errorf("offset %d: expected %d, got %d", i, req.Offset, offset.Offset)
		}
		if offset.LeaderEpoch != req.LeaderEpoch {
			t.Errorf("offset %d: expected leader epoch %d, got %d", i, req.LeaderEpoch, offset.LeaderEpoch)
		}
	}
}

func TestStore_OffsetKeyStoragePath(t *testing.T) {
	// This test verifies the offset is stored at /dray/v1/groups/<groupId>/offsets/<topic>/<partition>
	ctx := context.Background()
	metaStore := metadata.NewMockStore()
	store := NewStore(metaStore)
	nowMs := time.Now().UnixMilli()

	_, err := store.CommitOffset(ctx, CommitOffsetRequest{
		GroupID:   "my-group",
		Topic:     "my-topic",
		Partition: 3,
		Offset:    12345,
		NowMs:     nowMs,
	})
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	// Verify the key path matches the task requirement
	expectedKey := "/dray/v1/groups/my-group/offsets/my-topic/3"
	result, err := metaStore.Get(ctx, expectedKey)
	if err != nil {
		t.Fatalf("failed to get from metadata store: %v", err)
	}
	if !result.Exists {
		t.Errorf("expected key %s to exist", expectedKey)
	}
}
