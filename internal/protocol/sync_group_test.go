package protocol

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newTestSyncGroupHandler() (*SyncGroupHandler, *groups.Store) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	handler := NewSyncGroupHandler(groupStore, nil)
	return handler, groupStore
}

func setupTestGroup(t *testing.T, store *groups.Store, groupID string, members []string, leader string, generation int32) {
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	// Create group
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:          groupID,
		Type:             groups.GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add members
	for _, memberID := range members {
		_, err := store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          groupID,
			MemberID:         memberID,
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member %s: %v", memberID, err)
		}
	}

	// Set group to CompletingRebalance state with the specified generation and leader
	_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
		GroupID:      groupID,
		ToState:      groups.GroupStateCompletingRebalance,
		IncrementGen: true,
		Leader:       leader,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// If generation > 1, increment more
	for i := int32(1); i < generation; i++ {
		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      groupID,
			ToState:      groups.GroupStateCompletingRebalance,
			IncrementGen: true,
			Leader:       leader,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to increment generation: %v", err)
		}
	}
}

func TestSyncGroupHandler_InvalidGroupID(t *testing.T) {
	handler, _ := newTestSyncGroupHandler()

	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = "" // Empty group ID
	req.MemberID = "member-1"
	req.Generation = 1

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errInvalidGroupID {
		t.Errorf("expected error code %d (INVALID_GROUP_ID), got %d", errInvalidGroupID, resp.ErrorCode)
	}
}

func TestSyncGroupHandler_InvalidMemberID(t *testing.T) {
	handler, _ := newTestSyncGroupHandler()

	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = "test-group"
	req.MemberID = "" // Empty member ID
	req.Generation = 1

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupUnknownMemberID {
		t.Errorf("expected error code %d (UNKNOWN_MEMBER_ID), got %d", errSyncGroupUnknownMemberID, resp.ErrorCode)
	}
}

func TestSyncGroupHandler_GroupNotFound(t *testing.T) {
	handler, _ := newTestSyncGroupHandler()

	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = "nonexistent-group"
	req.MemberID = "member-1"
	req.Generation = 1

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupCoordinatorNotAvail {
		t.Errorf("expected error code %d (COORDINATOR_NOT_AVAILABLE), got %d", errSyncGroupCoordinatorNotAvail, resp.ErrorCode)
	}
}

func TestSyncGroupHandler_InvalidGeneration(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "gen-test-group"

	setupTestGroup(t, store, groupID, []string{"member-1"}, "member-1", 1)

	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = groupID
	req.MemberID = "member-1"
	req.Generation = 999 // Wrong generation

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupIllegalGeneration {
		t.Errorf("expected error code %d (ILLEGAL_GENERATION), got %d", errSyncGroupIllegalGeneration, resp.ErrorCode)
	}
}

func TestSyncGroupHandler_UnknownMember(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "unknown-member-group"

	setupTestGroup(t, store, groupID, []string{"member-1"}, "member-1", 1)

	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = groupID
	req.MemberID = "unknown-member" // Not a member of the group
	req.Generation = 1

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupUnknownMemberID {
		t.Errorf("expected error code %d (UNKNOWN_MEMBER_ID), got %d", errSyncGroupUnknownMemberID, resp.ErrorCode)
	}
}

func TestSyncGroupHandler_LeaderSendsAssignments(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "leader-sync-group"
	leader := "member-1"

	setupTestGroup(t, store, groupID, []string{leader}, leader, 1)

	// Leader sends assignments
	assignment := []byte("partition-assignment-data")
	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = groupID
	req.MemberID = leader
	req.Generation = 1
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leader, MemberAssignment: assignment},
	}

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupNone {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if string(resp.MemberAssignment) != string(assignment) {
		t.Errorf("expected assignment %q, got %q", assignment, resp.MemberAssignment)
	}

	// Verify assignment was stored
	ctx := context.Background()
	storedAssignment, err := store.GetAssignment(ctx, groupID, leader)
	if err != nil {
		t.Fatalf("failed to get stored assignment: %v", err)
	}
	if storedAssignment == nil {
		t.Fatal("expected stored assignment, got nil")
	}
	if string(storedAssignment.Data) != string(assignment) {
		t.Errorf("stored assignment mismatch: expected %q, got %q", assignment, storedAssignment.Data)
	}
}

func TestSyncGroupHandler_FollowerReceivesAssignment(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "follower-sync-group"
	leader := "leader-1"
	follower := "follower-1"

	setupTestGroup(t, store, groupID, []string{leader, follower}, leader, 1)

	var wg sync.WaitGroup
	var leaderResp, followerResp *kmsg.SyncGroupResponse

	// Start follower first - it should wait
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := kmsg.NewPtrSyncGroupRequest()
		req.SetVersion(5)
		req.Group = groupID
		req.MemberID = follower
		req.Generation = 1
		req.GroupAssignment = nil // Follower sends no assignments

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		followerResp = handler.Handle(ctx, 5, req)
	}()

	// Give follower time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Leader sends assignments
	wg.Add(1)
	go func() {
		defer wg.Done()
		leaderAssignment := []byte("leader-partition-data")
		followerAssignment := []byte("follower-partition-data")

		req := kmsg.NewPtrSyncGroupRequest()
		req.SetVersion(5)
		req.Group = groupID
		req.MemberID = leader
		req.Generation = 1
		req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
			{MemberID: leader, MemberAssignment: leaderAssignment},
			{MemberID: follower, MemberAssignment: followerAssignment},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		leaderResp = handler.Handle(ctx, 5, req)
	}()

	wg.Wait()

	// Verify leader response
	if leaderResp.ErrorCode != errSyncGroupNone {
		t.Errorf("leader: expected no error, got %d", leaderResp.ErrorCode)
	}
	if string(leaderResp.MemberAssignment) != "leader-partition-data" {
		t.Errorf("leader: expected assignment 'leader-partition-data', got %q", leaderResp.MemberAssignment)
	}

	// Verify follower response
	if followerResp.ErrorCode != errSyncGroupNone {
		t.Errorf("follower: expected no error, got %d", followerResp.ErrorCode)
	}
	if string(followerResp.MemberAssignment) != "follower-partition-data" {
		t.Errorf("follower: expected assignment 'follower-partition-data', got %q", followerResp.MemberAssignment)
	}
}

func TestSyncGroupHandler_MultipleMembers(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "multi-member-sync-group"
	leader := "member-0"
	members := []string{"member-0", "member-1", "member-2"}

	setupTestGroup(t, store, groupID, members, leader, 1)

	var wg sync.WaitGroup
	responses := make(map[string]*kmsg.SyncGroupResponse)
	var mu sync.Mutex

	// Start all members
	for i, memberID := range members {
		wg.Add(1)
		go func(m string, idx int) {
			defer wg.Done()

			// Delay non-leaders slightly to ensure leader goes last
			if m != leader {
				time.Sleep(time.Duration(10*(idx+1)) * time.Millisecond)
			} else {
				time.Sleep(100 * time.Millisecond) // Leader sends last
			}

			req := kmsg.NewPtrSyncGroupRequest()
			req.SetVersion(5)
			req.Group = groupID
			req.MemberID = m
			req.Generation = 1

			if m == leader {
				// Leader sends assignments for all members
				req.GroupAssignment = make([]kmsg.SyncGroupRequestGroupAssignment, len(members))
				for j, member := range members {
					req.GroupAssignment[j] = kmsg.SyncGroupRequestGroupAssignment{
						MemberID:         member,
						MemberAssignment: []byte("assignment-for-" + member),
					}
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp := handler.Handle(ctx, 5, req)

			mu.Lock()
			responses[m] = resp
			mu.Unlock()
		}(memberID, i)
	}

	wg.Wait()

	// Verify all members received correct assignments
	for _, memberID := range members {
		resp, ok := responses[memberID]
		if !ok {
			t.Errorf("no response for member %s", memberID)
			continue
		}
		if resp.ErrorCode != errSyncGroupNone {
			t.Errorf("member %s: expected no error, got %d", memberID, resp.ErrorCode)
			continue
		}
		expectedAssignment := "assignment-for-" + memberID
		if string(resp.MemberAssignment) != expectedAssignment {
			t.Errorf("member %s: expected assignment %q, got %q", memberID, expectedAssignment, resp.MemberAssignment)
		}
	}
}

func TestSyncGroupHandler_AssignmentStoredInMetadata(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "store-assignment-group"
	leader := "member-1"
	members := []string{leader, "member-2"}

	setupTestGroup(t, store, groupID, members, leader, 1)

	var wg sync.WaitGroup

	// Start follower first - it will wait for leader's assignments
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := kmsg.NewPtrSyncGroupRequest()
		req.SetVersion(5)
		req.Group = groupID
		req.MemberID = "member-2"
		req.Generation = 1
		req.GroupAssignment = nil

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		handler.Handle(ctx, 5, req)
	}()

	// Give follower time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Leader sends assignments
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := kmsg.NewPtrSyncGroupRequest()
		req.SetVersion(5)
		req.Group = groupID
		req.MemberID = leader
		req.Generation = 1
		req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
			{MemberID: leader, MemberAssignment: []byte("leader-data")},
			{MemberID: "member-2", MemberAssignment: []byte("member2-data")},
		}

		resp := handler.Handle(context.Background(), 5, req)

		if resp.ErrorCode != errSyncGroupNone {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}
	}()

	wg.Wait()

	// Verify assignments were stored for both members
	ctx := context.Background()
	for _, memberID := range members {
		assignment, err := store.GetAssignment(ctx, groupID, memberID)
		if err != nil {
			t.Errorf("failed to get assignment for %s: %v", memberID, err)
			continue
		}
		if assignment == nil {
			t.Errorf("no assignment stored for %s", memberID)
			continue
		}
		if assignment.Generation != 1 {
			t.Errorf("member %s: expected generation 1, got %d", memberID, assignment.Generation)
		}
	}
}

func TestSyncGroupHandler_ContextCancellation(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "cancel-sync-group"
	leader := "member-1"

	setupTestGroup(t, store, groupID, []string{leader, "member-2"}, leader, 1)

	// Follower tries to sync but context gets cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = groupID
	req.MemberID = "member-2"
	req.Generation = 1

	resp := handler.Handle(ctx, 5, req)

	// Should return REBALANCE_IN_PROGRESS due to timeout
	if resp.ErrorCode != errSyncGroupRebalanceInProgress {
		t.Errorf("expected REBALANCE_IN_PROGRESS error on context cancel, got %d", resp.ErrorCode)
	}
}

func TestSyncGroupHandler_EmptyAssignment(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "empty-assignment-group"
	leader := "member-1"

	setupTestGroup(t, store, groupID, []string{leader}, leader, 1)

	// Leader sends empty assignment
	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = groupID
	req.MemberID = leader
	req.Generation = 1
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leader, MemberAssignment: []byte{}}, // Empty assignment
	}

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupNone {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if len(resp.MemberAssignment) != 0 {
		t.Errorf("expected empty assignment, got %q", resp.MemberAssignment)
	}
}

func TestSyncGroupHandler_GroupTransitionsToStable(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "stable-transition-group"
	leader := "member-1"

	setupTestGroup(t, store, groupID, []string{leader}, leader, 1)

	// Verify group is in CompletingRebalance
	ctx := context.Background()
	state, err := store.GetGroupState(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}
	if state.State != groups.GroupStateCompletingRebalance {
		t.Fatalf("expected group in CompletingRebalance, got %s", state.State)
	}

	// Leader sends assignments
	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = groupID
	req.MemberID = leader
	req.Generation = 1
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leader, MemberAssignment: []byte("data")},
	}

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupNone {
		t.Fatalf("expected no error, got %d", resp.ErrorCode)
	}

	// Verify group transitioned to Stable
	state, err = store.GetGroupState(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}
	if state.State != groups.GroupStateStable {
		t.Errorf("expected group in Stable, got %s", state.State)
	}
}

func TestSyncGroupHandler_VersionCompatibility(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "version-compat-group"
	leader := "member-1"

	// Test with different versions
	versions := []int16{0, 1, 2, 3, 4, 5, 6, 7}

	for _, version := range versions {
		// Create fresh group for each version test
		testGroupID := groupID + string(rune('0'+version))
		setupTestGroup(t, store, testGroupID, []string{leader}, leader, 1)

		req := kmsg.NewPtrSyncGroupRequest()
		req.SetVersion(version)
		req.Group = testGroupID
		req.MemberID = leader
		req.Generation = 1
		req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
			{MemberID: leader, MemberAssignment: []byte("data")},
		}

		resp := handler.Handle(context.Background(), version, req)

		if resp.ErrorCode != errSyncGroupNone {
			t.Errorf("version %d: expected no error, got %d", version, resp.ErrorCode)
		}
	}
}

func TestSyncGroupHandler_ClearSyncState(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "clear-state-group"
	leader := "member-1"

	setupTestGroup(t, store, groupID, []string{leader, "member-2"}, leader, 1)

	// Start a sync from follower (it will wait)
	var wg sync.WaitGroup
	var resp *kmsg.SyncGroupResponse

	wg.Add(1)
	go func() {
		defer wg.Done()
		req := kmsg.NewPtrSyncGroupRequest()
		req.SetVersion(5)
		req.Group = groupID
		req.MemberID = "member-2"
		req.Generation = 1

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		resp = handler.Handle(ctx, 5, req)
	}()

	// Give follower time to start waiting
	time.Sleep(20 * time.Millisecond)

	// Clear sync state (simulating rebalance restart)
	handler.ClearSyncState(groupID)

	wg.Wait()

	// Follower should have received an error due to timeout after state cleared
	if resp.ErrorCode != errSyncGroupRebalanceInProgress {
		t.Logf("got error code %d (expected REBALANCE_IN_PROGRESS=%d)", resp.ErrorCode, errSyncGroupRebalanceInProgress)
	}
}

func TestSyncGroupHandler_InstanceIDValidation(t *testing.T) {
	handler, store := newTestSyncGroupHandler()
	groupID := "instance-id-group"
	leader := "member-1"
	instanceID := "instance-1"

	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	// Create group
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:          groupID,
		Type:             groups.GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add member with instance ID
	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:          groupID,
		MemberID:         leader,
		GroupInstanceID:  instanceID,
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Transition to CompletingRebalance
	_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
		GroupID:      groupID,
		ToState:      groups.GroupStateCompletingRebalance,
		IncrementGen: true,
		Leader:       leader,
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to transition state: %v", err)
	}

	// Try with wrong instance ID (v3+)
	wrongInstanceID := "wrong-instance"
	req := kmsg.NewPtrSyncGroupRequest()
	req.SetVersion(5)
	req.Group = groupID
	req.MemberID = leader
	req.Generation = 1
	req.InstanceID = &wrongInstanceID
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: leader, MemberAssignment: []byte("data")},
	}

	resp := handler.Handle(context.Background(), 5, req)

	if resp.ErrorCode != errSyncGroupFencedInstanceID {
		t.Errorf("expected FENCED_INSTANCE_ID error, got %d", resp.ErrorCode)
	}
}
