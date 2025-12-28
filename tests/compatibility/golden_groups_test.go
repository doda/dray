// Package compatibility provides golden tests for group coordination.
// Golden tests compare Dray responses against expected Kafka broker behavior,
// validating that state transitions and response fields match Kafka protocol expectations.
package compatibility

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestGolden_JoinGroup_NewMember validates JoinGroup for a new member.
func TestGolden_JoinGroup_NewMember(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-join-new-member"

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = groupID
	joinReq.MemberID = "" // New member
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	joinReq.RebalanceTimeoutMillis = 5000

	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	proto.Metadata = encodeSubscription([]string{"test-topic"})
	joinReq.Protocols = append(joinReq.Protocols, proto)

	joinResp, err := joinReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("JoinGroup request failed: %v", err)
	}

	// Golden check: MEMBER_ID_REQUIRED (error code 79) expected for v4+ with empty member ID
	if joinResp.ErrorCode == 79 {
		t.Log("[GOLDEN] MEMBER_ID_REQUIRED as expected for new member with empty MemberID")

		// Golden check: MemberID should be assigned
		if joinResp.MemberID == "" {
			t.Error("[GOLDEN] MemberID should be assigned even with MEMBER_ID_REQUIRED")
		}

		// Retry with assigned member ID
		joinReq.MemberID = joinResp.MemberID
		joinResp, err = joinReq.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("JoinGroup retry failed: %v", err)
		}
	}

	// Golden check: Successful join should have error code 0
	if joinResp.ErrorCode != 0 {
		t.Errorf("[GOLDEN] expected error code 0, got %d", joinResp.ErrorCode)
	}

	// Golden check: MemberID should not be empty
	if joinResp.MemberID == "" {
		t.Error("[GOLDEN] MemberID should not be empty after successful join")
	}

	// Golden check: Generation should be positive for first generation
	if joinResp.Generation < 1 {
		t.Errorf("[GOLDEN] expected generation >= 1, got %d", joinResp.Generation)
	}

	// Golden check: LeaderID should be this member (only member in group)
	if joinResp.LeaderID != joinResp.MemberID {
		t.Errorf("[GOLDEN] single member should be leader: leaderID=%s, memberID=%s",
			joinResp.LeaderID, joinResp.MemberID)
	}

	// Golden check: Protocol should match requested
	if joinResp.Protocol == nil || *joinResp.Protocol != "range" {
		protocol := ""
		if joinResp.Protocol != nil {
			protocol = *joinResp.Protocol
		}
		t.Errorf("[GOLDEN] expected protocol 'range', got %q", protocol)
	}

	// Golden check: Members list should contain this member (as leader)
	if len(joinResp.Members) != 1 {
		t.Errorf("[GOLDEN] expected 1 member, got %d", len(joinResp.Members))
	} else if joinResp.Members[0].MemberID != joinResp.MemberID {
		t.Errorf("[GOLDEN] member list should contain this member")
	}

	t.Logf("[GOLDEN] JoinGroup new member: memberID=%s, generation=%d, leader=%s",
		joinResp.MemberID, joinResp.Generation, joinResp.LeaderID)
}

// TestGolden_JoinGroup_TwoMembers validates JoinGroup rebalance with 2 members.
func TestGolden_JoinGroup_TwoMembers(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-join-two-members"

	client1, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client1: %v", err)
	}
	defer client1.Close()

	client2, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client2: %v", err)
	}
	defer client2.Close()

	// First member joins
	member1ID := joinWithRetry(t, ctx, client1, groupID, "range", []string{"test-topic"})
	t.Logf("[GOLDEN] First member joined: %s", member1ID)

	// Second member joins - triggers rebalance
	member2ID := joinWithRetry(t, ctx, client2, groupID, "range", []string{"test-topic"})
	t.Logf("[GOLDEN] Second member joined: %s", member2ID)

	// Golden check: Members should have different IDs
	if member1ID == member2ID {
		t.Error("[GOLDEN] members should have different IDs")
	}

	// First member rejoins to get the rebalance result
	joinReq1 := kmsg.NewPtrJoinGroupRequest()
	joinReq1.Group = groupID
	joinReq1.MemberID = member1ID
	joinReq1.ProtocolType = "consumer"
	joinReq1.SessionTimeoutMillis = 10000
	joinReq1.RebalanceTimeoutMillis = 5000
	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	proto.Metadata = encodeSubscription([]string{"test-topic"})
	joinReq1.Protocols = append(joinReq1.Protocols, proto)

	joinResp1, err := joinReq1.RequestWith(ctx, client1)
	if err != nil {
		t.Fatalf("member1 rejoin failed: %v", err)
	}

	// Golden check: Both members should see same generation
	if joinResp1.ErrorCode != 0 {
		t.Errorf("[GOLDEN] member1 rejoin error: %d", joinResp1.ErrorCode)
	}

	// Golden check: Generation should be at least 2 after rebalance
	if joinResp1.Generation < 2 {
		t.Errorf("[GOLDEN] expected generation >= 2 after rebalance, got %d", joinResp1.Generation)
	}

	// Golden check: One member should be leader
	leaderID := joinResp1.LeaderID
	if leaderID != member1ID && leaderID != member2ID {
		t.Errorf("[GOLDEN] leader should be one of the members: leader=%s", leaderID)
	}

	// Golden check: If this is the leader, members list should be populated
	if leaderID == member1ID && len(joinResp1.Members) != 2 {
		t.Errorf("[GOLDEN] leader should see 2 members, got %d", len(joinResp1.Members))
	}

	// Golden check: If not leader, members list should be empty
	if leaderID != member1ID && len(joinResp1.Members) != 0 {
		t.Errorf("[GOLDEN] non-leader should see 0 members, got %d", len(joinResp1.Members))
	}

	t.Logf("[GOLDEN] Two-member group: generation=%d, leader=%s", joinResp1.Generation, leaderID)
}

// TestGolden_JoinGroup_ProtocolSelection validates protocol selection.
func TestGolden_JoinGroup_ProtocolSelection(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-join-protocol-selection"

	client1, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client1: %v", err)
	}
	defer client1.Close()

	client2, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client2: %v", err)
	}
	defer client2.Close()

	// First member supports range and roundrobin
	member1ID := joinWithMultipleProtocols(t, ctx, client1, groupID,
		[]string{"range", "roundrobin"}, []string{"test-topic"})

	// Second member supports only roundrobin
	member2ID := joinWithMultipleProtocols(t, ctx, client2, groupID,
		[]string{"roundrobin"}, []string{"test-topic"})

	// Both members rejoin to complete rebalance
	joinResp := rejoinGroup(t, ctx, client1, groupID, member1ID, []string{"range", "roundrobin"}, []string{"test-topic"})

	// Golden check: Protocol should be the one supported by all members (roundrobin)
	if joinResp.Protocol == nil || *joinResp.Protocol != "roundrobin" {
		protocol := ""
		if joinResp.Protocol != nil {
			protocol = *joinResp.Protocol
		}
		t.Errorf("[GOLDEN] expected protocol 'roundrobin' (common), got %q", protocol)
	}

	t.Logf("[GOLDEN] Protocol selection: member1=%s, member2=%s, selected=%v",
		member1ID, member2ID, joinResp.Protocol)
}

// TestGolden_SyncGroup_LeaderAssignment validates SyncGroup leader assignment distribution.
func TestGolden_SyncGroup_LeaderAssignment(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-sync-topic"
	groupID := "golden-sync-leader"

	if err := suite.Broker().CreateTopic(ctx, topicName, 2); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Join the group
	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{topicName})

	// Get group state with generation
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{topicName})

	// As leader, send assignment via SyncGroup
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = joinResp.Generation

	// Create assignment for this member
	assignment := encodeAssignment([]topicPartition{
		{Topic: topicName, Partition: 0},
		{Topic: topicName, Partition: 1},
	})

	memberAssign := kmsg.NewSyncGroupRequestGroupAssignment()
	memberAssign.MemberID = memberID
	memberAssign.MemberAssignment = assignment
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, memberAssign)

	syncResp, err := syncReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("SyncGroup request failed: %v", err)
	}

	// Golden check: No error
	if syncResp.ErrorCode != 0 {
		t.Errorf("[GOLDEN] expected error code 0, got %d", syncResp.ErrorCode)
	}

	// Golden check: Assignment should be returned
	if len(syncResp.MemberAssignment) == 0 {
		t.Error("[GOLDEN] expected non-empty member assignment")
	}

	// Parse and verify assignment
	partitions := decodeAssignment(syncResp.MemberAssignment)
	if len(partitions) != 2 {
		t.Errorf("[GOLDEN] expected 2 partitions in assignment, got %d", len(partitions))
	}

	t.Logf("[GOLDEN] SyncGroup leader assignment: member=%s, partitions=%d",
		memberID, len(partitions))
}

// TestGolden_SyncGroup_TwoMemberCoordination validates two members coordinating through SyncGroup.
// Note: Multi-member synchronization requires careful coordination of join/sync ordering.
// This test validates that when both members properly join and then sync, they receive assignments.
func TestGolden_SyncGroup_TwoMemberCoordination(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "golden-sync-two-topic"
	groupID := "golden-sync-two"

	if err := suite.Broker().CreateTopic(ctx, topicName, 2); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client1, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client1: %v", err)
	}
	defer client1.Close()

	client2, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client2: %v", err)
	}
	defer client2.Close()

	// Two members join; member2 triggers rebalance.
	member1ID := joinWithRetry(t, ctx, client1, groupID, "range", []string{topicName})
	member2ID, member2JoinResp := joinWithRetryResponse(t, ctx, client2, groupID, "range", []string{topicName})
	joinResp1 := rejoinGroup(t, ctx, client1, groupID, member1ID, []string{"range"}, []string{topicName})

	// Golden check: members should have different IDs
	if member1ID == member2ID {
		t.Error("[GOLDEN] members should have different IDs")
	}

	// Golden check: both members should share generation
	if joinResp1.Generation != member2JoinResp.Generation {
		t.Errorf("[GOLDEN] generation mismatch: leader=%d follower=%d",
			joinResp1.Generation, member2JoinResp.Generation)
	}

	leaderID := joinResp1.LeaderID
	if leaderID != member1ID && leaderID != member2ID {
		t.Errorf("[GOLDEN] leader should be one of the members: leader=%s", leaderID)
	}

	// Leader sends assignments for both members; follower sends empty assignments.
	assignMember1 := kmsg.NewSyncGroupRequestGroupAssignment()
	assignMember1.MemberID = member1ID
	assignMember1.MemberAssignment = encodeAssignment([]topicPartition{{Topic: topicName, Partition: 0}})

	assignMember2 := kmsg.NewSyncGroupRequestGroupAssignment()
	assignMember2.MemberID = member2ID
	assignMember2.MemberAssignment = encodeAssignment([]topicPartition{{Topic: topicName, Partition: 1}})

	leaderClient := client1
	followerClient := client2
	followerID := member2ID
	expectedLeaderPartition := int32(0)
	expectedFollowerPartition := int32(1)
	if leaderID == member2ID {
		leaderClient = client2
		followerClient = client1
		followerID = member1ID
		expectedLeaderPartition = 1
		expectedFollowerPartition = 0
	}

	leaderCh := make(chan *kmsg.SyncGroupResponse, 1)
	leaderErrCh := make(chan error, 1)
	go func() {
		resp, err := syncGroupRequest(ctx, leaderClient, groupID, leaderID, joinResp1.Generation,
			[]kmsg.SyncGroupRequestGroupAssignment{assignMember1, assignMember2})
		leaderCh <- resp
		leaderErrCh <- err
	}()

	followerResp, err := syncGroupRequest(ctx, followerClient, groupID, followerID, joinResp1.Generation, nil)
	if err != nil {
		t.Fatalf("SyncGroup follower failed: %v", err)
	}

	leaderResp := <-leaderCh
	if err := <-leaderErrCh; err != nil {
		t.Fatalf("SyncGroup leader failed: %v", err)
	}

	// Golden check: Sync succeeds for both members
	if leaderResp.ErrorCode != 0 {
		t.Errorf("[GOLDEN] leader sync error: %d", leaderResp.ErrorCode)
	}
	if followerResp.ErrorCode != 0 {
		t.Errorf("[GOLDEN] follower sync error: %d", followerResp.ErrorCode)
	}

	leaderParts := decodeAssignment(leaderResp.MemberAssignment)
	if !assignmentHasPartition(leaderParts, topicName, expectedLeaderPartition) {
		t.Errorf("[GOLDEN] leader expected partition %d, got %+v", expectedLeaderPartition, leaderParts)
	}

	followerParts := decodeAssignment(followerResp.MemberAssignment)
	if !assignmentHasPartition(followerParts, topicName, expectedFollowerPartition) {
		t.Errorf("[GOLDEN] follower expected partition %d, got %+v", expectedFollowerPartition, followerParts)
	}

	t.Logf("[GOLDEN] SyncGroup two-member coordination: leader=%s follower=%s", leaderID, followerID)
}

// TestGolden_Heartbeat_Success validates successful heartbeat.
func TestGolden_Heartbeat_Success(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-heartbeat-success"

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Join and sync
	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{"test-topic"})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{"test-topic"})

	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = joinResp.Generation

	assignment := kmsg.NewSyncGroupRequestGroupAssignment()
	assignment.MemberID = memberID
	assignment.MemberAssignment = encodeAssignment([]topicPartition{{Topic: "test-topic", Partition: 0}})
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignment)

	if _, err := syncReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}

	// Send heartbeat
	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = groupID
	hbReq.MemberID = memberID
	hbReq.Generation = joinResp.Generation

	hbResp, err := hbReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	// Golden check: Successful heartbeat returns error code 0
	if hbResp.ErrorCode != 0 {
		t.Errorf("[GOLDEN] expected heartbeat error code 0, got %d", hbResp.ErrorCode)
	}

	t.Logf("[GOLDEN] Heartbeat success: member=%s, generation=%d", memberID, joinResp.Generation)
}

// TestGolden_Heartbeat_WrongGeneration validates heartbeat with wrong generation.
func TestGolden_Heartbeat_WrongGeneration(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-heartbeat-wrong-gen"

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{"test-topic"})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{"test-topic"})

	// Sync first
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = joinResp.Generation
	assignment := kmsg.NewSyncGroupRequestGroupAssignment()
	assignment.MemberID = memberID
	assignment.MemberAssignment = encodeAssignment([]topicPartition{{Topic: "test-topic", Partition: 0}})
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignment)
	if _, err := syncReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}

	// Send heartbeat with wrong generation
	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = groupID
	hbReq.MemberID = memberID
	hbReq.Generation = joinResp.Generation - 1 // Wrong generation

	hbResp, err := hbReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("Heartbeat request failed: %v", err)
	}

	// Golden check: Wrong generation should return ILLEGAL_GENERATION (22)
	// or REBALANCE_IN_PROGRESS (27) depending on group state
	if hbResp.ErrorCode != 22 && hbResp.ErrorCode != 27 {
		t.Errorf("[GOLDEN] expected ILLEGAL_GENERATION (22) or REBALANCE_IN_PROGRESS (27), got %d",
			hbResp.ErrorCode)
	}

	t.Logf("[GOLDEN] Heartbeat wrong generation: error=%d", hbResp.ErrorCode)
}

// TestGolden_Heartbeat_UnknownMember validates heartbeat from unknown member.
func TestGolden_Heartbeat_UnknownMember(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-heartbeat-unknown"

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Create a group first
	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{"test-topic"})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{"test-topic"})

	// Send heartbeat from unknown member
	hbReq := kmsg.NewPtrHeartbeatRequest()
	hbReq.Group = groupID
	hbReq.MemberID = "unknown-member-id"
	hbReq.Generation = joinResp.Generation

	hbResp, err := hbReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("Heartbeat request failed: %v", err)
	}

	// Golden check: Unknown member should return UNKNOWN_MEMBER_ID (25)
	if hbResp.ErrorCode != 25 {
		t.Errorf("[GOLDEN] expected UNKNOWN_MEMBER_ID (25), got %d", hbResp.ErrorCode)
	}

	t.Logf("[GOLDEN] Heartbeat unknown member: error=%d", hbResp.ErrorCode)
}

// TestGolden_LeaveGroup_Success validates successful leave group.
func TestGolden_LeaveGroup_Success(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-leave-success"

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{"test-topic"})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{"test-topic"})

	// Sync to complete join
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = joinResp.Generation
	assignment := kmsg.NewSyncGroupRequestGroupAssignment()
	assignment.MemberID = memberID
	assignment.MemberAssignment = encodeAssignment([]topicPartition{{Topic: "test-topic", Partition: 0}})
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignment)
	if _, err := syncReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}

	// Leave group
	leaveReq := kmsg.NewPtrLeaveGroupRequest()
	leaveReq.Group = groupID
	leaveReq.MemberID = memberID

	leaveResp, err := leaveReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("LeaveGroup request failed: %v", err)
	}

	// Golden check: Successful leave returns error code 0
	if leaveResp.ErrorCode != 0 {
		t.Errorf("[GOLDEN] expected leave error code 0, got %d", leaveResp.ErrorCode)
	}

	t.Logf("[GOLDEN] LeaveGroup success: member=%s", memberID)
}

// TestGolden_LeaveGroup_AllowsRejoin validates that after leaving, a member can rejoin.
// Note: The rebalance signaling mechanism varies by Kafka implementation version.
func TestGolden_LeaveGroup_AllowsRejoin(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-leave-rejoin"

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Join the group
	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{"test-topic"})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{"test-topic"})
	generation1 := joinResp.Generation

	// Sync to reach stable
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = generation1
	assignment := kmsg.NewSyncGroupRequestGroupAssignment()
	assignment.MemberID = memberID
	assignment.MemberAssignment = encodeAssignment([]topicPartition{{Topic: "test-topic", Partition: 0}})
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignment)
	if _, err := syncReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}

	// Leave the group
	leaveReq := kmsg.NewPtrLeaveGroupRequest()
	leaveReq.Group = groupID
	leaveReq.MemberID = memberID

	leaveResp, err := leaveReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("LeaveGroup failed: %v", err)
	}

	// Golden check: Leave should succeed
	if leaveResp.ErrorCode != 0 {
		t.Errorf("[GOLDEN] leave error: %d", leaveResp.ErrorCode)
	}

	// Create a new client to rejoin as a "new" member
	client2, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client2: %v", err)
	}
	defer client2.Close()

	// Join as a new member
	newMemberID := joinWithRetry(t, ctx, client2, groupID, "range", []string{"test-topic"})
	joinResp2 := rejoinGroup(t, ctx, client2, groupID, newMemberID, []string{"range"}, []string{"test-topic"})

	// Golden check: New member should be able to join
	if joinResp2.MemberID == "" {
		t.Error("[GOLDEN] new member should be assigned an ID")
	}

	// Golden check: New member should be leader (only member)
	if joinResp2.LeaderID != newMemberID {
		t.Errorf("[GOLDEN] new member should be leader")
	}

	t.Logf("[GOLDEN] LeaveGroup allows rejoin: old member=%s, new member=%s, generation=%d",
		memberID, newMemberID, joinResp2.Generation)
}

// TestGolden_GroupState_JoinToStable validates group transitions from join to stable.
// Note: The exact intermediate states may vary by implementation.
func TestGolden_GroupState_JoinToStable(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-state-join-stable"
	topicName := "golden-state-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	descClient, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create desc client: %v", err)
	}
	defer descClient.Close()

	// Initial state: group doesn't exist or is Empty/Dead
	initialState := describeGroupState(t, ctx, descClient, groupID)
	t.Logf("[GOLDEN] Initial state: %s", initialState)

	// Golden check: Initial state should be Empty, Dead, or nonexistent
	if initialState != "Empty" && initialState != "Dead" && initialState != "" {
		t.Errorf("[GOLDEN] expected initial state Empty/Dead/'', got %s", initialState)
	}

	// Member joins - should create the group
	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{topicName})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{topicName})

	stateAfterJoin := describeGroupState(t, ctx, descClient, groupID)
	t.Logf("[GOLDEN] State after join: %s", stateAfterJoin)

	// Golden check: After join, state should be an active state (not Dead)
	if stateAfterJoin == "Dead" {
		t.Error("[GOLDEN] group should not be Dead after join")
	}

	// Complete sync to reach Stable
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = joinResp.Generation
	assignment := kmsg.NewSyncGroupRequestGroupAssignment()
	assignment.MemberID = memberID
	assignment.MemberAssignment = encodeAssignment([]topicPartition{{Topic: topicName, Partition: 0}})
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignment)
	if _, err := syncReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}

	stateAfterSync := describeGroupState(t, ctx, descClient, groupID)
	t.Logf("[GOLDEN] State after sync: %s", stateAfterSync)

	// Golden check: After SyncGroup, state should be Stable
	if stateAfterSync != "Stable" {
		t.Errorf("[GOLDEN] expected Stable after sync, got %s", stateAfterSync)
	}

	t.Log("[GOLDEN] Group state join to stable verified")
}

// TestGolden_OffsetCommit_WithGroup validates offset commit within group.
func TestGolden_OffsetCommit_WithGroup(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-offset-commit-group"
	topicName := "golden-offset-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Join and sync
	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{topicName})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{topicName})

	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = joinResp.Generation
	assignment := kmsg.NewSyncGroupRequestGroupAssignment()
	assignment.MemberID = memberID
	assignment.MemberAssignment = encodeAssignment([]topicPartition{{Topic: topicName, Partition: 0}})
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignment)
	if _, err := syncReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}

	// Commit offset
	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.Group = groupID
	commitReq.MemberID = memberID
	commitReq.Generation = joinResp.Generation

	topicCommit := kmsg.NewOffsetCommitRequestTopic()
	topicCommit.Topic = topicName

	partCommit := kmsg.NewOffsetCommitRequestTopicPartition()
	partCommit.Partition = 0
	partCommit.Offset = 42
	partCommit.Metadata = new(string)
	*partCommit.Metadata = "golden-test"

	topicCommit.Partitions = append(topicCommit.Partitions, partCommit)
	commitReq.Topics = append(commitReq.Topics, topicCommit)

	commitResp, err := commitReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("OffsetCommit failed: %v", err)
	}

	// Golden check: Commit should succeed
	for _, topic := range commitResp.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode != 0 {
				t.Errorf("[GOLDEN] offset commit error for %s/%d: %d",
					topic.Topic, part.Partition, part.ErrorCode)
			}
		}
	}

	// Fetch offset back
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.Group = groupID

	topicFetch := kmsg.NewOffsetFetchRequestTopic()
	topicFetch.Topic = topicName
	topicFetch.Partitions = []int32{0}
	fetchReq.Topics = append(fetchReq.Topics, topicFetch)

	fetchResp, err := fetchReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("OffsetFetch failed: %v", err)
	}

	// Golden check: Offset should match committed value
	for _, topic := range fetchResp.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode != 0 {
				t.Errorf("[GOLDEN] offset fetch error: %d", part.ErrorCode)
			}
			if part.Offset != 42 {
				t.Errorf("[GOLDEN] expected offset 42, got %d", part.Offset)
			}
			if part.Metadata != nil && *part.Metadata != "golden-test" {
				t.Errorf("[GOLDEN] expected metadata 'golden-test', got %q", *part.Metadata)
			}
		}
	}

	t.Logf("[GOLDEN] Offset commit with group: member=%s, offset=42", memberID)
}

// TestGolden_DescribeGroups_Details validates DescribeGroups response.
func TestGolden_DescribeGroups_Details(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "golden-describe-group"
	topicName := "golden-describe-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 2); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Join and sync to create a stable group
	memberID := joinWithRetry(t, ctx, client, groupID, "range", []string{topicName})
	joinResp := rejoinGroup(t, ctx, client, groupID, memberID, []string{"range"}, []string{topicName})

	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = joinResp.Generation
	assignment := kmsg.NewSyncGroupRequestGroupAssignment()
	assignment.MemberID = memberID
	assignment.MemberAssignment = encodeAssignment([]topicPartition{
		{Topic: topicName, Partition: 0},
		{Topic: topicName, Partition: 1},
	})
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignment)
	if _, err := syncReq.RequestWith(ctx, client); err != nil {
		t.Fatalf("SyncGroup failed: %v", err)
	}

	// Describe group
	descReq := kmsg.NewPtrDescribeGroupsRequest()
	descReq.Groups = []string{groupID}

	descResp, err := descReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("DescribeGroups failed: %v", err)
	}

	if len(descResp.Groups) != 1 {
		t.Fatalf("[GOLDEN] expected 1 group, got %d", len(descResp.Groups))
	}

	g := descResp.Groups[0]

	// Golden check: Error code should be 0
	if g.ErrorCode != 0 {
		t.Errorf("[GOLDEN] describe error: %d", g.ErrorCode)
	}

	// Golden check: State should be Stable
	if g.State != "Stable" {
		t.Errorf("[GOLDEN] expected state Stable, got %s", g.State)
	}

	// Golden check: Protocol type should be consumer
	if g.ProtocolType != "consumer" {
		t.Errorf("[GOLDEN] expected protocol type 'consumer', got %s", g.ProtocolType)
	}

	// Golden check: Protocol should match
	if g.Protocol != "range" {
		t.Errorf("[GOLDEN] expected protocol 'range', got %s", g.Protocol)
	}

	// Golden check: Members should contain our member
	if len(g.Members) != 1 {
		t.Errorf("[GOLDEN] expected 1 member, got %d", len(g.Members))
	} else {
		m := g.Members[0]
		if m.MemberID != memberID {
			t.Errorf("[GOLDEN] member ID mismatch")
		}
		// Golden check: Member assignment should be populated
		if len(m.MemberAssignment) == 0 {
			t.Error("[GOLDEN] member assignment should not be empty")
		}
	}

	t.Logf("[GOLDEN] DescribeGroups: state=%s, protocol=%s, members=%d",
		g.State, g.Protocol, len(g.Members))
}

// Helper functions

type topicPartition struct {
	Topic     string
	Partition int32
}

func encodeSubscription(topics []string) []byte {
	// Minimal consumer subscription metadata
	// Version (2 bytes) + topic array length (4 bytes) + topic strings + userdata (4 bytes)
	var buf bytes.Buffer

	// Version 0
	buf.WriteByte(0)
	buf.WriteByte(0)

	// Topic array length
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(byte(len(topics)))

	for _, topic := range topics {
		// Topic string length (2 bytes) + topic
		buf.WriteByte(0)
		buf.WriteByte(byte(len(topic)))
		buf.WriteString(topic)
	}

	// Empty userdata
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)

	return buf.Bytes()
}

func encodeAssignment(partitions []topicPartition) []byte {
	if len(partitions) == 0 {
		return nil
	}

	// Group by topic
	topicMap := make(map[string][]int32)
	for _, tp := range partitions {
		topicMap[tp.Topic] = append(topicMap[tp.Topic], tp.Partition)
	}

	var buf bytes.Buffer

	// Version 0
	buf.WriteByte(0)
	buf.WriteByte(0)

	// Topic assignment array length
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(byte(len(topicMap)))

	// Sort topics for determinism
	var topics []string
	for t := range topicMap {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		parts := topicMap[topic]

		// Topic string length (2 bytes) + topic
		buf.WriteByte(0)
		buf.WriteByte(byte(len(topic)))
		buf.WriteString(topic)

		// Partition array length
		buf.WriteByte(0)
		buf.WriteByte(0)
		buf.WriteByte(0)
		buf.WriteByte(byte(len(parts)))

		for _, p := range parts {
			// Partition (4 bytes big-endian)
			buf.WriteByte(byte(p >> 24))
			buf.WriteByte(byte(p >> 16))
			buf.WriteByte(byte(p >> 8))
			buf.WriteByte(byte(p))
		}
	}

	// Empty userdata
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)

	return buf.Bytes()
}

func decodeAssignment(data []byte) []topicPartition {
	if len(data) < 6 {
		return nil
	}

	// Skip version (2 bytes)
	pos := 2

	// Topic count (4 bytes)
	if pos+4 > len(data) {
		return nil
	}
	topicCount := int(data[pos])<<24 | int(data[pos+1])<<16 | int(data[pos+2])<<8 | int(data[pos+3])
	pos += 4

	var result []topicPartition

	for i := 0; i < topicCount && pos < len(data); i++ {
		// Topic length (2 bytes)
		if pos+2 > len(data) {
			break
		}
		topicLen := int(data[pos])<<8 | int(data[pos+1])
		pos += 2

		if pos+topicLen > len(data) {
			break
		}
		topic := string(data[pos : pos+topicLen])
		pos += topicLen

		// Partition count (4 bytes)
		if pos+4 > len(data) {
			break
		}
		partCount := int(data[pos])<<24 | int(data[pos+1])<<16 | int(data[pos+2])<<8 | int(data[pos+3])
		pos += 4

		for j := 0; j < partCount && pos+4 <= len(data); j++ {
			partition := int32(data[pos])<<24 | int32(data[pos+1])<<16 | int32(data[pos+2])<<8 | int32(data[pos+3])
			pos += 4
			result = append(result, topicPartition{Topic: topic, Partition: partition})
		}
	}

	return result
}

func joinWithRetry(t *testing.T, ctx context.Context, client *kgo.Client, groupID, protocol string, topics []string) string {
	t.Helper()

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = groupID
	joinReq.MemberID = ""
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	joinReq.RebalanceTimeoutMillis = 5000

	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = protocol
	proto.Metadata = encodeSubscription(topics)
	joinReq.Protocols = append(joinReq.Protocols, proto)

	joinResp, err := joinReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}

	if joinResp.ErrorCode == 79 { // MEMBER_ID_REQUIRED
		joinReq.MemberID = joinResp.MemberID
		joinResp, err = joinReq.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("JoinGroup retry failed: %v", err)
		}
	}

	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error: %d", joinResp.ErrorCode)
	}

	return joinResp.MemberID
}

func joinWithRetryResponse(t *testing.T, ctx context.Context, client *kgo.Client, groupID, protocol string, topics []string) (string, *kmsg.JoinGroupResponse) {
	t.Helper()

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = groupID
	joinReq.MemberID = ""
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	joinReq.RebalanceTimeoutMillis = 5000

	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = protocol
	proto.Metadata = encodeSubscription(topics)
	joinReq.Protocols = append(joinReq.Protocols, proto)

	joinResp, err := joinReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}

	if joinResp.ErrorCode == 79 { // MEMBER_ID_REQUIRED
		joinReq.MemberID = joinResp.MemberID
		joinResp, err = joinReq.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("JoinGroup retry failed: %v", err)
		}
	}

	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error: %d", joinResp.ErrorCode)
	}

	return joinResp.MemberID, joinResp
}

func joinWithMultipleProtocols(t *testing.T, ctx context.Context, client *kgo.Client, groupID string, protocols []string, topics []string) string {
	t.Helper()

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = groupID
	joinReq.MemberID = ""
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	joinReq.RebalanceTimeoutMillis = 5000

	for _, p := range protocols {
		proto := kmsg.NewJoinGroupRequestProtocol()
		proto.Name = p
		proto.Metadata = encodeSubscription(topics)
		joinReq.Protocols = append(joinReq.Protocols, proto)
	}

	joinResp, err := joinReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("JoinGroup failed: %v", err)
	}

	if joinResp.ErrorCode == 79 { // MEMBER_ID_REQUIRED
		joinReq.MemberID = joinResp.MemberID
		joinResp, err = joinReq.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("JoinGroup retry failed: %v", err)
		}
	}

	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error: %d", joinResp.ErrorCode)
	}

	return joinResp.MemberID
}

func rejoinGroup(t *testing.T, ctx context.Context, client *kgo.Client, groupID, memberID string, protocols, topics []string) *kmsg.JoinGroupResponse {
	t.Helper()

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = groupID
	joinReq.MemberID = memberID
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	joinReq.RebalanceTimeoutMillis = 5000

	for _, p := range protocols {
		proto := kmsg.NewJoinGroupRequestProtocol()
		proto.Name = p
		proto.Metadata = encodeSubscription(topics)
		joinReq.Protocols = append(joinReq.Protocols, proto)
	}

	joinResp, err := joinReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("JoinGroup rejoin failed: %v", err)
	}

	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup rejoin error: %d", joinResp.ErrorCode)
	}

	return joinResp
}

func syncGroupRequest(ctx context.Context, client *kgo.Client, groupID, memberID string, generation int32, assignments []kmsg.SyncGroupRequestGroupAssignment) (*kmsg.SyncGroupResponse, error) {
	syncReq := kmsg.NewPtrSyncGroupRequest()
	syncReq.Group = groupID
	syncReq.MemberID = memberID
	syncReq.Generation = generation
	syncReq.GroupAssignment = append(syncReq.GroupAssignment, assignments...)

	return syncReq.RequestWith(ctx, client)
}

func assignmentHasPartition(parts []topicPartition, topic string, partition int32) bool {
	for _, tp := range parts {
		if tp.Topic == topic && tp.Partition == partition {
			return true
		}
	}
	return false
}

func describeGroupState(t *testing.T, ctx context.Context, client *kgo.Client, groupID string) string {
	t.Helper()

	descReq := kmsg.NewPtrDescribeGroupsRequest()
	descReq.Groups = []string{groupID}

	descResp, err := descReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("DescribeGroups failed: %v", err)
	}

	if len(descResp.Groups) == 0 {
		return ""
	}

	g := descResp.Groups[0]
	if g.ErrorCode != 0 {
		// Could be GROUP_ID_NOT_FOUND for non-existent group
		return "Dead"
	}

	return g.State
}
