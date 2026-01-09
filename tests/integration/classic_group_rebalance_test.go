package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// joinConsumer performs the JoinGroup flow for a single consumer.
// If initialMemberID is empty, it handles the MEMBER_ID_REQUIRED flow for v4+.
// Uses version 3 to avoid MEMBER_ID_REQUIRED complexity.
func joinConsumer(ctx context.Context, handler *protocol.JoinGroupHandler, groupID, initialMemberID, clientID string, req *kmsg.JoinGroupRequest) (*kmsg.JoinGroupResponse, string) {
	req.Group = groupID
	req.MemberID = initialMemberID

	// Use version 3 to avoid MEMBER_ID_REQUIRED flow
	resp := handler.Handle(ctx, 3, req, clientID)
	return resp, resp.MemberID
}

func TestClassicGroupJoin_InitialRebalanceDelay(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	joinHandler := protocol.NewJoinGroupHandler(
		protocol.JoinGroupHandlerConfig{
			MinSessionTimeoutMs:     1000,
			MaxSessionTimeoutMs:     60000,
			InitialRebalanceDelayMs: 50,
			RebalanceTimeoutMs:      60000,
		},
		groupStore,
		nil, // No lease manager for simpler testing
	)

	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = "initial-delay-group"
	req.SessionTimeoutMillis = 6000
	req.RebalanceTimeoutMillis = 10000
	req.ProtocolType = "consumer"

	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	proto.Metadata = buildConsumerMetadata("test-topic")
	req.Protocols = append(req.Protocols, proto)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp := joinHandler.Handle(ctx, 3, req, "test-client")
	if resp.ErrorCode != 0 {
		t.Fatalf("JoinGroup failed: error code %d", resp.ErrorCode)
	}
	if resp.MemberID == "" {
		t.Fatal("expected member ID to be set")
	}
	if resp.LeaderID == "" {
		t.Fatal("expected leader ID to be set")
	}
}

// TestClassicGroupRebalance_TwoConsumers tests a classic group with 2 consumers.
// Both consumers should receive assignments after joining.
func TestClassicGroupRebalance_TwoConsumers(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	// Use nil lease manager to avoid concurrent lease conflicts in tests
	// (lease management is tested separately)
	ctx := context.Background()

	joinHandler := protocol.NewJoinGroupHandler(
		protocol.JoinGroupHandlerConfig{
			MinSessionTimeoutMs: 1000,
			MaxSessionTimeoutMs: 30000,
			RebalanceTimeoutMs:  100, // Short timeout for tests (100ms)
		},
		groupStore,
		nil, // No lease manager for simpler testing
	)

	syncHandler := protocol.NewSyncGroupHandler(groupStore, nil)

	groupID := "test-group-2"
	topicName := "test-topic"
	numPartitions := int32(4)

	// Both consumers will join in parallel
	var wg sync.WaitGroup
	responses := make([]*kmsg.JoinGroupResponse, 2)
	memberIDs := make([]string, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := "consumer-" + string(rune('A'+idx))
			joinReq := buildJoinGroupRequest(groupID, "", topicName, numPartitions)
			resp, memberID := joinConsumer(ctx, joinHandler, groupID, "", clientID, joinReq)
			responses[idx] = resp
			memberIDs[idx] = memberID
		}(i)
	}

	wg.Wait()

	// Verify both responses
	for i, resp := range responses {
		if resp.ErrorCode != 0 {
			t.Errorf("consumer %d JoinGroup failed: error code %d", i, resp.ErrorCode)
		}
		if resp.Generation <= 0 {
			t.Errorf("consumer %d got invalid generation %d", i, resp.Generation)
		}
		if resp.MemberID == "" {
			t.Errorf("consumer %d got empty member ID", i)
		}
	}

	// Verify one of them is the leader
	leaderCount := 0
	var leaderIdx int
	for i, resp := range responses {
		if resp.MemberID == resp.LeaderID {
			leaderCount++
			leaderIdx = i
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaderCount)
	}

	// Leader should have members list
	if len(responses[leaderIdx].Members) != 2 {
		t.Errorf("leader should have 2 members, got %d", len(responses[leaderIdx].Members))
	}

	t.Logf("JoinGroup phase complete: leader=%s, generation=%d",
		responses[leaderIdx].MemberID, responses[leaderIdx].Generation)

	// Now perform SyncGroup - leader distributes assignments
	generation := responses[0].Generation

	// Leader creates assignments using range assignor
	assignments := buildGroupAssignments(responses[leaderIdx].Members, topicName, numPartitions)

	// All members sync - leader first to distribute assignments, then followers
	// This ensures the leader's assignments are stored before followers request them
	syncResponses := make([]*kmsg.SyncGroupResponse, 2)

	// Leader syncs first with assignments
	syncReq := buildSyncGroupRequest(groupID, memberIDs[leaderIdx], generation, assignments)
	syncResponses[leaderIdx] = syncHandler.Handle(ctx, 3, syncReq)

	// Then followers sync
	for i := 0; i < 2; i++ {
		if i == leaderIdx {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			syncReq := buildSyncGroupRequest(groupID, memberIDs[idx], generation, nil)
			syncResponses[idx] = syncHandler.Handle(ctx, 3, syncReq)
		}(i)
	}

	wg.Wait()

	// Verify all consumers got assignments
	for i, resp := range syncResponses {
		if resp.ErrorCode != 0 {
			t.Errorf("consumer %d SyncGroup failed: error code %d", i, resp.ErrorCode)
		}
		if len(resp.MemberAssignment) == 0 {
			t.Errorf("consumer %d got empty assignment", i)
		}
	}

	t.Log("SyncGroup phase complete: all 2 consumers have assignments")
}

// TestClassicGroupRebalance_AddThirdConsumer tests adding a third consumer triggers rebalance.
func TestClassicGroupRebalance_AddThirdConsumer(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	ctx := context.Background()

	joinHandler := protocol.NewJoinGroupHandler(
		protocol.JoinGroupHandlerConfig{
			MinSessionTimeoutMs: 1000,
			MaxSessionTimeoutMs: 30000,
			RebalanceTimeoutMs:  100, // Short timeout for tests
		},
		groupStore,
		nil,
	)

	syncHandler := protocol.NewSyncGroupHandler(groupStore, nil)

	groupID := "test-group-3"
	topicName := "test-topic"
	numPartitions := int32(6) // 6 partitions for better distribution

	// Phase 1: Start with 2 consumers
	var wg sync.WaitGroup
	responses := make([]*kmsg.JoinGroupResponse, 2)
	memberIDs := make([]string, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := "consumer-" + string(rune('A'+idx))
			joinReq := buildJoinGroupRequest(groupID, "", topicName, numPartitions)
			resp, memberID := joinConsumer(ctx, joinHandler, groupID, "", clientID, joinReq)
			responses[idx] = resp
			memberIDs[idx] = memberID
		}(i)
	}

	wg.Wait()

	// Find leader
	var leaderIdx int
	for i, resp := range responses {
		if resp.MemberID == resp.LeaderID {
			leaderIdx = i
			break
		}
	}

	generation1 := responses[0].Generation
	t.Logf("Phase 1: 2 consumers joined, generation=%d", generation1)

	// Complete SyncGroup for initial 2 consumers
	assignments := buildGroupAssignments(responses[leaderIdx].Members, topicName, numPartitions)

	syncResponses := make([]*kmsg.SyncGroupResponse, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			syncReq := buildSyncGroupRequest(groupID, memberIDs[idx], generation1, nil)
			if memberIDs[idx] == responses[leaderIdx].MemberID {
				syncReq.GroupAssignment = assignments
			}
			syncResponses[idx] = syncHandler.Handle(ctx, 3, syncReq)
		}(i)
	}

	wg.Wait()

	for i, resp := range syncResponses {
		if resp.ErrorCode != 0 {
			t.Fatalf("initial consumer %d sync failed: error code %d", i, resp.ErrorCode)
		}
	}

	t.Log("Phase 1 complete: 2 consumers stable with assignments")

	// Phase 2: Add third consumer - triggers rebalance
	// All three need to rejoin

	responses3 := make([]*kmsg.JoinGroupResponse, 3)
	memberIDs3 := make([]string, 3)
	memberIDs3[0] = memberIDs[0]
	memberIDs3[1] = memberIDs[1]

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var clientID string
			var reqMemberID string
			if idx < 2 {
				clientID = "consumer-" + string(rune('A'+idx))
				reqMemberID = memberIDs[idx]
			} else {
				clientID = "consumer-C"
				reqMemberID = ""
			}
			joinReq := buildJoinGroupRequest(groupID, reqMemberID, topicName, numPartitions)
			resp, memberID := joinConsumer(ctx, joinHandler, groupID, reqMemberID, clientID, joinReq)
			responses3[idx] = resp
			memberIDs3[idx] = memberID
		}(i)
	}

	wg.Wait()

	// Verify all 3 joined successfully
	for i, resp := range responses3 {
		if resp.ErrorCode != 0 {
			t.Errorf("phase 2 consumer %d JoinGroup failed: error code %d", i, resp.ErrorCode)
		}
	}

	generation2 := responses3[0].Generation
	if generation2 <= generation1 {
		t.Errorf("expected generation to increase: was %d, now %d", generation1, generation2)
	}

	t.Logf("Phase 2: 3 consumers joined, generation=%d (was %d)", generation2, generation1)

	// Find new leader
	var leader3Idx int
	for i, resp := range responses3 {
		if resp.MemberID == resp.LeaderID {
			leader3Idx = i
			break
		}
	}

	if len(responses3[leader3Idx].Members) != 3 {
		t.Errorf("expected leader to have 3 members, got %d", len(responses3[leader3Idx].Members))
	}

	// Complete SyncGroup for all 3 consumers
	assignments3 := buildGroupAssignments(responses3[leader3Idx].Members, topicName, numPartitions)

	syncResponses3 := make([]*kmsg.SyncGroupResponse, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			syncReq := buildSyncGroupRequest(groupID, memberIDs3[idx], generation2, nil)
			if memberIDs3[idx] == responses3[leader3Idx].MemberID {
				syncReq.GroupAssignment = assignments3
			}
			syncResponses3[idx] = syncHandler.Handle(ctx, 3, syncReq)
		}(i)
	}

	wg.Wait()

	// Verify all 3 consumers got assignments
	assignmentCount := 0
	for i, resp := range syncResponses3 {
		if resp.ErrorCode != 0 {
			t.Errorf("phase 2 consumer %d SyncGroup failed: error code %d", i, resp.ErrorCode)
		}
		if len(resp.MemberAssignment) > 0 {
			assignmentCount++
		}
	}

	if assignmentCount != 3 {
		t.Errorf("expected 3 consumers with assignments, got %d", assignmentCount)
	}

	t.Log("Phase 2 complete: 3 consumers stable with assignments")
}

// TestClassicGroupRebalance_RemoveConsumer tests removing a consumer triggers rebalance.
func TestClassicGroupRebalance_RemoveConsumer(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	ctx := context.Background()

	joinHandler := protocol.NewJoinGroupHandler(
		protocol.JoinGroupHandlerConfig{
			MinSessionTimeoutMs: 1000,
			MaxSessionTimeoutMs: 30000,
			RebalanceTimeoutMs:  100, // Short timeout for tests
		},
		groupStore,
		nil,
	)

	syncHandler := protocol.NewSyncGroupHandler(groupStore, nil)
	leaveHandler := protocol.NewLeaveGroupHandler(groupStore, nil, joinHandler)

	groupID := "test-group-leave"
	topicName := "test-topic"
	numPartitions := int32(6)

	// Phase 1: Start with 3 consumers
	var wg sync.WaitGroup
	responses := make([]*kmsg.JoinGroupResponse, 3)
	memberIDs := make([]string, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := "consumer-" + string(rune('A'+idx))
			joinReq := buildJoinGroupRequest(groupID, "", topicName, numPartitions)
			resp, memberID := joinConsumer(ctx, joinHandler, groupID, "", clientID, joinReq)
			responses[idx] = resp
			memberIDs[idx] = memberID
		}(i)
	}

	wg.Wait()

	for i, resp := range responses {
		if resp.ErrorCode != 0 {
			t.Fatalf("phase 1 consumer %d JoinGroup failed: error code %d", i, resp.ErrorCode)
		}
	}

	generation1 := responses[0].Generation
	t.Logf("Phase 1: 3 consumers joined, generation=%d", generation1)

	// Find leader and complete sync
	var leaderIdx int
	for i, resp := range responses {
		if resp.MemberID == resp.LeaderID {
			leaderIdx = i
			break
		}
	}

	assignments := buildGroupAssignments(responses[leaderIdx].Members, topicName, numPartitions)

	syncResponses := make([]*kmsg.SyncGroupResponse, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			syncReq := buildSyncGroupRequest(groupID, memberIDs[idx], generation1, nil)
			if memberIDs[idx] == responses[leaderIdx].MemberID {
				syncReq.GroupAssignment = assignments
			}
			syncResponses[idx] = syncHandler.Handle(ctx, 3, syncReq)
		}(i)
	}

	wg.Wait()

	for i, resp := range syncResponses {
		if resp.ErrorCode != 0 {
			t.Fatalf("phase 1 consumer %d sync failed: error code %d", i, resp.ErrorCode)
		}
	}

	t.Log("Phase 1 complete: 3 consumers stable")

	// Phase 2: Remove consumer 2 (index 2)
	leaveReq := buildLeaveGroupRequest(groupID, memberIDs[2])
	leaveResp := leaveHandler.Handle(ctx, 3, leaveReq)

	if leaveResp.ErrorCode != 0 {
		t.Fatalf("LeaveGroup failed: error code %d", leaveResp.ErrorCode)
	}

	t.Logf("Phase 2: consumer %s left the group", memberIDs[2])

	// Phase 3: Remaining 2 consumers rejoin
	responses2 := make([]*kmsg.JoinGroupResponse, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clientID := "consumer-" + string(rune('A'+idx))
			joinReq := buildJoinGroupRequest(groupID, memberIDs[idx], topicName, numPartitions)
			resp, _ := joinConsumer(ctx, joinHandler, groupID, memberIDs[idx], clientID, joinReq)
			responses2[idx] = resp
		}(i)
	}

	wg.Wait()

	for i, resp := range responses2 {
		if resp.ErrorCode != 0 {
			t.Errorf("phase 3 consumer %d JoinGroup failed: error code %d", i, resp.ErrorCode)
		}
	}

	generation2 := responses2[0].Generation
	if generation2 <= generation1 {
		t.Errorf("expected generation to increase: was %d, now %d", generation1, generation2)
	}

	t.Logf("Phase 3: 2 consumers rejoined, generation=%d (was %d)", generation2, generation1)

	// Find new leader
	var leader2Idx int
	for i, resp := range responses2 {
		if resp.MemberID == resp.LeaderID {
			leader2Idx = i
			break
		}
	}

	if len(responses2[leader2Idx].Members) != 2 {
		t.Errorf("expected leader to have 2 members, got %d", len(responses2[leader2Idx].Members))
	}

	// Complete sync for remaining 2 consumers
	assignments2 := buildGroupAssignments(responses2[leader2Idx].Members, topicName, numPartitions)

	syncResponses2 := make([]*kmsg.SyncGroupResponse, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			syncReq := buildSyncGroupRequest(groupID, memberIDs[idx], generation2, nil)
			if memberIDs[idx] == responses2[leader2Idx].MemberID {
				syncReq.GroupAssignment = assignments2
			}
			syncResponses2[idx] = syncHandler.Handle(ctx, 3, syncReq)
		}(i)
	}

	wg.Wait()

	assignmentCount := 0
	for i, resp := range syncResponses2 {
		if resp.ErrorCode != 0 {
			t.Errorf("phase 3 consumer %d SyncGroup failed: error code %d", i, resp.ErrorCode)
		}
		if len(resp.MemberAssignment) > 0 {
			assignmentCount++
		}
	}

	if assignmentCount != 2 {
		t.Errorf("expected 2 consumers with assignments, got %d", assignmentCount)
	}

	t.Log("Phase 3 complete: 2 consumers stable with redistributed partitions")
}

// TestClassicGroupRebalance_FullCycle tests the complete rebalance cycle:
// start with 2 -> add 1 -> remove 1 -> verify redistribution
func TestClassicGroupRebalance_FullCycle(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	ctx := context.Background()

	joinHandler := protocol.NewJoinGroupHandler(
		protocol.JoinGroupHandlerConfig{
			MinSessionTimeoutMs: 1000,
			MaxSessionTimeoutMs: 30000,
			RebalanceTimeoutMs:  100, // Short timeout for tests
		},
		groupStore,
		nil,
	)

	syncHandler := protocol.NewSyncGroupHandler(groupStore, nil)
	leaveHandler := protocol.NewLeaveGroupHandler(groupStore, nil, joinHandler)

	groupID := "test-group-full-cycle"
	topicName := "test-topic"
	numPartitions := int32(6)

	// Helper to join and sync a group of consumers
	joinAndSync := func(requestMemberIDs []string) ([]string, int32) {
		var wg sync.WaitGroup
		responses := make([]*kmsg.JoinGroupResponse, len(requestMemberIDs))
		actualMemberIDs := make([]string, len(requestMemberIDs))

		for i := 0; i < len(requestMemberIDs); i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				clientID := "consumer-" + string(rune('A'+idx))
				joinReq := buildJoinGroupRequest(groupID, requestMemberIDs[idx], topicName, numPartitions)
				resp, memberID := joinConsumer(ctx, joinHandler, groupID, requestMemberIDs[idx], clientID, joinReq)
				responses[idx] = resp
				actualMemberIDs[idx] = memberID
			}(i)
		}

		wg.Wait()

		for i, resp := range responses {
			if resp.ErrorCode != 0 {
				t.Fatalf("consumer %d JoinGroup failed: error code %d", i, resp.ErrorCode)
			}
		}

		generation := responses[0].Generation

		// Find leader
		var leaderIdx int
		for i, resp := range responses {
			if resp.MemberID == resp.LeaderID {
				leaderIdx = i
				break
			}
		}

		// Create and distribute assignments
		assignments := buildGroupAssignments(responses[leaderIdx].Members, topicName, numPartitions)

		syncResponses := make([]*kmsg.SyncGroupResponse, len(actualMemberIDs))
		for i := 0; i < len(actualMemberIDs); i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				syncReq := buildSyncGroupRequest(groupID, actualMemberIDs[idx], generation, nil)
				if actualMemberIDs[idx] == responses[leaderIdx].MemberID {
					syncReq.GroupAssignment = assignments
				}
				syncResponses[idx] = syncHandler.Handle(ctx, 3, syncReq)
			}(i)
		}

		wg.Wait()

		for i, resp := range syncResponses {
			if resp.ErrorCode != 0 {
				t.Fatalf("consumer %d SyncGroup failed: error code %d", i, resp.ErrorCode)
			}
		}

		return actualMemberIDs, generation
	}

	// Step 1: Start with 2 consumers
	memberIDs, gen1 := joinAndSync([]string{"", ""})
	t.Logf("Step 1: 2 consumers stable, generation=%d, members=%v", gen1, memberIDs)

	// Step 2: Add third consumer
	memberIDs3 := append(memberIDs, "")
	memberIDs, gen2 := joinAndSync(memberIDs3)
	if gen2 <= gen1 {
		t.Errorf("generation should increase: was %d, now %d", gen1, gen2)
	}
	t.Logf("Step 2: 3 consumers stable, generation=%d, members=%v", gen2, memberIDs)

	// Step 3: Remove one consumer
	leaveReq := buildLeaveGroupRequest(groupID, memberIDs[2])
	leaveResp := leaveHandler.Handle(ctx, 3, leaveReq)
	if leaveResp.ErrorCode != 0 {
		t.Fatalf("LeaveGroup failed: error code %d", leaveResp.ErrorCode)
	}
	t.Logf("Step 3: consumer %s left", memberIDs[2])

	// Step 4: Remaining 2 consumers rejoin
	memberIDs2 := memberIDs[:2]
	memberIDs, gen3 := joinAndSync(memberIDs2)
	if gen3 <= gen2 {
		t.Errorf("generation should increase: was %d, now %d", gen2, gen3)
	}

	if len(memberIDs) != 2 {
		t.Errorf("expected 2 members after removal, got %d", len(memberIDs))
	}

	t.Logf("Step 4: 2 consumers stable after removal, generation=%d, members=%v", gen3, memberIDs)

	// Verify group state
	state, err := groupStore.GetGroupState(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}
	if state.Generation != gen3 {
		t.Errorf("group state generation mismatch: expected %d, got %d", gen3, state.Generation)
	}

	t.Log("Full rebalance cycle complete!")
}

// buildJoinGroupRequest creates a JoinGroup request.
// Uses short timeouts for faster test execution.
func buildJoinGroupRequest(groupID, memberID, topicName string, numPartitions int32) *kmsg.JoinGroupRequest {
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = groupID
	req.MemberID = memberID
	req.ProtocolType = "consumer"
	req.SessionTimeoutMillis = 6000  // Minimum 6 seconds
	req.RebalanceTimeoutMillis = 100 // 100ms rebalance timeout for fast tests

	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	proto.Metadata = buildConsumerMetadata(topicName)
	req.Protocols = append(req.Protocols, proto)

	return req
}

// buildConsumerMetadata creates consumer protocol metadata.
func buildConsumerMetadata(topicName string) []byte {
	// Consumer protocol metadata format:
	// - Version (2 bytes, int16)
	// - Topics array length (4 bytes, int32)
	// - Topic name length (2 bytes, int16) + topic name
	// - UserData length (4 bytes, int32)

	topicBytes := []byte(topicName)
	size := 2 + 4 + 2 + len(topicBytes) + 4
	buf := make([]byte, size)

	offset := 0

	// Version = 0
	buf[offset] = 0
	buf[offset+1] = 0
	offset += 2

	// Topics array length = 1
	buf[offset] = 0
	buf[offset+1] = 0
	buf[offset+2] = 0
	buf[offset+3] = 1
	offset += 4

	// Topic name length
	buf[offset] = byte(len(topicBytes) >> 8)
	buf[offset+1] = byte(len(topicBytes))
	offset += 2

	// Topic name
	copy(buf[offset:], topicBytes)
	offset += len(topicBytes)

	// UserData length = 0
	buf[offset] = 0
	buf[offset+1] = 0
	buf[offset+2] = 0
	buf[offset+3] = 0

	return buf
}

// buildSyncGroupRequest creates a SyncGroup request.
func buildSyncGroupRequest(groupID, memberID string, generation int32, assignments []kmsg.SyncGroupRequestGroupAssignment) *kmsg.SyncGroupRequest {
	req := kmsg.NewPtrSyncGroupRequest()
	req.Group = groupID
	req.MemberID = memberID
	req.Generation = generation
	req.GroupAssignment = assignments
	return req
}

// buildGroupAssignments creates assignments using range strategy.
func buildGroupAssignments(members []kmsg.JoinGroupResponseMember, topicName string, numPartitions int32) []kmsg.SyncGroupRequestGroupAssignment {
	numMembers := int32(len(members))
	if numMembers == 0 {
		return nil
	}

	partitionsPerMember := numPartitions / numMembers
	extraPartitions := numPartitions % numMembers

	assignments := make([]kmsg.SyncGroupRequestGroupAssignment, len(members))
	currentPartition := int32(0)

	for i, member := range members {
		count := partitionsPerMember
		if int32(i) < extraPartitions {
			count++
		}

		partitions := make([]groups.TopicPartition, count)
		for j := int32(0); j < count; j++ {
			partitions[j] = groups.TopicPartition{
				Topic:     topicName,
				Partition: currentPartition,
			}
			currentPartition++
		}

		assignments[i] = kmsg.NewSyncGroupRequestGroupAssignment()
		assignments[i].MemberID = member.MemberID
		assignments[i].MemberAssignment = groups.EncodeAssignment(partitions)
	}

	return assignments
}

// buildLeaveGroupRequest creates a LeaveGroup request (v3+ format).
func buildLeaveGroupRequest(groupID, memberID string) *kmsg.LeaveGroupRequest {
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = groupID

	// For v3+, use Members array
	member := kmsg.NewLeaveGroupRequestMember()
	member.MemberID = memberID
	req.Members = append(req.Members, member)

	return req
}

// Small delay helper for test synchronization
func testDelay() {
	time.Sleep(10 * time.Millisecond)
}
