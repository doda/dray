package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestKIP848ConsumerGroup_SingleMemberJoin tests a single member joining
// a KIP-848 consumer group and receiving an assignment.
func TestKIP848ConsumerGroup_SingleMemberJoin(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	// Create a topic with 6 partitions for server-side assignment
	topicName := "kip848-test-topic"
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 6,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-test-group-1"

	// Step 1: Join group with ConsumerGroupHeartbeat
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group = groupID
	req.MemberID = ""
	req.MemberEpoch = 0
	req.SubscribedTopicNames = []string{topicName}

	resp := handler.Handle(ctx, 0, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected error code 0, got %d", resp.ErrorCode)
	}
	if resp.MemberID == nil || *resp.MemberID == "" {
		t.Error("expected non-empty member ID")
	}
	if resp.MemberEpoch != 1 {
		t.Errorf("expected member epoch 1, got %d", resp.MemberEpoch)
	}

	t.Logf("Member joined: memberID=%s, epoch=%d", *resp.MemberID, resp.MemberEpoch)

	// Step 2: Verify assignment received (server-side assignment for KIP-848)
	if resp.Assignment == nil {
		t.Error("expected non-nil assignment")
	} else if len(resp.Assignment.Topics) == 0 {
		t.Error("expected non-empty topics in assignment")
	} else {
		totalPartitions := int32(0)
		for _, topic := range resp.Assignment.Topics {
			totalPartitions += int32(len(topic.Partitions))
			t.Logf("Assigned topic %s with partitions: %v", topic.TopicID, topic.Partitions)
		}
		// Single member should get all 6 partitions
		if totalPartitions != 6 {
			t.Errorf("expected 6 total partitions, got %d", totalPartitions)
		}
	}

	// Step 3: Verify group type is consumer (KIP-848)
	groupType, err := groupStore.GetGroupType(ctx, groupID)
	if err != nil {
		t.Errorf("failed to get group type: %v", err)
	}
	if groupType != groups.GroupTypeConsumer {
		t.Errorf("expected group type 'consumer', got '%s'", groupType)
	}

	t.Log("KIP-848 single member join test passed")
}

// TestKIP848ConsumerGroup_TwoMemberRebalance tests two members joining
// and getting balanced partition assignment.
func TestKIP848ConsumerGroup_TwoMemberRebalance(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	topicName := "kip848-rebalance-topic"
	numPartitions := int32(6)
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: numPartitions,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-rebalance-group"

	// Phase 1: First member joins
	req1 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req1.Group = groupID
	req1.MemberID = ""
	req1.MemberEpoch = 0
	req1.SubscribedTopicNames = []string{topicName}

	resp1 := handler.Handle(ctx, 0, req1)
	if resp1.ErrorCode != 0 {
		t.Fatalf("member 1 join failed: error code %d", resp1.ErrorCode)
	}

	member1ID := *resp1.MemberID
	member1Epoch := resp1.MemberEpoch
	t.Logf("Member 1 joined: memberID=%s, epoch=%d", member1ID, member1Epoch)

	// Phase 2: Second member joins
	req2 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req2.Group = groupID
	req2.MemberID = ""
	req2.MemberEpoch = 0
	req2.SubscribedTopicNames = []string{topicName}

	resp2 := handler.Handle(ctx, 0, req2)
	if resp2.ErrorCode != 0 {
		t.Fatalf("member 2 join failed: error code %d", resp2.ErrorCode)
	}

	member2ID := *resp2.MemberID
	t.Logf("Member 2 joined: memberID=%s, epoch=%d", member2ID, resp2.MemberEpoch)

	// Phase 3: Member 1 sends heartbeat to get updated assignment
	hb1 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb1.Group = groupID
	hb1.MemberID = member1ID
	hb1.MemberEpoch = member1Epoch

	hbResp1 := handler.Handle(ctx, 0, hb1)
	if hbResp1.ErrorCode != 0 {
		t.Fatalf("member 1 heartbeat failed: error code %d", hbResp1.ErrorCode)
	}

	// Verify both got assignments
	if hbResp1.Assignment == nil || len(hbResp1.Assignment.Topics) == 0 {
		t.Error("member 1: expected assignment")
	}
	if resp2.Assignment == nil || len(resp2.Assignment.Topics) == 0 {
		t.Error("member 2: expected assignment")
	}

	// Count total partitions assigned (use member 1's updated assignment)
	member1Partitions := countPartitions(hbResp1.Assignment)
	member2Partitions := countPartitions(resp2.Assignment)

	t.Logf("Member 1 has %d partitions, Member 2 has %d partitions", member1Partitions, member2Partitions)

	// With 6 partitions and 2 members, each should get 3
	if member1Partitions+member2Partitions != int(numPartitions) {
		t.Errorf("expected total partitions %d, got %d", numPartitions, member1Partitions+member2Partitions)
	}

	// Range assignor should give 3 partitions to each member
	if member1Partitions != 3 {
		t.Errorf("expected member 1 to have 3 partitions, got %d", member1Partitions)
	}
	if member2Partitions != 3 {
		t.Errorf("expected member 2 to have 3 partitions, got %d", member2Partitions)
	}

	t.Log("KIP-848 two member rebalance test passed")
}

// TestKIP848ConsumerGroup_IncrementalRebalance tests incremental rebalance
// when a third member joins an existing group.
func TestKIP848ConsumerGroup_IncrementalRebalance(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	topicName := "kip848-incremental-topic"
	numPartitions := int32(6)
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: numPartitions,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-incremental-group"
	memberIDs := make([]string, 3)

	// Phase 1: Two members join
	for i := 0; i < 2; i++ {
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = groupID
		req.MemberID = ""
		req.MemberEpoch = 0
		req.SubscribedTopicNames = []string{topicName}

		resp := handler.Handle(ctx, 0, req)
		if resp.ErrorCode != 0 {
			t.Fatalf("member %d join failed: error code %d", i+1, resp.ErrorCode)
		}
		memberIDs[i] = *resp.MemberID
		t.Logf("Phase 1: Member %d joined: %s", i+1, memberIDs[i])
	}

	// Phase 2: Third member joins (triggers incremental rebalance)
	req3 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req3.Group = groupID
	req3.MemberID = ""
	req3.MemberEpoch = 0
	req3.SubscribedTopicNames = []string{topicName}

	resp3 := handler.Handle(ctx, 0, req3)
	if resp3.ErrorCode != 0 {
		t.Fatalf("member 3 join failed: error code %d", resp3.ErrorCode)
	}
	memberIDs[2] = *resp3.MemberID
	t.Logf("Phase 2: Member 3 joined: %s", memberIDs[2])

	// Verify member 3 got assignment
	member3Partitions := countPartitions(resp3.Assignment)
	t.Logf("Member 3 has %d partitions", member3Partitions)

	// With 6 partitions and 3 members, each should get 2
	if member3Partitions != 2 {
		t.Errorf("expected member 3 to have 2 partitions, got %d", member3Partitions)
	}

	// Verify all 3 members are in the group
	members, err := groupStore.ListMembers(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}

	t.Log("KIP-848 incremental rebalance (add member) test passed")
}

// TestKIP848ConsumerGroup_MemberLeave tests a member leaving the group
// and verifying remaining members get reassigned partitions.
func TestKIP848ConsumerGroup_MemberLeave(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	topicName := "kip848-leave-topic"
	numPartitions := int32(6)
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: numPartitions,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-leave-group"

	// Phase 1: Three members join
	memberIDs := make([]string, 3)
	memberEpochs := make([]int32, 3)

	for i := 0; i < 3; i++ {
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = groupID
		req.MemberID = ""
		req.MemberEpoch = 0
		req.SubscribedTopicNames = []string{topicName}

		resp := handler.Handle(ctx, 0, req)
		if resp.ErrorCode != 0 {
			t.Fatalf("member %d join failed: error code %d", i+1, resp.ErrorCode)
		}
		memberIDs[i] = *resp.MemberID
		memberEpochs[i] = resp.MemberEpoch
		t.Logf("Phase 1: Member %d joined: %s, epoch=%d", i+1, memberIDs[i], memberEpochs[i])
	}

	// Verify all 3 members exist
	members, _ := groupStore.ListMembers(ctx, groupID)
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}

	// Phase 2: Member 3 leaves (using MemberEpoch = -1)
	leaveReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	leaveReq.Group = groupID
	leaveReq.MemberID = memberIDs[2]
	leaveReq.MemberEpoch = -1

	leaveResp := handler.Handle(ctx, 0, leaveReq)
	if leaveResp.ErrorCode != 0 {
		t.Fatalf("member 3 leave failed: error code %d", leaveResp.ErrorCode)
	}
	if leaveResp.MemberEpoch != -1 {
		t.Errorf("expected member epoch -1 after leave, got %d", leaveResp.MemberEpoch)
	}
	t.Logf("Phase 2: Member 3 left: %s", memberIDs[2])

	// Verify member 3 is removed
	members, _ = groupStore.ListMembers(ctx, groupID)
	if len(members) != 2 {
		t.Errorf("expected 2 members after leave, got %d", len(members))
	}

	// Phase 3: Remaining members heartbeat to get updated assignments
	for i := 0; i < 2; i++ {
		hbReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		hbReq.Group = groupID
		hbReq.MemberID = memberIDs[i]
		hbReq.MemberEpoch = memberEpochs[i]

		hbResp := handler.Handle(ctx, 0, hbReq)
		if hbResp.ErrorCode != 0 {
			t.Errorf("member %d heartbeat failed: error code %d", i+1, hbResp.ErrorCode)
			continue
		}

		partitionCount := countPartitions(hbResp.Assignment)
		t.Logf("Phase 3: Member %d has %d partitions", i+1, partitionCount)

		// With 6 partitions and 2 members, each should get 3
		if partitionCount != 3 {
			t.Errorf("expected member %d to have 3 partitions after rebalance, got %d", i+1, partitionCount)
		}
	}

	t.Log("KIP-848 member leave test passed")
}

// TestKIP848ConsumerGroup_SubscriptionChange tests that changing subscription
// triggers epoch bump and assignment update.
func TestKIP848ConsumerGroup_SubscriptionChange(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	// Create two topics
	topic1 := "kip848-sub-topic-1"
	topic2 := "kip848-sub-topic-2"
	for _, topicName := range []string{topic1, topic2} {
		_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
			Name:           topicName,
			PartitionCount: 3,
			NowMs:          time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", topicName, err)
		}
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-subscription-group"

	// Phase 1: Join with subscription to topic1 only
	joinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	joinReq.Group = groupID
	joinReq.MemberID = ""
	joinReq.MemberEpoch = 0
	joinReq.SubscribedTopicNames = []string{topic1}

	joinResp := handler.Handle(ctx, 0, joinReq)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("join failed: error code %d", joinResp.ErrorCode)
	}

	memberID := *joinResp.MemberID
	initialEpoch := joinResp.MemberEpoch

	initialPartitions := countPartitions(joinResp.Assignment)
	t.Logf("Phase 1: Joined with epoch=%d, partitions=%d (topic1 only)", initialEpoch, initialPartitions)

	if initialPartitions != 3 {
		t.Errorf("expected 3 partitions (topic1), got %d", initialPartitions)
	}

	// Phase 2: Change subscription to include topic2
	updateReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	updateReq.Group = groupID
	updateReq.MemberID = memberID
	updateReq.MemberEpoch = initialEpoch
	updateReq.SubscribedTopicNames = []string{topic1, topic2}

	updateResp := handler.Handle(ctx, 0, updateReq)
	if updateResp.ErrorCode != 0 {
		t.Fatalf("subscription update failed: error code %d", updateResp.ErrorCode)
	}

	newEpoch := updateResp.MemberEpoch
	newPartitions := countPartitions(updateResp.Assignment)
	t.Logf("Phase 2: After subscription change, epoch=%d, partitions=%d", newEpoch, newPartitions)

	// Epoch should be bumped
	if newEpoch <= initialEpoch {
		t.Errorf("expected epoch to increase: was %d, now %d", initialEpoch, newEpoch)
	}

	// Should now have partitions from both topics (3 + 3 = 6)
	if newPartitions != 6 {
		t.Errorf("expected 6 partitions (topic1 + topic2), got %d", newPartitions)
	}

	t.Log("KIP-848 subscription change test passed")
}

// TestKIP848ConsumerGroup_ConcurrentJoin tests multiple members joining
// concurrently.
func TestKIP848ConsumerGroup_ConcurrentJoin(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	topicName := "kip848-concurrent-topic"
	numPartitions := int32(12) // 12 partitions for 4 members = 3 each
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: numPartitions,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-concurrent-group"
	numMembers := 4

	var wg sync.WaitGroup
	responses := make([]*kmsg.ConsumerGroupHeartbeatResponse, numMembers)

	for i := 0; i < numMembers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
			req.Group = groupID
			req.MemberID = ""
			req.MemberEpoch = 0
			req.SubscribedTopicNames = []string{topicName}

			responses[idx] = handler.Handle(ctx, 0, req)
		}(i)
	}

	wg.Wait()

	// Verify all joins succeeded
	memberIDs := make([]string, numMembers)
	for i, resp := range responses {
		if resp.ErrorCode != 0 {
			t.Errorf("member %d join failed: error code %d", i+1, resp.ErrorCode)
			continue
		}
		if resp.MemberID == nil || *resp.MemberID == "" {
			t.Errorf("member %d got empty member ID", i+1)
			continue
		}
		memberIDs[i] = *resp.MemberID
		t.Logf("Member %d joined: %s, epoch=%d", i+1, memberIDs[i], resp.MemberEpoch)
	}

	// Verify all members have unique IDs
	uniqueIDs := make(map[string]bool)
	for _, id := range memberIDs {
		if id == "" {
			continue
		}
		if uniqueIDs[id] {
			t.Errorf("duplicate member ID: %s", id)
		}
		uniqueIDs[id] = true
	}

	// Verify correct number of members in store
	members, err := groupStore.ListMembers(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != numMembers {
		t.Errorf("expected %d members in store, got %d", numMembers, len(members))
	}

	// Final heartbeat to get stabilized assignments
	totalPartitions := 0
	for i := 0; i < numMembers; i++ {
		if responses[i].ErrorCode != 0 {
			continue
		}

		hbReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		hbReq.Group = groupID
		hbReq.MemberID = memberIDs[i]
		hbReq.MemberEpoch = responses[i].MemberEpoch

		hbResp := handler.Handle(ctx, 0, hbReq)
		if hbResp.ErrorCode != 0 {
			t.Errorf("member %d heartbeat failed: error code %d", i+1, hbResp.ErrorCode)
			continue
		}

		partitionCount := countPartitions(hbResp.Assignment)
		totalPartitions += partitionCount
		t.Logf("Member %d has %d partitions", i+1, partitionCount)
	}

	// Verify all partitions are assigned
	if totalPartitions != int(numPartitions) {
		t.Errorf("expected total partitions %d, got %d", numPartitions, totalPartitions)
	}

	t.Log("KIP-848 concurrent join test passed")
}

// TestKIP848ConsumerGroup_StaticMembership tests static membership with instance IDs.
func TestKIP848ConsumerGroup_StaticMembership(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	topicName := "kip848-static-topic"
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-static-group"
	instanceID := "my-static-instance"

	// Phase 1: Join with instance ID
	joinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	joinReq.Group = groupID
	joinReq.MemberID = ""
	joinReq.MemberEpoch = 0
	joinReq.InstanceID = &instanceID
	joinReq.SubscribedTopicNames = []string{topicName}

	joinResp := handler.Handle(ctx, 0, joinReq)
	if joinResp.ErrorCode != 0 {
		t.Fatalf("join failed: error code %d", joinResp.ErrorCode)
	}

	firstMemberID := *joinResp.MemberID
	firstEpoch := joinResp.MemberEpoch
	t.Logf("Phase 1: Static member joined: %s, epoch=%d", firstMemberID, firstEpoch)

	// Phase 2: Rejoin with same instance ID (simulating restart)
	rejoinReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	rejoinReq.Group = groupID
	rejoinReq.MemberID = ""
	rejoinReq.MemberEpoch = 0
	rejoinReq.InstanceID = &instanceID
	rejoinReq.SubscribedTopicNames = []string{topicName}

	rejoinResp := handler.Handle(ctx, 0, rejoinReq)
	if rejoinResp.ErrorCode != 0 {
		t.Fatalf("rejoin failed: error code %d", rejoinResp.ErrorCode)
	}

	secondMemberID := *rejoinResp.MemberID
	secondEpoch := rejoinResp.MemberEpoch
	t.Logf("Phase 2: Static member rejoined: %s, epoch=%d", secondMemberID, secondEpoch)

	// Should get same member ID (static membership)
	if secondMemberID != firstMemberID {
		t.Errorf("expected same member ID %s, got %s", firstMemberID, secondMemberID)
	}

	// Epoch should be incremented
	if secondEpoch <= firstEpoch {
		t.Errorf("expected epoch to increase: was %d, now %d", firstEpoch, secondEpoch)
	}

	// Verify still only one member in the group
	members, _ := groupStore.ListMembers(ctx, groupID)
	if len(members) != 1 {
		t.Errorf("expected 1 member, got %d", len(members))
	}

	t.Log("KIP-848 static membership test passed")
}

// TestKIP848ConsumerGroup_FullCycle tests the complete lifecycle:
// join -> add member -> remove member -> verify assignments
func TestKIP848ConsumerGroup_FullCycle(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	topicStore := topics.NewStore(metaStore)

	topicName := "kip848-fullcycle-topic"
	numPartitions := int32(6)
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           topicName,
		PartitionCount: numPartitions,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := protocol.NewConsumerGroupHeartbeatHandler(groupStore, nil, topicStore)

	groupID := "kip848-fullcycle-group"

	// Step 1: First member joins
	req1 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req1.Group = groupID
	req1.MemberID = ""
	req1.MemberEpoch = 0
	req1.SubscribedTopicNames = []string{topicName}

	resp1 := handler.Handle(ctx, 0, req1)
	if resp1.ErrorCode != 0 {
		t.Fatalf("step 1 failed: error code %d", resp1.ErrorCode)
	}

	member1ID := *resp1.MemberID
	member1Epoch := resp1.MemberEpoch
	partitions1 := countPartitions(resp1.Assignment)
	t.Logf("Step 1: Member 1 joined with %d partitions", partitions1)

	if partitions1 != int(numPartitions) {
		t.Errorf("expected %d partitions for single member, got %d", numPartitions, partitions1)
	}

	// Step 2: Second member joins
	req2 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req2.Group = groupID
	req2.MemberID = ""
	req2.MemberEpoch = 0
	req2.SubscribedTopicNames = []string{topicName}

	resp2 := handler.Handle(ctx, 0, req2)
	if resp2.ErrorCode != 0 {
		t.Fatalf("step 2 failed: error code %d", resp2.ErrorCode)
	}

	member2ID := *resp2.MemberID
	member2Epoch := resp2.MemberEpoch
	partitions2 := countPartitions(resp2.Assignment)
	t.Logf("Step 2: Member 2 joined with %d partitions", partitions2)

	// Step 3: Both members heartbeat to get current assignments
	hb1 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb1.Group = groupID
	hb1.MemberID = member1ID
	hb1.MemberEpoch = member1Epoch

	hbResp1 := handler.Handle(ctx, 0, hb1)
	if hbResp1.ErrorCode != 0 {
		t.Errorf("step 3 member 1 heartbeat failed: error code %d", hbResp1.ErrorCode)
	}
	member1Epoch = hbResp1.MemberEpoch

	hb2 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb2.Group = groupID
	hb2.MemberID = member2ID
	hb2.MemberEpoch = member2Epoch

	hbResp2 := handler.Handle(ctx, 0, hb2)
	if hbResp2.ErrorCode != 0 {
		t.Errorf("step 3 member 2 heartbeat failed: error code %d", hbResp2.ErrorCode)
	}
	member2Epoch = hbResp2.MemberEpoch

	p1 := countPartitions(hbResp1.Assignment)
	p2 := countPartitions(hbResp2.Assignment)
	t.Logf("Step 3: Member 1 has %d, Member 2 has %d partitions", p1, p2)

	if p1+p2 != int(numPartitions) {
		t.Errorf("expected total %d partitions, got %d", numPartitions, p1+p2)
	}

	// Step 4: Third member joins
	req3 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req3.Group = groupID
	req3.MemberID = ""
	req3.MemberEpoch = 0
	req3.SubscribedTopicNames = []string{topicName}

	resp3 := handler.Handle(ctx, 0, req3)
	if resp3.ErrorCode != 0 {
		t.Fatalf("step 4 failed: error code %d", resp3.ErrorCode)
	}

	member3ID := *resp3.MemberID
	partitions3 := countPartitions(resp3.Assignment)
	t.Logf("Step 4: Member 3 joined with %d partitions", partitions3)

	// Step 5: Member 2 leaves
	leaveReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	leaveReq.Group = groupID
	leaveReq.MemberID = member2ID
	leaveReq.MemberEpoch = -1

	leaveResp := handler.Handle(ctx, 0, leaveReq)
	if leaveResp.ErrorCode != 0 {
		t.Fatalf("step 5 leave failed: error code %d", leaveResp.ErrorCode)
	}
	t.Logf("Step 5: Member 2 left")

	// Step 6: Remaining members heartbeat to get final assignments
	hb1Final := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb1Final.Group = groupID
	hb1Final.MemberID = member1ID
	hb1Final.MemberEpoch = member1Epoch

	hbResp1Final := handler.Handle(ctx, 0, hb1Final)
	if hbResp1Final.ErrorCode != 0 {
		t.Errorf("step 6 member 1 heartbeat failed: error code %d", hbResp1Final.ErrorCode)
	}

	hb3 := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb3.Group = groupID
	hb3.MemberID = member3ID
	hb3.MemberEpoch = resp3.MemberEpoch

	hbResp3 := handler.Handle(ctx, 0, hb3)
	if hbResp3.ErrorCode != 0 {
		t.Errorf("step 6 member 3 heartbeat failed: error code %d", hbResp3.ErrorCode)
	}

	finalP1 := countPartitions(hbResp1Final.Assignment)
	finalP3 := countPartitions(hbResp3.Assignment)
	t.Logf("Step 6: Final - Member 1 has %d, Member 3 has %d partitions", finalP1, finalP3)

	// With 6 partitions and 2 members, each should get 3
	if finalP1+finalP3 != int(numPartitions) {
		t.Errorf("expected total %d partitions, got %d", numPartitions, finalP1+finalP3)
	}

	// Verify only 2 members remain
	members, _ := groupStore.ListMembers(ctx, groupID)
	if len(members) != 2 {
		t.Errorf("expected 2 members after full cycle, got %d", len(members))
	}

	t.Log("KIP-848 full cycle test passed")
}

// countPartitions counts the total number of partitions in an assignment.
func countPartitions(assignment *kmsg.ConsumerGroupHeartbeatResponseAssignment) int {
	if assignment == nil {
		return 0
	}
	count := 0
	for _, topic := range assignment.Topics {
		count += len(topic.Partitions)
	}
	return count
}
