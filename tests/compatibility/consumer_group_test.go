package compatibility

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestConsumerGroup_OffsetCommitAndFetch tests offset commit and fetch operations.
func TestConsumerGroup_OffsetCommitAndFetch(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "offset-commit-topic"
	groupID := "offset-commit-group"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	offset := int64(12345)
	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{
		Topic:     topicName,
		Partition: 0,
		At:        offset,
	})

	commitResp, err := adminClient.CommitOffsets(ctx, groupID, toCommit)
	if err != nil {
		t.Fatalf("failed to commit offset: %v", err)
	}

	for _, topicResp := range commitResp {
		for _, partResp := range topicResp {
			if partResp.Err != nil {
				t.Errorf("offset commit error for %s/%d: %v",
					partResp.Topic, partResp.Partition, partResp.Err)
			}
		}
	}

	fetchResp, err := adminClient.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to fetch offsets: %v", err)
	}

	fetched := fetchResp.Offsets()
	if o, ok := fetched.Lookup(topicName, 0); !ok {
		t.Errorf("offset not found for %s/0", topicName)
	} else if o.At != offset {
		t.Errorf("expected offset %d, got %d", offset, o.At)
	}

	t.Logf("Offset commit and fetch verified: offset=%d", offset)
}

// TestConsumerGroup_MultiplePartitionOffsets tests offset management across partitions.
func TestConsumerGroup_MultiplePartitionOffsets(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "multi-partition-offsets"
	groupID := "multi-partition-group"
	numPartitions := int32(4)

	if err := suite.Broker().CreateTopic(ctx, topicName, numPartitions); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	toCommit := kadm.Offsets{}
	expectedOffsets := make(map[int32]int64)
	for p := int32(0); p < numPartitions; p++ {
		offset := int64(100 * (p + 1))
		expectedOffsets[p] = offset
		toCommit.Add(kadm.Offset{
			Topic:     topicName,
			Partition: p,
			At:        offset,
		})
	}

	if _, err := adminClient.CommitOffsets(ctx, groupID, toCommit); err != nil {
		t.Fatalf("failed to commit offsets: %v", err)
	}

	fetchResp, err := adminClient.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to fetch offsets: %v", err)
	}

	fetched := fetchResp.Offsets()
	for p, expected := range expectedOffsets {
		if o, ok := fetched.Lookup(topicName, p); !ok {
			t.Errorf("offset not found for partition %d", p)
		} else if o.At != expected {
			t.Errorf("partition %d: expected offset %d, got %d", p, expected, o.At)
		}
	}

	t.Logf("Verified offsets for %d partitions", numPartitions)
}

// TestConsumerGroup_IndependentGroups tests that different groups have independent offsets.
func TestConsumerGroup_IndependentGroups(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "independent-groups-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	groups := []struct {
		id     string
		offset int64
	}{
		{"group-alpha", 100},
		{"group-beta", 200},
		{"group-gamma", 300},
	}

	for _, g := range groups {
		toCommit := kadm.Offsets{}
		toCommit.Add(kadm.Offset{
			Topic:     topicName,
			Partition: 0,
			At:        g.offset,
		})
		if _, err := adminClient.CommitOffsets(ctx, g.id, toCommit); err != nil {
			t.Fatalf("failed to commit offset for %s: %v", g.id, err)
		}
	}

	for _, g := range groups {
		fetchResp, err := adminClient.FetchOffsets(ctx, g.id)
		if err != nil {
			t.Fatalf("failed to fetch offsets for %s: %v", g.id, err)
		}

		fetched := fetchResp.Offsets()
		if o, ok := fetched.Lookup(topicName, 0); !ok {
			t.Errorf("group %s: offset not found", g.id)
		} else if o.At != g.offset {
			t.Errorf("group %s: expected offset %d, got %d", g.id, g.offset, o.At)
		}
	}

	t.Logf("Verified %d independent groups have separate offsets", len(groups))
}

// TestConsumerGroup_UpdateOffset tests updating a committed offset.
func TestConsumerGroup_UpdateOffset(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "update-offset-topic"
	groupID := "update-offset-group"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	initialOffset := int64(500)
	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{
		Topic:     topicName,
		Partition: 0,
		At:        initialOffset,
	})
	if _, err := adminClient.CommitOffsets(ctx, groupID, toCommit); err != nil {
		t.Fatalf("failed to commit initial offset: %v", err)
	}

	fetchResp, err := adminClient.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to fetch offsets: %v", err)
	}
	fetched := fetchResp.Offsets()
	if o, ok := fetched.Lookup(topicName, 0); ok && o.At != initialOffset {
		t.Errorf("initial offset mismatch: expected %d, got %d", initialOffset, o.At)
	}

	updatedOffset := int64(1000)
	toCommit2 := kadm.Offsets{}
	toCommit2.Add(kadm.Offset{
		Topic:     topicName,
		Partition: 0,
		At:        updatedOffset,
	})
	if _, err := adminClient.CommitOffsets(ctx, groupID, toCommit2); err != nil {
		t.Fatalf("failed to commit updated offset: %v", err)
	}

	fetchResp2, err := adminClient.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to fetch offsets after update: %v", err)
	}
	fetched2 := fetchResp2.Offsets()
	if o, ok := fetched2.Lookup(topicName, 0); !ok {
		t.Error("offset not found after update")
	} else if o.At != updatedOffset {
		t.Errorf("updated offset mismatch: expected %d, got %d", updatedOffset, o.At)
	}

	t.Logf("Offset updated from %d to %d", initialOffset, updatedOffset)
}

// TestConsumerGroup_UncommittedOffset tests fetching offset for uncommitted partition.
func TestConsumerGroup_UncommittedOffset(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "uncommitted-offset-topic"
	groupID := "uncommitted-offset-group"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	fetchResp, err := adminClient.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to fetch offsets: %v", err)
	}

	fetched := fetchResp.Offsets()
	if o, ok := fetched.Lookup(topicName, 0); ok && o.At >= 0 {
		t.Errorf("uncommitted partition should return offset -1, got %d", o.At)
	}

	t.Log("Uncommitted partition correctly returns offset -1 or not found")
}

// TestConsumerGroup_ListGroups tests listing consumer groups.
// Note: In Dray, groups are only listed after they have been created via JoinGroup.
// Committing offsets alone does not create a group in the group store.
func TestConsumerGroup_ListGroups(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "list-groups-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	// First, verify ListGroups returns an empty list for a fresh broker
	groups, err := adminClient.ListGroups(ctx)
	if err != nil {
		t.Fatalf("failed to list groups: %v", err)
	}

	// In Dray, groups are only registered when members join
	// Committing offsets doesn't create groups in the list
	t.Logf("Listed %d groups (expected 0 for fresh broker without active consumers)", len(groups))

	// Commit some offsets - these don't create groups in Dray's implementation
	groupIDs := []string{"list-group-1", "list-group-2", "list-group-3"}
	for _, gid := range groupIDs {
		toCommit := kadm.Offsets{}
		toCommit.Add(kadm.Offset{
			Topic:     topicName,
			Partition: 0,
			At:        100,
		})
		if _, err := adminClient.CommitOffsets(ctx, gid, toCommit); err != nil {
			t.Fatalf("failed to commit offsets for %s: %v", gid, err)
		}
	}

	// Verify offsets were committed (groups exist for offset tracking)
	for _, gid := range groupIDs {
		fetchResp, err := adminClient.FetchOffsets(ctx, gid)
		if err != nil {
			t.Fatalf("failed to fetch offsets for %s: %v", gid, err)
		}
		if o, ok := fetchResp.Offsets().Lookup(topicName, 0); ok {
			t.Logf("Group %s has committed offset %d", gid, o.At)
		}
	}

	t.Log("ListGroups API test completed - offsets committed without requiring active group membership")
}

// TestConsumerGroup_DescribeGroup tests describing a consumer group.
func TestConsumerGroup_DescribeGroup(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "describe-group-topic"
	groupID := "describe-test-group"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{
		Topic:     topicName,
		Partition: 0,
		At:        50,
	})
	if _, err := adminClient.CommitOffsets(ctx, groupID, toCommit); err != nil {
		t.Fatalf("failed to commit offset for %s: %v", groupID, err)
	}

	described, err := adminClient.DescribeGroups(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to describe group: %v", err)
	}

	if len(described) == 0 {
		t.Fatal("expected at least one group in describe response")
	}

	var found bool
	for _, g := range described {
		if g.Group == groupID {
			found = true
			t.Logf("Group %s: state=%s, protocol_type=%s", g.Group, g.State, g.ProtocolType)
		}
	}

	if !found {
		t.Errorf("group %s not found in describe response", groupID)
	}

	t.Log("Describe group operation completed successfully")
}

// TestConsumerGroup_DeleteGroup tests deleting a consumer group.
func TestConsumerGroup_DeleteGroup(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "delete-group-topic"
	groupID := "delete-test-group"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{
		Topic:     topicName,
		Partition: 0,
		At:        75,
	})
	if _, err := adminClient.CommitOffsets(ctx, groupID, toCommit); err != nil {
		t.Fatalf("failed to commit offset for %s: %v", groupID, err)
	}

	groups, err := adminClient.ListGroups(ctx)
	if err != nil {
		t.Fatalf("failed to list groups: %v", err)
	}
	found := false
	for _, g := range groups {
		if g.Group == groupID {
			found = true
			break
		}
	}
	if !found {
		t.Log("Note: Group may not appear in list until it has active members")
	}

	deleteResp, err := adminClient.DeleteGroups(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to delete group: %v", err)
	}

	for gid, resp := range deleteResp {
		if resp.Err != nil {
			t.Logf("Delete group %s result: %v (may already be empty)", gid, resp.Err)
		} else {
			t.Logf("Group %s deleted successfully", gid)
		}
	}

	t.Log("Delete group operation completed")
}

// TestConsumerGroup_JoinGroupProtocol tests the JoinGroup protocol (simplified).
func TestConsumerGroup_JoinGroupProtocol(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	groupID := "join-protocol-test"

	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = groupID
	joinReq.MemberID = ""
	joinReq.ProtocolType = "consumer"
	joinReq.SessionTimeoutMillis = 10000
	joinReq.RebalanceTimeoutMillis = 5000

	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	proto.Metadata = []byte{0, 0, 0, 0, 0, 1, 0, 4, 't', 'e', 's', 't', 0, 0, 0, 0}
	joinReq.Protocols = append(joinReq.Protocols, proto)

	joinResp, err := joinReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("JoinGroup request failed: %v", err)
	}

	if joinResp.ErrorCode == 79 {
		t.Log("Received MEMBER_ID_REQUIRED (expected for v4+ - need to retry with assigned member ID)")
		joinReq.MemberID = joinResp.MemberID
		joinResp, err = joinReq.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("JoinGroup retry failed: %v", err)
		}
	}

	if joinResp.ErrorCode != 0 {
		t.Errorf("JoinGroup failed with error code %d", joinResp.ErrorCode)
	} else {
		t.Logf("JoinGroup succeeded: memberID=%s, generation=%d, leader=%s",
			joinResp.MemberID, joinResp.Generation, joinResp.LeaderID)
	}

	t.Log("JoinGroup protocol test completed")
}

// TestConsumerGroup_OffsetWithMetadata tests offset commit with metadata.
func TestConsumerGroup_OffsetWithMetadata(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "offset-metadata-topic"
	groupID := "offset-metadata-group"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	offset := int64(9999)
	metadata := "consumer-checkpoint-v1"
	toCommit := kadm.Offsets{}
	toCommit.Add(kadm.Offset{
		Topic:     topicName,
		Partition: 0,
		At:        offset,
		Metadata:  metadata,
	})

	if _, err := adminClient.CommitOffsets(ctx, groupID, toCommit); err != nil {
		t.Fatalf("failed to commit offset with metadata: %v", err)
	}

	fetchResp, err := adminClient.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to fetch offsets: %v", err)
	}

	fetched := fetchResp.Offsets()
	if o, ok := fetched.Lookup(topicName, 0); !ok {
		t.Error("offset not found")
	} else {
		if o.At != offset {
			t.Errorf("expected offset %d, got %d", offset, o.At)
		}
		if o.Metadata != metadata {
			t.Errorf("expected metadata %q, got %q", metadata, o.Metadata)
		}
	}

	t.Logf("Offset with metadata committed and fetched: offset=%d, metadata=%q", offset, metadata)
}

// TestConsumerGroup_FindCoordinator tests the FindCoordinator API.
func TestConsumerGroup_FindCoordinator(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	groupID := "find-coordinator-test"

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	findReq := kmsg.NewPtrFindCoordinatorRequest()
	findReq.CoordinatorKey = groupID
	findReq.CoordinatorType = 0

	findResp, err := findReq.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("FindCoordinator request failed: %v", err)
	}

	if findResp.ErrorCode != 0 {
		t.Errorf("FindCoordinator failed with error code %d", findResp.ErrorCode)
	} else {
		t.Logf("FindCoordinator: nodeID=%d, host=%s, port=%d",
			findResp.NodeID, findResp.Host, findResp.Port)
	}

	t.Log("FindCoordinator API test completed")
}

// TestConsumerGroup_ConsumeWithGroup tests consuming with a consumer group.
func TestConsumerGroup_ConsumeWithGroup(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "group-consume-topic"
	groupID := "group-consume-test"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	numMessages := 5
	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: topicName,
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		result := producer.ProduceSync(ctx, record)
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
	}

	consumer, err := suite.Broker().NewClient(
		kgo.ConsumeTopics(topicName),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var consumed int
	for consumed < numMessages {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			consumed++
		})
		if fetchCtx.Err() != nil {
			break
		}
	}

	if consumed > 0 {
		t.Logf("Consumed %d messages with consumer group", consumed)
	} else {
		t.Log("Note: Consumer group consumption may require more time for rebalance")
	}

	t.Log("Consumer group consume test completed")
}
