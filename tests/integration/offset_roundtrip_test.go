package integration

import (
	"context"
	"testing"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestOffsetRoundtrip_BasicCommitAndFetch tests that an offset committed for a
// partition can be fetched back with the same value.
func TestOffsetRoundtrip_BasicCommitAndFetch(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	commitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	groupID := "roundtrip-test-group"
	topicName := "roundtrip-test-topic"
	partition := int32(0)
	offset := int64(12345)

	// Step 1: Commit offset for partition
	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.SetVersion(0)
	commitReq.Group = groupID
	commitReq.Generation = -1 // Simple mode (no group membership required)
	commitReq.MemberID = ""

	commitTopic := kmsg.NewOffsetCommitRequestTopic()
	commitTopic.Topic = topicName

	commitPartition := kmsg.NewOffsetCommitRequestTopicPartition()
	commitPartition.Partition = partition
	commitPartition.Offset = offset

	commitTopic.Partitions = append(commitTopic.Partitions, commitPartition)
	commitReq.Topics = append(commitReq.Topics, commitTopic)

	commitResp := commitHandler.Handle(ctx, 0, commitReq)

	// Verify commit succeeded
	if len(commitResp.Topics) == 0 {
		t.Fatal("expected topics in commit response")
	}
	if len(commitResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in commit response")
	}
	if commitResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit failed with error code %d", commitResp.Topics[0].Partitions[0].ErrorCode)
	}

	t.Logf("Step 1 passed: offset %d committed for %s/%d", offset, topicName, partition)

	// Step 2: Fetch offset for same partition
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.SetVersion(0)
	fetchReq.Group = groupID

	fetchTopic := kmsg.NewOffsetFetchRequestTopic()
	fetchTopic.Topic = topicName
	fetchTopic.Partitions = []int32{partition}
	fetchReq.Topics = append(fetchReq.Topics, fetchTopic)

	fetchResp := fetchHandler.Handle(ctx, 0, fetchReq)

	// Step 3: Verify offset matches committed value
	if len(fetchResp.Topics) == 0 {
		t.Fatal("expected topics in fetch response")
	}
	if len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in fetch response")
	}

	fetchedPartition := fetchResp.Topics[0].Partitions[0]
	if fetchedPartition.ErrorCode != 0 {
		t.Fatalf("fetch failed with error code %d", fetchedPartition.ErrorCode)
	}
	if fetchedPartition.Offset != offset {
		t.Errorf("expected offset %d, got %d", offset, fetchedPartition.Offset)
	}

	t.Logf("Step 2-3 passed: fetched offset %d matches committed offset %d", fetchedPartition.Offset, offset)
}

// TestOffsetRoundtrip_WithMetadata tests that offset metadata is preserved
// through commit and fetch operations.
func TestOffsetRoundtrip_WithMetadata(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	commitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	groupID := "metadata-test-group"
	topicName := "metadata-test-topic"
	partition := int32(5)
	offset := int64(98765)
	metadataStr := "consumer-client-1/checkpoint-2025-12-28"

	// Step 1: Commit offset with metadata
	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.SetVersion(0)
	commitReq.Group = groupID
	commitReq.Generation = -1
	commitReq.MemberID = ""

	commitTopic := kmsg.NewOffsetCommitRequestTopic()
	commitTopic.Topic = topicName

	commitPartition := kmsg.NewOffsetCommitRequestTopicPartition()
	commitPartition.Partition = partition
	commitPartition.Offset = offset
	commitPartition.Metadata = &metadataStr

	commitTopic.Partitions = append(commitTopic.Partitions, commitPartition)
	commitReq.Topics = append(commitReq.Topics, commitTopic)

	commitResp := commitHandler.Handle(ctx, 0, commitReq)

	if len(commitResp.Topics) == 0 || len(commitResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in commit response")
	}
	if commitResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit with metadata failed: error code %d", commitResp.Topics[0].Partitions[0].ErrorCode)
	}

	t.Logf("Step 1 passed: offset %d committed with metadata '%s'", offset, metadataStr)

	// Step 2: Fetch offset and verify metadata is returned
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.SetVersion(0)
	fetchReq.Group = groupID

	fetchTopic := kmsg.NewOffsetFetchRequestTopic()
	fetchTopic.Topic = topicName
	fetchTopic.Partitions = []int32{partition}
	fetchReq.Topics = append(fetchReq.Topics, fetchTopic)

	fetchResp := fetchHandler.Handle(ctx, 0, fetchReq)

	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in fetch response")
	}

	fetchedPartition := fetchResp.Topics[0].Partitions[0]
	if fetchedPartition.ErrorCode != 0 {
		t.Fatalf("fetch failed: error code %d", fetchedPartition.ErrorCode)
	}
	if fetchedPartition.Offset != offset {
		t.Errorf("offset mismatch: expected %d, got %d", offset, fetchedPartition.Offset)
	}
	if fetchedPartition.Metadata == nil {
		t.Error("expected metadata to be returned")
	} else if *fetchedPartition.Metadata != metadataStr {
		t.Errorf("metadata mismatch: expected '%s', got '%s'", metadataStr, *fetchedPartition.Metadata)
	}

	t.Logf("Step 2 passed: fetched offset %d with metadata '%s'", fetchedPartition.Offset, *fetchedPartition.Metadata)
}

// TestOffsetRoundtrip_MultiplePartitions tests committing and fetching offsets
// for multiple partitions of the same topic.
func TestOffsetRoundtrip_MultiplePartitions(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	commitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	groupID := "multi-partition-test-group"
	topicName := "multi-partition-test-topic"

	// Commit offsets for partitions 0, 1, 2
	partitionOffsets := map[int32]int64{
		0: 100,
		1: 200,
		2: 300,
	}

	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.SetVersion(0)
	commitReq.Group = groupID
	commitReq.Generation = -1
	commitReq.MemberID = ""

	commitTopic := kmsg.NewOffsetCommitRequestTopic()
	commitTopic.Topic = topicName

	for partition, offset := range partitionOffsets {
		commitPartition := kmsg.NewOffsetCommitRequestTopicPartition()
		commitPartition.Partition = partition
		commitPartition.Offset = offset
		commitTopic.Partitions = append(commitTopic.Partitions, commitPartition)
	}
	commitReq.Topics = append(commitReq.Topics, commitTopic)

	commitResp := commitHandler.Handle(ctx, 0, commitReq)

	// Verify all commits succeeded
	if len(commitResp.Topics) == 0 {
		t.Fatal("expected topics in commit response")
	}
	for _, p := range commitResp.Topics[0].Partitions {
		if p.ErrorCode != 0 {
			t.Errorf("commit for partition %d failed: error code %d", p.Partition, p.ErrorCode)
		}
	}

	t.Logf("Committed offsets for %d partitions", len(partitionOffsets))

	// Fetch offsets for all partitions
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.SetVersion(0)
	fetchReq.Group = groupID

	fetchTopic := kmsg.NewOffsetFetchRequestTopic()
	fetchTopic.Topic = topicName
	fetchTopic.Partitions = []int32{0, 1, 2}
	fetchReq.Topics = append(fetchReq.Topics, fetchTopic)

	fetchResp := fetchHandler.Handle(ctx, 0, fetchReq)

	if len(fetchResp.Topics) == 0 {
		t.Fatal("expected topics in fetch response")
	}

	// Verify all fetched offsets match
	for _, p := range fetchResp.Topics[0].Partitions {
		if p.ErrorCode != 0 {
			t.Errorf("fetch for partition %d failed: error code %d", p.Partition, p.ErrorCode)
			continue
		}
		expected, ok := partitionOffsets[p.Partition]
		if !ok {
			t.Errorf("unexpected partition %d in response", p.Partition)
			continue
		}
		if p.Offset != expected {
			t.Errorf("partition %d: expected offset %d, got %d", p.Partition, expected, p.Offset)
		}
	}

	t.Log("Multiple partition offset roundtrip test passed")
}

// TestOffsetRoundtrip_UpdateOffset tests that committing a new offset for the
// same partition updates the stored value.
func TestOffsetRoundtrip_UpdateOffset(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	commitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	groupID := "update-test-group"
	topicName := "update-test-topic"
	partition := int32(0)
	initialOffset := int64(1000)
	updatedOffset := int64(2000)

	// Commit initial offset
	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.SetVersion(0)
	commitReq.Group = groupID
	commitReq.Generation = -1
	commitReq.MemberID = ""

	commitTopic := kmsg.NewOffsetCommitRequestTopic()
	commitTopic.Topic = topicName

	commitPartition := kmsg.NewOffsetCommitRequestTopicPartition()
	commitPartition.Partition = partition
	commitPartition.Offset = initialOffset
	commitTopic.Partitions = append(commitTopic.Partitions, commitPartition)
	commitReq.Topics = append(commitReq.Topics, commitTopic)

	commitHandler.Handle(ctx, 0, commitReq)

	// Verify initial offset
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.SetVersion(0)
	fetchReq.Group = groupID
	fetchTopic := kmsg.NewOffsetFetchRequestTopic()
	fetchTopic.Topic = topicName
	fetchTopic.Partitions = []int32{partition}
	fetchReq.Topics = append(fetchReq.Topics, fetchTopic)

	fetchResp := fetchHandler.Handle(ctx, 0, fetchReq)
	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in fetch response")
	}
	if fetchResp.Topics[0].Partitions[0].Offset != initialOffset {
		t.Fatalf("initial offset mismatch: expected %d, got %d",
			initialOffset, fetchResp.Topics[0].Partitions[0].Offset)
	}

	t.Logf("Initial offset %d committed and verified", initialOffset)

	// Commit updated offset
	commitReq2 := kmsg.NewPtrOffsetCommitRequest()
	commitReq2.SetVersion(0)
	commitReq2.Group = groupID
	commitReq2.Generation = -1
	commitReq2.MemberID = ""

	commitTopic2 := kmsg.NewOffsetCommitRequestTopic()
	commitTopic2.Topic = topicName

	commitPartition2 := kmsg.NewOffsetCommitRequestTopicPartition()
	commitPartition2.Partition = partition
	commitPartition2.Offset = updatedOffset
	commitTopic2.Partitions = append(commitTopic2.Partitions, commitPartition2)
	commitReq2.Topics = append(commitReq2.Topics, commitTopic2)

	commitHandler.Handle(ctx, 0, commitReq2)

	// Verify updated offset
	fetchReq2 := kmsg.NewPtrOffsetFetchRequest()
	fetchReq2.SetVersion(0)
	fetchReq2.Group = groupID
	fetchTopic2 := kmsg.NewOffsetFetchRequestTopic()
	fetchTopic2.Topic = topicName
	fetchTopic2.Partitions = []int32{partition}
	fetchReq2.Topics = append(fetchReq2.Topics, fetchTopic2)

	fetchResp2 := fetchHandler.Handle(ctx, 0, fetchReq2)
	if len(fetchResp2.Topics) == 0 || len(fetchResp2.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in fetch response")
	}
	if fetchResp2.Topics[0].Partitions[0].Offset != updatedOffset {
		t.Errorf("updated offset mismatch: expected %d, got %d",
			updatedOffset, fetchResp2.Topics[0].Partitions[0].Offset)
	}

	t.Logf("Offset updated from %d to %d and verified", initialOffset, updatedOffset)
}

// TestOffsetRoundtrip_UncommittedPartition tests that fetching an offset for
// a partition that has never had an offset committed returns -1.
func TestOffsetRoundtrip_UncommittedPartition(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	groupID := "uncommitted-test-group"
	topicName := "uncommitted-test-topic"
	partition := int32(0)

	// Fetch offset for partition that has no committed offset
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.SetVersion(0)
	fetchReq.Group = groupID

	fetchTopic := kmsg.NewOffsetFetchRequestTopic()
	fetchTopic.Topic = topicName
	fetchTopic.Partitions = []int32{partition}
	fetchReq.Topics = append(fetchReq.Topics, fetchTopic)

	fetchResp := fetchHandler.Handle(ctx, 0, fetchReq)

	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in fetch response")
	}

	fetchedPartition := fetchResp.Topics[0].Partitions[0]

	// No committed offset should return -1 with no error
	if fetchedPartition.ErrorCode != 0 {
		t.Errorf("expected no error for uncommitted partition, got error code %d", fetchedPartition.ErrorCode)
	}
	if fetchedPartition.Offset != -1 {
		t.Errorf("expected offset -1 for uncommitted partition, got %d", fetchedPartition.Offset)
	}

	t.Log("Uncommitted partition correctly returns offset -1")
}

// TestOffsetRoundtrip_MultipleGroups tests that offsets for different groups
// are stored independently.
func TestOffsetRoundtrip_MultipleGroups(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	commitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	group1 := "multi-group-test-1"
	group2 := "multi-group-test-2"
	topicName := "multi-group-test-topic"
	partition := int32(0)
	offset1 := int64(111)
	offset2 := int64(222)

	// Commit different offsets for two different groups
	for _, tc := range []struct {
		group  string
		offset int64
	}{
		{group1, offset1},
		{group2, offset2},
	} {
		commitReq := kmsg.NewPtrOffsetCommitRequest()
		commitReq.SetVersion(0)
		commitReq.Group = tc.group
		commitReq.Generation = -1
		commitReq.MemberID = ""

		commitTopic := kmsg.NewOffsetCommitRequestTopic()
		commitTopic.Topic = topicName

		commitPartition := kmsg.NewOffsetCommitRequestTopicPartition()
		commitPartition.Partition = partition
		commitPartition.Offset = tc.offset
		commitTopic.Partitions = append(commitTopic.Partitions, commitPartition)
		commitReq.Topics = append(commitReq.Topics, commitTopic)

		commitHandler.Handle(ctx, 0, commitReq)
	}

	// Fetch and verify each group's offset is independent
	for _, tc := range []struct {
		group          string
		expectedOffset int64
	}{
		{group1, offset1},
		{group2, offset2},
	} {
		fetchReq := kmsg.NewPtrOffsetFetchRequest()
		fetchReq.SetVersion(0)
		fetchReq.Group = tc.group

		fetchTopic := kmsg.NewOffsetFetchRequestTopic()
		fetchTopic.Topic = topicName
		fetchTopic.Partitions = []int32{partition}
		fetchReq.Topics = append(fetchReq.Topics, fetchTopic)

		fetchResp := fetchHandler.Handle(ctx, 0, fetchReq)
		if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
			t.Fatalf("expected partitions in fetch response for group %s", tc.group)
		}

		fetchedOffset := fetchResp.Topics[0].Partitions[0].Offset
		if fetchedOffset != tc.expectedOffset {
			t.Errorf("group %s: expected offset %d, got %d",
				tc.group, tc.expectedOffset, fetchedOffset)
		}
	}

	t.Log("Multiple groups have independent offsets")
}

// TestOffsetRoundtrip_LeaderEpochV5Plus tests that leader epoch is preserved
// through commit and fetch operations for v5+ protocol versions.
func TestOffsetRoundtrip_LeaderEpochV5Plus(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	commitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	groupID := "leader-epoch-test-group"
	topicName := "leader-epoch-test-topic"
	partition := int32(0)
	offset := int64(54321)
	leaderEpoch := int32(7)

	// Commit offset with leader epoch (v6+)
	commitReq := kmsg.NewPtrOffsetCommitRequest()
	commitReq.SetVersion(6)
	commitReq.Group = groupID
	commitReq.Generation = -1
	commitReq.MemberID = ""

	commitTopic := kmsg.NewOffsetCommitRequestTopic()
	commitTopic.Topic = topicName

	commitPartition := kmsg.NewOffsetCommitRequestTopicPartition()
	commitPartition.Partition = partition
	commitPartition.Offset = offset
	commitPartition.LeaderEpoch = leaderEpoch
	commitTopic.Partitions = append(commitTopic.Partitions, commitPartition)
	commitReq.Topics = append(commitReq.Topics, commitTopic)

	commitResp := commitHandler.Handle(ctx, 6, commitReq)

	if len(commitResp.Topics) == 0 || len(commitResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in commit response")
	}
	if commitResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit failed with error code %d", commitResp.Topics[0].Partitions[0].ErrorCode)
	}

	// Fetch offset with v5 (which includes leader epoch in response)
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.SetVersion(5)
	fetchReq.Group = groupID

	fetchTopic := kmsg.NewOffsetFetchRequestTopic()
	fetchTopic.Topic = topicName
	fetchTopic.Partitions = []int32{partition}
	fetchReq.Topics = append(fetchReq.Topics, fetchTopic)

	fetchResp := fetchHandler.Handle(ctx, 5, fetchReq)

	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatal("expected partitions in fetch response")
	}

	fetchedPartition := fetchResp.Topics[0].Partitions[0]
	if fetchedPartition.ErrorCode != 0 {
		t.Fatalf("fetch failed with error code %d", fetchedPartition.ErrorCode)
	}
	if fetchedPartition.Offset != offset {
		t.Errorf("offset mismatch: expected %d, got %d", offset, fetchedPartition.Offset)
	}
	if fetchedPartition.LeaderEpoch != leaderEpoch {
		t.Errorf("leader epoch mismatch: expected %d, got %d", leaderEpoch, fetchedPartition.LeaderEpoch)
	}

	t.Logf("Leader epoch %d preserved through commit and fetch", leaderEpoch)
}

// TestOffsetRoundtrip_FetchAllTopics tests v2+ fetch-all behavior where
// an empty topics list returns all committed offsets for the group.
func TestOffsetRoundtrip_FetchAllTopics(t *testing.T) {
	ctx := context.Background()

	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	commitHandler := protocol.NewOffsetCommitHandler(groupStore, nil)
	fetchHandler := protocol.NewOffsetFetchHandler(groupStore, nil)

	groupID := "fetch-all-test-group"

	// Commit offsets for multiple topics
	topicOffsets := map[string]map[int32]int64{
		"topic-a": {0: 100, 1: 101},
		"topic-b": {0: 200},
		"topic-c": {0: 300, 1: 301, 2: 302},
	}

	for topicName, partitions := range topicOffsets {
		commitReq := kmsg.NewPtrOffsetCommitRequest()
		commitReq.SetVersion(0)
		commitReq.Group = groupID
		commitReq.Generation = -1
		commitReq.MemberID = ""

		commitTopic := kmsg.NewOffsetCommitRequestTopic()
		commitTopic.Topic = topicName

		for partition, offset := range partitions {
			commitPartition := kmsg.NewOffsetCommitRequestTopicPartition()
			commitPartition.Partition = partition
			commitPartition.Offset = offset
			commitTopic.Partitions = append(commitTopic.Partitions, commitPartition)
		}
		commitReq.Topics = append(commitReq.Topics, commitTopic)

		commitHandler.Handle(ctx, 0, commitReq)
	}

	totalCommitted := 0
	for _, partitions := range topicOffsets {
		totalCommitted += len(partitions)
	}
	t.Logf("Committed %d offsets across %d topics", totalCommitted, len(topicOffsets))

	// Fetch all topics (v2+ with empty topics list)
	fetchReq := kmsg.NewPtrOffsetFetchRequest()
	fetchReq.SetVersion(2)
	fetchReq.Group = groupID
	fetchReq.Topics = nil // Empty means fetch all

	fetchResp := fetchHandler.Handle(ctx, 2, fetchReq)

	// Count total partitions returned
	totalFetched := 0
	for _, topic := range fetchResp.Topics {
		totalFetched += len(topic.Partitions)
	}

	if totalFetched != totalCommitted {
		t.Errorf("expected %d partitions, got %d", totalCommitted, totalFetched)
	}

	// Verify values match
	for _, respTopic := range fetchResp.Topics {
		expectedPartitions, ok := topicOffsets[respTopic.Topic]
		if !ok {
			t.Errorf("unexpected topic %s in response", respTopic.Topic)
			continue
		}
		for _, respPartition := range respTopic.Partitions {
			expectedOffset, ok := expectedPartitions[respPartition.Partition]
			if !ok {
				t.Errorf("unexpected partition %d for topic %s", respPartition.Partition, respTopic.Topic)
				continue
			}
			if respPartition.Offset != expectedOffset {
				t.Errorf("topic %s partition %d: expected offset %d, got %d",
					respTopic.Topic, respPartition.Partition, expectedOffset, respPartition.Offset)
			}
		}
	}

	t.Log("Fetch all topics returned correct offsets")
}
