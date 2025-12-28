package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestConsumerGroupDescribeHandler_Handle(t *testing.T) {
	ctx := context.Background()

	t.Run("success - describe single consumer group", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create group: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			ClientHost:       "/127.0.0.1",
			MemberEpoch:      1,
			RackID:           "us-east-1a",
			SubscribedTopics: []string{"topic1", "topic2"},
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}
		req.IncludeAuthorizedOperations = true

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}

		group := resp.Groups[0]
		if group.ErrorCode != errCGDNone {
			t.Errorf("expected error code %d, got %d", errCGDNone, group.ErrorCode)
		}
		if group.Group != "test-group" {
			t.Errorf("expected group ID test-group, got %s", group.Group)
		}
		if group.State != string(groups.GroupStateEmpty) {
			t.Errorf("expected state Empty, got %s", group.State)
		}

		if len(group.Members) != 1 {
			t.Fatalf("expected 1 member, got %d", len(group.Members))
		}

		member := group.Members[0]
		if member.MemberID != "member-1" {
			t.Errorf("expected member ID member-1, got %s", member.MemberID)
		}
		if member.MemberEpoch != 1 {
			t.Errorf("expected member epoch 1, got %d", member.MemberEpoch)
		}
		if member.ClientID != "client-1" {
			t.Errorf("expected client ID client-1, got %s", member.ClientID)
		}
		if member.ClientHost != "/127.0.0.1" {
			t.Errorf("expected client host /127.0.0.1, got %s", member.ClientHost)
		}
		if member.RackID == nil || *member.RackID != "us-east-1a" {
			t.Errorf("expected rack ID us-east-1a, got %v", member.RackID)
		}
		if len(member.SubscribedTopics) != 2 {
			t.Errorf("expected 2 subscribed topics, got %d", len(member.SubscribedTopics))
		}

		// Check authorized operations are included
		if group.AuthorizedOperations != 0x500 {
			t.Errorf("expected authorized operations 0x500, got 0x%x", group.AuthorizedOperations)
		}
	})

	t.Run("success - describe multiple groups", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		for _, groupID := range []string{"group-1", "group-2"} {
			_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
				GroupID:      groupID,
				Type:         groups.GroupTypeConsumer,
				ProtocolType: "consumer",
				NowMs:        nowMs,
			})
			if err != nil {
				t.Fatalf("failed to create group %s: %v", groupID, err)
			}
		}

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"group-1", "group-2"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(resp.Groups))
		}

		for i, groupID := range []string{"group-1", "group-2"} {
			if resp.Groups[i].Group != groupID {
				t.Errorf("expected group ID %s, got %s", groupID, resp.Groups[i].Group)
			}
			if resp.Groups[i].ErrorCode != errCGDNone {
				t.Errorf("expected no error for %s, got %d", groupID, resp.Groups[i].ErrorCode)
			}
		}
	})

	t.Run("error - group not found", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"nonexistent-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}

		group := resp.Groups[0]
		if group.ErrorCode != errCGDGroupIDNotFound {
			t.Errorf("expected error code %d, got %d", errCGDGroupIDNotFound, group.ErrorCode)
		}
		if group.ErrorMessage == nil {
			t.Error("expected error message")
		}
	})

	t.Run("error - classic group not returned", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "classic-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("failed to create classic group: %v", err)
		}

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"classic-group"}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}

		group := resp.Groups[0]
		if group.ErrorCode != errCGDGroupIDNotFound {
			t.Errorf("expected error code %d for classic group, got %d", errCGDGroupIDNotFound, group.ErrorCode)
		}
	})

	t.Run("error - empty group ID", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{""}

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}

		if resp.Groups[0].ErrorCode != errCGDInvalidGroupID {
			t.Errorf("expected error code %d, got %d", errCGDInvalidGroupID, resp.Groups[0].ErrorCode)
		}
	})

	t.Run("success - without authorized operations", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}
		req.IncludeAuthorizedOperations = false

		resp := handler.Handle(ctx, 0, req)

		group := resp.Groups[0]
		if group.AuthorizedOperations != -2147483648 {
			t.Errorf("expected default authorized operations, got 0x%x", group.AuthorizedOperations)
		}
	})
}

func TestConsumerGroupDescribeHandler_MemberDetails(t *testing.T) {
	ctx := context.Background()

	t.Run("member with instance ID (static membership)", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		_, err := store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:         "test-group",
			MemberID:        "member-1",
			GroupInstanceID: "instance-1",
			MemberEpoch:     5,
			NowMs:           nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		member := resp.Groups[0].Members[0]
		if member.InstanceID == nil || *member.InstanceID != "instance-1" {
			t.Errorf("expected instance ID instance-1, got %v", member.InstanceID)
		}
		if member.MemberEpoch != 5 {
			t.Errorf("expected member epoch 5, got %d", member.MemberEpoch)
		}
	})

	t.Run("member without rack ID", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		_, err := store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:     "test-group",
			MemberID:    "member-1",
			MemberEpoch: 1,
			NowMs:       nowMs,
		})
		if err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		member := resp.Groups[0].Members[0]
		if member.RackID != nil {
			t.Errorf("expected nil rack ID, got %v", member.RackID)
		}
	})
}

func TestConsumerGroupDescribeHandler_GroupState(t *testing.T) {
	ctx := context.Background()

	t.Run("returns group epoch from generation", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		// Increment generation
		_, _ = store.IncrementGeneration(ctx, "test-group", nowMs)
		_, _ = store.IncrementGeneration(ctx, "test-group", nowMs)

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if resp.Groups[0].Epoch != 2 {
			t.Errorf("expected epoch 2, got %d", resp.Groups[0].Epoch)
		}
	})

	t.Run("returns default assignor when not set", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if resp.Groups[0].AssignorName != "uniform" {
			t.Errorf("expected assignor name uniform, got %s", resp.Groups[0].AssignorName)
		}
	})
}

func TestConsumerGroupDescribeHandler_WithLeaseManager(t *testing.T) {
	ctx := context.Background()

	t.Run("success - with lease", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")
		handler := NewConsumerGroupDescribeHandler(store, leaseManager)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if resp.Groups[0].ErrorCode != errCGDNone {
			t.Errorf("expected no error, got %d", resp.Groups[0].ErrorCode)
		}
	})

	t.Run("error - not coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")
		handler := NewConsumerGroupDescribeHandler(store, leaseManager)

		// Simulate another broker holding the lease
		_, _ = metaStore.Put(ctx, "/dray/v1/groups/test-group/lease", []byte(`{"brokerID":"broker-2","acquiredAt":0}`))

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if resp.Groups[0].ErrorCode != errCGDNotCoordinator {
			t.Errorf("expected error code %d, got %d", errCGDNotCoordinator, resp.Groups[0].ErrorCode)
		}
	})
}

func TestConsumerGroupDescribeHandler_Assignment(t *testing.T) {
	ctx := context.Background()

	t.Run("returns member assignment", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		_, _ = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:     "test-group",
			MemberID:    "member-1",
			MemberEpoch: 1,
			NowMs:       nowMs,
		})

		// Build assignment data in classic format
		// Version (int16) + NumTopics (int32) + TopicName + NumPartitions + Partitions
		assignData := buildTestAssignmentData(map[string][]int32{
			"topic1": {0, 1, 2},
			"topic2": {0},
		})

		_, _ = store.SetAssignment(ctx, groups.SetAssignmentRequest{
			GroupID:    "test-group",
			MemberID:   "member-1",
			Generation: 1,
			Data:       assignData,
			NowMs:      nowMs,
		})

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		member := resp.Groups[0].Members[0]
		if len(member.Assignment.TopicPartitions) == 0 {
			t.Error("expected assignment to have topic partitions")
		}

		// Find topic1 assignment
		var topic1Found bool
		for _, tp := range member.Assignment.TopicPartitions {
			if tp.Topic == "topic1" {
				topic1Found = true
				if len(tp.Partitions) != 3 {
					t.Errorf("expected 3 partitions for topic1, got %d", len(tp.Partitions))
				}
			}
		}
		if !topic1Found {
			t.Error("expected to find topic1 in assignment")
		}
	})

	t.Run("handles empty assignment", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		_, _ = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:     "test-group",
			MemberID:    "member-1",
			MemberEpoch: 1,
			NowMs:       nowMs,
		})

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		member := resp.Groups[0].Members[0]
		if len(member.Assignment.TopicPartitions) != 0 {
			t.Errorf("expected empty assignment, got %d topic partitions", len(member.Assignment.TopicPartitions))
		}
	})
}

func TestConsumerGroupDescribeHandler_ResponseVersion(t *testing.T) {
	ctx := context.Background()

	t.Run("version 0 response", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 0, req)

		if resp.GetVersion() != 0 {
			t.Errorf("expected version 0, got %d", resp.GetVersion())
		}
	})

	t.Run("version 1 response", func(t *testing.T) {
		store := newTestConsumerGroupStore()
		handler := NewConsumerGroupDescribeHandler(store, nil)

		nowMs := time.Now().UnixMilli()
		_, _ = store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeConsumer,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})

		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"test-group"}

		resp := handler.Handle(ctx, 1, req)

		if resp.GetVersion() != 1 {
			t.Errorf("expected version 1, got %d", resp.GetVersion())
		}
	})
}

func TestParseAssignmentData(t *testing.T) {
	t.Run("parses valid assignment", func(t *testing.T) {
		data := buildTestAssignmentData(map[string][]int32{
			"topic1": {0, 1},
			"topic2": {0, 1, 2},
		})

		result := parseAssignmentData(data)

		if len(result.TopicPartitions) != 2 {
			t.Fatalf("expected 2 topics, got %d", len(result.TopicPartitions))
		}
	})

	t.Run("handles empty data", func(t *testing.T) {
		result := parseAssignmentData(nil)
		if len(result.TopicPartitions) != 0 {
			t.Errorf("expected empty result, got %d topics", len(result.TopicPartitions))
		}
	})

	t.Run("handles truncated data", func(t *testing.T) {
		result := parseAssignmentData([]byte{0, 0})
		if len(result.TopicPartitions) != 0 {
			t.Errorf("expected empty result for truncated data")
		}
	})
}

// newTestConsumerGroupStore creates a test group store.
func newTestConsumerGroupStore() *groups.Store {
	return groups.NewStore(metadata.NewMockStore())
}

// buildTestAssignmentData builds assignment data in classic consumer protocol format.
func buildTestAssignmentData(topics map[string][]int32) []byte {
	// Calculate size
	size := 2 + 4 // version + numTopics
	for topic, partitions := range topics {
		size += 2 + len(topic) + 4 + len(partitions)*4
	}
	size += 4 // userData length (empty)

	data := make([]byte, size)
	offset := 0

	// Version
	data[offset] = 0
	data[offset+1] = 0
	offset += 2

	// NumTopics
	numTopics := int32(len(topics))
	data[offset] = byte(numTopics >> 24)
	data[offset+1] = byte(numTopics >> 16)
	data[offset+2] = byte(numTopics >> 8)
	data[offset+3] = byte(numTopics)
	offset += 4

	// Topics (sorted for determinism in tests)
	topicOrder := make([]string, 0, len(topics))
	for topic := range topics {
		topicOrder = append(topicOrder, topic)
	}
	// Sort the topics
	for i := 0; i < len(topicOrder)-1; i++ {
		for j := i + 1; j < len(topicOrder); j++ {
			if topicOrder[i] > topicOrder[j] {
				topicOrder[i], topicOrder[j] = topicOrder[j], topicOrder[i]
			}
		}
	}

	for _, topic := range topicOrder {
		partitions := topics[topic]

		// Topic name length
		data[offset] = byte(len(topic) >> 8)
		data[offset+1] = byte(len(topic))
		offset += 2

		// Topic name
		copy(data[offset:], topic)
		offset += len(topic)

		// Num partitions
		numParts := int32(len(partitions))
		data[offset] = byte(numParts >> 24)
		data[offset+1] = byte(numParts >> 16)
		data[offset+2] = byte(numParts >> 8)
		data[offset+3] = byte(numParts)
		offset += 4

		// Partitions
		for _, p := range partitions {
			data[offset] = byte(p >> 24)
			data[offset+1] = byte(p >> 16)
			data[offset+2] = byte(p >> 8)
			data[offset+3] = byte(p)
			offset += 4
		}
	}

	// UserData length (empty)
	data[offset] = 0
	data[offset+1] = 0
	data[offset+2] = 0
	data[offset+3] = 0

	return data
}
