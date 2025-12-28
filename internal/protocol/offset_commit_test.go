package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOffsetCommitHandler_Handle(t *testing.T) {
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	t.Run("success - simple mode (no group validation)", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = -1 // Simple mode
		req.MemberID = ""   // Simple mode

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"

		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		metadata := "test-metadata"
		partition.Metadata = &metadata

		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
		}
		if len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("expected 1 partition, got %d", len(resp.Topics[0].Partitions))
		}
		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}

		// Verify offset was stored
		offset, err := store.GetCommittedOffset(ctx, "test-group", "test-topic", 0)
		if err != nil {
			t.Fatalf("get committed offset: %v", err)
		}
		if offset == nil {
			t.Fatal("expected offset to be committed")
		}
		if offset.Offset != 100 {
			t.Errorf("expected offset 100, got %d", offset.Offset)
		}
		if offset.Metadata != "test-metadata" {
			t.Errorf("expected metadata 'test-metadata', got %q", offset.Metadata)
		}
	})

	t.Run("success - with group validation", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		// Create group and member
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"

		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100

		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatal("unexpected response structure")
		}
		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}

		// Verify offset was stored
		offset, err := store.GetCommittedOffset(ctx, "test-group", "test-topic", 0)
		if err != nil {
			t.Fatalf("get committed offset: %v", err)
		}
		if offset == nil || offset.Offset != 100 {
			t.Error("offset was not stored correctly")
		}
	})

	t.Run("success - multiple topics and partitions", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = -1
		req.MemberID = ""

		// Topic 1 with 2 partitions
		topic1 := kmsg.NewOffsetCommitRequestTopic()
		topic1.Topic = "topic-1"

		p1 := kmsg.NewOffsetCommitRequestTopicPartition()
		p1.Partition = 0
		p1.Offset = 100
		topic1.Partitions = append(topic1.Partitions, p1)

		p2 := kmsg.NewOffsetCommitRequestTopicPartition()
		p2.Partition = 1
		p2.Offset = 200
		topic1.Partitions = append(topic1.Partitions, p2)

		req.Topics = append(req.Topics, topic1)

		// Topic 2 with 1 partition
		topic2 := kmsg.NewOffsetCommitRequestTopic()
		topic2.Topic = "topic-2"

		p3 := kmsg.NewOffsetCommitRequestTopicPartition()
		p3.Partition = 0
		p3.Offset = 300
		topic2.Partitions = append(topic2.Partitions, p3)

		req.Topics = append(req.Topics, topic2)

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Topics) != 2 {
			t.Fatalf("expected 2 topics, got %d", len(resp.Topics))
		}
		if len(resp.Topics[0].Partitions) != 2 {
			t.Fatalf("expected 2 partitions in topic 0, got %d", len(resp.Topics[0].Partitions))
		}
		if len(resp.Topics[1].Partitions) != 1 {
			t.Fatalf("expected 1 partition in topic 1, got %d", len(resp.Topics[1].Partitions))
		}

		// All should succeed
		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				if partition.ErrorCode != errOffsetCommitNone {
					t.Errorf("topic %s partition %d: expected no error, got %d",
						topic.Topic, partition.Partition, partition.ErrorCode)
				}
			}
		}

		// Verify all offsets were stored
		offset1, _ := store.GetCommittedOffset(ctx, "test-group", "topic-1", 0)
		offset2, _ := store.GetCommittedOffset(ctx, "test-group", "topic-1", 1)
		offset3, _ := store.GetCommittedOffset(ctx, "test-group", "topic-2", 0)

		if offset1 == nil || offset1.Offset != 100 {
			t.Error("topic-1 partition 0 offset not stored correctly")
		}
		if offset2 == nil || offset2.Offset != 200 {
			t.Error("topic-1 partition 1 offset not stored correctly")
		}
		if offset3 == nil || offset3.Offset != 300 {
			t.Error("topic-2 partition 0 offset not stored correctly")
		}
	})

	t.Run("error - empty group ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = ""

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 0, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitInvalidGroupID {
			t.Errorf("expected INVALID_GROUP_ID (24), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - empty member ID for v1+", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1 // Not simple mode
		req.MemberID = ""  // But no member ID

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitUnknownMemberID {
			t.Errorf("expected UNKNOWN_MEMBER_ID (25), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - group not found", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "nonexistent-group"
		req.Generation = 1
		req.MemberID = "member-1"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitCoordinatorNotAvail {
			t.Errorf("expected COORDINATOR_NOT_AVAILABLE (15), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - member not found", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		// Create group but no member
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "nonexistent-member"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitUnknownMemberID {
			t.Errorf("expected UNKNOWN_MEMBER_ID (25), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - generation mismatch", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 99 // Wrong generation
		req.MemberID = "member-1"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitIllegalGeneration {
			t.Errorf("expected ILLEGAL_GENERATION (22), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - dead group", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID: "test-group",
			ToState: groups.GroupStateDead,
			NowMs:   nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 0
		req.MemberID = "member-1"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitUnknownMemberID {
			t.Errorf("expected UNKNOWN_MEMBER_ID (25), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - rebalance in progress (PreparingRebalance)", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStatePreparingRebalance,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitRebalanceInProgress {
			t.Errorf("expected REBALANCE_IN_PROGRESS (27), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - rebalance in progress (CompletingRebalance)", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateCompletingRebalance,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitRebalanceInProgress {
			t.Errorf("expected REBALANCE_IN_PROGRESS (27), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("v6+ leader epoch stored", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = -1
		req.MemberID = ""

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"

		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		partition.LeaderEpoch = 5

		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 6, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}

		offset, err := store.GetCommittedOffset(ctx, "test-group", "test-topic", 0)
		if err != nil {
			t.Fatalf("get committed offset: %v", err)
		}
		if offset.LeaderEpoch != 5 {
			t.Errorf("expected leader epoch 5, got %d", offset.LeaderEpoch)
		}
	})

	t.Run("v7+ instance ID validation - success", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		instanceID := "instance-1"
		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			GroupInstanceID:  instanceID,
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"
		req.InstanceID = &instanceID

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 7, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("v7+ instance ID validation - fenced", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			GroupInstanceID:  "instance-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		wrongInstanceID := "wrong-instance"
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"
		req.InstanceID = &wrongInstanceID

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 7, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitFencedInstanceID {
			t.Errorf("expected FENCED_INSTANCE_ID (82), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("v2-v4 retention time stored", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = -1
		req.MemberID = ""
		req.RetentionTimeMillis = 3600000 // 1 hour

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 2, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}

		offset, err := store.GetCommittedOffset(ctx, "test-group", "test-topic", 0)
		if err != nil {
			t.Fatalf("get committed offset: %v", err)
		}
		if offset.ExpireTimestamp == -1 {
			t.Error("expected expiry timestamp to be set")
		}
	})

	t.Run("v5+ retention time ignored", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = -1
		req.MemberID = ""
		req.RetentionTimeMillis = 3600000 // Should be ignored for v5+

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 5, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}

		offset, err := store.GetCommittedOffset(ctx, "test-group", "test-topic", 0)
		if err != nil {
			t.Fatalf("get committed offset: %v", err)
		}
		if offset.ExpireTimestamp != -1 {
			t.Errorf("expected no expiry (-1), got %d", offset.ExpireTimestamp)
		}
	})

	t.Run("version response formatting", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetCommitHandler(store, nil)

		for _, version := range []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
			req := kmsg.NewPtrOffsetCommitRequest()
			req.Group = "test-group"
			req.Generation = -1
			req.MemberID = ""

			topic := kmsg.NewOffsetCommitRequestTopic()
			topic.Topic = "test-topic"
			partition := kmsg.NewOffsetCommitRequestTopicPartition()
			partition.Partition = 0
			partition.Offset = 100
			topic.Partitions = append(topic.Partitions, partition)
			req.Topics = append(req.Topics, topic)

			resp := handler.Handle(ctx, version, req)

			if resp.GetVersion() != version {
				t.Errorf("version %d: expected version %d, got %d", version, version, resp.GetVersion())
			}
		}
	})
}

func TestOffsetCommitHandler_WithLeaseManager(t *testing.T) {
	ctx := context.Background()
	nowMs := time.Now().UnixMilli()

	t.Run("not coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewOffsetCommitHandler(store, leaseManager)

		// Simulate another broker holding the lease
		_, _ = metaStore.Put(ctx, "/dray/v1/groups/test-group/lease", []byte(`{"brokerID":"broker-2","acquiredAt":0}`))

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = -1
		req.MemberID = ""

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 0, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNotCoordinator {
			t.Errorf("expected NOT_COORDINATOR (16), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("is coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewOffsetCommitHandler(store, leaseManager)

		// Create group and member
		_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "test-group",
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("create group: %v", err)
		}

		_, err = store.TransitionState(ctx, groups.TransitionStateRequest{
			GroupID:      "test-group",
			ToState:      groups.GroupStateStable,
			IncrementGen: true,
			NowMs:        nowMs,
		})
		if err != nil {
			t.Fatalf("transition state: %v", err)
		}

		_, err = store.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "test-group",
			MemberID:         "member-1",
			ClientID:         "client-1",
			SessionTimeoutMs: 30000,
			NowMs:            nowMs,
		})
		if err != nil {
			t.Fatalf("add member: %v", err)
		}

		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = "test-group"
		req.Generation = 1
		req.MemberID = "member-1"

		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "test-topic"
		partition := kmsg.NewOffsetCommitRequestTopicPartition()
		partition.Partition = 0
		partition.Offset = 100
		topic.Partitions = append(topic.Partitions, partition)
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 1, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})
}

func TestOffsetCommit_KeyStoragePath(t *testing.T) {
	ctx := context.Background()

	store := newTestGroupStore()
	handler := NewOffsetCommitHandler(store, nil)

	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = "my-group"
	req.Generation = -1
	req.MemberID = ""

	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "my-topic"

	partition := kmsg.NewOffsetCommitRequestTopicPartition()
	partition.Partition = 3
	partition.Offset = 12345

	topic.Partitions = append(topic.Partitions, partition)
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 0, req)

	if resp.Topics[0].Partitions[0].ErrorCode != errOffsetCommitNone {
		t.Fatalf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}

	// Verify the offset was stored at the correct path
	offset, err := store.GetCommittedOffset(ctx, "my-group", "my-topic", 3)
	if err != nil {
		t.Fatalf("get committed offset: %v", err)
	}
	if offset == nil {
		t.Fatal("expected offset to be stored")
	}
	if offset.GroupID != "my-group" {
		t.Errorf("expected group 'my-group', got %q", offset.GroupID)
	}
	if offset.Topic != "my-topic" {
		t.Errorf("expected topic 'my-topic', got %q", offset.Topic)
	}
	if offset.Partition != 3 {
		t.Errorf("expected partition 3, got %d", offset.Partition)
	}
	if offset.Offset != 12345 {
		t.Errorf("expected offset 12345, got %d", offset.Offset)
	}
}
