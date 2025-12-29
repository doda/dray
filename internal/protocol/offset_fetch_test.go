package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOffsetFetchHandler_Handle(t *testing.T) {
	ctx := context.Background()

	t.Run("success - fetch specific partitions", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		// Commit some offsets first
		_, err := store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID:   "test-group",
			Topic:     "test-topic",
			Partition: 0,
			Offset:    100,
			Metadata:  "test-metadata",
			NowMs:     time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("commit offset: %v", err)
		}

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = "test-topic"
		topic.Partitions = []int32{0}

		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
		}
		if len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("expected 1 partition, got %d", len(resp.Topics[0].Partitions))
		}
		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetFetchNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
		if resp.Topics[0].Partitions[0].Offset != 100 {
			t.Errorf("expected offset 100, got %d", resp.Topics[0].Partitions[0].Offset)
		}
		if resp.Topics[0].Partitions[0].Metadata == nil || *resp.Topics[0].Partitions[0].Metadata != "test-metadata" {
			t.Errorf("expected metadata 'test-metadata'")
		}
	})

	t.Run("success - fetch multiple topics and partitions", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)
		nowMs := time.Now().UnixMilli()

		// Commit offsets for multiple topics
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "test-group", Topic: "topic-1", Partition: 0, Offset: 100, NowMs: nowMs,
		})
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "test-group", Topic: "topic-1", Partition: 1, Offset: 200, NowMs: nowMs,
		})
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "test-group", Topic: "topic-2", Partition: 0, Offset: 300, NowMs: nowMs,
		})

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic1 := kmsg.NewOffsetFetchRequestTopic()
		topic1.Topic = "topic-1"
		topic1.Partitions = []int32{0, 1}
		req.Topics = append(req.Topics, topic1)

		topic2 := kmsg.NewOffsetFetchRequestTopic()
		topic2.Topic = "topic-2"
		topic2.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic2)

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Topics) != 2 {
			t.Fatalf("expected 2 topics, got %d", len(resp.Topics))
		}

		// Check topic 1
		if resp.Topics[0].Topic != "topic-1" {
			t.Errorf("expected topic 'topic-1', got %q", resp.Topics[0].Topic)
		}
		if len(resp.Topics[0].Partitions) != 2 {
			t.Fatalf("expected 2 partitions in topic-1, got %d", len(resp.Topics[0].Partitions))
		}

		// Check topic 2
		if resp.Topics[1].Topic != "topic-2" {
			t.Errorf("expected topic 'topic-2', got %q", resp.Topics[1].Topic)
		}
		if len(resp.Topics[1].Partitions) != 1 {
			t.Fatalf("expected 1 partition in topic-2, got %d", len(resp.Topics[1].Partitions))
		}
	})

	t.Run("missing offset returns -1", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = "nonexistent-topic"
		topic.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 0, req)

		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatal("unexpected response structure")
		}
		if resp.Topics[0].Partitions[0].Offset != -1 {
			t.Errorf("expected offset -1 for missing offset, got %d", resp.Topics[0].Partitions[0].Offset)
		}
		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetFetchNone {
			t.Errorf("expected no error for missing offset, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
	})

	t.Run("error - empty group ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = ""

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errOffsetFetchInvalidGroupID {
			t.Errorf("expected INVALID_GROUP_ID (24), got %d", resp.ErrorCode)
		}
	})

	t.Run("v2+ fetch all topics when Topics is empty", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)
		nowMs := time.Now().UnixMilli()

		// Commit offsets for multiple topics
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "test-group", Topic: "topic-1", Partition: 0, Offset: 100, NowMs: nowMs,
		})
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "test-group", Topic: "topic-2", Partition: 0, Offset: 200, NowMs: nowMs,
		})

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"
		req.Topics = nil // Empty topics means fetch all

		resp := handler.Handle(ctx, 2, req)

		if resp.ErrorCode != errOffsetFetchNone {
			t.Errorf("expected no error, got %d", resp.ErrorCode)
		}

		// Should have all committed topics
		if len(resp.Topics) != 2 {
			t.Fatalf("expected 2 topics, got %d", len(resp.Topics))
		}
	})

	t.Run("v3+ includes error code and throttle", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"
		req.Topics = nil

		resp := handler.Handle(ctx, 3, req)

		if resp.ErrorCode != errOffsetFetchNone {
			t.Errorf("expected error code 0, got %d", resp.ErrorCode)
		}
		if resp.ThrottleMillis != 0 {
			t.Errorf("expected throttle 0, got %d", resp.ThrottleMillis)
		}
	})

	t.Run("v5+ includes leader epoch", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		// Commit offset with leader epoch
		_, err := store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID:     "test-group",
			Topic:       "test-topic",
			Partition:   0,
			Offset:      100,
			LeaderEpoch: 5,
			NowMs:       time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("commit offset: %v", err)
		}

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = "test-topic"
		topic.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 5, req)

		if resp.Topics[0].Partitions[0].LeaderEpoch != 5 {
			t.Errorf("expected leader epoch 5, got %d", resp.Topics[0].Partitions[0].LeaderEpoch)
		}
	})

	t.Run("v5+ missing offset has leader epoch -1", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = "nonexistent-topic"
		topic.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 5, req)

		if resp.Topics[0].Partitions[0].LeaderEpoch != -1 {
			t.Errorf("expected leader epoch -1 for missing offset, got %d", resp.Topics[0].Partitions[0].LeaderEpoch)
		}
	})

	t.Run("version response formatting", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		for _, version := range []int16{0, 1, 2, 3, 4, 5, 6, 7} {
			req := kmsg.NewPtrOffsetFetchRequest()
			req.Group = "test-group"

			topic := kmsg.NewOffsetFetchRequestTopic()
			topic.Topic = "test-topic"
			topic.Partitions = []int32{0}
			req.Topics = append(req.Topics, topic)

			resp := handler.Handle(ctx, version, req)

			if resp.GetVersion() != version {
				t.Errorf("version %d: expected version %d, got %d", version, version, resp.GetVersion())
			}
		}
	})
}

func TestOffsetFetchHandler_BatchedRequest(t *testing.T) {
	ctx := context.Background()

	t.Run("v8+ batched groups request", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)
		nowMs := time.Now().UnixMilli()

		// Commit offsets for two groups
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "group-1", Topic: "topic-1", Partition: 0, Offset: 100, NowMs: nowMs,
		})
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "group-2", Topic: "topic-2", Partition: 0, Offset: 200, NowMs: nowMs,
		})

		req := kmsg.NewPtrOffsetFetchRequest()

		group1 := kmsg.NewOffsetFetchRequestGroup()
		group1.Group = "group-1"
		topic1 := kmsg.NewOffsetFetchRequestGroupTopic()
		topic1.Topic = "topic-1"
		topic1.Partitions = []int32{0}
		group1.Topics = append(group1.Topics, topic1)
		req.Groups = append(req.Groups, group1)

		group2 := kmsg.NewOffsetFetchRequestGroup()
		group2.Group = "group-2"
		topic2 := kmsg.NewOffsetFetchRequestGroupTopic()
		topic2.Topic = "topic-2"
		topic2.Partitions = []int32{0}
		group2.Topics = append(group2.Topics, topic2)
		req.Groups = append(req.Groups, group2)

		resp := handler.Handle(ctx, 8, req)

		if len(resp.Groups) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(resp.Groups))
		}

		// Check group 1
		if resp.Groups[0].Group != "group-1" {
			t.Errorf("expected group 'group-1', got %q", resp.Groups[0].Group)
		}
		if resp.Groups[0].ErrorCode != errOffsetFetchNone {
			t.Errorf("expected no error for group-1, got %d", resp.Groups[0].ErrorCode)
		}
		if len(resp.Groups[0].Topics) != 1 {
			t.Fatalf("expected 1 topic in group-1, got %d", len(resp.Groups[0].Topics))
		}
		if resp.Groups[0].Topics[0].Partitions[0].Offset != 100 {
			t.Errorf("expected offset 100 for group-1, got %d", resp.Groups[0].Topics[0].Partitions[0].Offset)
		}

		// Check group 2
		if resp.Groups[1].Group != "group-2" {
			t.Errorf("expected group 'group-2', got %q", resp.Groups[1].Group)
		}
		if resp.Groups[1].ErrorCode != errOffsetFetchNone {
			t.Errorf("expected no error for group-2, got %d", resp.Groups[1].ErrorCode)
		}
		if resp.Groups[1].Topics[0].Partitions[0].Offset != 200 {
			t.Errorf("expected offset 200 for group-2, got %d", resp.Groups[1].Topics[0].Partitions[0].Offset)
		}
	})

	t.Run("v8+ batched request fetch all topics", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)
		nowMs := time.Now().UnixMilli()

		// Commit multiple offsets
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "test-group", Topic: "topic-1", Partition: 0, Offset: 100, NowMs: nowMs,
		})
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID: "test-group", Topic: "topic-2", Partition: 0, Offset: 200, NowMs: nowMs,
		})

		req := kmsg.NewPtrOffsetFetchRequest()

		group := kmsg.NewOffsetFetchRequestGroup()
		group.Group = "test-group"
		group.Topics = nil // Empty topics means fetch all
		req.Groups = append(req.Groups, group)

		resp := handler.Handle(ctx, 8, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errOffsetFetchNone {
			t.Errorf("expected no error, got %d", resp.Groups[0].ErrorCode)
		}
		if len(resp.Groups[0].Topics) != 2 {
			t.Fatalf("expected 2 topics, got %d", len(resp.Groups[0].Topics))
		}
	})

	t.Run("v8+ batched request with empty group ID", func(t *testing.T) {
		store := newTestGroupStore()
		handler := NewOffsetFetchHandler(store, nil)

		req := kmsg.NewPtrOffsetFetchRequest()

		group := kmsg.NewOffsetFetchRequestGroup()
		group.Group = ""
		req.Groups = append(req.Groups, group)

		resp := handler.Handle(ctx, 8, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errOffsetFetchInvalidGroupID {
			t.Errorf("expected INVALID_GROUP_ID (24), got %d", resp.Groups[0].ErrorCode)
		}
	})
}

func TestOffsetFetchHandler_WithLeaseManager(t *testing.T) {
	ctx := context.Background()

	t.Run("not coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewOffsetFetchHandler(store, leaseManager)

		// Simulate another broker holding the lease
		_, _ = metaStore.Put(ctx, "/dray/v1/groups/test-group/lease", []byte(`{"brokerID":"broker-2","acquiredAt":0}`))

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = "test-topic"
		topic.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 0, req)

		if resp.ErrorCode != errOffsetFetchNotCoordinator {
			t.Errorf("expected NOT_COORDINATOR (16), got %d", resp.ErrorCode)
		}
	})

	t.Run("is coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewOffsetFetchHandler(store, leaseManager)

		// Commit an offset
		_, _ = store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID:   "test-group",
			Topic:     "test-topic",
			Partition: 0,
			Offset:    100,
			NowMs:     time.Now().UnixMilli(),
		})

		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = "test-topic"
		topic.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic)

		resp := handler.Handle(ctx, 0, req)

		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetFetchNone {
			t.Errorf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
		if resp.Topics[0].Partitions[0].Offset != 100 {
			t.Errorf("expected offset 100, got %d", resp.Topics[0].Partitions[0].Offset)
		}
	})

	t.Run("v8+ batched request not coordinator", func(t *testing.T) {
		metaStore := metadata.NewMockStore()
		store := groups.NewStore(metaStore)
		leaseManager := groups.NewLeaseManager(metaStore, "broker-1")

		handler := NewOffsetFetchHandler(store, leaseManager)

		// Simulate another broker holding the lease
		_, _ = metaStore.Put(ctx, "/dray/v1/groups/test-group/lease", []byte(`{"brokerID":"broker-2","acquiredAt":0}`))

		req := kmsg.NewPtrOffsetFetchRequest()

		group := kmsg.NewOffsetFetchRequestGroup()
		group.Group = "test-group"
		req.Groups = append(req.Groups, group)

		resp := handler.Handle(ctx, 8, req)

		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		if resp.Groups[0].ErrorCode != errOffsetFetchNotCoordinator {
			t.Errorf("expected NOT_COORDINATOR (16), got %d", resp.Groups[0].ErrorCode)
		}
	})
}

func TestOffsetFetchHandler_ACLEnforcement(t *testing.T) {
	nowMs := time.Now().UnixMilli()

	buildRequest := func() *kmsg.OffsetFetchRequest {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = "test-group"

		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = "test-topic"
		topic.Partitions = []int32{0}
		req.Topics = append(req.Topics, topic)
		return req
	}

	t.Run("allow", func(t *testing.T) {
		ctx := auth.WithPrincipal(context.Background(), "User:alice")
		store := newTestGroupStore()
		enforcer := newTestEnforcer(t, &auth.ACLEntry{
			ResourceType: auth.ResourceTypeGroup,
			ResourceName: "test-group",
			PatternType:  auth.PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    auth.OperationRead,
			Permission:   auth.PermissionAllow,
			CreatedAtMs:  nowMs,
		})

		_, err := store.CommitOffset(ctx, groups.CommitOffsetRequest{
			GroupID:   "test-group",
			Topic:     "test-topic",
			Partition: 0,
			Offset:    99,
			NowMs:     nowMs,
		})
		if err != nil {
			t.Fatalf("commit offset: %v", err)
		}

		handler := NewOffsetFetchHandler(store, nil).WithEnforcer(enforcer)
		resp := handler.Handle(ctx, 3, buildRequest())

		if resp.ErrorCode != errOffsetFetchNone {
			t.Fatalf("expected no error, got %d", resp.ErrorCode)
		}
		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetFetchNone {
			t.Fatalf("expected no partition error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
		if resp.Topics[0].Partitions[0].Offset != 99 {
			t.Fatalf("expected offset 99, got %d", resp.Topics[0].Partitions[0].Offset)
		}
	})

	t.Run("deny", func(t *testing.T) {
		ctx := auth.WithPrincipal(context.Background(), "User:alice")
		store := newTestGroupStore()
		enforcer := newTestEnforcer(t, &auth.ACLEntry{
			ResourceType: auth.ResourceTypeGroup,
			ResourceName: "test-group",
			PatternType:  auth.PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    auth.OperationRead,
			Permission:   auth.PermissionDeny,
			CreatedAtMs:  nowMs,
		})

		handler := NewOffsetFetchHandler(store, nil).WithEnforcer(enforcer)
		resp := handler.Handle(ctx, 3, buildRequest())

		if resp.ErrorCode != errOffsetFetchGroupAuthFailed {
			t.Fatalf("expected GROUP_AUTHORIZATION_FAILED (30), got %d", resp.ErrorCode)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected response structure")
		}
		if resp.Topics[0].Partitions[0].ErrorCode != errOffsetFetchGroupAuthFailed {
			t.Fatalf("expected GROUP_AUTHORIZATION_FAILED (30), got %d", resp.Topics[0].Partitions[0].ErrorCode)
		}
		if resp.Topics[0].Partitions[0].Offset != -1 {
			t.Fatalf("expected offset -1, got %d", resp.Topics[0].Partitions[0].Offset)
		}
	})
}

func TestOffsetFetch_KeyStoragePath(t *testing.T) {
	ctx := context.Background()

	store := newTestGroupStore()
	handler := NewOffsetFetchHandler(store, nil)

	// Commit an offset
	_, err := store.CommitOffset(ctx, groups.CommitOffsetRequest{
		GroupID:   "my-group",
		Topic:     "my-topic",
		Partition: 3,
		Offset:    12345,
		Metadata:  "test",
		NowMs:     time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = "my-group"

	topic := kmsg.NewOffsetFetchRequestTopic()
	topic.Topic = "my-topic"
	topic.Partitions = []int32{3}
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 0, req)

	if resp.Topics[0].Partitions[0].ErrorCode != errOffsetFetchNone {
		t.Fatalf("expected no error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
	if resp.Topics[0].Partitions[0].Offset != 12345 {
		t.Errorf("expected offset 12345, got %d", resp.Topics[0].Partitions[0].Offset)
	}
}
