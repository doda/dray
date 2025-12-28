package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// newTestJoinGroupHandlerForInterop creates a JoinGroupHandler with short timeouts for testing.
func newTestJoinGroupHandlerForInterop() (*JoinGroupHandler, *groups.Store) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	handler := NewJoinGroupHandler(JoinGroupHandlerConfig{
		MinSessionTimeoutMs: 1000,
		MaxSessionTimeoutMs: 60000,
		RebalanceTimeoutMs:  50, // Very short for tests
	}, groupStore, nil)
	return handler, groupStore
}

// TestJoinGroup_MismatchedEndpointType tests that JoinGroup returns MISMATCHED_ENDPOINT_TYPE
// when trying to use classic protocol with a consumer group that has active members.
func TestJoinGroup_MismatchedEndpointType(t *testing.T) {
	ctx := context.Background()
	handler, store := newTestJoinGroupHandlerForInterop()

	// Create a consumer protocol group
	nowMs := time.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      "consumer-group",
		Type:         groups.GroupTypeConsumer,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	// Add a member (simulating active consumer protocol usage)
	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:          "consumer-group",
		MemberID:         "consumer-member-1",
		ClientID:         "consumer-client",
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
		MemberEpoch:      1,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Try to join using classic protocol
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = "consumer-group"
	req.MemberID = ""
	req.ProtocolType = "consumer"
	req.SessionTimeoutMillis = 30000
	req.RebalanceTimeoutMillis = 60000
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	resp := handler.Handle(ctx, 9, req, "test-client")

	// Should return MISMATCHED_ENDPOINT_TYPE (114)
	if resp.ErrorCode != errMismatchedEndpointTypeJoinG {
		t.Errorf("expected error code %d (MISMATCHED_ENDPOINT_TYPE), got %d", errMismatchedEndpointTypeJoinG, resp.ErrorCode)
	}
}

// TestJoinGroup_ConvertEmptyConsumerGroup tests that JoinGroup can convert an empty
// consumer protocol group to classic protocol.
func TestJoinGroup_ConvertEmptyConsumerGroup(t *testing.T) {
	ctx := context.Background()
	handler, store := newTestJoinGroupHandlerForInterop()

	// Create an empty consumer protocol group
	nowMs := time.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      "empty-consumer-group",
		Type:         groups.GroupTypeConsumer,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	// Verify initial type
	groupType, err := store.GetGroupType(ctx, "empty-consumer-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != groups.GroupTypeConsumer {
		t.Errorf("expected consumer type initially, got %s", groupType)
	}

	// Join using classic protocol (group is empty, should convert)
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = "empty-consumer-group"
	req.MemberID = ""
	req.ProtocolType = "consumer"
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 100 // Short timeout for test
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	resp := handler.Handle(ctx, 9, req, "test-client")

	// Should succeed (error code 0) - the rebalance will complete with this single member
	if resp.ErrorCode != 0 {
		t.Errorf("expected success (0), got %d", resp.ErrorCode)
	}

	// Verify group type was converted
	groupType, err = store.GetGroupType(ctx, "empty-consumer-group")
	if err != nil {
		t.Fatalf("failed to get group type after join: %v", err)
	}
	if groupType != groups.GroupTypeClassic {
		t.Errorf("expected classic type after conversion, got %s", groupType)
	}
}

// TestConsumerGroupHeartbeat_MismatchedEndpointType tests that ConsumerGroupHeartbeat
// returns MISMATCHED_ENDPOINT_TYPE when trying to use consumer protocol with a
// classic group that has active members.
func TestConsumerGroupHeartbeat_MismatchedEndpointType(t *testing.T) {
	ctx := context.Background()
	store := groups.NewStore(metadata.NewMockStore())
	handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

	// Create a classic protocol group
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

	// Add a member (simulating active classic protocol usage)
	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:          "classic-group",
		MemberID:         "classic-member-1",
		ClientID:         "classic-client",
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Try to join using consumer protocol
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group = "classic-group"
	req.MemberID = ""
	req.MemberEpoch = 0
	req.SubscribedTopicNames = []string{"test-topic"}

	resp := handler.Handle(ctx, 0, req)

	// Should return MISMATCHED_ENDPOINT_TYPE (114)
	if resp.ErrorCode != errCGHBMismatchedEndpointType {
		t.Errorf("expected error code %d (MISMATCHED_ENDPOINT_TYPE), got %d", errCGHBMismatchedEndpointType, resp.ErrorCode)
	}

	// Verify error message is set
	if resp.ErrorMessage == nil || *resp.ErrorMessage == "" {
		t.Error("expected error message to be set")
	}
}

// TestConsumerGroupHeartbeat_ConvertEmptyClassicGroup tests that ConsumerGroupHeartbeat
// can convert an empty classic protocol group to consumer protocol.
func TestConsumerGroupHeartbeat_ConvertEmptyClassicGroup(t *testing.T) {
	ctx := context.Background()
	store := groups.NewStore(metadata.NewMockStore())
	handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

	// Create an empty classic protocol group
	nowMs := time.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      "empty-classic-group",
		Type:         groups.GroupTypeClassic,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create classic group: %v", err)
	}

	// Verify initial type
	groupType, err := store.GetGroupType(ctx, "empty-classic-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != groups.GroupTypeClassic {
		t.Errorf("expected classic type initially, got %s", groupType)
	}

	// Join using consumer protocol (group is empty, should convert)
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group = "empty-classic-group"
	req.MemberID = ""
	req.MemberEpoch = 0
	req.SubscribedTopicNames = []string{"test-topic"}

	resp := handler.Handle(ctx, 0, req)

	// Should succeed
	if resp.ErrorCode != 0 {
		t.Errorf("expected success (0), got %d", resp.ErrorCode)
	}

	// Verify group type was converted
	groupType, err = store.GetGroupType(ctx, "empty-classic-group")
	if err != nil {
		t.Fatalf("failed to get group type after heartbeat: %v", err)
	}
	if groupType != groups.GroupTypeConsumer {
		t.Errorf("expected consumer type after conversion, got %s", groupType)
	}
}

// TestGroupTypeIsFixed tests that group type is fixed once created and can only
// be changed when empty.
func TestGroupTypeIsFixed(t *testing.T) {
	ctx := context.Background()
	store := groups.NewStore(metadata.NewMockStore())

	// Create a classic group
	nowMs := time.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID: "fixed-type-group",
		Type:    groups.GroupTypeClassic,
		NowMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Verify type is classic
	groupType, err := store.GetGroupType(ctx, "fixed-type-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != groups.GroupTypeClassic {
		t.Errorf("expected classic type, got %s", groupType)
	}

	// Add a member
	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:  "fixed-type-group",
		MemberID: "member-1",
		NowMs:    nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Try to convert - should fail because group has member
	err = store.ConvertGroupType(ctx, "fixed-type-group", groups.GroupTypeConsumer)
	if err == nil {
		t.Error("expected error when converting non-empty group")
	}

	// Verify type is still classic
	groupType, err = store.GetGroupType(ctx, "fixed-type-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != groups.GroupTypeClassic {
		t.Errorf("expected classic type (unchanged), got %s", groupType)
	}

	// Remove member
	err = store.RemoveMember(ctx, "fixed-type-group", "member-1")
	if err != nil {
		t.Fatalf("failed to remove member: %v", err)
	}

	// Now conversion should work
	err = store.ConvertGroupType(ctx, "fixed-type-group", groups.GroupTypeConsumer)
	if err != nil {
		t.Fatalf("expected conversion to succeed for empty group: %v", err)
	}

	// Verify type changed
	groupType, err = store.GetGroupType(ctx, "fixed-type-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != groups.GroupTypeConsumer {
		t.Errorf("expected consumer type after conversion, got %s", groupType)
	}
}

// TestMismatchedEndpointTypeLogging tests that appropriate error messages are logged
// when protocol mismatch occurs.
func TestMismatchedEndpointTypeLogging(t *testing.T) {
	ctx := context.Background()
	store := groups.NewStore(metadata.NewMockStore())
	handler := NewConsumerGroupHeartbeatHandler(store, nil, nil)

	// Create a classic group with a member
	nowMs := time.Now().UnixMilli()
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      "logging-test-group",
		Type:         groups.GroupTypeClassic,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:  "logging-test-group",
		MemberID: "member-1",
		NowMs:    nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Try consumer protocol heartbeat
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group = "logging-test-group"
	req.MemberID = ""
	req.MemberEpoch = 0
	req.SubscribedTopicNames = []string{"test-topic"}

	resp := handler.Handle(ctx, 0, req)

	// Verify error response
	if resp.ErrorCode != errCGHBMismatchedEndpointType {
		t.Errorf("expected error code %d, got %d", errCGHBMismatchedEndpointType, resp.ErrorCode)
	}

	// Verify clear error message is provided
	if resp.ErrorMessage == nil {
		t.Error("expected error message to be set for MISMATCHED_ENDPOINT_TYPE")
	} else if *resp.ErrorMessage == "" {
		t.Error("expected non-empty error message")
	}
}
