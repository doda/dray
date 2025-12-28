package protocol

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newTestJoinGroupHandler() (*JoinGroupHandler, *groups.Store) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	handler := NewJoinGroupHandler(JoinGroupHandlerConfig{
		MinSessionTimeoutMs: 1000,
		MaxSessionTimeoutMs: 60000,
		RebalanceTimeoutMs:  50, // Very short for tests
	}, groupStore, nil)
	return handler, groupStore
}

func TestJoinGroupHandler_InvalidGroupID(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(5)
	req.Group = "" // Empty group ID
	req.SessionTimeoutMillis = 10000
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	resp := handler.Handle(context.Background(), 5, req, "test-client")

	if resp.ErrorCode != errInvalidGroupID {
		t.Errorf("expected error code %d (INVALID_GROUP_ID), got %d", errInvalidGroupID, resp.ErrorCode)
	}
}

func TestJoinGroupHandler_InvalidSessionTimeout(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	// Test session timeout too small
	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(5)
	req.Group = "test-group"
	req.SessionTimeoutMillis = 500 // Below minimum of 1000
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	resp := handler.Handle(context.Background(), 5, req, "test-client")

	if resp.ErrorCode != errInvalidSessionTimeout {
		t.Errorf("expected error code %d (INVALID_SESSION_TIMEOUT), got %d", errInvalidSessionTimeout, resp.ErrorCode)
	}

	// Test session timeout too large
	req.SessionTimeoutMillis = 100000 // Above maximum of 60000
	resp = handler.Handle(context.Background(), 5, req, "test-client")

	if resp.ErrorCode != errInvalidSessionTimeout {
		t.Errorf("expected error code %d (INVALID_SESSION_TIMEOUT), got %d", errInvalidSessionTimeout, resp.ErrorCode)
	}
}

func TestJoinGroupHandler_NoProtocols(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(5)
	req.Group = "test-group"
	req.SessionTimeoutMillis = 10000
	req.Protocols = nil // No protocols

	resp := handler.Handle(context.Background(), 5, req, "test-client")

	if resp.ErrorCode != errInconsistentGroupProto {
		t.Errorf("expected error code %d (INCONSISTENT_GROUP_PROTOCOL), got %d", errInconsistentGroupProto, resp.ErrorCode)
	}
}

func TestJoinGroupHandler_SingleMemberJoin(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(5)
	req.Group = "test-group"
	req.MemberID = ""
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 100
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	// Use a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp := handler.Handle(ctx, 5, req, "test-client")

	// Should succeed
	if resp.ErrorCode != errJoinGroupNone {
		t.Errorf("expected no error, got %d", resp.ErrorCode)
	}
	if resp.Generation < 1 {
		t.Errorf("expected generation >= 1, got %d", resp.Generation)
	}
	if resp.MemberID == "" {
		t.Error("expected member ID to be set")
	}
	if !strings.HasPrefix(resp.MemberID, "test-client-") {
		t.Errorf("expected member ID to start with 'test-client-', got %s", resp.MemberID)
	}
	if resp.LeaderID == "" {
		t.Error("expected leader ID to be set")
	}
	// Single member should be the leader
	if resp.MemberID != resp.LeaderID {
		t.Errorf("single member should be leader, memberID=%s, leaderID=%s", resp.MemberID, resp.LeaderID)
	}
	// Leader should receive member list
	if len(resp.Members) != 1 {
		t.Errorf("leader should receive member list with 1 member, got %d", len(resp.Members))
	}
}

func TestJoinGroupHandler_MemberIDGeneration(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	// For v5+, MEMBER_ID_REQUIRED is returned on first join to existing group
	// To test member ID generation, use v3 which doesn't have this behavior
	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(3)
	req.Group = "test-group-v3"
	req.MemberID = ""
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 100
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp1 := handler.Handle(ctx, 3, req, "client-a")
	if resp1.ErrorCode != errJoinGroupNone {
		t.Fatalf("first join failed: %d", resp1.ErrorCode)
	}

	// For second request, use different group to avoid MEMBER_ID_REQUIRED
	req.Group = "test-group-v3-b"
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()

	resp2 := handler.Handle(ctx2, 3, req, "client-b")
	if resp2.ErrorCode != errJoinGroupNone {
		t.Fatalf("second join failed: %d", resp2.ErrorCode)
	}

	if resp1.MemberID == resp2.MemberID {
		t.Error("expected different member IDs for different clients")
	}

	if !strings.HasPrefix(resp1.MemberID, "client-a-") {
		t.Errorf("first member ID should start with client-a-, got %s", resp1.MemberID)
	}
	if !strings.HasPrefix(resp2.MemberID, "client-b-") {
		t.Errorf("second member ID should start with client-b-, got %s", resp2.MemberID)
	}
}

func TestJoinGroupHandler_MultipleMembersJoin(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	var wg sync.WaitGroup
	results := make(chan *kmsg.JoinGroupResponse, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()

			req := kmsg.NewPtrJoinGroupRequest()
			req.SetVersion(5)
			req.Group = "multi-member-group"
			req.MemberID = ""
			req.SessionTimeoutMillis = 10000
			req.RebalanceTimeoutMillis = 200
			req.ProtocolType = "consumer"
			req.Protocols = []kmsg.JoinGroupRequestProtocol{
				{Name: "range", Metadata: []byte("test")},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			resp := handler.Handle(ctx, 5, req, "client")
			results <- resp
		}(i)
	}

	wg.Wait()
	close(results)

	var responses []*kmsg.JoinGroupResponse
	for resp := range results {
		responses = append(responses, resp)
	}

	if len(responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(responses))
	}

	// All should succeed with same generation
	var leaderCount int
	generation := responses[0].Generation
	leaderID := responses[0].LeaderID

	for i, resp := range responses {
		if resp.ErrorCode != errJoinGroupNone {
			t.Errorf("member %d: expected no error, got %d", i, resp.ErrorCode)
		}
		if resp.Generation != generation {
			t.Errorf("member %d: expected generation %d, got %d", i, generation, resp.Generation)
		}
		if resp.LeaderID != leaderID {
			t.Errorf("member %d: expected leader %s, got %s", i, leaderID, resp.LeaderID)
		}
		// Only leader should have member list
		if resp.MemberID == resp.LeaderID {
			leaderCount++
			if len(resp.Members) != 3 {
				t.Errorf("leader should receive all 3 members, got %d", len(resp.Members))
			}
		} else {
			if len(resp.Members) != 0 {
				t.Errorf("non-leader should have empty member list, got %d", len(resp.Members))
			}
		}
	}

	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaderCount)
	}
}

func TestJoinGroupHandler_ProtocolEncoding(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	protocols := []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("range-metadata")},
		{Name: "roundrobin", Metadata: []byte("roundrobin-metadata")},
		{Name: "sticky", Metadata: []byte("sticky-metadata")},
	}

	// Encode
	encoded := handler.encodeProtocols(protocols)

	// Decode
	decoded := handler.decodeProtocols(encoded)

	if len(decoded) != len(protocols) {
		t.Fatalf("expected %d protocols, got %d", len(protocols), len(decoded))
	}

	for i, p := range decoded {
		if p.Name != protocols[i].Name {
			t.Errorf("protocol %d: expected name %s, got %s", i, protocols[i].Name, p.Name)
		}
		if string(p.Metadata) != string(protocols[i].Metadata) {
			t.Errorf("protocol %d: metadata mismatch", i)
		}
	}
}

func TestJoinGroupHandler_ProtocolSelection(t *testing.T) {
	handler, store := newTestJoinGroupHandler()
	ctx := context.Background()
	groupID := "protocol-select-group"
	nowMs := time.Now().UnixMilli()

	// Create group
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      groupID,
		Type:         groups.GroupTypeClassic,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add members with different protocols
	member1Protocols := handler.encodeProtocols([]kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("m1-range")},
		{Name: "sticky", Metadata: []byte("m1-sticky")},
	})
	member2Protocols := handler.encodeProtocols([]kmsg.JoinGroupRequestProtocol{
		{Name: "sticky", Metadata: []byte("m2-sticky")},
		{Name: "roundrobin", Metadata: []byte("m2-rr")},
	})

	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:            groupID,
		MemberID:           "member-1",
		SupportedProtocols: member1Protocols,
		NowMs:              nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member 1: %v", err)
	}

	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:            groupID,
		MemberID:           "member-2",
		SupportedProtocols: member2Protocols,
		NowMs:              nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member 2: %v", err)
	}

	// Select protocol - should select "sticky" as it's common to both
	selected, err := handler.selectProtocol(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to select protocol: %v", err)
	}

	if selected != "sticky" {
		t.Errorf("expected 'sticky' as common protocol, got %s", selected)
	}
}

func TestJoinGroupHandler_ProtocolSelection_NoCommonProtocol(t *testing.T) {
	handler, store := newTestJoinGroupHandler()
	ctx := context.Background()
	groupID := "protocol-no-common-group"
	nowMs := time.Now().UnixMilli()

	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      groupID,
		Type:         groups.GroupTypeClassic,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	member1Protocols := handler.encodeProtocols([]kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("m1-range")},
	})
	member2Protocols := handler.encodeProtocols([]kmsg.JoinGroupRequestProtocol{
		{Name: "sticky", Metadata: []byte("m2-sticky")},
	})

	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:            groupID,
		MemberID:           "member-1",
		SupportedProtocols: member1Protocols,
		NowMs:              nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member 1: %v", err)
	}

	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:            groupID,
		MemberID:           "member-2",
		SupportedProtocols: member2Protocols,
		NowMs:              nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member 2: %v", err)
	}

	_, err = handler.selectProtocol(ctx, groupID)
	if err == nil || !errors.Is(err, errNoCommonProtocol) {
		t.Errorf("expected errNoCommonProtocol, got %v", err)
	}
}

func TestJoinGroupHandler_MaxGroupSize(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	handler := NewJoinGroupHandler(JoinGroupHandlerConfig{
		MinSessionTimeoutMs: 1000,
		MaxSessionTimeoutMs: 60000,
		RebalanceTimeoutMs:  50,
		MaxGroupSize:        2, // Limit to 2 members
	}, groupStore, nil)

	ctx := context.Background()
	groupID := "max-size-group"

	// Use v3 to avoid MEMBER_ID_REQUIRED behavior
	// Join with first two members
	for i := 0; i < 2; i++ {
		req := kmsg.NewPtrJoinGroupRequest()
		req.SetVersion(3)
		req.Group = groupID
		req.MemberID = ""
		req.SessionTimeoutMillis = 10000
		req.RebalanceTimeoutMillis = 100
		req.ProtocolType = "consumer"
		req.Protocols = []kmsg.JoinGroupRequestProtocol{
			{Name: "range", Metadata: []byte("test")},
		}

		tctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp := handler.Handle(tctx, 3, req, "client")
		cancel()

		if resp.ErrorCode != errJoinGroupNone {
			t.Errorf("member %d join failed: %d", i, resp.ErrorCode)
		}
	}

	// Third member should be rejected
	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(3)
	req.Group = groupID
	req.MemberID = ""
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 100
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	resp := handler.Handle(context.Background(), 3, req, "client-3")

	if resp.ErrorCode != errGroupMaxSizeReached {
		t.Errorf("expected error code %d (GROUP_MAX_SIZE_REACHED), got %d", errGroupMaxSizeReached, resp.ErrorCode)
	}
}

func TestJoinGroupHandler_RejoinWithMemberID(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	// First join
	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(5)
	req.Group = "rejoin-group"
	req.MemberID = ""
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 100
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	resp1 := handler.Handle(ctx, 5, req, "test-client")
	cancel()

	if resp1.ErrorCode != errJoinGroupNone {
		t.Fatalf("first join failed: %d", resp1.ErrorCode)
	}

	memberID := resp1.MemberID
	generation := resp1.Generation

	// Rejoin with same member ID
	req.MemberID = memberID

	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	resp2 := handler.Handle(ctx2, 5, req, "test-client")
	cancel2()

	if resp2.ErrorCode != errJoinGroupNone {
		t.Fatalf("rejoin failed: %d", resp2.ErrorCode)
	}

	if resp2.MemberID != memberID {
		t.Errorf("expected same member ID %s, got %s", memberID, resp2.MemberID)
	}

	// Generation should increment on rejoin
	if resp2.Generation <= generation {
		t.Errorf("expected generation > %d, got %d", generation, resp2.Generation)
	}
}

func TestJoinGroupHandler_SessionTimeoutTracking(t *testing.T) {
	handler, store := newTestJoinGroupHandler()
	ctx := context.Background()
	groupID := "timeout-group"
	nowMs := time.Now().UnixMilli()

	// Create group
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      groupID,
		Type:         groups.GroupTypeClassic,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add member with 100ms session timeout
	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:          groupID,
		MemberID:         "member-1",
		SessionTimeoutMs: 100,
		NowMs:            nowMs - 200, // Last heartbeat was 200ms ago
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Check for timed out members
	timedOut, err := handler.GetSessionTimeoutMembers(ctx, groupID, nowMs)
	if err != nil {
		t.Fatalf("failed to get timed out members: %v", err)
	}

	if len(timedOut) != 1 {
		t.Errorf("expected 1 timed out member, got %d", len(timedOut))
	}
	if len(timedOut) > 0 && timedOut[0] != "member-1" {
		t.Errorf("expected member-1 to be timed out, got %s", timedOut[0])
	}
}

func TestJoinGroupHandler_RemoveMember(t *testing.T) {
	handler, store := newTestJoinGroupHandler()
	ctx := context.Background()
	groupID := "remove-group"
	nowMs := time.Now().UnixMilli()

	// Create group
	_, err := store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:      groupID,
		Type:         groups.GroupTypeClassic,
		ProtocolType: "consumer",
		NowMs:        nowMs,
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add member
	_, err = store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:  groupID,
		MemberID: "member-to-remove",
		NowMs:    nowMs,
	})
	if err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Verify member exists
	_, err = store.GetMember(ctx, groupID, "member-to-remove")
	if err != nil {
		t.Fatalf("member should exist: %v", err)
	}

	// Remove member
	err = handler.RemoveMember(ctx, groupID, "member-to-remove")
	if err != nil {
		t.Fatalf("failed to remove member: %v", err)
	}

	// Verify member is gone
	_, err = store.GetMember(ctx, groupID, "member-to-remove")
	if err == nil {
		t.Error("member should not exist after removal")
	}
}

func TestJoinGroupHandler_DefaultConfig(t *testing.T) {
	cfg := DefaultJoinGroupHandlerConfig()

	if cfg.MinSessionTimeoutMs != 6000 {
		t.Errorf("expected min session timeout 6000, got %d", cfg.MinSessionTimeoutMs)
	}
	if cfg.MaxSessionTimeoutMs != 300000 {
		t.Errorf("expected max session timeout 300000, got %d", cfg.MaxSessionTimeoutMs)
	}
	if cfg.RebalanceTimeoutMs != 300000 {
		t.Errorf("expected rebalance timeout 300000, got %d", cfg.RebalanceTimeoutMs)
	}
	if cfg.MaxGroupSize != 0 {
		t.Errorf("expected max group size 0 (unlimited), got %d", cfg.MaxGroupSize)
	}
}

func TestJoinGroupHandler_ZeroConfigDefaults(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)

	// Create handler with zero config - should use defaults
	handler := NewJoinGroupHandler(JoinGroupHandlerConfig{}, groupStore, nil)

	if handler.cfg.MinSessionTimeoutMs != 6000 {
		t.Errorf("expected min session timeout 6000, got %d", handler.cfg.MinSessionTimeoutMs)
	}
	if handler.cfg.MaxSessionTimeoutMs != 300000 {
		t.Errorf("expected max session timeout 300000, got %d", handler.cfg.MaxSessionTimeoutMs)
	}
	if handler.cfg.RebalanceTimeoutMs != 300000 {
		t.Errorf("expected rebalance timeout 300000, got %d", handler.cfg.RebalanceTimeoutMs)
	}
}

func TestJoinGroupHandler_LeaderReceivesMemberList(t *testing.T) {
	handler, _ := newTestJoinGroupHandler()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var leader *kmsg.JoinGroupResponse
	var followers []*kmsg.JoinGroupResponse

	// Start 3 members
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			req := kmsg.NewPtrJoinGroupRequest()
			req.SetVersion(5)
			req.Group = "leader-test-group"
			req.MemberID = ""
			req.SessionTimeoutMillis = 10000
			req.RebalanceTimeoutMillis = 200
			req.ProtocolType = "consumer"
			req.Protocols = []kmsg.JoinGroupRequestProtocol{
				{Name: "range", Metadata: []byte("test-metadata")},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			resp := handler.Handle(ctx, 5, req, "client")

			mu.Lock()
			if resp.MemberID == resp.LeaderID {
				leader = resp
			} else {
				followers = append(followers, resp)
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	if leader == nil {
		t.Fatal("no leader found")
	}

	// Leader should have all members in the list
	if len(leader.Members) != 3 {
		t.Errorf("leader should have 3 members, got %d", len(leader.Members))
	}

	// Followers should have empty member list
	for i, f := range followers {
		if len(f.Members) != 0 {
			t.Errorf("follower %d should have empty member list, got %d", i, len(f.Members))
		}
	}

	// Verify member metadata is present for leader
	for i, m := range leader.Members {
		if m.MemberID == "" {
			t.Errorf("member %d: member ID should not be empty", i)
		}
		if len(m.ProtocolMetadata) == 0 {
			t.Errorf("member %d: protocol metadata should be present", i)
		}
	}
}

func TestJoinGroupHandler_ContextCancellation(t *testing.T) {
	metaStore := metadata.NewMockStore()
	groupStore := groups.NewStore(metaStore)
	handler := NewJoinGroupHandler(JoinGroupHandlerConfig{
		MinSessionTimeoutMs: 1000,
		MaxSessionTimeoutMs: 60000,
		RebalanceTimeoutMs:  60000, // Long timeout
	}, groupStore, nil)

	req := kmsg.NewPtrJoinGroupRequest()
	req.SetVersion(5)
	req.Group = "cancel-test"
	req.MemberID = ""
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 60000
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte("test")},
	}

	// Create a context that cancels quickly
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	resp := handler.Handle(ctx, 5, req, "test-client")

	// Should return error due to context cancellation
	if resp.ErrorCode != errRebalanceInProgress {
		t.Errorf("expected REBALANCE_IN_PROGRESS error on context cancel, got %d", resp.ErrorCode)
	}
}
