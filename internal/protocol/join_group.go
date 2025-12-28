package protocol

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for JoinGroup responses.
const (
	errJoinGroupNone                 int16 = 0
	errGroupCoordinatorNotAvail      int16 = 15
	errNotCoordinator                int16 = 16
	errInvalidSessionTimeout         int16 = 26
	errUnknownMemberID               int16 = 25
	errMemberIDRequired              int16 = 79
	errGroupMaxSizeReached           int16 = 81
	errRebalanceInProgress           int16 = 27
	errInconsistentGroupProto        int16 = 23
	errInvalidGroupID                int16 = 24
	errGroupAuthorizationFailedJoinG int16 = 30
	errMismatchedEndpointTypeJoinG   int16 = 114 // MISMATCHED_ENDPOINT_TYPE
)

var errProtocolTypeMismatch = errors.New("join group: protocol type mismatch")
var errNoCommonProtocol = errors.New("join group: no common protocol")
var errMismatchedEndpointType = errors.New("join group: mismatched endpoint type")

// JoinGroupHandlerConfig configures the JoinGroup handler.
type JoinGroupHandlerConfig struct {
	// MinSessionTimeoutMs is the minimum session timeout.
	MinSessionTimeoutMs int32
	// MaxSessionTimeoutMs is the maximum session timeout.
	MaxSessionTimeoutMs int32
	// RebalanceTimeoutMs is the default rebalance timeout.
	RebalanceTimeoutMs int32
	// MaxGroupSize is the maximum number of members per group.
	MaxGroupSize int
}

// DefaultJoinGroupHandlerConfig returns sensible defaults.
func DefaultJoinGroupHandlerConfig() JoinGroupHandlerConfig {
	return JoinGroupHandlerConfig{
		MinSessionTimeoutMs: 6000,   // 6 seconds
		MaxSessionTimeoutMs: 300000, // 5 minutes
		RebalanceTimeoutMs:  300000, // 5 minutes
		MaxGroupSize:        0,      // Unlimited
	}
}

// pendingJoin tracks a member waiting for the join phase to complete.
type pendingJoin struct {
	ch       chan *kmsg.JoinGroupResponse
	memberID string
	clientID string
}

// rebalanceState tracks members waiting for a rebalance to complete.
type rebalanceState struct {
	mu           sync.Mutex
	pending      map[string]*pendingJoin // memberID -> pending join
	timer        *time.Timer
	timeoutMs    int32
	generation   int32
	leaderID     string
	protocolType string
	protocol     string
}

// JoinGroupHandler handles JoinGroup (key 11) requests for classic consumer groups.
type JoinGroupHandler struct {
	cfg          JoinGroupHandlerConfig
	store        *groups.Store
	leaseManager *groups.LeaseManager
	enforcer     *auth.Enforcer

	mu              sync.Mutex
	rebalanceStates map[string]*rebalanceState // groupID -> state
}

// WithEnforcer sets the ACL enforcer for this handler.
func (h *JoinGroupHandler) WithEnforcer(enforcer *auth.Enforcer) *JoinGroupHandler {
	h.enforcer = enforcer
	return h
}

// NewJoinGroupHandler creates a new JoinGroup handler.
func NewJoinGroupHandler(cfg JoinGroupHandlerConfig, store *groups.Store, leaseManager *groups.LeaseManager) *JoinGroupHandler {
	if cfg.MinSessionTimeoutMs == 0 {
		cfg.MinSessionTimeoutMs = 6000
	}
	if cfg.MaxSessionTimeoutMs == 0 {
		cfg.MaxSessionTimeoutMs = 300000
	}
	if cfg.RebalanceTimeoutMs == 0 {
		cfg.RebalanceTimeoutMs = 300000
	}
	return &JoinGroupHandler{
		cfg:             cfg,
		store:           store,
		leaseManager:    leaseManager,
		rebalanceStates: make(map[string]*rebalanceState),
	}
}

// Handle processes a JoinGroup request.
// The clientID parameter is extracted from the request header.
func (h *JoinGroupHandler) Handle(ctx context.Context, version int16, req *kmsg.JoinGroupRequest, clientID string) *kmsg.JoinGroupResponse {
	resp := kmsg.NewPtrJoinGroupResponse()
	resp.SetVersion(version)
	resp.Generation = -1

	logger := logging.FromCtx(ctx)

	// Validate group ID
	if req.Group == "" {
		resp.ErrorCode = errInvalidGroupID
		return resp
	}

	// Check ACL before processing - need READ permission on group
	if h.enforcer != nil {
		if errCode := h.enforcer.AuthorizeGroupFromCtx(ctx, req.Group, auth.OperationRead); errCode != nil {
			resp.ErrorCode = *errCode
			return resp
		}
	}

	// Validate session timeout
	if req.SessionTimeoutMillis < h.cfg.MinSessionTimeoutMs ||
		req.SessionTimeoutMillis > h.cfg.MaxSessionTimeoutMs {
		resp.ErrorCode = errInvalidSessionTimeout
		return resp
	}

	// Validate protocols
	if len(req.Protocols) == 0 {
		resp.ErrorCode = errInconsistentGroupProto
		return resp
	}

	// Get rebalance timeout (default to session timeout for v0)
	rebalanceTimeoutMs := req.RebalanceTimeoutMillis
	if version < 1 || rebalanceTimeoutMs <= 0 {
		rebalanceTimeoutMs = req.SessionTimeoutMillis
	}

	nowMs := time.Now().UnixMilli()

	// Check if this is a new member or existing member
	isNewMember := req.MemberID == ""

	// Attempt to acquire or verify we hold the coordinator lease
	if h.leaseManager != nil {
		result, err := h.leaseManager.AcquireLease(ctx, req.Group)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": req.Group,
				"error": err.Error(),
			})
			resp.ErrorCode = errGroupCoordinatorNotAvail
			return resp
		}
		if !result.Acquired {
			// Another broker is the coordinator
			resp.ErrorCode = errNotCoordinator
			return resp
		}
	}

	// Get or create the group
	state, err := h.getOrCreateGroup(ctx, req, nowMs)
	if err != nil {
		if errors.Is(err, errProtocolTypeMismatch) {
			resp.ErrorCode = errInconsistentGroupProto
			return resp
		}
		if errors.Is(err, errMismatchedEndpointType) {
			resp.ErrorCode = errMismatchedEndpointTypeJoinG
			return resp
		}
		logger.Warnf("failed to get/create group", map[string]any{
			"group": req.Group,
			"error": err.Error(),
		})
		resp.ErrorCode = errGroupCoordinatorNotAvail
		return resp
	}

	// For v4+, MEMBER_ID_REQUIRED on first join
	if version >= 4 && isNewMember && state.State != groups.GroupStateEmpty {
		memberID := h.generateMemberID(clientID)
		resp.ErrorCode = errMemberIDRequired
		resp.MemberID = memberID
		return resp
	}

	// Generate or validate member ID
	memberID := req.MemberID
	if isNewMember {
		memberID = h.generateMemberID(clientID)
	}

	// Check max group size
	if h.cfg.MaxGroupSize > 0 {
		members, err := h.store.ListMembers(ctx, req.Group)
		if err == nil && len(members) >= h.cfg.MaxGroupSize {
			// Check if this member is already in the group
			found := false
			for _, m := range members {
				if m.MemberID == memberID {
					found = true
					break
				}
			}
			if !found {
				resp.ErrorCode = errGroupMaxSizeReached
				return resp
			}
		}
	}

	// Encode supported protocols for storage
	protocolsData := h.encodeProtocols(req.Protocols)

	// Add or update member in the group
	err = h.addOrUpdateMember(ctx, req, memberID, clientID, protocolsData, nowMs)
	if err != nil {
		if errors.Is(err, groups.ErrMemberNotFound) {
			resp.ErrorCode = errUnknownMemberID
			return resp
		}
		logger.Warnf("failed to add member", map[string]any{
			"group":  req.Group,
			"member": memberID,
			"error":  err.Error(),
		})
		resp.ErrorCode = errGroupCoordinatorNotAvail
		return resp
	}

	// Initiate or join the rebalance
	return h.handleRebalance(ctx, req.Group, memberID, clientID, req.Protocols, rebalanceTimeoutMs, version, nowMs)
}

// getOrCreateGroup gets the existing group or creates a new one.
func (h *JoinGroupHandler) getOrCreateGroup(ctx context.Context, req *kmsg.JoinGroupRequest, nowMs int64) (*groups.GroupState, error) {
	logger := logging.FromCtx(ctx)

	// First check the group type to handle protocol interop
	groupType, typeErr := h.store.GetGroupType(ctx, req.Group)
	if typeErr == nil && groupType == groups.GroupTypeConsumer {
		// Group exists as consumer protocol - check if empty
		members, listErr := h.store.ListMembers(ctx, req.Group)
		if listErr != nil {
			return nil, listErr
		}

		if len(members) > 0 {
			// Group has members using consumer protocol - return MISMATCHED_ENDPOINT_TYPE
			logger.Warnf("join group received for consumer protocol group with active members", map[string]any{
				"group":       req.Group,
				"groupType":   groupType,
				"memberCount": len(members),
			})
			return nil, errMismatchedEndpointType
		}

		// Group is empty - convert to classic protocol
		logger.Infof("converting empty group from consumer to classic protocol", map[string]any{
			"group": req.Group,
		})
		if convertErr := h.store.ConvertGroupType(ctx, req.Group, groups.GroupTypeClassic); convertErr != nil {
			return nil, convertErr
		}
	}

	state, err := h.store.GetGroupState(ctx, req.Group)
	if err == nil {
		// Validate protocol type matches
		if state.ProtocolType != "" && state.ProtocolType != req.ProtocolType {
			return nil, errProtocolTypeMismatch
		}
		return state, nil
	}

	if !errors.Is(err, groups.ErrGroupNotFound) {
		return nil, err
	}

	// Create new group
	state, err = h.store.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:          req.Group,
		Type:             groups.GroupTypeClassic,
		ProtocolType:     req.ProtocolType,
		SessionTimeoutMs: req.SessionTimeoutMillis,
		NowMs:            nowMs,
	})
	if err != nil {
		if errors.Is(err, groups.ErrGroupExists) {
			// Race condition - group was created by another request
			return h.store.GetGroupState(ctx, req.Group)
		}
		return nil, err
	}
	return state, nil
}

// addOrUpdateMember adds a new member or updates an existing one.
func (h *JoinGroupHandler) addOrUpdateMember(ctx context.Context, req *kmsg.JoinGroupRequest, memberID, clientID string, protocolsData []byte, nowMs int64) error {
	// Try to get existing member
	_, err := h.store.GetMember(ctx, req.Group, memberID)
	if err == nil {
		// Update existing member
		_, err = h.store.UpdateMember(ctx, groups.UpdateMemberRequest{
			GroupID:            req.Group,
			MemberID:           memberID,
			LastHeartbeatMs:    nowMs,
			SupportedProtocols: protocolsData,
		})
		return err
	}

	if !errors.Is(err, groups.ErrMemberNotFound) {
		return err
	}

	// Add new member
	var instanceID string
	if req.InstanceID != nil {
		instanceID = *req.InstanceID
	}

	_, err = h.store.AddMember(ctx, groups.AddMemberRequest{
		GroupID:            req.Group,
		MemberID:           memberID,
		ClientID:           clientID,
		ClientHost:         "", // Not available in the request
		ProtocolType:       req.ProtocolType,
		SessionTimeoutMs:   req.SessionTimeoutMillis,
		RebalanceTimeoutMs: req.RebalanceTimeoutMillis,
		GroupInstanceID:    instanceID,
		SupportedProtocols: protocolsData,
		NowMs:              nowMs,
	})
	return err
}

// handleRebalance manages the rebalance process for joining members.
func (h *JoinGroupHandler) handleRebalance(ctx context.Context, groupID, memberID, clientID string, protocols []kmsg.JoinGroupRequestProtocol, rebalanceTimeoutMs int32, version int16, nowMs int64) *kmsg.JoinGroupResponse {
	h.mu.Lock()
	rs, exists := h.rebalanceStates[groupID]
	if !exists {
		rs = &rebalanceState{
			pending:   make(map[string]*pendingJoin),
			timeoutMs: rebalanceTimeoutMs,
		}
		h.rebalanceStates[groupID] = rs
	}
	h.mu.Unlock()

	rs.mu.Lock()

	// Check if this member is already waiting
	if pj, ok := rs.pending[memberID]; ok {
		// Already waiting - update the channel
		rs.mu.Unlock()
		select {
		case resp := <-pj.ch:
			return resp
		case <-ctx.Done():
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.SetVersion(version)
			resp.ErrorCode = errRebalanceInProgress
			resp.Generation = -1
			return resp
		}
	}

	// Create a channel for this member to wait on
	ch := make(chan *kmsg.JoinGroupResponse, 1)
	rs.pending[memberID] = &pendingJoin{
		ch:       ch,
		memberID: memberID,
		clientID: clientID,
	}

	isFirst := len(rs.pending) == 1

	// Extend timeout if needed (use max of all members' rebalance timeouts)
	if rebalanceTimeoutMs > rs.timeoutMs {
		rs.timeoutMs = rebalanceTimeoutMs
	}

	if isFirst {
		// First member to join - start the timer
		timeout := time.Duration(rs.timeoutMs) * time.Millisecond
		rebalanceCtx := context.WithoutCancel(ctx)
		rs.timer = time.AfterFunc(timeout, func() {
			h.completeRebalance(rebalanceCtx, groupID, version)
		})
	} else if rs.timer != nil {
		// Additional member joined - reset the timer with initial delay
		// Per Kafka protocol, give a grace period for other members
		rs.timer.Stop()
		timeout := time.Duration(rs.timeoutMs) * time.Millisecond
		rebalanceCtx := context.WithoutCancel(ctx)
		rs.timer = time.AfterFunc(timeout, func() {
			h.completeRebalance(rebalanceCtx, groupID, version)
		})
	}

	rs.mu.Unlock()

	// Wait for rebalance to complete or context to be cancelled
	select {
	case resp := <-ch:
		return resp
	case <-ctx.Done():
		// Context cancelled - remove from pending
		h.mu.Lock()
		if rs, ok := h.rebalanceStates[groupID]; ok {
			rs.mu.Lock()
			delete(rs.pending, memberID)
			rs.mu.Unlock()
		}
		h.mu.Unlock()

		resp := kmsg.NewPtrJoinGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errRebalanceInProgress
		resp.Generation = -1
		return resp
	}
}

// completeRebalance completes the rebalance and notifies all waiting members.
func (h *JoinGroupHandler) completeRebalance(ctx context.Context, groupID string, version int16) {
	h.mu.Lock()
	rs, exists := h.rebalanceStates[groupID]
	if !exists {
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()

	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(rs.pending) == 0 {
		return
	}

	// Collect all members
	members := make([]string, 0, len(rs.pending))
	for memberID := range rs.pending {
		members = append(members, memberID)
	}

	// Select protocol (use first common protocol)
	selectedProtocol, err := h.selectProtocol(ctx, groupID)
	if err != nil {
		errCode := errGroupCoordinatorNotAvail
		if errors.Is(err, errNoCommonProtocol) {
			errCode = errInconsistentGroupProto
		}
		for _, pj := range rs.pending {
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.SetVersion(version)
			resp.ErrorCode = errCode
			resp.Generation = -1
			select {
			case pj.ch <- resp:
			default:
			}
		}
		rs.pending = make(map[string]*pendingJoin)
		return
	}

	// Select leader (first member to join)
	leaderID := members[0]

	// Increment generation and update group state
	nowMs := time.Now().UnixMilli()
	state, err := h.store.TransitionState(ctx, groups.TransitionStateRequest{
		GroupID:          groupID,
		ToState:          groups.GroupStateCompletingRebalance,
		IncrementGen:     true,
		Leader:           leaderID,
		Protocol:         selectedProtocol,
		ClearAssignments: true,
		NowMs:            nowMs,
	})
	if err != nil {
		// Error - notify all members with error
		for _, pj := range rs.pending {
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.SetVersion(version)
			resp.ErrorCode = errGroupCoordinatorNotAvail
			resp.Generation = -1
			select {
			case pj.ch <- resp:
			default:
			}
		}
		rs.pending = make(map[string]*pendingJoin)
		return
	}

	generation := state.Generation
	rs.generation = generation
	rs.leaderID = leaderID
	rs.protocolType = state.ProtocolType
	rs.protocol = selectedProtocol

	// Build member list for the leader
	memberList := h.buildMemberList(ctx, groupID, version, selectedProtocol)

	// Notify all members
	for memberID, pj := range rs.pending {
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errJoinGroupNone
		resp.Generation = generation
		resp.Protocol = &selectedProtocol
		if version >= 7 {
			resp.ProtocolType = &state.ProtocolType
		}
		resp.LeaderID = leaderID
		resp.MemberID = memberID

		// Only the leader receives the member list
		if memberID == leaderID {
			resp.Members = memberList
		}

		select {
		case pj.ch <- resp:
		default:
		}
	}

	// Clear pending joins
	rs.pending = make(map[string]*pendingJoin)
}

// selectProtocol selects a protocol that all members support.
func (h *JoinGroupHandler) selectProtocol(ctx context.Context, groupID string) (string, error) {
	members, err := h.store.ListMembers(ctx, groupID)
	if err != nil || len(members) == 0 {
		if err == nil {
			err = errors.New("join group: no members")
		}
		return "", err
	}

	firstProtocols := h.decodeProtocols(members[0].SupportedProtocols)
	if len(firstProtocols) == 0 {
		return "", errNoCommonProtocol
	}

	for _, candidate := range firstProtocols {
		allSupport := true
		for _, m := range members[1:] {
			supported := false
			for _, p := range h.decodeProtocols(m.SupportedProtocols) {
				if p.Name == candidate.Name {
					supported = true
					break
				}
			}
			if !supported {
				allSupport = false
				break
			}
		}
		if allSupport {
			return candidate.Name, nil
		}
	}

	return "", errNoCommonProtocol
}

// buildMemberList builds the member list for the leader response.
func (h *JoinGroupHandler) buildMemberList(ctx context.Context, groupID string, version int16, selectedProtocol string) []kmsg.JoinGroupResponseMember {
	members, err := h.store.ListMembers(ctx, groupID)
	if err != nil {
		return nil
	}

	result := make([]kmsg.JoinGroupResponseMember, 0, len(members))
	for _, m := range members {
		member := kmsg.NewJoinGroupResponseMember()
		member.MemberID = m.MemberID

		if version >= 5 && m.GroupInstanceID != "" {
			member.InstanceID = &m.GroupInstanceID
		}

		// Get the protocol metadata for the selected protocol
		protocols := h.decodeProtocols(m.SupportedProtocols)
		for _, p := range protocols {
			if p.Name == selectedProtocol {
				member.ProtocolMetadata = p.Metadata
				break
			}
		}
		if len(member.ProtocolMetadata) == 0 && len(protocols) > 0 {
			member.ProtocolMetadata = protocols[0].Metadata
		}

		result = append(result, member)
	}

	return result
}

// generateMemberID generates a unique member ID.
func (h *JoinGroupHandler) generateMemberID(clientID string) string {
	if clientID == "" {
		clientID = "consumer"
	}
	return clientID + "-" + uuid.New().String()
}

// encodeProtocols encodes protocols to JSON for storage.
func (h *JoinGroupHandler) encodeProtocols(protocols []kmsg.JoinGroupRequestProtocol) []byte {
	// Simple encoding: store as length-prefixed name + metadata pairs
	var buf []byte
	for _, p := range protocols {
		// Name length (4 bytes) + name + metadata length (4 bytes) + metadata
		nameBytes := []byte(p.Name)
		buf = append(buf, byte(len(nameBytes)>>24), byte(len(nameBytes)>>16), byte(len(nameBytes)>>8), byte(len(nameBytes)))
		buf = append(buf, nameBytes...)
		buf = append(buf, byte(len(p.Metadata)>>24), byte(len(p.Metadata)>>16), byte(len(p.Metadata)>>8), byte(len(p.Metadata)))
		buf = append(buf, p.Metadata...)
	}
	return buf
}

// decodeProtocols decodes protocols from storage.
func (h *JoinGroupHandler) decodeProtocols(data []byte) []kmsg.JoinGroupRequestProtocol {
	var protocols []kmsg.JoinGroupRequestProtocol
	offset := 0
	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}
		nameLen := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
		if offset+nameLen > len(data) {
			break
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		if offset+4 > len(data) {
			break
		}
		metaLen := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
		if offset+metaLen > len(data) {
			break
		}
		metadata := data[offset : offset+metaLen]
		offset += metaLen

		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name = name
		p.Metadata = metadata
		protocols = append(protocols, p)
	}
	return protocols
}

// RemoveMember removes a member from the group (called on session timeout).
func (h *JoinGroupHandler) RemoveMember(ctx context.Context, groupID, memberID string) error {
	return h.store.RemoveMember(ctx, groupID, memberID)
}

// GetSessionTimeoutMembers returns members that have exceeded their session timeout.
func (h *JoinGroupHandler) GetSessionTimeoutMembers(ctx context.Context, groupID string, nowMs int64) ([]string, error) {
	members, err := h.store.ListMembers(ctx, groupID)
	if err != nil {
		return nil, err
	}

	var timedOut []string
	for _, m := range members {
		if nowMs-m.LastHeartbeatMs > int64(m.SessionTimeoutMs) {
			timedOut = append(timedOut, m.MemberID)
		}
	}
	return timedOut, nil
}
