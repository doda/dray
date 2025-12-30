// Package groups implements the group coordinator for consumer groups.
// Supports both classic (JoinGroup/SyncGroup) and KIP-848 protocols.
package groups

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// GroupType represents the protocol type of a consumer group.
type GroupType string

const (
	// GroupTypeClassic is the classic JoinGroup/SyncGroup protocol.
	GroupTypeClassic GroupType = "classic"
	// GroupTypeConsumer is the KIP-848 consumer protocol.
	GroupTypeConsumer GroupType = "consumer"
)

// GroupStateName represents the state of a consumer group.
type GroupStateName string

const (
	// GroupStateEmpty indicates no members.
	GroupStateEmpty GroupStateName = "Empty"
	// GroupStateStable indicates all members have assignments.
	GroupStateStable GroupStateName = "Stable"
	// GroupStatePreparingRebalance indicates rebalance is pending.
	GroupStatePreparingRebalance GroupStateName = "PreparingRebalance"
	// GroupStateCompletingRebalance indicates awaiting sync.
	GroupStateCompletingRebalance GroupStateName = "CompletingRebalance"
	// GroupStateDead indicates the group is being deleted.
	GroupStateDead GroupStateName = "Dead"
)

// Common errors.
var (
	ErrGroupNotFound      = errors.New("groups: group not found")
	ErrGroupExists        = errors.New("groups: group already exists")
	ErrMemberNotFound     = errors.New("groups: member not found")
	ErrInvalidGroupID     = errors.New("groups: invalid group id")
	ErrInvalidMemberID    = errors.New("groups: invalid member id")
	ErrTypeMismatch       = errors.New("groups: group type mismatch")
	ErrGenerationMismatch = errors.New("groups: generation mismatch")
)

// GroupState holds the state of a consumer group.
type GroupState struct {
	GroupID          string         `json:"groupId"`
	Generation       int32          `json:"generation"`
	State            GroupStateName `json:"state"`
	ProtocolType     string         `json:"protocolType,omitempty"`
	Protocol         string         `json:"protocol,omitempty"`
	Leader           string         `json:"leader,omitempty"`
	LeaderEpoch      int32          `json:"leaderEpoch,omitempty"`
	LastHeartbeatMs  int64          `json:"lastHeartbeatMs,omitempty"`
	SessionTimeoutMs int32          `json:"sessionTimeoutMs,omitempty"`
	CreatedAtMs      int64          `json:"createdAtMs"`
	UpdatedAtMs      int64          `json:"updatedAtMs"`
}

// GroupMember represents a member of a consumer group.
type GroupMember struct {
	MemberID           string   `json:"memberId"`
	GroupID            string   `json:"groupId"`
	ClientID           string   `json:"clientId"`
	ClientHost         string   `json:"clientHost"`
	ProtocolType       string   `json:"protocolType,omitempty"`
	SessionTimeoutMs   int32    `json:"sessionTimeoutMs"`
	RebalanceTimeoutMs int32    `json:"rebalanceTimeoutMs"`
	GroupInstanceID    string   `json:"groupInstanceId,omitempty"`
	LastHeartbeatMs    int64    `json:"lastHeartbeatMs"`
	JoinedAtMs         int64    `json:"joinedAtMs"`
	Metadata           []byte   `json:"metadata,omitempty"`
	SupportedProtocols []byte   `json:"supportedProtocols,omitempty"`
	MemberEpoch        int32    `json:"memberEpoch,omitempty"`      // KIP-848: member epoch
	RackID             string   `json:"rackId,omitempty"`           // KIP-848: rack for zone-aware assignment
	SubscribedTopics   []string `json:"subscribedTopics,omitempty"` // KIP-848: topic subscriptions
}

// Assignment represents the partition assignment for a member.
type Assignment struct {
	MemberID    string `json:"memberId"`
	GroupID     string `json:"groupId"`
	Generation  int32  `json:"generation"`
	Data        []byte `json:"data"`
	AssignedAt  int64  `json:"assignedAt"`
	Partitions  []byte `json:"partitions,omitempty"`
	UserData    []byte `json:"userData,omitempty"`
	TopicStates []byte `json:"topicStates,omitempty"`
}

// Store provides consumer group metadata operations backed by MetadataStore.
type Store struct {
	meta metadata.MetadataStore
}

// NewStore creates a new consumer group metadata store.
func NewStore(meta metadata.MetadataStore) *Store {
	return &Store{meta: meta}
}

// GetGroupType retrieves the group type (classic|consumer).
func (s *Store) GetGroupType(ctx context.Context, groupID string) (GroupType, error) {
	if groupID == "" {
		return "", ErrInvalidGroupID
	}

	key := keys.GroupTypeKeyPath(groupID)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("groups: get type: %w", err)
	}
	if !result.Exists {
		return "", ErrGroupNotFound
	}

	return GroupType(result.Value), nil
}

// ErrGroupNotEmpty is returned when trying to convert a group with active members.
var ErrGroupNotEmpty = errors.New("groups: group is not empty")

func formatMemberCount(count int) []byte {
	return []byte(strconv.Itoa(count))
}

func parseMemberCount(data []byte) (int, error) {
	count, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("groups: parse member count: %w", err)
	}
	if count < 0 {
		return 0, fmt.Errorf("groups: invalid member count: %d", count)
	}
	return count, nil
}

const maxGroupTxnAttempts = 5

func shouldRetryGroupTxn(err error) bool {
	return errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict)
}

// ConvertGroupType converts a group from one protocol type to another.
// This is only allowed when the group has no active members.
func (s *Store) ConvertGroupType(ctx context.Context, groupID string, newType GroupType) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}
	if newType != GroupTypeClassic && newType != GroupTypeConsumer {
		return fmt.Errorf("groups: invalid group type: %s", newType)
	}

	typeKey := keys.GroupTypeKeyPath(groupID)
	countKey := keys.GroupMembersCountKeyPath(groupID)

	return s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		// Check current type exists
		_, version, err := txn.Get(typeKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrGroupNotFound
			}
			return err
		}

		countData, countVersion, err := txn.Get(countKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrGroupNotFound
			}
			return err
		}

		count, err := parseMemberCount(countData)
		if err != nil {
			return err
		}
		if count > 0 {
			return ErrGroupNotEmpty
		}

		// Update type
		txn.PutWithVersion(countKey, formatMemberCount(count), countVersion)
		txn.PutWithVersion(typeKey, []byte(newType), version)
		return nil
	})
}

// GetGroupState retrieves the group state.
func (s *Store) GetGroupState(ctx context.Context, groupID string) (*GroupState, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	key := keys.GroupStateKeyPath(groupID)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get state: %w", err)
	}
	if !result.Exists {
		return nil, ErrGroupNotFound
	}

	var state GroupState
	if err := json.Unmarshal(result.Value, &state); err != nil {
		return nil, fmt.Errorf("groups: unmarshal state: %w", err)
	}
	return &state, nil
}

// GetGroupStateWithVersion retrieves the group state with its version.
func (s *Store) GetGroupStateWithVersion(ctx context.Context, groupID string) (*GroupState, metadata.Version, error) {
	if groupID == "" {
		return nil, 0, ErrInvalidGroupID
	}

	key := keys.GroupStateKeyPath(groupID)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, 0, fmt.Errorf("groups: get state: %w", err)
	}
	if !result.Exists {
		return nil, 0, ErrGroupNotFound
	}

	var state GroupState
	if err := json.Unmarshal(result.Value, &state); err != nil {
		return nil, 0, fmt.Errorf("groups: unmarshal state: %w", err)
	}
	return &state, result.Version, nil
}

// CreateGroupRequest holds parameters for creating a new consumer group.
type CreateGroupRequest struct {
	GroupID          string
	Type             GroupType
	ProtocolType     string
	SessionTimeoutMs int32
	NowMs            int64
}

// CreateGroup creates a new consumer group atomically.
// Creates both the type and state keys in a single transaction.
func (s *Store) CreateGroup(ctx context.Context, req CreateGroupRequest) (*GroupState, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}
	if req.Type != GroupTypeClassic && req.Type != GroupTypeConsumer {
		req.Type = GroupTypeClassic
	}

	state := GroupState{
		GroupID:          req.GroupID,
		Generation:       0,
		State:            GroupStateEmpty,
		ProtocolType:     req.ProtocolType,
		SessionTimeoutMs: req.SessionTimeoutMs,
		CreatedAtMs:      req.NowMs,
		UpdatedAtMs:      req.NowMs,
	}

	stateData, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("groups: marshal state: %w", err)
	}

	typeKey := keys.GroupTypeKeyPath(req.GroupID)
	stateKey := keys.GroupStateKeyPath(req.GroupID)
	listKey := keys.GroupStateListKeyPath(req.GroupID)
	countKey := keys.GroupMembersCountKeyPath(req.GroupID)

	err = s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		// Check if group already exists
		_, _, err := txn.Get(typeKey)
		if err == nil {
			return ErrGroupExists
		}
		if !errors.Is(err, metadata.ErrKeyNotFound) {
			return err
		}

		// Create type key
		txn.Put(typeKey, []byte(req.Type))

		// Create state key
		txn.Put(stateKey, stateData)
		txn.Put(listKey, stateData)
		txn.Put(countKey, formatMemberCount(0))

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &state, nil
}

// UpdateGroupStateRequest holds parameters for updating group state.
type UpdateGroupStateRequest struct {
	GroupID         string
	State           GroupStateName
	Generation      int32
	Leader          string
	Protocol        string
	LeaderEpoch     int32
	ExpectedVersion metadata.Version
	NowMs           int64
}

// UpdateGroupState updates the group state atomically.
// Uses CAS to ensure the expected version matches.
func (s *Store) UpdateGroupState(ctx context.Context, req UpdateGroupStateRequest) (*GroupState, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}

	stateKey := keys.GroupStateKeyPath(req.GroupID)
	listKey := keys.GroupStateListKeyPath(req.GroupID)
	var updatedState *GroupState

	err := s.meta.Txn(ctx, stateKey, func(txn metadata.Txn) error {
		data, version, err := txn.Get(stateKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrGroupNotFound
			}
			return err
		}

		if req.ExpectedVersion > 0 && version != req.ExpectedVersion {
			return metadata.ErrVersionMismatch
		}

		var state GroupState
		if err := json.Unmarshal(data, &state); err != nil {
			return fmt.Errorf("unmarshal state: %w", err)
		}

		state.State = req.State
		state.Generation = req.Generation
		state.Leader = req.Leader
		state.Protocol = req.Protocol
		state.LeaderEpoch = req.LeaderEpoch
		state.UpdatedAtMs = req.NowMs

		stateData, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("marshal state: %w", err)
		}

		txn.PutWithVersion(stateKey, stateData, version)
		txn.Put(listKey, stateData)
		updatedState = &state
		return nil
	})

	if err != nil {
		return nil, err
	}

	return updatedState, nil
}

// IncrementGeneration atomically increments the group generation.
func (s *Store) IncrementGeneration(ctx context.Context, groupID string, nowMs int64) (*GroupState, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	stateKey := keys.GroupStateKeyPath(groupID)
	listKey := keys.GroupStateListKeyPath(groupID)
	var updatedState *GroupState

	err := s.meta.Txn(ctx, stateKey, func(txn metadata.Txn) error {
		data, version, err := txn.Get(stateKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrGroupNotFound
			}
			return err
		}

		var state GroupState
		if err := json.Unmarshal(data, &state); err != nil {
			return fmt.Errorf("unmarshal state: %w", err)
		}

		state.Generation++
		state.UpdatedAtMs = nowMs

		stateData, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("marshal state: %w", err)
		}

		txn.PutWithVersion(stateKey, stateData, version)
		txn.Put(listKey, stateData)
		updatedState = &state
		return nil
	})

	if err != nil {
		return nil, err
	}

	return updatedState, nil
}

// DeleteGroup deletes a consumer group and all its members/assignments atomically.
func (s *Store) DeleteGroup(ctx context.Context, groupID string) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}

	// First, get all members and assignments
	members, err := s.ListMembers(ctx, groupID)
	if err != nil && !errors.Is(err, ErrGroupNotFound) {
		return err
	}

	typeKey := keys.GroupTypeKeyPath(groupID)
	stateKey := keys.GroupStateKeyPath(groupID)
	listKey := keys.GroupStateListKeyPath(groupID)
	countKey := keys.GroupMembersCountKeyPath(groupID)

	return s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		// Delete all member keys
		for _, m := range members {
			memberKey := keys.GroupMemberKeyPath(groupID, m.MemberID)
			txn.Delete(memberKey)

			assignmentKey := keys.GroupAssignmentKeyPath(groupID, m.MemberID)
			txn.Delete(assignmentKey)
		}

		// Delete type and state
		txn.Delete(typeKey)
		txn.Delete(stateKey)
		txn.Delete(listKey)
		txn.Delete(countKey)

		return nil
	})
}

// GroupExists checks if a group exists.
func (s *Store) GroupExists(ctx context.Context, groupID string) (bool, error) {
	if groupID == "" {
		return false, ErrInvalidGroupID
	}

	key := keys.GroupTypeKeyPath(groupID)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return result.Exists, nil
}

// GetMember retrieves a group member.
func (s *Store) GetMember(ctx context.Context, groupID, memberID string) (*GroupMember, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}
	if memberID == "" {
		return nil, ErrInvalidMemberID
	}

	key := keys.GroupMemberKeyPath(groupID, memberID)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get member: %w", err)
	}
	if !result.Exists {
		return nil, ErrMemberNotFound
	}

	var member GroupMember
	if err := json.Unmarshal(result.Value, &member); err != nil {
		return nil, fmt.Errorf("groups: unmarshal member: %w", err)
	}
	return &member, nil
}

// ListMembers returns all members of a group.
func (s *Store) ListMembers(ctx context.Context, groupID string) ([]GroupMember, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	prefix := keys.GroupMembersPrefix(groupID)
	kvs, err := s.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("groups: list members: %w", err)
	}

	members := make([]GroupMember, 0, len(kvs))
	for _, kv := range kvs {
		var m GroupMember
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			return nil, fmt.Errorf("groups: unmarshal member: %w", err)
		}
		members = append(members, m)
	}
	return members, nil
}

// AddMemberRequest holds parameters for adding a member.
type AddMemberRequest struct {
	GroupID            string
	MemberID           string
	ClientID           string
	ClientHost         string
	ProtocolType       string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	GroupInstanceID    string
	Metadata           []byte
	SupportedProtocols []byte
	NowMs              int64
	MemberEpoch        int32    // KIP-848: member epoch
	RackID             string   // KIP-848: rack for zone-aware assignment
	SubscribedTopics   []string // KIP-848: topic subscriptions
}

// AddMember adds a member to the group atomically.
func (s *Store) AddMember(ctx context.Context, req AddMemberRequest) (*GroupMember, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}
	if req.MemberID == "" {
		return nil, ErrInvalidMemberID
	}

	member := GroupMember{
		MemberID:           req.MemberID,
		GroupID:            req.GroupID,
		ClientID:           req.ClientID,
		ClientHost:         req.ClientHost,
		ProtocolType:       req.ProtocolType,
		SessionTimeoutMs:   req.SessionTimeoutMs,
		RebalanceTimeoutMs: req.RebalanceTimeoutMs,
		GroupInstanceID:    req.GroupInstanceID,
		LastHeartbeatMs:    req.NowMs,
		JoinedAtMs:         req.NowMs,
		Metadata:           req.Metadata,
		SupportedProtocols: req.SupportedProtocols,
		MemberEpoch:        req.MemberEpoch,
		RackID:             req.RackID,
		SubscribedTopics:   req.SubscribedTopics,
	}

	memberData, err := json.Marshal(member)
	if err != nil {
		return nil, fmt.Errorf("groups: marshal member: %w", err)
	}

	typeKey := keys.GroupTypeKeyPath(req.GroupID)
	memberKey := keys.GroupMemberKeyPath(req.GroupID, req.MemberID)
	countKey := keys.GroupMembersCountKeyPath(req.GroupID)

	var lastErr error
	for attempt := 0; attempt < maxGroupTxnAttempts; attempt++ {
		lastErr = s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
			// Verify group exists
			_, _, err := txn.Get(typeKey)
			if err != nil {
				if errors.Is(err, metadata.ErrKeyNotFound) {
					return ErrGroupNotFound
				}
				return err
			}

			countData, countVersion, err := txn.Get(countKey)
			if err != nil {
				if errors.Is(err, metadata.ErrKeyNotFound) {
					return ErrGroupNotFound
				}
				return err
			}
			count, err := parseMemberCount(countData)
			if err != nil {
				return err
			}

			txn.PutWithVersion(countKey, formatMemberCount(count+1), countVersion)
			txn.Put(memberKey, memberData)
			return nil
		})
		if lastErr == nil {
			return &member, nil
		}
		if !shouldRetryGroupTxn(lastErr) {
			return nil, lastErr
		}
	}

	return nil, lastErr
}

// UpdateMemberRequest holds parameters for updating a member.
type UpdateMemberRequest struct {
	GroupID            string
	MemberID           string
	LastHeartbeatMs    int64
	Metadata           []byte
	SupportedProtocols []byte
}

// UpdateMember updates a member atomically.
func (s *Store) UpdateMember(ctx context.Context, req UpdateMemberRequest) (*GroupMember, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}
	if req.MemberID == "" {
		return nil, ErrInvalidMemberID
	}

	memberKey := keys.GroupMemberKeyPath(req.GroupID, req.MemberID)
	var updatedMember *GroupMember

	err := s.meta.Txn(ctx, memberKey, func(txn metadata.Txn) error {
		data, version, err := txn.Get(memberKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrMemberNotFound
			}
			return err
		}

		var member GroupMember
		if err := json.Unmarshal(data, &member); err != nil {
			return fmt.Errorf("unmarshal member: %w", err)
		}

		if req.LastHeartbeatMs > 0 {
			member.LastHeartbeatMs = req.LastHeartbeatMs
		}
		if req.Metadata != nil {
			member.Metadata = req.Metadata
		}
		if req.SupportedProtocols != nil {
			member.SupportedProtocols = req.SupportedProtocols
		}

		memberData, err := json.Marshal(member)
		if err != nil {
			return fmt.Errorf("marshal member: %w", err)
		}

		txn.PutWithVersion(memberKey, memberData, version)
		updatedMember = &member
		return nil
	})

	if err != nil {
		return nil, err
	}

	return updatedMember, nil
}

// UpdateMemberHeartbeat updates a member's last heartbeat time.
func (s *Store) UpdateMemberHeartbeat(ctx context.Context, groupID, memberID string, nowMs int64) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}
	if memberID == "" {
		return ErrInvalidMemberID
	}

	memberKey := keys.GroupMemberKeyPath(groupID, memberID)

	return s.meta.Txn(ctx, memberKey, func(txn metadata.Txn) error {
		data, version, err := txn.Get(memberKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrMemberNotFound
			}
			return err
		}

		var member GroupMember
		if err := json.Unmarshal(data, &member); err != nil {
			return fmt.Errorf("unmarshal member: %w", err)
		}

		member.LastHeartbeatMs = nowMs

		memberData, err := json.Marshal(member)
		if err != nil {
			return fmt.Errorf("marshal member: %w", err)
		}

		txn.PutWithVersion(memberKey, memberData, version)
		return nil
	})
}

// UpdateConsumerMemberRequest holds parameters for updating a KIP-848 consumer member.
type UpdateConsumerMemberRequest struct {
	GroupID          string
	MemberID         string
	MemberEpoch      int32
	LastHeartbeatMs  int64
	SubscribedTopics []string
	RackID           string
}

// UpdateConsumerMember updates a KIP-848 consumer member atomically.
// Returns the updated member.
func (s *Store) UpdateConsumerMember(ctx context.Context, req UpdateConsumerMemberRequest) (*GroupMember, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}
	if req.MemberID == "" {
		return nil, ErrInvalidMemberID
	}

	memberKey := keys.GroupMemberKeyPath(req.GroupID, req.MemberID)
	var updatedMember *GroupMember

	err := s.meta.Txn(ctx, memberKey, func(txn metadata.Txn) error {
		data, version, err := txn.Get(memberKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrMemberNotFound
			}
			return err
		}

		var member GroupMember
		if err := json.Unmarshal(data, &member); err != nil {
			return fmt.Errorf("unmarshal member: %w", err)
		}

		member.MemberEpoch = req.MemberEpoch
		member.LastHeartbeatMs = req.LastHeartbeatMs
		if req.SubscribedTopics != nil {
			member.SubscribedTopics = req.SubscribedTopics
		}
		if req.RackID != "" {
			member.RackID = req.RackID
		}

		memberData, err := json.Marshal(member)
		if err != nil {
			return fmt.Errorf("marshal member: %w", err)
		}

		txn.PutWithVersion(memberKey, memberData, version)
		updatedMember = &member
		return nil
	})

	if err != nil {
		return nil, err
	}

	return updatedMember, nil
}

// RemoveMember removes a member from the group.
func (s *Store) RemoveMember(ctx context.Context, groupID, memberID string) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}
	if memberID == "" {
		return ErrInvalidMemberID
	}

	memberKey := keys.GroupMemberKeyPath(groupID, memberID)
	assignmentKey := keys.GroupAssignmentKeyPath(groupID, memberID)
	typeKey := keys.GroupTypeKeyPath(groupID)
	countKey := keys.GroupMembersCountKeyPath(groupID)

	var lastErr error
	for attempt := 0; attempt < maxGroupTxnAttempts; attempt++ {
		lastErr = s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
			// Delete both member and assignment
			memberExists := true
			if _, _, err := txn.Get(memberKey); err != nil {
				if errors.Is(err, metadata.ErrKeyNotFound) {
					memberExists = false
				} else {
					return err
				}
			}

			if memberExists {
				countData, countVersion, err := txn.Get(countKey)
				if err != nil {
					if errors.Is(err, metadata.ErrKeyNotFound) {
						return ErrGroupNotFound
					}
					return err
				}
				count, err := parseMemberCount(countData)
				if err != nil {
					return err
				}
				if count == 0 {
					return fmt.Errorf("groups: member count underflow")
				}
				txn.PutWithVersion(countKey, formatMemberCount(count-1), countVersion)
			}

			txn.Delete(memberKey)
			txn.Delete(assignmentKey)
			return nil
		})
		if lastErr == nil {
			return nil
		}
		if !shouldRetryGroupTxn(lastErr) {
			return lastErr
		}
	}

	return lastErr
}

// GetAssignment retrieves a member's assignment.
func (s *Store) GetAssignment(ctx context.Context, groupID, memberID string) (*Assignment, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}
	if memberID == "" {
		return nil, ErrInvalidMemberID
	}

	key := keys.GroupAssignmentKeyPath(groupID, memberID)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get assignment: %w", err)
	}
	if !result.Exists {
		return nil, nil // No assignment is not an error
	}

	var assignment Assignment
	if err := json.Unmarshal(result.Value, &assignment); err != nil {
		return nil, fmt.Errorf("groups: unmarshal assignment: %w", err)
	}
	return &assignment, nil
}

// ListAssignments returns all assignments for a group.
func (s *Store) ListAssignments(ctx context.Context, groupID string) ([]Assignment, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	prefix := keys.GroupAssignmentsPrefix(groupID)
	kvs, err := s.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("groups: list assignments: %w", err)
	}

	assignments := make([]Assignment, 0, len(kvs))
	for _, kv := range kvs {
		var a Assignment
		if err := json.Unmarshal(kv.Value, &a); err != nil {
			return nil, fmt.Errorf("groups: unmarshal assignment: %w", err)
		}
		assignments = append(assignments, a)
	}
	return assignments, nil
}

// SetAssignmentRequest holds parameters for setting an assignment.
type SetAssignmentRequest struct {
	GroupID     string
	MemberID    string
	Generation  int32
	Data        []byte
	Partitions  []byte
	UserData    []byte
	TopicStates []byte
	NowMs       int64
}

// SetAssignment sets or updates a member's assignment atomically.
func (s *Store) SetAssignment(ctx context.Context, req SetAssignmentRequest) (*Assignment, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}
	if req.MemberID == "" {
		return nil, ErrInvalidMemberID
	}

	assignment := Assignment{
		MemberID:    req.MemberID,
		GroupID:     req.GroupID,
		Generation:  req.Generation,
		Data:        req.Data,
		AssignedAt:  req.NowMs,
		Partitions:  req.Partitions,
		UserData:    req.UserData,
		TopicStates: req.TopicStates,
	}

	assignmentData, err := json.Marshal(assignment)
	if err != nil {
		return nil, fmt.Errorf("groups: marshal assignment: %w", err)
	}

	typeKey := keys.GroupTypeKeyPath(req.GroupID)
	assignmentKey := keys.GroupAssignmentKeyPath(req.GroupID, req.MemberID)

	err = s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		// Verify group exists
		_, _, err := txn.Get(typeKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrGroupNotFound
			}
			return err
		}

		txn.Put(assignmentKey, assignmentData)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &assignment, nil
}

// DeleteAssignment deletes a member's assignment.
func (s *Store) DeleteAssignment(ctx context.Context, groupID, memberID string) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}
	if memberID == "" {
		return ErrInvalidMemberID
	}

	key := keys.GroupAssignmentKeyPath(groupID, memberID)
	return s.meta.Delete(ctx, key)
}

// SetAllAssignmentsRequest holds parameters for setting all assignments atomically.
type SetAllAssignmentsRequest struct {
	GroupID     string
	Generation  int32
	Assignments map[string][]byte // memberID -> assignment data
	NowMs       int64
}

// SetAllAssignments sets assignments for all members atomically.
// This is used during SyncGroup to distribute the leader's assignment.
func (s *Store) SetAllAssignments(ctx context.Context, req SetAllAssignmentsRequest) error {
	if req.GroupID == "" {
		return ErrInvalidGroupID
	}

	typeKey := keys.GroupTypeKeyPath(req.GroupID)

	return s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		// Verify group exists
		_, _, err := txn.Get(typeKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrGroupNotFound
			}
			return err
		}

		// Set all assignments
		for memberID, data := range req.Assignments {
			assignment := Assignment{
				MemberID:   memberID,
				GroupID:    req.GroupID,
				Generation: req.Generation,
				Data:       data,
				AssignedAt: req.NowMs,
			}

			assignmentData, err := json.Marshal(assignment)
			if err != nil {
				return fmt.Errorf("marshal assignment: %w", err)
			}

			assignmentKey := keys.GroupAssignmentKeyPath(req.GroupID, memberID)
			txn.Put(assignmentKey, assignmentData)
		}

		return nil
	})
}

// ClearAllAssignments deletes all assignments for a group.
func (s *Store) ClearAllAssignments(ctx context.Context, groupID string) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}

	// Get all current assignments
	assignments, err := s.ListAssignments(ctx, groupID)
	if err != nil {
		return err
	}

	if len(assignments) == 0 {
		return nil
	}

	typeKey := keys.GroupTypeKeyPath(groupID)

	return s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		for _, a := range assignments {
			key := keys.GroupAssignmentKeyPath(groupID, a.MemberID)
			txn.Delete(key)
		}
		return nil
	})
}

// TransitionStateRequest holds parameters for a state transition.
type TransitionStateRequest struct {
	GroupID            string
	FromState          GroupStateName
	ToState            GroupStateName
	IncrementGen       bool
	Leader             string
	Protocol           string
	ClearAssignments   bool
	ExpectedGeneration int32
	NowMs              int64
}

// TransitionState atomically transitions the group to a new state.
// Optionally increments generation and clears assignments.
func (s *Store) TransitionState(ctx context.Context, req TransitionStateRequest) (*GroupState, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}

	stateKey := keys.GroupStateKeyPath(req.GroupID)
	listKey := keys.GroupStateListKeyPath(req.GroupID)
	var updatedState *GroupState

	// If we need to clear assignments, get them first
	var assignments []Assignment
	if req.ClearAssignments {
		var err error
		assignments, err = s.ListAssignments(ctx, req.GroupID)
		if err != nil {
			return nil, err
		}
	}

	err := s.meta.Txn(ctx, stateKey, func(txn metadata.Txn) error {
		data, version, err := txn.Get(stateKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrGroupNotFound
			}
			return err
		}

		var state GroupState
		if err := json.Unmarshal(data, &state); err != nil {
			return fmt.Errorf("unmarshal state: %w", err)
		}

		// Validate current state if specified
		if req.FromState != "" && state.State != req.FromState {
			return fmt.Errorf("groups: invalid state transition from %s (expected %s)", state.State, req.FromState)
		}

		// Validate generation if specified
		if req.ExpectedGeneration > 0 && state.Generation != req.ExpectedGeneration {
			return ErrGenerationMismatch
		}

		// Update state
		state.State = req.ToState
		state.UpdatedAtMs = req.NowMs

		if req.IncrementGen {
			state.Generation++
		}
		if req.Leader != "" {
			state.Leader = req.Leader
		}
		if req.Protocol != "" {
			state.Protocol = req.Protocol
		}

		stateData, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("marshal state: %w", err)
		}

		txn.PutWithVersion(stateKey, stateData, version)
		txn.Put(listKey, stateData)

		// Clear assignments if requested
		if req.ClearAssignments {
			for _, a := range assignments {
				key := keys.GroupAssignmentKeyPath(req.GroupID, a.MemberID)
				txn.Delete(key)
			}
		}

		updatedState = &state
		return nil
	})

	if err != nil {
		return nil, err
	}

	return updatedState, nil
}

// ListGroups returns all consumer groups.
func (s *Store) ListGroups(ctx context.Context) ([]GroupState, error) {
	// List all group state listing keys to find all groups
	prefix := keys.GroupStateListPrefix()
	kvs, err := s.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("groups: list groups: %w", err)
	}

	var groups []GroupState

	for _, kv := range kvs {
		// Only process state keys
		if !isGroupStateListKey(kv.Key) {
			continue
		}

		var state GroupState
		if err := json.Unmarshal(kv.Value, &state); err != nil {
			continue
		}

		groups = append(groups, state)
	}

	return groups, nil
}

// isGroupStateListKey checks if a key is a group state listing key.
func isGroupStateListKey(key string) bool {
	// Format: /dray/v1/groups-state/<groupId>
	prefix := keys.GroupStateListPrefix()
	if len(key) <= len(prefix) {
		return false
	}
	rest := key[len(prefix):]
	return !strings.Contains(rest, "/")
}

// =============================================================================
// Committed Offset Storage
// =============================================================================

// CommittedOffset represents a committed offset for a topic-partition.
type CommittedOffset struct {
	GroupID         string `json:"groupId"`
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	Offset          int64  `json:"offset"`
	LeaderEpoch     int32  `json:"leaderEpoch,omitempty"` // -1 means not set
	Metadata        string `json:"metadata,omitempty"`
	CommitTimestamp int64  `json:"commitTimestamp"` // Unix milliseconds
	ExpireTimestamp int64  `json:"expireTimestamp"` // Unix milliseconds, -1 means no expiry
}

// CommitOffsetRequest holds parameters for committing an offset.
type CommitOffsetRequest struct {
	GroupID         string
	Topic           string
	Partition       int32
	Offset          int64
	LeaderEpoch     int32  // -1 means not set
	Metadata        string // Optional metadata
	RetentionTimeMs int64  // -1 means use default (no expiry until group deleted)
	NowMs           int64
}

// CommitOffset stores a committed offset for a topic-partition.
func (s *Store) CommitOffset(ctx context.Context, req CommitOffsetRequest) (*CommittedOffset, error) {
	if req.GroupID == "" {
		return nil, ErrInvalidGroupID
	}

	// Calculate expiry time
	expireTimestamp := int64(-1)
	if req.RetentionTimeMs > 0 {
		expireTimestamp = req.NowMs + req.RetentionTimeMs
	}

	offset := CommittedOffset{
		GroupID:         req.GroupID,
		Topic:           req.Topic,
		Partition:       req.Partition,
		Offset:          req.Offset,
		LeaderEpoch:     req.LeaderEpoch,
		Metadata:        req.Metadata,
		CommitTimestamp: req.NowMs,
		ExpireTimestamp: expireTimestamp,
	}

	offsetData, err := json.Marshal(offset)
	if err != nil {
		return nil, fmt.Errorf("groups: marshal offset: %w", err)
	}

	offsetKey := keys.GroupOffsetKeyPath(req.GroupID, req.Topic, req.Partition)

	// Simply put the offset - no need for a transaction here as each offset is independent
	if _, err := s.meta.Put(ctx, offsetKey, offsetData); err != nil {
		return nil, fmt.Errorf("groups: put offset: %w", err)
	}

	return &offset, nil
}

// CommitOffsets stores multiple committed offsets atomically.
func (s *Store) CommitOffsets(ctx context.Context, groupID string, offsets []CommitOffsetRequest, nowMs int64) ([]CommittedOffset, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}
	if len(offsets) == 0 {
		return nil, nil
	}

	committed := make([]CommittedOffset, len(offsets))

	// Use transaction for atomic commit of all offsets
	typeKey := keys.GroupTypeKeyPath(groupID)

	err := s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		for i, req := range offsets {
			// Calculate expiry time
			expireTimestamp := int64(-1)
			if req.RetentionTimeMs > 0 {
				expireTimestamp = nowMs + req.RetentionTimeMs
			}

			offset := CommittedOffset{
				GroupID:         groupID,
				Topic:           req.Topic,
				Partition:       req.Partition,
				Offset:          req.Offset,
				LeaderEpoch:     req.LeaderEpoch,
				Metadata:        req.Metadata,
				CommitTimestamp: nowMs,
				ExpireTimestamp: expireTimestamp,
			}

			offsetData, err := json.Marshal(offset)
			if err != nil {
				return fmt.Errorf("marshal offset: %w", err)
			}

			offsetKey := keys.GroupOffsetKeyPath(groupID, req.Topic, req.Partition)
			txn.Put(offsetKey, offsetData)
			committed[i] = offset
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return committed, nil
}

// GetCommittedOffset retrieves a committed offset for a topic-partition.
func (s *Store) GetCommittedOffset(ctx context.Context, groupID, topic string, partition int32) (*CommittedOffset, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	key := keys.GroupOffsetKeyPath(groupID, topic, partition)
	result, err := s.meta.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("groups: get offset: %w", err)
	}
	if !result.Exists {
		return nil, nil // No offset committed is not an error
	}

	var offset CommittedOffset
	if err := json.Unmarshal(result.Value, &offset); err != nil {
		return nil, fmt.Errorf("groups: unmarshal offset: %w", err)
	}
	return &offset, nil
}

// ListCommittedOffsets returns all committed offsets for a group.
func (s *Store) ListCommittedOffsets(ctx context.Context, groupID string) ([]CommittedOffset, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	prefix := keys.GroupOffsetsPrefix(groupID)
	kvs, err := s.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("groups: list offsets: %w", err)
	}

	offsets := make([]CommittedOffset, 0, len(kvs))
	for _, kv := range kvs {
		var o CommittedOffset
		if err := json.Unmarshal(kv.Value, &o); err != nil {
			return nil, fmt.Errorf("groups: unmarshal offset: %w", err)
		}
		offsets = append(offsets, o)
	}
	return offsets, nil
}

// ListCommittedOffsetsForTopic returns committed offsets for a specific topic in a group.
func (s *Store) ListCommittedOffsetsForTopic(ctx context.Context, groupID, topic string) ([]CommittedOffset, error) {
	if groupID == "" {
		return nil, ErrInvalidGroupID
	}

	prefix := keys.GroupTopicOffsetsPrefix(groupID, topic)
	kvs, err := s.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return nil, fmt.Errorf("groups: list topic offsets: %w", err)
	}

	offsets := make([]CommittedOffset, 0, len(kvs))
	for _, kv := range kvs {
		var o CommittedOffset
		if err := json.Unmarshal(kv.Value, &o); err != nil {
			return nil, fmt.Errorf("groups: unmarshal offset: %w", err)
		}
		offsets = append(offsets, o)
	}
	return offsets, nil
}

// DeleteCommittedOffset deletes a committed offset for a topic-partition.
func (s *Store) DeleteCommittedOffset(ctx context.Context, groupID, topic string, partition int32) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}

	key := keys.GroupOffsetKeyPath(groupID, topic, partition)
	return s.meta.Delete(ctx, key)
}

// DeleteAllCommittedOffsets deletes all committed offsets for a group.
func (s *Store) DeleteAllCommittedOffsets(ctx context.Context, groupID string) error {
	if groupID == "" {
		return ErrInvalidGroupID
	}

	// Get all offsets first
	offsets, err := s.ListCommittedOffsets(ctx, groupID)
	if err != nil {
		return err
	}

	if len(offsets) == 0 {
		return nil
	}

	// Delete all in a transaction
	typeKey := keys.GroupTypeKeyPath(groupID)
	return s.meta.Txn(ctx, typeKey, func(txn metadata.Txn) error {
		for _, o := range offsets {
			key := keys.GroupOffsetKeyPath(groupID, o.Topic, o.Partition)
			txn.Delete(key)
		}
		return nil
	})
}
