package protocol

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for DescribeGroups responses.
const (
	errDescribeGroupsNone                   int16 = 0
	errDescribeGroupsCoordinatorNotAvail    int16 = 15
	errDescribeGroupsNotCoordinator         int16 = 16
	errDescribeGroupsInvalidGroupID         int16 = 24
	errDescribeGroupsGroupAuthFailed        int16 = 30
	errDescribeGroupsGroupIDNotFound        int16 = 69 // KIP-1043, v6+
)

// DescribeGroupsHandler handles DescribeGroups (key 15) requests.
type DescribeGroupsHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
}

// NewDescribeGroupsHandler creates a new DescribeGroups handler.
func NewDescribeGroupsHandler(store *groups.Store, leaseManager *groups.LeaseManager) *DescribeGroupsHandler {
	return &DescribeGroupsHandler{
		store:        store,
		leaseManager: leaseManager,
	}
}

// Handle processes a DescribeGroups request.
func (h *DescribeGroupsHandler) Handle(ctx context.Context, version int16, req *kmsg.DescribeGroupsRequest) *kmsg.DescribeGroupsResponse {
	resp := kmsg.NewPtrDescribeGroupsResponse()
	resp.SetVersion(version)

	logger := logging.FromCtx(ctx)

	// Process each group
	resp.Groups = make([]kmsg.DescribeGroupsResponseGroup, len(req.Groups))
	for i, groupID := range req.Groups {
		resp.Groups[i] = h.describeGroup(ctx, version, groupID, req.IncludeAuthorizedOperations, logger)
	}

	return resp
}

// describeGroup describes a single group.
func (h *DescribeGroupsHandler) describeGroup(ctx context.Context, version int16, groupID string, includeAuthOps bool, logger *logging.Logger) kmsg.DescribeGroupsResponseGroup {
	group := kmsg.NewDescribeGroupsResponseGroup()
	group.Group = groupID

	// Set default authorized operations for v3+
	if version >= 3 {
		group.AuthorizedOperations = -2147483648 // Default value per Kafka protocol
	}

	// Validate group ID
	if groupID == "" {
		group.ErrorCode = errDescribeGroupsInvalidGroupID
		return group
	}

	// Check coordinator lease if available
	if h.leaseManager != nil {
		result, err := h.leaseManager.AcquireLease(ctx, groupID)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": groupID,
				"error": err.Error(),
			})
			group.ErrorCode = errDescribeGroupsCoordinatorNotAvail
			return group
		}
		if !result.Acquired {
			group.ErrorCode = errDescribeGroupsNotCoordinator
			return group
		}
	}

	// Get group state
	state, err := h.store.GetGroupState(ctx, groupID)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			// For v6+, use GROUP_ID_NOT_FOUND; otherwise, return a dead group state
			if version >= 6 {
				group.ErrorCode = errDescribeGroupsGroupIDNotFound
				errMsg := "Group does not exist"
				group.ErrorMessage = &errMsg
			} else {
				// For older versions, return an empty response with Dead state
				group.ErrorCode = errDescribeGroupsNone
				group.State = string(groups.GroupStateDead)
				group.ProtocolType = ""
				group.Protocol = ""
				group.Members = nil
			}
			return group
		}
		logger.Warnf("failed to get group state", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		group.ErrorCode = errDescribeGroupsCoordinatorNotAvail
		return group
	}

	// Get group type
	groupType, err := h.store.GetGroupType(ctx, groupID)
	if err != nil {
		logger.Warnf("failed to get group type", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		group.ErrorCode = errDescribeGroupsCoordinatorNotAvail
		return group
	}

	// Set group state, protocol type, and protocol
	group.State = string(state.State)
	group.ProtocolType = state.ProtocolType
	group.Protocol = state.Protocol

	// If ProtocolType is empty but group type is classic, use "consumer" as default
	if group.ProtocolType == "" && groupType == groups.GroupTypeClassic {
		group.ProtocolType = "consumer"
	}

	// Get members
	members, err := h.store.ListMembers(ctx, groupID)
	if err != nil {
		logger.Warnf("failed to list members", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		group.ErrorCode = errDescribeGroupsCoordinatorNotAvail
		return group
	}

	// Get assignments for all members
	assignments, err := h.store.ListAssignments(ctx, groupID)
	if err != nil {
		logger.Warnf("failed to list assignments", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		group.ErrorCode = errDescribeGroupsCoordinatorNotAvail
		return group
	}

	// Build assignment map for quick lookup
	assignmentMap := make(map[string][]byte, len(assignments))
	for _, a := range assignments {
		assignmentMap[a.MemberID] = a.Data
	}

	// Build member responses
	group.Members = make([]kmsg.DescribeGroupsResponseGroupMember, len(members))
	for i, m := range members {
		member := kmsg.NewDescribeGroupsResponseGroupMember()
		member.MemberID = m.MemberID
		member.ClientID = m.ClientID
		member.ClientHost = m.ClientHost
		member.ProtocolMetadata = m.Metadata

		// Include assignment if available
		if assignment, ok := assignmentMap[m.MemberID]; ok {
			member.MemberAssignment = assignment
		}

		// Include instance ID for v4+
		if version >= 4 && m.GroupInstanceID != "" {
			member.InstanceID = &m.GroupInstanceID
		}

		group.Members[i] = member
	}

	// Include authorized operations for v3+ if requested
	if version >= 3 && includeAuthOps {
		// Return all operations allowed (a bitfield)
		// For now, allow all basic operations: READ, DESCRIBE
		// Bit 8 = READ, Bit 10 = DESCRIBE
		// 0x400 | 0x100 = 0x500 = 1280
		group.AuthorizedOperations = 0x500
	}

	group.ErrorCode = errDescribeGroupsNone
	return group
}
