package protocol

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for DeleteGroups responses.
const (
	errDeleteGroupsNone                int16 = 0
	errDeleteGroupsCoordinatorNotAvail int16 = 15
	errDeleteGroupsNotCoordinator      int16 = 16
	errDeleteGroupsInvalidGroupID      int16 = 24
	errDeleteGroupsGroupAuthFailed     int16 = 30
	errDeleteGroupsNonEmptyGroup       int16 = 68
	errDeleteGroupsGroupIDNotFound     int16 = 69
)

// DeleteGroupsHandler handles DeleteGroups (key 42) requests.
type DeleteGroupsHandler struct {
	store        *groups.Store
	leaseManager *groups.LeaseManager
	meta         metadata.MetadataStore
}

// NewDeleteGroupsHandler creates a new DeleteGroups handler.
func NewDeleteGroupsHandler(store *groups.Store, leaseManager *groups.LeaseManager, meta metadata.MetadataStore) *DeleteGroupsHandler {
	return &DeleteGroupsHandler{
		store:        store,
		leaseManager: leaseManager,
		meta:         meta,
	}
}

// Handle processes a DeleteGroups request.
func (h *DeleteGroupsHandler) Handle(ctx context.Context, version int16, req *kmsg.DeleteGroupsRequest) *kmsg.DeleteGroupsResponse {
	resp := kmsg.NewPtrDeleteGroupsResponse()
	resp.SetVersion(version)

	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	resp.Groups = make([]kmsg.DeleteGroupsResponseGroup, len(req.Groups))
	for i, groupID := range req.Groups {
		resp.Groups[i] = h.deleteGroup(ctx, groupID)
	}

	return resp
}

// deleteGroup deletes a single group.
func (h *DeleteGroupsHandler) deleteGroup(ctx context.Context, groupID string) kmsg.DeleteGroupsResponseGroup {
	result := kmsg.NewDeleteGroupsResponseGroup()
	result.Group = groupID

	logger := logging.FromCtx(ctx)

	if groupID == "" {
		result.ErrorCode = errDeleteGroupsInvalidGroupID
		return result
	}

	// Check coordinator lease if available
	if h.leaseManager != nil {
		leaseResult, err := h.leaseManager.AcquireLease(ctx, groupID)
		if err != nil {
			logger.Warnf("failed to acquire lease", map[string]any{
				"group": groupID,
				"error": err.Error(),
			})
			result.ErrorCode = errDeleteGroupsCoordinatorNotAvail
			return result
		}
		if !leaseResult.Acquired {
			result.ErrorCode = errDeleteGroupsNotCoordinator
			return result
		}
	}

	// Get group state
	state, err := h.store.GetGroupState(ctx, groupID)
	if err != nil {
		if errors.Is(err, groups.ErrGroupNotFound) {
			result.ErrorCode = errDeleteGroupsGroupIDNotFound
			return result
		}
		logger.Warnf("failed to get group state", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		result.ErrorCode = errDeleteGroupsCoordinatorNotAvail
		return result
	}

	// Check if group has active members (not Empty or Dead)
	if state.State != groups.GroupStateEmpty && state.State != groups.GroupStateDead {
		members, err := h.store.ListMembers(ctx, groupID)
		if err != nil {
			logger.Warnf("failed to list members", map[string]any{
				"group": groupID,
				"error": err.Error(),
			})
			result.ErrorCode = errDeleteGroupsCoordinatorNotAvail
			return result
		}
		if len(members) > 0 {
			result.ErrorCode = errDeleteGroupsNonEmptyGroup
			return result
		}
	}

	// Delete committed offsets for the group
	if err := h.deleteGroupOffsets(ctx, groupID); err != nil {
		logger.Warnf("failed to delete group offsets", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		result.ErrorCode = errDeleteGroupsCoordinatorNotAvail
		return result
	}

	// Delete the group (including members and assignments)
	if err := h.store.DeleteGroup(ctx, groupID); err != nil {
		logger.Warnf("failed to delete group", map[string]any{
			"group": groupID,
			"error": err.Error(),
		})
		result.ErrorCode = errDeleteGroupsCoordinatorNotAvail
		return result
	}

	logger.Infof("deleted group", map[string]any{
		"group": groupID,
	})

	result.ErrorCode = errDeleteGroupsNone
	return result
}

// deleteGroupOffsets deletes all committed offsets for a group.
func (h *DeleteGroupsHandler) deleteGroupOffsets(ctx context.Context, groupID string) error {
	if h.meta == nil {
		return nil
	}

	prefix := keys.GroupOffsetsPrefix(groupID)
	kvs, err := h.meta.List(ctx, prefix, "", 0)
	if err != nil {
		return err
	}

	if len(kvs) == 0 {
		return nil
	}

	// Delete each offset key
	for _, kv := range kvs {
		if err := h.meta.Delete(ctx, kv.Key); err != nil {
			return err
		}
	}

	return nil
}
