package protocol

import (
	"context"

	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for ListGroups responses.
const (
	errListGroupsNone                int16 = 0
	errListGroupsCoordinatorNotAvail int16 = 15
	errListGroupsAuthFailed          int16 = 30
)

// ListGroupsHandler handles ListGroups (key 16) requests.
type ListGroupsHandler struct {
	store *groups.Store
}

// NewListGroupsHandler creates a new ListGroups handler.
func NewListGroupsHandler(store *groups.Store) *ListGroupsHandler {
	return &ListGroupsHandler{
		store: store,
	}
}

// Handle processes a ListGroups request.
func (h *ListGroupsHandler) Handle(ctx context.Context, version int16, req *kmsg.ListGroupsRequest) *kmsg.ListGroupsResponse {
	resp := kmsg.NewPtrListGroupsResponse()
	resp.SetVersion(version)

	logger := logging.FromCtx(ctx)

	// Set throttle time for v1+ responses.
	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	// Get all groups from the store
	allGroups, err := h.store.ListGroups(ctx)
	if err != nil {
		logger.Warnf("failed to list groups", map[string]any{
			"error": err.Error(),
		})
		resp.ErrorCode = errListGroupsCoordinatorNotAvail
		return resp
	}

	// Build a set of state filters if provided (v4+)
	var stateFilters map[string]bool
	if version >= 4 && len(req.StatesFilter) > 0 {
		stateFilters = make(map[string]bool, len(req.StatesFilter))
		for _, state := range req.StatesFilter {
			stateFilters[state] = true
		}
	}

	// Filter groups and build response
	resp.Groups = make([]kmsg.ListGroupsResponseGroup, 0, len(allGroups))
	for _, g := range allGroups {
		// Apply state filter if provided
		if stateFilters != nil {
			if !stateFilters[string(g.State)] {
				continue
			}
		}

		group := kmsg.NewListGroupsResponseGroup()
		group.Group = g.GroupID
		group.ProtocolType = g.ProtocolType

		// For v4+, include the group state
		if version >= 4 {
			group.GroupState = string(g.State)
		}

		resp.Groups = append(resp.Groups, group)
	}

	resp.ErrorCode = errListGroupsNone
	return resp
}
