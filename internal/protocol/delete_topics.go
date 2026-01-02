package protocol

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka error codes for DeleteTopics.
const (
	errUnknownTopic int16 = 3 // UNKNOWN_TOPIC_OR_PARTITION
)

// DeleteTopicsHandlerConfig configures the DeleteTopics handler.
type DeleteTopicsHandlerConfig struct {
	// IcebergEnabled enables Iceberg table deletion for topics.
	IcebergEnabled bool
	// IcebergNamespace is the Iceberg namespace for tables.
	// Defaults to ["dray"] if not specified.
	IcebergNamespace []string
}

// DeleteTopicsHandler handles DeleteTopics (key 20) requests.
type DeleteTopicsHandler struct {
	cfg            DeleteTopicsHandlerConfig
	topicStore     *topics.Store
	streamManager  *index.StreamManager
	icebergCatalog catalog.Catalog
	enforcer       *auth.Enforcer
}

// NewDeleteTopicsHandler creates a new DeleteTopics handler.
func NewDeleteTopicsHandler(
	cfg DeleteTopicsHandlerConfig,
	topicStore *topics.Store,
	streamManager *index.StreamManager,
	icebergCatalog catalog.Catalog,
) *DeleteTopicsHandler {
	return &DeleteTopicsHandler{
		cfg:            cfg,
		topicStore:     topicStore,
		streamManager:  streamManager,
		icebergCatalog: icebergCatalog,
	}
}

// WithEnforcer sets the ACL enforcer for this handler.
func (h *DeleteTopicsHandler) WithEnforcer(enforcer *auth.Enforcer) *DeleteTopicsHandler {
	h.enforcer = enforcer
	return h
}

// Handle processes a DeleteTopics request.
func (h *DeleteTopicsHandler) Handle(ctx context.Context, version int16, req *kmsg.DeleteTopicsRequest) *kmsg.DeleteTopicsResponse {
	resp := kmsg.NewPtrDeleteTopicsResponse()
	resp.SetVersion(version)

	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	// Collect topics to delete from either the old (v0-v5) or new (v6+) format
	type topicToDelete struct {
		name    *string
		topicID [16]byte
	}
	var toDelete []topicToDelete

	if version <= 5 {
		// v0-v5: TopicNames is a list of topic names
		for _, name := range req.TopicNames {
			nameCopy := name
			toDelete = append(toDelete, topicToDelete{name: &nameCopy})
		}
	} else {
		// v6+: Topics is a list of {Name, TopicID}
		for _, t := range req.Topics {
			toDelete = append(toDelete, topicToDelete{
				name:    t.Topic,
				topicID: t.TopicID,
			})
		}
	}

	for _, td := range toDelete {
		topicResp := h.handleTopic(ctx, version, td.name, td.topicID)
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// handleTopic handles deletion of a single topic.
func (h *DeleteTopicsHandler) handleTopic(ctx context.Context, version int16, name *string, topicID [16]byte) kmsg.DeleteTopicsResponseTopic {
	resp := kmsg.NewDeleteTopicsResponseTopic()

	// Determine the topic name
	var topicName string
	if name != nil && *name != "" {
		topicName = *name
		resp.Topic = name
	} else if !isZeroTopicID(topicID) {
		// Look up topic by ID
		topic, err := h.topicStore.GetTopicByID(ctx, formatTopicID(topicID))
		if err != nil {
			if errors.Is(err, topics.ErrTopicNotFound) {
				resp.ErrorCode = errUnknownTopic
				if version >= 5 {
					errMsg := "Topic with specified ID not found"
					resp.ErrorMessage = &errMsg
				}
			} else {
				resp.ErrorCode = errUnknownTopicOrPartition
				if version >= 5 {
					errMsg := err.Error()
					resp.ErrorMessage = &errMsg
				}
			}
			if version >= 6 {
				resp.TopicID = topicID
			}
			return resp
		}
		topicName = topic.Name
		resp.Topic = &topic.Name
	} else {
		resp.ErrorCode = errInvalidTopicException
		if version >= 5 {
			errMsg := "Topic name or ID required"
			resp.ErrorMessage = &errMsg
		}
		return resp
	}

	// Check ACL before processing - need DELETE permission on topic
	if h.enforcer != nil {
		if errCode := h.enforcer.AuthorizeTopicFromCtx(ctx, topicName, auth.OperationDelete); errCode != nil {
			resp.ErrorCode = *errCode
			return resp
		}
	}

	// Get topic metadata before deletion for response
	topicMeta, err := h.topicStore.GetTopic(ctx, topicName)
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			resp.ErrorCode = errUnknownTopic
			if version >= 5 {
				errMsg := "Topic not found"
				resp.ErrorMessage = &errMsg
			}
		} else {
			resp.ErrorCode = errUnknownTopicOrPartition
			if version >= 5 {
				errMsg := err.Error()
				resp.ErrorMessage = &errMsg
			}
		}
		return resp
	}

	// Delete the topic from metadata store
	result, err := h.topicStore.DeleteTopic(ctx, topicName)
	if err != nil {
		if errors.Is(err, topics.ErrTopicNotFound) {
			resp.ErrorCode = errUnknownTopic
			if version >= 5 {
				errMsg := "Topic not found"
				resp.ErrorMessage = &errMsg
			}
		} else {
			resp.ErrorCode = errUnknownTopicOrPartition
			if version >= 5 {
				errMsg := err.Error()
				resp.ErrorMessage = &errMsg
			}
		}
		return resp
	}

	// Schedule stream cleanup for each partition
	// This marks the streams for later WAL/Parquet garbage collection
	for _, partition := range result.Partitions {
		// Mark stream for deletion - the actual cleanup of WAL and Parquet files
		// will be handled by the GC service asynchronously
		if h.streamManager != nil {
			_ = h.streamManager.MarkStreamDeleted(ctx, partition.StreamID)
		}
	}

	// Drop Iceberg table if enabled
	if h.cfg.IcebergEnabled && h.icebergCatalog != nil {
		namespace := h.cfg.IcebergNamespace
		if len(namespace) == 0 {
			namespace = []string{"dray"}
		}
		tableID := catalog.NewTableIdentifier(namespace, topicName)
		if err := h.icebergCatalog.DropTable(ctx, tableID); err != nil {
			// Iceberg table drop failure should not fail topic deletion
			// per spec - the topic is already deleted from metadata.
			logging.FromCtx(ctx).Errorf("failed to drop Iceberg table", map[string]any{
				"topic":     topicName,
				"namespace": namespace,
				"error":     err.Error(),
			})
		}
	}

	// Success
	resp.ErrorCode = errNone
	if version >= 6 {
		resp.TopicID = parseTopicID(topicMeta.TopicID)
	}

	return resp
}

// isZeroTopicID checks if a topic ID is all zeros (unset).
func isZeroTopicID(id [16]byte) bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return true
}

// formatTopicID formats a topic ID as a UUID string.
func formatTopicID(id [16]byte) string {
	const hex = "0123456789abcdef"
	buf := make([]byte, 36)
	j := 0
	for i := 0; i < 16; i++ {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			buf[j] = '-'
			j++
		}
		buf[j] = hex[id[i]>>4]
		buf[j+1] = hex[id[i]&0x0f]
		j += 2
	}
	return string(buf)
}
