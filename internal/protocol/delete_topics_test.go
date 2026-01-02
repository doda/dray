package protocol

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDeleteTopicsHandler_BasicDeletion(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	// Create a topic first
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create streams for each partition
	partitions, err := topicStore.ListPartitions(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to list partitions: %v", err)
	}
	for _, p := range partitions {
		if err := streamManager.CreateStreamWithID(ctx, p.StreamID, "test-topic", p.Partition); err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
	}

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"test-topic"}

	resp := handler.Handle(ctx, 3, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if topicResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", topicResp.ErrorCode)
	}
	if *topicResp.Topic != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got '%s'", *topicResp.Topic)
	}

	// Verify topic was deleted from store
	_, err = topicStore.GetTopic(ctx, "test-topic")
	if err != topics.ErrTopicNotFound {
		t.Errorf("expected topic to be deleted, but got err=%v", err)
	}

	// Verify streams were marked deleted (HWM should not exist)
	for _, p := range partitions {
		_, _, err := streamManager.GetHWM(ctx, p.StreamID)
		if err != index.ErrStreamNotFound {
			t.Errorf("expected stream to be deleted for partition %d, but got err=%v", p.Partition, err)
		}
	}
}

func TestDeleteTopicsHandler_TopicNotFound(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"nonexistent-topic"}

	resp := handler.Handle(ctx, 3, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if topicResp.ErrorCode != errUnknownTopic {
		t.Errorf("expected UNKNOWN_TOPIC_OR_PARTITION error code %d, got %d", errUnknownTopic, topicResp.ErrorCode)
	}
}

func TestDeleteTopicsHandler_MultipleTopics(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	// Create topics
	for _, name := range []string{"topic-1", "topic-2"} {
		result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
			Name:           name,
			PartitionCount: 2,
			NowMs:          time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
		for _, p := range result.Partitions {
			if err := streamManager.CreateStreamWithID(ctx, p.StreamID, name, p.Partition); err != nil {
				t.Fatalf("failed to create stream: %v", err)
			}
		}
	}

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"topic-1", "topic-2", "nonexistent-topic"}

	resp := handler.Handle(ctx, 3, req)

	if len(resp.Topics) != 3 {
		t.Fatalf("expected 3 topic responses, got %d", len(resp.Topics))
	}

	// topic-1 should succeed
	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("topic-1 should succeed, got error %d", resp.Topics[0].ErrorCode)
	}

	// topic-2 should succeed
	if resp.Topics[1].ErrorCode != 0 {
		t.Errorf("topic-2 should succeed, got error %d", resp.Topics[1].ErrorCode)
	}

	// nonexistent-topic should fail
	if resp.Topics[2].ErrorCode != errUnknownTopic {
		t.Errorf("nonexistent-topic should fail with UNKNOWN_TOPIC, got error %d", resp.Topics[2].ErrorCode)
	}

	// Verify topics were deleted
	for _, name := range []string{"topic-1", "topic-2"} {
		_, err := topicStore.GetTopic(ctx, name)
		if err != topics.ErrTopicNotFound {
			t.Errorf("topic %s should be deleted, but got err=%v", name, err)
		}
	}
}

func TestDeleteTopicsHandler_WithIceberg(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	icebergCatalog := newMockIcebergCatalog()

	// Create topic
	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "iceberg-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create Iceberg table
	tableID := catalog.NewTableIdentifier([]string{"dray"}, "iceberg-topic")
	_, err = icebergCatalog.CreateTableIfMissing(ctx, tableID, catalog.CreateTableOptions{})
	if err != nil {
		t.Fatalf("failed to create Iceberg table: %v", err)
	}

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{
			IcebergEnabled: true,
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"iceberg-topic"}

	resp := handler.Handle(ctx, 3, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify Iceberg table was dropped
	exists, err := icebergCatalog.TableExists(ctx, tableID)
	if err != nil {
		t.Fatalf("failed to check table existence: %v", err)
	}
	if exists {
		t.Error("expected Iceberg table to be dropped")
	}
}

func TestDeleteTopicsHandler_IcebergDropFailureLogs(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(logging.Config{
		Level:  logging.LevelInfo,
		Format: logging.FormatJSON,
		Output: &buf,
	})
	ctx := logging.WithLoggerCtx(context.Background(), logger)
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	icebergCatalog := newMockIcebergCatalog()
	icebergCatalog.dropError = catalog.ErrCatalogUnavailable

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "iceberg-drop-fail-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{
			IcebergEnabled: true,
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"iceberg-drop-fail-topic"}

	resp := handler.Handle(ctx, 3, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("expected no error despite Iceberg drop failure, got error code %d", resp.Topics[0].ErrorCode)
	}

	entry := decodeLastLogEntry(t, &buf)
	if entry.Message != "failed to drop Iceberg table" {
		t.Fatalf("expected log message to mention Iceberg drop failure, got %q", entry.Message)
	}
	if entry.Fields["topic"] != "iceberg-drop-fail-topic" {
		t.Fatalf("expected log to include topic name, got %v", entry.Fields["topic"])
	}
	if entry.Fields["error"] != catalog.ErrCatalogUnavailable.Error() {
		t.Fatalf("expected log to include error details, got %v", entry.Fields["error"])
	}
}

func TestDeleteTopicsHandler_WithIcebergCustomNamespace(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	icebergCatalog := newMockIcebergCatalog()

	_, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "iceberg-ns-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	customNamespace := []string{"prod", "dray"}
	tableID := catalog.NewTableIdentifier(customNamespace, "iceberg-ns-topic")
	_, err = icebergCatalog.CreateTableIfMissing(ctx, tableID, catalog.CreateTableOptions{})
	if err != nil {
		t.Fatalf("failed to create Iceberg table: %v", err)
	}

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{
			IcebergEnabled:   true,
			IcebergNamespace: customNamespace,
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"iceberg-ns-topic"}

	resp := handler.Handle(ctx, 3, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	exists, err := icebergCatalog.TableExists(ctx, tableID)
	if err != nil {
		t.Fatalf("failed to check table existence: %v", err)
	}
	if exists {
		t.Error("expected Iceberg table to be dropped in custom namespace")
	}
}

func TestDeleteTopicsHandler_V6WithTopicID(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	// Create a topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "v6-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Parse the topic ID
	topicID := parseTopicID(result.Topic.TopicID)

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	// Use v6 format with topic ID
	req := kmsg.NewPtrDeleteTopicsRequest()
	topicReq := kmsg.NewDeleteTopicsRequestTopic()
	topicReq.TopicID = topicID
	req.Topics = []kmsg.DeleteTopicsRequestTopic{topicReq}

	resp := handler.Handle(ctx, 6, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if topicResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", topicResp.ErrorCode)
	}

	// Verify topic was deleted
	_, err = topicStore.GetTopic(ctx, "v6-topic")
	if err != topics.ErrTopicNotFound {
		t.Errorf("expected topic to be deleted, but got err=%v", err)
	}

	// Verify TopicID is in response
	if topicResp.TopicID != topicID {
		t.Errorf("expected TopicID in response to match, got %v", topicResp.TopicID)
	}
}

func TestDeleteTopicsHandler_V6WithTopicName(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	// Create a topic
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "v6-name-topic",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	expectedTopicID := parseTopicID(result.Topic.TopicID)

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	// Use v6 format with topic name
	req := kmsg.NewPtrDeleteTopicsRequest()
	topicName := "v6-name-topic"
	topicReq := kmsg.NewDeleteTopicsRequestTopic()
	topicReq.Topic = &topicName
	req.Topics = []kmsg.DeleteTopicsRequestTopic{topicReq}

	resp := handler.Handle(ctx, 6, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if topicResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", topicResp.ErrorCode)
	}

	// Verify TopicID is in response (should be looked up from metadata)
	if topicResp.TopicID != expectedTopicID {
		t.Errorf("expected TopicID in response to match, got %v", topicResp.TopicID)
	}
}

func TestDeleteTopicsHandler_ErrorMessageInV5(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{"nonexistent"}

	resp := handler.Handle(ctx, 5, req)

	topicResp := resp.Topics[0]
	if topicResp.ErrorCode != errUnknownTopic {
		t.Errorf("expected UNKNOWN_TOPIC error, got %d", topicResp.ErrorCode)
	}
	if topicResp.ErrorMessage == nil {
		t.Error("expected ErrorMessage to be set for v5+")
	}
}

func TestDeleteTopicsHandler_ThrottleMillis(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{}

	// v0 should not have ThrottleMillis
	resp := handler.Handle(ctx, 0, req)
	if resp.ThrottleMillis != 0 {
		t.Errorf("v0 should have ThrottleMillis=0, got %d", resp.ThrottleMillis)
	}

	// v1+ should have ThrottleMillis=0
	resp = handler.Handle(ctx, 1, req)
	if resp.ThrottleMillis != 0 {
		t.Errorf("v1 should have ThrottleMillis=0, got %d", resp.ThrottleMillis)
	}
}

func TestDeleteTopicsHandler_EmptyTopicNameAndID(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewDeleteTopicsHandler(
		DeleteTopicsHandlerConfig{},
		topicStore,
		streamManager,
		nil,
	)

	// v6 format with neither name nor ID
	req := kmsg.NewPtrDeleteTopicsRequest()
	topicReq := kmsg.NewDeleteTopicsRequestTopic()
	// Both Topic and TopicID are empty/zero
	req.Topics = []kmsg.DeleteTopicsRequestTopic{topicReq}

	resp := handler.Handle(ctx, 6, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if topicResp.ErrorCode != errInvalidTopicException {
		t.Errorf("expected INVALID_TOPIC_EXCEPTION error, got %d", topicResp.ErrorCode)
	}
}

func TestFormatTopicID(t *testing.T) {
	// Test that formatTopicID correctly formats a UUID
	id := [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	expected := "01234567-89ab-cdef-0123-456789abcdef"
	result := formatTopicID(id)
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestIsZeroTopicID(t *testing.T) {
	// Test zero ID
	var zeroID [16]byte
	if !isZeroTopicID(zeroID) {
		t.Error("expected zero ID to be detected")
	}

	// Test non-zero ID
	nonZeroID := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	if isZeroTopicID(nonZeroID) {
		t.Error("expected non-zero ID to be detected")
	}
}
