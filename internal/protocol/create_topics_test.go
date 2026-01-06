package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/dray-io/dray/internal/iceberg/catalog"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// mockIcebergCatalog implements catalog.Catalog for testing.
type mockIcebergCatalog struct {
	tables      map[string]catalog.Table
	createError error
	dropError   error
}

func newMockIcebergCatalog() *mockIcebergCatalog {
	return &mockIcebergCatalog{
		tables: make(map[string]catalog.Table),
	}
}

func (m *mockIcebergCatalog) LoadTable(ctx context.Context, identifier catalog.TableIdentifier) (catalog.Table, error) {
	if table, ok := m.tables[catalog.TableIdentifierString(identifier)]; ok {
		return table, nil
	}
	return nil, catalog.ErrTableNotFound
}

func (m *mockIcebergCatalog) CreateTableIfMissing(ctx context.Context, identifier catalog.TableIdentifier, opts catalog.CreateTableOptions) (catalog.Table, error) {
	if m.createError != nil {
		return nil, m.createError
	}
	table := &mockTable{
		id:     identifier,
		schema: opts.Schema,
		props:  opts.Properties,
	}
	m.tables[catalog.TableIdentifierString(identifier)] = table
	return table, nil
}

func (m *mockIcebergCatalog) GetCurrentSnapshot(ctx context.Context, identifier catalog.TableIdentifier) (*table.Snapshot, error) {
	return nil, catalog.ErrSnapshotNotFound
}

func (m *mockIcebergCatalog) AppendDataFiles(ctx context.Context, identifier catalog.TableIdentifier, files []catalog.DataFile, opts *catalog.AppendFilesOptions) (*table.Snapshot, error) {
	return nil, nil
}

func (m *mockIcebergCatalog) DropTable(ctx context.Context, identifier catalog.TableIdentifier) error {
	if m.dropError != nil {
		return m.dropError
	}
	delete(m.tables, catalog.TableIdentifierString(identifier))
	return nil
}

func (m *mockIcebergCatalog) ListTables(ctx context.Context, namespace []string) ([]catalog.TableIdentifier, error) {
	return nil, nil
}

func (m *mockIcebergCatalog) TableExists(ctx context.Context, identifier catalog.TableIdentifier) (bool, error) {
	_, ok := m.tables[catalog.TableIdentifierString(identifier)]
	return ok, nil
}

func (m *mockIcebergCatalog) Close() error {
	return nil
}

// mockTable implements catalog.Table for testing.
type mockTable struct {
	id       catalog.TableIdentifier
	schema   *iceberg.Schema
	props    catalog.TableProperties
	location string
}

func (m *mockTable) Identifier() catalog.TableIdentifier { return m.id }
func (m *mockTable) Schema() *iceberg.Schema             { return m.schema }
func (m *mockTable) CurrentSnapshot(ctx context.Context) (*table.Snapshot, error) {
	return nil, catalog.ErrSnapshotNotFound
}
func (m *mockTable) Snapshots(ctx context.Context) ([]table.Snapshot, error) { return nil, nil }
func (m *mockTable) AppendFiles(ctx context.Context, files []catalog.DataFile, opts *catalog.AppendFilesOptions) (*table.Snapshot, error) {
	return nil, nil
}
func (m *mockTable) ReplaceFiles(ctx context.Context, added []catalog.DataFile, removed []catalog.DataFile, opts *catalog.ReplaceFilesOptions) (*table.Snapshot, error) {
	return nil, nil
}
func (m *mockTable) Properties() catalog.TableProperties { return m.props }
func (m *mockTable) Location() string                    { return m.location }
func (m *mockTable) Refresh(ctx context.Context) error   { return nil }
func (m *mockTable) ExpireSnapshots(ctx context.Context, olderThan time.Duration, retainLast int) error {
	return nil
}

func TestCreateTopicsHandler_BasicCreation(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil, // no iceberg catalog
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "test-topic"
	topic.NumPartitions = 5
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 7, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if topicResp.Topic != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got '%s'", topicResp.Topic)
	}
	if topicResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", topicResp.ErrorCode)
	}
	if topicResp.NumPartitions != 5 {
		t.Errorf("expected 5 partitions, got %d", topicResp.NumPartitions)
	}
	if topicResp.ReplicationFactor != 1 {
		t.Errorf("expected replication factor 1, got %d", topicResp.ReplicationFactor)
	}

	// Verify topic was created in store
	topicMeta, err := topicStore.GetTopic(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topicMeta.PartitionCount != 5 {
		t.Errorf("expected 5 partitions in store, got %d", topicMeta.PartitionCount)
	}

	// Verify streams were created with HWM = 0
	partitions, err := topicStore.ListPartitions(ctx, "test-topic")
	if err != nil {
		t.Fatalf("failed to list partitions: %v", err)
	}
	if len(partitions) != 5 {
		t.Fatalf("expected 5 partitions, got %d", len(partitions))
	}

	for _, p := range partitions {
		hwm, _, err := streamManager.GetHWM(ctx, p.StreamID)
		if err != nil {
			t.Errorf("failed to get HWM for partition %d: %v", p.Partition, err)
		}
		if hwm != 0 {
			t.Errorf("expected HWM=0 for partition %d, got %d", p.Partition, hwm)
		}
	}
}

func TestCreateTopicsHandler_DefaultPartitions(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        8,
			DefaultReplicationFactor: 3,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "default-topic"
	topic.NumPartitions = -1     // Use default
	topic.ReplicationFactor = -1 // Use default
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if topicResp.ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", topicResp.ErrorCode)
	}
	if topicResp.NumPartitions != 8 {
		t.Errorf("expected 8 default partitions, got %d", topicResp.NumPartitions)
	}
	if topicResp.ReplicationFactor != 3 {
		t.Errorf("expected 3 default replication factor, got %d", topicResp.ReplicationFactor)
	}

	// Verify topic in store
	topicMeta, err := topicStore.GetTopic(ctx, "default-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topicMeta.PartitionCount != 8 {
		t.Errorf("expected 8 partitions in store, got %d", topicMeta.PartitionCount)
	}
}

func TestCreateTopicsHandler_TopicAlreadyExists(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	// Create topic first
	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "existing-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)
	if resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("first creation should succeed, got error %d", resp.Topics[0].ErrorCode)
	}

	// Try to create again
	resp = handler.Handle(ctx, 5, req)
	if resp.Topics[0].ErrorCode != errTopicAlreadyExists {
		t.Errorf("expected TOPIC_ALREADY_EXISTS error code %d, got %d", errTopicAlreadyExists, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopicsHandler_InvalidPartitions(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "invalid-partitions"
	topic.NumPartitions = 0 // Invalid
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != errInvalidPartitions {
		t.Errorf("expected INVALID_PARTITIONS error code %d, got %d", errInvalidPartitions, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopicsHandler_InvalidReplicationFactor(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "invalid-replication"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 0 // Invalid
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != errInvalidReplicationFactor {
		t.Errorf("expected INVALID_REPLICATION_FACTOR error code %d, got %d", errInvalidReplicationFactor, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopicsHandler_EmptyTopicName(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "" // Empty name
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != errInvalidTopicException {
		t.Errorf("expected INVALID_TOPIC_EXCEPTION error code %d, got %d", errInvalidTopicException, resp.Topics[0].ErrorCode)
	}
}

func TestCreateTopicsHandler_ValidateOnly(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.ValidateOnly = true
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "validate-only-topic"
	topic.NumPartitions = 5
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}
	if resp.Topics[0].NumPartitions != 5 {
		t.Errorf("expected 5 partitions, got %d", resp.Topics[0].NumPartitions)
	}

	// Verify topic was NOT created
	_, err := topicStore.GetTopic(ctx, "validate-only-topic")
	if err != topics.ErrTopicNotFound {
		t.Errorf("expected topic not to exist, but got err=%v", err)
	}
}

func TestCreateTopicsHandler_WithConfig(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "config-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1

	// Add config entries
	retention := kmsg.NewCreateTopicsRequestTopicConfig()
	retention.Name = "retention.ms"
	retentionValue := "86400000"
	retention.Value = &retentionValue
	topic.Configs = append(topic.Configs, retention)

	cleanup := kmsg.NewCreateTopicsRequestTopicConfig()
	cleanup.Name = "cleanup.policy"
	cleanupValue := "compact"
	cleanup.Value = &cleanupValue
	topic.Configs = append(topic.Configs, cleanup)

	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify configs in response (v5+)
	if len(resp.Topics[0].Configs) != 2 {
		t.Errorf("expected 2 config entries in response, got %d", len(resp.Topics[0].Configs))
	}

	// Verify configs stored in topic
	topicMeta, err := topicStore.GetTopic(ctx, "config-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}
	if topicMeta.Config["retention.ms"] != "86400000" {
		t.Errorf("expected retention.ms=86400000, got %s", topicMeta.Config["retention.ms"])
	}
	if topicMeta.Config["cleanup.policy"] != "compact" {
		t.Errorf("expected cleanup.policy=compact, got %s", topicMeta.Config["cleanup.policy"])
	}
}

func TestCreateTopicsHandler_MultipleTopics(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()

	topic1 := kmsg.NewCreateTopicsRequestTopic()
	topic1.Topic = "topic-1"
	topic1.NumPartitions = 3
	topic1.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic1)

	topic2 := kmsg.NewCreateTopicsRequestTopic()
	topic2.Topic = "topic-2"
	topic2.NumPartitions = 5
	topic2.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic2)

	topic3 := kmsg.NewCreateTopicsRequestTopic()
	topic3.Topic = "" // Invalid - should fail
	topic3.NumPartitions = 3
	topic3.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic3)

	resp := handler.Handle(ctx, 5, req)

	if len(resp.Topics) != 3 {
		t.Fatalf("expected 3 topic responses, got %d", len(resp.Topics))
	}

	// topic-1 should succeed
	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("topic-1 should succeed, got error %d", resp.Topics[0].ErrorCode)
	}
	if resp.Topics[0].NumPartitions != 3 {
		t.Errorf("topic-1 should have 3 partitions, got %d", resp.Topics[0].NumPartitions)
	}

	// topic-2 should succeed
	if resp.Topics[1].ErrorCode != 0 {
		t.Errorf("topic-2 should succeed, got error %d", resp.Topics[1].ErrorCode)
	}
	if resp.Topics[1].NumPartitions != 5 {
		t.Errorf("topic-2 should have 5 partitions, got %d", resp.Topics[1].NumPartitions)
	}

	// topic-3 (empty name) should fail
	if resp.Topics[2].ErrorCode != errInvalidTopicException {
		t.Errorf("topic-3 should fail with INVALID_TOPIC_EXCEPTION, got error %d", resp.Topics[2].ErrorCode)
	}
}

func TestCreateTopicsHandler_WithIceberg(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	icebergCatalog := newMockIcebergCatalog()

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
			IcebergEnabled:           true,
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "iceberg-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify Iceberg table was created
	tableID := catalog.NewTableIdentifier([]string{"dray"}, "iceberg-topic")
	exists, err := icebergCatalog.TableExists(ctx, tableID)
	if err != nil {
		t.Fatalf("failed to check table existence: %v", err)
	}
	if !exists {
		t.Error("expected Iceberg table to be created")
	}
}

func TestCreateTopicsHandler_IcebergFailureDoesNotBlockTopicCreation(t *testing.T) {
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
	icebergCatalog.createError = catalog.ErrCatalogUnavailable

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
			IcebergEnabled:           true,
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "iceberg-fail-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	// Topic creation should still succeed even though Iceberg failed
	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error despite Iceberg failure, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify topic was created
	_, err := topicStore.GetTopic(ctx, "iceberg-fail-topic")
	if err != nil {
		t.Errorf("topic should exist despite Iceberg failure: %v", err)
	}

	entry := decodeLastLogEntry(t, &buf)
	if entry.Message != "failed to create Iceberg table" {
		t.Fatalf("expected log message to mention Iceberg create failure, got %q", entry.Message)
	}
	if entry.Fields["topic"] != "iceberg-fail-topic" {
		t.Fatalf("expected log to include topic name, got %v", entry.Fields["topic"])
	}
	if entry.Fields["error"] != catalog.ErrCatalogUnavailable.Error() {
		t.Fatalf("expected log to include error details, got %v", entry.Fields["error"])
	}
}

func decodeLastLogEntry(t *testing.T, buf *bytes.Buffer) logging.Entry {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[len(lines)-1] == "" {
		t.Fatal("expected log entry but buffer was empty")
	}

	var entry logging.Entry
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}
	return entry
}

func TestCreateTopicsHandler_TopicIDInV7Response(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "topicid-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 7, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify TopicID is set (non-zero)
	var zeroID [16]byte
	if resp.Topics[0].TopicID == zeroID {
		t.Error("expected non-zero TopicID in v7 response")
	}
}

func TestCreateTopicsHandler_OlderVersionNoExtendedInfo(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
		},
		topicStore,
		streamManager,
		nil,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "old-version-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	// Use version 4 (before extended info in response)
	resp := handler.Handle(ctx, 4, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// NumPartitions and ReplicationFactor should be -1 (default/unset) for v4
	// because the handler doesn't set them for versions < 5
	if resp.Topics[0].NumPartitions != -1 {
		t.Errorf("expected NumPartitions=-1 (default) for v4, got %d", resp.Topics[0].NumPartitions)
	}
	if resp.Topics[0].ReplicationFactor != -1 {
		t.Errorf("expected ReplicationFactor=-1 (default) for v4, got %d", resp.Topics[0].ReplicationFactor)
	}
}

func TestCreateTopicsHandler_IcebergTableProperties(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	icebergCatalog := newMockIcebergCatalog()

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
			IcebergEnabled:           true,
			ClusterID:                "my-test-cluster",
			IcebergNamespace:         []string{"custom", "ns"},
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "props-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify Iceberg table was created with correct properties
	tableID := catalog.NewTableIdentifier([]string{"custom", "ns"}, "props-topic")
	table, err := icebergCatalog.LoadTable(ctx, tableID)
	if err != nil {
		t.Fatalf("failed to load Iceberg table: %v", err)
	}

	props := table.Properties()

	// Verify dray.topic property
	if props[catalog.PropertyDrayTopic] != "props-topic" {
		t.Errorf("expected dray.topic = 'props-topic', got %q", props[catalog.PropertyDrayTopic])
	}

	// Verify dray.cluster_id property
	if props[catalog.PropertyDrayClusterID] != "my-test-cluster" {
		t.Errorf("expected dray.cluster_id = 'my-test-cluster', got %q", props[catalog.PropertyDrayClusterID])
	}

	// Verify dray.schema_version property
	if props[catalog.PropertyDraySchemaVersion] != "2" {
		t.Errorf("expected dray.schema_version = '2', got %q", props[catalog.PropertyDraySchemaVersion])
	}
}

func TestCreateTopicsHandler_IcebergTableSchema(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	icebergCatalog := newMockIcebergCatalog()

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
			IcebergEnabled:           true,
			ClusterID:                "test-cluster",
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "schema-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify Iceberg table schema
	tableID := catalog.NewTableIdentifier([]string{"dray"}, "schema-topic")
	table, err := icebergCatalog.LoadTable(ctx, tableID)
	if err != nil {
		t.Fatalf("failed to load Iceberg table: %v", err)
	}

	schema := table.Schema()

	// Verify required fields per SPEC.md section 5.3
	expectedFields := map[string]bool{
		"partition":  true,
		"offset":     true,
		"timestamp":  true,
		"key":        true,
		"value":      true,
		"headers":    true,
		"attributes": true,
	}

	fieldNames := make(map[string]bool)
	for _, f := range schema.Fields() {
		fieldNames[f.Name] = true
	}

	for name := range expectedFields {
		if !fieldNames[name] {
			t.Errorf("missing required field %q in Iceberg table schema", name)
		}
	}
}

func TestCreateTopicsHandler_IcebergExistingTableGraceful(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	icebergCatalog := newMockIcebergCatalog()

	// Pre-create the table
	tableID := catalog.NewTableIdentifier([]string{"dray"}, "existing-iceberg-topic")
	_, err := icebergCatalog.CreateTableIfMissing(ctx, tableID, catalog.CreateTableOptions{
		Schema:     catalog.DefaultSchema(),
		Properties: catalog.DefaultTableProperties("existing-iceberg-topic", "old-cluster"),
	})
	if err != nil {
		t.Fatalf("failed to pre-create Iceberg table: %v", err)
	}

	handler := NewCreateTopicsHandler(
		CreateTopicsHandlerConfig{
			DefaultPartitions:        3,
			DefaultReplicationFactor: 1,
			IcebergEnabled:           true,
			ClusterID:                "new-cluster",
		},
		topicStore,
		streamManager,
		icebergCatalog,
	)

	req := kmsg.NewPtrCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic = "existing-iceberg-topic"
	topic.NumPartitions = 3
	topic.ReplicationFactor = 1
	req.Topics = append(req.Topics, topic)

	resp := handler.Handle(ctx, 5, req)

	// Topic creation should succeed even though Iceberg table already exists
	if resp.Topics[0].ErrorCode != 0 {
		t.Errorf("expected no error, got error code %d", resp.Topics[0].ErrorCode)
	}

	// Verify topic was created
	_, err = topicStore.GetTopic(ctx, "existing-iceberg-topic")
	if err != nil {
		t.Errorf("topic should exist: %v", err)
	}
}
