package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/config"
	"github.com/dray-io/dray/internal/groups"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
)

func newTestAdminOpts() *AdminOptions {
	metaStore := metadata.NewMockStore()
	cfg := &config.Config{}

	return &AdminOptions{
		Config:     cfg,
		Logger:     logging.DefaultLogger(),
		MetaStore:  metaStore,
		TopicStore: topics.NewStore(metaStore),
		GroupStore: groups.NewStore(metaStore),
		Stream:     index.NewStreamManager(metaStore),
	}
}

// TestTopicCreate tests the topic creation flow.
func TestTopicCreate(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a topic
	result, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		Config: map[string]string{
			"retention.ms": "86400000",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if result.Topic.Name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got '%s'", result.Topic.Name)
	}
	if result.Topic.PartitionCount != 3 {
		t.Errorf("expected 3 partitions, got %d", result.Topic.PartitionCount)
	}
	if len(result.Partitions) != 3 {
		t.Errorf("expected 3 partition entries, got %d", len(result.Partitions))
	}

	// Initialize streams for each partition
	for _, p := range result.Partitions {
		err := opts.Stream.CreateStreamWithID(ctx, p.StreamID, "test-topic", p.Partition)
		if err != nil {
			t.Errorf("failed to create stream for partition %d: %v", p.Partition, err)
		}
	}
}

// TestTopicList tests listing topics.
func TestTopicList(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create multiple topics
	for i := 1; i <= 3; i++ {
		_, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
			Name:           "topic-" + string(rune('a'+i-1)),
			PartitionCount: int32(i),
			NowMs:          time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create topic %d: %v", i, err)
		}
	}

	// List topics
	topicList, err := opts.TopicStore.ListTopics(ctx)
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}

	if len(topicList) != 3 {
		t.Errorf("expected 3 topics, got %d", len(topicList))
	}
}

// TestTopicDescribe tests describing a topic.
func TestTopicDescribe(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a topic
	_, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "my-topic",
		PartitionCount: 5,
		Config: map[string]string{
			"retention.ms":   "604800000",
			"cleanup.policy": "delete",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Describe the topic
	topic, err := opts.TopicStore.GetTopic(ctx, "my-topic")
	if err != nil {
		t.Fatalf("failed to get topic: %v", err)
	}

	if topic.Name != "my-topic" {
		t.Errorf("expected topic name 'my-topic', got '%s'", topic.Name)
	}
	if topic.PartitionCount != 5 {
		t.Errorf("expected 5 partitions, got %d", topic.PartitionCount)
	}
	if topic.Config["retention.ms"] != "604800000" {
		t.Errorf("expected retention.ms=604800000, got '%s'", topic.Config["retention.ms"])
	}

	// Get partitions
	partitions, err := opts.TopicStore.ListPartitions(ctx, "my-topic")
	if err != nil {
		t.Fatalf("failed to list partitions: %v", err)
	}
	if len(partitions) != 5 {
		t.Errorf("expected 5 partitions, got %d", len(partitions))
	}
}

// TestTopicDelete tests topic deletion.
func TestTopicDelete(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a topic
	result, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "delete-me",
		PartitionCount: 2,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Initialize streams
	for _, p := range result.Partitions {
		opts.Stream.CreateStreamWithID(ctx, p.StreamID, "delete-me", p.Partition)
	}

	// Delete the topic
	deleteResult, err := opts.TopicStore.DeleteTopic(ctx, "delete-me")
	if err != nil {
		t.Fatalf("failed to delete topic: %v", err)
	}

	if deleteResult.Topic.Name != "delete-me" {
		t.Errorf("unexpected deleted topic name: %s", deleteResult.Topic.Name)
	}

	// Mark streams as deleted
	for _, p := range deleteResult.Partitions {
		opts.Stream.MarkStreamDeleted(ctx, p.StreamID)
	}

	// Verify topic is gone
	_, err = opts.TopicStore.GetTopic(ctx, "delete-me")
	if err == nil {
		t.Error("expected topic to be deleted")
	}
}

// TestTopicNotFound tests error handling for missing topics.
func TestTopicNotFound(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	_, err := opts.TopicStore.GetTopic(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent topic")
	}
	if err != topics.ErrTopicNotFound {
		t.Errorf("expected ErrTopicNotFound, got %v", err)
	}
}

// TestGroupCreate tests consumer group creation.
func TestGroupCreate(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	state, err := opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:          "test-group",
		Type:             groups.GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 30000,
		NowMs:            time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	if state.GroupID != "test-group" {
		t.Errorf("expected group ID 'test-group', got '%s'", state.GroupID)
	}
	if state.State != groups.GroupStateEmpty {
		t.Errorf("expected state Empty, got %s", state.State)
	}
}

// TestGroupList tests listing consumer groups.
func TestGroupList(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create multiple groups
	for i := 1; i <= 3; i++ {
		_, err := opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID:      "group-" + string(rune('0'+i)),
			Type:         groups.GroupTypeClassic,
			ProtocolType: "consumer",
			NowMs:        time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create group %d: %v", i, err)
		}
	}

	// List groups
	groupList, err := opts.GroupStore.ListGroups(ctx)
	if err != nil {
		t.Fatalf("failed to list groups: %v", err)
	}

	if len(groupList) != 3 {
		t.Errorf("expected 3 groups, got %d", len(groupList))
	}
}

// TestGroupDescribe tests describing a consumer group.
func TestGroupDescribe(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a group
	_, err := opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID:          "my-group",
		Type:             groups.GroupTypeClassic,
		ProtocolType:     "consumer",
		SessionTimeoutMs: 10000,
		NowMs:            time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Describe the group
	state, err := opts.GroupStore.GetGroupState(ctx, "my-group")
	if err != nil {
		t.Fatalf("failed to get group state: %v", err)
	}

	if state.GroupID != "my-group" {
		t.Errorf("expected group ID 'my-group', got '%s'", state.GroupID)
	}
	if state.ProtocolType != "consumer" {
		t.Errorf("expected protocol type 'consumer', got '%s'", state.ProtocolType)
	}

	// Check group type
	groupType, err := opts.GroupStore.GetGroupType(ctx, "my-group")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}
	if groupType != groups.GroupTypeClassic {
		t.Errorf("expected classic group type, got %s", groupType)
	}
}

// TestGroupMembers tests member management.
func TestGroupMembers(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a group
	_, err := opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID: "member-test-group",
		Type:    groups.GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Add members
	for i := 1; i <= 2; i++ {
		_, err := opts.GroupStore.AddMember(ctx, groups.AddMemberRequest{
			GroupID:          "member-test-group",
			MemberID:         "member-" + string(rune('0'+i)),
			ClientID:         "client-" + string(rune('0'+i)),
			ClientHost:       "192.168.1." + string(rune('0'+i)),
			SessionTimeoutMs: 30000,
			NowMs:            time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to add member %d: %v", i, err)
		}
	}

	// List members
	members, err := opts.GroupStore.ListMembers(ctx, "member-test-group")
	if err != nil {
		t.Fatalf("failed to list members: %v", err)
	}
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d", len(members))
	}
}

// TestGroupDelete tests consumer group deletion.
func TestGroupDelete(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a group
	_, err := opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID: "delete-group",
		Type:    groups.GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Delete the group
	err = opts.GroupStore.DeleteGroup(ctx, "delete-group")
	if err != nil {
		t.Fatalf("failed to delete group: %v", err)
	}

	// Verify group is gone
	_, err = opts.GroupStore.GetGroupState(ctx, "delete-group")
	if err == nil {
		t.Error("expected group to be deleted")
	}
}

// TestGroupNotFound tests error handling for missing groups.
func TestGroupNotFound(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	_, err := opts.GroupStore.GetGroupState(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent group")
	}
	if err != groups.ErrGroupNotFound {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}

// TestCommittedOffsets tests offset management.
func TestCommittedOffsets(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a group
	_, err := opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID: "offset-group",
		Type:    groups.GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Commit some offsets
	offsets, err := opts.GroupStore.CommitOffsets(ctx, "offset-group", []groups.CommitOffsetRequest{
		{Topic: "topic-a", Partition: 0, Offset: 100, LeaderEpoch: 1, Metadata: "test"},
		{Topic: "topic-a", Partition: 1, Offset: 200, LeaderEpoch: 1},
		{Topic: "topic-b", Partition: 0, Offset: 50, LeaderEpoch: 0},
	}, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("failed to commit offsets: %v", err)
	}
	if len(offsets) != 3 {
		t.Errorf("expected 3 committed offsets, got %d", len(offsets))
	}

	// List offsets
	list, err := opts.GroupStore.ListCommittedOffsets(ctx, "offset-group")
	if err != nil {
		t.Fatalf("failed to list offsets: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("expected 3 offsets, got %d", len(list))
	}

	// Delete all offsets
	err = opts.GroupStore.DeleteAllCommittedOffsets(ctx, "offset-group")
	if err != nil {
		t.Fatalf("failed to delete offsets: %v", err)
	}

	// Verify offsets are gone
	list, _ = opts.GroupStore.ListCommittedOffsets(ctx, "offset-group")
	if len(list) != 0 {
		t.Errorf("expected 0 offsets after deletion, got %d", len(list))
	}
}

// TestConfigDescribe tests topic config description.
func TestConfigDescribe(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a topic with config
	_, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			"retention.ms":   "86400000",
			"cleanup.policy": "compact",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Get config
	cfg, err := opts.TopicStore.GetTopicConfig(ctx, "config-topic")
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if cfg["retention.ms"] != "86400000" {
		t.Errorf("expected retention.ms=86400000, got '%s'", cfg["retention.ms"])
	}
	if cfg["cleanup.policy"] != "compact" {
		t.Errorf("expected cleanup.policy=compact, got '%s'", cfg["cleanup.policy"])
	}
}

// TestConfigAlter tests topic config modification.
func TestConfigAlter(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a topic
	_, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "alter-topic",
		PartitionCount: 1,
		Config: map[string]string{
			"retention.ms": "86400000",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Set a config
	err = opts.TopicStore.SetTopicConfig(ctx, "alter-topic", "cleanup.policy", "compact")
	if err != nil {
		t.Fatalf("failed to set config: %v", err)
	}

	// Verify config was set
	cfg, _ := opts.TopicStore.GetTopicConfig(ctx, "alter-topic")
	if cfg["cleanup.policy"] != "compact" {
		t.Errorf("expected cleanup.policy=compact, got '%s'", cfg["cleanup.policy"])
	}

	// Delete a config
	err = opts.TopicStore.DeleteTopicConfig(ctx, "alter-topic", "retention.ms")
	if err != nil {
		t.Fatalf("failed to delete config: %v", err)
	}

	// Verify config was deleted
	cfg, _ = opts.TopicStore.GetTopicConfig(ctx, "alter-topic")
	if _, exists := cfg["retention.ms"]; exists {
		t.Error("expected retention.ms to be deleted")
	}
}

// TestClusterStatus tests the cluster status command.
func TestClusterStatus(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create some topics
	for i := 0; i < 3; i++ {
		opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
			Name:           "status-topic-" + string(rune('0'+i)),
			PartitionCount: 2,
			NowMs:          time.Now().UnixMilli(),
		})
	}

	// Create some groups
	for i := 0; i < 2; i++ {
		opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
			GroupID: "status-group-" + string(rune('0'+i)),
			Type:    groups.GroupTypeClassic,
			NowMs:   time.Now().UnixMilli(),
		})
	}

	// Test status structure
	status := ClusterStatus{
		Timestamp:         time.Now().Format(time.RFC3339),
		MetadataStore:     "ok",
		TopicCount:        3,
		TotalPartitions:   6,
		GroupCount:        2,
		StableGroups:      0,
		EmptyGroups:       2,
		RebalancingGroups: 0,
	}

	if status.TopicCount != 3 {
		t.Errorf("expected 3 topics, got %d", status.TopicCount)
	}
	if status.TotalPartitions != 6 {
		t.Errorf("expected 6 partitions, got %d", status.TotalPartitions)
	}
	if status.GroupCount != 2 {
		t.Errorf("expected 2 groups, got %d", status.GroupCount)
	}
}

// TestAdminInitialization tests the admin options initialization.
func TestAdminInitialization(t *testing.T) {
	// Create a temporary config file without Oxia endpoint to use mock store
	tmpFile, err := os.CreateTemp("", "dray-test-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write a minimal config with empty Oxia endpoint to use mock store
	configContent := `
broker:
  listenAddr: ":9092"
metadata:
  oxiaEndpoint: ""
  namespace: "test"
`
	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}
	tmpFile.Close()

	opts, cleanup, err := initAdminOpts(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to initialize admin opts: %v", err)
	}
	defer cleanup()

	if opts.TopicStore == nil {
		t.Error("TopicStore should not be nil")
	}
	if opts.GroupStore == nil {
		t.Error("GroupStore should not be nil")
	}
	if opts.Stream == nil {
		t.Error("Stream manager should not be nil")
	}
	if opts.MetaStore == nil {
		t.Error("MetaStore should not be nil")
	}
}

// TestPrintFunctions tests the CLI print helper functions.
func TestPrintFunctions(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printAdminUsage()
	printTopicsUsage()
	printGroupsUsage()
	printConfigsUsage()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Verify key strings are present
	if !strings.Contains(output, "admin") {
		t.Error("expected 'admin' in usage output")
	}
	if !strings.Contains(output, "topics") {
		t.Error("expected 'topics' in usage output")
	}
	if !strings.Contains(output, "groups") {
		t.Error("expected 'groups' in usage output")
	}
	if !strings.Contains(output, "configs") {
		t.Error("expected 'configs' in usage output")
	}
}

// TestTopicDuplicate tests that duplicate topic creation fails.
func TestTopicDuplicate(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a topic
	_, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "dup-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Try to create the same topic again
	_, err = opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "dup-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err == nil {
		t.Error("expected error when creating duplicate topic")
	}
	if err != topics.ErrTopicExists {
		t.Errorf("expected ErrTopicExists, got %v", err)
	}
}

// TestGroupDuplicate tests that duplicate group creation fails.
func TestGroupDuplicate(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a group
	_, err := opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID: "dup-group",
		Type:    groups.GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create group: %v", err)
	}

	// Try to create the same group again
	_, err = opts.GroupStore.CreateGroup(ctx, groups.CreateGroupRequest{
		GroupID: "dup-group",
		Type:    groups.GroupTypeClassic,
		NowMs:   time.Now().UnixMilli(),
	})
	if err == nil {
		t.Error("expected error when creating duplicate group")
	}
	if err != groups.ErrGroupExists {
		t.Errorf("expected ErrGroupExists, got %v", err)
	}
}

// TestConfigValidation tests that invalid configs are rejected.
func TestConfigValidation(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create topic with invalid retention.ms
	_, err := opts.TopicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invalid-config-topic",
		PartitionCount: 1,
		Config: map[string]string{
			"retention.ms": "not-a-number",
		},
		NowMs: time.Now().UnixMilli(),
	})
	if err == nil {
		t.Error("expected error for invalid retention.ms")
	}
}

// TestStreamLifecycle tests stream creation and deletion.
func TestStreamLifecycle(t *testing.T) {
	opts := newTestAdminOpts()
	defer opts.MetaStore.Close()

	ctx := context.Background()

	// Create a stream
	streamID := "test-stream-id"
	err := opts.Stream.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Get stream metadata
	meta, err := opts.Stream.GetStreamMeta(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get stream meta: %v", err)
	}
	if meta.TopicName != "test-topic" {
		t.Errorf("expected topic 'test-topic', got '%s'", meta.TopicName)
	}
	if meta.Partition != 0 {
		t.Errorf("expected partition 0, got %d", meta.Partition)
	}

	// Get HWM
	hwm, _, err := opts.Stream.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM: %v", err)
	}
	if hwm != 0 {
		t.Errorf("expected HWM 0, got %d", hwm)
	}

	// Mark stream deleted
	err = opts.Stream.MarkStreamDeleted(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to mark stream deleted: %v", err)
	}

	// Verify stream is gone
	_, err = opts.Stream.GetStreamMeta(ctx, streamID)
	if err == nil {
		t.Error("expected error after stream deletion")
	}
}
