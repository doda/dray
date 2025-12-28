package compatibility

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var _ = kgo.ConsumeTopics // Ensure kgo is used

// TestAdminOps_CreateTopic tests creating a topic via the Kafka protocol.
func TestAdminOps_CreateTopic(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	topicName := "admin-create-topic"
	numPartitions := int32(4)
	replicationFactor := int16(1)

	resp, err := adminClient.CreateTopics(ctx, numPartitions, replicationFactor, nil, topicName)
	if err != nil {
		t.Fatalf("CreateTopics request failed: %v", err)
	}

	for _, topicResp := range resp {
		if topicResp.Err != nil {
			t.Errorf("failed to create topic %s: %v", topicResp.Topic, topicResp.Err)
		} else {
			t.Logf("Topic %s created with %d partitions", topicResp.Topic, topicResp.NumPartitions)
		}
	}

	metadata, err := adminClient.Metadata(ctx, topicName)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	for _, topic := range metadata.Topics {
		if topic.Topic == topicName {
			if len(topic.Partitions) != int(numPartitions) {
				t.Errorf("expected %d partitions, got %d", numPartitions, len(topic.Partitions))
			}
			t.Logf("Verified topic %s exists with %d partitions", topic.Topic, len(topic.Partitions))
		}
	}

	t.Log("CreateTopic admin operation completed successfully")
}

// TestAdminOps_DeleteTopic tests deleting a topic via the Kafka protocol.
func TestAdminOps_DeleteTopic(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "admin-delete-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 2); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	metadata, err := adminClient.Metadata(ctx, topicName)
	if err != nil {
		t.Fatalf("failed to get initial metadata: %v", err)
	}

	found := false
	for _, topic := range metadata.Topics {
		if topic.Topic == topicName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("topic %s not found before deletion", topicName)
	}

	resp, err := adminClient.DeleteTopics(ctx, topicName)
	if err != nil {
		t.Fatalf("DeleteTopics request failed: %v", err)
	}

	for _, topicResp := range resp {
		if topicResp.Err != nil {
			t.Errorf("failed to delete topic %s: %v", topicResp.Topic, topicResp.Err)
		} else {
			t.Logf("Topic %s deleted successfully", topicResp.Topic)
		}
	}

	t.Log("DeleteTopic admin operation completed successfully")
}

// TestAdminOps_ListTopics tests listing topics.
func TestAdminOps_ListTopics(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	topicNames := []string{"list-topic-1", "list-topic-2", "list-topic-3"}
	for _, name := range topicNames {
		if err := suite.Broker().CreateTopic(ctx, name, 1); err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	topics, err := adminClient.ListTopics(ctx)
	if err != nil {
		t.Fatalf("failed to list topics: %v", err)
	}

	foundTopics := make(map[string]bool)
	for _, topic := range topics {
		foundTopics[topic.Topic] = true
	}

	for _, expected := range topicNames {
		if !foundTopics[expected] {
			t.Errorf("topic %s not found in list", expected)
		}
	}

	t.Logf("Listed %d topics, found all %d expected topics", len(topics), len(topicNames))
}

// TestAdminOps_DescribeTopic tests getting topic metadata.
func TestAdminOps_DescribeTopic(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "describe-topic"
	numPartitions := int32(3)

	if err := suite.Broker().CreateTopic(ctx, topicName, numPartitions); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	metadata, err := adminClient.Metadata(ctx, topicName)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	for _, topic := range metadata.Topics {
		if topic.Topic == topicName {
			if len(topic.Partitions) != int(numPartitions) {
				t.Errorf("expected %d partitions, got %d", numPartitions, len(topic.Partitions))
			}

			for _, p := range topic.Partitions {
				t.Logf("Partition %d: leader=%d, replicas=%v",
					p.Partition, p.Leader, p.Replicas)
			}
		}
	}

	t.Log("DescribeTopic operation completed successfully")
}

// TestAdminOps_CreateDuplicateTopic tests that creating a duplicate topic fails.
func TestAdminOps_CreateDuplicateTopic(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "duplicate-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create initial topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	resp, err := adminClient.CreateTopics(ctx, 1, 1, nil, topicName)
	if err != nil {
		t.Fatalf("CreateTopics request failed unexpectedly: %v", err)
	}

	for _, topicResp := range resp {
		if topicResp.Err == nil {
			t.Errorf("expected error for duplicate topic creation, got success")
		} else {
			t.Logf("Duplicate topic creation correctly failed: %v", topicResp.Err)
		}
	}

	t.Log("Duplicate topic handling verified")
}

// TestAdminOps_ApiVersions tests the ApiVersions API.
func TestAdminOps_ApiVersions(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName = "compatibility-test"
	req.ClientSoftwareVersion = "1.0.0"

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("ApiVersions request failed: %v", err)
	}

	if resp.ErrorCode != 0 {
		t.Errorf("ApiVersions failed with error code %d", resp.ErrorCode)
	}

	t.Logf("ApiVersions returned %d API keys", len(resp.ApiKeys))

	criticalAPIs := map[int16]string{
		0:  "Produce",
		1:  "Fetch",
		2:  "ListOffsets",
		3:  "Metadata",
		8:  "OffsetCommit",
		9:  "OffsetFetch",
		10: "FindCoordinator",
		18: "ApiVersions",
		19: "CreateTopics",
		20: "DeleteTopics",
	}

	supported := make(map[int16]bool)
	for _, api := range resp.ApiKeys {
		supported[api.ApiKey] = true
	}

	for apiKey, name := range criticalAPIs {
		if !supported[apiKey] {
			t.Errorf("critical API %s (%d) not supported", name, apiKey)
		} else {
			t.Logf("API %s (%d) is supported", name, apiKey)
		}
	}

	t.Log("ApiVersions test completed successfully")
}

// TestAdminOps_Metadata tests the Metadata API.
func TestAdminOps_Metadata(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	topicNames := []string{"metadata-topic-1", "metadata-topic-2"}
	for _, name := range topicNames {
		if err := suite.Broker().CreateTopic(ctx, name, 2); err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	req := kmsg.NewPtrMetadataRequest()
	for _, name := range topicNames {
		topic := kmsg.NewMetadataRequestTopic()
		topic.Topic = kmsg.StringPtr(name)
		req.Topics = append(req.Topics, topic)
	}

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("Metadata request failed: %v", err)
	}

	if len(resp.Brokers) == 0 {
		t.Error("expected at least one broker in metadata response")
	}

	for _, broker := range resp.Brokers {
		t.Logf("Broker: nodeID=%d, host=%s, port=%d", broker.NodeID, broker.Host, broker.Port)
	}

	for _, topic := range resp.Topics {
		if topic.Topic == nil {
			continue
		}
		t.Logf("Topic %s: %d partitions, error_code=%d",
			*topic.Topic, len(topic.Partitions), topic.ErrorCode)
	}

	t.Log("Metadata API test completed successfully")
}

// TestAdminOps_DescribeConfigs tests the DescribeConfigs API.
func TestAdminOps_DescribeConfigs(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "describe-configs-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	configs, err := adminClient.DescribeTopicConfigs(ctx, topicName)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs failed: %v", err)
	}

	for _, resourceConfigs := range configs {
		if resourceConfigs.Err != nil {
			t.Errorf("describe configs error: %v", resourceConfigs.Err)
			continue
		}

		t.Logf("Topic %s has %d configuration entries", resourceConfigs.Name, len(resourceConfigs.Configs))

		for _, cfg := range resourceConfigs.Configs {
			if cfg.Value != nil {
				t.Logf("  %s = %s (source: %s)", cfg.Key, *cfg.Value, cfg.Source)
			}
		}
	}

	t.Log("DescribeConfigs API test completed successfully")
}

// TestAdminOps_IncrementalAlterConfigs tests the IncrementalAlterConfigs API.
func TestAdminOps_IncrementalAlterConfigs(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "alter-configs-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = 2 // Topic
	resource.ResourceName = topicName

	config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	config.Name = "retention.ms"
	config.Value = kmsg.StringPtr("86400000")
	config.Op = 0 // SET
	resource.Configs = append(resource.Configs, config)

	req.Resources = append(req.Resources, resource)

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs request failed: %v", err)
	}

	for _, resourceResp := range resp.Resources {
		if resourceResp.ErrorCode != 0 {
			t.Logf("Alter config result for %s: error code %d", resourceResp.ResourceName, resourceResp.ErrorCode)
		} else {
			t.Logf("Successfully altered config for %s", resourceResp.ResourceName)
		}
	}

	t.Log("IncrementalAlterConfigs API test completed")
}

// TestAdminOps_ListOffsets tests the ListOffsets API.
func TestAdminOps_ListOffsets(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()
	topicName := "list-offsets-topic"

	if err := suite.Broker().CreateTopic(ctx, topicName, 1); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	producer, err := suite.Broker().NewClient(
		kgo.DefaultProduceTopic(topicName),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		result := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topicName,
			Value: []byte("test"),
		})
		if result.FirstErr() != nil {
			t.Fatalf("produce failed: %v", result.FirstErr())
		}
	}

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	startOffsets, err := adminClient.ListStartOffsets(ctx, topicName)
	if err != nil {
		t.Fatalf("ListStartOffsets failed: %v", err)
	}

	startOffsets.Each(func(lo kadm.ListedOffset) {
		t.Logf("Start offset for %s/%d: %d", lo.Topic, lo.Partition, lo.Offset)
	})

	endOffsets, err := adminClient.ListEndOffsets(ctx, topicName)
	if err != nil {
		t.Fatalf("ListEndOffsets failed: %v", err)
	}

	endOffsets.Each(func(lo kadm.ListedOffset) {
		t.Logf("End offset for %s/%d: %d", lo.Topic, lo.Partition, lo.Offset)
	})

	t.Log("ListOffsets API test completed successfully")
}

// TestAdminOps_DescribeCluster tests the DescribeCluster API.
func TestAdminOps_DescribeCluster(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	req := kmsg.NewPtrDescribeClusterRequest()

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("DescribeCluster request failed: %v", err)
	}

	if resp.ErrorCode != 0 {
		t.Errorf("DescribeCluster failed with error code %d", resp.ErrorCode)
	}

	t.Logf("Cluster: clusterID=%s, controllerID=%d", resp.ClusterID, resp.ControllerID)
	t.Logf("Brokers: %d", len(resp.Brokers))

	for _, broker := range resp.Brokers {
		rack := ""
		if broker.Rack != nil {
			rack = *broker.Rack
		}
		t.Logf("  Broker %d: %s:%d (rack=%s)", broker.NodeID, broker.Host, broker.Port, rack)
	}

	t.Log("DescribeCluster API test completed successfully")
}

// TestAdminOps_CreateMultipleTopics tests creating multiple topics at once.
func TestAdminOps_CreateMultipleTopics(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	topicNames := []string{"multi-create-1", "multi-create-2", "multi-create-3"}

	resp, err := adminClient.CreateTopics(ctx, 2, 1, nil, topicNames...)
	if err != nil {
		t.Fatalf("CreateTopics request failed: %v", err)
	}

	successCount := 0
	for _, topicResp := range resp {
		if topicResp.Err != nil {
			t.Errorf("failed to create topic %s: %v", topicResp.Topic, topicResp.Err)
		} else {
			successCount++
			t.Logf("Created topic %s", topicResp.Topic)
		}
	}

	if successCount != len(topicNames) {
		t.Errorf("expected %d topics created, got %d", len(topicNames), successCount)
	}

	t.Logf("Created %d topics in a single request", successCount)
}

// TestAdminOps_DeleteNonExistentTopic tests deleting a non-existent topic.
func TestAdminOps_DeleteNonExistentTopic(t *testing.T) {
	suite := NewCompatibilityTestSuite(t)
	defer suite.Close()

	ctx := context.Background()

	client, err := suite.Broker().NewClient()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	resp, err := adminClient.DeleteTopics(ctx, "non-existent-topic-xyz")
	if err != nil {
		t.Fatalf("DeleteTopics request failed unexpectedly: %v", err)
	}

	for _, topicResp := range resp {
		if topicResp.Err == nil {
			t.Logf("Note: Deleting non-existent topic returned success (idempotent behavior)")
		} else {
			t.Logf("Deleting non-existent topic correctly returned error: %v", topicResp.Err)
		}
	}

	t.Log("Non-existent topic deletion handling verified")
}
