package protocol

import (
	"testing"

	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHandleLeaderAndISR(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION for v5+", func(t *testing.T) {
		for version := int16(5); version <= 7; version++ {
			req := kmsg.NewPtrLeaderAndISRRequest()
			req.SetVersion(version)
			req.ControllerID = 1
			req.BrokerEpoch = 100

			// v5+ uses TopicStates
			topic := kmsg.NewLeaderAndISRRequestTopicState()
			topic.Topic = "test-topic"
			part := kmsg.NewLeaderAndISRRequestTopicPartition()
			part.Topic = "test-topic"
			part.Partition = 0
			part.Leader = 1
			topic.PartitionStates = append(topic.PartitionStates, part)
			req.TopicStates = append(req.TopicStates, topic)

			resp := HandleLeaderAndISR(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected top-level UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.ErrorCode)
			}

			// Verify per-partition responses also have error (v5+ uses Topics array)
			if len(resp.Topics) != 1 {
				t.Errorf("version %d: expected 1 topic response, got %d", version, len(resp.Topics))
				continue
			}

			if len(resp.Topics[0].Partitions) != 1 {
				t.Errorf("version %d: expected 1 partition response, got %d", version, len(resp.Topics[0].Partitions))
				continue
			}

			if resp.Topics[0].Partitions[0].ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected per-partition UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.Topics[0].Partitions[0].ErrorCode)
			}
		}
	})

	t.Run("returns UNSUPPORTED_VERSION for v0-v4", func(t *testing.T) {
		for version := int16(0); version <= 4; version++ {
			req := kmsg.NewPtrLeaderAndISRRequest()
			req.SetVersion(version)
			req.ControllerID = 1
			req.BrokerEpoch = 100

			// v0-v4 uses flat PartitionStates array
			part := kmsg.NewLeaderAndISRRequestTopicPartition()
			part.Topic = "test-topic"
			part.Partition = 0
			part.Leader = 1
			req.PartitionStates = append(req.PartitionStates, part)

			resp := HandleLeaderAndISR(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected top-level UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.ErrorCode)
			}

			// v0-v4 uses flat Partitions array
			if len(resp.Partitions) != 1 {
				t.Errorf("version %d: expected 1 partition response, got %d", version, len(resp.Partitions))
				continue
			}

			if resp.Partitions[0].ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected per-partition UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.Partitions[0].ErrorCode)
			}
		}
	})

	t.Run("handles empty topic list", func(t *testing.T) {
		req := kmsg.NewPtrLeaderAndISRRequest()
		req.SetVersion(5)
		req.ControllerID = 1

		resp := HandleLeaderAndISR(5, req, logger)

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error, got %d", resp.ErrorCode)
		}

		if len(resp.Topics) != 0 {
			t.Errorf("expected 0 topic responses, got %d", len(resp.Topics))
		}
	})

	t.Run("handles multiple partitions", func(t *testing.T) {
		req := kmsg.NewPtrLeaderAndISRRequest()
		req.SetVersion(5)
		req.ControllerID = 1

		topic := kmsg.NewLeaderAndISRRequestTopicState()
		topic.Topic = "test-topic"
		for i := int32(0); i < 3; i++ {
			part := kmsg.NewLeaderAndISRRequestTopicPartition()
			part.Topic = "test-topic"
			part.Partition = i
			part.Leader = 1
			topic.PartitionStates = append(topic.PartitionStates, part)
		}
		req.TopicStates = append(req.TopicStates, topic)

		resp := HandleLeaderAndISR(5, req, logger)

		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
		}

		if len(resp.Topics[0].Partitions) != 3 {
			t.Errorf("expected 3 partition responses, got %d", len(resp.Topics[0].Partitions))
		}

		for i, part := range resp.Topics[0].Partitions {
			if part.Partition != int32(i) {
				t.Errorf("partition %d: expected partition %d, got %d", i, i, part.Partition)
			}
			if part.ErrorCode != errUnsupportedVersion {
				t.Errorf("partition %d: expected UNSUPPORTED_VERSION error", i)
			}
		}
	})
}

func TestHandleStopReplica(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION with v3+ PartitionStates", func(t *testing.T) {
		for version := int16(3); version <= 4; version++ {
			req := kmsg.NewPtrStopReplicaRequest()
			req.SetVersion(version)
			req.ControllerID = 1
			req.BrokerEpoch = 100

			// v3+ uses PartitionStates
			topic := kmsg.NewStopReplicaRequestTopic()
			topic.Topic = "test-topic"
			ps := kmsg.NewStopReplicaRequestTopicPartitionState()
			ps.Partition = 0
			topic.PartitionStates = append(topic.PartitionStates, ps)
			req.Topics = append(req.Topics, topic)

			resp := HandleStopReplica(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected top-level UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.ErrorCode)
			}

			if len(resp.Partitions) != 1 {
				t.Errorf("version %d: expected 1 partition response, got %d", version, len(resp.Partitions))
				continue
			}

			if resp.Partitions[0].Topic != "test-topic" {
				t.Errorf("version %d: expected topic 'test-topic', got %q", version, resp.Partitions[0].Topic)
			}

			if resp.Partitions[0].ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected per-partition UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.Partitions[0].ErrorCode)
			}
		}
	})

	t.Run("returns UNSUPPORTED_VERSION with v2 Partitions array", func(t *testing.T) {
		req := kmsg.NewPtrStopReplicaRequest()
		req.SetVersion(2)
		req.ControllerID = 1
		req.BrokerEpoch = 100

		// v2 uses Partitions array
		topic := kmsg.NewStopReplicaRequestTopic()
		topic.Topic = "test-topic"
		topic.Partitions = []int32{0, 1}
		req.Topics = append(req.Topics, topic)

		resp := HandleStopReplica(2, req, logger)

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error, got %d", resp.ErrorCode)
		}

		if len(resp.Partitions) != 2 {
			t.Errorf("expected 2 partition responses, got %d", len(resp.Partitions))
		}
	})

	t.Run("returns UNSUPPORTED_VERSION with v1 single Partition", func(t *testing.T) {
		req := kmsg.NewPtrStopReplicaRequest()
		req.SetVersion(1)
		req.ControllerID = 1
		req.BrokerEpoch = 100

		// v1 uses single Partition field
		topic := kmsg.NewStopReplicaRequestTopic()
		topic.Topic = "test-topic"
		topic.Partition = 0
		req.Topics = append(req.Topics, topic)

		resp := HandleStopReplica(1, req, logger)

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error, got %d", resp.ErrorCode)
		}

		if len(resp.Partitions) != 1 {
			t.Errorf("expected 1 partition response, got %d", len(resp.Partitions))
		}
	})

	t.Run("handles empty partition list", func(t *testing.T) {
		req := kmsg.NewPtrStopReplicaRequest()
		req.SetVersion(3)
		req.ControllerID = 1

		resp := HandleStopReplica(3, req, logger)

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error, got %d", resp.ErrorCode)
		}

		if len(resp.Partitions) != 0 {
			t.Errorf("expected 0 partition responses, got %d", len(resp.Partitions))
		}
	})

	t.Run("handles multiple topics and partitions", func(t *testing.T) {
		req := kmsg.NewPtrStopReplicaRequest()
		req.SetVersion(3)
		req.ControllerID = 1

		for _, topicName := range []string{"topic1", "topic2"} {
			topic := kmsg.NewStopReplicaRequestTopic()
			topic.Topic = topicName
			for i := int32(0); i < 2; i++ {
				ps := kmsg.NewStopReplicaRequestTopicPartitionState()
				ps.Partition = i
				topic.PartitionStates = append(topic.PartitionStates, ps)
			}
			req.Topics = append(req.Topics, topic)
		}

		resp := HandleStopReplica(3, req, logger)

		if len(resp.Partitions) != 4 {
			t.Errorf("expected 4 partition responses (2 topics * 2 partitions), got %d", len(resp.Partitions))
		}

		for _, part := range resp.Partitions {
			if part.ErrorCode != errUnsupportedVersion {
				t.Errorf("expected UNSUPPORTED_VERSION error for %s/%d", part.Topic, part.Partition)
			}
		}
	})
}

func TestHandleUpdateMetadata(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION for all versions", func(t *testing.T) {
		for version := int16(0); version <= 8; version++ {
			req := kmsg.NewPtrUpdateMetadataRequest()
			req.SetVersion(version)
			req.ControllerID = 1
			req.BrokerEpoch = 100

			resp := HandleUpdateMetadata(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.ErrorCode)
			}
		}
	})

	t.Run("handles request with broker states", func(t *testing.T) {
		req := kmsg.NewPtrUpdateMetadataRequest()
		req.SetVersion(5)
		req.ControllerID = 1
		req.ControllerEpoch = 10

		// Add a broker state
		broker := kmsg.NewUpdateMetadataRequestLiveBroker()
		broker.ID = 1
		broker.Host = "localhost"
		broker.Port = 9092
		req.LiveBrokers = append(req.LiveBrokers, broker)

		resp := HandleUpdateMetadata(5, req, logger)

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error, got %d", resp.ErrorCode)
		}
	})

	t.Run("handles request with topic partitions", func(t *testing.T) {
		req := kmsg.NewPtrUpdateMetadataRequest()
		req.SetVersion(5)
		req.ControllerID = 1

		// Add topic state
		topic := kmsg.NewUpdateMetadataRequestTopicState()
		topic.Topic = "test-topic"
		part := kmsg.NewUpdateMetadataRequestTopicPartition()
		part.Topic = "test-topic"
		part.Partition = 0
		part.Leader = 1
		topic.PartitionStates = append(topic.PartitionStates, part)
		req.TopicStates = append(req.TopicStates, topic)

		resp := HandleUpdateMetadata(5, req, logger)

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error, got %d", resp.ErrorCode)
		}
	})
}

func TestHandleControlledShutdown(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION for supported versions", func(t *testing.T) {
		for version := int16(0); version <= 1; version++ {
			req := kmsg.NewPtrControlledShutdownRequest()
			req.SetVersion(version)

			resp := HandleControlledShutdown(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.ErrorCode)
			}
		}
	})
}

func TestInterBrokerAPIsNotAdvertised(t *testing.T) {
	// Verify that inter-broker APIs are not in the supported API list
	apis := GetSupportedAPIs()

	interBrokerKeys := map[int16]string{
		4: "LeaderAndISR",
		5: "StopReplica",
		6: "UpdateMetadata",
		7: "ControlledShutdown",
	}

	for apiKey, name := range interBrokerKeys {
		for _, api := range apis {
			if api.APIKey == apiKey {
				t.Errorf("%s (key %d) should not be advertised in ApiVersions", name, apiKey)
			}
		}
	}
}
