package protocol

import (
	"testing"

	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHandleAddPartitionsToTxn(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION for v0-v3", func(t *testing.T) {
		for version := int16(0); version <= 3; version++ {
			req := kmsg.NewPtrAddPartitionsToTxnRequest()
			req.SetVersion(version)
			req.TransactionalID = "test-txn-id"
			req.ProducerID = 1
			req.ProducerEpoch = 0

			topic := kmsg.NewAddPartitionsToTxnRequestTopic()
			topic.Topic = "test-topic"
			topic.Partitions = []int32{0, 1, 2}
			req.Topics = append(req.Topics, topic)

			resp := HandleAddPartitionsToTxn(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			// For v0-v3, errors are per-partition
			if len(resp.Topics) != 1 {
				t.Errorf("version %d: expected 1 topic response, got %d", version, len(resp.Topics))
				continue
			}

			if resp.Topics[0].Topic != "test-topic" {
				t.Errorf("version %d: expected topic 'test-topic', got %q", version, resp.Topics[0].Topic)
			}

			if len(resp.Topics[0].Partitions) != 3 {
				t.Errorf("version %d: expected 3 partition responses, got %d", version, len(resp.Topics[0].Partitions))
				continue
			}

			for _, part := range resp.Topics[0].Partitions {
				if part.ErrorCode != errUnsupportedVersion {
					t.Errorf("version %d: expected UNSUPPORTED_VERSION error (%d), got %d", version, errUnsupportedVersion, part.ErrorCode)
				}
			}
		}
	})

	t.Run("returns UNSUPPORTED_VERSION for v4+ in top-level ErrorCode", func(t *testing.T) {
		req := kmsg.NewPtrAddPartitionsToTxnRequest()
		req.SetVersion(4)
		req.TransactionalID = "test-txn-id"

		resp := HandleAddPartitionsToTxn(4, req, logger)

		if resp.Version != 4 {
			t.Errorf("expected response version 4, got %d", resp.Version)
		}

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected top-level UNSUPPORTED_VERSION error (%d), got %d", errUnsupportedVersion, resp.ErrorCode)
		}
	})

	t.Run("handles empty topics list", func(t *testing.T) {
		req := kmsg.NewPtrAddPartitionsToTxnRequest()
		req.SetVersion(1)
		req.TransactionalID = "test-txn-id"
		// No topics

		resp := HandleAddPartitionsToTxn(1, req, logger)

		if len(resp.Topics) != 0 {
			t.Errorf("expected 0 topic responses for empty request, got %d", len(resp.Topics))
		}
	})
}

func TestHandleAddOffsetsToTxn(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION for all versions", func(t *testing.T) {
		for version := int16(0); version <= 3; version++ {
			req := kmsg.NewPtrAddOffsetsToTxnRequest()
			req.SetVersion(version)
			req.TransactionalID = "test-txn-id"
			req.ProducerID = 1
			req.ProducerEpoch = 0
			req.Group = "test-group"

			resp := HandleAddOffsetsToTxn(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected UNSUPPORTED_VERSION error (%d), got %d", version, errUnsupportedVersion, resp.ErrorCode)
			}

			if resp.ThrottleMillis != 0 {
				t.Errorf("version %d: expected ThrottleMillis 0, got %d", version, resp.ThrottleMillis)
			}
		}
	})
}

func TestHandleEndTxn(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION for commit=true", func(t *testing.T) {
		req := kmsg.NewPtrEndTxnRequest()
		req.SetVersion(2)
		req.TransactionalID = "test-txn-id"
		req.ProducerID = 1
		req.ProducerEpoch = 0
		req.Commit = true

		resp := HandleEndTxn(2, req, logger)

		if resp.Version != 2 {
			t.Errorf("expected response version 2, got %d", resp.Version)
		}

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error (%d), got %d", errUnsupportedVersion, resp.ErrorCode)
		}
	})

	t.Run("returns UNSUPPORTED_VERSION for commit=false (abort)", func(t *testing.T) {
		req := kmsg.NewPtrEndTxnRequest()
		req.SetVersion(2)
		req.TransactionalID = "test-txn-id"
		req.ProducerID = 1
		req.ProducerEpoch = 0
		req.Commit = false

		resp := HandleEndTxn(2, req, logger)

		if resp.ErrorCode != errUnsupportedVersion {
			t.Errorf("expected UNSUPPORTED_VERSION error (%d), got %d", errUnsupportedVersion, resp.ErrorCode)
		}
	})

	t.Run("returns UNSUPPORTED_VERSION for all versions", func(t *testing.T) {
		for version := int16(0); version <= 4; version++ {
			req := kmsg.NewPtrEndTxnRequest()
			req.SetVersion(version)
			req.TransactionalID = "test-txn-id"
			req.ProducerID = 1
			req.ProducerEpoch = 0
			req.Commit = true

			resp := HandleEndTxn(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected UNSUPPORTED_VERSION error (%d), got %d", version, errUnsupportedVersion, resp.ErrorCode)
			}

			if resp.ThrottleMillis != 0 {
				t.Errorf("version %d: expected ThrottleMillis 0, got %d", version, resp.ThrottleMillis)
			}
		}
	})
}

func TestHandleTxnOffsetCommit(t *testing.T) {
	logger := logging.DefaultLogger()

	t.Run("returns UNSUPPORTED_VERSION for all partitions", func(t *testing.T) {
		req := kmsg.NewPtrTxnOffsetCommitRequest()
		req.SetVersion(3)
		req.TransactionalID = "test-txn-id"
		req.Group = "test-group"
		req.ProducerID = 1
		req.ProducerEpoch = 0

		topic := kmsg.NewTxnOffsetCommitRequestTopic()
		topic.Topic = "test-topic"

		for i := int32(0); i < 3; i++ {
			part := kmsg.NewTxnOffsetCommitRequestTopicPartition()
			part.Partition = i
			part.Offset = int64(i * 100)
			topic.Partitions = append(topic.Partitions, part)
		}
		req.Topics = append(req.Topics, topic)

		resp := HandleTxnOffsetCommit(3, req, logger)

		if resp.Version != 3 {
			t.Errorf("expected response version 3, got %d", resp.Version)
		}

		if len(resp.Topics) != 1 {
			t.Errorf("expected 1 topic response, got %d", len(resp.Topics))
			return
		}

		if resp.Topics[0].Topic != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %q", resp.Topics[0].Topic)
		}

		if len(resp.Topics[0].Partitions) != 3 {
			t.Errorf("expected 3 partition responses, got %d", len(resp.Topics[0].Partitions))
			return
		}

		for i, part := range resp.Topics[0].Partitions {
			if part.Partition != int32(i) {
				t.Errorf("partition %d: expected partition %d, got %d", i, i, part.Partition)
			}
			if part.ErrorCode != errUnsupportedVersion {
				t.Errorf("partition %d: expected UNSUPPORTED_VERSION error (%d), got %d", i, errUnsupportedVersion, part.ErrorCode)
			}
		}
	})

	t.Run("returns UNSUPPORTED_VERSION for all versions", func(t *testing.T) {
		for version := int16(0); version <= 3; version++ {
			req := kmsg.NewPtrTxnOffsetCommitRequest()
			req.SetVersion(version)
			req.TransactionalID = "test-txn-id"
			req.Group = "test-group"

			topic := kmsg.NewTxnOffsetCommitRequestTopic()
			topic.Topic = "test-topic"
			part := kmsg.NewTxnOffsetCommitRequestTopicPartition()
			part.Partition = 0
			part.Offset = 100
			topic.Partitions = append(topic.Partitions, part)
			req.Topics = append(req.Topics, topic)

			resp := HandleTxnOffsetCommit(version, req, logger)

			if resp.Version != version {
				t.Errorf("version %d: expected response version %d, got %d", version, version, resp.Version)
			}

			if resp.ThrottleMillis != 0 {
				t.Errorf("version %d: expected ThrottleMillis 0, got %d", version, resp.ThrottleMillis)
			}

			if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
				t.Errorf("version %d: expected 1 topic/1 partition response", version)
				continue
			}

			if resp.Topics[0].Partitions[0].ErrorCode != errUnsupportedVersion {
				t.Errorf("version %d: expected UNSUPPORTED_VERSION error (%d), got %d",
					version, errUnsupportedVersion, resp.Topics[0].Partitions[0].ErrorCode)
			}
		}
	})

	t.Run("handles multiple topics", func(t *testing.T) {
		req := kmsg.NewPtrTxnOffsetCommitRequest()
		req.SetVersion(3)
		req.TransactionalID = "test-txn-id"
		req.Group = "test-group"

		for _, topicName := range []string{"topic1", "topic2", "topic3"} {
			topic := kmsg.NewTxnOffsetCommitRequestTopic()
			topic.Topic = topicName
			part := kmsg.NewTxnOffsetCommitRequestTopicPartition()
			part.Partition = 0
			part.Offset = 100
			topic.Partitions = append(topic.Partitions, part)
			req.Topics = append(req.Topics, topic)
		}

		resp := HandleTxnOffsetCommit(3, req, logger)

		if len(resp.Topics) != 3 {
			t.Errorf("expected 3 topic responses, got %d", len(resp.Topics))
			return
		}

		for i, topicResp := range resp.Topics {
			expectedTopic := []string{"topic1", "topic2", "topic3"}[i]
			if topicResp.Topic != expectedTopic {
				t.Errorf("topic %d: expected %q, got %q", i, expectedTopic, topicResp.Topic)
			}
			if len(topicResp.Partitions) != 1 {
				t.Errorf("topic %d: expected 1 partition, got %d", i, len(topicResp.Partitions))
				continue
			}
			if topicResp.Partitions[0].ErrorCode != errUnsupportedVersion {
				t.Errorf("topic %d: expected UNSUPPORTED_VERSION error", i)
			}
		}
	})

	t.Run("handles empty topics list", func(t *testing.T) {
		req := kmsg.NewPtrTxnOffsetCommitRequest()
		req.SetVersion(3)
		req.TransactionalID = "test-txn-id"
		req.Group = "test-group"
		// No topics

		resp := HandleTxnOffsetCommit(3, req, logger)

		if len(resp.Topics) != 0 {
			t.Errorf("expected 0 topic responses for empty request, got %d", len(resp.Topics))
		}
	})
}
