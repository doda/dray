package protocol

import (
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Transaction API error codes.
// Using UNSUPPORTED_VERSION (35) per task requirements to indicate
// that transactions are not supported in Dray (spec 14.3).
const (
	errUnsupportedVersion int16 = 35 // UNSUPPORTED_VERSION
)

// HandleAddPartitionsToTxn handles AddPartitionsToTxn (key 24) requests.
// Transactions are explicitly deferred per spec 14.3, so this returns
// UNSUPPORTED_VERSION for all requests.
func HandleAddPartitionsToTxn(version int16, req *kmsg.AddPartitionsToTxnRequest, logger *logging.Logger) *kmsg.AddPartitionsToTxnResponse {
	logger.Warnf("transaction API rejected: AddPartitionsToTxn", map[string]any{
		"transactionalId": req.TransactionalID,
		"reason":          "transactions are deferred per spec 14.3",
	})

	resp := kmsg.NewPtrAddPartitionsToTxnResponse()
	resp.SetVersion(version)
	resp.ThrottleMillis = 0

	// For v4+, use the top-level ErrorCode field
	if version >= 4 {
		resp.ErrorCode = errUnsupportedVersion
		return resp
	}

	// For v0-v3, populate per-topic responses with error code
	for _, topic := range req.Topics {
		topicResp := kmsg.NewAddPartitionsToTxnResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
			partResp.Partition = partition
			partResp.ErrorCode = errUnsupportedVersion
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}

// HandleAddOffsetsToTxn handles AddOffsetsToTxn (key 25) requests.
// Transactions are explicitly deferred per spec 14.3, so this returns
// UNSUPPORTED_VERSION for all requests.
func HandleAddOffsetsToTxn(version int16, req *kmsg.AddOffsetsToTxnRequest, logger *logging.Logger) *kmsg.AddOffsetsToTxnResponse {
	logger.Warnf("transaction API rejected: AddOffsetsToTxn", map[string]any{
		"transactionalId": req.TransactionalID,
		"groupId":         req.Group,
		"reason":          "transactions are deferred per spec 14.3",
	})

	resp := kmsg.NewPtrAddOffsetsToTxnResponse()
	resp.SetVersion(version)
	resp.ThrottleMillis = 0
	resp.ErrorCode = errUnsupportedVersion

	return resp
}

// HandleEndTxn handles EndTxn (key 26) requests.
// Transactions are explicitly deferred per spec 14.3, so this returns
// UNSUPPORTED_VERSION for all requests.
func HandleEndTxn(version int16, req *kmsg.EndTxnRequest, logger *logging.Logger) *kmsg.EndTxnResponse {
	logger.Warnf("transaction API rejected: EndTxn", map[string]any{
		"transactionalId": req.TransactionalID,
		"commit":          req.Commit,
		"reason":          "transactions are deferred per spec 14.3",
	})

	resp := kmsg.NewPtrEndTxnResponse()
	resp.SetVersion(version)
	resp.ThrottleMillis = 0
	resp.ErrorCode = errUnsupportedVersion

	return resp
}

// HandleTxnOffsetCommit handles TxnOffsetCommit (key 28) requests.
// Transactions are explicitly deferred per spec 14.3, so this returns
// UNSUPPORTED_VERSION for all requests.
func HandleTxnOffsetCommit(version int16, req *kmsg.TxnOffsetCommitRequest, logger *logging.Logger) *kmsg.TxnOffsetCommitResponse {
	logger.Warnf("transaction API rejected: TxnOffsetCommit", map[string]any{
		"transactionalId": req.TransactionalID,
		"groupId":         req.Group,
		"reason":          "transactions are deferred per spec 14.3",
	})

	resp := kmsg.NewPtrTxnOffsetCommitResponse()
	resp.SetVersion(version)
	resp.ThrottleMillis = 0

	// Populate per-topic responses with error code
	for _, topic := range req.Topics {
		topicResp := kmsg.NewTxnOffsetCommitResponseTopic()
		topicResp.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.ErrorCode = errUnsupportedVersion
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp
}
