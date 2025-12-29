package protocol

import (
	"github.com/dray-io/dray/internal/logging"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Inter-broker/controller APIs are explicitly not supported in Dray.
// Per spec 14.4, these APIs are for Kafka's internal replication protocol
// which is not needed in Dray's leaderless architecture.
//
// These handlers return UNSUPPORTED_VERSION (35) for any requests.
// The APIs are also not advertised in ApiVersions responses.

// HandleLeaderAndISR handles LeaderAndISR (key 4) requests.
// This is an inter-broker API not supported by Dray.
func HandleLeaderAndISR(version int16, req *kmsg.LeaderAndISRRequest, logger *logging.Logger) *kmsg.LeaderAndISRResponse {
	logger.Warnf("inter-broker API rejected: LeaderAndISR", map[string]any{
		"controllerId": req.ControllerID,
		"brokerEpoch":  req.BrokerEpoch,
		"reason":       "inter-broker APIs not supported per spec 14.4",
	})

	resp := kmsg.NewPtrLeaderAndISRResponse()
	resp.SetVersion(version)
	resp.ErrorCode = errUnsupportedVersion

	// For completeness, populate per-partition responses if present
	// The response structure depends on version:
	// - v0-v4: Uses flat Partitions array
	// - v5+: Uses Topics array (grouped by topic ID)
	if version >= 5 {
		for _, topic := range req.TopicStates {
			topicResp := kmsg.NewLeaderAndISRResponseTopic()
			topicResp.TopicID = topic.TopicID
			for _, part := range topic.PartitionStates {
				partResp := kmsg.NewLeaderAndISRResponseTopicPartition()
				partResp.Topic = topic.Topic
				partResp.Partition = part.Partition
				partResp.ErrorCode = errUnsupportedVersion
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
	} else {
		for _, part := range req.PartitionStates {
			partResp := kmsg.NewLeaderAndISRResponseTopicPartition()
			partResp.Topic = part.Topic
			partResp.Partition = part.Partition
			partResp.ErrorCode = errUnsupportedVersion
			resp.Partitions = append(resp.Partitions, partResp)
		}
	}

	return resp
}

// HandleStopReplica handles StopReplica (key 5) requests.
// This is an inter-broker API not supported by Dray.
func HandleStopReplica(version int16, req *kmsg.StopReplicaRequest, logger *logging.Logger) *kmsg.StopReplicaResponse {
	logger.Warnf("inter-broker API rejected: StopReplica", map[string]any{
		"controllerId": req.ControllerID,
		"brokerEpoch":  req.BrokerEpoch,
		"reason":       "inter-broker APIs not supported per spec 14.4",
	})

	resp := kmsg.NewPtrStopReplicaResponse()
	resp.SetVersion(version)
	resp.ErrorCode = errUnsupportedVersion

	// Populate per-partition responses from Topics array
	for _, topic := range req.Topics {
		// Version 3+ uses PartitionStates, older uses Partitions
		if len(topic.PartitionStates) > 0 {
			for _, ps := range topic.PartitionStates {
				partResp := kmsg.NewStopReplicaResponsePartition()
				partResp.Topic = topic.Topic
				partResp.Partition = ps.Partition
				partResp.ErrorCode = errUnsupportedVersion
				resp.Partitions = append(resp.Partitions, partResp)
			}
		} else if len(topic.Partitions) > 0 {
			for _, p := range topic.Partitions {
				partResp := kmsg.NewStopReplicaResponsePartition()
				partResp.Topic = topic.Topic
				partResp.Partition = p
				partResp.ErrorCode = errUnsupportedVersion
				resp.Partitions = append(resp.Partitions, partResp)
			}
		} else {
			// v1 uses a single Partition field
			partResp := kmsg.NewStopReplicaResponsePartition()
			partResp.Topic = topic.Topic
			partResp.Partition = topic.Partition
			partResp.ErrorCode = errUnsupportedVersion
			resp.Partitions = append(resp.Partitions, partResp)
		}
	}

	return resp
}

// HandleUpdateMetadata handles UpdateMetadata (key 6) requests.
// This is an inter-broker API not supported by Dray.
func HandleUpdateMetadata(version int16, req *kmsg.UpdateMetadataRequest, logger *logging.Logger) *kmsg.UpdateMetadataResponse {
	logger.Warnf("inter-broker API rejected: UpdateMetadata", map[string]any{
		"controllerId": req.ControllerID,
		"brokerEpoch":  req.BrokerEpoch,
		"reason":       "inter-broker APIs not supported per spec 14.4",
	})

	resp := kmsg.NewPtrUpdateMetadataResponse()
	resp.SetVersion(version)
	resp.ErrorCode = errUnsupportedVersion

	return resp
}

// HandleControlledShutdown handles ControlledShutdown (key 7) requests.
// This is a controller API not supported by Dray.
func HandleControlledShutdown(version int16, _ *kmsg.ControlledShutdownRequest, logger *logging.Logger) *kmsg.ControlledShutdownResponse {
	logger.Warnf("inter-broker API rejected: ControlledShutdown", map[string]any{
		"reason": "inter-broker APIs not supported per spec 14.4",
	})

	resp := kmsg.NewPtrControlledShutdownResponse()
	resp.SetVersion(version)
	resp.ErrorCode = errUnsupportedVersion

	return resp
}
