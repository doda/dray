package protocol

import "github.com/twmb/franz-go/pkg/kmsg"

// BuildFallbackErrorResponse builds an encoded response for handler failures
// using UNKNOWN_SERVER_ERROR and the original correlation ID.
func BuildFallbackErrorResponse(apiKey, version int16, correlationID int32, payload []byte) ([]byte, error) {
	resp, err := BuildErrorResponse(apiKey, version, payload, errUnknownServerError)
	if err != nil {
		return nil, err
	}
	encoder := NewEncoder()
	return encoder.EncodeResponseWithCorrelationID(correlationID, resp), nil
}

// BuildErrorResponse builds a response for the given API key with a uniform error code.
// It attempts to decode the request payload so it can mirror requested topics/partitions.
func BuildErrorResponse(apiKey, version int16, payload []byte, errorCode int16) (*Response, error) {
	req, err := NewRequest(apiKey)
	if err != nil {
		return buildEmptyErrorResponse(apiKey, version, errorCode)
	}
	req.SetVersion(version)
	if err := req.ReadFrom(payload); err != nil {
		return buildEmptyErrorResponse(apiKey, version, errorCode)
	}
	return buildErrorResponseFromRequest(version, req.Inner(), errorCode)
}

func buildErrorResponseFromRequest(version int16, req kmsg.Request, errorCode int16) (*Response, error) {
	switch r := req.(type) {
	case *kmsg.ApiVersionsRequest:
		resp := kmsg.NewPtrApiVersionsResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		if version >= 1 {
			resp.ThrottleMillis = 0
		}
		return WrapResponse(resp), nil

	case *kmsg.MetadataRequest:
		resp := kmsg.NewPtrMetadataResponse()
		resp.SetVersion(version)
		for _, topic := range r.Topics {
			topicResp := kmsg.NewMetadataResponseTopic()
			topicResp.Topic = topic.Topic
			topicResp.ErrorCode = errorCode
			resp.Topics = append(resp.Topics, topicResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.ProduceRequest:
		resp := kmsg.NewPtrProduceResponse()
		resp.SetVersion(version)
		for _, topic := range r.Topics {
			topicResp := kmsg.NewProduceResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewProduceResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = errorCode
				partResp.BaseOffset = -1
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		if version >= 1 {
			resp.ThrottleMillis = 0
		}
		return WrapResponse(resp), nil

	case *kmsg.FetchRequest:
		resp := kmsg.NewPtrFetchResponse()
		resp.SetVersion(version)
		for _, topic := range r.Topics {
			topicResp := kmsg.NewFetchResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewFetchResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = errorCode
				partResp.HighWatermark = 0
				partResp.LastStableOffset = 0
				partResp.LogStartOffset = 0
				if version >= 11 {
					partResp.PreferredReadReplica = -1
				}
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		if version >= 1 {
			resp.ThrottleMillis = 0
		}
		return WrapResponse(resp), nil

	case *kmsg.ListOffsetsRequest:
		resp := kmsg.NewPtrListOffsetsResponse()
		resp.SetVersion(version)
		for _, topic := range r.Topics {
			topicResp := kmsg.NewListOffsetsResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewListOffsetsResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = errorCode
				if version >= 1 {
					partResp.Timestamp = -1
					partResp.Offset = -1
				}
				if version >= 4 {
					partResp.LeaderEpoch = -1
				}
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		if version >= 1 {
			resp.ThrottleMillis = 0
		}
		return WrapResponse(resp), nil

	case *kmsg.OffsetCommitRequest:
		resp := kmsg.NewPtrOffsetCommitResponse()
		resp.SetVersion(version)
		for _, topic := range r.Topics {
			topicResp := kmsg.NewOffsetCommitResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetCommitResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = errorCode
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.OffsetFetchRequest:
		resp := kmsg.NewPtrOffsetFetchResponse()
		resp.SetVersion(version)
		if version >= 8 && len(r.Groups) > 0 {
			resp.ThrottleMillis = 0
			for _, group := range r.Groups {
				groupResp := kmsg.NewOffsetFetchResponseGroup()
				groupResp.Group = group.Group
				groupResp.ErrorCode = errorCode
				groupResp.Topics = buildOffsetFetchGroupErrorTopics(version, group.Topics, errorCode)
				resp.Groups = append(resp.Groups, groupResp)
			}
			return WrapResponse(resp), nil
		}

		resp.ErrorCode = errorCode
		if version >= 3 {
			resp.ThrottleMillis = 0
		}
		for _, topic := range r.Topics {
			topicResp := kmsg.NewOffsetFetchResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewOffsetFetchResponseTopicPartition()
				partResp.Partition = partition
				partResp.Offset = -1
				partResp.ErrorCode = errorCode
				if version >= 5 {
					partResp.LeaderEpoch = -1
				}
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.FindCoordinatorRequest:
		resp := kmsg.NewPtrFindCoordinatorResponse()
		resp.SetVersion(version)
		if version >= 4 {
			for _, key := range r.CoordinatorKeys {
				coordinator := kmsg.NewFindCoordinatorResponseCoordinator()
				coordinator.Key = key
				coordinator.ErrorCode = errorCode
				coordinator.NodeID = -1
				coordinator.Port = -1
				resp.Coordinators = append(resp.Coordinators, coordinator)
			}
			return WrapResponse(resp), nil
		}
		resp.ErrorCode = errorCode
		resp.NodeID = -1
		resp.Port = -1
		return WrapResponse(resp), nil

	case *kmsg.JoinGroupRequest:
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		resp.Generation = -1
		return WrapResponse(resp), nil

	case *kmsg.HeartbeatRequest:
		resp := kmsg.NewPtrHeartbeatResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.LeaveGroupRequest:
		resp := kmsg.NewPtrLeaveGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.SyncGroupRequest:
		resp := kmsg.NewPtrSyncGroupResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.DescribeGroupsRequest:
		resp := kmsg.NewPtrDescribeGroupsResponse()
		resp.SetVersion(version)
		for _, groupID := range r.Groups {
			group := kmsg.NewDescribeGroupsResponseGroup()
			group.Group = groupID
			group.ErrorCode = errorCode
			resp.Groups = append(resp.Groups, group)
		}
		return WrapResponse(resp), nil

	case *kmsg.ListGroupsRequest:
		resp := kmsg.NewPtrListGroupsResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		if version >= 1 {
			resp.ThrottleMillis = 0
		}
		return WrapResponse(resp), nil

	case *kmsg.CreateTopicsRequest:
		resp := kmsg.NewPtrCreateTopicsResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		for _, topic := range r.Topics {
			topicResp := kmsg.NewCreateTopicsResponseTopic()
			topicResp.Topic = topic.Topic
			topicResp.ErrorCode = errorCode
			resp.Topics = append(resp.Topics, topicResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.DeleteTopicsRequest:
		resp := kmsg.NewPtrDeleteTopicsResponse()
		resp.SetVersion(version)
		if version >= 1 {
			resp.ThrottleMillis = 0
		}
		if version <= 5 {
			for _, name := range r.TopicNames {
				topicResp := kmsg.NewDeleteTopicsResponseTopic()
				topicResp.Topic = &name
				topicResp.ErrorCode = errorCode
				resp.Topics = append(resp.Topics, topicResp)
			}
		} else {
			for _, topic := range r.Topics {
				topicResp := kmsg.NewDeleteTopicsResponseTopic()
				topicResp.Topic = topic.Topic
				topicResp.TopicID = topic.TopicID
				topicResp.ErrorCode = errorCode
				resp.Topics = append(resp.Topics, topicResp)
			}
		}
		return WrapResponse(resp), nil

	case *kmsg.DescribeConfigsRequest:
		resp := kmsg.NewPtrDescribeConfigsResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		for _, resource := range r.Resources {
			resourceResp := kmsg.NewDescribeConfigsResponseResource()
			resourceResp.ResourceType = resource.ResourceType
			resourceResp.ResourceName = resource.ResourceName
			resourceResp.ErrorCode = errorCode
			resp.Resources = append(resp.Resources, resourceResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.IncrementalAlterConfigsRequest:
		resp := kmsg.NewPtrIncrementalAlterConfigsResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		for _, resource := range r.Resources {
			resourceResp := kmsg.NewIncrementalAlterConfigsResponseResource()
			resourceResp.ResourceType = resource.ResourceType
			resourceResp.ResourceName = resource.ResourceName
			resourceResp.ErrorCode = errorCode
			resp.Resources = append(resp.Resources, resourceResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.DeleteGroupsRequest:
		resp := kmsg.NewPtrDeleteGroupsResponse()
		resp.SetVersion(version)
		for _, groupID := range r.Groups {
			groupResp := kmsg.NewDeleteGroupsResponseGroup()
			groupResp.Group = groupID
			groupResp.ErrorCode = errorCode
			resp.Groups = append(resp.Groups, groupResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.DescribeClusterRequest:
		resp := kmsg.NewPtrDescribeClusterResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.ConsumerGroupHeartbeatRequest:
		resp := kmsg.NewPtrConsumerGroupHeartbeatResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.ConsumerGroupDescribeRequest:
		resp := kmsg.NewPtrConsumerGroupDescribeResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		for _, groupID := range r.Groups {
			group := kmsg.NewConsumerGroupDescribeResponseGroup()
			group.Group = groupID
			group.ErrorCode = errorCode
			resp.Groups = append(resp.Groups, group)
		}
		return WrapResponse(resp), nil

	case *kmsg.AddPartitionsToTxnRequest:
		resp := kmsg.NewPtrAddPartitionsToTxnResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		if version >= 4 {
			resp.ErrorCode = errorCode
			return WrapResponse(resp), nil
		}
		for _, topic := range r.Topics {
			topicResp := kmsg.NewAddPartitionsToTxnResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
				partResp.Partition = partition
				partResp.ErrorCode = errorCode
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.AddOffsetsToTxnRequest:
		resp := kmsg.NewPtrAddOffsetsToTxnResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.EndTxnRequest:
		resp := kmsg.NewPtrEndTxnResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.TxnOffsetCommitRequest:
		resp := kmsg.NewPtrTxnOffsetCommitResponse()
		resp.SetVersion(version)
		resp.ThrottleMillis = 0
		for _, topic := range r.Topics {
			topicResp := kmsg.NewTxnOffsetCommitResponseTopic()
			topicResp.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				partResp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = errorCode
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}
			resp.Topics = append(resp.Topics, topicResp)
		}
		return WrapResponse(resp), nil

	case *kmsg.LeaderAndISRRequest:
		resp := kmsg.NewPtrLeaderAndISRResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.StopReplicaRequest:
		resp := kmsg.NewPtrStopReplicaResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.UpdateMetadataRequest:
		resp := kmsg.NewPtrUpdateMetadataResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	case *kmsg.ControlledShutdownRequest:
		resp := kmsg.NewPtrControlledShutdownResponse()
		resp.SetVersion(version)
		resp.ErrorCode = errorCode
		return WrapResponse(resp), nil

	default:
		return buildEmptyErrorResponse(req.Key(), version, errorCode)
	}
}

func buildOffsetFetchGroupErrorTopics(version int16, topics []kmsg.OffsetFetchRequestGroupTopic, errorCode int16) []kmsg.OffsetFetchResponseGroupTopic {
	var respTopics []kmsg.OffsetFetchResponseGroupTopic
	for _, topic := range topics {
		respTopic := kmsg.NewOffsetFetchResponseGroupTopic()
		respTopic.Topic = topic.Topic
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
			partResp.Partition = partition
			partResp.Offset = -1
			partResp.ErrorCode = errorCode
			partResp.LeaderEpoch = -1
			respTopic.Partitions = append(respTopic.Partitions, partResp)
		}
		respTopics = append(respTopics, respTopic)
	}
	return respTopics
}

func buildEmptyErrorResponse(apiKey, version int16, errorCode int16) (*Response, error) {
	resp, err := NewResponse(apiKey)
	if err != nil {
		return nil, err
	}
	resp.SetVersion(version)
	switch r := resp.Inner().(type) {
	case *kmsg.ApiVersionsResponse:
		r.ErrorCode = errorCode
		if version >= 1 {
			r.ThrottleMillis = 0
		}
	case *kmsg.ListGroupsResponse:
		r.ErrorCode = errorCode
		if version >= 1 {
			r.ThrottleMillis = 0
		}
	case *kmsg.DescribeClusterResponse:
		r.ErrorCode = errorCode
		r.ThrottleMillis = 0
	case *kmsg.OffsetFetchResponse:
		r.ErrorCode = errorCode
		if version >= 3 {
			r.ThrottleMillis = 0
		}
	case *kmsg.FindCoordinatorResponse:
		r.ErrorCode = errorCode
		r.NodeID = -1
		r.Port = -1
	case *kmsg.JoinGroupResponse:
		r.ErrorCode = errorCode
		r.Generation = -1
	case *kmsg.HeartbeatResponse:
		r.ErrorCode = errorCode
	case *kmsg.LeaveGroupResponse:
		r.ErrorCode = errorCode
	case *kmsg.SyncGroupResponse:
		r.ErrorCode = errorCode
	case *kmsg.CreateTopicsResponse:
		r.ThrottleMillis = 0
	case *kmsg.DeleteTopicsResponse:
		if version >= 1 {
			r.ThrottleMillis = 0
		}
	case *kmsg.DescribeConfigsResponse:
		r.ThrottleMillis = 0
	case *kmsg.IncrementalAlterConfigsResponse:
		r.ThrottleMillis = 0
	case *kmsg.DeleteGroupsResponse:
	case *kmsg.ConsumerGroupHeartbeatResponse:
		r.ErrorCode = errorCode
	case *kmsg.ConsumerGroupDescribeResponse:
		r.ThrottleMillis = 0
	case *kmsg.AddOffsetsToTxnResponse:
		r.ErrorCode = errorCode
		r.ThrottleMillis = 0
	case *kmsg.EndTxnResponse:
		r.ErrorCode = errorCode
		r.ThrottleMillis = 0
	case *kmsg.UpdateMetadataResponse:
		r.ErrorCode = errorCode
	case *kmsg.ControlledShutdownResponse:
		r.ErrorCode = errorCode
	}
	return resp, nil
}
