package groups

import (
	"encoding/binary"
	"errors"
	"sort"
)

// Assignor is the interface for partition assignment strategies.
type Assignor interface {
	// Name returns the name of the assignor.
	Name() string

	// Assign assigns partitions to members.
	// members maps memberID -> subscription (parsed metadata)
	// topics maps topicName -> partition count
	// Returns memberID -> TopicPartitions assignment
	Assign(members map[string]*Subscription, topics map[string]int32) (map[string][]TopicPartition, error)
}

// Subscription represents a member's topic subscription parsed from metadata.
type Subscription struct {
	Topics   []string
	UserData []byte
}

// TopicPartition represents a topic-partition pair.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// ParseSubscription parses consumer protocol metadata into a Subscription.
// The format is:
//   - Version (2 bytes, int16)
//   - Topics array length (4 bytes, int32)
//   - For each topic: string length (2 bytes, int16) + topic name
//   - UserData length (4 bytes, int32)
//   - UserData bytes
func ParseSubscription(data []byte) (*Subscription, error) {
	if len(data) < 6 {
		return nil, errors.New("subscription data too short")
	}

	offset := 0

	// Version (ignored for now, just skip)
	offset += 2

	// Topics array length
	if offset+4 > len(data) {
		return nil, errors.New("subscription data truncated at topics length")
	}
	topicsLen := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if topicsLen < 0 {
		return nil, errors.New("invalid topics array length")
	}

	topics := make([]string, 0, topicsLen)
	for i := int32(0); i < topicsLen; i++ {
		if offset+2 > len(data) {
			return nil, errors.New("subscription data truncated at topic name length")
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if offset+topicLen > len(data) {
			return nil, errors.New("subscription data truncated at topic name")
		}
		topic := string(data[offset : offset+topicLen])
		offset += topicLen
		topics = append(topics, topic)
	}

	// UserData (optional)
	var userData []byte
	if offset+4 <= len(data) {
		userDataLen := int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if userDataLen > 0 && offset+int(userDataLen) <= len(data) {
			userData = data[offset : offset+int(userDataLen)]
		}
	}

	return &Subscription{
		Topics:   topics,
		UserData: userData,
	}, nil
}

// EncodeAssignment encodes a partition assignment to consumer protocol format.
// The format is:
//   - Version (2 bytes, int16) = 0
//   - Topics array length (4 bytes, int32)
//   - For each topic:
//   - Topic name length (2 bytes, int16) + topic name
//   - Partitions array length (4 bytes, int32)
//   - For each partition: partition id (4 bytes, int32)
//   - UserData length (4 bytes, int32) = 0 (no user data)
func EncodeAssignment(partitions []TopicPartition) []byte {
	// Group partitions by topic
	byTopic := make(map[string][]int32)
	for _, tp := range partitions {
		byTopic[tp.Topic] = append(byTopic[tp.Topic], tp.Partition)
	}

	// Calculate size
	size := 2 + 4 // version + topics array length
	for topic, parts := range byTopic {
		size += 2 + len(topic) // topic name length + topic name
		size += 4              // partitions array length
		size += 4 * len(parts) // partition ids
	}
	size += 4 // user data length

	buf := make([]byte, size)
	offset := 0

	// Version
	binary.BigEndian.PutUint16(buf[offset:], 0)
	offset += 2

	// Topics array length
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(byTopic)))
	offset += 4

	// Sort topics for deterministic output
	topics := make([]string, 0, len(byTopic))
	for topic := range byTopic {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		parts := byTopic[topic]

		// Topic name length
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(topic)))
		offset += 2

		// Topic name
		copy(buf[offset:], topic)
		offset += len(topic)

		// Sort partitions
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })

		// Partitions array length
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(parts)))
		offset += 4

		// Partition ids
		for _, p := range parts {
			binary.BigEndian.PutUint32(buf[offset:], uint32(p))
			offset += 4
		}
	}

	// UserData length (0)
	binary.BigEndian.PutUint32(buf[offset:], 0)

	return buf
}

// RangeAssignor implements the range partition assignment strategy.
// For each topic, it sorts consumers by member ID and assigns partitions
// in ranges. The first few consumers get ceil(N/M) partitions while the
// remaining get floor(N/M), where N is the number of partitions and M
// is the number of consumers subscribed to that topic.
type RangeAssignor struct{}

// Name returns the assignor name.
func (r *RangeAssignor) Name() string {
	return "range"
}

// Assign implements the range assignment strategy.
func (r *RangeAssignor) Assign(members map[string]*Subscription, topics map[string]int32) (map[string][]TopicPartition, error) {
	if len(members) == 0 {
		return nil, nil
	}

	// Initialize result
	result := make(map[string][]TopicPartition)
	for memberID := range members {
		result[memberID] = nil
	}

	// Collect all topic names sorted for deterministic assignment
	topicNames := make([]string, 0, len(topics))
	for topic := range topics {
		topicNames = append(topicNames, topic)
	}
	sort.Strings(topicNames)

	// For each topic, find subscribed consumers and assign partitions
	for _, topic := range topicNames {
		numPartitions := topics[topic]
		if numPartitions <= 0 {
			continue
		}

		// Collect consumers subscribed to this topic, sorted by member ID
		var consumers []string
		for memberID, sub := range members {
			for _, t := range sub.Topics {
				if t == topic {
					consumers = append(consumers, memberID)
					break
				}
			}
		}

		if len(consumers) == 0 {
			continue
		}

		// Sort consumers by member ID for deterministic assignment
		sort.Strings(consumers)

		// Assign partitions using range strategy
		numConsumers := int32(len(consumers))
		partitionsPerConsumer := numPartitions / numConsumers
		consumersWithExtraPartition := numPartitions % numConsumers

		currentPartition := int32(0)
		for i, consumer := range consumers {
			// Determine how many partitions this consumer gets
			numToAssign := partitionsPerConsumer
			if int32(i) < consumersWithExtraPartition {
				numToAssign++
			}

			// Assign the partition range
			for j := int32(0); j < numToAssign; j++ {
				result[consumer] = append(result[consumer], TopicPartition{
					Topic:     topic,
					Partition: currentPartition,
				})
				currentPartition++
			}
		}
	}

	return result, nil
}

// NewRangeAssignor creates a new range assignor.
func NewRangeAssignor() Assignor {
	return &RangeAssignor{}
}

// RoundRobinAssignor implements the round-robin partition assignment strategy.
// It sorts all partitions by topic-partition and then assigns them to consumers
// in a round-robin fashion. This results in more even distribution across topics
// compared to the range assignor.
type RoundRobinAssignor struct{}

// Name returns the assignor name.
func (r *RoundRobinAssignor) Name() string {
	return "roundrobin"
}

// Assign implements the round-robin assignment strategy.
// This uses true cursor-based round-robin: partitions are sorted by topic-partition,
// consumers are sorted by member ID, and a cursor advances through consumers
// for each partition assignment, skipping consumers not subscribed to the topic.
func (r *RoundRobinAssignor) Assign(members map[string]*Subscription, topics map[string]int32) (map[string][]TopicPartition, error) {
	if len(members) == 0 {
		return nil, nil
	}

	// Initialize result
	result := make(map[string][]TopicPartition)
	for memberID := range members {
		result[memberID] = nil
	}

	// Build set of topic subscriptions per member
	memberTopics := make(map[string]map[string]bool)
	for memberID, sub := range members {
		memberTopics[memberID] = make(map[string]bool)
		for _, topic := range sub.Topics {
			memberTopics[memberID][topic] = true
		}
	}

	// Collect all partitions from all topics, sorted by topic then partition
	var allPartitions []TopicPartition
	topicNames := make([]string, 0, len(topics))
	for topic := range topics {
		topicNames = append(topicNames, topic)
	}
	sort.Strings(topicNames)

	for _, topic := range topicNames {
		numPartitions := topics[topic]
		for p := int32(0); p < numPartitions; p++ {
			allPartitions = append(allPartitions, TopicPartition{
				Topic:     topic,
				Partition: p,
			})
		}
	}

	// Collect and sort member IDs for deterministic ordering
	memberIDs := make([]string, 0, len(members))
	for memberID := range members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	// True round-robin: maintain a cursor that advances through consumers
	cursor := 0
	numMembers := len(memberIDs)

	// Assign partitions round-robin to consumers that are subscribed to the topic
	for _, partition := range allPartitions {
		// Find the next consumer subscribed to this partition's topic
		// starting from current cursor position
		for i := 0; i < numMembers; i++ {
			memberIdx := (cursor + i) % numMembers
			memberID := memberIDs[memberIdx]
			if memberTopics[memberID][partition.Topic] {
				result[memberID] = append(result[memberID], partition)
				// Advance cursor past this member for next partition
				cursor = (memberIdx + 1) % numMembers
				break
			}
		}
		// If no member is subscribed, partition is unassigned (cursor unchanged).
	}

	return result, nil
}

// NewRoundRobinAssignor creates a new round-robin assignor.
func NewRoundRobinAssignor() Assignor {
	return &RoundRobinAssignor{}
}
