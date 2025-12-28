package groups

import (
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
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

// UniformAssignor implements the uniform partition assignment strategy for KIP-848.
// It balances partitions evenly across members while minimizing partition movement
// on rebalance through sticky behavior. This is the default assignor for KIP-848
// consumer groups.
//
// The algorithm:
// 1. Use current assignments as a starting point (sticky behavior)
// 2. Calculate target: each member should have floor(P/M) or ceil(P/M) partitions
// 3. Revoke partitions from over-assigned members
// 4. Assign unassigned partitions to under-assigned members
// 5. Balance across members to minimize difference in partition counts
type UniformAssignor struct{}

// Name returns the assignor name.
func (u *UniformAssignor) Name() string {
	return "uniform"
}

// Assign implements the uniform assignment strategy.
// members: map of memberID -> subscription
// topics: map of topicName -> partition count
// Returns memberID -> list of TopicPartition assignments
//
// The algorithm implements global balancing:
// 1. Calculate total partitions across all topics each member can consume
// 2. Compute global target: each member gets floor(total/M) or ceil(total/M) partitions
// 3. Remainder allocation is based on current assignment counts (sticky-aware)
// 4. Keep existing assignments up to target, revoke excess
// 5. Assign unassigned partitions to under-assigned members
func (u *UniformAssignor) Assign(members map[string]*Subscription, topics map[string]int32) (map[string][]TopicPartition, error) {
	if len(members) == 0 {
		return nil, nil
	}

	// Initialize result for all members
	result := make(map[string][]TopicPartition)
	for memberID := range members {
		result[memberID] = nil
	}

	// Build topic subscription mapping: topic -> list of subscribed members
	topicSubscribers := make(map[string][]string)
	for memberID, sub := range members {
		for _, topic := range sub.Topics {
			topicSubscribers[topic] = append(topicSubscribers[topic], memberID)
		}
	}

	// Sort subscribers for deterministic assignment
	for topic := range topicSubscribers {
		sort.Strings(topicSubscribers[topic])
	}

	// Collect topic names sorted for deterministic processing
	topicNames := make([]string, 0, len(topics))
	for topic := range topics {
		topicNames = append(topicNames, topic)
	}
	sort.Strings(topicNames)

	// Get sorted member IDs
	memberIDs := make([]string, 0, len(members))
	for memberID := range members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	// Try to parse existing assignments from user data (for sticky behavior)
	currentAssignments := u.parseCurrentAssignments(members)

	// Calculate global target: total partitions each member should handle
	// Members are grouped by their subscription set - members with identical
	// subscriptions should get the same number of partitions.
	// For simplicity, we compute per-topic targets but use global counts
	// to determine remainder allocation.

	// First pass: count total partitions each member can receive
	memberCapacity := make(map[string]int32)
	for _, topic := range topicNames {
		numPartitions := topics[topic]
		subscribers := topicSubscribers[topic]
		if len(subscribers) == 0 || numPartitions <= 0 {
			continue
		}

		// Each subscriber to this topic can receive partitions
		for _, memberID := range subscribers {
			memberCapacity[memberID] += numPartitions
		}
	}

	// Phase 1: Collect valid sticky assignments and track what's already assigned
	// assignedPartitions tracks which member has which partition (globally)
	assignedPartitions := make(map[string]string) // "topic:partition" -> memberID
	memberAssignments := make(map[string][]TopicPartition)

	for _, memberID := range memberIDs {
		if partitions, ok := currentAssignments[memberID]; ok {
			for _, tp := range partitions {
				// Check if this topic-partition is valid
				if numPartitions, exists := topics[tp.Topic]; !exists || tp.Partition >= numPartitions {
					continue // topic doesn't exist or partition out of range
				}
				// Check if member is subscribed to this topic
				if _, subscribed := memberCapacity[memberID]; !subscribed {
					continue
				}
				subscribed := false
				if sub, ok := members[memberID]; ok {
					for _, t := range sub.Topics {
						if t == tp.Topic {
							subscribed = true
							break
						}
					}
				}
				if !subscribed {
					continue
				}
				key := partitionKey(tp)
				if _, already := assignedPartitions[key]; !already {
					memberAssignments[memberID] = append(memberAssignments[memberID], tp)
					assignedPartitions[key] = memberID
				}
			}
		}
	}

	// Phase 2: Calculate per-topic targets with global-aware remainder allocation
	// For each topic, compute base and remainder, but allocate remainder to members
	// with fewer current total assignments to minimize movement
	for _, topic := range topicNames {
		numPartitions := topics[topic]
		subscribers := topicSubscribers[topic]
		if len(subscribers) == 0 || numPartitions <= 0 {
			continue
		}

		numSubscribers := int32(len(subscribers))
		baseCount := numPartitions / numSubscribers
		remainder := numPartitions % numSubscribers

		// Build target counts for each subscriber
		// Remainder goes to members with fewer total current assignments
		targets := make(map[string]int32)
		for _, memberID := range subscribers {
			targets[memberID] = baseCount
		}

		// Allocate remainder based on current total assignments (fewest first, then by memberID for determinism)
		if remainder > 0 {
			// Sort subscribers by current assignment count (ascending), then by memberID
			type memberCount struct {
				memberID string
				count    int
			}
			counts := make([]memberCount, len(subscribers))
			for i, memberID := range subscribers {
				counts[i] = memberCount{memberID: memberID, count: len(memberAssignments[memberID])}
			}
			sort.Slice(counts, func(i, j int) bool {
				if counts[i].count != counts[j].count {
					return counts[i].count < counts[j].count
				}
				return counts[i].memberID < counts[j].memberID
			})

			// Give extra partition to members with lowest counts
			for i := int32(0); i < remainder; i++ {
				targets[counts[i].memberID]++
			}
		}

		// Collect current assignments for this topic
		topicAssignments := make(map[string][]TopicPartition)
		for _, memberID := range subscribers {
			for _, tp := range memberAssignments[memberID] {
				if tp.Topic == topic {
					topicAssignments[memberID] = append(topicAssignments[memberID], tp)
				}
			}
		}

		// Revoke excess partitions from over-assigned members
		for _, memberID := range subscribers {
			current := topicAssignments[memberID]
			target := targets[memberID]

			if int32(len(current)) > target {
				// Sort to keep lower partition numbers for determinism
				sort.Slice(current, func(i, j int) bool {
					return current[i].Partition < current[j].Partition
				})

				keep := current[:target]
				revoke := current[target:]

				topicAssignments[memberID] = keep
				for _, tp := range revoke {
					delete(assignedPartitions, partitionKey(tp))
					// Also remove from memberAssignments
					newAssign := make([]TopicPartition, 0)
					for _, existing := range memberAssignments[memberID] {
						if existing.Topic != tp.Topic || existing.Partition != tp.Partition {
							newAssign = append(newAssign, existing)
						}
					}
					memberAssignments[memberID] = newAssign
				}
			}
		}

		// Find unassigned partitions for this topic
		unassignedPartitions := make([]TopicPartition, 0)
		for p := int32(0); p < numPartitions; p++ {
			key := partitionKey(TopicPartition{Topic: topic, Partition: p})
			if _, assigned := assignedPartitions[key]; !assigned {
				unassignedPartitions = append(unassignedPartitions, TopicPartition{Topic: topic, Partition: p})
			}
		}

		// Sort for deterministic assignment
		sort.Slice(unassignedPartitions, func(i, j int) bool {
			return unassignedPartitions[i].Partition < unassignedPartitions[j].Partition
		})

		// Assign unassigned partitions to under-assigned members
		for _, tp := range unassignedPartitions {
			// Find member with lowest count that's still under target
			var targetMember string
			minCount := int32(-1)

			for _, memberID := range subscribers {
				count := int32(len(topicAssignments[memberID]))
				target := targets[memberID]

				if count < target && (minCount < 0 || count < minCount) {
					minCount = count
					targetMember = memberID
				}
			}

			if targetMember != "" {
				topicAssignments[targetMember] = append(topicAssignments[targetMember], tp)
				assignedPartitions[partitionKey(tp)] = targetMember
				memberAssignments[targetMember] = append(memberAssignments[targetMember], tp)
			}
		}
	}

	// Build final result from memberAssignments
	for memberID, partitions := range memberAssignments {
		result[memberID] = partitions
	}

	return result, nil
}

// parseCurrentAssignments extracts current assignments from member user data.
// For sticky behavior, members encode their current assignment in the subscription userData.
// If no user data is provided, returns empty assignments.
func (u *UniformAssignor) parseCurrentAssignments(members map[string]*Subscription) map[string][]TopicPartition {
	assignments := make(map[string][]TopicPartition)

	for memberID, sub := range members {
		if len(sub.UserData) == 0 {
			continue
		}

		// Try to parse as assignment data (same format as EncodeAssignment output)
		partitions, err := decodeAssignmentData(sub.UserData)
		if err == nil && len(partitions) > 0 {
			assignments[memberID] = partitions
		}
	}

	return assignments
}

// decodeAssignmentData decodes assignment data from user data bytes.
// Format is the same as EncodeAssignment output.
func decodeAssignmentData(data []byte) ([]TopicPartition, error) {
	if len(data) < 6 {
		return nil, errors.New("data too short")
	}

	offset := 0

	// Version
	offset += 2

	// Topics array length
	if offset+4 > len(data) {
		return nil, errors.New("truncated at topics length")
	}
	topicsLen := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if topicsLen < 0 {
		return nil, errors.New("invalid topics length")
	}

	var result []TopicPartition
	for i := int32(0); i < topicsLen; i++ {
		// Topic name length
		if offset+2 > len(data) {
			return nil, errors.New("truncated at topic name length")
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		// Topic name
		if offset+topicLen > len(data) {
			return nil, errors.New("truncated at topic name")
		}
		topic := string(data[offset : offset+topicLen])
		offset += topicLen

		// Partitions array length
		if offset+4 > len(data) {
			return nil, errors.New("truncated at partitions length")
		}
		partitionsLen := int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		for j := int32(0); j < partitionsLen; j++ {
			if offset+4 > len(data) {
				return nil, errors.New("truncated at partition id")
			}
			partition := int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			result = append(result, TopicPartition{Topic: topic, Partition: partition})
		}
	}

	return result, nil
}

// partitionKey returns a unique string key for a topic-partition pair.
func partitionKey(tp TopicPartition) string {
	return tp.Topic + ":" + strconv.Itoa(int(tp.Partition))
}

// NewUniformAssignor creates a new uniform assignor.
func NewUniformAssignor() Assignor {
	return &UniformAssignor{}
}
