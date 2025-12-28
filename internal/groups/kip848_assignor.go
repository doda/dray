package groups

import (
	"context"
	"encoding/binary"
	"sort"

	"github.com/dray-io/dray/internal/topics"
	"github.com/google/uuid"
)

// KIP848Assignor is the interface for server-side partition assignment in KIP-848.
// Unlike the classic Assignor interface, this uses SubscribedTopics from GroupMember
// and returns assignments with topic IDs.
type KIP848Assignor interface {
	// Name returns the assignor name.
	Name() string

	// Assign performs server-side assignment for KIP-848 consumer groups.
	// members are the group members with their subscribed topics.
	// topicStore is used to resolve topic metadata and partition counts.
	// Returns memberID -> list of (topicID, partitions) assignments.
	Assign(ctx context.Context, members []GroupMember, topicStore *topics.Store) (map[string][]KIP848Assignment, error)
}

// KIP848Assignment represents a topic assignment for KIP-848.
type KIP848Assignment struct {
	TopicID    uuid.UUID
	TopicName  string
	Partitions []int32
}

// KIP848RangeAssignor implements the range partition assignment strategy for KIP-848.
// For each topic, it sorts consumers by member ID and assigns partitions
// in contiguous ranges. The first few consumers get ceil(N/M) partitions while the
// remaining get floor(N/M), where N is the number of partitions and M
// is the number of consumers subscribed to that topic.
type KIP848RangeAssignor struct{}

// Name returns the assignor name.
func (r *KIP848RangeAssignor) Name() string {
	return "range"
}

// Assign implements the range assignment strategy for KIP-848.
func (r *KIP848RangeAssignor) Assign(ctx context.Context, members []GroupMember, topicStore *topics.Store) (map[string][]KIP848Assignment, error) {
	if len(members) == 0 {
		return nil, nil
	}

	// Initialize result for all members
	result := make(map[string][]KIP848Assignment)
	for _, m := range members {
		result[m.MemberID] = nil
	}

	// Collect all subscribed topics
	subscribedTopics := make(map[string]bool)
	for _, m := range members {
		for _, topic := range m.SubscribedTopics {
			subscribedTopics[topic] = true
		}
	}

	// Resolve topic metadata
	topicMeta := make(map[string]*topics.TopicMeta)
	for topicName := range subscribedTopics {
		meta, err := topicStore.GetTopic(ctx, topicName)
		if err != nil {
			// Skip topics that don't exist
			continue
		}
		topicMeta[topicName] = meta
	}

	// Sort topic names for deterministic assignment
	topicNames := make([]string, 0, len(topicMeta))
	for name := range topicMeta {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	// Build subscription map: memberID -> set of subscribed topics
	memberSubscriptions := make(map[string]map[string]bool)
	for _, m := range members {
		memberSubscriptions[m.MemberID] = make(map[string]bool)
		for _, topic := range m.SubscribedTopics {
			memberSubscriptions[m.MemberID][topic] = true
		}
	}

	// For each topic, find subscribed consumers and assign partitions
	for _, topicName := range topicNames {
		meta := topicMeta[topicName]
		numPartitions := meta.PartitionCount
		if numPartitions <= 0 {
			continue
		}

		// Parse topic ID
		topicID, err := uuid.Parse(meta.TopicID)
		if err != nil {
			// Generate from topic name if parse fails
			topicID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(topicName))
		}

		// Collect consumers subscribed to this topic, sorted by member ID
		var consumers []string
		for _, m := range members {
			if memberSubscriptions[m.MemberID][topicName] {
				consumers = append(consumers, m.MemberID)
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
		for i, consumerID := range consumers {
			// Determine how many partitions this consumer gets
			numToAssign := partitionsPerConsumer
			if int32(i) < consumersWithExtraPartition {
				numToAssign++
			}

			if numToAssign == 0 {
				continue
			}

			// Build the partition list for this consumer
			partitions := make([]int32, 0, numToAssign)
			for j := int32(0); j < numToAssign; j++ {
				partitions = append(partitions, currentPartition)
				currentPartition++
			}

			// Add assignment for this topic
			result[consumerID] = append(result[consumerID], KIP848Assignment{
				TopicID:    topicID,
				TopicName:  topicName,
				Partitions: partitions,
			})
		}
	}

	return result, nil
}

// NewKIP848RangeAssignor creates a new range assignor for KIP-848.
func NewKIP848RangeAssignor() KIP848Assignor {
	return &KIP848RangeAssignor{}
}

// KIP848UniformAssignor wraps the uniform assignor for KIP-848 server-side assignment.
// It provides the same balanced sticky behavior as the classic uniform assignor
// but uses the KIP-848 subscription model.
type KIP848UniformAssignor struct {
	uniform *UniformAssignor
}

// Name returns the assignor name.
func (u *KIP848UniformAssignor) Name() string {
	return "uniform"
}

// Assign implements the uniform assignment strategy for KIP-848.
func (u *KIP848UniformAssignor) Assign(ctx context.Context, members []GroupMember, topicStore *topics.Store) (map[string][]KIP848Assignment, error) {
	if len(members) == 0 {
		return nil, nil
	}

	// Initialize result for all members
	result := make(map[string][]KIP848Assignment)
	for _, m := range members {
		result[m.MemberID] = nil
	}

	// Collect all subscribed topics
	subscribedTopics := make(map[string]bool)
	for _, m := range members {
		for _, topic := range m.SubscribedTopics {
			subscribedTopics[topic] = true
		}
	}

	// Resolve topic metadata and build topics map
	topicMeta := make(map[string]*topics.TopicMeta)
	topicsMap := make(map[string]int32)
	for topicName := range subscribedTopics {
		meta, err := topicStore.GetTopic(ctx, topicName)
		if err != nil {
			continue
		}
		topicMeta[topicName] = meta
		topicsMap[topicName] = meta.PartitionCount
	}

	// Convert members to classic subscription format
	classicMembers := make(map[string]*Subscription)
	for _, m := range members {
		classicMembers[m.MemberID] = &Subscription{
			Topics: m.SubscribedTopics,
		}
	}

	// Use the classic uniform assignor
	classicResult, err := u.uniform.Assign(classicMembers, topicsMap)
	if err != nil {
		return nil, err
	}

	// Convert classic result to KIP-848 format
	for memberID, partitions := range classicResult {
		// Group partitions by topic
		byTopic := make(map[string][]int32)
		for _, tp := range partitions {
			byTopic[tp.Topic] = append(byTopic[tp.Topic], tp.Partition)
		}

		// Build KIP848Assignment list
		for topicName, parts := range byTopic {
			meta, ok := topicMeta[topicName]
			if !ok {
				continue
			}

			topicID, err := uuid.Parse(meta.TopicID)
			if err != nil {
				topicID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(topicName))
			}

			// Sort partitions for deterministic output
			sort.Slice(parts, func(i, j int) bool {
				return parts[i] < parts[j]
			})

			result[memberID] = append(result[memberID], KIP848Assignment{
				TopicID:    topicID,
				TopicName:  topicName,
				Partitions: parts,
			})
		}

		// Sort assignments by topic name for deterministic output
		sort.Slice(result[memberID], func(i, j int) bool {
			return result[memberID][i].TopicName < result[memberID][j].TopicName
		})
	}

	return result, nil
}

// NewKIP848UniformAssignor creates a new uniform assignor for KIP-848.
func NewKIP848UniformAssignor() KIP848Assignor {
	return &KIP848UniformAssignor{
		uniform: &UniformAssignor{},
	}
}

// GetKIP848Assignor returns the assignor for the given name.
// Returns nil if the assignor is not supported.
func GetKIP848Assignor(name string) KIP848Assignor {
	switch name {
	case "range":
		return NewKIP848RangeAssignor()
	case "uniform":
		return NewKIP848UniformAssignor()
	default:
		return nil
	}
}

// EncodeKIP848Assignment encodes a KIP-848 assignment to bytes.
// Format is designed for KIP-848 ConsumerGroupHeartbeat response.
func EncodeKIP848Assignment(assignments []KIP848Assignment) []byte {
	if len(assignments) == 0 {
		return nil
	}

	// Calculate size
	size := 4 // number of topics
	for _, a := range assignments {
		size += 16                     // topic ID (UUID)
		size += 4                      // number of partitions
		size += 4 * len(a.Partitions)  // partition IDs
	}

	buf := make([]byte, size)
	offset := 0

	// Number of topics
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(assignments)))
	offset += 4

	for _, a := range assignments {
		// Topic ID (UUID)
		copy(buf[offset:], a.TopicID[:])
		offset += 16

		// Number of partitions
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(a.Partitions)))
		offset += 4

		// Partition IDs
		for _, p := range a.Partitions {
			binary.BigEndian.PutUint32(buf[offset:], uint32(p))
			offset += 4
		}
	}

	return buf
}

// DecodeKIP848Assignment decodes a KIP-848 assignment from bytes.
func DecodeKIP848Assignment(data []byte) ([]KIP848Assignment, error) {
	if len(data) < 4 {
		return nil, nil
	}

	offset := 0

	// Number of topics
	numTopics := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	assignments := make([]KIP848Assignment, 0, numTopics)
	for i := 0; i < numTopics; i++ {
		if offset+16 > len(data) {
			break
		}

		// Topic ID
		var topicID uuid.UUID
		copy(topicID[:], data[offset:offset+16])
		offset += 16

		if offset+4 > len(data) {
			break
		}

		// Number of partitions
		numPartitions := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		partitions := make([]int32, 0, numPartitions)
		for j := 0; j < numPartitions; j++ {
			if offset+4 > len(data) {
				break
			}
			partitions = append(partitions, int32(binary.BigEndian.Uint32(data[offset:])))
			offset += 4
		}

		assignments = append(assignments, KIP848Assignment{
			TopicID:    topicID,
			Partitions: partitions,
		})
	}

	return assignments, nil
}
