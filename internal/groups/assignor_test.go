package groups

import (
	"encoding/binary"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSubscription(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    *Subscription
		wantErr bool
	}{
		{
			name:    "empty data",
			data:    nil,
			wantErr: true,
		},
		{
			name:    "too short",
			data:    []byte{0, 0, 0},
			wantErr: true,
		},
		{
			name: "single topic",
			data: buildSubscriptionMetadata([]string{"topic1"}, nil),
			want: &Subscription{
				Topics: []string{"topic1"},
			},
		},
		{
			name: "multiple topics",
			data: buildSubscriptionMetadata([]string{"topic1", "topic2", "topic3"}, nil),
			want: &Subscription{
				Topics: []string{"topic1", "topic2", "topic3"},
			},
		},
		{
			name: "with user data",
			data: buildSubscriptionMetadata([]string{"topic1"}, []byte("user-data")),
			want: &Subscription{
				Topics:   []string{"topic1"},
				UserData: []byte("user-data"),
			},
		},
		{
			name: "no topics",
			data: buildSubscriptionMetadata([]string{}, nil),
			want: &Subscription{
				Topics: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSubscription(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want.Topics, got.Topics)
			assert.Equal(t, tt.want.UserData, got.UserData)
		})
	}
}

func TestEncodeAssignment(t *testing.T) {
	tests := []struct {
		name       string
		partitions []TopicPartition
	}{
		{
			name:       "empty assignment",
			partitions: nil,
		},
		{
			name: "single partition",
			partitions: []TopicPartition{
				{Topic: "topic1", Partition: 0},
			},
		},
		{
			name: "multiple partitions same topic",
			partitions: []TopicPartition{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic1", Partition: 1},
				{Topic: "topic1", Partition: 2},
			},
		},
		{
			name: "multiple topics",
			partitions: []TopicPartition{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic2", Partition: 0},
				{Topic: "topic2", Partition: 1},
			},
		},
		{
			name: "unsorted input gets sorted",
			partitions: []TopicPartition{
				{Topic: "topic2", Partition: 1},
				{Topic: "topic1", Partition: 0},
				{Topic: "topic2", Partition: 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeAssignment(tt.partitions)

			// Verify we can decode the result
			decoded, err := decodeAssignment(encoded)
			require.NoError(t, err)

			// Compare sorted results
			expected := make([]TopicPartition, len(tt.partitions))
			copy(expected, tt.partitions)
			sortPartitions(expected)
			sortPartitions(decoded)

			// Handle empty vs nil slice comparison
			if len(expected) == 0 && len(decoded) == 0 {
				return
			}
			assert.Equal(t, expected, decoded)
		})
	}
}

func TestRangeAssignor_Name(t *testing.T) {
	a := NewRangeAssignor()
	assert.Equal(t, "range", a.Name())
}

func TestRangeAssignor_Assign(t *testing.T) {
	tests := []struct {
		name    string
		members map[string]*Subscription
		topics  map[string]int32
		want    map[string][]TopicPartition
	}{
		{
			name:    "empty members",
			members: nil,
			topics:  map[string]int32{"topic1": 3},
			want:    nil,
		},
		{
			name: "single consumer single topic",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 3},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 2},
				},
			},
		},
		{
			name: "two consumers even split",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 4},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 2},
					{Topic: "topic1", Partition: 3},
				},
			},
		},
		{
			name: "two consumers uneven split - remainder partitions",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 5},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 2},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 3},
					{Topic: "topic1", Partition: 4},
				},
			},
		},
		{
			name: "three consumers uneven split",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 7},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 2},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 3},
					{Topic: "topic1", Partition: 4},
				},
				"consumer-3": {
					{Topic: "topic1", Partition: 5},
					{Topic: "topic1", Partition: 6},
				},
			},
		},
		{
			name: "more consumers than partitions",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic1"}},
				"consumer-4": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 2},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 1},
				},
				"consumer-3": nil,
				"consumer-4": nil,
			},
		},
		{
			name: "multiple topics",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "topic2"}},
				"consumer-2": {Topics: []string{"topic1", "topic2"}},
			},
			topics: map[string]int32{"topic1": 3, "topic2": 2},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
					{Topic: "topic2", Partition: 0},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 2},
					{Topic: "topic2", Partition: 1},
				},
			},
		},
		{
			name: "partial subscription",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "topic2"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic2"}},
			},
			topics: map[string]int32{"topic1": 4, "topic2": 4},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
					{Topic: "topic2", Partition: 0},
					{Topic: "topic2", Partition: 1},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 2},
					{Topic: "topic1", Partition: 3},
				},
				"consumer-3": {
					{Topic: "topic2", Partition: 2},
					{Topic: "topic2", Partition: 3},
				},
			},
		},
		{
			name: "consumer subscribed to non-existent topic",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "nonexistent"}},
			},
			topics: map[string]int32{"topic1": 2},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
				},
			},
		},
		{
			name: "topic with no subscribers",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 2, "topic2": 2},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
				},
			},
		},
		{
			name: "consumer ID ordering is deterministic",
			members: map[string]*Subscription{
				"z-consumer": {Topics: []string{"topic1"}},
				"a-consumer": {Topics: []string{"topic1"}},
				"m-consumer": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 6},
			want: map[string][]TopicPartition{
				"a-consumer": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 1},
				},
				"m-consumer": {
					{Topic: "topic1", Partition: 2},
					{Topic: "topic1", Partition: 3},
				},
				"z-consumer": {
					{Topic: "topic1", Partition: 4},
					{Topic: "topic1", Partition: 5},
				},
			},
		},
	}

	assignor := NewRangeAssignor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := assignor.Assign(tt.members, tt.topics)
			require.NoError(t, err)

			if tt.want == nil {
				assert.Nil(t, got)
				return
			}

			assert.Equal(t, len(tt.want), len(got), "number of members should match")

			for memberID, wantPartitions := range tt.want {
				gotPartitions := got[memberID]
				assert.ElementsMatch(t, wantPartitions, gotPartitions, "partitions for %s", memberID)
			}
		})
	}
}

func TestRangeAssignor_Determinism(t *testing.T) {
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 5, "topic2": 3}

	assignor := NewRangeAssignor()

	// Run multiple times and verify results are identical
	var firstResult map[string][]TopicPartition
	for i := 0; i < 10; i++ {
		result, err := assignor.Assign(members, topics)
		require.NoError(t, err)

		if firstResult == nil {
			firstResult = result
		} else {
			for memberID := range firstResult {
				assert.ElementsMatch(t, firstResult[memberID], result[memberID],
					"iteration %d: assignment for %s should match", i, memberID)
			}
		}
	}
}

func TestRangeAssignor_NoGaps(t *testing.T) {
	// Verify all partitions are assigned
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 10}

	assignor := NewRangeAssignor()
	result, err := assignor.Assign(members, topics)
	require.NoError(t, err)

	// Collect all assigned partitions
	assigned := make(map[int32]bool)
	for _, partitions := range result {
		for _, p := range partitions {
			assert.False(t, assigned[p.Partition], "partition %d assigned twice", p.Partition)
			assigned[p.Partition] = true
		}
	}

	// Verify all partitions were assigned
	for i := int32(0); i < 10; i++ {
		assert.True(t, assigned[i], "partition %d not assigned", i)
	}
}

func TestRangeAssignor_NoOverlap(t *testing.T) {
	// Verify no partition is assigned to multiple consumers
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 6, "topic2": 4}

	assignor := NewRangeAssignor()
	result, err := assignor.Assign(members, topics)
	require.NoError(t, err)

	// Track which consumer got each partition
	assignments := make(map[string]string) // "topic:partition" -> memberID
	for memberID, partitions := range result {
		for _, p := range partitions {
			key := p.Topic + ":" + string(rune(p.Partition+'0'))
			existing, ok := assignments[key]
			assert.False(t, ok, "partition %s assigned to both %s and %s", key, existing, memberID)
			assignments[key] = memberID
		}
	}
}

// Helper functions

func buildSubscriptionMetadata(topics []string, userData []byte) []byte {
	// Calculate size
	size := 2 + 4 // version + topics array length
	for _, topic := range topics {
		size += 2 + len(topic) // topic name length + topic name
	}
	size += 4 + len(userData) // user data length + user data

	buf := make([]byte, size)
	offset := 0

	// Version
	binary.BigEndian.PutUint16(buf[offset:], 0)
	offset += 2

	// Topics array length
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(topics)))
	offset += 4

	for _, topic := range topics {
		// Topic name length
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(topic)))
		offset += 2
		// Topic name
		copy(buf[offset:], topic)
		offset += len(topic)
	}

	// UserData length
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(userData)))
	offset += 4
	// UserData
	copy(buf[offset:], userData)

	return buf
}

func decodeAssignment(data []byte) ([]TopicPartition, error) {
	if len(data) < 6 {
		return nil, nil
	}

	offset := 0

	// Version
	offset += 2

	// Topics array length
	topicsLen := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	var result []TopicPartition
	for i := int32(0); i < topicsLen; i++ {
		// Topic name length
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		// Topic name
		topic := string(data[offset : offset+topicLen])
		offset += topicLen

		// Partitions array length
		partitionsLen := int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		for j := int32(0); j < partitionsLen; j++ {
			partition := int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			result = append(result, TopicPartition{Topic: topic, Partition: partition})
		}
	}

	return result, nil
}

func sortPartitions(partitions []TopicPartition) {
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
}
