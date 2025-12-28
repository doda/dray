package groups

import (
	"encoding/binary"
	"sort"
	"strconv"
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
			key := p.Topic + ":" + strconv.FormatInt(int64(p.Partition), 10)
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

// RoundRobinAssignor tests

func TestRoundRobinAssignor_Name(t *testing.T) {
	a := NewRoundRobinAssignor()
	assert.Equal(t, "roundrobin", a.Name())
}

func TestRoundRobinAssignor_Assign(t *testing.T) {
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
					{Topic: "topic1", Partition: 2},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 3},
				},
			},
		},
		{
			name: "two consumers uneven split",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 5},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 2},
					{Topic: "topic1", Partition: 4},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 3},
				},
			},
		},
		{
			name: "three consumers round-robin",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic1"}},
			},
			topics: map[string]int32{"topic1": 7},
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 3},
					{Topic: "topic1", Partition: 6},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 4},
				},
				"consumer-3": {
					{Topic: "topic1", Partition: 2},
					{Topic: "topic1", Partition: 5},
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
			name: "multiple topics - round robin across all partitions",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "topic2"}},
				"consumer-2": {Topics: []string{"topic1", "topic2"}},
			},
			topics: map[string]int32{"topic1": 3, "topic2": 3},
			// All 6 partitions assigned round-robin: t1-0, t1-1, t1-2, t2-0, t2-1, t2-2
			// consumer-1 gets: t1-0, t1-2, t2-1
			// consumer-2 gets: t1-1, t2-0, t2-2
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 2},
					{Topic: "topic2", Partition: 1},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 1},
					{Topic: "topic2", Partition: 0},
					{Topic: "topic2", Partition: 2},
				},
			},
		},
		{
			name: "partial subscription - different topics per consumer",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "topic2"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic2"}},
			},
			topics: map[string]int32{"topic1": 4, "topic2": 4},
			// True cursor-based round-robin (cursor starts at 0):
			// t1-0: cursor=0, c1 subscribed -> c1, cursor=1
			// t1-1: cursor=1, c2 subscribed -> c2, cursor=2
			// t1-2: cursor=2, c3 NOT subscribed, c1 subscribed -> c1, cursor=1
			// t1-3: cursor=1, c2 subscribed -> c2, cursor=2
			// t2-0: cursor=2, c3 subscribed -> c3, cursor=0
			// t2-1: cursor=0, c1 subscribed -> c1, cursor=1
			// t2-2: cursor=1, c2 NOT subscribed, c3 subscribed -> c3, cursor=0
			// t2-3: cursor=0, c1 subscribed -> c1, cursor=1
			want: map[string][]TopicPartition{
				"consumer-1": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 2},
					{Topic: "topic2", Partition: 1},
					{Topic: "topic2", Partition: 3},
				},
				"consumer-2": {
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 3},
				},
				"consumer-3": {
					{Topic: "topic2", Partition: 0},
					{Topic: "topic2", Partition: 2},
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
			// Sorted order: a-consumer, m-consumer, z-consumer
			// Round-robin: p0->a, p1->m, p2->z, p3->a, p4->m, p5->z
			want: map[string][]TopicPartition{
				"a-consumer": {
					{Topic: "topic1", Partition: 0},
					{Topic: "topic1", Partition: 3},
				},
				"m-consumer": {
					{Topic: "topic1", Partition: 1},
					{Topic: "topic1", Partition: 4},
				},
				"z-consumer": {
					{Topic: "topic1", Partition: 2},
					{Topic: "topic1", Partition: 5},
				},
			},
		},
	}

	assignor := NewRoundRobinAssignor()

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

func TestRoundRobinAssignor_Determinism(t *testing.T) {
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 5, "topic2": 3}

	assignor := NewRoundRobinAssignor()

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

func TestRoundRobinAssignor_NoGaps(t *testing.T) {
	// Verify all partitions are assigned
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 10}

	assignor := NewRoundRobinAssignor()
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

func TestRoundRobinAssignor_NoOverlap(t *testing.T) {
	// Verify no partition is assigned to multiple consumers
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 6, "topic2": 4}

	assignor := NewRoundRobinAssignor()
	result, err := assignor.Assign(members, topics)
	require.NoError(t, err)

	// Track which consumer got each partition
	assignments := make(map[string]string) // "topic:partition" -> memberID
	for memberID, partitions := range result {
		for _, p := range partitions {
			key := p.Topic + ":" + strconv.FormatInt(int64(p.Partition), 10)
			existing, ok := assignments[key]
			assert.False(t, ok, "partition %s assigned to both %s and %s", key, existing, memberID)
			assignments[key] = memberID
		}
	}
}

func TestRoundRobinAssignor_EvenDistribution(t *testing.T) {
	// Verify that partitions are evenly distributed
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 12}

	assignor := NewRoundRobinAssignor()
	result, err := assignor.Assign(members, topics)
	require.NoError(t, err)

	// Each consumer should get exactly 4 partitions
	for memberID, partitions := range result {
		assert.Equal(t, 4, len(partitions), "consumer %s should have 4 partitions", memberID)
	}
}

func TestRoundRobinAssignor_DifferenceFromRange(t *testing.T) {
	// Demonstrate the difference between range and round-robin assignment
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
	}
	topics := map[string]int32{"topic1": 2, "topic2": 2}

	rangeAssignor := NewRangeAssignor()
	roundRobinAssignor := NewRoundRobinAssignor()

	rangeResult, err := rangeAssignor.Assign(members, topics)
	require.NoError(t, err)

	roundRobinResult, err := roundRobinAssignor.Assign(members, topics)
	require.NoError(t, err)

	// Range assignor: consumer-1 gets topic1-0, topic2-0; consumer-2 gets topic1-1, topic2-1
	// Round-robin: consumer-1 gets topic1-0, topic1-2/topic2-1; consumer-2 gets topic1-1, topic2-0/topic2-2
	// Both should assign all partitions but in different patterns

	// Verify total partitions assigned is the same
	rangeTotal := 0
	roundRobinTotal := 0
	for _, partitions := range rangeResult {
		rangeTotal += len(partitions)
	}
	for _, partitions := range roundRobinResult {
		roundRobinTotal += len(partitions)
	}
	assert.Equal(t, 4, rangeTotal)
	assert.Equal(t, 4, roundRobinTotal)
}

// UniformAssignor tests

func TestUniformAssignor_Name(t *testing.T) {
	a := NewUniformAssignor()
	assert.Equal(t, "uniform", a.Name())
}

func TestUniformAssignor_Assign(t *testing.T) {
	tests := []struct {
		name              string
		members           map[string]*Subscription
		topics            map[string]int32
		wantCounts        map[string]int // expected partition counts per member
		wantTotal         int            // total partitions expected
	}{
		{
			name:    "empty members",
			members: nil,
			topics:  map[string]int32{"topic1": 3},
		},
		{
			name: "single consumer single topic",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
			},
			topics:     map[string]int32{"topic1": 3},
			wantCounts: map[string]int{"consumer-1": 3},
			wantTotal:  3,
		},
		{
			name: "two consumers even split",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
			},
			topics:     map[string]int32{"topic1": 4},
			wantCounts: map[string]int{"consumer-1": 2, "consumer-2": 2},
			wantTotal:  4,
		},
		{
			name: "two consumers uneven split",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
			},
			topics:     map[string]int32{"topic1": 5},
			wantCounts: map[string]int{"consumer-1": 3, "consumer-2": 2},
			wantTotal:  5,
		},
		{
			name: "three consumers uneven split",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic1"}},
			},
			topics:     map[string]int32{"topic1": 7},
			wantCounts: map[string]int{"consumer-1": 3, "consumer-2": 2, "consumer-3": 2},
			wantTotal:  7,
		},
		{
			name: "more consumers than partitions",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic1"}},
				"consumer-4": {Topics: []string{"topic1"}},
			},
			topics:     map[string]int32{"topic1": 2},
			wantCounts: map[string]int{"consumer-1": 1, "consumer-2": 1, "consumer-3": 0, "consumer-4": 0},
			wantTotal:  2,
		},
		{
			name: "multiple topics",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "topic2"}},
				"consumer-2": {Topics: []string{"topic1", "topic2"}},
			},
			topics:     map[string]int32{"topic1": 3, "topic2": 2},
			wantCounts: map[string]int{"consumer-1": 3, "consumer-2": 2},
			wantTotal:  5,
		},
		{
			name: "multiple topics evenly distributed",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "topic2"}},
				"consumer-2": {Topics: []string{"topic1", "topic2"}},
			},
			topics:     map[string]int32{"topic1": 1, "topic2": 1},
			wantCounts: map[string]int{"consumer-1": 1, "consumer-2": 1},
			wantTotal:  2,
		},
		{
			name: "partial subscription",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "topic2"}},
				"consumer-2": {Topics: []string{"topic1"}},
				"consumer-3": {Topics: []string{"topic2"}},
			},
			topics:     map[string]int32{"topic1": 4, "topic2": 4},
			wantCounts: map[string]int{"consumer-1": 4, "consumer-2": 2, "consumer-3": 2},
			wantTotal:  8,
		},
		{
			name: "consumer subscribed to non-existent topic",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1", "nonexistent"}},
			},
			topics:     map[string]int32{"topic1": 2},
			wantCounts: map[string]int{"consumer-1": 2},
			wantTotal:  2,
		},
		{
			name: "topic with no subscribers",
			members: map[string]*Subscription{
				"consumer-1": {Topics: []string{"topic1"}},
			},
			topics:     map[string]int32{"topic1": 2, "topic2": 2},
			wantCounts: map[string]int{"consumer-1": 2},
			wantTotal:  2,
		},
	}

	assignor := NewUniformAssignor()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := assignor.Assign(tt.members, tt.topics)
			require.NoError(t, err)

			if tt.wantCounts == nil {
				assert.Nil(t, got)
				return
			}

			// Verify partition counts
			total := 0
			for memberID, wantCount := range tt.wantCounts {
				gotCount := len(got[memberID])
				assert.Equal(t, wantCount, gotCount, "partition count for %s", memberID)
				total += gotCount
			}

			// Verify total partitions (only subscribed topics are assigned)
			assert.Equal(t, tt.wantTotal, total, "total partitions")

			// Verify no overlapping assignments
			assigned := make(map[string]string) // "topic:partition" -> memberID
			for memberID, partitions := range got {
				for _, p := range partitions {
					key := p.Topic + ":" + strconv.FormatInt(int64(p.Partition), 10)
					existing, ok := assigned[key]
					assert.False(t, ok, "partition %s assigned to both %s and %s", key, existing, memberID)
					assigned[key] = memberID
				}
			}
		})
	}
}

func TestUniformAssignor_Determinism(t *testing.T) {
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 5, "topic2": 3}

	assignor := NewUniformAssignor()

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

func TestUniformAssignor_NoGaps(t *testing.T) {
	// Verify all partitions are assigned
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 10}

	assignor := NewUniformAssignor()
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

func TestUniformAssignor_NoOverlap(t *testing.T) {
	// Verify no partition is assigned to multiple consumers
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 6, "topic2": 4}

	assignor := NewUniformAssignor()
	result, err := assignor.Assign(members, topics)
	require.NoError(t, err)

	// Track which consumer got each partition
	assignments := make(map[string]string) // "topic:partition" -> memberID
	for memberID, partitions := range result {
		for _, p := range partitions {
			key := p.Topic + ":" + strconv.FormatInt(int64(p.Partition), 10)
			existing, ok := assignments[key]
			assert.False(t, ok, "partition %s assigned to both %s and %s", key, existing, memberID)
			assignments[key] = memberID
		}
	}
}

func TestUniformAssignor_EvenDistribution(t *testing.T) {
	// Verify that partitions are evenly distributed
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}
	topics := map[string]int32{"topic1": 12}

	assignor := NewUniformAssignor()
	result, err := assignor.Assign(members, topics)
	require.NoError(t, err)

	// Each consumer should get exactly 4 partitions
	for memberID, partitions := range result {
		assert.Equal(t, 4, len(partitions), "consumer %s should have 4 partitions", memberID)
	}
}

func TestUniformAssignor_StickyBehavior(t *testing.T) {
	// Test that existing assignments are preserved when possible
	assignor := NewUniformAssignor()
	topics := map[string]int32{"topic1": 6}

	// Initial assignment with 2 consumers
	initialMembers := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
	}

	initial, err := assignor.Assign(initialMembers, topics)
	require.NoError(t, err)

	// Encode the assignments as user data for sticky behavior
	consumer1UserData := EncodeAssignment(initial["consumer-1"])
	consumer2UserData := EncodeAssignment(initial["consumer-2"])

	// Add a third consumer with existing assignments in user data
	newMembers := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}, UserData: consumer1UserData},
		"consumer-2": {Topics: []string{"topic1"}, UserData: consumer2UserData},
		"consumer-3": {Topics: []string{"topic1"}},
	}

	rebalanced, err := assignor.Assign(newMembers, topics)
	require.NoError(t, err)

	// Verify all 6 partitions are still assigned
	totalPartitions := 0
	for _, partitions := range rebalanced {
		totalPartitions += len(partitions)
	}
	assert.Equal(t, 6, totalPartitions, "all 6 partitions should be assigned")

	// Verify distribution is roughly even (2 partitions each)
	for memberID, partitions := range rebalanced {
		assert.Equal(t, 2, len(partitions), "consumer %s should have 2 partitions", memberID)
	}

	// Count how many partitions moved from consumer-1 and consumer-2
	moved := 0
	for _, tp := range initial["consumer-1"] {
		found := false
		for _, newTP := range rebalanced["consumer-1"] {
			if tp.Topic == newTP.Topic && tp.Partition == newTP.Partition {
				found = true
				break
			}
		}
		if !found {
			moved++
		}
	}
	for _, tp := range initial["consumer-2"] {
		found := false
		for _, newTP := range rebalanced["consumer-2"] {
			if tp.Topic == newTP.Topic && tp.Partition == newTP.Partition {
				found = true
				break
			}
		}
		if !found {
			moved++
		}
	}

	// With sticky behavior, we should minimize movement
	// Each original consumer had 3 partitions, now each has 2
	// So at most 2 partitions should have moved (1 from each)
	assert.LessOrEqual(t, moved, 2, "at most 2 partitions should have moved")
}

func TestUniformAssignor_StickyBehavior_RemoveMember(t *testing.T) {
	// Test that assignments are preserved when a member is removed
	assignor := NewUniformAssignor()
	topics := map[string]int32{"topic1": 6}

	// Initial assignment with 3 consumers
	initialMembers := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
		"consumer-3": {Topics: []string{"topic1"}},
	}

	initial, err := assignor.Assign(initialMembers, topics)
	require.NoError(t, err)

	// Each should have 2 partitions
	for memberID, partitions := range initial {
		assert.Equal(t, 2, len(partitions), "consumer %s should have 2 partitions initially", memberID)
	}

	// Encode assignments as user data
	consumer1UserData := EncodeAssignment(initial["consumer-1"])
	consumer2UserData := EncodeAssignment(initial["consumer-2"])

	// Remove consumer-3
	newMembers := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}, UserData: consumer1UserData},
		"consumer-2": {Topics: []string{"topic1"}, UserData: consumer2UserData},
	}

	rebalanced, err := assignor.Assign(newMembers, topics)
	require.NoError(t, err)

	// Verify all 6 partitions are still assigned (3 each now)
	for memberID, partitions := range rebalanced {
		assert.Equal(t, 3, len(partitions), "consumer %s should have 3 partitions", memberID)
	}

	// Count how many of consumer-1's original partitions it still has
	retained := 0
	for _, tp := range initial["consumer-1"] {
		for _, newTP := range rebalanced["consumer-1"] {
			if tp.Topic == newTP.Topic && tp.Partition == newTP.Partition {
				retained++
				break
			}
		}
	}
	assert.Equal(t, 2, retained, "consumer-1 should retain all its original 2 partitions")

	retained = 0
	for _, tp := range initial["consumer-2"] {
		for _, newTP := range rebalanced["consumer-2"] {
			if tp.Topic == newTP.Topic && tp.Partition == newTP.Partition {
				retained++
				break
			}
		}
	}
	assert.Equal(t, 2, retained, "consumer-2 should retain all its original 2 partitions")
}

func TestUniformAssignor_StickyBehavior_SubscriptionChange(t *testing.T) {
	// Test that assignments change when subscription changes
	assignor := NewUniformAssignor()
	topics := map[string]int32{"topic1": 4, "topic2": 4}

	// Initial assignment - consumer-1 subscribed to both topics
	initialMembers := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2"}},
		"consumer-2": {Topics: []string{"topic1", "topic2"}},
	}

	initial, err := assignor.Assign(initialMembers, topics)
	require.NoError(t, err)

	// Encode assignments
	consumer1UserData := EncodeAssignment(initial["consumer-1"])
	consumer2UserData := EncodeAssignment(initial["consumer-2"])

	// Now consumer-1 unsubscribes from topic2
	newMembers := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}, UserData: consumer1UserData},
		"consumer-2": {Topics: []string{"topic1", "topic2"}, UserData: consumer2UserData},
	}

	rebalanced, err := assignor.Assign(newMembers, topics)
	require.NoError(t, err)

	// Verify consumer-1 has no topic2 partitions
	for _, tp := range rebalanced["consumer-1"] {
		assert.NotEqual(t, "topic2", tp.Topic, "consumer-1 should not have topic2 partitions")
	}

	// Verify consumer-2 has all topic2 partitions
	topic2Count := 0
	for _, tp := range rebalanced["consumer-2"] {
		if tp.Topic == "topic2" {
			topic2Count++
		}
	}
	assert.Equal(t, 4, topic2Count, "consumer-2 should have all 4 topic2 partitions")
}

func TestUniformAssignor_BalancesAcrossTopics(t *testing.T) {
	// Verify that uniform assignor balances per-topic
	// When both consumers subscribe to all topics, each topic is balanced independently
	members := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1", "topic2", "topic3"}},
		"consumer-2": {Topics: []string{"topic1", "topic2", "topic3"}},
	}
	topics := map[string]int32{"topic1": 3, "topic2": 3, "topic3": 3}

	assignor := NewUniformAssignor()
	result, err := assignor.Assign(members, topics)
	require.NoError(t, err)

	// Total of 9 partitions should all be assigned
	c1Count := len(result["consumer-1"])
	c2Count := len(result["consumer-2"])
	assert.Equal(t, 9, c1Count+c2Count, "all 9 partitions should be assigned")

	// For each topic individually, the balance should be within 1
	// Since each topic has 3 partitions and 2 subscribers:
	// - Each consumer gets 1-2 partitions per topic
	for _, topic := range []string{"topic1", "topic2", "topic3"} {
		c1TopicCount := 0
		c2TopicCount := 0
		for _, tp := range result["consumer-1"] {
			if tp.Topic == topic {
				c1TopicCount++
			}
		}
		for _, tp := range result["consumer-2"] {
			if tp.Topic == topic {
				c2TopicCount++
			}
		}

		assert.Equal(t, 3, c1TopicCount+c2TopicCount, "topic %s: all 3 partitions should be assigned", topic)
		diff := c1TopicCount - c2TopicCount
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, 1, "topic %s: partition count difference should be at most 1", topic)
	}
}

func TestDecodeAssignmentData(t *testing.T) {
	tests := []struct {
		name       string
		partitions []TopicPartition
	}{
		{
			name:       "empty assignment",
			partitions: []TopicPartition{},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode and decode
			encoded := EncodeAssignment(tt.partitions)
			decoded, err := decodeAssignmentData(encoded)
			require.NoError(t, err)

			// Sort both for comparison
			sortPartitions(tt.partitions)
			sortPartitions(decoded)

			if len(tt.partitions) == 0 && len(decoded) == 0 {
				return
			}
			assert.Equal(t, tt.partitions, decoded)
		})
	}
}

func TestUniformAssignor_MinimizesMovement(t *testing.T) {
	// Comprehensive test that verifies minimal partition movement
	assignor := NewUniformAssignor()
	topics := map[string]int32{"topic1": 12}

	// Start with 2 consumers
	members2 := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}},
		"consumer-2": {Topics: []string{"topic1"}},
	}

	result2, err := assignor.Assign(members2, topics)
	require.NoError(t, err)

	// Each should have 6
	assert.Equal(t, 6, len(result2["consumer-1"]))
	assert.Equal(t, 6, len(result2["consumer-2"]))

	// Add consumer-3
	members3 := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}, UserData: EncodeAssignment(result2["consumer-1"])},
		"consumer-2": {Topics: []string{"topic1"}, UserData: EncodeAssignment(result2["consumer-2"])},
		"consumer-3": {Topics: []string{"topic1"}},
	}

	result3, err := assignor.Assign(members3, topics)
	require.NoError(t, err)

	// Each should have 4
	assert.Equal(t, 4, len(result3["consumer-1"]))
	assert.Equal(t, 4, len(result3["consumer-2"]))
	assert.Equal(t, 4, len(result3["consumer-3"]))

	// Count retained partitions
	consumer1Retained := countRetained(result2["consumer-1"], result3["consumer-1"])
	consumer2Retained := countRetained(result2["consumer-2"], result3["consumer-2"])

	// Each consumer should retain 4 of their original 6 partitions
	assert.Equal(t, 4, consumer1Retained, "consumer-1 should retain 4 partitions")
	assert.Equal(t, 4, consumer2Retained, "consumer-2 should retain 4 partitions")

	// Add consumer-4
	members4 := map[string]*Subscription{
		"consumer-1": {Topics: []string{"topic1"}, UserData: EncodeAssignment(result3["consumer-1"])},
		"consumer-2": {Topics: []string{"topic1"}, UserData: EncodeAssignment(result3["consumer-2"])},
		"consumer-3": {Topics: []string{"topic1"}, UserData: EncodeAssignment(result3["consumer-3"])},
		"consumer-4": {Topics: []string{"topic1"}},
	}

	result4, err := assignor.Assign(members4, topics)
	require.NoError(t, err)

	// Each should have 3
	assert.Equal(t, 3, len(result4["consumer-1"]))
	assert.Equal(t, 3, len(result4["consumer-2"]))
	assert.Equal(t, 3, len(result4["consumer-3"]))
	assert.Equal(t, 3, len(result4["consumer-4"]))

	// Verify all 12 partitions are assigned
	allAssigned := make(map[string]bool)
	for _, partitions := range result4 {
		for _, p := range partitions {
			key := p.Topic + ":" + strconv.FormatInt(int64(p.Partition), 10)
			assert.False(t, allAssigned[key], "partition %s assigned twice", key)
			allAssigned[key] = true
		}
	}
	assert.Equal(t, 12, len(allAssigned), "all 12 partitions should be assigned")
}

func countRetained(old, new []TopicPartition) int {
	count := 0
	for _, o := range old {
		for _, n := range new {
			if o.Topic == n.Topic && o.Partition == n.Partition {
				count++
				break
			}
		}
	}
	return count
}
