package groups

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/topics"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTopicStore is a test helper that provides topic metadata.
type mockTopicStore struct {
	topics map[string]*topics.TopicMeta
}

func newMockTopicStore(topicDefs map[string]int32) *topics.Store {
	// Create a mock metadata store
	mock := metadata.NewMockStore()
	store := topics.NewStore(mock)

	ctx := context.Background()

	// Create topics with the specified partition counts
	for topicName, partitionCount := range topicDefs {
		_, _ = store.CreateTopic(ctx, topics.CreateTopicRequest{
			Name:           topicName,
			PartitionCount: partitionCount,
			NowMs:          1000,
		})
	}

	return store
}

func TestKIP848RangeAssignor_Name(t *testing.T) {
	a := NewKIP848RangeAssignor()
	assert.Equal(t, "range", a.Name())
}

func TestKIP848RangeAssignor_EmptyMembers(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 3})
	assignor := NewKIP848RangeAssignor()

	result, err := assignor.Assign(ctx, nil, topicStore)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestKIP848RangeAssignor_SingleConsumerSingleTopic(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 3})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 1)

	// Consumer should get all 3 partitions
	assignments := result["consumer-1"]
	require.Len(t, assignments, 1)
	assert.Equal(t, "topic1", assignments[0].TopicName)
	assert.ElementsMatch(t, []int32{0, 1, 2}, assignments[0].Partitions)
}

func TestKIP848RangeAssignor_TwoConsumersEvenSplit(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 4})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Consumer-1 (alphabetically first) gets partitions 0-1
	c1 := result["consumer-1"]
	require.Len(t, c1, 1)
	assert.ElementsMatch(t, []int32{0, 1}, c1[0].Partitions)

	// Consumer-2 gets partitions 2-3
	c2 := result["consumer-2"]
	require.Len(t, c2, 1)
	assert.ElementsMatch(t, []int32{2, 3}, c2[0].Partitions)
}

func TestKIP848RangeAssignor_TwoConsumersUnevenSplit(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 5})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Consumer-1 (first) gets 3 partitions (ceil(5/2))
	c1 := result["consumer-1"]
	require.Len(t, c1, 1)
	assert.ElementsMatch(t, []int32{0, 1, 2}, c1[0].Partitions)

	// Consumer-2 gets 2 partitions (floor(5/2))
	c2 := result["consumer-2"]
	require.Len(t, c2, 1)
	assert.ElementsMatch(t, []int32{3, 4}, c2[0].Partitions)
}

func TestKIP848RangeAssignor_ThreeConsumersUnevenSplit(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 7})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-3", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 3)

	// Consumer-1 gets 3 partitions (ceil(7/3))
	c1 := result["consumer-1"]
	require.Len(t, c1, 1)
	assert.ElementsMatch(t, []int32{0, 1, 2}, c1[0].Partitions)

	// Consumer-2 gets 2 partitions
	c2 := result["consumer-2"]
	require.Len(t, c2, 1)
	assert.ElementsMatch(t, []int32{3, 4}, c2[0].Partitions)

	// Consumer-3 gets 2 partitions
	c3 := result["consumer-3"]
	require.Len(t, c3, 1)
	assert.ElementsMatch(t, []int32{5, 6}, c3[0].Partitions)
}

func TestKIP848RangeAssignor_MoreConsumersThanPartitions(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 2})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-3", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-4", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 4)

	// Consumer-1 gets partition 0
	c1 := result["consumer-1"]
	require.Len(t, c1, 1)
	assert.ElementsMatch(t, []int32{0}, c1[0].Partitions)

	// Consumer-2 gets partition 1
	c2 := result["consumer-2"]
	require.Len(t, c2, 1)
	assert.ElementsMatch(t, []int32{1}, c2[0].Partitions)

	// Consumer-3 and 4 get no partitions
	assert.Empty(t, result["consumer-3"])
	assert.Empty(t, result["consumer-4"])
}

func TestKIP848RangeAssignor_MultipleTopics(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{
		"topic1": 3,
		"topic2": 2,
	})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1", "topic2"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1", "topic2"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Consumer-1 gets topic1:0,1 and topic2:0
	c1 := result["consumer-1"]
	require.Len(t, c1, 2)

	// Sort assignments by topic name for consistent checking
	sort.Slice(c1, func(i, j int) bool {
		return c1[i].TopicName < c1[j].TopicName
	})
	assert.Equal(t, "topic1", c1[0].TopicName)
	assert.ElementsMatch(t, []int32{0, 1}, c1[0].Partitions)
	assert.Equal(t, "topic2", c1[1].TopicName)
	assert.ElementsMatch(t, []int32{0}, c1[1].Partitions)

	// Consumer-2 gets topic1:2 and topic2:1
	c2 := result["consumer-2"]
	require.Len(t, c2, 2)
	sort.Slice(c2, func(i, j int) bool {
		return c2[i].TopicName < c2[j].TopicName
	})
	assert.Equal(t, "topic1", c2[0].TopicName)
	assert.ElementsMatch(t, []int32{2}, c2[0].Partitions)
	assert.Equal(t, "topic2", c2[1].TopicName)
	assert.ElementsMatch(t, []int32{1}, c2[1].Partitions)
}

func TestKIP848RangeAssignor_PartialSubscription(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{
		"topic1": 4,
		"topic2": 4,
	})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1", "topic2"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-3", SubscribedTopics: []string{"topic2"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 3)

	// For topic1: consumer-1 and consumer-2 are subscribed
	// consumer-1 gets 0,1; consumer-2 gets 2,3
	// For topic2: consumer-1 and consumer-3 are subscribed
	// consumer-1 gets 0,1; consumer-3 gets 2,3

	c1 := result["consumer-1"]
	require.Len(t, c1, 2) // subscribed to both topics

	c2 := result["consumer-2"]
	require.Len(t, c2, 1) // only topic1
	assert.Equal(t, "topic1", c2[0].TopicName)
	assert.ElementsMatch(t, []int32{2, 3}, c2[0].Partitions)

	c3 := result["consumer-3"]
	require.Len(t, c3, 1) // only topic2
	assert.Equal(t, "topic2", c3[0].TopicName)
	assert.ElementsMatch(t, []int32{2, 3}, c3[0].Partitions)
}

func TestKIP848RangeAssignor_NonExistentTopic(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 2})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1", "nonexistent"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 1)

	// Only topic1 should be assigned
	c1 := result["consumer-1"]
	require.Len(t, c1, 1)
	assert.Equal(t, "topic1", c1[0].TopicName)
	assert.ElementsMatch(t, []int32{0, 1}, c1[0].Partitions)
}

func TestKIP848RangeAssignor_TopicWithNoSubscribers(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{
		"topic1": 2,
		"topic2": 2,
	})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 1)

	// Only topic1 should be assigned
	c1 := result["consumer-1"]
	require.Len(t, c1, 1)
	assert.Equal(t, "topic1", c1[0].TopicName)
	assert.ElementsMatch(t, []int32{0, 1}, c1[0].Partitions)
}

func TestKIP848RangeAssignor_DeterministicOrdering(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 6})
	assignor := NewKIP848RangeAssignor()

	// Members added in different order
	members := []GroupMember{
		{MemberID: "z-consumer", SubscribedTopics: []string{"topic1"}},
		{MemberID: "a-consumer", SubscribedTopics: []string{"topic1"}},
		{MemberID: "m-consumer", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 3)

	// a-consumer (alphabetically first) gets partitions 0,1
	assert.ElementsMatch(t, []int32{0, 1}, result["a-consumer"][0].Partitions)
	// m-consumer gets partitions 2,3
	assert.ElementsMatch(t, []int32{2, 3}, result["m-consumer"][0].Partitions)
	// z-consumer gets partitions 4,5
	assert.ElementsMatch(t, []int32{4, 5}, result["z-consumer"][0].Partitions)
}

func TestKIP848RangeAssignor_Determinism(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{
		"topic1": 5,
		"topic2": 3,
	})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1", "topic2"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1", "topic2"}},
		{MemberID: "consumer-3", SubscribedTopics: []string{"topic1"}},
	}

	// Run multiple times and verify results are identical
	var firstResult map[string][]KIP848Assignment
	for i := 0; i < 10; i++ {
		result, err := assignor.Assign(ctx, members, topicStore)
		require.NoError(t, err)

		if firstResult == nil {
			firstResult = result
		} else {
			for memberID := range firstResult {
				assert.Equal(t, len(firstResult[memberID]), len(result[memberID]),
					"iteration %d: assignment count for %s should match", i, memberID)

				for j, a := range firstResult[memberID] {
					assert.Equal(t, a.TopicName, result[memberID][j].TopicName,
						"iteration %d: topic for %s should match", i, memberID)
					assert.ElementsMatch(t, a.Partitions, result[memberID][j].Partitions,
						"iteration %d: partitions for %s should match", i, memberID)
				}
			}
		}
	}
}

func TestKIP848RangeAssignor_NoGaps(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 10})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)

	// Collect all assigned partitions
	assigned := make(map[int32]bool)
	for _, assignments := range result {
		for _, a := range assignments {
			for _, p := range a.Partitions {
				assert.False(t, assigned[p], "partition %d assigned twice", p)
				assigned[p] = true
			}
		}
	}

	// Verify all partitions were assigned
	for i := int32(0); i < 10; i++ {
		assert.True(t, assigned[i], "partition %d not assigned", i)
	}
}

func TestKIP848RangeAssignor_NoOverlap(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{
		"topic1": 6,
		"topic2": 4,
	})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1", "topic2"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1", "topic2"}},
		{MemberID: "consumer-3", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)

	// Track which consumer got each partition
	assignments := make(map[string]string) // "topic:partition" -> memberID
	for memberID, memberAssignments := range result {
		for _, a := range memberAssignments {
			for _, p := range a.Partitions {
				key := a.TopicName + ":" + strconv.FormatInt(int64(p), 10)
				existing, ok := assignments[key]
				assert.False(t, ok, "partition %s assigned to both %s and %s", key, existing, memberID)
				assignments[key] = memberID
			}
		}
	}
}

func TestKIP848RangeAssignor_ContiguousRanges(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 12})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-3", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)

	// Each consumer should have contiguous ranges
	// consumer-1: 0-3, consumer-2: 4-7, consumer-3: 8-11
	c1 := result["consumer-1"][0].Partitions
	sort.Slice(c1, func(i, j int) bool { return c1[i] < c1[j] })
	assert.Equal(t, []int32{0, 1, 2, 3}, c1)

	c2 := result["consumer-2"][0].Partitions
	sort.Slice(c2, func(i, j int) bool { return c2[i] < c2[j] })
	assert.Equal(t, []int32{4, 5, 6, 7}, c2)

	c3 := result["consumer-3"][0].Partitions
	sort.Slice(c3, func(i, j int) bool { return c3[i] < c3[j] })
	assert.Equal(t, []int32{8, 9, 10, 11}, c3)
}

func TestKIP848RangeAssignor_TopicIDAssigned(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 2})
	assignor := NewKIP848RangeAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)

	// Topic ID should be set (non-zero UUID)
	c1 := result["consumer-1"]
	require.Len(t, c1, 1)
	assert.NotEqual(t, uuid.Nil, c1[0].TopicID, "topic ID should be set")
}

// KIP848UniformAssignor tests

func TestKIP848UniformAssignor_Name(t *testing.T) {
	a := NewKIP848UniformAssignor()
	assert.Equal(t, "uniform", a.Name())
}

func TestKIP848UniformAssignor_BasicAssignment(t *testing.T) {
	ctx := context.Background()
	topicStore := newMockTopicStore(map[string]int32{"topic1": 6})
	assignor := NewKIP848UniformAssignor()

	members := []GroupMember{
		{MemberID: "consumer-1", SubscribedTopics: []string{"topic1"}},
		{MemberID: "consumer-2", SubscribedTopics: []string{"topic1"}},
	}

	result, err := assignor.Assign(ctx, members, topicStore)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Each consumer should get 3 partitions
	assert.Len(t, result["consumer-1"][0].Partitions, 3)
	assert.Len(t, result["consumer-2"][0].Partitions, 3)

	// Total should be 6
	allPartitions := make(map[int32]bool)
	for _, assignments := range result {
		for _, a := range assignments {
			for _, p := range a.Partitions {
				allPartitions[p] = true
			}
		}
	}
	assert.Len(t, allPartitions, 6)
}

func TestGetKIP848Assignor(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"range", "range"},
		{"uniform", "uniform"},
		{"unknown", ""},
		{"sticky", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := GetKIP848Assignor(tt.name)
			if tt.expected == "" {
				assert.Nil(t, a)
			} else {
				require.NotNil(t, a)
				assert.Equal(t, tt.expected, a.Name())
			}
		})
	}
}

func TestEncodeDecodeKIP848Assignment(t *testing.T) {
	tests := []struct {
		name        string
		assignments []KIP848Assignment
	}{
		{
			name:        "empty",
			assignments: nil,
		},
		{
			name: "single topic",
			assignments: []KIP848Assignment{
				{
					TopicID:    uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
					TopicName:  "topic1",
					Partitions: []int32{0, 1, 2},
				},
			},
		},
		{
			name: "multiple topics",
			assignments: []KIP848Assignment{
				{
					TopicID:    uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
					TopicName:  "topic1",
					Partitions: []int32{0, 1},
				},
				{
					TopicID:    uuid.MustParse("87654321-4321-4321-4321-cba987654321"),
					TopicName:  "topic2",
					Partitions: []int32{0, 1, 2, 3},
				},
			},
		},
		{
			name: "empty partitions",
			assignments: []KIP848Assignment{
				{
					TopicID:    uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
					TopicName:  "topic1",
					Partitions: []int32{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeKIP848Assignment(tt.assignments)
			decoded, err := DecodeKIP848Assignment(encoded)
			require.NoError(t, err)

			if len(tt.assignments) == 0 {
				assert.Nil(t, decoded)
				return
			}

			require.Len(t, decoded, len(tt.assignments))
			for i, a := range tt.assignments {
				assert.Equal(t, a.TopicID, decoded[i].TopicID)
				assert.ElementsMatch(t, a.Partitions, decoded[i].Partitions)
			}
		})
	}
}
