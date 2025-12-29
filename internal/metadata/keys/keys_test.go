package keys

import (
	"math"
	"sort"
	"testing"
)

func TestEncodeUint64(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		width    int
		expected string
	}{
		{"zero", 0, 20, "00000000000000000000"},
		{"one", 1, 20, "00000000000000000001"},
		{"hundred", 100, 20, "00000000000000000100"},
		{"max_int64", uint64(math.MaxInt64), 20, "09223372036854775807"},
		{"large", 12345678901234567890, 20, "12345678901234567890"},
		{"short_width", 42, 5, "00042"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := EncodeUint64(tc.value, tc.width)
			if result != tc.expected {
				t.Errorf("EncodeUint64(%d, %d) = %q, want %q", tc.value, tc.width, result, tc.expected)
			}
		})
	}
}

func TestDecodeUint64(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint64
		wantErr  bool
	}{
		{"zero", "00000000000000000000", 0, false},
		{"one", "00000000000000000001", 1, false},
		{"hundred", "00000000000000000100", 100, false},
		{"max_int64", "09223372036854775807", uint64(math.MaxInt64), false},
		{"large", "12345678901234567890", 12345678901234567890, false},
		{"no_padding", "42", 42, false},
		{"invalid", "abc", 0, true},
		{"negative", "-1", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DecodeUint64(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("DecodeUint64(%q) expected error, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Errorf("DecodeUint64(%q) unexpected error: %v", tc.input, err)
				return
			}
			if result != tc.expected {
				t.Errorf("DecodeUint64(%q) = %d, want %d", tc.input, result, tc.expected)
			}
		})
	}
}

func TestEncodeInt64(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		width    int
		expected string
		wantErr  bool
	}{
		{"zero", 0, 20, "00000000000000000000", false},
		{"one", 1, 20, "00000000000000000001", false},
		{"max_int64", math.MaxInt64, 20, "09223372036854775807", false},
		{"negative", -1, 20, "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := EncodeInt64(tc.value, tc.width)
			if tc.wantErr {
				if err == nil {
					t.Errorf("EncodeInt64(%d, %d) expected error, got nil", tc.value, tc.width)
				}
				return
			}
			if err != nil {
				t.Errorf("EncodeInt64(%d, %d) unexpected error: %v", tc.value, tc.width, err)
				return
			}
			if result != tc.expected {
				t.Errorf("EncodeInt64(%d, %d) = %q, want %q", tc.value, tc.width, result, tc.expected)
			}
		})
	}
}

func TestDecodeInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
		wantErr  bool
	}{
		{"zero", "00000000000000000000", 0, false},
		{"one", "00000000000000000001", 1, false},
		{"max_int64", "09223372036854775807", math.MaxInt64, false},
		{"no_padding", "42", 42, false},
		{"invalid", "abc", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DecodeInt64(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("DecodeInt64(%q) expected error, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Errorf("DecodeInt64(%q) unexpected error: %v", tc.input, err)
				return
			}
			if result != tc.expected {
				t.Errorf("DecodeInt64(%q) = %d, want %d", tc.input, result, tc.expected)
			}
		})
	}
}

func TestEncodeOffsetEnd(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		expected string
		wantErr  bool
	}{
		{"zero", 0, "00000000000000000000", false},
		{"one", 1, "00000000000000000001", false},
		{"thousand", 1000, "00000000000000001000", false},
		{"large", 1234567890123456789, "01234567890123456789", false},
		{"negative", -1, "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := EncodeOffsetEnd(tc.value)
			if tc.wantErr {
				if err == nil {
					t.Errorf("EncodeOffsetEnd(%d) expected error, got nil", tc.value)
				}
				return
			}
			if err != nil {
				t.Errorf("EncodeOffsetEnd(%d) unexpected error: %v", tc.value, err)
				return
			}
			if result != tc.expected {
				t.Errorf("EncodeOffsetEnd(%d) = %q, want %q", tc.value, result, tc.expected)
			}
			if len(result) != OffsetWidth {
				t.Errorf("EncodeOffsetEnd(%d) length = %d, want %d", tc.value, len(result), OffsetWidth)
			}
		})
	}
}

func TestEncodeCumulativeSize(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		expected string
		wantErr  bool
	}{
		{"zero", 0, "00000000000000000000", false},
		{"one", 1, "00000000000000000001", false},
		{"megabyte", 1024 * 1024, "00000000000001048576", false},
		{"negative", -1, "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := EncodeCumulativeSize(tc.value)
			if tc.wantErr {
				if err == nil {
					t.Errorf("EncodeCumulativeSize(%d) expected error, got nil", tc.value)
				}
				return
			}
			if err != nil {
				t.Errorf("EncodeCumulativeSize(%d) unexpected error: %v", tc.value, err)
				return
			}
			if result != tc.expected {
				t.Errorf("EncodeCumulativeSize(%d) = %q, want %q", tc.value, result, tc.expected)
			}
			if len(result) != SizeWidth {
				t.Errorf("EncodeCumulativeSize(%d) length = %d, want %d", tc.value, len(result), SizeWidth)
			}
		})
	}
}

func TestOffsetIndexKeyPath(t *testing.T) {
	tests := []struct {
		name           string
		streamID       string
		offsetEnd      int64
		cumulativeSize int64
		expected       string
		wantErr        bool
	}{
		{
			name:           "basic",
			streamID:       "stream-abc-123",
			offsetEnd:      100,
			cumulativeSize: 5000,
			expected:       "/dray/v1/streams/stream-abc-123/offset-index/00000000000000000100/00000000000000005000",
			wantErr:        false,
		},
		{
			name:           "zero_values",
			streamID:       "stream-0",
			offsetEnd:      0,
			cumulativeSize: 0,
			expected:       "/dray/v1/streams/stream-0/offset-index/00000000000000000000/00000000000000000000",
			wantErr:        false,
		},
		{
			name:           "large_values",
			streamID:       "uuid-12345",
			offsetEnd:      math.MaxInt64,
			cumulativeSize: 1234567890123456789,
			expected:       "/dray/v1/streams/uuid-12345/offset-index/09223372036854775807/01234567890123456789",
			wantErr:        false,
		},
		{
			name:           "negative_offset",
			streamID:       "stream-1",
			offsetEnd:      -1,
			cumulativeSize: 100,
			expected:       "",
			wantErr:        true,
		},
		{
			name:           "negative_size",
			streamID:       "stream-1",
			offsetEnd:      100,
			cumulativeSize: -1,
			expected:       "",
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := OffsetIndexKeyPath(tc.streamID, tc.offsetEnd, tc.cumulativeSize)
			if tc.wantErr {
				if err == nil {
					t.Errorf("OffsetIndexKeyPath() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("OffsetIndexKeyPath() unexpected error: %v", err)
				return
			}
			if result != tc.expected {
				t.Errorf("OffsetIndexKeyPath() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestOffsetIndexPrefix(t *testing.T) {
	streamID := "stream-abc"
	expected := "/dray/v1/streams/stream-abc/offset-index/"
	result := OffsetIndexPrefix(streamID)
	if result != expected {
		t.Errorf("OffsetIndexPrefix(%q) = %q, want %q", streamID, result, expected)
	}
}

func TestOffsetIndexStartKey(t *testing.T) {
	streamID := "stream-abc"
	offsetEnd := int64(100)
	expected := "/dray/v1/streams/stream-abc/offset-index/00000000000000000100/"
	result, err := OffsetIndexStartKey(streamID, offsetEnd)
	if err != nil {
		t.Errorf("OffsetIndexStartKey() unexpected error: %v", err)
	}
	if result != expected {
		t.Errorf("OffsetIndexStartKey() = %q, want %q", result, expected)
	}
}

func TestParseOffsetIndexKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		want    OffsetIndexKey
		wantErr bool
	}{
		{
			name: "valid_key",
			key:  "/dray/v1/streams/stream-abc-123/offset-index/00000000000000000100/00000000000000005000",
			want: OffsetIndexKey{
				StreamID:       "stream-abc-123",
				OffsetEnd:      100,
				CumulativeSize: 5000,
			},
			wantErr: false,
		},
		{
			name: "zero_values",
			key:  "/dray/v1/streams/stream-0/offset-index/00000000000000000000/00000000000000000000",
			want: OffsetIndexKey{
				StreamID:       "stream-0",
				OffsetEnd:      0,
				CumulativeSize: 0,
			},
			wantErr: false,
		},
		{
			name: "large_values",
			key:  "/dray/v1/streams/uuid-12345/offset-index/09223372036854775807/01234567890123456789",
			want: OffsetIndexKey{
				StreamID:       "uuid-12345",
				OffsetEnd:      9223372036854775807,
				CumulativeSize: 1234567890123456789,
			},
			wantErr: false,
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/v1/streams/stream-abc/offset-index/00000000000000000100/00000000000000005000",
			wantErr: true,
		},
		{
			name:    "missing_offset_index",
			key:     "/dray/v1/streams/stream-abc/00000000000000000100/00000000000000005000",
			wantErr: true,
		},
		{
			name:    "missing_cumulative_size",
			key:     "/dray/v1/streams/stream-abc/offset-index/00000000000000000100",
			wantErr: true,
		},
		{
			name:    "extra_component",
			key:     "/dray/v1/streams/stream-abc/offset-index/00000000000000000100/00000000000000005000/extra",
			wantErr: true,
		},
		{
			name:    "invalid_offset",
			key:     "/dray/v1/streams/stream-abc/offset-index/invalid/00000000000000005000",
			wantErr: true,
		},
		{
			name:    "invalid_size",
			key:     "/dray/v1/streams/stream-abc/offset-index/00000000000000000100/invalid",
			wantErr: true,
		},
		{
			name:    "negative_offset",
			key:     "/dray/v1/streams/stream-abc/offset-index/-1/00000000000000005000",
			wantErr: true,
		},
		{
			name:    "negative_size",
			key:     "/dray/v1/streams/stream-abc/offset-index/00000000000000000100/-1",
			wantErr: true,
		},
		{
			name:    "empty_stream_id",
			key:     "/dray/v1/streams//offset-index/00000000000000000100/00000000000000005000",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseOffsetIndexKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseOffsetIndexKey(%q) expected error, got nil", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseOffsetIndexKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if result != tc.want {
				t.Errorf("ParseOffsetIndexKey(%q) = %+v, want %+v", tc.key, result, tc.want)
			}
		})
	}
}

func TestLexicographicOrdering(t *testing.T) {
	// This is a critical test: verify that when sorted lexicographically,
	// the keys are in the correct numeric order
	type entry struct {
		offsetEnd      int64
		cumulativeSize int64
	}

	entries := []entry{
		{offsetEnd: 1000, cumulativeSize: 50000},
		{offsetEnd: 100, cumulativeSize: 5000},
		{offsetEnd: 100, cumulativeSize: 6000},
		{offsetEnd: 10, cumulativeSize: 1000},
		{offsetEnd: 9223372036854775807, cumulativeSize: 0},
		{offsetEnd: 0, cumulativeSize: 0},
		{offsetEnd: 99, cumulativeSize: 9999},
		{offsetEnd: 100, cumulativeSize: 4999},
		{offsetEnd: 500, cumulativeSize: 20000},
	}

	streamID := "test-stream"
	keys := make([]string, len(entries))
	for i, e := range entries {
		key, err := OffsetIndexKeyPath(streamID, e.offsetEnd, e.cumulativeSize)
		if err != nil {
			t.Fatalf("Failed to create key: %v", err)
		}
		keys[i] = key
	}

	// Sort lexicographically
	sort.Strings(keys)

	// Parse back and verify ordering
	var prevOffset int64 = -1
	var prevSize int64 = -1
	for i, key := range keys {
		parsed, err := ParseOffsetIndexKey(key)
		if err != nil {
			t.Fatalf("Failed to parse key: %v", err)
		}

		// Verify ordering: should be sorted by offsetEnd first, then by cumulativeSize
		if parsed.OffsetEnd < prevOffset {
			t.Errorf("Key %d: offsetEnd %d < prevOffset %d - lexicographic ordering violated", i, parsed.OffsetEnd, prevOffset)
		} else if parsed.OffsetEnd == prevOffset && parsed.CumulativeSize < prevSize {
			t.Errorf("Key %d: same offsetEnd but cumulativeSize %d < prevSize %d - lexicographic ordering violated", i, parsed.CumulativeSize, prevSize)
		}

		prevOffset = parsed.OffsetEnd
		prevSize = parsed.CumulativeSize
	}
}

func TestLexicographicOrderingEdgeCases(t *testing.T) {
	// Test that numbers of different magnitudes sort correctly
	values := []int64{
		0,
		1,
		9,
		10,
		99,
		100,
		999,
		1000,
		9999,
		10000,
		math.MaxInt64,
	}

	streamID := "test-stream"
	keys := make([]string, len(values))
	for i, v := range values {
		key, err := OffsetIndexKeyPath(streamID, v, 0)
		if err != nil {
			t.Fatalf("Failed to create key for value %d: %v", v, err)
		}
		keys[i] = key
	}

	// Sort lexicographically
	sort.Strings(keys)

	// Verify order matches the original order (values were already sorted)
	for i, key := range keys {
		parsed, err := ParseOffsetIndexKey(key)
		if err != nil {
			t.Fatalf("Failed to parse key: %v", err)
		}
		if parsed.OffsetEnd != values[i] {
			t.Errorf("After sort, index %d has offsetEnd %d, want %d", i, parsed.OffsetEnd, values[i])
		}
	}
}

func TestRoundTrip(t *testing.T) {
	// Test that encoding and decoding produces the same values
	testCases := []struct {
		streamID       string
		offsetEnd      int64
		cumulativeSize int64
	}{
		{"stream-1", 0, 0},
		{"stream-2", 1, 100},
		{"stream-3", 12345, 67890},
		{"uuid-abc-123-def", math.MaxInt64, 9999999999},
		{"stream/with/slashes", 500, 1000}, // Note: slashes in streamID work due to /offset-index/ separator
	}

	for _, tc := range testCases {
		key, err := OffsetIndexKeyPath(tc.streamID, tc.offsetEnd, tc.cumulativeSize)
		if err != nil {
			t.Errorf("OffsetIndexKeyPath(%q, %d, %d) error: %v", tc.streamID, tc.offsetEnd, tc.cumulativeSize, err)
			continue
		}

		parsed, err := ParseOffsetIndexKey(key)
		if err != nil {
			t.Errorf("ParseOffsetIndexKey(%q) error: %v", key, err)
			continue
		}

		if parsed.StreamID != tc.streamID {
			t.Errorf("Round trip streamID: got %q, want %q", parsed.StreamID, tc.streamID)
		}
		if parsed.OffsetEnd != tc.offsetEnd {
			t.Errorf("Round trip offsetEnd: got %d, want %d", parsed.OffsetEnd, tc.offsetEnd)
		}
		if parsed.CumulativeSize != tc.cumulativeSize {
			t.Errorf("Round trip cumulativeSize: got %d, want %d", parsed.CumulativeSize, tc.cumulativeSize)
		}
	}
}

func TestHwmKeyPath(t *testing.T) {
	streamID := "test-stream-123"
	expected := "/dray/v1/streams/test-stream-123/hwm"
	result := HwmKeyPath(streamID)
	if result != expected {
		t.Errorf("HwmKeyPath(%q) = %q, want %q", streamID, result, expected)
	}
}

func TestTopicKeyPath(t *testing.T) {
	topicName := "my-topic"
	expected := "/dray/v1/topics/my-topic"
	result := TopicKeyPath(topicName)
	if result != expected {
		t.Errorf("TopicKeyPath(%q) = %q, want %q", topicName, result, expected)
	}
}

func TestStreamMetaKeyPath(t *testing.T) {
	streamID := "stream-abc"
	expected := "/dray/v1/streams/stream-abc/meta"
	result := StreamMetaKeyPath(streamID)
	if result != expected {
		t.Errorf("StreamMetaKeyPath(%q) = %q, want %q", streamID, result, expected)
	}
}

func TestConstants(t *testing.T) {
	if OffsetWidth != 20 {
		t.Errorf("OffsetWidth = %d, want 20", OffsetWidth)
	}
	if SizeWidth != 20 {
		t.Errorf("SizeWidth = %d, want 20", SizeWidth)
	}
}

func TestPrefixes(t *testing.T) {
	if Prefix != "/dray/v1" {
		t.Errorf("Prefix = %q, want %q", Prefix, "/dray/v1")
	}
	if StreamsPrefix != "/dray/v1/streams" {
		t.Errorf("StreamsPrefix = %q, want %q", StreamsPrefix, "/dray/v1/streams")
	}
	if TopicsPrefix != "/dray/v1/topics" {
		t.Errorf("TopicsPrefix = %q, want %q", TopicsPrefix, "/dray/v1/topics")
	}
	if ClusterPrefix != "/dray/v1/cluster" {
		t.Errorf("ClusterPrefix = %q, want %q", ClusterPrefix, "/dray/v1/cluster")
	}
	if GroupsPrefix != "/dray/v1/groups" {
		t.Errorf("GroupsPrefix = %q, want %q", GroupsPrefix, "/dray/v1/groups")
	}
	if ACLsPrefix != "/dray/v1/acls" {
		t.Errorf("ACLsPrefix = %q, want %q", ACLsPrefix, "/dray/v1/acls")
	}
	if WALStagingPrefix != "/dray/v1/wal/staging" {
		t.Errorf("WALStagingPrefix = %q, want %q", WALStagingPrefix, "/dray/v1/wal/staging")
	}
	if WALObjectsPrefix != "/dray/v1/wal/objects" {
		t.Errorf("WALObjectsPrefix = %q, want %q", WALObjectsPrefix, "/dray/v1/wal/objects")
	}
	if WALGCPrefix != "/dray/v1/wal/gc" {
		t.Errorf("WALGCPrefix = %q, want %q", WALGCPrefix, "/dray/v1/wal/gc")
	}
	if CompactionLocksPrefix != "/dray/v1/compaction/locks" {
		t.Errorf("CompactionLocksPrefix = %q, want %q", CompactionLocksPrefix, "/dray/v1/compaction/locks")
	}
	if CompactionJobsPrefix != "/dray/v1/compaction" {
		t.Errorf("CompactionJobsPrefix = %q, want %q", CompactionJobsPrefix, "/dray/v1/compaction")
	}
	if IcebergLocksPrefix != "/dray/v1/iceberg" {
		t.Errorf("IcebergLocksPrefix = %q, want %q", IcebergLocksPrefix, "/dray/v1/iceberg")
	}
}

// =============================================================================
// Cluster and Broker Key Tests (§6.3.1)
// =============================================================================

func TestBrokerKeyPath(t *testing.T) {
	tests := []struct {
		clusterID string
		brokerID  string
		want      string
	}{
		{"cluster-1", "broker-1", "/dray/v1/cluster/cluster-1/brokers/broker-1"},
		{"prod-us-east", "node-abc-123", "/dray/v1/cluster/prod-us-east/brokers/node-abc-123"},
	}

	for _, tc := range tests {
		got := BrokerKeyPath(tc.clusterID, tc.brokerID)
		if got != tc.want {
			t.Errorf("BrokerKeyPath(%q, %q) = %q, want %q", tc.clusterID, tc.brokerID, got, tc.want)
		}
	}
}

func TestBrokersPrefix(t *testing.T) {
	got := BrokersPrefix("my-cluster")
	want := "/dray/v1/cluster/my-cluster/brokers/"
	if got != want {
		t.Errorf("BrokersPrefix() = %q, want %q", got, want)
	}
}

func TestClusterKeyPath(t *testing.T) {
	got := ClusterKeyPath("prod-cluster")
	want := "/dray/v1/cluster/prod-cluster"
	if got != want {
		t.Errorf("ClusterKeyPath() = %q, want %q", got, want)
	}
}

func TestParseBrokerKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		wantClust string
		wantBrok  string
		wantErr   bool
	}{
		{
			name:      "valid",
			key:       "/dray/v1/cluster/cluster-1/brokers/broker-1",
			wantClust: "cluster-1",
			wantBrok:  "broker-1",
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/cluster-1/brokers/broker-1",
			wantErr: true,
		},
		{
			name:    "missing_brokers",
			key:     "/dray/v1/cluster/cluster-1/broker-1",
			wantErr: true,
		},
		{
			name:    "empty_cluster",
			key:     "/dray/v1/cluster//brokers/broker-1",
			wantErr: true,
		},
		{
			name:    "empty_broker",
			key:     "/dray/v1/cluster/cluster-1/brokers/",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clust, brok, err := ParseBrokerKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseBrokerKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseBrokerKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if clust != tc.wantClust || brok != tc.wantBrok {
				t.Errorf("ParseBrokerKey(%q) = (%q, %q), want (%q, %q)",
					tc.key, clust, brok, tc.wantClust, tc.wantBrok)
			}
		})
	}
}

// =============================================================================
// Topic and Partition Key Tests (§6.3.2)
// =============================================================================

func TestTopicPartitionKeyPath(t *testing.T) {
	tests := []struct {
		topic     string
		partition int32
		want      string
	}{
		{"my-topic", 0, "/dray/v1/topics/my-topic/partitions/0"},
		{"orders", 5, "/dray/v1/topics/orders/partitions/5"},
		{"events", 100, "/dray/v1/topics/events/partitions/100"},
	}

	for _, tc := range tests {
		got := TopicPartitionKeyPath(tc.topic, tc.partition)
		if got != tc.want {
			t.Errorf("TopicPartitionKeyPath(%q, %d) = %q, want %q", tc.topic, tc.partition, got, tc.want)
		}
	}
}

func TestTopicPartitionsPrefix(t *testing.T) {
	got := TopicPartitionsPrefix("my-topic")
	want := "/dray/v1/topics/my-topic/partitions/"
	if got != want {
		t.Errorf("TopicPartitionsPrefix() = %q, want %q", got, want)
	}
}

func TestParseTopicPartitionKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		wantTopic string
		wantPart  int32
		wantErr   bool
	}{
		{
			name:      "valid",
			key:       "/dray/v1/topics/my-topic/partitions/5",
			wantTopic: "my-topic",
			wantPart:  5,
		},
		{
			name:      "partition_zero",
			key:       "/dray/v1/topics/orders/partitions/0",
			wantTopic: "orders",
			wantPart:  0,
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/topics/my-topic/partitions/5",
			wantErr: true,
		},
		{
			name:    "missing_partitions",
			key:     "/dray/v1/topics/my-topic/5",
			wantErr: true,
		},
		{
			name:    "invalid_partition",
			key:     "/dray/v1/topics/my-topic/partitions/abc",
			wantErr: true,
		},
		{
			name:    "negative_partition",
			key:     "/dray/v1/topics/my-topic/partitions/-1",
			wantErr: true,
		},
		{
			name:    "empty_topic",
			key:     "/dray/v1/topics//partitions/5",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topic, part, err := ParseTopicPartitionKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseTopicPartitionKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseTopicPartitionKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if topic != tc.wantTopic || part != tc.wantPart {
				t.Errorf("ParseTopicPartitionKey(%q) = (%q, %d), want (%q, %d)",
					tc.key, topic, part, tc.wantTopic, tc.wantPart)
			}
		})
	}
}

// =============================================================================
// Compaction Task Key Tests (§6.3.5)
// =============================================================================

func TestCompactionTaskKeyPath(t *testing.T) {
	got := CompactionTaskKeyPath("stream-123", "task-456")
	want := "/dray/v1/streams/stream-123/compaction/tasks/task-456"
	if got != want {
		t.Errorf("CompactionTaskKeyPath() = %q, want %q", got, want)
	}
}

func TestCompactionTasksPrefix(t *testing.T) {
	got := CompactionTasksPrefix("stream-123")
	want := "/dray/v1/streams/stream-123/compaction/tasks/"
	if got != want {
		t.Errorf("CompactionTasksPrefix() = %q, want %q", got, want)
	}
}

func TestParseCompactionTaskKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantStream string
		wantTask   string
		wantErr    bool
	}{
		{
			name:       "valid",
			key:        "/dray/v1/streams/stream-123/compaction/tasks/task-456",
			wantStream: "stream-123",
			wantTask:   "task-456",
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/streams/stream-123/compaction/tasks/task-456",
			wantErr: true,
		},
		{
			name:    "missing_compaction",
			key:     "/dray/v1/streams/stream-123/tasks/task-456",
			wantErr: true,
		},
		{
			name:    "empty_stream",
			key:     "/dray/v1/streams//compaction/tasks/task-456",
			wantErr: true,
		},
		{
			name:    "empty_task",
			key:     "/dray/v1/streams/stream-123/compaction/tasks/",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream, task, err := ParseCompactionTaskKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseCompactionTaskKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseCompactionTaskKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if stream != tc.wantStream || task != tc.wantTask {
				t.Errorf("ParseCompactionTaskKey(%q) = (%q, %q), want (%q, %q)",
					tc.key, stream, task, tc.wantStream, tc.wantTask)
			}
		})
	}
}

// =============================================================================
// Consumer Group Key Tests (§6.3.6)
// =============================================================================

func TestGroupStateKeyPath(t *testing.T) {
	got := GroupStateKeyPath("my-group")
	want := "/dray/v1/groups/my-group/state"
	if got != want {
		t.Errorf("GroupStateKeyPath() = %q, want %q", got, want)
	}
}

func TestGroupTypeKeyPath(t *testing.T) {
	got := GroupTypeKeyPath("my-group")
	want := "/dray/v1/groups/my-group/type"
	if got != want {
		t.Errorf("GroupTypeKeyPath() = %q, want %q", got, want)
	}
}

func TestGroupMemberKeyPath(t *testing.T) {
	got := GroupMemberKeyPath("my-group", "member-1")
	want := "/dray/v1/groups/my-group/members/member-1"
	if got != want {
		t.Errorf("GroupMemberKeyPath() = %q, want %q", got, want)
	}
}

func TestGroupMembersPrefix(t *testing.T) {
	got := GroupMembersPrefix("my-group")
	want := "/dray/v1/groups/my-group/members/"
	if got != want {
		t.Errorf("GroupMembersPrefix() = %q, want %q", got, want)
	}
}

func TestParseGroupMemberKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantGroup  string
		wantMember string
		wantErr    bool
	}{
		{
			name:       "valid",
			key:        "/dray/v1/groups/my-group/members/member-1",
			wantGroup:  "my-group",
			wantMember: "member-1",
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/groups/my-group/members/member-1",
			wantErr: true,
		},
		{
			name:    "missing_members",
			key:     "/dray/v1/groups/my-group/member-1",
			wantErr: true,
		},
		{
			name:    "empty_group",
			key:     "/dray/v1/groups//members/member-1",
			wantErr: true,
		},
		{
			name:    "empty_member",
			key:     "/dray/v1/groups/my-group/members/",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			group, member, err := ParseGroupMemberKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseGroupMemberKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseGroupMemberKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if group != tc.wantGroup || member != tc.wantMember {
				t.Errorf("ParseGroupMemberKey(%q) = (%q, %q), want (%q, %q)",
					tc.key, group, member, tc.wantGroup, tc.wantMember)
			}
		})
	}
}

func TestGroupAssignmentKeyPath(t *testing.T) {
	got := GroupAssignmentKeyPath("my-group", "member-1")
	want := "/dray/v1/groups/my-group/assignment/member-1"
	if got != want {
		t.Errorf("GroupAssignmentKeyPath() = %q, want %q", got, want)
	}
}

func TestGroupAssignmentsPrefix(t *testing.T) {
	got := GroupAssignmentsPrefix("my-group")
	want := "/dray/v1/groups/my-group/assignment/"
	if got != want {
		t.Errorf("GroupAssignmentsPrefix() = %q, want %q", got, want)
	}
}

func TestGroupOffsetKeyPath(t *testing.T) {
	tests := []struct {
		groupID   string
		topic     string
		partition int32
		want      string
	}{
		{"group-1", "topic-1", 0, "/dray/v1/groups/group-1/offsets/topic-1/0"},
		{"my-group", "orders", 5, "/dray/v1/groups/my-group/offsets/orders/5"},
	}

	for _, tc := range tests {
		got := GroupOffsetKeyPath(tc.groupID, tc.topic, tc.partition)
		if got != tc.want {
			t.Errorf("GroupOffsetKeyPath(%q, %q, %d) = %q, want %q",
				tc.groupID, tc.topic, tc.partition, got, tc.want)
		}
	}
}

func TestGroupOffsetsPrefix(t *testing.T) {
	got := GroupOffsetsPrefix("my-group")
	want := "/dray/v1/groups/my-group/offsets/"
	if got != want {
		t.Errorf("GroupOffsetsPrefix() = %q, want %q", got, want)
	}
}

func TestGroupTopicOffsetsPrefix(t *testing.T) {
	got := GroupTopicOffsetsPrefix("my-group", "my-topic")
	want := "/dray/v1/groups/my-group/offsets/my-topic/"
	if got != want {
		t.Errorf("GroupTopicOffsetsPrefix() = %q, want %q", got, want)
	}
}

func TestParseGroupOffsetKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		wantGroup string
		wantTopic string
		wantPart  int32
		wantErr   bool
	}{
		{
			name:      "valid",
			key:       "/dray/v1/groups/group-1/offsets/topic-1/5",
			wantGroup: "group-1",
			wantTopic: "topic-1",
			wantPart:  5,
		},
		{
			name:      "partition_zero",
			key:       "/dray/v1/groups/my-group/offsets/orders/0",
			wantGroup: "my-group",
			wantTopic: "orders",
			wantPart:  0,
		},
		{
			name:      "topic_with_slashes",
			key:       "/dray/v1/groups/my-group/offsets/ns/topic/3",
			wantGroup: "my-group",
			wantTopic: "ns/topic",
			wantPart:  3,
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/groups/group-1/offsets/topic-1/5",
			wantErr: true,
		},
		{
			name:    "missing_offsets",
			key:     "/dray/v1/groups/group-1/topic-1/5",
			wantErr: true,
		},
		{
			name:    "empty_group",
			key:     "/dray/v1/groups//offsets/topic-1/5",
			wantErr: true,
		},
		{
			name:    "invalid_partition",
			key:     "/dray/v1/groups/group-1/offsets/topic-1/abc",
			wantErr: true,
		},
		{
			name:    "negative_partition",
			key:     "/dray/v1/groups/group-1/offsets/topic-1/-1",
			wantErr: true,
		},
		{
			name:    "missing_partition",
			key:     "/dray/v1/groups/group-1/offsets/topic-1",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			group, topic, part, err := ParseGroupOffsetKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseGroupOffsetKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseGroupOffsetKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if group != tc.wantGroup || topic != tc.wantTopic || part != tc.wantPart {
				t.Errorf("ParseGroupOffsetKey(%q) = (%q, %q, %d), want (%q, %q, %d)",
					tc.key, group, topic, part, tc.wantGroup, tc.wantTopic, tc.wantPart)
			}
		})
	}
}

func TestGroupKeyPath(t *testing.T) {
	got := GroupKeyPath("my-group")
	want := "/dray/v1/groups/my-group"
	if got != want {
		t.Errorf("GroupKeyPath() = %q, want %q", got, want)
	}
}

func TestGroupLeaseKeyPath(t *testing.T) {
	got := GroupLeaseKeyPath("my-group")
	want := "/dray/v1/groups/my-group/lease"
	if got != want {
		t.Errorf("GroupLeaseKeyPath() = %q, want %q", got, want)
	}
}

func TestParseGroupLeaseKey(t *testing.T) {
	tests := []struct {
		key       string
		wantGroup string
		wantErr   bool
	}{
		{"/dray/v1/groups/my-group/lease", "my-group", false},
		{"/dray/v1/groups/group-123/lease", "group-123", false},
		{"/dray/v1/groups/test-consumer-group/lease", "test-consumer-group", false},
		{"/dray/v1/groups//lease", "", true},                   // empty groupID
		{"/dray/v1/groups/my-group/state", "", true},           // wrong suffix
		{"/dray/v1/groups/my-group", "", true},                 // no /lease suffix
		{"/other/prefix/my-group/lease", "", true},             // wrong prefix
		{"/dray/v1/groups/lease", "", true},                    // missing groupID
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			groupID, err := ParseGroupLeaseKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseGroupLeaseKey(%q) expected error, got groupID=%q", tc.key, groupID)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseGroupLeaseKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if groupID != tc.wantGroup {
				t.Errorf("ParseGroupLeaseKey(%q) = %q, want %q", tc.key, groupID, tc.wantGroup)
			}
		})
	}
}

// =============================================================================
// WAL Key Tests (§9)
// =============================================================================

func TestWALStagingKeyPath(t *testing.T) {
	tests := []struct {
		domain int
		walID  string
		want   string
	}{
		{0, "wal-123", "/dray/v1/wal/staging/0/wal-123"},
		{5, "abc-def-ghi", "/dray/v1/wal/staging/5/abc-def-ghi"},
		{100, "uuid-1234", "/dray/v1/wal/staging/100/uuid-1234"},
	}

	for _, tc := range tests {
		got := WALStagingKeyPath(tc.domain, tc.walID)
		if got != tc.want {
			t.Errorf("WALStagingKeyPath(%d, %q) = %q, want %q", tc.domain, tc.walID, got, tc.want)
		}
	}
}

func TestWALStagingDomainPrefix(t *testing.T) {
	got := WALStagingDomainPrefix(5)
	want := "/dray/v1/wal/staging/5/"
	if got != want {
		t.Errorf("WALStagingDomainPrefix() = %q, want %q", got, want)
	}
}

func TestParseWALStagingKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantDomain int
		wantWAL    string
		wantErr    bool
	}{
		{
			name:       "valid",
			key:        "/dray/v1/wal/staging/5/wal-123",
			wantDomain: 5,
			wantWAL:    "wal-123",
		},
		{
			name:       "domain_zero",
			key:        "/dray/v1/wal/staging/0/abc",
			wantDomain: 0,
			wantWAL:    "abc",
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/staging/5/wal-123",
			wantErr: true,
		},
		{
			name:    "invalid_domain",
			key:     "/dray/v1/wal/staging/abc/wal-123",
			wantErr: true,
		},
		{
			name:    "negative_domain",
			key:     "/dray/v1/wal/staging/-1/wal-123",
			wantErr: true,
		},
		{
			name:    "empty_wal",
			key:     "/dray/v1/wal/staging/5/",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			domain, wal, err := ParseWALStagingKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseWALStagingKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseWALStagingKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if domain != tc.wantDomain || wal != tc.wantWAL {
				t.Errorf("ParseWALStagingKey(%q) = (%d, %q), want (%d, %q)",
					tc.key, domain, wal, tc.wantDomain, tc.wantWAL)
			}
		})
	}
}

func TestWALObjectKeyPath(t *testing.T) {
	got := WALObjectKeyPath(3, "wal-uuid")
	want := "/dray/v1/wal/objects/3/wal-uuid"
	if got != want {
		t.Errorf("WALObjectKeyPath() = %q, want %q", got, want)
	}
}

func TestWALObjectsDomainPrefix(t *testing.T) {
	got := WALObjectsDomainPrefix(3)
	want := "/dray/v1/wal/objects/3/"
	if got != want {
		t.Errorf("WALObjectsDomainPrefix() = %q, want %q", got, want)
	}
}

func TestParseWALObjectKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantDomain int
		wantWAL    string
		wantErr    bool
	}{
		{
			name:       "valid",
			key:        "/dray/v1/wal/objects/3/wal-uuid",
			wantDomain: 3,
			wantWAL:    "wal-uuid",
		},
		{
			name:    "wrong_prefix",
			key:     "/dray/v1/wal/staging/3/wal-uuid",
			wantErr: true,
		},
		{
			name:    "invalid_domain",
			key:     "/dray/v1/wal/objects/abc/wal-uuid",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			domain, wal, err := ParseWALObjectKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseWALObjectKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseWALObjectKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if domain != tc.wantDomain || wal != tc.wantWAL {
				t.Errorf("ParseWALObjectKey(%q) = (%d, %q), want (%d, %q)",
					tc.key, domain, wal, tc.wantDomain, tc.wantWAL)
			}
		})
	}
}

func TestWALGCKeyPath(t *testing.T) {
	got := WALGCKeyPath(7, "wal-gc-id")
	want := "/dray/v1/wal/gc/7/wal-gc-id"
	if got != want {
		t.Errorf("WALGCKeyPath() = %q, want %q", got, want)
	}
}

func TestWALGCDomainPrefix(t *testing.T) {
	got := WALGCDomainPrefix(7)
	want := "/dray/v1/wal/gc/7/"
	if got != want {
		t.Errorf("WALGCDomainPrefix() = %q, want %q", got, want)
	}
}

func TestParseWALGCKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantDomain int
		wantWAL    string
		wantErr    bool
	}{
		{
			name:       "valid",
			key:        "/dray/v1/wal/gc/7/wal-gc-id",
			wantDomain: 7,
			wantWAL:    "wal-gc-id",
		},
		{
			name:    "wrong_prefix",
			key:     "/dray/v1/wal/objects/7/wal-gc-id",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			domain, wal, err := ParseWALGCKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseWALGCKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseWALGCKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if domain != tc.wantDomain || wal != tc.wantWAL {
				t.Errorf("ParseWALGCKey(%q) = (%d, %q), want (%d, %q)",
					tc.key, domain, wal, tc.wantDomain, tc.wantWAL)
			}
		})
	}
}

// =============================================================================
// Compaction Lock Key Tests (§11)
// =============================================================================

func TestCompactionLockKeyPath(t *testing.T) {
	got := CompactionLockKeyPath("stream-123")
	want := "/dray/v1/compaction/locks/stream-123"
	if got != want {
		t.Errorf("CompactionLockKeyPath() = %q, want %q", got, want)
	}
}

func TestCompactionJobKeyPath(t *testing.T) {
	got := CompactionJobKeyPath("stream-123", "job-456")
	want := "/dray/v1/compaction/stream-123/jobs/job-456"
	if got != want {
		t.Errorf("CompactionJobKeyPath() = %q, want %q", got, want)
	}
}

func TestCompactionJobsForStreamPrefix(t *testing.T) {
	got := CompactionJobsForStreamPrefix("stream-123")
	want := "/dray/v1/compaction/stream-123/jobs/"
	if got != want {
		t.Errorf("CompactionJobsForStreamPrefix() = %q, want %q", got, want)
	}
}

func TestParseCompactionJobKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantStream string
		wantJob    string
		wantErr    bool
	}{
		{
			name:       "valid",
			key:        "/dray/v1/compaction/stream-123/jobs/job-456",
			wantStream: "stream-123",
			wantJob:    "job-456",
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/stream-123/jobs/job-456",
			wantErr: true,
		},
		{
			name:    "missing_jobs",
			key:     "/dray/v1/compaction/stream-123/job-456",
			wantErr: true,
		},
		{
			name:    "empty_stream",
			key:     "/dray/v1/compaction//jobs/job-456",
			wantErr: true,
		},
		{
			name:    "empty_job",
			key:     "/dray/v1/compaction/stream-123/jobs/",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream, job, err := ParseCompactionJobKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseCompactionJobKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseCompactionJobKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if stream != tc.wantStream || job != tc.wantJob {
				t.Errorf("ParseCompactionJobKey(%q) = (%q, %q), want (%q, %q)",
					tc.key, stream, job, tc.wantStream, tc.wantJob)
			}
		})
	}
}

// =============================================================================
// Iceberg Lock Key Tests (§11)
// =============================================================================

func TestIcebergLockKeyPath(t *testing.T) {
	got := IcebergLockKeyPath("my-topic")
	want := "/dray/v1/iceberg/my-topic/lock"
	if got != want {
		t.Errorf("IcebergLockKeyPath() = %q, want %q", got, want)
	}
}

func TestParseIcebergLockKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		wantTopic string
		wantErr   bool
	}{
		{
			name:      "valid",
			key:       "/dray/v1/iceberg/my-topic/lock",
			wantTopic: "my-topic",
		},
		{
			name:      "topic_with_dash",
			key:       "/dray/v1/iceberg/orders-v2/lock",
			wantTopic: "orders-v2",
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/my-topic/lock",
			wantErr: true,
		},
		{
			name:    "missing_lock",
			key:     "/dray/v1/iceberg/my-topic",
			wantErr: true,
		},
		{
			name:    "wrong_suffix",
			key:     "/dray/v1/iceberg/my-topic/locks",
			wantErr: true,
		},
		{
			name:    "empty_topic",
			key:     "/dray/v1/iceberg//lock",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topic, err := ParseIcebergLockKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseIcebergLockKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseIcebergLockKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if topic != tc.wantTopic {
				t.Errorf("ParseIcebergLockKey(%q) = %q, want %q", tc.key, topic, tc.wantTopic)
			}
		})
	}
}

// =============================================================================
// ACL Key Tests (§13)
// =============================================================================

func TestACLKeyPath(t *testing.T) {
	tests := []struct {
		resType   string
		resName   string
		pattern   string
		princip   string
		operation string
		permission string
		host      string
		want      string
	}{
		{"topic", "my-topic", "literal", "User:alice", "READ", "ALLOW", "*", "/dray/v1/acls/topic/my-topic/literal/User:alice/READ/ALLOW/*"},
		{"group", "consumer-group", "literal", "User:bob", "WRITE", "DENY", "*", "/dray/v1/acls/group/consumer-group/literal/User:bob/WRITE/DENY/*"},
		{"cluster", "*", "literal", "User:admin", "ALL", "ALLOW", "*", "/dray/v1/acls/cluster/*/literal/User:admin/ALL/ALLOW/*"},
	}

	for _, tc := range tests {
		got := ACLKeyPath(tc.resType, tc.resName, tc.pattern, tc.princip, tc.operation, tc.permission, tc.host)
		if got != tc.want {
			t.Errorf("ACLKeyPath(%q, %q, %q, %q, %q, %q, %q) = %q, want %q",
				tc.resType, tc.resName, tc.pattern, tc.princip, tc.operation, tc.permission, tc.host, got, tc.want)
		}
	}
}

func TestACLResourcePrefix(t *testing.T) {
	got := ACLResourcePrefix("topic", "my-topic")
	want := "/dray/v1/acls/topic/my-topic/"
	if got != want {
		t.Errorf("ACLResourcePrefix() = %q, want %q", got, want)
	}
}

func TestACLTypePrefix(t *testing.T) {
	got := ACLTypePrefix("topic")
	want := "/dray/v1/acls/topic/"
	if got != want {
		t.Errorf("ACLTypePrefix() = %q, want %q", got, want)
	}
}

func TestParseACLKey(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		wantType     string
		wantName     string
		wantPattern  string
		wantPrincip  string
		wantOperation string
		wantPermission string
		wantHost     string
		wantErr      bool
	}{
		{
			name:          "valid",
			key:           "/dray/v1/acls/topic/my-topic/literal/User:alice/READ/ALLOW/*",
			wantType:      "topic",
			wantName:      "my-topic",
			wantPattern:   "literal",
			wantPrincip:   "User:alice",
			wantOperation: "READ",
			wantPermission: "ALLOW",
			wantHost:      "*",
		},
		{
			name:          "cluster_wildcard",
			key:           "/dray/v1/acls/cluster/*/literal/User:admin/ALL/ALLOW/*",
			wantType:      "cluster",
			wantName:      "*",
			wantPattern:   "literal",
			wantPrincip:   "User:admin",
			wantOperation: "ALL",
			wantPermission: "ALLOW",
			wantHost:      "*",
		},
		{
			name:    "wrong_prefix",
			key:     "/wrong/acls/topic/my-topic/literal/User:alice/READ/ALLOW/*",
			wantErr: true,
		},
		{
			name:    "missing_principal",
			key:     "/dray/v1/acls/topic/my-topic/literal/User:alice/READ/ALLOW",
			wantErr: true,
		},
		{
			name:    "empty_type",
			key:     "/dray/v1/acls//my-topic/literal/User:alice/READ/ALLOW/*",
			wantErr: true,
		},
		{
			name:    "empty_name",
			key:     "/dray/v1/acls/topic//literal/User:alice/READ/ALLOW/*",
			wantErr: true,
		},
		{
			name:    "empty_pattern",
			key:     "/dray/v1/acls/topic/my-topic//User:alice/READ/ALLOW/*",
			wantErr: true,
		},
		{
			name:    "empty_principal",
			key:     "/dray/v1/acls/topic/my-topic/literal//READ/ALLOW/*",
			wantErr: true,
		},
		{
			name:    "empty_operation",
			key:     "/dray/v1/acls/topic/my-topic/literal/User:alice//ALLOW/*",
			wantErr: true,
		},
		{
			name:    "empty_permission",
			key:     "/dray/v1/acls/topic/my-topic/literal/User:alice/READ///*",
			wantErr: true,
		},
		{
			name:    "empty_host",
			key:     "/dray/v1/acls/topic/my-topic/literal/User:alice/READ/ALLOW/",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resType, resName, patternType, princip, operation, permission, host, err := ParseACLKey(tc.key)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseACLKey(%q) expected error", tc.key)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseACLKey(%q) unexpected error: %v", tc.key, err)
				return
			}
			if resType != tc.wantType || resName != tc.wantName || patternType != tc.wantPattern ||
				princip != tc.wantPrincip || operation != tc.wantOperation || permission != tc.wantPermission || host != tc.wantHost {
				t.Errorf("ParseACLKey(%q) = (%q, %q, %q, %q, %q, %q, %q), want (%q, %q, %q, %q, %q, %q, %q)",
					tc.key, resType, resName, patternType, princip, operation, permission, host,
					tc.wantType, tc.wantName, tc.wantPattern, tc.wantPrincip, tc.wantOperation, tc.wantPermission, tc.wantHost)
			}
		})
	}
}

// =============================================================================
// Round-trip Tests for New Key Types
// =============================================================================

func TestBrokerKeyRoundTrip(t *testing.T) {
	clusterID := "prod-cluster"
	brokerID := "broker-xyz-123"
	key := BrokerKeyPath(clusterID, brokerID)
	parsedCluster, parsedBroker, err := ParseBrokerKey(key)
	if err != nil {
		t.Fatalf("ParseBrokerKey failed: %v", err)
	}
	if parsedCluster != clusterID || parsedBroker != brokerID {
		t.Errorf("Round trip failed: got (%q, %q), want (%q, %q)",
			parsedCluster, parsedBroker, clusterID, brokerID)
	}
}

func TestTopicPartitionKeyRoundTrip(t *testing.T) {
	topic := "my-orders"
	partition := int32(42)
	key := TopicPartitionKeyPath(topic, partition)
	parsedTopic, parsedPart, err := ParseTopicPartitionKey(key)
	if err != nil {
		t.Fatalf("ParseTopicPartitionKey failed: %v", err)
	}
	if parsedTopic != topic || parsedPart != partition {
		t.Errorf("Round trip failed: got (%q, %d), want (%q, %d)",
			parsedTopic, parsedPart, topic, partition)
	}
}

func TestGroupOffsetKeyRoundTrip(t *testing.T) {
	groupID := "consumer-group-1"
	topic := "events"
	partition := int32(7)
	key := GroupOffsetKeyPath(groupID, topic, partition)
	parsedGroup, parsedTopic, parsedPart, err := ParseGroupOffsetKey(key)
	if err != nil {
		t.Fatalf("ParseGroupOffsetKey failed: %v", err)
	}
	if parsedGroup != groupID || parsedTopic != topic || parsedPart != partition {
		t.Errorf("Round trip failed: got (%q, %q, %d), want (%q, %q, %d)",
			parsedGroup, parsedTopic, parsedPart, groupID, topic, partition)
	}
}

func TestWALStagingKeyRoundTrip(t *testing.T) {
	metaDomain := 12
	walID := "wal-uuid-abc-123"
	key := WALStagingKeyPath(metaDomain, walID)
	parsedDomain, parsedWAL, err := ParseWALStagingKey(key)
	if err != nil {
		t.Fatalf("ParseWALStagingKey failed: %v", err)
	}
	if parsedDomain != metaDomain || parsedWAL != walID {
		t.Errorf("Round trip failed: got (%d, %q), want (%d, %q)",
			parsedDomain, parsedWAL, metaDomain, walID)
	}
}

func TestCompactionJobKeyRoundTrip(t *testing.T) {
	streamID := "stream-abc"
	jobID := "job-xyz-456"
	key := CompactionJobKeyPath(streamID, jobID)
	parsedStream, parsedJob, err := ParseCompactionJobKey(key)
	if err != nil {
		t.Fatalf("ParseCompactionJobKey failed: %v", err)
	}
	if parsedStream != streamID || parsedJob != jobID {
		t.Errorf("Round trip failed: got (%q, %q), want (%q, %q)",
			parsedStream, parsedJob, streamID, jobID)
	}
}

func TestIcebergLockKeyRoundTrip(t *testing.T) {
	topic := "iceberg-topic-123"
	key := IcebergLockKeyPath(topic)
	parsedTopic, err := ParseIcebergLockKey(key)
	if err != nil {
		t.Fatalf("ParseIcebergLockKey failed: %v", err)
	}
	if parsedTopic != topic {
		t.Errorf("Round trip failed: got %q, want %q", parsedTopic, topic)
	}
}

func TestACLKeyRoundTrip(t *testing.T) {
	resType := "topic"
	resName := "my-topic"
	patternType := "literal"
	principal := "User:alice"
	operation := "READ"
	permission := "ALLOW"
	host := "*"
	key := ACLKeyPath(resType, resName, patternType, principal, operation, permission, host)
	parsedType, parsedName, parsedPattern, parsedPrincipal, parsedOperation, parsedPermission, parsedHost, err := ParseACLKey(key)
	if err != nil {
		t.Fatalf("ParseACLKey failed: %v", err)
	}
	if parsedType != resType || parsedName != resName || parsedPattern != patternType ||
		parsedPrincipal != principal || parsedOperation != operation || parsedPermission != permission || parsedHost != host {
		t.Errorf("Round trip failed: got (%q, %q, %q, %q, %q, %q, %q), want (%q, %q, %q, %q, %q, %q, %q)",
			parsedType, parsedName, parsedPattern, parsedPrincipal, parsedOperation, parsedPermission, parsedHost,
			resType, resName, patternType, principal, operation, permission, host)
	}
}
