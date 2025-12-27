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
}
