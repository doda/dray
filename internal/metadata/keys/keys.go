// Package keys provides key encoding/decoding for the Oxia keyspace.
// Keys use zero-padded numeric encoding for lexicographic ordering.
//
// Per SPEC.md section 6.3, offset index keys are formatted as:
//
//	/dray/v1/streams/<streamId>/offset-index/<offsetEndZ>/<cumulativeSizeZ>
//
// where offsetEndZ and cumulativeSizeZ are zero-padded decimal width 20
// to preserve lexicographic ordering for numeric comparisons.
package keys

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Key component widths for zero-padded encoding.
const (
	// OffsetWidth is the number of digits for zero-padded offsets.
	// Width 20 supports values up to 99999999999999999999 (20 nines),
	// which is well beyond the max int64 value (9223372036854775807).
	OffsetWidth = 20

	// SizeWidth is the number of digits for zero-padded sizes.
	SizeWidth = 20
)

// Key prefixes.
const (
	// Prefix is the root prefix for all Dray keys.
	Prefix = "/dray/v1"

	// StreamsPrefix is the prefix for stream-related keys.
	StreamsPrefix = Prefix + "/streams"

	// TopicsPrefix is the prefix for topic metadata.
	TopicsPrefix = Prefix + "/topics"

	// ClusterPrefix is the prefix for cluster metadata.
	ClusterPrefix = Prefix + "/cluster"
)

// Common errors.
var (
	// ErrInvalidKey is returned when a key cannot be parsed.
	ErrInvalidKey = errors.New("keys: invalid key format")

	// ErrInvalidOffset is returned when an offset value is negative.
	ErrInvalidOffset = errors.New("keys: offset must be non-negative")

	// ErrInvalidSize is returned when a size value is negative.
	ErrInvalidSize = errors.New("keys: size must be non-negative")
)

// EncodeUint64 encodes an unsigned 64-bit integer as a zero-padded
// decimal string of the specified width for lexicographic ordering.
func EncodeUint64(v uint64, width int) string {
	return fmt.Sprintf("%0*d", width, v)
}

// DecodeUint64 decodes a zero-padded decimal string back to uint64.
// Leading zeros are handled correctly by strconv.ParseUint.
func DecodeUint64(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

// EncodeInt64 encodes a signed 64-bit integer as a zero-padded
// decimal string. Negative values are not supported and return an error.
func EncodeInt64(v int64, width int) (string, error) {
	if v < 0 {
		return "", fmt.Errorf("keys: negative value %d not supported", v)
	}
	return fmt.Sprintf("%0*d", width, v), nil
}

// DecodeInt64 decodes a zero-padded decimal string back to int64.
func DecodeInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// EncodeOffsetEnd encodes an offset end value (exclusive upper bound)
// for use in offset index keys.
func EncodeOffsetEnd(offsetEnd int64) (string, error) {
	if offsetEnd < 0 {
		return "", ErrInvalidOffset
	}
	return fmt.Sprintf("%0*d", OffsetWidth, offsetEnd), nil
}

// EncodeCumulativeSize encodes a cumulative size value for use in
// offset index keys.
func EncodeCumulativeSize(size int64) (string, error) {
	if size < 0 {
		return "", ErrInvalidSize
	}
	return fmt.Sprintf("%0*d", SizeWidth, size), nil
}

// OffsetIndexKey represents a parsed offset index key.
type OffsetIndexKey struct {
	StreamID       string
	OffsetEnd      int64
	CumulativeSize int64
}

// OffsetIndexKeyPath builds an offset index key path for the given parameters.
// The key format is: /dray/v1/streams/<streamId>/offset-index/<offsetEndZ>/<cumulativeSizeZ>
func OffsetIndexKeyPath(streamID string, offsetEnd, cumulativeSize int64) (string, error) {
	offsetEndZ, err := EncodeOffsetEnd(offsetEnd)
	if err != nil {
		return "", err
	}
	cumulativeSizeZ, err := EncodeCumulativeSize(cumulativeSize)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/offset-index/%s/%s", StreamsPrefix, streamID, offsetEndZ, cumulativeSizeZ), nil
}

// OffsetIndexPrefix returns the prefix for listing all offset index entries
// for a stream.
func OffsetIndexPrefix(streamID string) string {
	return fmt.Sprintf("%s/%s/offset-index/", StreamsPrefix, streamID)
}

// OffsetIndexStartKey returns a key for listing offset index entries
// starting at or after the given offset.
func OffsetIndexStartKey(streamID string, offsetEnd int64) (string, error) {
	offsetEndZ, err := EncodeOffsetEnd(offsetEnd)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/offset-index/%s/", StreamsPrefix, streamID, offsetEndZ), nil
}

// ParseOffsetIndexKey parses an offset index key into its components.
// Returns ErrInvalidKey if the key is not a valid offset index key.
func ParseOffsetIndexKey(key string) (OffsetIndexKey, error) {
	// Expected format: /dray/v1/streams/<streamId>/offset-index/<offsetEndZ>/<cumulativeSizeZ>
	prefix := StreamsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return OffsetIndexKey{}, ErrInvalidKey
	}

	// Remove prefix
	rest := key[len(prefix):]

	// Split by /offset-index/
	parts := strings.SplitN(rest, "/offset-index/", 2)
	if len(parts) != 2 {
		return OffsetIndexKey{}, ErrInvalidKey
	}

	streamID := parts[0]
	if streamID == "" {
		return OffsetIndexKey{}, ErrInvalidKey
	}

	// Split the suffix into offsetEndZ and cumulativeSizeZ
	suffixParts := strings.Split(parts[1], "/")
	if len(suffixParts) != 2 {
		return OffsetIndexKey{}, ErrInvalidKey
	}

	offsetEnd, err := DecodeInt64(suffixParts[0])
	if err != nil {
		return OffsetIndexKey{}, fmt.Errorf("%w: invalid offsetEnd: %v", ErrInvalidKey, err)
	}
	if offsetEnd < 0 {
		return OffsetIndexKey{}, ErrInvalidOffset
	}

	cumulativeSize, err := DecodeInt64(suffixParts[1])
	if err != nil {
		return OffsetIndexKey{}, fmt.Errorf("%w: invalid cumulativeSize: %v", ErrInvalidKey, err)
	}
	if cumulativeSize < 0 {
		return OffsetIndexKey{}, ErrInvalidSize
	}

	return OffsetIndexKey{
		StreamID:       streamID,
		OffsetEnd:      offsetEnd,
		CumulativeSize: cumulativeSize,
	}, nil
}

// HwmKeyPath returns the high water mark key for a stream.
func HwmKeyPath(streamID string) string {
	return fmt.Sprintf("%s/%s/hwm", StreamsPrefix, streamID)
}

// TopicKeyPath returns the key for topic metadata.
func TopicKeyPath(topicName string) string {
	return fmt.Sprintf("%s/%s", TopicsPrefix, topicName)
}

// StreamMetaKeyPath returns the key for stream metadata.
func StreamMetaKeyPath(streamID string) string {
	return fmt.Sprintf("%s/%s/meta", StreamsPrefix, streamID)
}
