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

	// GroupsPrefix is the prefix for consumer group keys.
	GroupsPrefix = Prefix + "/groups"
	// GroupsStatePrefix is the prefix for group state listing keys.
	// Format: /dray/v1/groups-state/<groupId>
	GroupsStatePrefix = Prefix + "/groups-state"

	// ACLsPrefix is the prefix for ACL entries.
	ACLsPrefix = Prefix + "/acls"

	// WALStagingPrefix is the prefix for WAL staging markers.
	// Format: /dray/v1/wal/staging/<metaDomain>/<walId>
	WALStagingPrefix = Prefix + "/wal/staging"

	// WALObjectsPrefix is the prefix for WAL object records.
	// Format: /dray/v1/wal/objects/<metaDomain>/<walId>
	WALObjectsPrefix = Prefix + "/wal/objects"

	// WALGCPrefix is the prefix for WAL GC markers.
	// Format: /dray/v1/wal/gc/<metaDomain>/<walId>
	WALGCPrefix = Prefix + "/wal/gc"

	// ParquetGCPrefix is the prefix for Parquet GC markers.
	// Format: /dray/v1/parquet/gc/<streamId>/<parquetId>
	ParquetGCPrefix = Prefix + "/parquet/gc"

	// CompactionLocksPrefix is the prefix for compaction stream locks (ephemeral).
	// Format: /dray/v1/compaction/locks/<streamId>
	CompactionLocksPrefix = Prefix + "/compaction/locks"

	// CompactionJobsPrefix is the prefix for compaction job state.
	// Format: /dray/v1/compaction/<streamId>/jobs/<jobId>
	CompactionJobsPrefix = Prefix + "/compaction"

	// IcebergLocksPrefix is the prefix for Iceberg commit locks (ephemeral).
	// Format: /dray/v1/iceberg/<topic>/lock
	IcebergLocksPrefix = Prefix + "/iceberg"
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
//
// IMPORTANT: Oxia uses hierarchical key sorting that groups keys by their
// path depth (number of '/' segments). To make range queries work correctly,
// the startKey must have the same number of segments as the actual keys.
// Offset index keys have format: .../offset-index/<offsetEndZ>/<cumulativeSizeZ>
// So we include a minimum cumulativeSize (0) to match the segment count.
func OffsetIndexStartKey(streamID string, offsetEnd int64) (string, error) {
	offsetEndZ, err := EncodeOffsetEnd(offsetEnd)
	if err != nil {
		return "", err
	}
	// Use minimum cumulativeSize (0) to get keys >= this offset
	minCumulativeSize := EncodeUint64(0, SizeWidth)
	return fmt.Sprintf("%s/%s/offset-index/%s/%s", StreamsPrefix, streamID, offsetEndZ, minCumulativeSize), nil
}

// OffsetIndexEndKey returns the end key for listing offset index entries.
// This is used as the exclusive upper bound for range queries.
//
// IMPORTANT: Oxia uses hierarchical key sorting that groups keys by their
// path depth. The endKey must have the same number of segments as startKey.
func OffsetIndexEndKey(streamID string) string {
	// Use maximum possible values for offsetEnd and cumulativeSize
	// to capture all keys in the offset-index namespace.
	maxOffset := EncodeUint64(^uint64(0), OffsetWidth)      // All 9s
	maxSize := EncodeUint64(^uint64(0), SizeWidth)          // All 9s
	return fmt.Sprintf("%s/%s/offset-index/%s/%s", StreamsPrefix, streamID, maxOffset, maxSize)
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

// =============================================================================
// Cluster and Broker Keys (§6.3.1)
// =============================================================================

// BrokerKeyPath returns the key for a broker registration (ephemeral).
// Format: /dray/v1/cluster/<clusterId>/brokers/<brokerId>
func BrokerKeyPath(clusterID, brokerID string) string {
	return fmt.Sprintf("%s/%s/brokers/%s", ClusterPrefix, clusterID, brokerID)
}

// BrokersPrefix returns the prefix for listing all brokers in a cluster.
func BrokersPrefix(clusterID string) string {
	return fmt.Sprintf("%s/%s/brokers/", ClusterPrefix, clusterID)
}

// ClusterKeyPath returns the key for cluster metadata.
func ClusterKeyPath(clusterID string) string {
	return fmt.Sprintf("%s/%s", ClusterPrefix, clusterID)
}

// ParseBrokerKey parses a broker key into its components.
// Returns ErrInvalidKey if the key is not a valid broker key.
func ParseBrokerKey(key string) (clusterID, brokerID string, err error) {
	prefix := ClusterPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.Split(rest, "/brokers/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", ErrInvalidKey
	}

	return parts[0], parts[1], nil
}

// =============================================================================
// Topic and Partition Keys (§6.3.2)
// =============================================================================

// TopicPartitionKeyPath returns the key for a topic partition.
// Format: /dray/v1/topics/<topicName>/partitions/<p>
func TopicPartitionKeyPath(topicName string, partition int32) string {
	return fmt.Sprintf("%s/%s/partitions/%d", TopicsPrefix, topicName, partition)
}

// TopicPartitionsPrefix returns the prefix for listing all partitions of a topic.
func TopicPartitionsPrefix(topicName string) string {
	return fmt.Sprintf("%s/%s/partitions/", TopicsPrefix, topicName)
}

// ParseTopicPartitionKey parses a topic partition key.
func ParseTopicPartitionKey(key string) (topicName string, partition int32, err error) {
	prefix := TopicsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", 0, ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.Split(rest, "/partitions/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", 0, ErrInvalidKey
	}

	p, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil || p < 0 {
		return "", 0, fmt.Errorf("%w: invalid partition number", ErrInvalidKey)
	}

	return parts[0], int32(p), nil
}

// =============================================================================
// Compaction Task Keys (§6.3.5)
// =============================================================================

// CompactionTaskKeyPath returns the key for a compaction task.
// Format: /dray/v1/streams/<streamId>/compaction/tasks/<taskId>
func CompactionTaskKeyPath(streamID, taskID string) string {
	return fmt.Sprintf("%s/%s/compaction/tasks/%s", StreamsPrefix, streamID, taskID)
}

// CompactionTasksPrefix returns the prefix for listing all compaction tasks for a stream.
func CompactionTasksPrefix(streamID string) string {
	return fmt.Sprintf("%s/%s/compaction/tasks/", StreamsPrefix, streamID)
}

// ParseCompactionTaskKey parses a compaction task key.
func ParseCompactionTaskKey(key string) (streamID, taskID string, err error) {
	prefix := StreamsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.Split(rest, "/compaction/tasks/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", ErrInvalidKey
	}

	return parts[0], parts[1], nil
}

// =============================================================================
// Consumer Group Keys (§6.3.6)
// =============================================================================

// GroupStateKeyPath returns the key for consumer group state.
// Format: /dray/v1/groups/<groupId>/state
func GroupStateKeyPath(groupID string) string {
	return fmt.Sprintf("%s/%s/state", GroupsPrefix, groupID)
}

// GroupStateListKeyPath returns the key for listing consumer group states.
// Format: /dray/v1/groups-state/<groupId>
func GroupStateListKeyPath(groupID string) string {
	return fmt.Sprintf("%s/%s", GroupsStatePrefix, groupID)
}

// GroupStateListPrefix returns the prefix for listing all group state keys.
func GroupStateListPrefix() string {
	return GroupsStatePrefix + "/"
}

// GroupTypeKeyPath returns the key for consumer group type (classic|consumer).
// Format: /dray/v1/groups/<groupId>/type
func GroupTypeKeyPath(groupID string) string {
	return fmt.Sprintf("%s/%s/type", GroupsPrefix, groupID)
}

// GroupMemberKeyPath returns the key for a consumer group member.
// Format: /dray/v1/groups/<groupId>/members/<memberId>
func GroupMemberKeyPath(groupID, memberID string) string {
	return fmt.Sprintf("%s/%s/members/%s", GroupsPrefix, groupID, memberID)
}

// GroupMembersPrefix returns the prefix for listing all members of a group.
func GroupMembersPrefix(groupID string) string {
	return fmt.Sprintf("%s/%s/members/", GroupsPrefix, groupID)
}

// GroupMembersCountKeyPath returns the key for tracking member count in a group.
// Format: /dray/v1/groups/<groupId>/members-count
func GroupMembersCountKeyPath(groupID string) string {
	return fmt.Sprintf("%s/%s/members-count", GroupsPrefix, groupID)
}

// ParseGroupMemberKey parses a group member key.
func ParseGroupMemberKey(key string) (groupID, memberID string, err error) {
	prefix := GroupsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.Split(rest, "/members/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", ErrInvalidKey
	}

	return parts[0], parts[1], nil
}

// GroupAssignmentKeyPath returns the key for a member's assignment.
// Format: /dray/v1/groups/<groupId>/assignment/<memberId>
func GroupAssignmentKeyPath(groupID, memberID string) string {
	return fmt.Sprintf("%s/%s/assignment/%s", GroupsPrefix, groupID, memberID)
}

// GroupAssignmentsPrefix returns the prefix for listing all assignments in a group.
func GroupAssignmentsPrefix(groupID string) string {
	return fmt.Sprintf("%s/%s/assignment/", GroupsPrefix, groupID)
}

// GroupOffsetKeyPath returns the key for a committed offset.
// Format: /dray/v1/groups/<groupId>/offsets/<topic>/<partition>
func GroupOffsetKeyPath(groupID, topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/offsets/%s/%d", GroupsPrefix, groupID, topic, partition)
}

// GroupOffsetsPrefix returns the prefix for listing all offsets for a group.
func GroupOffsetsPrefix(groupID string) string {
	return fmt.Sprintf("%s/%s/offsets/", GroupsPrefix, groupID)
}

// GroupTopicOffsetsPrefix returns the prefix for listing all offsets for a topic in a group.
func GroupTopicOffsetsPrefix(groupID, topic string) string {
	return fmt.Sprintf("%s/%s/offsets/%s/", GroupsPrefix, groupID, topic)
}

// ParseGroupOffsetKey parses a group offset key.
func ParseGroupOffsetKey(key string) (groupID, topic string, partition int32, err error) {
	prefix := GroupsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", 0, ErrInvalidKey
	}

	rest := key[len(prefix):]

	// Find /offsets/ separator
	offsIdx := strings.Index(rest, "/offsets/")
	if offsIdx == -1 {
		return "", "", 0, ErrInvalidKey
	}

	groupID = rest[:offsIdx]
	if groupID == "" {
		return "", "", 0, ErrInvalidKey
	}

	// Parse topic/partition
	remaining := rest[offsIdx+len("/offsets/"):]
	lastSlash := strings.LastIndex(remaining, "/")
	if lastSlash == -1 || lastSlash == 0 || lastSlash == len(remaining)-1 {
		return "", "", 0, ErrInvalidKey
	}

	topic = remaining[:lastSlash]
	partStr := remaining[lastSlash+1:]

	p, err := strconv.ParseInt(partStr, 10, 32)
	if err != nil || p < 0 {
		return "", "", 0, fmt.Errorf("%w: invalid partition number", ErrInvalidKey)
	}

	return groupID, topic, int32(p), nil
}

// GroupKeyPath returns the key for a consumer group.
func GroupKeyPath(groupID string) string {
	return fmt.Sprintf("%s/%s", GroupsPrefix, groupID)
}

// GroupLeaseKeyPath returns the key for a consumer group coordinator lease.
// Format: /dray/v1/groups/<groupId>/lease
// This key is ephemeral - tied to the broker's session.
func GroupLeaseKeyPath(groupID string) string {
	return fmt.Sprintf("%s/%s/lease", GroupsPrefix, groupID)
}

// ParseGroupLeaseKey parses a group lease key and returns the groupID.
func ParseGroupLeaseKey(key string) (groupID string, err error) {
	prefix := GroupsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	if !strings.HasSuffix(rest, "/lease") {
		return "", ErrInvalidKey
	}

	groupID = rest[:len(rest)-len("/lease")]
	if groupID == "" {
		return "", ErrInvalidKey
	}

	return groupID, nil
}

// =============================================================================
// WAL Keys (§9)
// =============================================================================

// WALStagingKeyPath returns the key for a WAL staging marker.
// Format: /dray/v1/wal/staging/<metaDomain>/<walId>
func WALStagingKeyPath(metaDomain int, walID string) string {
	return fmt.Sprintf("%s/%d/%s", WALStagingPrefix, metaDomain, walID)
}

// WALStagingDomainPrefix returns the prefix for listing all staging markers in a domain.
func WALStagingDomainPrefix(metaDomain int) string {
	return fmt.Sprintf("%s/%d/", WALStagingPrefix, metaDomain)
}

// ParseWALStagingKey parses a WAL staging key.
func ParseWALStagingKey(key string) (metaDomain int, walID string, err error) {
	prefix := WALStagingPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return 0, "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, "", ErrInvalidKey
	}

	domain, err := strconv.Atoi(parts[0])
	if err != nil || domain < 0 {
		return 0, "", fmt.Errorf("%w: invalid meta domain", ErrInvalidKey)
	}

	return domain, parts[1], nil
}

// WALObjectKeyPath returns the key for a WAL object record.
// Format: /dray/v1/wal/objects/<metaDomain>/<walId>
func WALObjectKeyPath(metaDomain int, walID string) string {
	return fmt.Sprintf("%s/%d/%s", WALObjectsPrefix, metaDomain, walID)
}

// WALObjectsDomainPrefix returns the prefix for listing all WAL objects in a domain.
func WALObjectsDomainPrefix(metaDomain int) string {
	return fmt.Sprintf("%s/%d/", WALObjectsPrefix, metaDomain)
}

// ParseWALObjectKey parses a WAL object key.
func ParseWALObjectKey(key string) (metaDomain int, walID string, err error) {
	prefix := WALObjectsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return 0, "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, "", ErrInvalidKey
	}

	domain, err := strconv.Atoi(parts[0])
	if err != nil || domain < 0 {
		return 0, "", fmt.Errorf("%w: invalid meta domain", ErrInvalidKey)
	}

	return domain, parts[1], nil
}

// WALGCKeyPath returns the key for a WAL GC marker.
// Format: /dray/v1/wal/gc/<metaDomain>/<walId>
func WALGCKeyPath(metaDomain int, walID string) string {
	return fmt.Sprintf("%s/%d/%s", WALGCPrefix, metaDomain, walID)
}

// WALGCDomainPrefix returns the prefix for listing all WAL GC markers in a domain.
func WALGCDomainPrefix(metaDomain int) string {
	return fmt.Sprintf("%s/%d/", WALGCPrefix, metaDomain)
}

// ParseWALGCKey parses a WAL GC key.
func ParseWALGCKey(key string) (metaDomain int, walID string, err error) {
	prefix := WALGCPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return 0, "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, "", ErrInvalidKey
	}

	domain, err := strconv.Atoi(parts[0])
	if err != nil || domain < 0 {
		return 0, "", fmt.Errorf("%w: invalid meta domain", ErrInvalidKey)
	}

	return domain, parts[1], nil
}

// =============================================================================
// Parquet GC Keys
// =============================================================================

// ParquetGCKeyPath returns the key for a Parquet GC marker.
// Format: /dray/v1/parquet/gc/<streamId>/<parquetId>
func ParquetGCKeyPath(streamID, parquetID string) string {
	return fmt.Sprintf("%s/%s/%s", ParquetGCPrefix, streamID, parquetID)
}

// ParquetGCStreamPrefix returns the prefix for listing all Parquet GC markers for a stream.
func ParquetGCStreamPrefix(streamID string) string {
	return fmt.Sprintf("%s/%s/", ParquetGCPrefix, streamID)
}

// ParquetGCAllPrefix returns the prefix for listing all Parquet GC markers.
func ParquetGCAllPrefix() string {
	return ParquetGCPrefix + "/"
}

// ParseParquetGCKey parses a Parquet GC key.
func ParseParquetGCKey(key string) (streamID, parquetID string, err error) {
	prefix := ParquetGCPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", ErrInvalidKey
	}

	return parts[0], parts[1], nil
}

// =============================================================================
// Compaction Lock Keys (§11)
// =============================================================================

// CompactionLockKeyPath returns the key for a compaction stream lock (ephemeral).
// Format: /dray/v1/compaction/locks/<streamId>
func CompactionLockKeyPath(streamID string) string {
	return fmt.Sprintf("%s/%s", CompactionLocksPrefix, streamID)
}

// CompactionJobKeyPath returns the key for a compaction job state.
// Format: /dray/v1/compaction/<streamId>/jobs/<jobId>
func CompactionJobKeyPath(streamID, jobID string) string {
	return fmt.Sprintf("%s/%s/jobs/%s", CompactionJobsPrefix, streamID, jobID)
}

// CompactionJobsForStreamPrefix returns the prefix for listing all jobs for a stream.
func CompactionJobsForStreamPrefix(streamID string) string {
	return fmt.Sprintf("%s/%s/jobs/", CompactionJobsPrefix, streamID)
}

// ParseCompactionJobKey parses a compaction job key.
func ParseCompactionJobKey(key string) (streamID, jobID string, err error) {
	prefix := CompactionJobsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.Split(rest, "/jobs/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", ErrInvalidKey
	}

	return parts[0], parts[1], nil
}

// =============================================================================
// Iceberg Lock Keys (§11)
// =============================================================================

// IcebergLockKeyPath returns the key for an Iceberg commit lock (ephemeral).
// Format: /dray/v1/iceberg/<topic>/lock
func IcebergLockKeyPath(topic string) string {
	return fmt.Sprintf("%s/%s/lock", IcebergLocksPrefix, topic)
}

// ParseIcebergLockKey parses an Iceberg lock key.
func ParseIcebergLockKey(key string) (topic string, err error) {
	prefix := IcebergLocksPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	if !strings.HasSuffix(rest, "/lock") {
		return "", ErrInvalidKey
	}

	topic = rest[:len(rest)-len("/lock")]
	if topic == "" {
		return "", ErrInvalidKey
	}

	return topic, nil
}

// =============================================================================
// ACL Keys (§13)
// =============================================================================

// ACLKeyPath returns the key for an ACL entry.
// Format: /dray/v1/acls/<resourceType>/<resourceName>/<patternType>/<principal>/<operation>/<permission>/<host>
func ACLKeyPath(resourceType, resourceName, patternType, principal, operation, permission, host string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s/%s",
		ACLsPrefix,
		resourceType,
		resourceName,
		patternType,
		principal,
		operation,
		permission,
		host,
	)
}

// ACLResourcePrefix returns the prefix for listing all ACLs for a resource.
func ACLResourcePrefix(resourceType, resourceName string) string {
	return fmt.Sprintf("%s/%s/%s/", ACLsPrefix, resourceType, resourceName)
}

// ACLTypePrefix returns the prefix for listing all ACLs for a resource type.
func ACLTypePrefix(resourceType string) string {
	return fmt.Sprintf("%s/%s/", ACLsPrefix, resourceType)
}

// ParseACLKey parses an ACL key.
func ParseACLKey(key string) (resourceType, resourceName, patternType, principal, operation, permission, host string, err error) {
	prefix := ACLsPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", "", "", "", "", "", ErrInvalidKey
	}

	rest := key[len(prefix):]
	parts := strings.Split(rest, "/")
	if len(parts) != 7 {
		return "", "", "", "", "", "", "", ErrInvalidKey
	}
	for _, part := range parts {
		if part == "" {
			return "", "", "", "", "", "", "", ErrInvalidKey
		}
	}

	return parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], nil
}
