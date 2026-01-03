// Package index implements offset index operations for stream lookup.
// The index maps logical offsets to WAL/Parquet object locations.
package index

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/google/uuid"
)

// Common errors for stream operations.
var (
	// ErrStreamExists is returned when creating a stream that already exists.
	ErrStreamExists = errors.New("index: stream already exists")

	// ErrStreamNotFound is returned when a stream does not exist.
	ErrStreamNotFound = errors.New("index: stream not found")
)

// StreamMeta contains metadata about a stream.
type StreamMeta struct {
	// StreamID is the unique identifier for the stream.
	StreamID string `json:"streamId"`
	// TopicName is the Kafka topic this stream belongs to.
	TopicName string `json:"topicName"`
	// Partition is the partition number within the topic.
	Partition int32 `json:"partition"`
	// CreatedAt is the timestamp when the stream was created.
	CreatedAt time.Time `json:"createdAt"`
}

// StreamManager handles stream creation and management.
type StreamManager struct {
	store            metadata.MetadataStore
	timestampScanner TimestampScanner
	hwmCache         *HWMCache
	indexCache       *IndexCache
}

// NewStreamManager creates a new StreamManager with the given metadata store.
func NewStreamManager(store metadata.MetadataStore) *StreamManager {
	return &StreamManager{store: store}
}

// SetTimestampScanner configures a scanner used for timestamp-based offset lookups
// when batch index data is unavailable.
func (sm *StreamManager) SetTimestampScanner(scanner TimestampScanner) {
	sm.timestampScanner = scanner
}

// SetHWMCache configures the HWM cache for faster high watermark lookups.
// When set, GetHWM will use the cache to avoid metadata store hits.
func (sm *StreamManager) SetHWMCache(cache *HWMCache) {
	sm.hwmCache = cache
}

// SetIndexCache configures the index cache for faster offset lookups.
// When set, LookupOffset will use cached entries when available.
func (sm *StreamManager) SetIndexCache(cache *IndexCache) {
	sm.indexCache = cache
}

// CreateStream creates a new stream with a generated UUID.
// It atomically:
//   - Generates a new UUID for the stream
//   - Creates /dray/v1/streams/<streamId>/hwm with value 0
//   - Stores stream metadata at /dray/v1/streams/<streamId>/meta
//
// Returns the new stream ID or an error if the stream already exists.
func (sm *StreamManager) CreateStream(ctx context.Context, topicName string, partition int32) (string, error) {
	streamID := uuid.New().String()

	now := time.Now().UTC()
	meta := StreamMeta{
		StreamID:  streamID,
		TopicName: topicName,
		Partition: partition,
		CreatedAt: now,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}

	hwmKey := keys.HwmKeyPath(streamID)
	metaKey := keys.StreamMetaKeyPath(streamID)

	// Encode HWM as int64 (0)
	hwmBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hwmBytes, 0)

	// Use a transaction to atomically create both the hwm and meta keys.
	// The scopeKey is the stream prefix to ensure all operations are in the same shard.
	err = sm.store.Txn(ctx, hwmKey, func(txn metadata.Txn) error {
		// Check if stream already exists by checking hwm key
		_, _, err := txn.Get(hwmKey)
		if err == nil {
			return ErrStreamExists
		}
		if !errors.Is(err, metadata.ErrKeyNotFound) {
			return err
		}

		// Create hwm with initial value 0
		txn.Put(hwmKey, hwmBytes)
		// Store stream metadata
		txn.Put(metaKey, metaBytes)

		return nil
	})

	if err != nil {
		return "", err
	}

	return streamID, nil
}

// CreateStreamWithID creates a new stream with the specified stream ID.
// This is useful when the stream ID is already known (e.g., from partition metadata).
// It atomically:
//   - Creates /dray/v1/streams/<streamId>/hwm with value 0
//   - Stores stream metadata at /dray/v1/streams/<streamId>/meta
//
// Returns an error if the stream already exists.
func (sm *StreamManager) CreateStreamWithID(ctx context.Context, streamID, topicName string, partition int32) error {
	now := time.Now().UTC()
	meta := StreamMeta{
		StreamID:  streamID,
		TopicName: topicName,
		Partition: partition,
		CreatedAt: now,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	hwmKey := keys.HwmKeyPath(streamID)
	metaKey := keys.StreamMetaKeyPath(streamID)

	// Encode HWM as int64 (0)
	hwmBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hwmBytes, 0)

	// Use a transaction to atomically create both the hwm and meta keys.
	err = sm.store.Txn(ctx, hwmKey, func(txn metadata.Txn) error {
		// Check if stream already exists by checking hwm key
		_, _, err := txn.Get(hwmKey)
		if err == nil {
			return ErrStreamExists
		}
		if !errors.Is(err, metadata.ErrKeyNotFound) {
			return err
		}

		// Create hwm with initial value 0
		txn.Put(hwmKey, hwmBytes)
		// Store stream metadata
		txn.Put(metaKey, metaBytes)

		return nil
	})

	return err
}

// GetStreamMeta retrieves the metadata for a stream.
func (sm *StreamManager) GetStreamMeta(ctx context.Context, streamID string) (*StreamMeta, error) {
	metaKey := keys.StreamMetaKeyPath(streamID)
	result, err := sm.store.Get(ctx, metaKey)
	if err != nil {
		return nil, err
	}
	if !result.Exists {
		return nil, ErrStreamNotFound
	}

	var meta StreamMeta
	if err := json.Unmarshal(result.Value, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

// GetHWM retrieves the high watermark for a stream.
// The HWM is the log end offset (exclusive upper bound of committed offsets).
// If an HWMCache is configured, it uses the cache to avoid metadata store hits.
func (sm *StreamManager) GetHWM(ctx context.Context, streamID string) (int64, metadata.Version, error) {
	// Use cache if available
	if sm.hwmCache != nil {
		return sm.hwmCache.Get(ctx, streamID)
	}

	// Fall back to direct store access
	hwmKey := keys.HwmKeyPath(streamID)
	result, err := sm.store.Get(ctx, hwmKey)
	if err != nil {
		return 0, 0, err
	}
	if !result.Exists {
		return 0, 0, ErrStreamNotFound
	}

	if len(result.Value) != 8 {
		return 0, 0, errors.New("index: invalid hwm encoding")
	}

	hwm := int64(binary.BigEndian.Uint64(result.Value))
	return hwm, result.Version, nil
}

// EncodeHWM encodes a high watermark value as bytes for storage.
func EncodeHWM(hwm int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(hwm))
	return b
}

// DecodeHWM decodes a high watermark value from bytes.
func DecodeHWM(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, errors.New("index: invalid hwm encoding")
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// IncrementHWM atomically increments the high watermark for a stream by delta.
// It uses CAS (compare-and-set) semantics to ensure atomic updates:
//   - Reads the current hwm with its version
//   - Increments by delta
//   - Writes back with version checking
//
// Returns the new hwm value after increment, or an error if the operation fails.
// The expectedVersion parameter specifies the version we expect the hwm to be at;
// if it doesn't match, ErrVersionMismatch is returned and the caller should retry.
func (sm *StreamManager) IncrementHWM(ctx context.Context, streamID string, delta int64, expectedVersion metadata.Version) (int64, metadata.Version, error) {
	hwmKey := keys.HwmKeyPath(streamID)
	var newHwm int64
	var newVersion metadata.Version

	err := sm.store.Txn(ctx, hwmKey, func(txn metadata.Txn) error {
		// Read current hwm and version
		value, currentVersion, err := txn.Get(hwmKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrStreamNotFound
			}
			return err
		}

		// Verify expected version matches current version
		if expectedVersion != currentVersion {
			return metadata.ErrVersionMismatch
		}

		// Decode current hwm
		currentHwm, err := DecodeHWM(value)
		if err != nil {
			return err
		}

		// Calculate new hwm
		newHwm = currentHwm + delta

		// Write new hwm with version check
		txn.PutWithVersion(hwmKey, EncodeHWM(newHwm), expectedVersion)

		return nil
	})

	if err != nil {
		return 0, 0, err
	}

	// The new version is expectedVersion + 1 after successful commit
	newVersion = expectedVersion + 1
	return newHwm, newVersion, nil
}

// SetHWM atomically sets the high watermark for a stream to a specific value.
// This is useful for initialization or recovery scenarios.
// It uses CAS semantics to ensure atomic updates.
//
// Returns the new version after the update, or an error if the operation fails.
func (sm *StreamManager) SetHWM(ctx context.Context, streamID string, hwm int64, expectedVersion metadata.Version) (metadata.Version, error) {
	hwmKey := keys.HwmKeyPath(streamID)
	var newVersion metadata.Version

	err := sm.store.Txn(ctx, hwmKey, func(txn metadata.Txn) error {
		// Verify the stream exists and check version
		_, currentVersion, err := txn.Get(hwmKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return ErrStreamNotFound
			}
			return err
		}

		if expectedVersion != currentVersion {
			return metadata.ErrVersionMismatch
		}

		// Write new hwm with version check
		txn.PutWithVersion(hwmKey, EncodeHWM(hwm), expectedVersion)
		return nil
	})

	if err != nil {
		return 0, err
	}

	newVersion = expectedVersion + 1
	return newVersion, nil
}

// MarkStreamDeleted marks a stream for deletion by removing its HWM and metadata.
// The actual cleanup of WAL and Parquet objects is handled by the GC service.
// Returns nil if the stream doesn't exist (idempotent).
func (sm *StreamManager) MarkStreamDeleted(ctx context.Context, streamID string) error {
	hwmKey := keys.HwmKeyPath(streamID)
	metaKey := keys.StreamMetaKeyPath(streamID)

	err := sm.store.Txn(ctx, hwmKey, func(txn metadata.Txn) error {
		// Delete both hwm and meta keys
		txn.Delete(hwmKey)
		txn.Delete(metaKey)
		return nil
	})

	return err
}
