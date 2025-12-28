package index

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/google/uuid"
)

// mockMetadataStore is a simple in-memory implementation of MetadataStore for testing.
type mockMetadataStore struct {
	data     map[string]mockKV
	closed   bool
	txnCount int
}

type mockKV struct {
	value   []byte
	version metadata.Version
}

func newMockMetadataStore() *mockMetadataStore {
	return &mockMetadataStore{
		data: make(map[string]mockKV),
	}
}

func (m *mockMetadataStore) Get(ctx context.Context, key string) (metadata.GetResult, error) {
	if m.closed {
		return metadata.GetResult{}, metadata.ErrStoreClosed
	}
	if kv, ok := m.data[key]; ok {
		return metadata.GetResult{
			Value:   kv.value,
			Version: kv.version,
			Exists:  true,
		}, nil
	}
	return metadata.GetResult{Exists: false}, nil
}

func (m *mockMetadataStore) Put(ctx context.Context, key string, value []byte, opts ...metadata.PutOption) (metadata.Version, error) {
	if m.closed {
		return 0, metadata.ErrStoreClosed
	}
	expectedVersion := metadata.ExtractExpectedVersion(opts)
	if expectedVersion != nil {
		if kv, ok := m.data[key]; ok {
			if kv.version != *expectedVersion {
				return 0, metadata.ErrVersionMismatch
			}
		} else if *expectedVersion != 0 {
			return 0, metadata.ErrVersionMismatch
		}
	}
	newVersion := metadata.Version(1)
	if kv, ok := m.data[key]; ok {
		newVersion = kv.version + 1
	}
	m.data[key] = mockKV{value: value, version: newVersion}
	return newVersion, nil
}

func (m *mockMetadataStore) Delete(ctx context.Context, key string, opts ...metadata.DeleteOption) error {
	if m.closed {
		return metadata.ErrStoreClosed
	}
	delete(m.data, key)
	return nil
}

func (m *mockMetadataStore) List(ctx context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	if m.closed {
		return nil, metadata.ErrStoreClosed
	}
	return nil, nil
}

func (m *mockMetadataStore) Txn(ctx context.Context, scopeKey string, fn func(metadata.Txn) error) error {
	if m.closed {
		return metadata.ErrStoreClosed
	}
	m.txnCount++
	txn := &mockTxn{store: m, pending: make(map[string][]byte)}
	if err := fn(txn); err != nil {
		return err
	}
	// Apply pending writes
	for key, value := range txn.pending {
		newVersion := metadata.Version(1)
		if kv, ok := m.data[key]; ok {
			newVersion = kv.version + 1
		}
		m.data[key] = mockKV{value: value, version: newVersion}
	}
	return nil
}

func (m *mockMetadataStore) Notifications(ctx context.Context) (metadata.NotificationStream, error) {
	return nil, nil
}

func (m *mockMetadataStore) PutEphemeral(ctx context.Context, key string, value []byte, opts ...metadata.EphemeralOption) (metadata.Version, error) {
	return m.Put(ctx, key, value)
}

func (m *mockMetadataStore) Close() error {
	m.closed = true
	return nil
}

type mockTxn struct {
	store   *mockMetadataStore
	pending map[string][]byte
}

func (t *mockTxn) Get(key string) (value []byte, version metadata.Version, err error) {
	if kv, ok := t.store.data[key]; ok {
		return kv.value, kv.version, nil
	}
	return nil, 0, metadata.ErrKeyNotFound
}

func (t *mockTxn) Put(key string, value []byte) {
	t.pending[key] = value
}

func (t *mockTxn) PutWithVersion(key string, value []byte, expectedVersion metadata.Version) {
	t.pending[key] = value
}

func (t *mockTxn) Delete(key string) {
	t.pending[key] = nil
}

func (t *mockTxn) DeleteWithVersion(key string, expectedVersion metadata.Version) {
	t.pending[key] = nil
}

func TestCreateStream(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create a new stream
	streamID, err := sm.CreateStream(ctx, "my-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Verify streamID is a valid UUID
	if _, err := uuid.Parse(streamID); err != nil {
		t.Errorf("streamID is not a valid UUID: %s", streamID)
	}

	// Verify hwm was created with value 0
	hwmKey := keys.HwmKeyPath(streamID)
	hwmResult, err := store.Get(ctx, hwmKey)
	if err != nil {
		t.Fatalf("Get hwm failed: %v", err)
	}
	if !hwmResult.Exists {
		t.Error("hwm key should exist")
	}
	if len(hwmResult.Value) != 8 {
		t.Errorf("hwm value should be 8 bytes, got %d", len(hwmResult.Value))
	}
	hwm := int64(binary.BigEndian.Uint64(hwmResult.Value))
	if hwm != 0 {
		t.Errorf("hwm should be 0, got %d", hwm)
	}

	// Verify stream metadata was created
	metaKey := keys.StreamMetaKeyPath(streamID)
	metaResult, err := store.Get(ctx, metaKey)
	if err != nil {
		t.Fatalf("Get meta failed: %v", err)
	}
	if !metaResult.Exists {
		t.Error("meta key should exist")
	}

	var meta StreamMeta
	if err := json.Unmarshal(metaResult.Value, &meta); err != nil {
		t.Fatalf("Unmarshal meta failed: %v", err)
	}
	if meta.StreamID != streamID {
		t.Errorf("meta.StreamID = %s, want %s", meta.StreamID, streamID)
	}
	if meta.TopicName != "my-topic" {
		t.Errorf("meta.TopicName = %s, want my-topic", meta.TopicName)
	}
	if meta.Partition != 0 {
		t.Errorf("meta.Partition = %d, want 0", meta.Partition)
	}
	if meta.CreatedAt.IsZero() {
		t.Error("meta.CreatedAt should not be zero")
	}
}

func TestCreateStreamWithID(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID := uuid.New().String()

	// Create a new stream with a specific ID
	err := sm.CreateStreamWithID(ctx, streamID, "test-topic", 5)
	if err != nil {
		t.Fatalf("CreateStreamWithID failed: %v", err)
	}

	// Verify hwm was created with value 0
	hwmKey := keys.HwmKeyPath(streamID)
	hwmResult, err := store.Get(ctx, hwmKey)
	if err != nil {
		t.Fatalf("Get hwm failed: %v", err)
	}
	if !hwmResult.Exists {
		t.Error("hwm key should exist")
	}
	hwm := int64(binary.BigEndian.Uint64(hwmResult.Value))
	if hwm != 0 {
		t.Errorf("hwm should be 0, got %d", hwm)
	}

	// Verify stream metadata was created
	metaKey := keys.StreamMetaKeyPath(streamID)
	metaResult, err := store.Get(ctx, metaKey)
	if err != nil {
		t.Fatalf("Get meta failed: %v", err)
	}
	if !metaResult.Exists {
		t.Error("meta key should exist")
	}

	var meta StreamMeta
	if err := json.Unmarshal(metaResult.Value, &meta); err != nil {
		t.Fatalf("Unmarshal meta failed: %v", err)
	}
	if meta.StreamID != streamID {
		t.Errorf("meta.StreamID = %s, want %s", meta.StreamID, streamID)
	}
	if meta.TopicName != "test-topic" {
		t.Errorf("meta.TopicName = %s, want test-topic", meta.TopicName)
	}
	if meta.Partition != 5 {
		t.Errorf("meta.Partition = %d, want 5", meta.Partition)
	}
}

func TestCreateStreamAlreadyExists(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create a stream
	streamID, err := sm.CreateStream(ctx, "my-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	// Try to create another stream with the same ID
	err = sm.CreateStreamWithID(ctx, streamID, "other-topic", 1)
	if err != ErrStreamExists {
		t.Errorf("CreateStreamWithID should return ErrStreamExists, got %v", err)
	}
}

func TestGetStreamMeta(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "meta-topic", 3)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	meta, err := sm.GetStreamMeta(ctx, streamID)
	if err != nil {
		t.Fatalf("GetStreamMeta failed: %v", err)
	}

	if meta.StreamID != streamID {
		t.Errorf("meta.StreamID = %s, want %s", meta.StreamID, streamID)
	}
	if meta.TopicName != "meta-topic" {
		t.Errorf("meta.TopicName = %s, want meta-topic", meta.TopicName)
	}
	if meta.Partition != 3 {
		t.Errorf("meta.Partition = %d, want 3", meta.Partition)
	}
}

func TestGetStreamMetaNotFound(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, err := sm.GetStreamMeta(ctx, "nonexistent-stream")
	if err != ErrStreamNotFound {
		t.Errorf("GetStreamMeta should return ErrStreamNotFound, got %v", err)
	}
}

func TestGetHWM(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	streamID, err := sm.CreateStream(ctx, "hwm-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	hwm, version, err := sm.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("GetHWM failed: %v", err)
	}

	if hwm != 0 {
		t.Errorf("hwm = %d, want 0", hwm)
	}
	if version == 0 {
		t.Error("version should not be 0")
	}
}

func TestGetHWMNotFound(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, _, err := sm.GetHWM(ctx, "nonexistent-stream")
	if err != ErrStreamNotFound {
		t.Errorf("GetHWM should return ErrStreamNotFound, got %v", err)
	}
}

func TestEncodeDecodeHWM(t *testing.T) {
	testCases := []int64{0, 1, 100, 1000000, 9223372036854775807}

	for _, tc := range testCases {
		encoded := EncodeHWM(tc)
		decoded, err := DecodeHWM(encoded)
		if err != nil {
			t.Fatalf("DecodeHWM(%d) failed: %v", tc, err)
		}
		if decoded != tc {
			t.Errorf("DecodeHWM(EncodeHWM(%d)) = %d, want %d", tc, decoded, tc)
		}
	}
}

func TestDecodeHWMInvalid(t *testing.T) {
	testCases := [][]byte{
		nil,
		{},
		{1, 2, 3},
		{1, 2, 3, 4, 5, 6, 7},
		{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	for _, tc := range testCases {
		_, err := DecodeHWM(tc)
		if err == nil {
			t.Errorf("DecodeHWM(%v) should return error", tc)
		}
	}
}

func TestStreamManagerUsesTransactions(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	_, err := sm.CreateStream(ctx, "txn-topic", 0)
	if err != nil {
		t.Fatalf("CreateStream failed: %v", err)
	}

	if store.txnCount != 1 {
		t.Errorf("txnCount = %d, want 1", store.txnCount)
	}
}

func TestMultipleStreamsForSameTopic(t *testing.T) {
	store := newMockMetadataStore()
	sm := NewStreamManager(store)
	ctx := context.Background()

	// Create multiple partitions for the same topic
	streamIDs := make([]string, 3)
	for i := int32(0); i < 3; i++ {
		streamID, err := sm.CreateStream(ctx, "multi-partition-topic", i)
		if err != nil {
			t.Fatalf("CreateStream partition %d failed: %v", i, err)
		}
		streamIDs[i] = streamID
	}

	// Verify all streams are unique
	seen := make(map[string]bool)
	for _, id := range streamIDs {
		if seen[id] {
			t.Errorf("duplicate streamID: %s", id)
		}
		seen[id] = true
	}

	// Verify each stream has correct metadata
	for i, streamID := range streamIDs {
		meta, err := sm.GetStreamMeta(ctx, streamID)
		if err != nil {
			t.Fatalf("GetStreamMeta failed: %v", err)
		}
		if meta.Partition != int32(i) {
			t.Errorf("partition %d: meta.Partition = %d, want %d", i, meta.Partition, i)
		}
		if meta.TopicName != "multi-partition-topic" {
			t.Errorf("partition %d: meta.TopicName = %s, want multi-partition-topic", i, meta.TopicName)
		}
	}
}
