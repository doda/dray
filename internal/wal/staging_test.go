package wal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// mockMetadataStore implements metadata.MetadataStore for testing.
type mockMetadataStore struct {
	data    map[string]metadata.KV
	nextVer metadata.Version
	putErr  error
	closed  bool
}

func newMockMetadataStore() *mockMetadataStore {
	return &mockMetadataStore{
		data:    make(map[string]metadata.KV),
		nextVer: 1,
	}
}

func (m *mockMetadataStore) Get(_ context.Context, key string) (metadata.GetResult, error) {
	if m.closed {
		return metadata.GetResult{}, metadata.ErrStoreClosed
	}
	kv, ok := m.data[key]
	if !ok {
		return metadata.GetResult{Exists: false}, nil
	}
	return metadata.GetResult{Value: kv.Value, Version: kv.Version, Exists: true}, nil
}

func (m *mockMetadataStore) Put(_ context.Context, key string, value []byte, _ ...metadata.PutOption) (metadata.Version, error) {
	if m.closed {
		return 0, metadata.ErrStoreClosed
	}
	if m.putErr != nil {
		return 0, m.putErr
	}
	ver := m.nextVer
	m.nextVer++
	m.data[key] = metadata.KV{Key: key, Value: value, Version: ver}
	return ver, nil
}

func (m *mockMetadataStore) Delete(_ context.Context, key string, _ ...metadata.DeleteOption) error {
	if m.closed {
		return metadata.ErrStoreClosed
	}
	delete(m.data, key)
	return nil
}

func (m *mockMetadataStore) List(_ context.Context, startKey, endKey string, limit int) ([]metadata.KV, error) {
	if m.closed {
		return nil, metadata.ErrStoreClosed
	}
	var result []metadata.KV
	for k, kv := range m.data {
		if endKey == "" {
			if strings.HasPrefix(k, startKey) {
				result = append(result, kv)
			}
		} else if k >= startKey && k < endKey {
			result = append(result, kv)
		}
	}
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (m *mockMetadataStore) Txn(_ context.Context, _ string, fn func(metadata.Txn) error) error {
	if m.closed {
		return metadata.ErrStoreClosed
	}
	txn := &mockMetaTxn{store: m, pending: make(map[string]txnOp)}
	if err := fn(txn); err != nil {
		return err
	}
	for key, op := range txn.pending {
		if op.delete {
			delete(m.data, key)
		} else {
			ver := m.nextVer
			m.nextVer++
			m.data[key] = metadata.KV{Key: key, Value: op.value, Version: ver}
		}
	}
	return nil
}

func (m *mockMetadataStore) Notifications(_ context.Context) (metadata.NotificationStream, error) {
	return nil, errors.New("not implemented")
}

func (m *mockMetadataStore) PutEphemeral(_ context.Context, key string, value []byte, _ ...metadata.EphemeralOption) (metadata.Version, error) {
	return m.Put(context.Background(), key, value)
}

func (m *mockMetadataStore) Close() error {
	m.closed = true
	return nil
}

type txnOp struct {
	value  []byte
	delete bool
}

type mockMetaTxn struct {
	store   *mockMetadataStore
	pending map[string]txnOp
}

func (t *mockMetaTxn) Get(key string) ([]byte, metadata.Version, error) {
	kv, ok := t.store.data[key]
	if !ok {
		return nil, 0, metadata.ErrKeyNotFound
	}
	return kv.Value, kv.Version, nil
}

func (t *mockMetaTxn) Put(key string, value []byte) {
	t.pending[key] = txnOp{value: value}
}

func (t *mockMetaTxn) PutWithVersion(key string, value []byte, _ metadata.Version) {
	t.pending[key] = txnOp{value: value}
}

func (t *mockMetaTxn) Delete(key string) {
	t.pending[key] = txnOp{delete: true}
}

func (t *mockMetaTxn) DeleteWithVersion(key string, _ metadata.Version) {
	t.pending[key] = txnOp{delete: true}
}

func TestStagingMarkerMarshalJSON(t *testing.T) {
	marker := &StagingMarker{
		Path:      "wal/domain=5/abc123.wo",
		CreatedAt: 1703721600000,
		SizeBytes: 102400,
	}

	data, err := json.Marshal(marker)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded StagingMarker
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Path != marker.Path {
		t.Errorf("Path = %q, want %q", decoded.Path, marker.Path)
	}
	if decoded.CreatedAt != marker.CreatedAt {
		t.Errorf("CreatedAt = %d, want %d", decoded.CreatedAt, marker.CreatedAt)
	}
	if decoded.SizeBytes != marker.SizeBytes {
		t.Errorf("SizeBytes = %d, want %d", decoded.SizeBytes, marker.SizeBytes)
	}
}

func TestParseStagingMarker(t *testing.T) {
	jsonData := `{"path":"wal/domain=42/uuid-here.wo","createdAt":1703721600000,"sizeBytes":51200}`

	marker, err := ParseStagingMarker([]byte(jsonData))
	if err != nil {
		t.Fatalf("ParseStagingMarker failed: %v", err)
	}

	if marker.Path != "wal/domain=42/uuid-here.wo" {
		t.Errorf("Path = %q", marker.Path)
	}
	if marker.CreatedAt != 1703721600000 {
		t.Errorf("CreatedAt = %d", marker.CreatedAt)
	}
	if marker.SizeBytes != 51200 {
		t.Errorf("SizeBytes = %d", marker.SizeBytes)
	}
}

func TestParseStagingMarkerInvalid(t *testing.T) {
	_, err := ParseStagingMarker([]byte("invalid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestStagingWriterFlush(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunk := Chunk{
		StreamID:       100,
		RecordCount:    10,
		MinTimestampMs: 1000,
		MaxTimestampMs: 2000,
		Batches: []BatchEntry{
			{Data: []byte("batch1")},
		},
	}

	err := w.AddChunk(chunk, 5)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	ctx := context.Background()
	result, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify result fields
	if result.MetaDomain != 5 {
		t.Errorf("MetaDomain = %d, want 5", result.MetaDomain)
	}
	if result.Size <= 0 {
		t.Error("Size should be positive")
	}
	if result.StagingKey == "" {
		t.Error("StagingKey should not be empty")
	}

	// Verify staging key format
	expectedKeyPrefix := keys.WALStagingPrefix + "/5/"
	if !strings.HasPrefix(result.StagingKey, expectedKeyPrefix) {
		t.Errorf("StagingKey = %q, want prefix %q", result.StagingKey, expectedKeyPrefix)
	}

	// Verify staging marker was written to metadata store
	stagingData, ok := metaStore.data[result.StagingKey]
	if !ok {
		t.Fatal("Staging marker not found in metadata store")
	}

	var marker StagingMarker
	err = json.Unmarshal(stagingData.Value, &marker)
	if err != nil {
		t.Fatalf("Failed to unmarshal staging marker: %v", err)
	}

	if marker.Path != result.Path {
		t.Errorf("Staging marker path = %q, want %q", marker.Path, result.Path)
	}
	if marker.CreatedAt != result.CreatedAtUnixMs {
		t.Errorf("Staging marker createdAt = %d, want %d", marker.CreatedAt, result.CreatedAtUnixMs)
	}
	if marker.SizeBytes != result.Size {
		t.Errorf("Staging marker sizeBytes = %d, want %d", marker.SizeBytes, result.Size)
	}

	// Verify WAL object was written to object store
	if len(objStore.objects) != 1 {
		t.Errorf("Expected 1 object in store, got %d", len(objStore.objects))
	}
	if _, ok := objStore.objects[result.Path]; !ok {
		t.Errorf("WAL object not found at path %q", result.Path)
	}
}

// orderTrackingMetaStore wraps mockMetadataStore to track write order
type orderTrackingMetaStore struct {
	*mockMetadataStore
	order *[]string
}

func (m *orderTrackingMetaStore) Put(ctx context.Context, key string, value []byte, opts ...metadata.PutOption) (metadata.Version, error) {
	*m.order = append(*m.order, "meta:"+key)
	return m.mockMetadataStore.Put(ctx, key, value, opts...)
}

// orderTrackingObjStore wraps mockStore to track write order
type orderTrackingObjStore struct {
	*mockStore
	order *[]string
}

func (s *orderTrackingObjStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	*s.order = append(*s.order, "obj:"+key)
	return s.mockStore.Put(ctx, key, reader, size, contentType)
}

func TestStagingWriterStagingKeyBeforeWAL(t *testing.T) {
	// This test verifies that the staging marker is written before the WAL object
	// by tracking the order of operations

	var writeOrder []string

	baseMeta := newMockMetadataStore()
	metaStore := &orderTrackingMetaStore{mockMetadataStore: baseMeta, order: &writeOrder}

	baseObj := newMockStore()
	objStore := &orderTrackingObjStore{mockStore: baseObj, order: &writeOrder}

	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunk := Chunk{
		StreamID:    1,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("data")}},
	}
	w.AddChunk(chunk, 0)

	_, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify order: staging marker should be written before WAL object
	if len(writeOrder) != 2 {
		t.Fatalf("Expected 2 writes, got %d: %v", len(writeOrder), writeOrder)
	}

	if !strings.HasPrefix(writeOrder[0], "meta:"+keys.WALStagingPrefix) {
		t.Errorf("First write should be staging marker, got %q", writeOrder[0])
	}
	if !strings.HasPrefix(writeOrder[1], "obj:") {
		t.Errorf("Second write should be object, got %q", writeOrder[1])
	}
}

func TestStagingWriterMetaDomainMismatch(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunk1 := Chunk{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("data1")}}}
	chunk2 := Chunk{StreamID: 2, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("data2")}}}

	err := w.AddChunk(chunk1, 0)
	if err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	err = w.AddChunk(chunk2, 1)
	if !errors.Is(err, ErrMetaDomainMismatch) {
		t.Errorf("expected ErrMetaDomainMismatch, got %v", err)
	}
}

func TestStagingWriterFlushEmpty(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	_, err := w.Flush(context.Background())
	if !errors.Is(err, ErrEmptyWAL) {
		t.Errorf("expected ErrEmptyWAL, got %v", err)
	}
}

func TestStagingWriterMetadataError(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	metaStore.putErr = errors.New("metadata unavailable")

	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunk := Chunk{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("data")}}}
	w.AddChunk(chunk, 0)

	_, err := w.Flush(context.Background())
	if err == nil {
		t.Error("expected error from Flush")
	}
	if !strings.Contains(err.Error(), "staging marker") {
		t.Errorf("error should mention staging marker, got: %v", err)
	}

	// Verify no WAL object was written
	if len(objStore.objects) != 0 {
		t.Error("WAL object should not be written when staging marker fails")
	}
}

func TestStagingWriterObjectStoreError(t *testing.T) {
	objStore := newMockStore()
	objStore.putErr = errors.New("storage unavailable")
	metaStore := newMockMetadataStore()

	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunk := Chunk{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("data")}}}
	w.AddChunk(chunk, 0)

	_, err := w.Flush(context.Background())
	if err == nil {
		t.Error("expected error from Flush")
	}

	// Staging marker should remain (for orphan GC cleanup)
	stagingKeys := 0
	for key := range metaStore.data {
		if strings.HasPrefix(key, keys.WALStagingPrefix) {
			stagingKeys++
		}
	}
	if stagingKeys != 1 {
		t.Errorf("Expected 1 staging marker to remain, found %d", stagingKeys)
	}
}

func TestStagingWriterClosed(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	w := NewStagingWriter(objStore, metaStore, nil)

	chunk := Chunk{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("data")}}}

	w.Close()

	err := w.AddChunk(chunk, 0)
	if !errors.Is(err, ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed from AddChunk, got %v", err)
	}

	_, err = w.Flush(context.Background())
	if !errors.Is(err, ErrWriterClosed) {
		t.Errorf("expected ErrWriterClosed from Flush, got %v", err)
	}
}

func TestStagingWriterReset(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunk := Chunk{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("data")}}}
	w.AddChunk(chunk, 0)

	if w.ChunkCount() != 1 {
		t.Fatal("expected 1 chunk before reset")
	}

	w.Reset()

	if w.ChunkCount() != 0 {
		t.Error("ChunkCount should be 0 after reset")
	}
	if w.MetaDomain() != nil {
		t.Error("MetaDomain should be nil after reset")
	}
}

func TestDeleteStagingKey(t *testing.T) {
	metaStore := newMockMetadataStore()

	// Put a staging key
	stagingKey := keys.WALStagingKeyPath(5, "test-wal-id")
	metaStore.Put(context.Background(), stagingKey, []byte(`{"path":"test","createdAt":123,"sizeBytes":456}`))

	// Verify it exists
	result, _ := metaStore.Get(context.Background(), stagingKey)
	if !result.Exists {
		t.Fatal("Staging key should exist before delete")
	}

	// Delete via transaction
	err := metaStore.Txn(context.Background(), stagingKey, func(txn metadata.Txn) error {
		DeleteStagingKey(txn, stagingKey)
		return nil
	})
	if err != nil {
		t.Fatalf("Txn failed: %v", err)
	}

	// Verify it's gone
	result, _ = metaStore.Get(context.Background(), stagingKey)
	if result.Exists {
		t.Error("Staging key should be deleted after transaction")
	}
}

func TestStagingWriterMultipleFlushes(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	ctx := context.Background()

	// First flush
	chunk1 := Chunk{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("first")}}}
	w.AddChunk(chunk1, 0)
	result1, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("First Flush failed: %v", err)
	}

	// Second flush with different MetaDomain
	chunk2 := Chunk{StreamID: 2, RecordCount: 10, Batches: []BatchEntry{{Data: []byte("second")}}}
	w.AddChunk(chunk2, 7)
	result2, err := w.Flush(ctx)
	if err != nil {
		t.Fatalf("Second Flush failed: %v", err)
	}

	// Verify different WAL IDs
	if result1.WalID == result2.WalID {
		t.Error("WalIDs should be different")
	}

	// Verify different staging keys
	if result1.StagingKey == result2.StagingKey {
		t.Error("StagingKeys should be different")
	}

	// Verify both staging markers exist
	if len(metaStore.data) != 2 {
		t.Errorf("Expected 2 staging markers, got %d", len(metaStore.data))
	}
}

func TestStagingWriterWithCustomPathFormatter(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()

	customFormatter := &DefaultPathFormatter{Prefix: "custom/prefix"}
	w := NewStagingWriter(objStore, metaStore, &StagingWriterConfig{PathFormatter: customFormatter})
	defer w.Close()

	chunk := Chunk{StreamID: 1, RecordCount: 5, Batches: []BatchEntry{{Data: []byte("data")}}}
	w.AddChunk(chunk, 42)

	result, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	expectedPrefix := "custom/prefix/wal/domain=42/"
	if !strings.HasPrefix(result.Path, expectedPrefix) {
		t.Errorf("Path = %s, expected prefix %s", result.Path, expectedPrefix)
	}

	// Verify staging marker has correct path
	stagingData := metaStore.data[result.StagingKey]
	var marker StagingMarker
	json.Unmarshal(stagingData.Value, &marker)
	if marker.Path != result.Path {
		t.Errorf("Staging marker path mismatch: %q vs %q", marker.Path, result.Path)
	}
}

func TestStagingWriterChunkOffsets(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()
	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunks := []Chunk{
		{StreamID: 300, RecordCount: 15, MinTimestampMs: 3000, MaxTimestampMs: 3500, Batches: []BatchEntry{{Data: []byte("a")}, {Data: []byte("b")}}},
		{StreamID: 100, RecordCount: 5, MinTimestampMs: 1000, MaxTimestampMs: 1500, Batches: []BatchEntry{{Data: []byte("c")}}},
	}

	for _, chunk := range chunks {
		w.AddChunk(chunk, 1)
	}

	result, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if len(result.ChunkOffsets) != 2 {
		t.Fatalf("ChunkOffsets len = %d, want 2", len(result.ChunkOffsets))
	}

	// ChunkOffsets are sorted by StreamID to match the WAL encoding order.
	// This is necessary because ByteOffset and ByteLength must match the actual
	// positions in the encoded WAL, which sorts chunks by StreamID.
	sortedChunks := []Chunk{
		{StreamID: 100, RecordCount: 5, MinTimestampMs: 1000, MaxTimestampMs: 1500, Batches: []BatchEntry{{Data: []byte("c")}}},
		{StreamID: 300, RecordCount: 15, MinTimestampMs: 3000, MaxTimestampMs: 3500, Batches: []BatchEntry{{Data: []byte("a")}, {Data: []byte("b")}}},
	}

	for i, chunk := range sortedChunks {
		offset := result.ChunkOffsets[i]
		if offset.StreamID != chunk.StreamID {
			t.Errorf("ChunkOffsets[%d].StreamID = %d, want %d", i, offset.StreamID, chunk.StreamID)
		}
		if offset.RecordCount != chunk.RecordCount {
			t.Errorf("ChunkOffsets[%d].RecordCount = %d, want %d", i, offset.RecordCount, chunk.RecordCount)
		}
		if offset.BatchCount != uint32(len(chunk.Batches)) {
			t.Errorf("ChunkOffsets[%d].BatchCount = %d, want %d", i, offset.BatchCount, len(chunk.Batches))
		}
	}

	// Verify ByteOffset and ByteLength are set correctly
	// First chunk should start at HeaderSize (49 bytes)
	if result.ChunkOffsets[0].ByteOffset != HeaderSize {
		t.Errorf("ChunkOffsets[0].ByteOffset = %d, want %d", result.ChunkOffsets[0].ByteOffset, HeaderSize)
	}
	// First chunk (StreamID=100) has 1 batch of 1 byte: 4 bytes length + 1 byte data = 5 bytes
	if result.ChunkOffsets[0].ByteLength != 5 {
		t.Errorf("ChunkOffsets[0].ByteLength = %d, want 5", result.ChunkOffsets[0].ByteLength)
	}
	// Second chunk should start after first chunk
	expectedSecondOffset := uint64(HeaderSize) + 5
	if result.ChunkOffsets[1].ByteOffset != expectedSecondOffset {
		t.Errorf("ChunkOffsets[1].ByteOffset = %d, want %d", result.ChunkOffsets[1].ByteOffset, expectedSecondOffset)
	}
	// Second chunk (StreamID=300) has 2 batches: (4+1) + (4+1) = 10 bytes
	if result.ChunkOffsets[1].ByteLength != 10 {
		t.Errorf("ChunkOffsets[1].ByteLength = %d, want 10", result.ChunkOffsets[1].ByteLength)
	}
}

func TestStagingWriterWithMetrics(t *testing.T) {
	objStore := newMockStore()
	metaStore := newMockMetadataStore()

	// Create metrics with a custom registry
	reg := prometheus.NewRegistry()
	walMetrics := metrics.NewWALMetricsWithRegistry(reg)

	w := NewStagingWriter(objStore, metaStore, &StagingWriterConfig{
		Metrics: walMetrics,
	})
	defer w.Close()

	// Perform first flush
	chunk1 := Chunk{
		StreamID:    1,
		RecordCount: 10,
		Batches:     []BatchEntry{{Data: []byte("batch-data-1")}},
	}
	w.AddChunk(chunk1, 0)

	result1, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("First Flush failed: %v", err)
	}

	// Perform second flush
	chunk2 := Chunk{
		StreamID:    2,
		RecordCount: 5,
		Batches:     []BatchEntry{{Data: []byte("batch-data-2")}},
	}
	w.AddChunk(chunk2, 0)

	result2, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Second Flush failed: %v", err)
	}

	// Verify objects created counter
	counterMetric := &dto.Metric{}
	if err := walMetrics.ObjectsCreatedTotal.Write(counterMetric); err != nil {
		t.Fatalf("failed to write counter: %v", err)
	}
	if got := counterMetric.Counter.GetValue(); got != 2 {
		t.Errorf("objects created = %f, want 2", got)
	}

	// Verify size histogram has 2 samples
	sizeMetric := &dto.Metric{}
	if err := walMetrics.SizeHistogram.Write(sizeMetric); err != nil {
		t.Fatalf("failed to write size metric: %v", err)
	}
	if got := sizeMetric.Histogram.GetSampleCount(); got != 2 {
		t.Errorf("size sample count = %d, want 2", got)
	}
	// The sum should match the total bytes of both WAL objects
	expectedSum := float64(result1.Size + result2.Size)
	if got := sizeMetric.Histogram.GetSampleSum(); got != expectedSum {
		t.Errorf("size sum = %f, want %f", got, expectedSum)
	}

	// Verify latency histogram has 2 samples
	latencyMetric := &dto.Metric{}
	if err := walMetrics.FlushLatencyHistogram.Write(latencyMetric); err != nil {
		t.Fatalf("failed to write latency metric: %v", err)
	}
	if got := latencyMetric.Histogram.GetSampleCount(); got != 2 {
		t.Errorf("latency sample count = %d, want 2", got)
	}
	// Latencies should be positive (non-zero)
	if latencyMetric.Histogram.GetSampleSum() <= 0 {
		t.Error("expected positive latency sum")
	}
}

func TestStagingWriterWithMetricsNil(t *testing.T) {
	// Verify that flush works correctly when metrics is nil
	objStore := newMockStore()
	metaStore := newMockMetadataStore()

	w := NewStagingWriter(objStore, metaStore, nil)
	defer w.Close()

	chunk := Chunk{
		StreamID:    1,
		RecordCount: 10,
		Batches:     []BatchEntry{{Data: []byte("batch-data")}},
	}
	w.AddChunk(chunk, 0)

	_, err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestStagingWriterMetricsNotRecordedOnError(t *testing.T) {
	objStore := newMockStore()
	objStore.putErr = errors.New("storage unavailable")
	metaStore := newMockMetadataStore()

	reg := prometheus.NewRegistry()
	walMetrics := metrics.NewWALMetricsWithRegistry(reg)

	w := NewStagingWriter(objStore, metaStore, &StagingWriterConfig{
		Metrics: walMetrics,
	})
	defer w.Close()

	chunk := Chunk{
		StreamID:    1,
		RecordCount: 10,
		Batches:     []BatchEntry{{Data: []byte("batch-data")}},
	}
	w.AddChunk(chunk, 0)

	_, err := w.Flush(context.Background())
	if err == nil {
		t.Fatal("expected error from Flush")
	}

	// Verify no metrics were recorded (because flush failed)
	counterMetric := &dto.Metric{}
	if err := walMetrics.ObjectsCreatedTotal.Write(counterMetric); err != nil {
		t.Fatalf("failed to write counter: %v", err)
	}
	if got := counterMetric.Counter.GetValue(); got != 0 {
		t.Errorf("objects created = %f, want 0 (no metrics on failure)", got)
	}

	sizeMetric := &dto.Metric{}
	if err := walMetrics.SizeHistogram.Write(sizeMetric); err != nil {
		t.Fatalf("failed to write size metric: %v", err)
	}
	if got := sizeMetric.Histogram.GetSampleCount(); got != 0 {
		t.Errorf("size sample count = %d, want 0 (no metrics on failure)", got)
	}
}
