package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/topics"
	"github.com/dray-io/dray/internal/wal"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestFetchHandler_ValidateTopicAndPartition tests that fetch handler validates topic and partition existence.
func TestFetchHandler_ValidateTopicAndPartition(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	// Create a test topic with 3 partitions
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Create streams for partitions
	for _, p := range result.Partitions {
		err := streamManager.CreateStreamWithID(ctx, p.StreamID, "test-topic", p.Partition)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
	}

	mockObjStore := NewMockObjectStore()
	fetcher := fetch.NewFetcher(mockObjStore, streamManager)
	handler := NewFetchHandler(FetchHandlerConfig{MaxBytes: 1024 * 1024}, topicStore, fetcher, streamManager)

	tests := []struct {
		name        string
		topic       string
		partition   int32
		wantErr     bool
		wantErrCode int16
	}{
		{
			name:      "valid topic and partition",
			topic:     "test-topic",
			partition: 0,
			wantErr:   false,
		},
		{
			name:      "valid topic and last partition",
			topic:     "test-topic",
			partition: 2,
			wantErr:   false,
		},
		{
			name:        "unknown topic",
			topic:       "nonexistent-topic",
			partition:   0,
			wantErr:     true,
			wantErrCode: errUnknownTopicOrPartitionErr,
		},
		{
			name:        "unknown partition",
			topic:       "test-topic",
			partition:   5,
			wantErr:     true,
			wantErrCode: errUnknownTopicOrPartitionErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildFetchRequest(tt.topic, tt.partition, 0)

			resp := handler.Handle(ctx, 12, req)

			if len(resp.Topics) != 1 {
				t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
			}

			topicResp := resp.Topics[0]
			if len(topicResp.Partitions) != 1 {
				t.Fatalf("expected 1 partition response, got %d", len(topicResp.Partitions))
			}

			partResp := topicResp.Partitions[0]

			if tt.wantErr {
				if partResp.ErrorCode != tt.wantErrCode {
					t.Errorf("expected error code %d, got %d", tt.wantErrCode, partResp.ErrorCode)
				}
			} else {
				if partResp.ErrorCode != 0 {
					t.Errorf("expected success, got error code %d", partResp.ErrorCode)
				}
			}
		})
	}
}

// TestFetchHandler_EmptyStream tests that fetch from an empty stream returns HWM=0.
func TestFetchHandler_EmptyStream(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for _, p := range result.Partitions {
		err := streamManager.CreateStreamWithID(ctx, p.StreamID, "test-topic", p.Partition)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
	}

	mockObjStore := NewMockObjectStore()
	fetcher := fetch.NewFetcher(mockObjStore, streamManager)
	handler := NewFetchHandler(FetchHandlerConfig{MaxBytes: 1024 * 1024}, topicStore, fetcher, streamManager)

	req := buildFetchRequest("test-topic", 0, 0)
	resp := handler.Handle(ctx, 12, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]

	// Empty stream - should return OffsetBeyondHWM behavior
	// HWM is 0, fetchOffset is 0, so it's at the end
	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}

	if partResp.HighWatermark != 0 {
		t.Errorf("expected HWM=0, got %d", partResp.HighWatermark)
	}
}

// TestFetchHandler_WithData tests fetch with actual data in WAL.
func TestFetchHandler_WithData(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create a WAL object with some records
	mockObjStore := NewMockObjectStore()
	walPath := "wal/domain=0/test.wal"

	// Build record batch
	batch := buildRecordBatch(5)

	// Build WAL with chunk
	walID := uuid.New()
	walObj := wal.NewWAL(walID, 0, time.Now().UnixMilli())

	// For WAL, we need to use the stream ID as uint64
	// Parse the UUID and use first 8 bytes as stream ID
	parsedStreamID := parseStreamIDToUint64(streamID)

	walObj.AddChunk(wal.Chunk{
		StreamID:       parsedStreamID,
		Batches:        []wal.BatchEntry{{Data: batch}},
		RecordCount:    5,
		MinTimestampMs: time.Now().UnixMilli(),
		MaxTimestampMs: time.Now().UnixMilli(),
	})

	walData, err := wal.EncodeToBytes(walObj)
	if err != nil {
		t.Fatalf("failed to encode WAL: %v", err)
	}

	mockObjStore.Put(ctx, walPath, bytes.NewReader(walData), int64(len(walData)), "application/octet-stream")

	// Calculate chunk offset and length from the WAL format
	// Header is 49 bytes, then chunk body starts
	chunkOffset := uint64(wal.HeaderSize)
	chunkLength := 4 + uint32(len(batch)) // 4 bytes length prefix + batch data

	// Create index entry
	_, err = streamManager.AppendIndexEntry(ctx, index.AppendRequest{
		StreamID:       streamID,
		RecordCount:    5,
		ChunkSizeBytes: int64(chunkLength),
		CreatedAtMs:    time.Now().UnixMilli(),
		MinTimestampMs: time.Now().UnixMilli(),
		MaxTimestampMs: time.Now().UnixMilli(),
		WalID:          walID.String(),
		WalPath:        walPath,
		ChunkOffset:    chunkOffset,
		ChunkLength:    chunkLength,
	})
	if err != nil {
		t.Fatalf("failed to append index entry: %v", err)
	}

	fetcher := fetch.NewFetcher(mockObjStore, streamManager)
	handler := NewFetchHandler(FetchHandlerConfig{MaxBytes: 1024 * 1024}, topicStore, fetcher, streamManager)

	req := buildFetchRequest("test-topic", 0, 0)
	resp := handler.Handle(ctx, 12, req)

	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response structure")
	}

	partResp := resp.Topics[0].Partitions[0]

	if partResp.ErrorCode != 0 {
		t.Errorf("expected success, got error code %d", partResp.ErrorCode)
	}

	if partResp.HighWatermark != 5 {
		t.Errorf("expected HWM=5, got %d", partResp.HighWatermark)
	}

	// Should have record batches
	if len(partResp.RecordBatches) == 0 {
		t.Error("expected record batches in response")
	}
}

// TestFetchHandler_HighWatermark tests that HWM is correctly set in response.
func TestFetchHandler_HighWatermark(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	mockObjStore := NewMockObjectStore()
	fetcher := fetch.NewFetcher(mockObjStore, streamManager)
	handler := NewFetchHandler(FetchHandlerConfig{MaxBytes: 1024 * 1024}, topicStore, fetcher, streamManager)

	// Fetch from empty stream
	req := buildFetchRequest("test-topic", 0, 0)
	resp := handler.Handle(ctx, 12, req)

	partResp := resp.Topics[0].Partitions[0]
	if partResp.HighWatermark != 0 {
		t.Errorf("expected HWM=0 for empty stream, got %d", partResp.HighWatermark)
	}
}

// TestFetchHandler_FetchBeyondHWM tests fetch at or beyond HWM.
func TestFetchHandler_FetchBeyondHWM(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	err = streamManager.CreateStreamWithID(ctx, streamID, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	mockObjStore := NewMockObjectStore()
	fetcher := fetch.NewFetcher(mockObjStore, streamManager)
	handler := NewFetchHandler(FetchHandlerConfig{MaxBytes: 1024 * 1024}, topicStore, fetcher, streamManager)

	// Fetch offset 100 from empty stream (HWM=0)
	req := buildFetchRequest("test-topic", 0, 100)
	resp := handler.Handle(ctx, 12, req)

	partResp := resp.Topics[0].Partitions[0]
	// Should succeed but with no data (waiting at end)
	if partResp.ErrorCode != 0 {
		t.Errorf("expected success for fetch at HWM, got error code %d", partResp.ErrorCode)
	}
}

// TestFetchHandler_MultiplePartitions tests fetch from multiple partitions.
func TestFetchHandler_MultiplePartitions(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "test-topic",
		PartitionCount: 3,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for _, p := range result.Partitions {
		err := streamManager.CreateStreamWithID(ctx, p.StreamID, "test-topic", p.Partition)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
	}

	mockObjStore := NewMockObjectStore()
	fetcher := fetch.NewFetcher(mockObjStore, streamManager)
	handler := NewFetchHandler(FetchHandlerConfig{MaxBytes: 1024 * 1024}, topicStore, fetcher, streamManager)

	// Build request for multiple partitions
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(12)
	req.MaxBytes = 1024 * 1024

	topicReq := kmsg.NewFetchRequestTopic()
	topicReq.Topic = "test-topic"

	for i := int32(0); i < 3; i++ {
		partReq := kmsg.NewFetchRequestTopicPartition()
		partReq.Partition = i
		partReq.FetchOffset = 0
		partReq.PartitionMaxBytes = 1024 * 1024
		topicReq.Partitions = append(topicReq.Partitions, partReq)
	}

	req.Topics = append(req.Topics, topicReq)

	resp := handler.Handle(ctx, 12, req)

	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}

	topicResp := resp.Topics[0]
	if len(topicResp.Partitions) != 3 {
		t.Fatalf("expected 3 partition responses, got %d", len(topicResp.Partitions))
	}

	for i, partResp := range topicResp.Partitions {
		if partResp.ErrorCode != 0 {
			t.Errorf("partition %d: expected success, got error code %d", i, partResp.ErrorCode)
		}
	}
}

// TestFetchHandler_MultipleTopics tests fetch from multiple topics.
func TestFetchHandler_MultipleTopics(t *testing.T) {
	store := metadata.NewMockStore()
	topicStore := topics.NewStore(store)
	streamManager := index.NewStreamManager(store)
	ctx := context.Background()

	// Create two topics
	for _, topicName := range []string{"topic-a", "topic-b"} {
		result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
			Name:           topicName,
			PartitionCount: 1,
			NowMs:          time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", topicName, err)
		}
		for _, p := range result.Partitions {
			err := streamManager.CreateStreamWithID(ctx, p.StreamID, topicName, p.Partition)
			if err != nil {
				t.Fatalf("failed to create stream: %v", err)
			}
		}
	}

	mockObjStore := NewMockObjectStore()
	fetcher := fetch.NewFetcher(mockObjStore, streamManager)
	handler := NewFetchHandler(FetchHandlerConfig{MaxBytes: 1024 * 1024}, topicStore, fetcher, streamManager)

	// Build request for multiple topics
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(12)
	req.MaxBytes = 1024 * 1024

	for _, topicName := range []string{"topic-a", "topic-b"} {
		topicReq := kmsg.NewFetchRequestTopic()
		topicReq.Topic = topicName

		partReq := kmsg.NewFetchRequestTopicPartition()
		partReq.Partition = 0
		partReq.FetchOffset = 0
		partReq.PartitionMaxBytes = 1024 * 1024
		topicReq.Partitions = append(topicReq.Partitions, partReq)

		req.Topics = append(req.Topics, topicReq)
	}

	resp := handler.Handle(ctx, 12, req)

	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topic responses, got %d", len(resp.Topics))
	}

	for _, topicResp := range resp.Topics {
		if len(topicResp.Partitions) != 1 {
			t.Errorf("topic %s: expected 1 partition, got %d", topicResp.Topic, len(topicResp.Partitions))
		}
		if topicResp.Partitions[0].ErrorCode != 0 {
			t.Errorf("topic %s: expected success, got error code %d", topicResp.Topic, topicResp.Partitions[0].ErrorCode)
		}
	}
}

// buildFetchRequest creates a minimal fetch request for testing.
func buildFetchRequest(topic string, partition int32, fetchOffset int64) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(12)
	req.MaxBytes = 1024 * 1024

	topicReq := kmsg.NewFetchRequestTopic()
	topicReq.Topic = topic

	partReq := kmsg.NewFetchRequestTopicPartition()
	partReq.Partition = partition
	partReq.FetchOffset = fetchOffset
	partReq.PartitionMaxBytes = 1024 * 1024

	topicReq.Partitions = append(topicReq.Partitions, partReq)
	req.Topics = append(req.Topics, topicReq)

	return req
}

// parseStreamIDToUint64 parses a UUID string to uint64 for WAL encoding.
func parseStreamIDToUint64(streamID string) uint64 {
	u, err := uuid.Parse(streamID)
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(u[:8])
}

// MockObjectStore implements objectstore.Store for testing.
type MockObjectStore struct {
	objects map[string][]byte
}

func NewMockObjectStore() *MockObjectStore {
	return &MockObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *MockObjectStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.objects[key] = data
	return nil
}

func (m *MockObjectStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts objectstore.PutOptions) error {
	return m.Put(ctx, key, reader, size, contentType)
}

func (m *MockObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MockObjectStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	if start < 0 || start >= int64(len(data)) {
		return nil, objectstore.ErrInvalidRange
	}
	if end < 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (m *MockObjectStore) Head(ctx context.Context, key string) (objectstore.ObjectMeta, error) {
	data, ok := m.objects[key]
	if !ok {
		return objectstore.ObjectMeta{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectMeta{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *MockObjectStore) Delete(ctx context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

func (m *MockObjectStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectMeta, error) {
	var result []objectstore.ObjectMeta
	for key, data := range m.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, objectstore.ObjectMeta{
				Key:  key,
				Size: int64(len(data)),
			})
		}
	}
	return result, nil
}

func (m *MockObjectStore) Close() error {
	return nil
}
