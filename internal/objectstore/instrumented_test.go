package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// mockMetrics records all metric calls for testing.
type mockMetrics struct {
	puts      []putCall
	gets      []getCall
	getRanges []getRangeCall
	heads     []headCall
	deletes   []deleteCall
	lists     []listCall
}

type putCall struct {
	duration float64
	success  bool
	bytes    int64
}

type getCall struct {
	duration float64
	success  bool
	bytes    int64
}

type getRangeCall struct {
	duration float64
	success  bool
	bytes    int64
}

type headCall struct {
	duration float64
	success  bool
}

type deleteCall struct {
	duration float64
	success  bool
}

type listCall struct {
	duration float64
	success  bool
}

func (m *mockMetrics) RecordPut(duration float64, success bool, bytes int64) {
	m.puts = append(m.puts, putCall{duration, success, bytes})
}

func (m *mockMetrics) RecordGet(duration float64, success bool, bytes int64) {
	m.gets = append(m.gets, getCall{duration, success, bytes})
}

func (m *mockMetrics) RecordGetRange(duration float64, success bool, bytes int64) {
	m.getRanges = append(m.getRanges, getRangeCall{duration, success, bytes})
}

func (m *mockMetrics) RecordHead(duration float64, success bool) {
	m.heads = append(m.heads, headCall{duration, success})
}

func (m *mockMetrics) RecordDelete(duration float64, success bool) {
	m.deletes = append(m.deletes, deleteCall{duration, success})
}

func (m *mockMetrics) RecordList(duration float64, success bool) {
	m.lists = append(m.lists, listCall{duration, success})
}

// mockStore is a simple in-memory store for testing.
type mockStore struct {
	data   map[string][]byte
	closed bool
}

func newMockStore() *mockStore {
	return &mockStore{data: make(map[string][]byte)}
}

func (s *mockStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.data[key] = data
	return nil
}

func (s *mockStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts PutOptions) error {
	return s.Put(ctx, key, reader, size, contentType)
}

func (s *mockStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *mockStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	data, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	if start < 0 || end < start || int(end) >= len(data) {
		return nil, ErrInvalidRange
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (s *mockStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	data, ok := s.data[key]
	if !ok {
		return ObjectMeta{}, ErrNotFound
	}
	return ObjectMeta{Key: key, Size: int64(len(data))}, nil
}

func (s *mockStore) Delete(ctx context.Context, key string) error {
	delete(s.data, key)
	return nil
}

func (s *mockStore) List(ctx context.Context, prefix string) ([]ObjectMeta, error) {
	var result []ObjectMeta
	for key, data := range s.data {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, ObjectMeta{Key: key, Size: int64(len(data))})
		}
	}
	return result, nil
}

func (s *mockStore) Close() error {
	s.closed = true
	return nil
}

func TestInstrumentedStore_Put(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	data := []byte("test data")
	err := instrumented.Put(ctx, "key1", bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if len(metrics.puts) != 1 {
		t.Fatalf("Expected 1 put call, got %d", len(metrics.puts))
	}
	if !metrics.puts[0].success {
		t.Error("Expected success=true")
	}
	if metrics.puts[0].bytes != int64(len(data)) {
		t.Errorf("Expected bytes=%d, got %d", len(data), metrics.puts[0].bytes)
	}
	if metrics.puts[0].duration <= 0 {
		t.Error("Expected positive duration")
	}
}

func TestInstrumentedStore_PutWithOptions(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	data := []byte("test data with options")
	err := instrumented.PutWithOptions(ctx, "key2", bytes.NewReader(data), int64(len(data)), "text/plain", PutOptions{})
	if err != nil {
		t.Fatalf("PutWithOptions failed: %v", err)
	}

	if len(metrics.puts) != 1 {
		t.Fatalf("Expected 1 put call, got %d", len(metrics.puts))
	}
	if !metrics.puts[0].success {
		t.Error("Expected success=true")
	}
}

func TestInstrumentedStore_Get_Success(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	data := []byte("test data for get")
	store.data["key1"] = data

	rc, err := instrumented.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	readData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Error("Read data doesn't match")
	}

	err = rc.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Metrics should be recorded on close
	if len(metrics.gets) != 1 {
		t.Fatalf("Expected 1 get call, got %d", len(metrics.gets))
	}
	if !metrics.gets[0].success {
		t.Error("Expected success=true")
	}
	if metrics.gets[0].bytes != int64(len(data)) {
		t.Errorf("Expected bytes=%d, got %d", len(data), metrics.gets[0].bytes)
	}
}

func TestInstrumentedStore_Get_NotFound(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	_, err := instrumented.Get(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound, got: %v", err)
	}

	// Failure should be recorded immediately
	if len(metrics.gets) != 1 {
		t.Fatalf("Expected 1 get call, got %d", len(metrics.gets))
	}
	if metrics.gets[0].success {
		t.Error("Expected success=false")
	}
	if metrics.gets[0].bytes != 0 {
		t.Errorf("Expected bytes=0, got %d", metrics.gets[0].bytes)
	}
}

func TestInstrumentedStore_GetRange_Success(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	data := []byte("0123456789")
	store.data["key1"] = data

	rc, err := instrumented.GetRange(ctx, "key1", 2, 5)
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}

	readData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(readData) != "2345" {
		t.Errorf("Expected '2345', got '%s'", string(readData))
	}

	err = rc.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Metrics should be recorded on close
	if len(metrics.getRanges) != 1 {
		t.Fatalf("Expected 1 getRange call, got %d", len(metrics.getRanges))
	}
	if !metrics.getRanges[0].success {
		t.Error("Expected success=true")
	}
	if metrics.getRanges[0].bytes != 4 {
		t.Errorf("Expected bytes=4, got %d", metrics.getRanges[0].bytes)
	}
}

func TestInstrumentedStore_GetRange_NotFound(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	_, err := instrumented.GetRange(ctx, "nonexistent", 0, 10)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound, got: %v", err)
	}

	if len(metrics.getRanges) != 1 {
		t.Fatalf("Expected 1 getRange call, got %d", len(metrics.getRanges))
	}
	if metrics.getRanges[0].success {
		t.Error("Expected success=false")
	}
}

func TestInstrumentedStore_Head_Success(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	store.data["key1"] = []byte("test data")

	meta, err := instrumented.Head(ctx, "key1")
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if meta.Key != "key1" {
		t.Errorf("Expected key='key1', got '%s'", meta.Key)
	}

	if len(metrics.heads) != 1 {
		t.Fatalf("Expected 1 head call, got %d", len(metrics.heads))
	}
	if !metrics.heads[0].success {
		t.Error("Expected success=true")
	}
}

func TestInstrumentedStore_Head_NotFound(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	_, err := instrumented.Head(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound, got: %v", err)
	}

	if len(metrics.heads) != 1 {
		t.Fatalf("Expected 1 head call, got %d", len(metrics.heads))
	}
	if metrics.heads[0].success {
		t.Error("Expected success=false")
	}
}

func TestInstrumentedStore_Delete(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	store.data["key1"] = []byte("test")

	err := instrumented.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if len(metrics.deletes) != 1 {
		t.Fatalf("Expected 1 delete call, got %d", len(metrics.deletes))
	}
	if !metrics.deletes[0].success {
		t.Error("Expected success=true")
	}
}

func TestInstrumentedStore_List(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	store.data["prefix/key1"] = []byte("1")
	store.data["prefix/key2"] = []byte("2")

	result, err := instrumented.List(ctx, "prefix/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result))
	}

	if len(metrics.lists) != 1 {
		t.Fatalf("Expected 1 list call, got %d", len(metrics.lists))
	}
	if !metrics.lists[0].success {
		t.Error("Expected success=true")
	}
}

func TestInstrumentedStore_Close(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	err := instrumented.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !store.closed {
		t.Error("Underlying store should be closed")
	}
}

func TestInstrumentedStore_NilMetrics(t *testing.T) {
	store := newMockStore()
	instrumented := NewInstrumentedStore(store, nil)

	ctx := context.Background()
	data := []byte("test")

	// All operations should work without metrics
	err := instrumented.Put(ctx, "key1", bytes.NewReader(data), int64(len(data)), "text/plain")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	rc, err := instrumented.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	rc.Close()

	rc, err = instrumented.GetRange(ctx, "key1", 0, 3)
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}
	rc.Close()

	_, err = instrumented.Head(ctx, "key1")
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}

	_, err = instrumented.List(ctx, "")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	err = instrumented.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	err = instrumented.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestInstrumentedReadCloser_DoubleClose(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	store.data["key1"] = []byte("test data")

	rc, err := instrumented.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Close twice should be safe
	err = rc.Close()
	if err != nil {
		t.Fatalf("First close failed: %v", err)
	}
	err = rc.Close()
	if err != nil {
		t.Fatalf("Second close failed: %v", err)
	}

	// Should only record metrics once
	if len(metrics.gets) != 1 {
		t.Errorf("Expected 1 get call (double close protection), got %d", len(metrics.gets))
	}
}

func TestInstrumentedReadCloser_PartialRead(t *testing.T) {
	store := newMockStore()
	metrics := &mockMetrics{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()
	store.data["key1"] = []byte("0123456789")

	rc, err := instrumented.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Only read 5 bytes
	buf := make([]byte, 5)
	n, err := rc.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 5 {
		t.Errorf("Expected to read 5 bytes, got %d", n)
	}

	err = rc.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should record only the bytes actually read
	if len(metrics.gets) != 1 {
		t.Fatalf("Expected 1 get call, got %d", len(metrics.gets))
	}
	if metrics.gets[0].bytes != 5 {
		t.Errorf("Expected bytes=5 (partial read), got %d", metrics.gets[0].bytes)
	}
}
