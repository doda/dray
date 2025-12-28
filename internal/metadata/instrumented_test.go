package metadata

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockMetricsRecorder tracks calls for testing.
type mockMetricsRecorder struct {
	getCalls         []recordedCall
	putCalls         []recordedCall
	deleteCalls      []recordedCall
	listCalls        []recordedCall
	txnCalls         []recordedCall
	putEphemeralCalls []recordedCall
	retryCalls       []string
}

type recordedCall struct {
	duration float64
	success  bool
}

func (m *mockMetricsRecorder) RecordGet(duration float64, success bool) {
	m.getCalls = append(m.getCalls, recordedCall{duration, success})
}

func (m *mockMetricsRecorder) RecordPut(duration float64, success bool) {
	m.putCalls = append(m.putCalls, recordedCall{duration, success})
}

func (m *mockMetricsRecorder) RecordDelete(duration float64, success bool) {
	m.deleteCalls = append(m.deleteCalls, recordedCall{duration, success})
}

func (m *mockMetricsRecorder) RecordList(duration float64, success bool) {
	m.listCalls = append(m.listCalls, recordedCall{duration, success})
}

func (m *mockMetricsRecorder) RecordTxn(duration float64, success bool) {
	m.txnCalls = append(m.txnCalls, recordedCall{duration, success})
}

func (m *mockMetricsRecorder) RecordPutEphemeral(duration float64, success bool) {
	m.putEphemeralCalls = append(m.putEphemeralCalls, recordedCall{duration, success})
}

func (m *mockMetricsRecorder) RecordRetry(operation string) {
	m.retryCalls = append(m.retryCalls, operation)
}

// slowMockStore adds configurable delays to operations.
type slowMockStore struct {
	delay time.Duration
	data  map[string][]byte
	err   error
}

func newSlowMockStore(delay time.Duration) *slowMockStore {
	return &slowMockStore{
		delay: delay,
		data:  make(map[string][]byte),
	}
}

func (s *slowMockStore) Get(ctx context.Context, key string) (GetResult, error) {
	time.Sleep(s.delay)
	if s.err != nil {
		return GetResult{}, s.err
	}
	if v, ok := s.data[key]; ok {
		return GetResult{Value: v, Version: 1, Exists: true}, nil
	}
	return GetResult{Exists: false}, nil
}

func (s *slowMockStore) Put(ctx context.Context, key string, value []byte, opts ...PutOption) (Version, error) {
	time.Sleep(s.delay)
	if s.err != nil {
		return 0, s.err
	}
	s.data[key] = value
	return 1, nil
}

func (s *slowMockStore) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	time.Sleep(s.delay)
	if s.err != nil {
		return s.err
	}
	delete(s.data, key)
	return nil
}

func (s *slowMockStore) List(ctx context.Context, startKey, endKey string, limit int) ([]KV, error) {
	time.Sleep(s.delay)
	if s.err != nil {
		return nil, s.err
	}
	return []KV{}, nil
}

func (s *slowMockStore) Txn(ctx context.Context, scopeKey string, fn func(Txn) error) error {
	time.Sleep(s.delay)
	if s.err != nil {
		return s.err
	}
	return nil
}

func (s *slowMockStore) Notifications(ctx context.Context) (NotificationStream, error) {
	if s.err != nil {
		return nil, s.err
	}
	return nil, nil
}

func (s *slowMockStore) PutEphemeral(ctx context.Context, key string, value []byte, opts ...EphemeralOption) (Version, error) {
	time.Sleep(s.delay)
	if s.err != nil {
		return 0, s.err
	}
	s.data[key] = value
	return 1, nil
}

func (s *slowMockStore) Close() error {
	return nil
}

func TestInstrumentedStore_Get(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	store.data["key1"] = []byte("value1")
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	// Successful get
	result, err := instrumented.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Exists {
		t.Error("expected key to exist")
	}
	if string(result.Value) != "value1" {
		t.Errorf("expected value1, got %s", string(result.Value))
	}

	// Check metrics recorded
	if len(metrics.getCalls) != 1 {
		t.Fatalf("expected 1 get call, got %d", len(metrics.getCalls))
	}
	if !metrics.getCalls[0].success {
		t.Error("expected success=true")
	}
	if metrics.getCalls[0].duration <= 0 {
		t.Error("expected positive duration")
	}
}

func TestInstrumentedStore_Get_Error(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	store.err = errors.New("connection failed")
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	_, err := instrumented.Get(ctx, "key1")
	if err == nil {
		t.Fatal("expected error")
	}

	if len(metrics.getCalls) != 1 {
		t.Fatalf("expected 1 get call, got %d", len(metrics.getCalls))
	}
	if metrics.getCalls[0].success {
		t.Error("expected success=false")
	}
}

func TestInstrumentedStore_Put(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	_, err := instrumented.Put(ctx, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(metrics.putCalls) != 1 {
		t.Fatalf("expected 1 put call, got %d", len(metrics.putCalls))
	}
	if !metrics.putCalls[0].success {
		t.Error("expected success=true")
	}
}

func TestInstrumentedStore_Delete(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	err := instrumented.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(metrics.deleteCalls) != 1 {
		t.Fatalf("expected 1 delete call, got %d", len(metrics.deleteCalls))
	}
	if !metrics.deleteCalls[0].success {
		t.Error("expected success=true")
	}
}

func TestInstrumentedStore_List(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	_, err := instrumented.List(ctx, "prefix/", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(metrics.listCalls) != 1 {
		t.Fatalf("expected 1 list call, got %d", len(metrics.listCalls))
	}
	if !metrics.listCalls[0].success {
		t.Error("expected success=true")
	}
}

func TestInstrumentedStore_Txn(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	err := instrumented.Txn(ctx, "scope", func(txn Txn) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(metrics.txnCalls) != 1 {
		t.Fatalf("expected 1 txn call, got %d", len(metrics.txnCalls))
	}
	if !metrics.txnCalls[0].success {
		t.Error("expected success=true")
	}
}

func TestInstrumentedStore_PutEphemeral(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	_, err := instrumented.PutEphemeral(ctx, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(metrics.putEphemeralCalls) != 1 {
		t.Fatalf("expected 1 put_ephemeral call, got %d", len(metrics.putEphemeralCalls))
	}
	if !metrics.putEphemeralCalls[0].success {
		t.Error("expected success=true")
	}
}

func TestInstrumentedStore_NilMetrics(t *testing.T) {
	store := newSlowMockStore(time.Millisecond)
	instrumented := NewInstrumentedStore(store, nil)

	ctx := context.Background()

	// Operations should work without metrics
	_, err := instrumented.Put(ctx, "key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := instrumented.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Exists {
		t.Error("expected key to exist")
	}

	err = instrumented.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = instrumented.List(ctx, "prefix/", "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = instrumented.Txn(ctx, "scope", func(txn Txn) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = instrumented.PutEphemeral(ctx, "key2", []byte("value2"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInstrumentedStore_Notifications(t *testing.T) {
	store := newSlowMockStore(0)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	// Notifications pass through without metrics
	_, err := instrumented.Notifications(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInstrumentedStore_Close(t *testing.T) {
	store := newSlowMockStore(0)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	err := instrumented.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInstrumentedStore_LatencyMeasurement(t *testing.T) {
	delay := 10 * time.Millisecond
	store := newSlowMockStore(delay)
	metrics := &mockMetricsRecorder{}
	instrumented := NewInstrumentedStore(store, metrics)

	ctx := context.Background()

	_, err := instrumented.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(metrics.getCalls) != 1 {
		t.Fatalf("expected 1 get call, got %d", len(metrics.getCalls))
	}

	// Duration should be at least the delay
	if metrics.getCalls[0].duration < delay.Seconds()*0.9 {
		t.Errorf("expected duration >= %v, got %v", delay.Seconds()*0.9, metrics.getCalls[0].duration)
	}
}
