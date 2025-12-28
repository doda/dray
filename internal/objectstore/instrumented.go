package objectstore

import (
	"context"
	"io"
	"time"
)

// ObjectStoreMetricsRecorder is the interface for recording object store operation metrics.
// This allows the objectstore package to be decoupled from the metrics package.
type ObjectStoreMetricsRecorder interface {
	RecordPut(durationSeconds float64, success bool, bytes int64)
	RecordGet(durationSeconds float64, success bool, bytes int64)
	RecordGetRange(durationSeconds float64, success bool, bytes int64)
	RecordHead(durationSeconds float64, success bool)
	RecordDelete(durationSeconds float64, success bool)
	RecordList(durationSeconds float64, success bool)
}

// InstrumentedStore wraps a Store and records metrics for each operation.
type InstrumentedStore struct {
	store   Store
	metrics ObjectStoreMetricsRecorder
}

// NewInstrumentedStore creates an instrumented wrapper around a Store.
// If metrics is nil, no metrics are recorded and operations pass through directly.
func NewInstrumentedStore(store Store, metrics ObjectStoreMetricsRecorder) *InstrumentedStore {
	return &InstrumentedStore{
		store:   store,
		metrics: metrics,
	}
}

// Put stores an object at the given key.
func (s *InstrumentedStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	start := time.Now()
	err := s.store.Put(ctx, key, reader, size, contentType)
	if s.metrics != nil {
		s.metrics.RecordPut(time.Since(start).Seconds(), err == nil, size)
	}
	return err
}

// PutWithOptions stores an object with additional options.
func (s *InstrumentedStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts PutOptions) error {
	start := time.Now()
	err := s.store.PutWithOptions(ctx, key, reader, size, contentType, opts)
	if s.metrics != nil {
		s.metrics.RecordPut(time.Since(start).Seconds(), err == nil, size)
	}
	return err
}

// Get retrieves an entire object.
func (s *InstrumentedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.store.Get(ctx, key)
	if s.metrics != nil {
		if err != nil {
			s.metrics.RecordGet(time.Since(start).Seconds(), false, 0)
			return nil, err
		}
		// Wrap the reader to track bytes read
		return &instrumentedReadCloser{
			ReadCloser: rc,
			start:      start,
			metrics:    s.metrics,
			recordGet:  true,
		}, nil
	}
	return rc, err
}

// GetRange retrieves a byte range of an object.
func (s *InstrumentedStore) GetRange(ctx context.Context, key string, startByte, end int64) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.store.GetRange(ctx, key, startByte, end)
	if s.metrics != nil {
		if err != nil {
			s.metrics.RecordGetRange(time.Since(start).Seconds(), false, 0)
			return nil, err
		}
		// Wrap the reader to track bytes read
		return &instrumentedReadCloser{
			ReadCloser: rc,
			start:      start,
			metrics:    s.metrics,
			recordGet:  false, // recordGet=false means use RecordGetRange
		}, nil
	}
	return rc, err
}

// Head retrieves object metadata without the body.
func (s *InstrumentedStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	start := time.Now()
	meta, err := s.store.Head(ctx, key)
	if s.metrics != nil {
		s.metrics.RecordHead(time.Since(start).Seconds(), err == nil)
	}
	return meta, err
}

// Delete removes an object.
func (s *InstrumentedStore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := s.store.Delete(ctx, key)
	if s.metrics != nil {
		s.metrics.RecordDelete(time.Since(start).Seconds(), err == nil)
	}
	return err
}

// List returns objects matching the given prefix.
func (s *InstrumentedStore) List(ctx context.Context, prefix string) ([]ObjectMeta, error) {
	start := time.Now()
	result, err := s.store.List(ctx, prefix)
	if s.metrics != nil {
		s.metrics.RecordList(time.Since(start).Seconds(), err == nil)
	}
	return result, err
}

// Close releases resources associated with the store.
func (s *InstrumentedStore) Close() error {
	return s.store.Close()
}

// instrumentedReadCloser wraps a ReadCloser to track bytes read and record metrics on close.
type instrumentedReadCloser struct {
	io.ReadCloser
	start     time.Time
	metrics   ObjectStoreMetricsRecorder
	bytesRead int64
	readErr   bool
	closed    bool
	recordGet bool // true = RecordGet, false = RecordGetRange
}

func (r *instrumentedReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.bytesRead += int64(n)
	if err != nil && err != io.EOF {
		r.readErr = true
	}
	return n, err
}

func (r *instrumentedReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.ReadCloser.Close()
	success := err == nil && !r.readErr
	// Record metrics on close with bytes read
	if r.recordGet {
		r.metrics.RecordGet(time.Since(r.start).Seconds(), success, r.bytesRead)
	} else {
		r.metrics.RecordGetRange(time.Since(r.start).Seconds(), success, r.bytesRead)
	}
	return err
}

// Ensure InstrumentedStore implements Store.
var _ Store = (*InstrumentedStore)(nil)
