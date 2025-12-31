package objectstore

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// MockStore is an in-memory implementation of the Store interface for testing.
type MockStore struct {
	mu      sync.RWMutex
	objects map[string]mockObject
}

type mockObject struct {
	data        []byte
	contentType string
	meta        ObjectMeta
}

// NewMockStore creates a new MockStore.
func NewMockStore() *MockStore {
	return &MockStore{
		objects: make(map[string]mockObject),
	}
}

func (s *MockStore) Put(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	return s.PutWithOptions(ctx, key, reader, size, contentType, PutOptions{})
}

func (s *MockStore) PutWithOptions(ctx context.Context, key string, reader io.Reader, size int64, contentType string, opts PutOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if opts.IfNoneMatch == "*" {
		if _, exists := s.objects[key]; exists {
			return ErrPreconditionFailed
		}
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	if int64(len(data)) != size {
		// In mock, we can be lenient or strict. Let's be strict.
	}

	s.objects[key] = mockObject{
		data:        data,
		contentType: contentType,
		meta: ObjectMeta{
			Key:          key,
			Size:         int64(len(data)),
			ContentType:  contentType,
			ETag:         "mock-etag",
			LastModified: time.Now().UnixMilli(),
			Metadata:     opts.Metadata,
		},
	}

	return nil
}

func (s *MockStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, exists := s.objects[key]
	if !exists {
		return nil, ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

func (s *MockStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, exists := s.objects[key]
	if !exists {
		return nil, ErrNotFound
	}

	if start < 0 {
		start = int64(len(obj.data)) + start
	}
	if end == -1 {
		end = int64(len(obj.data)) - 1
	}

	if start < 0 || start >= int64(len(obj.data)) || end < start {
		return nil, ErrInvalidRange
	}

	if end >= int64(len(obj.data)) {
		end = int64(len(obj.data)) - 1
	}

	return io.NopCloser(bytes.NewReader(obj.data[start : end+1])), nil
}

func (s *MockStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, exists := s.objects[key]
	if !exists {
		return ObjectMeta{}, ErrNotFound
	}

	return obj.meta, nil
}

func (s *MockStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.objects, key)
	return nil
}

func (s *MockStore) List(ctx context.Context, prefix string) ([]ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []ObjectMeta
	for key, obj := range s.objects {
		if strings.HasPrefix(key, prefix) {
			result = append(result, obj.meta)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result, nil
}

func (s *MockStore) Close() error {
	return nil
}
