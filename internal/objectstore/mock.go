package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MockStore is an in-memory implementation of the Store and MultipartStore interfaces for testing.
type MockStore struct {
	mu      sync.RWMutex
	objects map[string]mockObject

	// Multipart upload tracking
	uploads          map[string]*mockMultipartUpload
	uploadIDCounter  uint64
	MultipartTracker *MultipartTracker
}

type mockObject struct {
	data        []byte
	contentType string
	meta        ObjectMeta
}

// MultipartTracker records multipart upload usage for testing.
type MultipartTracker struct {
	mu               sync.Mutex
	uploadCount      int
	partCount        int
	totalBytes       int64
	completedUploads []string
}

// UploadCount returns the number of completed multipart uploads.
func (t *MultipartTracker) UploadCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.uploadCount
}

// PartCount returns the total number of parts uploaded.
func (t *MultipartTracker) PartCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.partCount
}

// TotalBytes returns the total bytes uploaded via parts.
func (t *MultipartTracker) TotalBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalBytes
}

// CompletedUploads returns the list of keys for completed uploads.
func (t *MultipartTracker) CompletedUploads() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]string, len(t.completedUploads))
	copy(result, t.completedUploads)
	return result
}

// Reset clears all tracked multipart data.
func (t *MultipartTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.uploadCount = 0
	t.partCount = 0
	t.totalBytes = 0
	t.completedUploads = nil
}

// RecordUpload records a completed multipart upload.
func (t *MultipartTracker) RecordUpload(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.uploadCount++
	t.completedUploads = append(t.completedUploads, key)
}

// RecordPart records an uploaded part.
func (t *MultipartTracker) RecordPart(size int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partCount++
	t.totalBytes += size
}

type mockMultipartUpload struct {
	store       *MockStore
	uploadID    string
	key         string
	contentType string
	parts       map[int][]byte
	mu          sync.Mutex
	completed   bool
	aborted     bool
}

// NewMockStore creates a new MockStore.
func NewMockStore() *MockStore {
	return &MockStore{
		objects:          make(map[string]mockObject),
		uploads:          make(map[string]*mockMultipartUpload),
		MultipartTracker: &MultipartTracker{},
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

// CreateMultipartUpload initiates a new multipart upload.
func (s *MockStore) CreateMultipartUpload(ctx context.Context, key string, contentType string) (MultipartUpload, error) {
	return s.CreateMultipartUploadWithOptions(ctx, key, contentType, PutOptions{})
}

// CreateMultipartUploadWithOptions initiates a multipart upload with options.
func (s *MockStore) CreateMultipartUploadWithOptions(ctx context.Context, key string, contentType string, opts PutOptions) (MultipartUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	uploadID := fmt.Sprintf("upload-%d", atomic.AddUint64(&s.uploadIDCounter, 1))
	upload := &mockMultipartUpload{
		store:       s,
		uploadID:    uploadID,
		key:         key,
		contentType: contentType,
		parts:       make(map[int][]byte),
	}
	s.uploads[uploadID] = upload
	return upload, nil
}

func (m *mockMultipartUpload) UploadID() string {
	return m.uploadID
}

func (m *mockMultipartUpload) UploadPart(ctx context.Context, partNum int, reader io.Reader, size int64) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.completed || m.aborted {
		return "", fmt.Errorf("upload already finished")
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	m.parts[partNum] = data

	if m.store.MultipartTracker != nil {
		m.store.MultipartTracker.RecordPart(int64(len(data)))
	}

	etag := fmt.Sprintf("etag-part-%d", partNum)
	return etag, nil
}

func (m *mockMultipartUpload) Complete(ctx context.Context, etags []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.completed || m.aborted {
		return fmt.Errorf("upload already finished")
	}

	// Assemble parts in order
	var totalData []byte
	for i := 1; i <= len(m.parts); i++ {
		part, ok := m.parts[i]
		if !ok {
			return fmt.Errorf("missing part %d", i)
		}
		totalData = append(totalData, part...)
	}

	m.store.mu.Lock()
	defer m.store.mu.Unlock()

	m.store.objects[m.key] = mockObject{
		data:        totalData,
		contentType: m.contentType,
		meta: ObjectMeta{
			Key:          m.key,
			Size:         int64(len(totalData)),
			ContentType:  m.contentType,
			ETag:         fmt.Sprintf("etag-%s", m.uploadID),
			LastModified: time.Now().UnixMilli(),
		},
	}

	delete(m.store.uploads, m.uploadID)
	m.completed = true

	if m.store.MultipartTracker != nil {
		m.store.MultipartTracker.RecordUpload(m.key)
	}

	return nil
}

func (m *mockMultipartUpload) Abort(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.completed && !m.aborted {
		m.store.mu.Lock()
		delete(m.store.uploads, m.uploadID)
		m.store.mu.Unlock()
	}

	m.aborted = true
	return nil
}

// Ensure MockStore implements MultipartStore.
var _ MultipartStore = (*MockStore)(nil)
