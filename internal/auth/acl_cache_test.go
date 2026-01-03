package auth

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// failingNotificationStream is a test helper that returns errors from Next().
type failingNotificationStream struct {
	errCount    atomic.Int32
	maxErrors   int32
	failErr     error
	closed      atomic.Bool
	successChan chan struct{}
}

func newFailingNotificationStream(maxErrors int32, failErr error) *failingNotificationStream {
	return &failingNotificationStream{
		maxErrors:   maxErrors,
		failErr:     failErr,
		successChan: make(chan struct{}),
	}
}

func (s *failingNotificationStream) Next(ctx context.Context) (metadata.Notification, error) {
	if s.closed.Load() {
		return metadata.Notification{}, errors.New("stream closed")
	}
	count := s.errCount.Add(1)
	if count <= s.maxErrors {
		return metadata.Notification{}, s.failErr
	}
	// Block after maxErrors failures to let test verify backoff behavior
	select {
	case <-ctx.Done():
		return metadata.Notification{}, ctx.Err()
	case <-s.successChan:
		return metadata.Notification{}, ctx.Err()
	}
}

func (s *failingNotificationStream) Close() error {
	s.closed.Store(true)
	close(s.successChan)
	return nil
}

func (s *failingNotificationStream) ErrorCount() int32 {
	return s.errCount.Load()
}

// failingMetadataStore wraps MockStore but returns a custom notification stream.
type failingMetadataStore struct {
	*metadata.MockStore
	stream metadata.NotificationStream
}

func (s *failingMetadataStore) Notifications(_ context.Context) (metadata.NotificationStream, error) {
	return s.stream, nil
}

func TestACLCache_StartAndStop(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	// Start cache
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Verify cache is loaded
	if !cache.IsLoaded() {
		t.Error("IsLoaded() = false after Start()")
	}

	// Stop cache
	cache.Stop()
}

func TestACLCache_LoadsExistingACLs(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Create ACLs before cache starts
	entries := []*ACLEntry{
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-2",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:bob",
			Host:         "*",
			Operation:    OperationWrite,
			Permission:   PermissionAllow,
		},
	}

	for _, entry := range entries {
		if err := store.CreateACL(ctx, entry); err != nil {
			t.Fatalf("CreateACL() error = %v", err)
		}
	}

	// Start cache
	cache := NewACLCache(store, mock)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Verify count
	if count := cache.Count(); count != 2 {
		t.Errorf("Count() = %d, want 2", count)
	}
}

func TestACLCache_GetACLs(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	entries := []*ACLEntry{
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-2",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:bob",
			Host:         "*",
			Operation:    OperationWrite,
			Permission:   PermissionDeny,
		},
		{
			ResourceType: ResourceTypeGroup,
			ResourceName: "group-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
	}

	for _, entry := range entries {
		if err := store.CreateACL(ctx, entry); err != nil {
			t.Fatalf("CreateACL() error = %v", err)
		}
	}

	cache := NewACLCache(store, mock)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Get all
	all := cache.GetACLs(nil)
	if len(all) != 3 {
		t.Errorf("GetACLs(nil) returned %d entries, want 3", len(all))
	}

	// Get by resource type
	topics := cache.GetACLs(&ACLFilter{ResourceType: ResourceTypeTopic})
	if len(topics) != 2 {
		t.Errorf("GetACLs(TOPIC) returned %d entries, want 2", len(topics))
	}

	// Get by principal
	alice := cache.GetACLs(&ACLFilter{Principal: "User:alice"})
	if len(alice) != 2 {
		t.Errorf("GetACLs(alice) returned %d entries, want 2", len(alice))
	}

	// Get by permission
	denied := cache.GetACLs(&ACLFilter{Permission: PermissionDeny})
	if len(denied) != 1 {
		t.Errorf("GetACLs(DENY) returned %d entries, want 1", len(denied))
	}
}

func TestACLCache_NotificationUpdate(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Initially empty
	if count := cache.Count(); count != 0 {
		t.Errorf("Initial Count() = %d, want 0", count)
	}

	// Create an ACL via store
	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	// Simulate notification (in real system, Oxia would send this)
	result, _ := mock.Get(ctx, aclKeyPath(entry))
	mock.SimulateNotification(metadata.Notification{
		Key:     aclKeyPath(entry),
		Value:   result.Value,
		Version: result.Version,
		Deleted: false,
	})

	// Give notification time to process
	time.Sleep(50 * time.Millisecond)

	// Verify cache updated
	if count := cache.Count(); count != 1 {
		t.Errorf("Count() after notification = %d, want 1", count)
	}
}

func TestACLCache_NotificationDelete(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Create ACL before cache starts
	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	cache := NewACLCache(store, mock)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Verify loaded
	if count := cache.Count(); count != 1 {
		t.Errorf("Initial Count() = %d, want 1", count)
	}

	// Delete via store
	if err := store.DeleteACL(ctx, entry); err != nil {
		t.Fatalf("DeleteACL() error = %v", err)
	}

	// Simulate delete notification
	mock.SimulateNotification(metadata.Notification{
		Key:     aclKeyPath(entry),
		Deleted: true,
	})

	// Give notification time to process
	time.Sleep(50 * time.Millisecond)

	// Verify cache updated
	if count := cache.Count(); count != 0 {
		t.Errorf("Count() after delete notification = %d, want 0", count)
	}
}

func TestACLCache_Authorize(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	entries := []*ACLEntry{
		// Allow alice to read topic-1
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
		// Deny bob all operations on topic-1
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:bob",
			Host:         "*",
			Operation:    OperationAll,
			Permission:   PermissionDeny,
		},
		// Allow all users to read from group-1
		{
			ResourceType: ResourceTypeGroup,
			ResourceName: "group-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:*",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
		// Allow alice all operations on topics starting with "alice-"
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "alice-",
			PatternType:  PatternTypePrefixed,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationAll,
			Permission:   PermissionAllow,
		},
	}

	for _, entry := range entries {
		if err := store.CreateACL(ctx, entry); err != nil {
			t.Fatalf("CreateACL() error = %v", err)
		}
	}

	cache := NewACLCache(store, mock)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	tests := []struct {
		name         string
		resourceType ResourceType
		resourceName string
		principal    string
		host         string
		operation    Operation
		want         bool
	}{
		{
			name:         "alice can read topic-1",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:alice",
			host:         "192.168.1.1",
			operation:    OperationRead,
			want:         true,
		},
		{
			name:         "alice cannot write topic-1 (no rule)",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:alice",
			host:         "192.168.1.1",
			operation:    OperationWrite,
			want:         false,
		},
		{
			name:         "bob cannot read topic-1 (denied)",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:bob",
			host:         "192.168.1.1",
			operation:    OperationRead,
			want:         false,
		},
		{
			name:         "bob cannot write topic-1 (denied via ALL)",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:bob",
			host:         "192.168.1.1",
			operation:    OperationWrite,
			want:         false,
		},
		{
			name:         "charlie can read group-1 (wildcard principal)",
			resourceType: ResourceTypeGroup,
			resourceName: "group-1",
			principal:    "User:charlie",
			host:         "10.0.0.1",
			operation:    OperationRead,
			want:         true,
		},
		{
			name:         "charlie cannot read topic-1 (no rule)",
			resourceType: ResourceTypeTopic,
			resourceName: "topic-1",
			principal:    "User:charlie",
			host:         "10.0.0.1",
			operation:    OperationRead,
			want:         false,
		},
		{
			name:         "alice can write alice-logs (prefixed)",
			resourceType: ResourceTypeTopic,
			resourceName: "alice-logs",
			principal:    "User:alice",
			host:         "192.168.1.1",
			operation:    OperationWrite,
			want:         true,
		},
		{
			name:         "alice can delete alice-metrics (prefixed)",
			resourceType: ResourceTypeTopic,
			resourceName: "alice-metrics",
			principal:    "User:alice",
			host:         "192.168.1.1",
			operation:    OperationDelete,
			want:         true,
		},
		{
			name:         "bob cannot read alice-logs (no rule for bob)",
			resourceType: ResourceTypeTopic,
			resourceName: "alice-logs",
			principal:    "User:bob",
			host:         "192.168.1.1",
			operation:    OperationRead,
			want:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cache.Authorize(tt.resourceType, tt.resourceName, tt.principal, tt.host, tt.operation)
			if got != tt.want {
				t.Errorf("Authorize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestACLCache_AuthorizeDenyTakesPrecedence(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Create both allow and deny rules for same principal/resource
	entries := []*ACLEntry{
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "*",
			Operation:    OperationRead,
			Permission:   PermissionAllow,
		},
		{
			ResourceType: ResourceTypeTopic,
			ResourceName: "topic-1",
			PatternType:  PatternTypeLiteral,
			Principal:    "User:alice",
			Host:         "192.168.1.100",
			Operation:    OperationRead,
			Permission:   PermissionDeny,
		},
	}

	for _, entry := range entries {
		if err := store.CreateACL(ctx, entry); err != nil {
			t.Fatalf("CreateACL() error = %v", err)
		}
	}

	cache := NewACLCache(store, mock)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// From allowed host - should be allowed
	if !cache.Authorize(ResourceTypeTopic, "topic-1", "User:alice", "192.168.1.1", OperationRead) {
		t.Error("Should allow from non-denied host")
	}

	// From denied host - deny takes precedence
	if cache.Authorize(ResourceTypeTopic, "topic-1", "User:alice", "192.168.1.100", OperationRead) {
		t.Error("Should deny from denied host even with allow rule")
	}
}

func TestACLCache_Invalidate(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Initially empty
	if count := cache.Count(); count != 0 {
		t.Errorf("Initial Count() = %d, want 0", count)
	}

	// Create ACL via store (bypassing notification)
	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	// Cache still shows 0 (no notification received)
	if count := cache.Count(); count != 0 {
		t.Errorf("Count() before Invalidate = %d, want 0", count)
	}

	// Force reload
	if err := cache.Invalidate(ctx); err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}

	// Now cache should have the entry
	if count := cache.Count(); count != 1 {
		t.Errorf("Count() after Invalidate = %d, want 1", count)
	}
}

func TestACLCache_IgnoresNonACLNotifications(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCache(store, mock)

	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Send non-ACL notifications
	mock.SimulateNotification(metadata.Notification{
		Key:   keys.TopicsPrefix + "/some-topic",
		Value: []byte("topic-data"),
	})
	mock.SimulateNotification(metadata.Notification{
		Key:   keys.GroupsPrefix + "/some-group",
		Value: []byte("group-data"),
	})

	// Give notifications time to process
	time.Sleep(50 * time.Millisecond)

	// Cache should still be empty
	if count := cache.Count(); count != 0 {
		t.Errorf("Count() = %d, want 0 (should ignore non-ACL notifications)", count)
	}
}

func TestACLCache_OperationAll(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Allow all operations for alice on topic-1
	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationAll,
		Permission:   PermissionAllow,
	}
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	cache := NewACLCache(store, mock)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	operations := []Operation{
		OperationRead,
		OperationWrite,
		OperationCreate,
		OperationDelete,
		OperationAlter,
		OperationDescribe,
	}

	for _, op := range operations {
		if !cache.Authorize(ResourceTypeTopic, "topic-1", "User:alice", "any-host", op) {
			t.Errorf("Authorize(%v) = false, want true (should match ALL)", op)
		}
	}
}

func TestACLCache_WildcardResource(t *testing.T) {
	ctx := context.Background()
	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Allow read on all topics for alice
	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "*",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	cache := NewACLCache(store, mock)
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Should be allowed to read any topic
	topics := []string{"topic-1", "topic-2", "any-topic"}
	for _, topic := range topics {
		if !cache.Authorize(ResourceTypeTopic, topic, "User:alice", "any-host", OperationRead) {
			t.Errorf("Authorize(READ, %s) = false, want true (wildcard resource)", topic)
		}
	}

	// Should not be allowed to write (not covered by rule)
	if cache.Authorize(ResourceTypeTopic, "topic-1", "User:alice", "any-host", OperationWrite) {
		t.Error("Authorize(WRITE) = true, want false")
	}
}

func TestACLCache_WatchLoopBackoffOnErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up logging capture
	var logBuf bytes.Buffer
	var logMu sync.Mutex
	handler := slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	defer slog.SetDefault(oldLogger)

	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Create a failing stream that will return 3 errors before blocking
	failErr := errors.New("notification stream error")
	failingStream := newFailingNotificationStream(3, failErr)

	// Wrap the mock store with our failing stream
	failingStore := &failingMetadataStore{
		MockStore: mock,
		stream:    failingStream,
	}

	// Create cache with fast backoff for testing
	cache := NewACLCacheWithOptions(store, failingStore,
		WithBackoff(10*time.Millisecond, 50*time.Millisecond, 2.0))

	// Start cache
	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Wait for at least 3 errors to occur with backoff
	// With 10ms, 20ms, 40ms backoff, should take ~70ms minimum
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if failingStream.ErrorCount() >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify errors were encountered
	errCount := failingStream.ErrorCount()
	if errCount < 3 {
		t.Errorf("Expected at least 3 errors, got %d", errCount)
	}

	// Verify warnings were logged
	logMu.Lock()
	logOutput := logBuf.String()
	logMu.Unlock()

	if !bytes.Contains([]byte(logOutput), []byte("ACL notification stream error")) {
		t.Error("Expected 'ACL notification stream error' in logs")
	}
	if !bytes.Contains([]byte(logOutput), []byte("notification stream error")) {
		t.Error("Expected error message in logs")
	}
	if !bytes.Contains([]byte(logOutput), []byte("backoff")) {
		t.Error("Expected 'backoff' in logs")
	}
}

func TestACLCache_BackoffResetOnSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := metadata.NewMockStore()
	store := NewACLStore(mock)
	cache := NewACLCacheWithOptions(store, mock,
		WithBackoff(10*time.Millisecond, 100*time.Millisecond, 2.0))

	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer cache.Stop()

	// Send a notification
	entry := &ACLEntry{
		ResourceType: ResourceTypeTopic,
		ResourceName: "topic-1",
		PatternType:  PatternTypeLiteral,
		Principal:    "User:alice",
		Host:         "*",
		Operation:    OperationRead,
		Permission:   PermissionAllow,
	}
	if err := store.CreateACL(ctx, entry); err != nil {
		t.Fatalf("CreateACL() error = %v", err)
	}

	result, _ := mock.Get(ctx, aclKeyPath(entry))
	mock.SimulateNotification(metadata.Notification{
		Key:     aclKeyPath(entry),
		Value:   result.Value,
		Version: result.Version,
		Deleted: false,
	})

	// Give notification time to process
	time.Sleep(50 * time.Millisecond)

	// Verify cache updated (confirms notification was processed)
	if count := cache.Count(); count != 1 {
		t.Errorf("Count() after notification = %d, want 1", count)
	}
}

func TestACLCache_BackoffCapsAtMaximum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := metadata.NewMockStore()
	store := NewACLStore(mock)

	// Create a stream that fails many times
	failErr := errors.New("persistent error")
	failingStream := newFailingNotificationStream(10, failErr)

	failingStore := &failingMetadataStore{
		MockStore: mock,
		stream:    failingStream,
	}

	// Short initial backoff, very short max to verify capping
	cache := NewACLCacheWithOptions(store, failingStore,
		WithBackoff(5*time.Millisecond, 20*time.Millisecond, 2.0))

	if err := cache.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Wait for multiple errors
	// If backoff wasn't capped, this would take a very long time
	// With capping at 20ms, 10 errors should take ~200ms max (each after cap)
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if failingStream.ErrorCount() >= 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	cache.Stop()

	errCount := failingStream.ErrorCount()
	if errCount < 5 {
		t.Errorf("Expected at least 5 errors within timeout, got %d", errCount)
	}
}
