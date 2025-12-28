package oxia

import (
	"context"
	"errors"
	"testing"
	"time"

	oxiaclient "github.com/oxia-db/oxia/oxia"

	"github.com/dray-io/dray/internal/metadata"
)

// mockNotifications implements oxiaclient.Notifications for testing.
type mockNotifications struct {
	ch        chan *oxiaclient.Notification
	closed    bool
	closeErr  error
	closeCh   chan struct{}
}

func newMockNotifications() *mockNotifications {
	return &mockNotifications{
		ch:      make(chan *oxiaclient.Notification, 10),
		closeCh: make(chan struct{}),
	}
}

func (m *mockNotifications) Ch() <-chan *oxiaclient.Notification {
	return m.ch
}

func (m *mockNotifications) Close() error {
	if !m.closed {
		m.closed = true
		close(m.closeCh)
	}
	return m.closeErr
}

// TestConvertNotification verifies that Oxia notifications are correctly
// converted to metadata.Notification.
func TestConvertNotification(t *testing.T) {
	tests := []struct {
		name     string
		input    *oxiaclient.Notification
		expected metadata.Notification
	}{
		{
			name: "key created",
			input: &oxiaclient.Notification{
				Type:      oxiaclient.KeyCreated,
				Key:       "/dray/v1/topics/test",
				VersionId: 1,
			},
			expected: metadata.Notification{
				Key:     "/dray/v1/topics/test",
				Version: 1,
				Deleted: false,
			},
		},
		{
			name: "key modified",
			input: &oxiaclient.Notification{
				Type:      oxiaclient.KeyModified,
				Key:       "/dray/v1/streams/abc/hwm",
				VersionId: 42,
			},
			expected: metadata.Notification{
				Key:     "/dray/v1/streams/abc/hwm",
				Version: 42,
				Deleted: false,
			},
		},
		{
			name: "key deleted",
			input: &oxiaclient.Notification{
				Type:      oxiaclient.KeyDeleted,
				Key:       "/dray/v1/topics/old-topic",
				VersionId: -1,
			},
			expected: metadata.Notification{
				Key:     "/dray/v1/topics/old-topic",
				Version: -1,
				Deleted: true,
			},
		},
		{
			name: "key range deleted",
			input: &oxiaclient.Notification{
				Type:        oxiaclient.KeyRangeRangeDeleted,
				Key:         "/dray/v1/groups/g1/",
				VersionId:   -1,
				KeyRangeEnd: "/dray/v1/groups/g1/~",
			},
			expected: metadata.Notification{
				Key:     "/dray/v1/groups/g1/",
				Version: -1,
				Deleted: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertNotification(tt.input)
			if result.Key != tt.expected.Key {
				t.Errorf("key mismatch: got %q, want %q", result.Key, tt.expected.Key)
			}
			if result.Version != tt.expected.Version {
				t.Errorf("version mismatch: got %d, want %d", result.Version, tt.expected.Version)
			}
			if result.Deleted != tt.expected.Deleted {
				t.Errorf("deleted mismatch: got %v, want %v", result.Deleted, tt.expected.Deleted)
			}
		})
	}
}

// TestNotificationStreamNext verifies that Next properly blocks and returns
// notifications when available.
func TestNotificationStreamNext(t *testing.T) {
	mock := newMockNotifications()
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}

	// Send a notification
	mock.ch <- &oxiaclient.Notification{
		Type:      oxiaclient.KeyCreated,
		Key:       "/dray/v1/test/key",
		VersionId: 100,
	}

	// Receive the notification
	notif, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if notif.Key != "/dray/v1/test/key" {
		t.Errorf("wrong key: got %q", notif.Key)
	}
	if notif.Version != 100 {
		t.Errorf("wrong version: got %d", notif.Version)
	}

	stream.Close()
}

// TestNotificationStreamNextContextCancelled verifies that Next returns
// when the provided context is cancelled.
func TestNotificationStreamNextContextCancelled(t *testing.T) {
	mock := newMockNotifications()

	ctx, cancel := context.WithCancel(context.Background())

	stream := &notificationStream{
		notifications: mock,
		ctx:           context.Background(), // stream's own context
	}

	// Cancel the context before calling Next
	cancel()

	// Next should return immediately with context error
	_, err := stream.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	stream.Close()
}

// TestNotificationStreamNextStreamContextCancelled verifies that Next returns
// when the stream's context is cancelled.
func TestNotificationStreamNextStreamContextCancelled(t *testing.T) {
	mock := newMockNotifications()

	ctx := context.Background()
	streamCtx, streamCancel := context.WithCancel(context.Background())

	stream := &notificationStream{
		notifications: mock,
		ctx:           streamCtx,
	}

	// Cancel the stream's context
	streamCancel()

	// Next should return with the stream context error
	_, err := stream.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	stream.Close()
}

// TestNotificationStreamNextChannelClosed verifies that Next returns
// ErrStoreClosed when the notifications channel is closed.
func TestNotificationStreamNextChannelClosed(t *testing.T) {
	mock := newMockNotifications()
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}

	// Close the channel
	close(mock.ch)

	// Next should return ErrStoreClosed
	_, err := stream.Next(ctx)
	if !errors.Is(err, metadata.ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}
}

// TestNotificationStreamNextTimeout verifies that Next returns when
// a timeout context expires.
func TestNotificationStreamNextTimeout(t *testing.T) {
	mock := newMockNotifications()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	stream := &notificationStream{
		notifications: mock,
		ctx:           context.Background(),
	}

	// Next should return with deadline exceeded
	_, err := stream.Next(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}

	stream.Close()
}

// TestNotificationStreamMultipleNotifications verifies that multiple
// notifications are delivered in order.
func TestNotificationStreamMultipleNotifications(t *testing.T) {
	mock := newMockNotifications()
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}
	defer stream.Close()

	// Send multiple notifications
	for i := 1; i <= 5; i++ {
		mock.ch <- &oxiaclient.Notification{
			Type:      oxiaclient.KeyModified,
			Key:       "/dray/v1/test/key",
			VersionId: int64(i),
		}
	}

	// Receive all notifications in order
	for i := 1; i <= 5; i++ {
		notif, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("Next %d failed: %v", i, err)
		}
		if notif.Version != metadata.Version(i) {
			t.Errorf("notification %d: wrong version: got %d, want %d", i, notif.Version, i)
		}
	}
}

// TestNotificationStreamClose verifies that Close releases resources.
func TestNotificationStreamClose(t *testing.T) {
	mock := newMockNotifications()
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}

	err := stream.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify the mock was closed
	if !mock.closed {
		t.Error("mock should be closed")
	}
}

// TestNotificationStreamCloseError verifies that Close returns errors.
func TestNotificationStreamCloseError(t *testing.T) {
	mock := newMockNotifications()
	mock.closeErr = errors.New("close failed")
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}

	err := stream.Close()
	if err == nil {
		t.Error("expected error from Close")
	}
	if err.Error() != "close failed" {
		t.Errorf("wrong error: %v", err)
	}
}

// TestNotificationDeliveryAfterPut verifies the notification delivery workflow
// using the mock notification stream - simulating what happens after a Put.
func TestNotificationDeliveryAfterPut(t *testing.T) {
	mock := newMockNotifications()
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}
	defer stream.Close()

	key := "/dray/v1/streams/abc123/hwm"

	// Simulate notification that would be sent after a Put operation
	go func() {
		time.Sleep(10 * time.Millisecond)
		mock.ch <- &oxiaclient.Notification{
			Type:      oxiaclient.KeyCreated,
			Key:       key,
			VersionId: 1,
		}
	}()

	// Wait for notification
	notif, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("failed to receive notification: %v", err)
	}

	if notif.Key != key {
		t.Errorf("wrong key: got %q, want %q", notif.Key, key)
	}
	if notif.Version != 1 {
		t.Errorf("wrong version: got %d, want 1", notif.Version)
	}
	if notif.Deleted {
		t.Error("notification should not be deleted")
	}
}

// TestNotificationDeliveryAfterDelete verifies that delete notifications
// are correctly delivered.
func TestNotificationDeliveryAfterDelete(t *testing.T) {
	mock := newMockNotifications()
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}
	defer stream.Close()

	key := "/dray/v1/topics/deleted-topic"

	// Simulate delete notification
	mock.ch <- &oxiaclient.Notification{
		Type:      oxiaclient.KeyDeleted,
		Key:       key,
		VersionId: -1,
	}

	// Receive notification
	notif, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("failed to receive notification: %v", err)
	}

	if notif.Key != key {
		t.Errorf("wrong key: got %q, want %q", notif.Key, key)
	}
	if !notif.Deleted {
		t.Error("notification should be marked as deleted")
	}
}

// TestNotificationStreamRestart simulates stream restart by creating
// a new stream and verifying it can receive new notifications.
// This tests the recovery pattern: when a stream fails, create a new one.
func TestNotificationStreamRestart(t *testing.T) {
	ctx := context.Background()

	// First stream
	mock1 := newMockNotifications()
	stream1 := &notificationStream{
		notifications: mock1,
		ctx:           ctx,
	}

	// Send a notification on first stream
	mock1.ch <- &oxiaclient.Notification{
		Type:      oxiaclient.KeyCreated,
		Key:       "/dray/v1/test/key1",
		VersionId: 1,
	}

	// Receive it
	notif, err := stream1.Next(ctx)
	if err != nil {
		t.Fatalf("stream1 Next failed: %v", err)
	}
	if notif.Key != "/dray/v1/test/key1" {
		t.Errorf("wrong key on stream1")
	}

	// Close the first stream (simulating failure/restart)
	stream1.Close()

	// Create a new stream (simulating restart/recovery)
	mock2 := newMockNotifications()
	stream2 := &notificationStream{
		notifications: mock2,
		ctx:           ctx,
	}
	defer stream2.Close()

	// Send a notification on the new stream
	mock2.ch <- &oxiaclient.Notification{
		Type:      oxiaclient.KeyCreated,
		Key:       "/dray/v1/test/key2",
		VersionId: 2,
	}

	// Should receive on the new stream
	notif, err = stream2.Next(ctx)
	if err != nil {
		t.Fatalf("stream2 Next failed: %v", err)
	}
	if notif.Key != "/dray/v1/test/key2" {
		t.Errorf("wrong key on stream2: got %q", notif.Key)
	}
	if notif.Version != 2 {
		t.Errorf("wrong version on stream2: got %d", notif.Version)
	}
}

// TestNoNotificationsMissedAfterSubscription verifies that once subscribed,
// all subsequent notifications are received. This is a simulation since
// we can't test the actual Oxia guarantee in unit tests.
func TestNoNotificationsMissedAfterSubscription(t *testing.T) {
	mock := newMockNotifications()
	ctx := context.Background()

	stream := &notificationStream{
		notifications: mock,
		ctx:           ctx,
	}
	defer stream.Close()

	// Simulate a sequence of changes happening after subscription
	numNotifications := 100
	go func() {
		for i := 1; i <= numNotifications; i++ {
			mock.ch <- &oxiaclient.Notification{
				Type:      oxiaclient.KeyModified,
				Key:       "/dray/v1/test/key",
				VersionId: int64(i),
			}
		}
	}()

	// Receive all notifications and verify none are missed
	received := make([]int64, 0, numNotifications)
	for i := 0; i < numNotifications; i++ {
		notif, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("failed to receive notification %d: %v", i, err)
		}
		received = append(received, int64(notif.Version))
	}

	// Verify all notifications were received in order
	for i, v := range received {
		expected := int64(i + 1)
		if v != expected {
			t.Errorf("notification %d: got version %d, want %d", i, v, expected)
		}
	}
}

// TestNotificationStreamInterfaceCompliance verifies that notificationStream
// implements metadata.NotificationStream.
func TestNotificationStreamInterfaceCompliance(t *testing.T) {
	var _ metadata.NotificationStream = (*notificationStream)(nil)
}
