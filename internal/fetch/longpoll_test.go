package fetch

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

func TestHWMWatcher_ImmediateReturn_WhenHWMAboveThreshold(t *testing.T) {
	store := metadata.NewMockStore()
	streamManager := index.NewStreamManager(store)
	watcher := NewHWMWatcher(store, streamManager)
	ctx := context.Background()

	// Create stream with some data (HWM > 0)
	streamID, err := streamManager.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Increment HWM to 100
	_, version, err := streamManager.GetHWM(ctx, streamID)
	if err != nil {
		t.Fatalf("failed to get HWM: %v", err)
	}
	_, _, err = streamManager.IncrementHWM(ctx, streamID, 100, version)
	if err != nil {
		t.Fatalf("failed to increment HWM: %v", err)
	}

	// Wait for HWM above threshold 50 - should return immediately since HWM=100
	start := time.Now()
	result, err := watcher.WaitForHWM(ctx, streamID, 50, 5*time.Second)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if elapsed > 100*time.Millisecond {
		t.Errorf("expected immediate return, but took %v", elapsed)
	}

	if !result.NewDataAvailable {
		t.Error("expected NewDataAvailable to be true")
	}

	if result.CurrentHWM != 100 {
		t.Errorf("expected CurrentHWM=100, got %d", result.CurrentHWM)
	}

	if result.TimedOut {
		t.Error("expected TimedOut to be false")
	}
}

func TestHWMWatcher_Timeout_WhenNoNewData(t *testing.T) {
	store := metadata.NewMockStore()
	streamManager := index.NewStreamManager(store)
	watcher := NewHWMWatcher(store, streamManager)
	ctx := context.Background()

	// Create empty stream (HWM=0)
	streamID, err := streamManager.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Wait for HWM above threshold 0 - should timeout since HWM=0
	start := time.Now()
	result, err := watcher.WaitForHWM(ctx, streamID, 0, 100*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have waited close to the timeout
	if elapsed < 90*time.Millisecond {
		t.Errorf("expected to wait close to timeout, but only waited %v", elapsed)
	}

	if result.NewDataAvailable {
		t.Error("expected NewDataAvailable to be false")
	}

	if result.CurrentHWM != 0 {
		t.Errorf("expected CurrentHWM=0, got %d", result.CurrentHWM)
	}

	if !result.TimedOut {
		t.Error("expected TimedOut to be true")
	}
}

func TestHWMWatcher_WakeOnHWMIncrease(t *testing.T) {
	store := metadata.NewMockStore()
	streamManager := index.NewStreamManager(store)
	watcher := NewHWMWatcher(store, streamManager)
	ctx := context.Background()

	// Create empty stream (HWM=0)
	streamID, err := streamManager.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Start waiting in a goroutine
	resultCh := make(chan *WaitResult)
	errCh := make(chan error)

	go func() {
		result, err := watcher.WaitForHWM(ctx, streamID, 0, 5*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	// Wait a bit then simulate HWM notification
	time.Sleep(50 * time.Millisecond)

	// Simulate HWM update by sending notification
	hwmKey := keys.HwmKeyPath(streamID)
	store.SimulateNotification(metadata.Notification{
		Key:     hwmKey,
		Value:   index.EncodeHWM(10),
		Version: 2,
		Deleted: false,
	})

	// Wait for result with timeout
	select {
	case result := <-resultCh:
		if !result.NewDataAvailable {
			t.Error("expected NewDataAvailable to be true")
		}
		if result.CurrentHWM != 10 {
			t.Errorf("expected CurrentHWM=10, got %d", result.CurrentHWM)
		}
		if result.TimedOut {
			t.Error("expected TimedOut to be false")
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func TestHWMWatcher_IgnoreOtherNotifications(t *testing.T) {
	store := metadata.NewMockStore()
	streamManager := index.NewStreamManager(store)
	watcher := NewHWMWatcher(store, streamManager)
	ctx := context.Background()

	// Create empty stream (HWM=0)
	streamID, err := streamManager.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Start waiting in a goroutine
	resultCh := make(chan *WaitResult)
	errCh := make(chan error)

	go func() {
		result, err := watcher.WaitForHWM(ctx, streamID, 0, 500*time.Millisecond)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	// Wait a bit then send unrelated notifications
	time.Sleep(20 * time.Millisecond)

	// Notification for a different stream
	store.SimulateNotification(metadata.Notification{
		Key:     keys.HwmKeyPath("other-stream-id"),
		Value:   index.EncodeHWM(100),
		Version: 2,
		Deleted: false,
	})

	// Non-HWM notification
	store.SimulateNotification(metadata.Notification{
		Key:     "/dray/v1/topics/test-topic",
		Value:   []byte("topic data"),
		Version: 3,
		Deleted: false,
	})

	// Wait for result - should timeout since our stream's HWM wasn't updated
	select {
	case result := <-resultCh:
		if result.NewDataAvailable {
			t.Error("expected NewDataAvailable to be false")
		}
		if !result.TimedOut {
			t.Error("expected TimedOut to be true")
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func TestHWMWatcher_HandleStreamDeletion(t *testing.T) {
	store := metadata.NewMockStore()
	streamManager := index.NewStreamManager(store)
	watcher := NewHWMWatcher(store, streamManager)
	ctx := context.Background()

	// Create empty stream (HWM=0)
	streamID, err := streamManager.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Start waiting in a goroutine
	resultCh := make(chan *WaitResult)
	errCh := make(chan error)

	go func() {
		result, err := watcher.WaitForHWM(ctx, streamID, 0, 5*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	// Wait a bit then simulate HWM deletion (stream deleted)
	time.Sleep(50 * time.Millisecond)

	hwmKey := keys.HwmKeyPath(streamID)
	store.SimulateNotification(metadata.Notification{
		Key:     hwmKey,
		Value:   nil,
		Version: 2,
		Deleted: true,
	})

	// Wait for result
	select {
	case result := <-resultCh:
		if result.NewDataAvailable {
			t.Error("expected NewDataAvailable to be false for deleted stream")
		}
		if result.CurrentHWM != 0 {
			t.Errorf("expected CurrentHWM=0 for deleted stream, got %d", result.CurrentHWM)
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func TestHWMWatcher_ContextCancellation(t *testing.T) {
	store := metadata.NewMockStore()
	streamManager := index.NewStreamManager(store)
	watcher := NewHWMWatcher(store, streamManager)

	// Create empty stream (HWM=0)
	ctx := context.Background()
	streamID, err := streamManager.CreateStream(ctx, "test-topic", 0)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Create cancellable context
	cancelCtx, cancel := context.WithCancel(ctx)

	// Start waiting in a goroutine
	resultCh := make(chan *WaitResult)
	errCh := make(chan error)

	go func() {
		result, err := watcher.WaitForHWM(cancelCtx, streamID, 0, 5*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- result
	}()

	// Wait a bit then cancel context
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for result - should return quickly after cancellation
	select {
	case result := <-resultCh:
		if result.NewDataAvailable {
			t.Error("expected NewDataAvailable to be false after cancellation")
		}
		if !result.TimedOut {
			t.Error("expected TimedOut to be true after cancellation")
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func TestHWMWatcher_StreamNotFound(t *testing.T) {
	store := metadata.NewMockStore()
	streamManager := index.NewStreamManager(store)
	watcher := NewHWMWatcher(store, streamManager)
	ctx := context.Background()

	// Try to wait on non-existent stream
	_, err := watcher.WaitForHWM(ctx, "nonexistent-stream", 0, 100*time.Millisecond)

	if err == nil {
		t.Error("expected error for non-existent stream")
	}
}

func TestExtractStreamIDFromHWMKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "valid hwm key",
			key:      "/dray/v1/streams/abc123/hwm",
			expected: "abc123",
		},
		{
			name:     "valid hwm key with UUID",
			key:      "/dray/v1/streams/550e8400-e29b-41d4-a716-446655440000/hwm",
			expected: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "not an hwm key - wrong suffix",
			key:      "/dray/v1/streams/abc123/meta",
			expected: "",
		},
		{
			name:     "not an hwm key - wrong prefix",
			key:      "/other/prefix/abc123/hwm",
			expected: "",
		},
		{
			name:     "not an hwm key - nested path",
			key:      "/dray/v1/streams/abc123/nested/hwm",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractStreamIDFromHWMKey(tt.key)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}
