// Package fetch provides long-poll support for fetch waiting.
package fetch

import (
	"context"
	"strings"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
)

// WaitResult indicates whether new data is available after waiting.
type WaitResult struct {
	// NewDataAvailable is true if HWM increased during the wait.
	NewDataAvailable bool
	// CurrentHWM is the HWM value after waiting.
	CurrentHWM int64
	// TimedOut is true if the wait ended due to timeout.
	TimedOut bool
}

// HWMWatcher watches for HWM changes on streams to support long-poll fetch.
type HWMWatcher struct {
	store         metadata.MetadataStore
	streamManager *index.StreamManager
}

// NewHWMWatcher creates a new HWM watcher.
func NewHWMWatcher(store metadata.MetadataStore, streamManager *index.StreamManager) *HWMWatcher {
	return &HWMWatcher{
		store:         store,
		streamManager: streamManager,
	}
}

// WaitForHWM waits for the HWM of a stream to increase beyond the given threshold
// or until the timeout expires. This implements the long-poll waiting behavior
// per the Kafka fetch protocol:
//   - If fetchOffset >= hwm and maxWaitMs > 0, wait
//   - Subscribe to hwm notification for stream
//   - Wake on hwm increase or timeout
//   - Return empty if timeout and no new data
//
// Returns a WaitResult indicating whether new data is available.
func (w *HWMWatcher) WaitForHWM(ctx context.Context, streamID string, threshold int64, maxWait time.Duration) (*WaitResult, error) {
	// First check current HWM
	currentHWM, _, err := w.streamManager.GetHWM(ctx, streamID)
	if err != nil {
		return nil, err
	}

	// If HWM is already above threshold, return immediately
	if currentHWM > threshold {
		return &WaitResult{
			NewDataAvailable: true,
			CurrentHWM:       currentHWM,
			TimedOut:         false,
		}, nil
	}

	// Create context with timeout for the wait
	waitCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	// Subscribe to notifications
	stream, err := w.store.Notifications(waitCtx)
	if err != nil {
		// If we can't subscribe, treat as timeout
		return &WaitResult{
			NewDataAvailable: false,
			CurrentHWM:       currentHWM,
			TimedOut:         true,
		}, nil
	}
	defer stream.Close()

	// Re-check HWM after subscribing to avoid missing updates between initial check and subscription.
	currentHWM, _, err = w.streamManager.GetHWM(waitCtx, streamID)
	if err != nil {
		return nil, err
	}
	if currentHWM > threshold {
		return &WaitResult{
			NewDataAvailable: true,
			CurrentHWM:       currentHWM,
			TimedOut:         false,
		}, nil
	}

	// Build the HWM key to watch for
	hwmKey := keys.HwmKeyPath(streamID)

	// Wait for notifications
	for {
		notification, err := stream.Next(waitCtx)
		if err != nil {
			// Context cancelled or deadline exceeded = timeout
			if waitCtx.Err() != nil {
				return &WaitResult{
					NewDataAvailable: false,
					CurrentHWM:       currentHWM,
					TimedOut:         true,
				}, nil
			}
			// Stream error - check current HWM and return
			newHWM, _, _ := w.streamManager.GetHWM(ctx, streamID)
			return &WaitResult{
				NewDataAvailable: newHWM > threshold,
				CurrentHWM:       newHWM,
				TimedOut:         true,
			}, nil
		}

		// Check if this is the HWM key we're watching
		if notification.Key != hwmKey {
			continue
		}

		// HWM was updated - decode and check
		if notification.Deleted {
			// Stream was deleted - return with no data
			return &WaitResult{
				NewDataAvailable: false,
				CurrentHWM:       0,
				TimedOut:         false,
			}, nil
		}

		// Decode new HWM
		newHWM, err := index.DecodeHWM(notification.Value)
		if err != nil {
			continue
		}

		currentHWM = newHWM

		// Check if HWM is now above threshold
		if newHWM > threshold {
			return &WaitResult{
				NewDataAvailable: true,
				CurrentHWM:       newHWM,
				TimedOut:         false,
			}, nil
		}
	}
}

// extractStreamIDFromHWMKey extracts the stream ID from an HWM key.
// Returns empty string if the key is not an HWM key.
func extractStreamIDFromHWMKey(key string) string {
	const hwmSuffix = "/hwm"
	if !strings.HasSuffix(key, hwmSuffix) {
		return ""
	}
	if !strings.HasPrefix(key, keys.StreamsPrefix+"/") {
		return ""
	}

	remaining := strings.TrimPrefix(key, keys.StreamsPrefix+"/")
	remaining = strings.TrimSuffix(remaining, hwmSuffix)

	if strings.Contains(remaining, "/") {
		return ""
	}

	return remaining
}
