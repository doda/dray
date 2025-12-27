package oxia

import (
	"context"

	oxiaclient "github.com/oxia-db/oxia/oxia"

	"github.com/dray-io/dray/internal/metadata"
)

// notificationStream implements metadata.NotificationStream for Oxia.
type notificationStream struct {
	notifications oxiaclient.Notifications
	ctx           context.Context
}

// Next blocks until the next notification is available or the context is cancelled.
func (s *notificationStream) Next(ctx context.Context) (metadata.Notification, error) {
	select {
	case <-ctx.Done():
		return metadata.Notification{}, ctx.Err()
	case <-s.ctx.Done():
		return metadata.Notification{}, s.ctx.Err()
	case n, ok := <-s.notifications.Ch():
		if !ok {
			return metadata.Notification{}, metadata.ErrStoreClosed
		}
		return convertNotification(n), nil
	}
}

// Close releases resources associated with the stream.
func (s *notificationStream) Close() error {
	return s.notifications.Close()
}

// convertNotification converts an Oxia notification to a metadata notification.
func convertNotification(n *oxiaclient.Notification) metadata.Notification {
	result := metadata.Notification{
		Key:     n.Key,
		Version: metadata.Version(n.VersionId),
	}

	switch n.Type {
	case oxiaclient.KeyCreated, oxiaclient.KeyModified:
		result.Deleted = false
	case oxiaclient.KeyDeleted, oxiaclient.KeyRangeRangeDeleted:
		result.Deleted = true
	}

	return result
}
