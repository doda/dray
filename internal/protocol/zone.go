package protocol

import "context"

// zoneContextKey is the context key for storing the client's zone_id.
type zoneContextKey struct{}

// ZoneIDFromContext retrieves the zone_id from context.
// Returns empty string if not set.
func ZoneIDFromContext(ctx context.Context) string {
	if v := ctx.Value(zoneContextKey{}); v != nil {
		return v.(string)
	}
	return ""
}

// WithZoneID returns a new context with the given zone_id.
func WithZoneID(ctx context.Context, zoneID string) context.Context {
	return context.WithValue(ctx, zoneContextKey{}, zoneID)
}
