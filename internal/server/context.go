package server

import "context"

// contextKey is a custom type for context keys to avoid collisions.
type contextKey int

const (
	zoneIDKey contextKey = iota
	clientIDKey
)

// WithZoneID returns a context with the zone_id value.
func WithZoneID(ctx context.Context, zoneID string) context.Context {
	return context.WithValue(ctx, zoneIDKey, zoneID)
}

// ZoneIDFromContext returns the zone_id from the context.
// Returns empty string if not set.
func ZoneIDFromContext(ctx context.Context) string {
	if v := ctx.Value(zoneIDKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// WithClientID returns a context with the client ID value.
func WithClientID(ctx context.Context, clientID string) context.Context {
	return context.WithValue(ctx, clientIDKey, clientID)
}

// ClientIDFromContext returns the client ID from the context.
// Returns empty string if not set.
func ClientIDFromContext(ctx context.Context) string {
	if v := ctx.Value(clientIDKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
