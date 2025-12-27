package server

import (
	"context"
	"testing"
)

func TestZoneIDContext(t *testing.T) {
	tests := []struct {
		name   string
		zoneID string
	}{
		{"simple zone", "us-west-2a"},
		{"empty zone", ""},
		{"complex zone", "az1.region.cloud.provider.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = WithZoneID(ctx, tt.zoneID)

			got := ZoneIDFromContext(ctx)
			if got != tt.zoneID {
				t.Errorf("ZoneIDFromContext() = %q, want %q", got, tt.zoneID)
			}
		})
	}
}

func TestZoneIDFromContextEmpty(t *testing.T) {
	ctx := context.Background()
	got := ZoneIDFromContext(ctx)
	if got != "" {
		t.Errorf("ZoneIDFromContext(empty ctx) = %q, want empty", got)
	}
}

func TestClientIDContext(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
	}{
		{"simple client", "my-kafka-client"},
		{"empty client", ""},
		{"client with zone", "zone_id=us-west-2a,client=myapp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = WithClientID(ctx, tt.clientID)

			got := ClientIDFromContext(ctx)
			if got != tt.clientID {
				t.Errorf("ClientIDFromContext() = %q, want %q", got, tt.clientID)
			}
		})
	}
}

func TestClientIDFromContextEmpty(t *testing.T) {
	ctx := context.Background()
	got := ClientIDFromContext(ctx)
	if got != "" {
		t.Errorf("ClientIDFromContext(empty ctx) = %q, want empty", got)
	}
}

func TestBothZoneIDAndClientIDInContext(t *testing.T) {
	ctx := context.Background()
	ctx = WithZoneID(ctx, "us-east-1a")
	ctx = WithClientID(ctx, "zone_id=us-east-1a,app=myapp")

	zoneID := ZoneIDFromContext(ctx)
	clientID := ClientIDFromContext(ctx)

	if zoneID != "us-east-1a" {
		t.Errorf("ZoneIDFromContext() = %q, want %q", zoneID, "us-east-1a")
	}
	if clientID != "zone_id=us-east-1a,app=myapp" {
		t.Errorf("ClientIDFromContext() = %q, want %q", clientID, "zone_id=us-east-1a,app=myapp")
	}
}
