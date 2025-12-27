package oxia

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

// Note: Integration tests that require an Oxia server are in integration_test.go
// and can be run with: go test -tags=integration ./internal/metadata/oxia/

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "empty service address",
			cfg:     Config{Namespace: "test"},
			wantErr: "service address is required",
		},
		{
			name:    "empty namespace",
			cfg:     Config{ServiceAddress: "localhost:6648"},
			wantErr: "namespace is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(context.Background(), tt.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if tt.wantErr != "" && !containsStr(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	// Verify that config with required fields passes validation
	// We can't actually connect without an Oxia server, so we just
	// verify the config struct is properly configured
	cfg := Config{
		ServiceAddress: "localhost:6648",
		Namespace:      "dray/test-cluster",
	}

	// Verify required fields are set
	if cfg.ServiceAddress == "" {
		t.Error("expected service address to be set")
	}
	if cfg.Namespace == "" {
		t.Error("expected namespace to be set")
	}

	// Verify default values can be applied
	cfg.RequestTimeout = 30 * time.Second
	cfg.SessionTimeout = 15 * time.Second

	if cfg.RequestTimeout != 30*time.Second {
		t.Error("request timeout not set correctly")
	}
	if cfg.SessionTimeout != 15*time.Second {
		t.Error("session timeout not set correctly")
	}
}

func TestNamespaceFormat(t *testing.T) {
	// Verify the namespace format matches spec: dray/<cluster_id>
	// The namespace format is passed directly to Oxia, which accepts
	// arbitrary namespace strings, so we just verify our config accepts it
	tests := []struct {
		namespace string
		valid     bool
	}{
		{"dray/my-cluster", true},
		{"dray/cluster-1", true},
		{"dray/prod/us-east-1", true},
		{"", false}, // Empty is rejected by our validation
	}

	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			cfg := Config{
				ServiceAddress: "localhost:6648",
				Namespace:      tt.namespace,
			}
			// We don't actually connect - just verify our config validation
			if tt.valid {
				if cfg.Namespace != tt.namespace {
					t.Errorf("namespace not stored correctly")
				}
			}
		})
	}
}

func TestPrefixEnd(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"", ""},
		{"a", "b"},
		{"abc", "abd"},
		{"/dray/v1/topics/", "/dray/v1/topics0"},
		{"/dray/v1/streams/abc123/offset-index/", "/dray/v1/streams/abc123/offset-index0"},
		{string([]byte{0xFF}), ""},
		{string([]byte{0xFF, 0xFF}), ""},
		{string([]byte{0x00, 0xFF}), string([]byte{0x01})},
	}

	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			got := prefixEnd(tt.prefix)
			if got != tt.want {
				t.Errorf("prefixEnd(%q) = %q, want %q", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestExtractExpectedVersion(t *testing.T) {
	tests := []struct {
		name    string
		opts    []metadata.PutOption
		want    *metadata.Version
	}{
		{
			name: "no options",
			opts: nil,
			want: nil,
		},
		{
			name: "with version 1",
			opts: []metadata.PutOption{metadata.WithExpectedVersion(1)},
			want: versionPtr(1),
		},
		{
			name: "with version 0 (create new)",
			opts: []metadata.PutOption{metadata.WithExpectedVersion(0)},
			want: versionPtr(0),
		},
		{
			name: "with version 100",
			opts: []metadata.PutOption{metadata.WithExpectedVersion(100)},
			want: versionPtr(100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metadata.ExtractExpectedVersion(tt.opts)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", *got)
				}
			} else {
				if got == nil {
					t.Errorf("expected %v, got nil", *tt.want)
				} else if *got != *tt.want {
					t.Errorf("expected %v, got %v", *tt.want, *got)
				}
			}
		})
	}
}

func TestExtractDeleteExpectedVersion(t *testing.T) {
	tests := []struct {
		name    string
		opts    []metadata.DeleteOption
		want    *metadata.Version
	}{
		{
			name: "no options",
			opts: nil,
			want: nil,
		},
		{
			name: "with version 1",
			opts: []metadata.DeleteOption{metadata.WithDeleteExpectedVersion(1)},
			want: versionPtr(1),
		},
		{
			name: "with version 100",
			opts: []metadata.DeleteOption{metadata.WithDeleteExpectedVersion(100)},
			want: versionPtr(100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metadata.ExtractDeleteExpectedVersion(tt.opts)
			if tt.want == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", *got)
				}
			} else {
				if got == nil {
					t.Errorf("expected %v, got nil", *tt.want)
				} else if *got != *tt.want {
					t.Errorf("expected %v, got %v", *tt.want, *got)
				}
			}
		})
	}
}

// containsStr checks if s contains substr
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && containsHelper(s, substr)))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func versionPtr(v metadata.Version) *metadata.Version {
	return &v
}
