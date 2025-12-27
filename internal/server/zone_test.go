package server

import (
	"testing"
)

func TestParseZoneID(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		want     string
	}{
		{
			name:     "simple zone_id first",
			clientID: "zone_id=us-west-2a",
			want:     "us-west-2a",
		},
		{
			name:     "zone_id first with other pairs",
			clientID: "zone_id=us-east-1b,client=myapp,version=1.0",
			want:     "us-east-1b",
		},
		{
			name:     "zone_id in middle",
			clientID: "client=myapp,zone_id=eu-west-1c,version=1.0",
			want:     "eu-west-1c",
		},
		{
			name:     "zone_id last",
			clientID: "client=myapp,version=1.0,zone_id=ap-northeast-1a",
			want:     "ap-northeast-1a",
		},
		{
			name:     "zone_id with spaces around",
			clientID: " zone_id = us-west-2a , client = myapp ",
			want:     "us-west-2a",
		},
		{
			name:     "empty client id",
			clientID: "",
			want:     "",
		},
		{
			name:     "no zone_id present",
			clientID: "client=myapp,version=1.0",
			want:     "",
		},
		{
			name:     "zone_id with empty value",
			clientID: "zone_id=,client=myapp",
			want:     "",
		},
		{
			name:     "zone_id key without equals",
			clientID: "zone_id,client=myapp",
			want:     "",
		},
		{
			name:     "only zone_id key without value",
			clientID: "zone_id=",
			want:     "",
		},
		{
			name:     "zone_id with hyphenated zone name",
			clientID: "zone_id=us-east-1a-extended",
			want:     "us-east-1a-extended",
		},
		{
			name:     "zone_id with complex zone name",
			clientID: "zone_id=az1.region.cloud.provider.com",
			want:     "az1.region.cloud.provider.com",
		},
		{
			name:     "multiple equals in value",
			clientID: "zone_id=zone=a=b",
			want:     "zone=a=b",
		},
		{
			name:     "plain client id without pairs",
			clientID: "my-kafka-client",
			want:     "",
		},
		{
			name:     "empty pairs between commas",
			clientID: "zone_id=us-west-2a,,client=myapp",
			want:     "us-west-2a",
		},
		{
			name:     "only commas",
			clientID: ",,,",
			want:     "",
		},
		{
			name:     "whitespace only",
			clientID: "   ",
			want:     "",
		},
		{
			name:     "zone_id value with spaces",
			clientID: "zone_id= zone-a ",
			want:     "zone-a",
		},
		{
			name:     "case sensitivity - ZONE_ID is different key",
			clientID: "ZONE_ID=us-west-2a",
			want:     "",
		},
		{
			name:     "numeric zone id",
			clientID: "zone_id=1",
			want:     "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseZoneID(tt.clientID)
			if got != tt.want {
				t.Errorf("ParseZoneID(%q) = %q, want %q", tt.clientID, got, tt.want)
			}
		})
	}
}

func TestParseClientIDPairs(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		want     map[string]string
	}{
		{
			name:     "empty client id",
			clientID: "",
			want:     map[string]string{},
		},
		{
			name:     "single pair",
			clientID: "zone_id=us-west-2a",
			want:     map[string]string{"zone_id": "us-west-2a"},
		},
		{
			name:     "multiple pairs",
			clientID: "zone_id=us-west-2a,client=myapp,version=1.0",
			want: map[string]string{
				"zone_id": "us-west-2a",
				"client":  "myapp",
				"version": "1.0",
			},
		},
		{
			name:     "pairs with spaces",
			clientID: " zone_id = us-west-2a , client = myapp ",
			want: map[string]string{
				"zone_id": "us-west-2a",
				"client":  "myapp",
			},
		},
		{
			name:     "no pairs - plain string",
			clientID: "my-kafka-client",
			want:     map[string]string{},
		},
		{
			name:     "empty key ignored",
			clientID: "=value,zone_id=test",
			want:     map[string]string{"zone_id": "test"},
		},
		{
			name:     "empty value allowed",
			clientID: "zone_id=,client=myapp",
			want: map[string]string{
				"zone_id": "",
				"client":  "myapp",
			},
		},
		{
			name:     "empty pairs between commas",
			clientID: "zone_id=test,,client=app",
			want: map[string]string{
				"zone_id": "test",
				"client":  "app",
			},
		},
		{
			name:     "value contains equals",
			clientID: "key=a=b=c",
			want:     map[string]string{"key": "a=b=c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseClientIDPairs(tt.clientID)
			if len(got) != len(tt.want) {
				t.Errorf("ParseClientIDPairs(%q) returned %d pairs, want %d", tt.clientID, len(got), len(tt.want))
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("ParseClientIDPairs(%q)[%q] = %q, want %q", tt.clientID, k, got[k], v)
				}
			}
		})
	}
}
