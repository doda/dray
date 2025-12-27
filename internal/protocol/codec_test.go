package protocol

import (
	"bytes"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestNewDecoder(t *testing.T) {
	d := NewDecoder()
	if d == nil {
		t.Fatal("NewDecoder returned nil")
	}
}

func TestNewEncoder(t *testing.T) {
	e := NewEncoder()
	if e == nil {
		t.Fatal("NewEncoder returned nil")
	}
}

func TestNewRequest(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  int16
		wantErr bool
	}{
		{"Produce", 0, false},
		{"Fetch", 1, false},
		{"Metadata", 3, false},
		{"ApiVersions", 18, false},
		{"Invalid negative", -1, true},
		{"Invalid too high", 999, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := NewRequest(tt.apiKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRequest(%d) error = %v, wantErr %v", tt.apiKey, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if req == nil {
					t.Error("NewRequest returned nil request without error")
				} else if req.Key() != tt.apiKey {
					t.Errorf("NewRequest(%d).Key() = %d", tt.apiKey, req.Key())
				}
			}
		})
	}
}

func TestNewResponse(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  int16
		wantErr bool
	}{
		{"Produce", 0, false},
		{"Fetch", 1, false},
		{"Metadata", 3, false},
		{"ApiVersions", 18, false},
		{"Invalid negative", -1, true},
		{"Invalid too high", 999, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := NewResponse(tt.apiKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewResponse(%d) error = %v, wantErr %v", tt.apiKey, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if resp == nil {
					t.Error("NewResponse returned nil response without error")
				} else if resp.Key() != tt.apiKey {
					t.Errorf("NewResponse(%d).Key() = %d", tt.apiKey, resp.Key())
				}
			}
		})
	}
}

func TestApiVersionsRequestRoundTrip(t *testing.T) {
	// Create an ApiVersionsRequest using kmsg
	origReq := kmsg.NewPtrApiVersionsRequest()
	origReq.SetVersion(0)
	origReq.ClientSoftwareName = "test-client"
	origReq.ClientSoftwareVersion = "1.0.0"

	// Encode the request
	encoded := origReq.AppendTo(nil)

	// Decode it using our decoder
	decoder := NewDecoder()
	req, err := decoder.DecodeRequest(18, 0, encoded) // ApiVersions = 18
	if err != nil {
		t.Fatalf("DecodeRequest failed: %v", err)
	}

	// Verify the decoded request
	if req.Key() != 18 {
		t.Errorf("decoded request key = %d, want 18", req.Key())
	}

	// Re-encode and verify it matches
	reencoded := req.AppendTo(nil)
	if !bytes.Equal(encoded, reencoded) {
		t.Errorf("re-encoded bytes differ from original")
	}
}

func TestMetadataRequestRoundTrip(t *testing.T) {
	// Create a MetadataRequest with some topics
	origReq := kmsg.NewPtrMetadataRequest()
	origReq.SetVersion(1)
	origReq.Topics = []kmsg.MetadataRequestTopic{
		{Topic: kmsg.StringPtr("test-topic-1")},
		{Topic: kmsg.StringPtr("test-topic-2")},
	}

	// Encode the request
	encoded := origReq.AppendTo(nil)

	// Decode it using our decoder
	decoder := NewDecoder()
	req, err := decoder.DecodeRequest(3, 1, encoded) // Metadata = 3
	if err != nil {
		t.Fatalf("DecodeRequest failed: %v", err)
	}

	// Verify the decoded request
	if req.Key() != 3 {
		t.Errorf("decoded request key = %d, want 3", req.Key())
	}

	// Re-encode and verify it matches
	reencoded := req.AppendTo(nil)
	if !bytes.Equal(encoded, reencoded) {
		t.Errorf("re-encoded bytes differ from original")
	}

	// Verify we can access the inner type
	metaReq, ok := req.Inner().(*kmsg.MetadataRequest)
	if !ok {
		t.Fatal("Inner() did not return *kmsg.MetadataRequest")
	}
	if len(metaReq.Topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(metaReq.Topics))
	}
}

func TestProduceRequestRoundTrip(t *testing.T) {
	// Create a ProduceRequest
	origReq := kmsg.NewPtrProduceRequest()
	origReq.SetVersion(0)
	origReq.Acks = -1
	origReq.TimeoutMillis = 30000
	origReq.Topics = []kmsg.ProduceRequestTopic{
		{
			Topic: "test-topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{
				{
					Partition: 0,
					Records:   []byte{0, 0, 0, 0}, // minimal record batch
				},
			},
		},
	}

	// Encode the request
	encoded := origReq.AppendTo(nil)

	// Decode it using our decoder
	decoder := NewDecoder()
	req, err := decoder.DecodeRequest(0, 0, encoded) // Produce = 0
	if err != nil {
		t.Fatalf("DecodeRequest failed: %v", err)
	}

	// Verify the decoded request
	if req.Key() != 0 {
		t.Errorf("decoded request key = %d, want 0", req.Key())
	}

	// Re-encode and verify it matches
	reencoded := req.AppendTo(nil)
	if !bytes.Equal(encoded, reencoded) {
		t.Errorf("re-encoded bytes differ from original")
	}
}

func TestApiVersionsResponseRoundTrip(t *testing.T) {
	// Create an ApiVersionsResponse
	origResp := kmsg.NewPtrApiVersionsResponse()
	origResp.SetVersion(0)
	origResp.ErrorCode = 0
	origResp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: 0, MinVersion: 0, MaxVersion: 9},
		{ApiKey: 1, MinVersion: 0, MaxVersion: 12},
		{ApiKey: 3, MinVersion: 0, MaxVersion: 9},
		{ApiKey: 18, MinVersion: 0, MaxVersion: 3},
	}

	// Encode the response
	encoded := origResp.AppendTo(nil)

	// Create a wrapped response and decode
	resp, err := NewResponse(18)
	if err != nil {
		t.Fatalf("NewResponse failed: %v", err)
	}
	resp.SetVersion(0)
	if err := resp.ReadFrom(encoded); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Re-encode and verify it matches
	encoder := NewEncoder()
	reencoded := encoder.EncodeResponse(resp)
	if !bytes.Equal(encoded, reencoded) {
		t.Errorf("re-encoded bytes differ from original")
	}
}

func TestEncodeResponseWithCorrelationID(t *testing.T) {
	// Create a simple ApiVersionsResponse
	resp, err := NewResponse(18)
	if err != nil {
		t.Fatalf("NewResponse failed: %v", err)
	}
	resp.SetVersion(0) // Non-flexible version

	encoder := NewEncoder()
	encoded := encoder.EncodeResponseWithCorrelationID(12345, resp)

	// First 4 bytes should be correlation ID (big-endian)
	if len(encoded) < 4 {
		t.Fatalf("encoded response too short: %d bytes", len(encoded))
	}

	correlationID := int32(encoded[0])<<24 | int32(encoded[1])<<16 | int32(encoded[2])<<8 | int32(encoded[3])
	if correlationID != 12345 {
		t.Errorf("correlation ID = %d, want 12345", correlationID)
	}
}

func TestEncodeResponseWithCorrelationIDFlexible(t *testing.T) {
	// Create a response with a flexible version (v3 for ApiVersions)
	resp, err := NewResponse(18)
	if err != nil {
		t.Fatalf("NewResponse failed: %v", err)
	}
	resp.SetVersion(3) // Flexible version

	encoder := NewEncoder()
	encoded := encoder.EncodeResponseWithCorrelationID(12345, resp)

	// First 4 bytes should be correlation ID (big-endian)
	if len(encoded) < 5 {
		t.Fatalf("encoded response too short: %d bytes", len(encoded))
	}

	correlationID := int32(encoded[0])<<24 | int32(encoded[1])<<16 | int32(encoded[2])<<8 | int32(encoded[3])
	if correlationID != 12345 {
		t.Errorf("correlation ID = %d, want 12345", correlationID)
	}

	// For flexible versions, byte 5 should be the empty tag buffer (0)
	if encoded[4] != 0 {
		t.Errorf("expected empty tag buffer (0), got %d", encoded[4])
	}
}

func TestDecodeRequestVersionTooHigh(t *testing.T) {
	decoder := NewDecoder()

	// Try to decode with a version higher than max
	_, err := decoder.DecodeRequest(18, 100, nil)
	if err == nil {
		t.Error("expected error for version higher than max")
	}
}

func TestDecodeRequestInvalidPayload(t *testing.T) {
	decoder := NewDecoder()

	// Try to decode with invalid payload
	_, err := decoder.DecodeRequest(3, 1, []byte{0xFF, 0xFF}) // Invalid metadata request
	if err == nil {
		t.Error("expected error for invalid payload")
	}
}

func TestRequestResponseKind(t *testing.T) {
	req, err := NewRequest(18) // ApiVersions
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}

	resp := req.ResponseKind()
	if resp == nil {
		t.Fatal("ResponseKind returned nil")
	}

	if resp.Key() != 18 {
		t.Errorf("response key = %d, want 18", resp.Key())
	}
}

func TestWrapRequest(t *testing.T) {
	origReq := kmsg.NewPtrMetadataRequest()
	origReq.SetVersion(1)

	wrapped := WrapRequest(origReq)
	if wrapped.Key() != 3 {
		t.Errorf("wrapped request key = %d, want 3", wrapped.Key())
	}
	if wrapped.GetVersion() != 1 {
		t.Errorf("wrapped request version = %d, want 1", wrapped.GetVersion())
	}
}

func TestWrapResponse(t *testing.T) {
	origResp := kmsg.NewPtrApiVersionsResponse()
	origResp.SetVersion(2)

	wrapped := WrapResponse(origResp)
	if wrapped.Key() != 18 {
		t.Errorf("wrapped response key = %d, want 18", wrapped.Key())
	}
	if wrapped.GetVersion() != 2 {
		t.Errorf("wrapped response version = %d, want 2", wrapped.GetVersion())
	}
}

func TestAPIKeyName(t *testing.T) {
	tests := []struct {
		key  int16
		want string
	}{
		{0, "Produce"},
		{1, "Fetch"},
		{3, "Metadata"},
		{18, "ApiVersions"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := APIKeyName(tt.key)
			if got != tt.want {
				t.Errorf("APIKeyName(%d) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestMaxSupportedVersion(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  int16
		wantErr bool
	}{
		{"Produce", 0, false},
		{"Fetch", 1, false},
		{"Metadata", 3, false},
		{"ApiVersions", 18, false},
		{"Invalid", 999, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, err := MaxSupportedVersion(tt.apiKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("MaxSupportedVersion(%d) error = %v, wantErr %v", tt.apiKey, err, tt.wantErr)
				return
			}
			if !tt.wantErr && version < 0 {
				t.Errorf("MaxSupportedVersion(%d) = %d, want >= 0", tt.apiKey, version)
			}
		})
	}
}

func TestRequestIsFlexible(t *testing.T) {
	req, err := NewRequest(18) // ApiVersions
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}

	// Version 0-2 are non-flexible, 3+ are flexible
	req.SetVersion(0)
	if req.IsFlexible() {
		t.Error("version 0 should not be flexible")
	}

	req.SetVersion(3)
	if !req.IsFlexible() {
		t.Error("version 3 should be flexible")
	}
}

func TestAllSupportedAPIKeys(t *testing.T) {
	// Test that all API keys from 0 to MaxKey can be created
	for apiKey := int16(0); apiKey <= kmsg.MaxKey; apiKey++ {
		req, err := NewRequest(apiKey)
		if err != nil {
			// Some keys may not be supported
			continue
		}
		if req == nil {
			t.Errorf("NewRequest(%d) returned nil without error", apiKey)
			continue
		}
		if req.Key() != apiKey {
			t.Errorf("NewRequest(%d).Key() = %d", apiKey, req.Key())
		}

		resp, err := NewResponse(apiKey)
		if err != nil {
			continue
		}
		if resp == nil {
			t.Errorf("NewResponse(%d) returned nil without error", apiKey)
			continue
		}
		if resp.Key() != apiKey {
			t.Errorf("NewResponse(%d).Key() = %d", apiKey, resp.Key())
		}
	}
}

func TestFetchRequestRoundTrip(t *testing.T) {
	// Create a FetchRequest
	origReq := kmsg.NewPtrFetchRequest()
	origReq.SetVersion(4)
	origReq.ReplicaID = -1
	origReq.MaxWaitMillis = 500
	origReq.MinBytes = 1
	origReq.MaxBytes = 1024 * 1024
	origReq.IsolationLevel = 0
	origReq.Topics = []kmsg.FetchRequestTopic{
		{
			Topic: "test-topic",
			Partitions: []kmsg.FetchRequestTopicPartition{
				{
					Partition:         0,
					FetchOffset:       0,
					PartitionMaxBytes: 1024 * 1024,
				},
			},
		},
	}

	// Encode the request
	encoded := origReq.AppendTo(nil)

	// Decode it using our decoder
	decoder := NewDecoder()
	req, err := decoder.DecodeRequest(1, 4, encoded) // Fetch = 1
	if err != nil {
		t.Fatalf("DecodeRequest failed: %v", err)
	}

	// Verify we can access the inner type
	fetchReq, ok := req.Inner().(*kmsg.FetchRequest)
	if !ok {
		t.Fatal("Inner() did not return *kmsg.FetchRequest")
	}
	if fetchReq.MaxWaitMillis != 500 {
		t.Errorf("MaxWaitMillis = %d, want 500", fetchReq.MaxWaitMillis)
	}
	if len(fetchReq.Topics) != 1 {
		t.Errorf("expected 1 topic, got %d", len(fetchReq.Topics))
	}

	// Re-encode and verify it matches
	reencoded := req.AppendTo(nil)
	if !bytes.Equal(encoded, reencoded) {
		t.Errorf("re-encoded bytes differ from original")
	}
}
