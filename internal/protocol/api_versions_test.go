package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestGetSupportedAPIs(t *testing.T) {
	apis := GetSupportedAPIs()

	if len(apis) == 0 {
		t.Fatal("expected at least one supported API")
	}

	// Verify we have core APIs
	coreAPIs := map[int16]string{
		0:  "Produce",
		1:  "Fetch",
		2:  "ListOffsets",
		3:  "Metadata",
		8:  "OffsetCommit",
		9:  "OffsetFetch",
		10: "FindCoordinator",
		11: "JoinGroup",
		12: "Heartbeat",
		13: "LeaveGroup",
		14: "SyncGroup",
		15: "DescribeGroups",
		16: "ListGroups",
		18: "ApiVersions",
	}

	found := make(map[int16]bool)
	for _, api := range apis {
		found[api.APIKey] = true
	}

	for key, name := range coreAPIs {
		if !found[key] {
			t.Errorf("missing core API %d (%s)", key, name)
		}
	}
}

func TestExcludedTransactionAPIs(t *testing.T) {
	// Per spec 14.3, transaction APIs must NOT be advertised
	transactionAPIs := []int16{
		22, // InitProducerId
		24, // AddPartitionsToTxn
		25, // AddOffsetsToTxn
		26, // EndTxn
		27, // WriteTxnMarkers
		28, // TxnOffsetCommit
		65, // DescribeTransactions
		66, // ListTransactions
	}

	apis := GetSupportedAPIs()
	supported := make(map[int16]bool)
	for _, api := range apis {
		supported[api.APIKey] = true
	}

	for _, key := range transactionAPIs {
		if supported[key] {
			t.Errorf("transaction API %d should not be advertised", key)
		}
	}
}

func TestExcludedInterBrokerAPIs(t *testing.T) {
	// Per spec 14.4, inter-broker/controller APIs must NOT be advertised
	interBrokerAPIs := []int16{
		4,  // LeaderAndIsr
		5,  // StopReplica
		6,  // UpdateMetadata
		7,  // ControlledShutdown
		21, // OffsetForLeaderEpoch
		43, // ElectLeaders
	}

	apis := GetSupportedAPIs()
	supported := make(map[int16]bool)
	for _, api := range apis {
		supported[api.APIKey] = true
	}

	for _, key := range interBrokerAPIs {
		if supported[key] {
			t.Errorf("inter-broker API %d should not be advertised", key)
		}
	}
}

func TestIsAPISupported(t *testing.T) {
	tests := []struct {
		apiKey   int16
		expected bool
		name     string
	}{
		{0, true, "Produce"},
		{1, true, "Fetch"},
		{18, true, "ApiVersions"},
		{22, false, "InitProducerId (excluded)"},
		{4, false, "LeaderAndIsr (excluded)"},
		{999, false, "Unknown API"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAPISupported(tt.apiKey)
			if result != tt.expected {
				t.Errorf("IsAPISupported(%d) = %v, expected %v", tt.apiKey, result, tt.expected)
			}
		})
	}
}

func TestGetAPIVersionRange(t *testing.T) {
	tests := []struct {
		apiKey      int16
		wantMin     int16
		wantMax     int16
		wantSupport bool
		name        string
	}{
		{0, 0, 11, true, "Produce"},
		{1, 0, 16, true, "Fetch"},
		{18, 0, 4, true, "ApiVersions"},
		{22, 0, 0, false, "InitProducerId (excluded)"},
		{999, 0, 0, false, "Unknown API"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max, supported := GetAPIVersionRange(tt.apiKey)
			if supported != tt.wantSupport {
				t.Errorf("GetAPIVersionRange(%d) supported = %v, expected %v", tt.apiKey, supported, tt.wantSupport)
			}
			if supported {
				if min != tt.wantMin {
					t.Errorf("GetAPIVersionRange(%d) min = %v, expected %v", tt.apiKey, min, tt.wantMin)
				}
				if max != tt.wantMax {
					t.Errorf("GetAPIVersionRange(%d) max = %v, expected %v", tt.apiKey, max, tt.wantMax)
				}
			}
		})
	}
}

func TestFlexibleHeaderMappingMatchesSupportedMatrix(t *testing.T) {
	apis := GetSupportedAPIsWithSASL()
	for _, api := range apis {
		req, err := NewRequest(api.APIKey)
		if err != nil {
			t.Fatalf("failed to build request for api %d: %v", api.APIKey, err)
		}
		for version := api.MinVersion; version <= api.MaxVersion; version++ {
			req.SetVersion(version)
			expected := req.IsFlexible()
			if api.APIKey == 18 {
				expected = false
			}
			if got := IsFlexibleRequestHeader(api.APIKey, version); got != expected {
				t.Errorf("api %d version %d flexible header = %v, expected %v", api.APIKey, version, got, expected)
			}
		}
	}
}

func TestHandleApiVersions(t *testing.T) {
	tests := []struct {
		version int16
		name    string
	}{
		{0, "version 0"},
		{1, "version 1"},
		{2, "version 2"},
		{3, "version 3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := kmsg.NewPtrApiVersionsRequest()
			req.SetVersion(tt.version)

			resp := HandleApiVersions(tt.version, req)

			if resp.ErrorCode != 0 {
				t.Errorf("expected ErrorCode 0, got %d", resp.ErrorCode)
			}

			if len(resp.ApiKeys) == 0 {
				t.Error("expected at least one API key in response")
			}

			// Verify response version matches request
			if resp.GetVersion() != tt.version {
				t.Errorf("expected response version %d, got %d", tt.version, resp.GetVersion())
			}

			// Verify all expected APIs are present
			foundAPIs := make(map[int16]bool)
			for _, api := range resp.ApiKeys {
				foundAPIs[api.ApiKey] = true

				// Verify min <= max for each API
				if api.MinVersion > api.MaxVersion {
					t.Errorf("API %d has MinVersion %d > MaxVersion %d",
						api.ApiKey, api.MinVersion, api.MaxVersion)
				}
			}

			// Core APIs must be present
			for _, key := range []int16{0, 1, 2, 3, 18} {
				if !foundAPIs[key] {
					t.Errorf("missing core API key %d", key)
				}
			}
		})
	}
}

func TestHandleApiVersionsKIP848APIs(t *testing.T) {
	// Verify KIP-848 consumer group APIs are advertised
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)

	resp := HandleApiVersions(3, req)

	kip848APIs := map[int16]string{
		68: "ConsumerGroupHeartbeat",
		69: "ConsumerGroupDescribe",
	}

	foundAPIs := make(map[int16]bool)
	for _, api := range resp.ApiKeys {
		foundAPIs[api.ApiKey] = true
	}

	for key, name := range kip848APIs {
		if !foundAPIs[key] {
			t.Errorf("KIP-848 API %d (%s) not advertised", key, name)
		}
	}
}

func TestApiVersionsRoundTrip(t *testing.T) {
	// Test that ApiVersions request/response can be encoded and decoded
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)

	// Encode request
	reqBytes := req.AppendTo(nil)
	if len(reqBytes) == 0 {
		t.Error("encoded request is empty")
	}

	// Get response
	resp := HandleApiVersions(3, req)

	// Encode response
	respBytes := resp.AppendTo(nil)
	if len(respBytes) == 0 {
		t.Error("encoded response is empty")
	}

	// Decode response
	decoded := kmsg.NewPtrApiVersionsResponse()
	decoded.SetVersion(3)
	if err := decoded.ReadFrom(respBytes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify decoded response matches
	if decoded.ErrorCode != resp.ErrorCode {
		t.Errorf("decoded ErrorCode = %d, expected %d", decoded.ErrorCode, resp.ErrorCode)
	}
	if len(decoded.ApiKeys) != len(resp.ApiKeys) {
		t.Errorf("decoded ApiKeys length = %d, expected %d", len(decoded.ApiKeys), len(resp.ApiKeys))
	}
}

func TestApiVersionsHandlerStruct(t *testing.T) {
	handler := NewApiVersionsHandler()

	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)

	resp := handler.Handle(3, req)

	if resp.ErrorCode != 0 {
		t.Errorf("expected ErrorCode 0, got %d", resp.ErrorCode)
	}

	if len(resp.ApiKeys) == 0 {
		t.Error("expected at least one API key in response")
	}
}

func TestApiVersionsMatchesKafka4Expectations(t *testing.T) {
	// This test verifies that the API version matrix matches what
	// a Kafka v4 client would expect

	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)

	resp := HandleApiVersions(3, req)

	// Build a map for easy lookup
	apiVersions := make(map[int16]struct {
		min int16
		max int16
	})
	for _, api := range resp.ApiKeys {
		apiVersions[api.ApiKey] = struct {
			min int16
			max int16
		}{api.MinVersion, api.MaxVersion}
	}

	// Kafka v4 clients expect these minimum versions to be supported
	kafkaV4Requirements := []struct {
		key        int16
		minVersion int16
		name       string
	}{
		{0, 3, "Produce (needs v3+ for transactional field in request)"},
		{1, 4, "Fetch (needs v4+ for isolation level)"},
		{3, 4, "Metadata (needs v4+ for rack support)"},
		{11, 2, "JoinGroup (needs v2+ for rebalance timeout)"},
	}

	for _, req := range kafkaV4Requirements {
		versions, ok := apiVersions[req.key]
		if !ok {
			t.Errorf("Kafka v4 required API %d (%s) not advertised", req.key, req.name)
			continue
		}
		if versions.max < req.minVersion {
			t.Errorf("Kafka v4 requires %s max version >= %d, but got %d",
				req.name, req.minVersion, versions.max)
		}
	}

	// Verify throttle time is included (Kafka 4 expects this)
	if resp.ThrottleMillis != 0 {
		t.Logf("ThrottleMillis = %d (expected 0)", resp.ThrottleMillis)
	}
}

func TestNoShareGroupAPIs(t *testing.T) {
	// Per spec 12.1, ShareGroup APIs (76+) are explicitly not supported
	apis := GetSupportedAPIs()
	for _, api := range apis {
		if api.APIKey >= 76 {
			t.Errorf("ShareGroup API %d should not be advertised", api.APIKey)
		}
	}
}

func TestAPIVersionRangesAreValid(t *testing.T) {
	apis := GetSupportedAPIs()

	for _, api := range apis {
		// Min should be non-negative
		if api.MinVersion < 0 {
			t.Errorf("API %d has negative MinVersion %d", api.APIKey, api.MinVersion)
		}

		// Max should be >= Min
		if api.MaxVersion < api.MinVersion {
			t.Errorf("API %d has MaxVersion %d < MinVersion %d",
				api.APIKey, api.MaxVersion, api.MinVersion)
		}

		// API key should be non-negative
		if api.APIKey < 0 {
			t.Errorf("API key %d is negative", api.APIKey)
		}
	}
}

func TestSupportedAPIsNoDuplicates(t *testing.T) {
	apis := GetSupportedAPIs()
	seen := make(map[int16]bool)

	for _, api := range apis {
		if seen[api.APIKey] {
			t.Errorf("duplicate API key %d in supportedAPIs", api.APIKey)
		}
		seen[api.APIKey] = true
	}
}
