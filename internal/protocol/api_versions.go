package protocol

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// APIVersionEntry defines a supported API with its version range.
type APIVersionEntry struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// supportedAPIs is the authoritative list of APIs supported by Dray.
// This matrix is based on Kafka v4 protocol requirements, with explicit exclusions
// for transaction/idempotence APIs (spec 14.3) and inter-broker APIs (spec 14.4).
var supportedAPIs = []APIVersionEntry{
	// Core produce/consume APIs (MUST implement per spec 14.1)
	{APIKey: 0, MinVersion: 0, MaxVersion: 11},  // Produce (v0-v11 in Kafka 4.x)
	{APIKey: 1, MinVersion: 0, MaxVersion: 16},  // Fetch
	{APIKey: 2, MinVersion: 0, MaxVersion: 8},   // ListOffsets
	{APIKey: 3, MinVersion: 0, MaxVersion: 12},  // Metadata

	// Offset management (MUST implement per spec 14.1)
	{APIKey: 8, MinVersion: 0, MaxVersion: 9},  // OffsetCommit
	{APIKey: 9, MinVersion: 0, MaxVersion: 9},  // OffsetFetch
	{APIKey: 10, MinVersion: 0, MaxVersion: 6}, // FindCoordinator

	// Classic group coordination (MUST implement per spec 14.1)
	{APIKey: 11, MinVersion: 0, MaxVersion: 9}, // JoinGroup
	{APIKey: 12, MinVersion: 0, MaxVersion: 5}, // Heartbeat
	{APIKey: 13, MinVersion: 0, MaxVersion: 5}, // LeaveGroup
	{APIKey: 14, MinVersion: 0, MaxVersion: 7}, // SyncGroup
	{APIKey: 15, MinVersion: 0, MaxVersion: 5}, // DescribeGroups
	{APIKey: 16, MinVersion: 0, MaxVersion: 4}, // ListGroups

	// ApiVersions itself
	{APIKey: 18, MinVersion: 0, MaxVersion: 4}, // ApiVersions (v4 required for Kafka 4.x)

	// Admin APIs (SHOULD implement per spec 14.2)
	{APIKey: 19, MinVersion: 0, MaxVersion: 7}, // CreateTopics
	{APIKey: 20, MinVersion: 0, MaxVersion: 6}, // DeleteTopics
	{APIKey: 32, MinVersion: 0, MaxVersion: 4}, // DescribeConfigs
	{APIKey: 42, MinVersion: 0, MaxVersion: 2}, // DeleteGroups
	{APIKey: 44, MinVersion: 0, MaxVersion: 1}, // IncrementalAlterConfigs
	{APIKey: 60, MinVersion: 0, MaxVersion: 1}, // DescribeCluster

	// Consumer group APIs for KIP-848 (MUST implement per spec 14.1)
	{APIKey: 68, MinVersion: 0, MaxVersion: 0}, // ConsumerGroupHeartbeat
	{APIKey: 69, MinVersion: 0, MaxVersion: 0}, // ConsumerGroupDescribe
}

// excludedAPIs documents APIs that Dray explicitly does NOT support.
// These are not advertised in ApiVersions response.
//
// Transaction APIs (spec 14.3):
//   - 22: InitProducerId (idempotence-related)
//   - 24: AddPartitionsToTxn
//   - 25: AddOffsetsToTxn
//   - 26: EndTxn
//   - 27: WriteTxnMarkers
//   - 28: TxnOffsetCommit
//   - 65: DescribeTransactions
//   - 66: ListTransactions
//
// Inter-broker/Controller APIs (spec 14.4):
//   - 4: LeaderAndIsr
//   - 5: StopReplica
//   - 6: UpdateMetadata
//   - 7: ControlledShutdown
//   - 17: SaslHandshake (handled separately if SASL enabled)
//   - 21: OffsetForLeaderEpoch (internal)
//   - 29: DescribeAcls
//   - 30: CreateAcls
//   - 31: DeleteAcls
//   - 33: AlterConfigs (deprecated, use IncrementalAlterConfigs)
//   - 35: DescribeLogDirs (broker internal)
//   - 36: SaslAuthenticate (handled separately if SASL enabled)
//   - 37: CreatePartitions (deferred)
//   - 38: CreateDelegationToken
//   - 39: RenewDelegationToken
//   - 40: ExpireDelegationToken
//   - 41: DescribeDelegationToken
//   - 43: ElectLeaders (not applicable)
//   - 45-67: Various internal/advanced APIs
//   - 76+: ShareGroup APIs (explicitly not supported per spec 12.1)
//
// This list is for documentation; we simply don't include them in supportedAPIs.

// saslAPIs are the SASL authentication APIs, included only when SASL is enabled.
var saslAPIs = []APIVersionEntry{
	{APIKey: 17, MinVersion: 0, MaxVersion: 1}, // SaslHandshake
	{APIKey: 36, MinVersion: 0, MaxVersion: 2}, // SaslAuthenticate
}

// GetSupportedAPIs returns the list of APIs supported by this broker.
// Does not include SASL APIs - use GetSupportedAPIsWithSASL for that.
func GetSupportedAPIs() []APIVersionEntry {
	result := make([]APIVersionEntry, len(supportedAPIs))
	copy(result, supportedAPIs)
	return result
}

// GetSupportedAPIsWithSASL returns the list of APIs including SASL APIs.
func GetSupportedAPIsWithSASL() []APIVersionEntry {
	result := make([]APIVersionEntry, 0, len(supportedAPIs)+len(saslAPIs))
	result = append(result, supportedAPIs...)
	result = append(result, saslAPIs...)
	return result
}

// IsAPISupported checks if a given API key is supported.
func IsAPISupported(apiKey int16) bool {
	for _, api := range supportedAPIs {
		if api.APIKey == apiKey {
			return true
		}
	}
	return false
}

// GetAPIVersionRange returns the supported version range for an API key.
// Returns (min, max, true) if supported, (0, 0, false) if not.
func GetAPIVersionRange(apiKey int16) (minVersion, maxVersion int16, supported bool) {
	for _, api := range supportedAPIs {
		if api.APIKey == apiKey {
			return api.MinVersion, api.MaxVersion, true
		}
	}
	return 0, 0, false
}

// HandleApiVersions handles the ApiVersions (key 18) request.
// This handler can be called before full authentication and is used by clients
// to discover the broker's supported API versions.
func HandleApiVersions(version int16, req *kmsg.ApiVersionsRequest) *kmsg.ApiVersionsResponse {
	return HandleApiVersionsWithOptions(version, req, false)
}

// HandleApiVersionsWithOptions handles the ApiVersions request with options.
// When saslEnabled is true, SASL APIs are included in the response.
func HandleApiVersionsWithOptions(version int16, req *kmsg.ApiVersionsRequest, saslEnabled bool) *kmsg.ApiVersionsResponse {
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.SetVersion(version)

	// Set error code to 0 (no error)
	resp.ErrorCode = 0

	// Get the appropriate API list
	var apis []APIVersionEntry
	if saslEnabled {
		apis = GetSupportedAPIsWithSASL()
	} else {
		apis = GetSupportedAPIs()
	}

	// Populate the API keys
	for _, api := range apis {
		apiKey := kmsg.NewApiVersionsResponseApiKey()
		apiKey.ApiKey = api.APIKey
		apiKey.MinVersion = api.MinVersion
		apiKey.MaxVersion = api.MaxVersion
		resp.ApiKeys = append(resp.ApiKeys, apiKey)
	}

	// For version 3+, we can include throttle time and supported features
	if version >= 1 {
		resp.ThrottleMillis = 0
	}

	// For version 3+, include supported features (empty for now)
	// Note: Kafka 4 uses features for zkMigrationReady, kraftControllerFeatures, etc.
	// Dray doesn't need these as it's not ZK-based.
	if version >= 3 {
		resp.SupportedFeatures = nil
		resp.FinalizedFeaturesEpoch = -1
		resp.FinalizedFeatures = nil
		resp.ZkMigrationReady = false
	}

	return resp
}

// ApiVersionsHandler is a stateless handler for ApiVersions requests.
type ApiVersionsHandler struct{}

// NewApiVersionsHandler creates a new ApiVersions handler.
func NewApiVersionsHandler() *ApiVersionsHandler {
	return &ApiVersionsHandler{}
}

// Handle processes an ApiVersions request.
func (h *ApiVersionsHandler) Handle(version int16, req *kmsg.ApiVersionsRequest) *kmsg.ApiVersionsResponse {
	return HandleApiVersions(version, req)
}
