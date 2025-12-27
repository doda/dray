// Package protocol implements Kafka protocol encoding/decoding using kmsg.
//
// This package wraps the twmb/franz-go/pkg/kmsg package to provide a unified
// API for decoding Kafka protocol requests and encoding responses. It follows
// the franz-go guidance of using New/Default initializers to ensure proper
// defaults are set.
//
// Usage:
//
//	// Decoding a request
//	decoder := protocol.NewDecoder()
//	req, err := decoder.DecodeRequest(apiKey, version, payload)
//	if err != nil {
//		return err
//	}
//
//	// Access specific request type
//	if metaReq, ok := req.Inner().(*kmsg.MetadataRequest); ok {
//		for _, topic := range metaReq.Topics {
//			// process topic
//		}
//	}
//
//	// Encoding a response
//	encoder := protocol.NewEncoder()
//	resp := req.ResponseKind()
//	resp.SetVersion(version)
//	// ... populate response fields ...
//	bytes := encoder.EncodeResponseWithCorrelationID(correlationID, resp)
//
// The package supports all Kafka API keys up to kmsg.MaxKey and properly
// handles both flexible and non-flexible protocol versions.
package protocol
