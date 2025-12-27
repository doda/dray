package protocol

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Request wraps a kmsg request type to provide a unified API.
type Request struct {
	inner kmsg.Request
}

// Response wraps a kmsg response type to provide a unified API.
type Response struct {
	inner kmsg.Response
}

// Decoder decodes Kafka protocol requests from bytes.
type Decoder struct{}

// NewDecoder creates a new protocol decoder.
func NewDecoder() *Decoder {
	return &Decoder{}
}

// DecodeRequest decodes a Kafka request from the payload bytes.
// The apiKey and version determine which request type to decode.
func (d *Decoder) DecodeRequest(apiKey, version int16, payload []byte) (*Request, error) {
	req, err := NewRequest(apiKey)
	if err != nil {
		return nil, err
	}

	if version > req.MaxVersion() {
		return nil, fmt.Errorf("version %d exceeds max version %d for API key %d",
			version, req.MaxVersion(), apiKey)
	}

	req.SetVersion(version)

	if err := req.ReadFrom(payload); err != nil {
		return nil, fmt.Errorf("failed to decode request: %w", err)
	}

	return req, nil
}

// Encoder encodes Kafka protocol responses to bytes.
type Encoder struct{}

// NewEncoder creates a new protocol encoder.
func NewEncoder() *Encoder {
	return &Encoder{}
}

// EncodeResponse encodes a Kafka response to bytes.
func (e *Encoder) EncodeResponse(resp *Response) []byte {
	return resp.AppendTo(nil)
}

// EncodeResponseWithCorrelationID encodes a Kafka response with correlation ID.
func (e *Encoder) EncodeResponseWithCorrelationID(correlationID int32, resp *Response) []byte {
	buf := make([]byte, 0, 64)

	// Append correlation ID as big-endian int32
	buf = append(buf, byte(correlationID>>24), byte(correlationID>>16), byte(correlationID>>8), byte(correlationID))

	// For flexible versions (v1+ response header), append empty tag buffer
	if resp.IsFlexible() {
		buf = append(buf, 0)
	}

	return resp.AppendTo(buf)
}

// Key returns the API key for this request.
func (r *Request) Key() int16 {
	return r.inner.Key()
}

// MaxVersion returns the maximum supported version.
func (r *Request) MaxVersion() int16 {
	return r.inner.MaxVersion()
}

// SetVersion sets the protocol version.
func (r *Request) SetVersion(version int16) {
	r.inner.SetVersion(version)
}

// GetVersion returns the current protocol version.
func (r *Request) GetVersion() int16 {
	return r.inner.GetVersion()
}

// IsFlexible returns true if this version uses flexible encoding.
func (r *Request) IsFlexible() bool {
	return r.inner.IsFlexible()
}

// ReadFrom decodes the request from bytes.
func (r *Request) ReadFrom(src []byte) error {
	return r.inner.ReadFrom(src)
}

// AppendTo encodes the request to bytes.
func (r *Request) AppendTo(dst []byte) []byte {
	return r.inner.AppendTo(dst)
}

// ResponseKind returns a new empty response for this request type.
func (r *Request) ResponseKind() *Response {
	return &Response{inner: r.inner.ResponseKind()}
}

// Inner returns the underlying kmsg request type.
// Use type assertion to access specific request fields.
func (r *Request) Inner() kmsg.Request {
	return r.inner
}

// Key returns the API key for this response.
func (r *Response) Key() int16 {
	return r.inner.Key()
}

// MaxVersion returns the maximum supported version.
func (r *Response) MaxVersion() int16 {
	return r.inner.MaxVersion()
}

// SetVersion sets the protocol version.
func (r *Response) SetVersion(version int16) {
	r.inner.SetVersion(version)
}

// GetVersion returns the current protocol version.
func (r *Response) GetVersion() int16 {
	return r.inner.GetVersion()
}

// IsFlexible returns true if this version uses flexible encoding.
func (r *Response) IsFlexible() bool {
	return r.inner.IsFlexible()
}

// ReadFrom decodes the response from bytes.
func (r *Response) ReadFrom(src []byte) error {
	return r.inner.ReadFrom(src)
}

// AppendTo encodes the response to bytes.
func (r *Response) AppendTo(dst []byte) []byte {
	return r.inner.AppendTo(dst)
}

// Inner returns the underlying kmsg response type.
// Use type assertion to access specific response fields.
func (r *Response) Inner() kmsg.Response {
	return r.inner
}

// NewRequest creates a new request of the specified API key.
// Uses kmsg.Key.Request() which returns the appropriate request type.
func NewRequest(apiKey int16) (*Request, error) {
	key := kmsg.Key(apiKey)
	if apiKey < 0 || apiKey > kmsg.MaxKey {
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
	req := key.Request()
	if req == nil {
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
	return &Request{inner: req}, nil
}

// NewResponse creates a new response of the specified API key.
// Uses kmsg.Key.Response() which returns the appropriate response type.
func NewResponse(apiKey int16) (*Response, error) {
	key := kmsg.Key(apiKey)
	if apiKey < 0 || apiKey > kmsg.MaxKey {
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
	resp := key.Response()
	if resp == nil {
		return nil, fmt.Errorf("unsupported API key: %d", apiKey)
	}
	return &Response{inner: resp}, nil
}

// WrapRequest wraps an existing kmsg.Request.
func WrapRequest(req kmsg.Request) *Request {
	return &Request{inner: req}
}

// WrapResponse wraps an existing kmsg.Response.
func WrapResponse(resp kmsg.Response) *Response {
	return &Response{inner: resp}
}

// APIKeyName returns the human-readable name for an API key.
func APIKeyName(key int16) string {
	return kmsg.Key(key).Name()
}

// MaxSupportedVersion returns the maximum protocol version supported for an API key.
func MaxSupportedVersion(apiKey int16) (int16, error) {
	req, err := NewRequest(apiKey)
	if err != nil {
		return 0, err
	}
	return req.MaxVersion(), nil
}
