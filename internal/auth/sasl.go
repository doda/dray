package auth

import (
	"bytes"
	"errors"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// SASL mechanism constants.
const (
	MechanismPLAIN = "PLAIN"
)

// SASL error codes.
const (
	// ErrorCodeNone indicates no error.
	ErrorCodeNone = 0
	// ErrorCodeUnsupportedSASLMechanism indicates the mechanism is not supported.
	ErrorCodeUnsupportedSASLMechanism = 33
	// ErrorCodeIllegalSASLState indicates invalid SASL state.
	ErrorCodeIllegalSASLState = 34
	// ErrorCodeSASLAuthenticationFailed indicates authentication failed.
	ErrorCodeSASLAuthenticationFailed = 58
)

// SASLAuthenticator handles SASL authentication.
type SASLAuthenticator struct {
	credentials      *CredentialStore
	enabledMechanism string
}

// NewSASLAuthenticator creates a new SASL authenticator.
func NewSASLAuthenticator(credentials *CredentialStore, mechanism string) *SASLAuthenticator {
	return &SASLAuthenticator{
		credentials:      credentials,
		enabledMechanism: mechanism,
	}
}

// IsMechanismSupported checks if the given mechanism is supported.
func (a *SASLAuthenticator) IsMechanismSupported(mechanism string) bool {
	return mechanism == a.enabledMechanism && mechanism == MechanismPLAIN
}

// EnabledMechanisms returns the list of enabled mechanisms.
func (a *SASLAuthenticator) EnabledMechanisms() []string {
	return []string{a.enabledMechanism}
}

// HandleSASLHandshake handles the SASLHandshake request (API key 17).
// This tells the client which SASL mechanisms are supported.
func (a *SASLAuthenticator) HandleSASLHandshake(version int16, req *kmsg.SASLHandshakeRequest) *kmsg.SASLHandshakeResponse {
	resp := kmsg.NewPtrSASLHandshakeResponse()
	resp.SetVersion(version)

	// Check if the requested mechanism is supported
	if a.IsMechanismSupported(req.Mechanism) {
		resp.ErrorCode = ErrorCodeNone
	} else {
		resp.ErrorCode = ErrorCodeUnsupportedSASLMechanism
	}

	// Return the list of enabled mechanisms
	resp.SupportedMechanisms = a.EnabledMechanisms()

	return resp
}

// HandleSASLAuthenticate handles the SASLAuthenticate request (API key 36).
// For PLAIN mechanism, the auth bytes format is: [NUL]authzid[NUL]authcid[NUL]passwd
// where authzid is optional (and usually empty), authcid is the username,
// and passwd is the password.
func (a *SASLAuthenticator) HandleSASLAuthenticate(version int16, req *kmsg.SASLAuthenticateRequest, mechanism string) *kmsg.SASLAuthenticateResponse {
	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(version)

	// For v1+, set SessionLifetimeMillis
	if version >= 1 {
		resp.SessionLifetimeMillis = 0 // No session lifetime limit
	}

	// Check mechanism
	if mechanism != MechanismPLAIN {
		resp.ErrorCode = ErrorCodeUnsupportedSASLMechanism
		resp.ErrorMessage = strPtr("unsupported SASL mechanism")
		return resp
	}

	// Parse PLAIN authentication data
	username, password, err := parsePlainAuth(req.SASLAuthBytes)
	if err != nil {
		resp.ErrorCode = ErrorCodeSASLAuthenticationFailed
		resp.ErrorMessage = strPtr("invalid SASL/PLAIN authentication data")
		return resp
	}

	// Validate credentials
	if err := a.credentials.Validate(username, password); err != nil {
		resp.ErrorCode = ErrorCodeSASLAuthenticationFailed
		resp.ErrorMessage = strPtr("authentication failed")
		return resp
	}

	// Authentication successful
	resp.ErrorCode = ErrorCodeNone
	resp.SASLAuthBytes = nil // No additional data for PLAIN

	return resp
}

// parsePlainAuth parses SASL/PLAIN authentication data.
// Format: [NUL]authzid[NUL]authcid[NUL]passwd
// authzid (authorization identity) is typically empty.
// authcid (authentication identity) is the username.
// passwd is the password.
func parsePlainAuth(data []byte) (username, password string, err error) {
	if len(data) == 0 {
		return "", "", errors.New("empty auth data")
	}

	// Split by NUL bytes
	parts := bytes.Split(data, []byte{0})

	// We expect exactly 3 parts: authzid, authcid (username), passwd
	if len(parts) != 3 {
		return "", "", errors.New("invalid PLAIN auth format")
	}

	// authzid is parts[0] - we ignore it for now
	// authcid (username) is parts[1]
	// passwd is parts[2]

	username = string(parts[1])
	password = string(parts[2])

	if username == "" {
		return "", "", errors.New("empty username")
	}

	return username, password, nil
}

func strPtr(s string) *string {
	return &s
}

// AuthResult contains the result of SASL authentication.
type AuthResult struct {
	Authenticated bool
	Username      string
}
