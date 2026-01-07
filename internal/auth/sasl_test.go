package auth

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSASLAuthenticator_HandleSASLHandshake(t *testing.T) {
	cs := NewCredentialStore()
	cs.Add("user", "pass")
	auth := NewSASLAuthenticator(cs, MechanismPLAIN)

	tests := []struct {
		name      string
		version   int16
		mechanism string
		wantCode  int16
		wantMechs []string
	}{
		{
			name:      "PLAIN mechanism supported",
			version:   0,
			mechanism: "PLAIN",
			wantCode:  ErrorCodeNone,
			wantMechs: []string{"PLAIN"},
		},
		{
			name:      "PLAIN mechanism v1",
			version:   1,
			mechanism: "PLAIN",
			wantCode:  ErrorCodeNone,
			wantMechs: []string{"PLAIN"},
		},
		{
			name:      "SCRAM-SHA-256 not supported",
			version:   0,
			mechanism: "SCRAM-SHA-256",
			wantCode:  ErrorCodeUnsupportedSASLMechanism,
			wantMechs: []string{"PLAIN"},
		},
		{
			name:      "unknown mechanism",
			version:   0,
			mechanism: "UNKNOWN",
			wantCode:  ErrorCodeUnsupportedSASLMechanism,
			wantMechs: []string{"PLAIN"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := kmsg.NewPtrSASLHandshakeRequest()
			req.SetVersion(tt.version)
			req.Mechanism = tt.mechanism

			resp := auth.HandleSASLHandshake(tt.version, req)

			if resp.ErrorCode != tt.wantCode {
				t.Errorf("ErrorCode = %d, want %d", resp.ErrorCode, tt.wantCode)
			}
			if len(resp.SupportedMechanisms) != len(tt.wantMechs) {
				t.Errorf("SupportedMechanisms = %v, want %v", resp.SupportedMechanisms, tt.wantMechs)
			} else {
				for i, mech := range resp.SupportedMechanisms {
					if mech != tt.wantMechs[i] {
						t.Errorf("SupportedMechanisms[%d] = %q, want %q", i, mech, tt.wantMechs[i])
					}
				}
			}
		})
	}
}

func TestSASLAuthenticator_HandleSASLAuthenticate(t *testing.T) {
	cs := NewCredentialStore()
	cs.Add("alice", "secret123")
	cs.Add("bob", "password456")
	auth := NewSASLAuthenticator(cs, MechanismPLAIN)

	tests := []struct {
		name      string
		version   int16
		mechanism string
		authBytes []byte
		wantCode  int16
	}{
		{
			name:      "valid auth - alice",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: buildPlainAuth("", "alice", "secret123"),
			wantCode:  ErrorCodeNone,
		},
		{
			name:      "valid auth - bob",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: buildPlainAuth("", "bob", "password456"),
			wantCode:  ErrorCodeNone,
		},
		{
			name:      "valid auth v2",
			version:   2,
			mechanism: MechanismPLAIN,
			authBytes: buildPlainAuth("", "alice", "secret123"),
			wantCode:  ErrorCodeNone,
		},
		{
			name:      "wrong password",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: buildPlainAuth("", "alice", "wrongpassword"),
			wantCode:  ErrorCodeSASLAuthenticationFailed,
		},
		{
			name:      "unknown user",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: buildPlainAuth("", "unknown", "anypassword"),
			wantCode:  ErrorCodeSASLAuthenticationFailed,
		},
		{
			name:      "empty username",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: buildPlainAuth("", "", "password"),
			wantCode:  ErrorCodeSASLAuthenticationFailed,
		},
		{
			name:      "empty auth bytes",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: []byte{},
			wantCode:  ErrorCodeSASLAuthenticationFailed,
		},
		{
			name:      "malformed auth bytes",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: []byte("not-valid-format"),
			wantCode:  ErrorCodeSASLAuthenticationFailed,
		},
		{
			name:      "wrong mechanism",
			version:   0,
			mechanism: "SCRAM-SHA-256",
			authBytes: buildPlainAuth("", "alice", "secret123"),
			wantCode:  ErrorCodeUnsupportedSASLMechanism,
		},
		{
			name:      "with authzid",
			version:   0,
			mechanism: MechanismPLAIN,
			authBytes: buildPlainAuth("admin", "alice", "secret123"),
			wantCode:  ErrorCodeNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := kmsg.NewPtrSASLAuthenticateRequest()
			req.SetVersion(tt.version)
			req.SASLAuthBytes = tt.authBytes

			resp := auth.HandleSASLAuthenticate(tt.version, req, tt.mechanism)

			if resp.ErrorCode != tt.wantCode {
				t.Errorf("ErrorCode = %d, want %d", resp.ErrorCode, tt.wantCode)
			}
		})
	}
}

func TestSASLAuthenticator_HandleSASLAuthenticate_NilStore(t *testing.T) {
	auth := NewSASLAuthenticator(nil, MechanismPLAIN)

	req := kmsg.NewPtrSASLAuthenticateRequest()
	req.SetVersion(0)
	req.SASLAuthBytes = buildPlainAuth("", "alice", "secret123")

	resp := auth.HandleSASLAuthenticate(0, req, MechanismPLAIN)

	if resp.ErrorCode != ErrorCodeSASLAuthenticationFailed {
		t.Errorf("ErrorCode = %d, want %d", resp.ErrorCode, ErrorCodeSASLAuthenticationFailed)
	}
	if resp.ErrorMessage == nil || *resp.ErrorMessage == "" {
		t.Errorf("ErrorMessage is empty, want non-empty")
	}
}

func TestParsePlainAuth(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		wantUser string
		wantPass string
		wantErr  bool
	}{
		{
			name:     "standard format",
			data:     buildPlainAuth("", "user", "password"),
			wantUser: "user",
			wantPass: "password",
			wantErr:  false,
		},
		{
			name:     "with authzid",
			data:     buildPlainAuth("authzid", "user", "password"),
			wantUser: "user",
			wantPass: "password",
			wantErr:  false,
		},
		{
			name:     "empty password allowed",
			data:     buildPlainAuth("", "user", ""),
			wantUser: "user",
			wantPass: "",
			wantErr:  false,
		},
		{
			name:     "password with special chars",
			data:     buildPlainAuth("", "user", "p@ss\x00w\x00rd"),
			wantUser: "",
			wantPass: "",
			wantErr:  true, // NUL in password creates too many parts
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "only one null",
			data:    []byte{0, 'u', 's', 'e', 'r'},
			wantErr: true,
		},
		{
			name:     "empty username",
			data:     buildPlainAuth("", "", "password"),
			wantUser: "",
			wantPass: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, pass, err := parsePlainAuth(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePlainAuth() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if user != tt.wantUser {
					t.Errorf("parsePlainAuth() user = %q, want %q", user, tt.wantUser)
				}
				if pass != tt.wantPass {
					t.Errorf("parsePlainAuth() pass = %q, want %q", pass, tt.wantPass)
				}
			}
		})
	}
}

func TestSASLAuthenticator_IsMechanismSupported(t *testing.T) {
	cs := NewCredentialStore()
	auth := NewSASLAuthenticator(cs, MechanismPLAIN)

	if !auth.IsMechanismSupported("PLAIN") {
		t.Error("IsMechanismSupported(PLAIN) = false, want true")
	}
	if auth.IsMechanismSupported("SCRAM-SHA-256") {
		t.Error("IsMechanismSupported(SCRAM-SHA-256) = true, want false")
	}
}

func TestSASLAuthenticator_EnabledMechanisms(t *testing.T) {
	cs := NewCredentialStore()
	auth := NewSASLAuthenticator(cs, MechanismPLAIN)

	mechs := auth.EnabledMechanisms()
	if len(mechs) != 1 || mechs[0] != "PLAIN" {
		t.Errorf("EnabledMechanisms() = %v, want [PLAIN]", mechs)
	}
}

// buildPlainAuth builds SASL/PLAIN auth bytes in the format:
// [NUL]authzid[NUL]authcid[NUL]passwd
func buildPlainAuth(authzid, authcid, passwd string) []byte {
	return []byte(authzid + "\x00" + authcid + "\x00" + passwd)
}
