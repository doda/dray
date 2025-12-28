package integration

import (
	"os"
	"testing"

	"github.com/dray-io/dray/internal/auth"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSASL_HandshakeRequest(t *testing.T) {
	cs := auth.NewCredentialStore()
	cs.Add("testuser", "testpass")
	authenticator := auth.NewSASLAuthenticator(cs, auth.MechanismPLAIN)

	tests := []struct {
		name      string
		mechanism string
		wantErr   bool
	}{
		{
			name:      "PLAIN mechanism supported",
			mechanism: "PLAIN",
			wantErr:   false,
		},
		{
			name:      "SCRAM-SHA-256 not supported",
			mechanism: "SCRAM-SHA-256",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := kmsg.NewPtrSASLHandshakeRequest()
			req.SetVersion(1)
			req.Mechanism = tt.mechanism

			resp := authenticator.HandleSASLHandshake(1, req)

			if tt.wantErr {
				if resp.ErrorCode == 0 {
					t.Error("expected error code, got 0")
				}
			} else {
				if resp.ErrorCode != 0 {
					t.Errorf("expected no error, got code %d", resp.ErrorCode)
				}
			}

			if len(resp.SupportedMechanisms) == 0 {
				t.Error("expected at least one supported mechanism")
			}
			if resp.SupportedMechanisms[0] != "PLAIN" {
				t.Errorf("expected PLAIN mechanism, got %s", resp.SupportedMechanisms[0])
			}
		})
	}
}

func TestSASL_AuthenticateWithPLAIN(t *testing.T) {
	cs := auth.NewCredentialStore()
	cs.Add("alice", "secret123")
	cs.Add("bob", "password456")
	authenticator := auth.NewSASLAuthenticator(cs, auth.MechanismPLAIN)

	tests := []struct {
		name     string
		authzid  string
		username string
		password string
		wantErr  bool
	}{
		{
			name:     "valid alice credentials",
			authzid:  "",
			username: "alice",
			password: "secret123",
			wantErr:  false,
		},
		{
			name:     "valid bob credentials",
			authzid:  "",
			username: "bob",
			password: "password456",
			wantErr:  false,
		},
		{
			name:     "wrong password",
			authzid:  "",
			username: "alice",
			password: "wrongpassword",
			wantErr:  true,
		},
		{
			name:     "unknown user",
			authzid:  "",
			username: "unknown",
			password: "anypassword",
			wantErr:  true,
		},
		{
			name:     "with authzid (ignored)",
			authzid:  "admin",
			username: "alice",
			password: "secret123",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build SASL/PLAIN auth bytes: [NUL]authzid[NUL]authcid[NUL]passwd
			authBytes := []byte(tt.authzid + "\x00" + tt.username + "\x00" + tt.password)

			req := kmsg.NewPtrSASLAuthenticateRequest()
			req.SetVersion(2)
			req.SASLAuthBytes = authBytes

			resp := authenticator.HandleSASLAuthenticate(2, req, auth.MechanismPLAIN)

			if tt.wantErr {
				if resp.ErrorCode == 0 {
					t.Error("expected error code, got 0")
				}
				t.Logf("Got expected error: %d", resp.ErrorCode)
			} else {
				if resp.ErrorCode != 0 {
					msg := ""
					if resp.ErrorMessage != nil {
						msg = *resp.ErrorMessage
					}
					t.Errorf("expected no error, got code %d: %s", resp.ErrorCode, msg)
				}
			}
		})
	}
}

func TestSASL_CredentialFromFile(t *testing.T) {
	// Write credentials to temp file
	tmpDir := t.TempDir()
	credFile := tmpDir + "/credentials.txt"

	credContent := `# SASL credentials file
admin:supersecret
user1:password1
user2:password2
`
	if err := os.WriteFile(credFile, []byte(credContent), 0644); err != nil {
		t.Fatalf("Failed to write credentials file: %v", err)
	}

	cs := auth.NewCredentialStore()
	if err := cs.LoadFromFile(credFile); err != nil {
		t.Fatalf("Failed to load credentials: %v", err)
	}

	if cs.Count() != 3 {
		t.Errorf("Expected 3 credentials, got %d", cs.Count())
	}

	// Verify each credential
	tests := []struct {
		username string
		password string
		wantErr  bool
	}{
		{"admin", "supersecret", false},
		{"user1", "password1", false},
		{"user2", "password2", false},
		{"admin", "wrong", true},
		{"unknown", "password", true},
	}

	for _, tt := range tests {
		err := cs.Validate(tt.username, tt.password)
		if (err != nil) != tt.wantErr {
			t.Errorf("Validate(%s, %s) error = %v, wantErr %v", tt.username, tt.password, err, tt.wantErr)
		}
	}
}

func TestSASL_CredentialFromEnvString(t *testing.T) {
	cs := auth.NewCredentialStore()
	if err := cs.LoadFromString("alice:secret,bob:password,admin:supersecret"); err != nil {
		t.Fatalf("Failed to load credentials: %v", err)
	}

	if cs.Count() != 3 {
		t.Errorf("Expected 3 credentials, got %d", cs.Count())
	}

	// Verify each credential
	tests := []struct {
		username string
		password string
		wantErr  bool
	}{
		{"alice", "secret", false},
		{"bob", "password", false},
		{"admin", "supersecret", false},
		{"alice", "wrong", true},
	}

	for _, tt := range tests {
		err := cs.Validate(tt.username, tt.password)
		if (err != nil) != tt.wantErr {
			t.Errorf("Validate(%s, %s) error = %v, wantErr %v", tt.username, tt.password, err, tt.wantErr)
		}
	}
}

func TestSASL_ApiVersionsIncludesSASLApis(t *testing.T) {
	apis := protocol.GetSupportedAPIsWithSASL()

	var foundHandshake, foundAuthenticate bool
	for _, api := range apis {
		if api.APIKey == 17 {
			foundHandshake = true
			if api.MinVersion != 0 || api.MaxVersion != 1 {
				t.Errorf("SASLHandshake version range: want 0-1, got %d-%d", api.MinVersion, api.MaxVersion)
			}
		}
		if api.APIKey == 36 {
			foundAuthenticate = true
			if api.MinVersion != 0 || api.MaxVersion != 2 {
				t.Errorf("SASLAuthenticate version range: want 0-2, got %d-%d", api.MinVersion, api.MaxVersion)
			}
		}
	}

	if !foundHandshake {
		t.Error("SASLHandshake (key 17) not found in SASL-enabled API list")
	}
	if !foundAuthenticate {
		t.Error("SASLAuthenticate (key 36) not found in SASL-enabled API list")
	}
}

func TestSASL_ApiVersionsExcludesSASLApisWhenDisabled(t *testing.T) {
	apis := protocol.GetSupportedAPIs()

	for _, api := range apis {
		if api.APIKey == 17 {
			t.Error("SASLHandshake (key 17) should not be in non-SASL API list")
		}
		if api.APIKey == 36 {
			t.Error("SASLAuthenticate (key 36) should not be in non-SASL API list")
		}
	}
}

func TestSASL_ApiVersionsResponseWithSASL(t *testing.T) {
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)

	resp := protocol.HandleApiVersionsWithOptions(3, req, true)

	if resp.ErrorCode != 0 {
		t.Errorf("Expected no error, got %d", resp.ErrorCode)
	}

	var foundHandshake, foundAuthenticate bool
	for _, api := range resp.ApiKeys {
		if api.ApiKey == 17 {
			foundHandshake = true
		}
		if api.ApiKey == 36 {
			foundAuthenticate = true
		}
	}

	if !foundHandshake {
		t.Error("SASLHandshake (key 17) not in ApiVersions response with SASL enabled")
	}
	if !foundAuthenticate {
		t.Error("SASLAuthenticate (key 36) not in ApiVersions response with SASL enabled")
	}
}

func TestSASL_ApiVersionsResponseWithoutSASL(t *testing.T) {
	req := kmsg.NewPtrApiVersionsRequest()
	req.SetVersion(3)

	resp := protocol.HandleApiVersionsWithOptions(3, req, false)

	if resp.ErrorCode != 0 {
		t.Errorf("Expected no error, got %d", resp.ErrorCode)
	}

	for _, api := range resp.ApiKeys {
		if api.ApiKey == 17 {
			t.Error("SASLHandshake (key 17) should not be in response when SASL disabled")
		}
		if api.ApiKey == 36 {
			t.Error("SASLAuthenticate (key 36) should not be in response when SASL disabled")
		}
	}
}
