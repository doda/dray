package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCredentialStore_LoadFromString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantCnt  int
		validate []struct {
			user     string
			password string
			wantErr  bool
		}
	}{
		{
			name:    "single user",
			input:   "user1:password1",
			wantCnt: 1,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"user1", "password1", false},
				{"user1", "wrongpass", true},
				{"unknown", "password1", true},
			},
		},
		{
			name:    "multiple users",
			input:   "user1:pass1,user2:pass2,admin:secret",
			wantCnt: 3,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"user1", "pass1", false},
				{"user2", "pass2", false},
				{"admin", "secret", false},
				{"user1", "pass2", true},
			},
		},
		{
			name:    "with spaces",
			input:   "user1:pass1 , user2:pass2",
			wantCnt: 2,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"user1", "pass1", false},
				{"user2", "pass2", false},
			},
		},
		{
			name:    "password with colon",
			input:   "user1:pass:with:colons",
			wantCnt: 1,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"user1", "pass:with:colons", false},
			},
		},
		{
			name:    "empty string",
			input:   "",
			wantCnt: 0,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"anyuser", "anypass", true},
			},
		},
		{
			name:    "malformed entries skipped",
			input:   "valid:password,invalidnopassword,also:valid",
			wantCnt: 2,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"valid", "password", false},
				{"also", "valid", false},
			},
		},
		{
			name:    "empty username skipped",
			input:   ":password,valid:pass",
			wantCnt: 1,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"valid", "pass", false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewCredentialStore()
			if err := cs.LoadFromString(tt.input); err != nil {
				t.Fatalf("LoadFromString() error = %v", err)
			}

			if got := cs.Count(); got != tt.wantCnt {
				t.Errorf("Count() = %d, want %d", got, tt.wantCnt)
			}

			for _, v := range tt.validate {
				err := cs.Validate(v.user, v.password)
				if (err != nil) != v.wantErr {
					t.Errorf("Validate(%q, %q) error = %v, wantErr %v", v.user, v.password, err, v.wantErr)
				}
			}
		})
	}
}

func TestCredentialStore_LoadFromFile(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantCnt  int
		validate []struct {
			user     string
			password string
			wantErr  bool
		}
	}{
		{
			name: "basic file",
			content: `user1:password1
user2:password2
`,
			wantCnt: 2,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"user1", "password1", false},
				{"user2", "password2", false},
			},
		},
		{
			name: "with comments and empty lines",
			content: `# This is a comment
user1:password1

# Another comment
user2:password2

`,
			wantCnt: 2,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"user1", "password1", false},
				{"user2", "password2", false},
			},
		},
		{
			name: "password with special chars",
			content: `admin:P@ssw0rd!#$%
user:pass:with:colons
`,
			wantCnt: 2,
			validate: []struct {
				user     string
				password string
				wantErr  bool
			}{
				{"admin", "P@ssw0rd!#$%", false},
				{"user", "pass:with:colons", false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "credentials.txt")
			if err := os.WriteFile(path, []byte(tt.content), 0600); err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}

			cs := NewCredentialStore()
			if err := cs.LoadFromFile(path); err != nil {
				t.Fatalf("LoadFromFile() error = %v", err)
			}

			if got := cs.Count(); got != tt.wantCnt {
				t.Errorf("Count() = %d, want %d", got, tt.wantCnt)
			}

			for _, v := range tt.validate {
				err := cs.Validate(v.user, v.password)
				if (err != nil) != v.wantErr {
					t.Errorf("Validate(%q, %q) error = %v, wantErr %v", v.user, v.password, err, v.wantErr)
				}
			}
		})
	}
}

func TestCredentialStore_LoadFromFile_NotFound(t *testing.T) {
	cs := NewCredentialStore()
	err := cs.LoadFromFile("/nonexistent/path/credentials.txt")
	if err == nil {
		t.Error("LoadFromFile() expected error for non-existent file")
	}
}

func TestCredentialStore_Add(t *testing.T) {
	cs := NewCredentialStore()
	cs.Add("user1", "password1")

	if cs.Count() != 1 {
		t.Errorf("Count() = %d, want 1", cs.Count())
	}

	if err := cs.Validate("user1", "password1"); err != nil {
		t.Errorf("Validate() error = %v after Add()", err)
	}

	// Overwrite
	cs.Add("user1", "newpassword")
	if err := cs.Validate("user1", "newpassword"); err != nil {
		t.Errorf("Validate() error = %v after overwrite", err)
	}
	if err := cs.Validate("user1", "password1"); err == nil {
		t.Error("Validate() should fail with old password after overwrite")
	}
}

func TestCredentialStore_IsEmpty(t *testing.T) {
	cs := NewCredentialStore()
	if !cs.IsEmpty() {
		t.Error("IsEmpty() = false, want true for new store")
	}

	cs.Add("user", "pass")
	if cs.IsEmpty() {
		t.Error("IsEmpty() = true, want false after Add()")
	}
}

func TestCredentialStore_Validate_ConstantTime(t *testing.T) {
	// This is a simple sanity check that we don't short-circuit on username lookup.
	// True constant-time verification is hard to test, but we check basic behavior.
	cs := NewCredentialStore()
	cs.Add("validuser", "validpass")

	// Both should fail but go through the comparison
	err1 := cs.Validate("validuser", "wrongpass")
	err2 := cs.Validate("unknownuser", "anypass")

	if err1 == nil {
		t.Error("Validate() should fail for wrong password")
	}
	if err2 == nil {
		t.Error("Validate() should fail for unknown user")
	}
	if err1 != ErrInvalidCredentials {
		t.Errorf("Validate() error = %v, want ErrInvalidCredentials", err1)
	}
	if err2 != ErrInvalidCredentials {
		t.Errorf("Validate() error = %v, want ErrInvalidCredentials", err2)
	}
}
