package auth

import (
	"bufio"
	"crypto/subtle"
	"errors"
	"os"
	"strings"
	"sync"
)

// ErrInvalidCredentials is returned when authentication fails.
var ErrInvalidCredentials = errors.New("invalid credentials")

// ErrNoCredentials is returned when no credentials are configured.
var ErrNoCredentials = errors.New("no credentials configured")

// CredentialStore manages SASL credentials.
type CredentialStore struct {
	mu          sync.RWMutex
	credentials map[string]string // username -> password
}

// NewCredentialStore creates an empty credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		credentials: make(map[string]string),
	}
}

// LoadFromFile loads credentials from a file.
// Format: one "username:password" pair per line.
// Lines starting with # are ignored as comments.
// Empty lines are ignored.
func (cs *CredentialStore) LoadFromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	cs.mu.Lock()
	defer cs.mu.Unlock()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue // Skip malformed lines silently
		}

		username := strings.TrimSpace(parts[0])
		password := parts[1] // Don't trim password - spaces may be intentional

		if username == "" {
			continue // Skip entries with empty username
		}

		cs.credentials[username] = password
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

// LoadFromString loads credentials from a comma-separated string.
// Format: "user1:pass1,user2:pass2"
func (cs *CredentialStore) LoadFromString(data string) error {
	if data == "" {
		return nil
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	pairs := strings.Split(data, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue // Skip malformed entries
		}

		username := strings.TrimSpace(parts[0])
		password := parts[1] // Don't trim password

		if username == "" {
			continue
		}

		cs.credentials[username] = password
	}

	return nil
}

// Add adds or updates a credential.
func (cs *CredentialStore) Add(username, password string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.credentials[username] = password
}

// Validate checks if the given username/password is valid.
// Uses constant-time comparison to prevent timing attacks.
func (cs *CredentialStore) Validate(username, password string) error {
	cs.mu.RLock()
	storedPassword, ok := cs.credentials[username]
	cs.mu.RUnlock()

	if !ok {
		// Perform a dummy comparison to maintain constant time
		subtle.ConstantTimeCompare([]byte(password), []byte("dummy-password-comparison"))
		return ErrInvalidCredentials
	}

	if subtle.ConstantTimeCompare([]byte(password), []byte(storedPassword)) != 1 {
		return ErrInvalidCredentials
	}

	return nil
}

// Count returns the number of stored credentials.
func (cs *CredentialStore) Count() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.credentials)
}

// IsEmpty returns true if no credentials are configured.
func (cs *CredentialStore) IsEmpty() bool {
	return cs.Count() == 0
}
