package oxia

import (
	"context"
	"testing"
	"time"
)

// Oxia requires a minimum session timeout of 5 seconds
const minSessionTimeout = 5 * time.Second

// TestEphemeral_SessionBinding verifies that ephemeral keys are bound to the client session.
func TestEphemeral_SessionBinding(t *testing.T) {
	server := StartTestServer(t)
	addr := server.Addr()

	cfg := Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()
	key := "/test/ephemeral/session-binding"
	value := []byte("ephemeral-value")

	// Create ephemeral key
	version, err := store.PutEphemeral(ctx, key, value)
	if err != nil {
		t.Fatalf("PutEphemeral failed: %v", err)
	}
	if version < 1 {
		t.Errorf("expected version >= 1, got %d", version)
	}

	// Verify the key exists
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("ephemeral key should exist immediately after creation")
	}
	if string(result.Value) != string(value) {
		t.Errorf("value mismatch: got %q, want %q", result.Value, value)
	}

	store.Close()
}

// TestEphemeral_DeletedOnSessionExpiry verifies that ephemeral keys are deleted
// when the client session expires.
func TestEphemeral_DeletedOnSessionExpiry(t *testing.T) {
	server := StartTestServer(t)
	addr := server.Addr()

	cfg := Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()
	key := "/test/ephemeral/session-expiry"

	// Create ephemeral key
	_, err = store.PutEphemeral(ctx, key, []byte("will-be-deleted"))
	if err != nil {
		t.Fatalf("PutEphemeral failed: %v", err)
	}

	// Verify it exists
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("ephemeral key should exist")
	}

	// Close the store (ends the session)
	store.Close()

	// Create a new store to verify the key was deleted
	store2, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	// Wait for session expiry plus some buffer
	// Session timeout is 5s, so we need to wait at least that plus grace period
	time.Sleep(minSessionTimeout + 2*time.Second)

	// Key should be deleted
	result, err = store2.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("ephemeral key should be deleted after session expires")
	}
}

// TestEphemeral_SurvivesWhileSessionActive verifies that ephemeral keys persist
// as long as the client session remains active.
func TestEphemeral_SurvivesWhileSessionActive(t *testing.T) {
	server := StartTestServer(t)
	addr := server.Addr()

	cfg := Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "/test/ephemeral/survives-active"

	// Create ephemeral key
	_, err = store.PutEphemeral(ctx, key, []byte("persistent-while-active"))
	if err != nil {
		t.Fatalf("PutEphemeral failed: %v", err)
	}

	// Keep the session alive by making periodic requests
	// Check that the key still exists over multiple intervals (2 seconds each)
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)

		result, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed at iteration %d: %v", i, err)
		}
		if !result.Exists {
			t.Errorf("ephemeral key should still exist at iteration %d", i)
		}
	}

	// Final check - key should still exist
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("final Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("ephemeral key should survive while session is active")
	}
}

// TestEphemeral_SessionRenewalKeepsKeyAlive verifies that the client's session
// renewal mechanism keeps ephemeral keys alive.
func TestEphemeral_SessionRenewalKeepsKeyAlive(t *testing.T) {
	server := StartTestServer(t)
	addr := server.Addr()

	// Use minimum session timeout; the Oxia client should auto-renew
	cfg := Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "/test/ephemeral/session-renewal"

	// Create ephemeral key
	_, err = store.PutEphemeral(ctx, key, []byte("renewed-session"))
	if err != nil {
		t.Fatalf("PutEphemeral failed: %v", err)
	}

	// Wait longer than the session timeout but keep the session active
	// The Oxia client should automatically renew the session
	// We wait for 10 seconds (2x the session timeout) to verify renewal works
	totalWait := 2 * minSessionTimeout
	checkInterval := 1 * time.Second
	deadline := time.Now().Add(totalWait)

	for time.Now().Before(deadline) {
		time.Sleep(checkInterval)

		result, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !result.Exists {
			t.Fatal("ephemeral key should exist while session is being renewed")
		}
	}

	// Key should still exist after waiting longer than session timeout
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("final Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("ephemeral key should survive session renewal")
	}
}

// TestEphemeral_MultipleKeys verifies that multiple ephemeral keys can be
// created and all are deleted when the session ends.
func TestEphemeral_MultipleKeys(t *testing.T) {
	server := StartTestServer(t)
	addr := server.Addr()

	cfg := Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: minSessionTimeout,
	}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()
	keys := []string{
		"/test/ephemeral/multi/key1",
		"/test/ephemeral/multi/key2",
		"/test/ephemeral/multi/key3",
	}

	// Create multiple ephemeral keys
	for _, key := range keys {
		_, err := store.PutEphemeral(ctx, key, []byte("value-"+key))
		if err != nil {
			t.Fatalf("PutEphemeral for %s failed: %v", key, err)
		}
	}

	// Verify all exist
	for _, key := range keys {
		result, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get for %s failed: %v", key, err)
		}
		if !result.Exists {
			t.Errorf("ephemeral key %s should exist", key)
		}
	}

	// Close the store
	store.Close()

	// Create new store and wait for expiry
	store2, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	time.Sleep(minSessionTimeout + 2*time.Second)

	// All keys should be deleted
	for _, key := range keys {
		result, err := store2.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get for %s failed: %v", key, err)
		}
		if result.Exists {
			t.Errorf("ephemeral key %s should be deleted after session expires", key)
		}
	}
}

// TestEphemeral_UpdateValue verifies that ephemeral keys can have their value updated.
func TestEphemeral_UpdateValue(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	key := "/test/ephemeral/update"

	// Create ephemeral key
	v1, err := store.PutEphemeral(ctx, key, []byte("original"))
	if err != nil {
		t.Fatalf("PutEphemeral failed: %v", err)
	}

	// Update the key
	v2, err := store.PutEphemeral(ctx, key, []byte("updated"))
	if err != nil {
		t.Fatalf("second PutEphemeral failed: %v", err)
	}

	if v2 <= v1 {
		t.Errorf("version should increase on update: v1=%d, v2=%d", v1, v2)
	}

	// Verify updated value
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(result.Value) != "updated" {
		t.Errorf("expected updated value, got %q", result.Value)
	}
}

// TestEphemeral_RegularDeleteDoesNotAffectSessionBinding verifies that
// deleting an ephemeral key with regular Delete works.
func TestEphemeral_RegularDeleteWorks(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	key := "/test/ephemeral/delete"

	// Create ephemeral key
	_, err := store.PutEphemeral(ctx, key, []byte("to-delete"))
	if err != nil {
		t.Fatalf("PutEphemeral failed: %v", err)
	}

	// Verify it exists
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("ephemeral key should exist")
	}

	// Delete it explicitly
	err = store.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	result, err = store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if result.Exists {
		t.Error("ephemeral key should be deleted after explicit Delete")
	}
}

// TestEphemeral_ClosedStoreReturnsError verifies that PutEphemeral returns
// an error when called on a closed store.
func TestEphemeral_ClosedStoreReturnsError(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	// Close the store (test cleanup will try to close again, but that's OK)
	store.Close()

	// Try to create ephemeral key
	_, err := store.PutEphemeral(ctx, "/test/closed", []byte("value"))
	if err == nil {
		t.Error("expected error when calling PutEphemeral on closed store")
	}
}
