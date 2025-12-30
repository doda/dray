package oxia

import (
	"context"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
)

// These tests use an embedded Oxia standalone server by default.
// To test against an external server, set the OXIA_SERVICE_ADDRESS environment variable.

// newIntegrationTestStore creates a new test store with its own embedded Oxia server.
// Each test gets a fresh server to ensure isolation.
func newIntegrationTestStore(t *testing.T) *Store {
	t.Helper()

	server := StartTestServer(t)
	addr := server.Addr()

	cfg := Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: 15 * time.Second,
	}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	t.Cleanup(func() {
		store.Close()
	})

	return store
}

func TestIntegration_BasicGetPut(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	// Test Put
	key := "/test/key1"
	value := []byte("test-value-1")

	version, err := store.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	// Versions should be >= 1 (we map Oxia's 0-based to 1-based)
	if version < 1 {
		t.Errorf("expected version >= 1, got %d", version)
	}

	// Test Get
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !result.Exists {
		t.Error("expected key to exist")
	}
	if string(result.Value) != string(value) {
		t.Errorf("value mismatch: got %q, want %q", result.Value, value)
	}
	if result.Version != version {
		t.Errorf("version mismatch: got %d, want %d", result.Version, version)
	}
}

func TestIntegration_GetNonExistent(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	result, err := store.Get(ctx, "/nonexistent/key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("expected key to not exist")
	}
}

func TestIntegration_PutWithVersion_CAS(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	key := "/test/cas-key"
	value1 := []byte("value1")
	value2 := []byte("value2")

	// First put should succeed
	version1, err := store.Put(ctx, key, value1)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// Update with correct version should succeed
	version2, err := store.Put(ctx, key, value2, metadata.WithExpectedVersion(version1))
	if err != nil {
		t.Fatalf("Second Put with correct version failed: %v", err)
	}
	if version2 <= version1 {
		t.Errorf("expected version to increase: got %d, previous %d", version2, version1)
	}

	// Update with old version should fail
	_, err = store.Put(ctx, key, []byte("value3"), metadata.WithExpectedVersion(version1))
	if err != metadata.ErrVersionMismatch {
		t.Errorf("expected ErrVersionMismatch, got %v", err)
	}

	// Verify current value
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(result.Value) != string(value2) {
		t.Errorf("value mismatch: got %q, want %q", result.Value, value2)
	}
}

func TestIntegration_Delete(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	key := "/test/delete-key"
	value := []byte("to-be-deleted")

	// Put
	_, err := store.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete
	err = store.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	result, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result.Exists {
		t.Error("expected key to be deleted")
	}

	// Delete again should be idempotent
	err = store.Delete(ctx, key)
	if err != nil {
		t.Errorf("Second Delete should be idempotent, got: %v", err)
	}
}

func TestIntegration_List(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	prefix := "/test/list/"

	// Create some keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		_, err := store.Put(ctx, prefix+k, []byte("value-"+k))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// List all
	results, err := store.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(results) != len(keys) {
		t.Errorf("expected %d results, got %d", len(keys), len(results))
	}

	// List with limit
	results, err = store.List(ctx, prefix, "", 3)
	if err != nil {
		t.Fatalf("List with limit failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestIntegration_Transaction(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	key1 := "/test/txn/key1"
	key2 := "/test/txn/key2"

	// Setup initial state
	v1, err := store.Put(ctx, key1, []byte("initial1"))
	if err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}

	// Execute transaction
	err = store.Txn(ctx, key1, func(txn metadata.Txn) error {
		// Read current value
		val, _, err := txn.Get(key1)
		if err != nil {
			return err
		}

		// Update with version check
		txn.PutWithVersion(key1, []byte("updated1"), v1)

		// Create new key
		txn.Put(key2, append(val, []byte("-derived")...))

		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify results
	result1, _ := store.Get(ctx, key1)
	if string(result1.Value) != "updated1" {
		t.Errorf("key1 value mismatch: got %q", result1.Value)
	}

	result2, _ := store.Get(ctx, key2)
	if string(result2.Value) != "initial1-derived" {
		t.Errorf("key2 value mismatch: got %q", result2.Value)
	}
}

func TestIntegration_TransactionConflict(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	key := "/test/txn-conflict/key"

	// Setup initial state
	_, err := store.Put(ctx, key, []byte("initial"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get version
	result, _ := store.Get(ctx, key)
	version := result.Version

	// Modify outside transaction
	_, err = store.Put(ctx, key, []byte("modified-externally"))
	if err != nil {
		t.Fatalf("External Put failed: %v", err)
	}

	// Transaction with stale version should fail
	err = store.Txn(ctx, key, func(txn metadata.Txn) error {
		txn.PutWithVersion(key, []byte("transaction-update"), version)
		return nil
	})
	if err != metadata.ErrTxnConflict {
		t.Errorf("expected ErrTxnConflict, got %v", err)
	}
}

func TestIntegration_TransactionRollbackOnPartialFailure(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx := context.Background()

	key1 := "/test/txn-rollback/key1"
	key2 := "/test/txn-rollback/key2"

	version, err := store.Put(ctx, key2, []byte("value2"))
	if err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}

	err = store.Txn(ctx, key1, func(txn metadata.Txn) error {
		txn.Put(key1, []byte("value1"))
		txn.PutWithVersion(key2, []byte("value2-updated"), version+1)
		return nil
	})
	if err != metadata.ErrTxnConflict {
		t.Fatalf("expected ErrTxnConflict, got %v", err)
	}

	result1, err := store.Get(ctx, key1)
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if result1.Exists {
		t.Errorf("expected key1 to be absent after rollback, got %q", result1.Value)
	}

	result2, err := store.Get(ctx, key2)
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if string(result2.Value) != "value2" {
		t.Errorf("expected key2 to be unchanged, got %q", result2.Value)
	}
}

func TestIntegration_Ephemeral(t *testing.T) {
	server := StartTestServer(t)
	addr := server.Addr()

	cfg := Config{
		ServiceAddress: addr,
		Namespace:      "default",
		RequestTimeout: 10 * time.Second,
		SessionTimeout: 5 * time.Second,
	}

	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx := context.Background()
	key := "/test/ephemeral/key"

	// Create ephemeral key
	_, err = store.PutEphemeral(ctx, key, []byte("ephemeral-value"))
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

	// Close the store
	store.Close()

	// Create new store with same namespace
	store2, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	// Wait a bit for session expiry
	time.Sleep(100 * time.Millisecond)

	// The ephemeral key should eventually be deleted when session expires
	// (this may not happen immediately)
}

func TestIntegration_Notifications(t *testing.T) {
	store := newIntegrationTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start notifications before making changes
	stream, err := store.Notifications(ctx)
	if err != nil {
		t.Fatalf("failed to get notifications: %v", err)
	}
	defer stream.Close()

	key := "/test/notifications/key"

	// Make a change in a goroutine
	go func() {
		time.Sleep(100 * time.Millisecond)
		store.Put(context.Background(), key, []byte("notification-test"))
	}()

	// Wait for notification
	notification, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("failed to receive notification: %v", err)
	}

	if notification.Key != key {
		t.Errorf("notification key mismatch: got %q, want %q", notification.Key, key)
	}
	if notification.Deleted {
		t.Error("expected created notification, got deleted")
	}
}
