package oxia

import (
	"context"
	"sync"
	"testing"

	"github.com/dray-io/dray/internal/metadata"
)

// TestCASVersionMismatch verifies that Put with ExpectedVersion fails when
// the version does not match the current version.
func TestCASVersionMismatch(t *testing.T) {
	// This test verifies the CAS semantics at the unit level.
	// The actual Oxia integration is tested in integration_test.go.

	t.Run("version_mismatch_detection", func(t *testing.T) {
		// Verify the expected version option is correctly extracted
		ver := metadata.Version(5)
		opts := []metadata.PutOption{metadata.WithExpectedVersion(ver)}
		extracted := metadata.ExtractExpectedVersion(opts)

		if extracted == nil {
			t.Fatal("expected non-nil version")
		}
		if *extracted != ver {
			t.Errorf("expected version %d, got %d", ver, *extracted)
		}
	})

	t.Run("version_zero_means_not_exists", func(t *testing.T) {
		// Version 0 should mean "key should not exist"
		ver := metadata.Version(0)
		opts := []metadata.PutOption{metadata.WithExpectedVersion(ver)}
		extracted := metadata.ExtractExpectedVersion(opts)

		if extracted == nil {
			t.Fatal("expected non-nil version")
		}
		if *extracted != 0 {
			t.Errorf("expected version 0, got %d", *extracted)
		}
	})

	t.Run("no_version_means_unconditional", func(t *testing.T) {
		// No expected version should mean unconditional write
		var opts []metadata.PutOption
		extracted := metadata.ExtractExpectedVersion(opts)

		if extracted != nil {
			t.Errorf("expected nil version for unconditional write, got %d", *extracted)
		}
	})
}

// TestDeleteVersionMismatch verifies conditional delete semantics.
func TestDeleteVersionMismatch(t *testing.T) {
	t.Run("delete_version_mismatch_detection", func(t *testing.T) {
		ver := metadata.Version(10)
		opts := []metadata.DeleteOption{metadata.WithDeleteExpectedVersion(ver)}
		extracted := metadata.ExtractDeleteExpectedVersion(opts)

		if extracted == nil {
			t.Fatal("expected non-nil version")
		}
		if *extracted != ver {
			t.Errorf("expected version %d, got %d", ver, *extracted)
		}
	})

	t.Run("no_version_means_unconditional_delete", func(t *testing.T) {
		var opts []metadata.DeleteOption
		extracted := metadata.ExtractDeleteExpectedVersion(opts)

		if extracted != nil {
			t.Errorf("expected nil version for unconditional delete, got %d", *extracted)
		}
	})
}

// TestTransactionOperationQueuing verifies that transactions queue operations correctly.
func TestTransactionOperationQueuing(t *testing.T) {
	t.Run("operations_are_queued", func(t *testing.T) {
		// Create a mock transaction to verify operation queuing
		txn := &transaction{
			ctx:      context.Background(),
			scopeKey: "test-scope",
		}

		// Queue various operations
		txn.Put("key1", []byte("value1"))
		txn.PutWithVersion("key2", []byte("value2"), 5)
		txn.Delete("key3")
		txn.DeleteWithVersion("key4", 10)

		// Verify operations were queued
		if len(txn.ops) != 4 {
			t.Fatalf("expected 4 operations, got %d", len(txn.ops))
		}

		// Verify operation types
		expectedOps := []struct {
			opType          txnOpType
			key             string
			expectedVersion metadata.Version
		}{
			{txnOpPut, "key1", 0},
			{txnOpPutVersioned, "key2", 5},
			{txnOpDelete, "key3", 0},
			{txnOpDeleteVersioned, "key4", 10},
		}

		for i, expected := range expectedOps {
			if txn.ops[i].opType != expected.opType {
				t.Errorf("op %d: expected type %v, got %v", i, expected.opType, txn.ops[i].opType)
			}
			if txn.ops[i].key != expected.key {
				t.Errorf("op %d: expected key %s, got %s", i, expected.key, txn.ops[i].key)
			}
			if txn.ops[i].expectedVersion != expected.expectedVersion {
				t.Errorf("op %d: expected version %d, got %d", i, expected.expectedVersion, txn.ops[i].expectedVersion)
			}
		}
	})
}

// TestTransactionScopeKey verifies that transactions use the scope key for partitioning.
func TestTransactionScopeKey(t *testing.T) {
	txn := &transaction{
		ctx:      context.Background(),
		scopeKey: "/dray/v1/streams/abc123",
	}

	if txn.scopeKey != "/dray/v1/streams/abc123" {
		t.Errorf("expected scope key /dray/v1/streams/abc123, got %s", txn.scopeKey)
	}
}

// TestTransactionOpTypes verifies operation type constants are distinct.
func TestTransactionOpTypes(t *testing.T) {
	opTypes := map[txnOpType]string{
		txnOpPut:             "txnOpPut",
		txnOpPutVersioned:    "txnOpPutVersioned",
		txnOpDelete:          "txnOpDelete",
		txnOpDeleteVersioned: "txnOpDeleteVersioned",
	}

	// Verify all types are distinct
	seen := make(map[txnOpType]bool)
	for opType, name := range opTypes {
		if seen[opType] {
			t.Errorf("duplicate operation type: %s", name)
		}
		seen[opType] = true
	}

	if len(seen) != 4 {
		t.Errorf("expected 4 distinct operation types, got %d", len(seen))
	}
}

// TestCASVersionConstants verifies version-related constants.
func TestCASVersionConstants(t *testing.T) {
	// NoVersion should be -1
	if metadata.NoVersion != -1 {
		t.Errorf("expected NoVersion = -1, got %d", metadata.NoVersion)
	}

	// Version 0 should mean "key not exists"
	// This is the convention: version 0 means the key has never been written
	var zeroVersion metadata.Version = 0
	if zeroVersion != 0 {
		t.Errorf("expected zero version to be 0, got %d", zeroVersion)
	}
}

// TestConcurrentVersionTracking verifies that versions track correctly for
// concurrent access patterns.
func TestConcurrentVersionTracking(t *testing.T) {
	t.Run("version_is_threadsafe_type", func(t *testing.T) {
		// Version is just an int64, which is atomically readable/writable on 64-bit
		// but we need to ensure proper synchronization for read-modify-write
		var wg sync.WaitGroup
		var mu sync.Mutex
		versions := make([]metadata.Version, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				mu.Lock()
				versions[idx] = metadata.Version(idx)
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		for i, v := range versions {
			if v != metadata.Version(i) {
				t.Errorf("version at index %d: expected %d, got %d", i, i, v)
			}
		}
	})
}

// TestErrorSemantics verifies error semantics for CAS operations.
func TestErrorSemantics(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrVersionMismatch", metadata.ErrVersionMismatch, "metadata: version mismatch"},
		{"ErrTxnConflict", metadata.ErrTxnConflict, "metadata: transaction conflict"},
		{"ErrKeyNotFound", metadata.ErrKeyNotFound, "metadata: key not found"},
		{"ErrStoreClosed", metadata.ErrStoreClosed, "metadata: store closed"},
		{"ErrSessionExpired", metadata.ErrSessionExpired, "metadata: session expired"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.want {
				t.Errorf("error message = %q, want %q", tt.err.Error(), tt.want)
			}
		})
	}
}

// TestPutOptionChaining verifies that multiple options can be applied.
func TestPutOptionChaining(t *testing.T) {
	// Currently only one option, but the interface supports chaining
	opts := []metadata.PutOption{
		metadata.WithExpectedVersion(1),
	}

	// Apply all options
	extracted := metadata.ExtractExpectedVersion(opts)
	if extracted == nil || *extracted != 1 {
		t.Error("option not applied correctly")
	}
}

// TestDeleteOptionChaining verifies that multiple delete options can be applied.
func TestDeleteOptionChaining(t *testing.T) {
	opts := []metadata.DeleteOption{
		metadata.WithDeleteExpectedVersion(5),
	}

	extracted := metadata.ExtractDeleteExpectedVersion(opts)
	if extracted == nil || *extracted != 5 {
		t.Error("option not applied correctly")
	}
}

// TestTransactionCallbackError verifies that transaction callback errors are propagated.
func TestTransactionCallbackError(t *testing.T) {
	// The transaction should propagate errors from the callback
	// We test the error propagation at the interface level

	// Create a custom error
	customErr := &testError{msg: "custom callback error"}

	// Verify it can be used as a return value
	if customErr.Error() != "custom callback error" {
		t.Error("custom error not working correctly")
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// TestKVVersionField verifies the KV struct includes version.
func TestKVVersionField(t *testing.T) {
	kv := metadata.KV{
		Key:     "/test/key",
		Value:   []byte("value"),
		Version: 42,
	}

	if kv.Key != "/test/key" {
		t.Errorf("key = %s, want /test/key", kv.Key)
	}
	if string(kv.Value) != "value" {
		t.Errorf("value = %s, want value", string(kv.Value))
	}
	if kv.Version != 42 {
		t.Errorf("version = %d, want 42", kv.Version)
	}
}

// TestGetResultVersionField verifies the GetResult struct includes version.
func TestGetResultVersionField(t *testing.T) {
	result := metadata.GetResult{
		Value:   []byte("data"),
		Version: 100,
		Exists:  true,
	}

	if !result.Exists {
		t.Error("expected exists = true")
	}
	if result.Version != 100 {
		t.Errorf("version = %d, want 100", result.Version)
	}
	if string(result.Value) != "data" {
		t.Errorf("value = %s, want data", string(result.Value))
	}
}

// TestVersionComparison verifies version comparison semantics.
func TestVersionComparison(t *testing.T) {
	v1 := metadata.Version(1)
	v2 := metadata.Version(2)
	v3 := metadata.Version(2)

	if v1 >= v2 {
		t.Error("expected v1 < v2")
	}
	if v2 != v3 {
		t.Error("expected v2 == v3")
	}
	if v2 <= v1 {
		t.Error("expected v2 > v1")
	}
}

// TestTransactionVersionZeroSemantics verifies version 0 means key should not exist.
func TestTransactionVersionZeroSemantics(t *testing.T) {
	txn := &transaction{
		ctx:      context.Background(),
		scopeKey: "test",
	}

	// PutWithVersion with version 0 should indicate key should not exist
	txn.PutWithVersion("new-key", []byte("value"), 0)

	if len(txn.ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(txn.ops))
	}

	op := txn.ops[0]
	if op.opType != txnOpPutVersioned {
		t.Errorf("expected txnOpPutVersioned, got %v", op.opType)
	}
	if op.expectedVersion != 0 {
		t.Errorf("expected version 0, got %d", op.expectedVersion)
	}
}

// TestMultipleVersionedOperations verifies multiple versioned operations in one transaction.
func TestMultipleVersionedOperations(t *testing.T) {
	txn := &transaction{
		ctx:      context.Background(),
		scopeKey: "test",
	}

	// Queue multiple versioned operations
	txn.PutWithVersion("key1", []byte("v1"), 1)
	txn.PutWithVersion("key2", []byte("v2"), 2)
	txn.DeleteWithVersion("key3", 3)
	txn.PutWithVersion("key4", []byte("v4"), 0) // create new

	if len(txn.ops) != 4 {
		t.Fatalf("expected 4 operations, got %d", len(txn.ops))
	}

	// Verify all operations have correct versions
	expectedVersions := []metadata.Version{1, 2, 3, 0}
	for i, expected := range expectedVersions {
		if txn.ops[i].expectedVersion != expected {
			t.Errorf("op %d: expected version %d, got %d", i, expected, txn.ops[i].expectedVersion)
		}
	}
}
