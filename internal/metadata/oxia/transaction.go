package oxia

import (
	"context"
	"errors"
	"fmt"

	oxiaclient "github.com/oxia-db/oxia/oxia"

	"github.com/dray-io/dray/internal/metadata"
)

// transaction implements metadata.Txn for Oxia.
// Note: Oxia provides per-key atomicity but not multi-key transactions.
// This implementation executes operations sequentially, checking versions
// to detect conflicts. For true atomicity, callers should ensure all keys
// are in the same shard (using PartitionKey).
type transaction struct {
	store    *Store
	ctx      context.Context
	scopeKey string

	// Operations queued for commit
	ops []txnOp
}

type txnOpType int

const (
	txnOpPut txnOpType = iota
	txnOpPutVersioned
	txnOpDelete
	txnOpDeleteVersioned
)

type txnOp struct {
	opType          txnOpType
	key             string
	value           []byte
	expectedVersion metadata.Version
}

// Get retrieves a value within the transaction.
func (t *transaction) Get(key string) ([]byte, metadata.Version, error) {
	_, value, version, err := t.store.client.Get(t.ctx, key, oxiaclient.PartitionKey(t.scopeKey))
	if err != nil {
		if errors.Is(err, oxiaclient.ErrKeyNotFound) {
			return nil, 0, metadata.ErrKeyNotFound
		}
		return nil, 0, err
	}

	return value, metadata.Version(version.VersionId), nil
}

// Put queues a write operation within the transaction.
func (t *transaction) Put(key string, value []byte) {
	t.ops = append(t.ops, txnOp{
		opType: txnOpPut,
		key:    key,
		value:  value,
	})
}

// PutWithVersion queues a conditional write within the transaction.
func (t *transaction) PutWithVersion(key string, value []byte, expectedVersion metadata.Version) {
	t.ops = append(t.ops, txnOp{
		opType:          txnOpPutVersioned,
		key:             key,
		value:           value,
		expectedVersion: expectedVersion,
	})
}

// Delete queues a delete operation within the transaction.
func (t *transaction) Delete(key string) {
	t.ops = append(t.ops, txnOp{
		opType: txnOpDelete,
		key:    key,
	})
}

// DeleteWithVersion queues a conditional delete within the transaction.
func (t *transaction) DeleteWithVersion(key string, expectedVersion metadata.Version) {
	t.ops = append(t.ops, txnOp{
		opType:          txnOpDeleteVersioned,
		key:             key,
		expectedVersion: expectedVersion,
	})
}

// commit executes all queued operations.
// Operations are executed sequentially. If any operation fails due to a version
// mismatch, the transaction returns ErrTxnConflict. Note that earlier operations
// that succeeded are NOT rolled back (Oxia doesn't support rollback).
func (t *transaction) commit() error {
	for _, op := range t.ops {
		var err error

		switch op.opType {
		case txnOpPut:
			_, _, err = t.store.client.Put(t.ctx, op.key, op.value, oxiaclient.PartitionKey(t.scopeKey))

		case txnOpPutVersioned:
			opts := []oxiaclient.PutOption{oxiaclient.PartitionKey(t.scopeKey)}
			if op.expectedVersion == 0 {
				opts = append(opts, oxiaclient.ExpectedRecordNotExists())
			} else {
				opts = append(opts, oxiaclient.ExpectedVersionId(int64(op.expectedVersion)))
			}
			_, _, err = t.store.client.Put(t.ctx, op.key, op.value, opts...)

		case txnOpDelete:
			err = t.store.client.Delete(t.ctx, op.key, oxiaclient.PartitionKey(t.scopeKey))

		case txnOpDeleteVersioned:
			err = t.store.client.Delete(t.ctx, op.key,
				oxiaclient.PartitionKey(t.scopeKey),
				oxiaclient.ExpectedVersionId(int64(op.expectedVersion)))
		}

		if err != nil {
			if errors.Is(err, oxiaclient.ErrUnexpectedVersionId) {
				return metadata.ErrTxnConflict
			}
			if errors.Is(err, oxiaclient.ErrKeyNotFound) && (op.opType == txnOpDelete || op.opType == txnOpDeleteVersioned) {
				// Delete of non-existent key is OK
				continue
			}
			return fmt.Errorf("oxia: transaction operation failed: %w", err)
		}
	}

	return nil
}
