package oxia

import (
	"context"
	"errors"
	"fmt"

	"github.com/oxia-db/oxia/common/proto"
	oxiaclient "github.com/oxia-db/oxia/oxia"

	"github.com/dray-io/dray/internal/metadata"
)

// transaction implements metadata.Txn for Oxia.
type transaction struct {
	store    *Store
	ctx      context.Context
	scopeKey string

	// Operations queued for commit
	ops []txnOp

	reads map[string]txnRead
}

type txnOpType int

const (
	txnOpPut txnOpType = iota
	txnOpPutVersioned
	txnOpDelete
	txnOpDeleteVersioned
)

type txnRead struct {
	value   []byte
	version metadata.Version
	exists  bool
}

type txnOp struct {
	opType          txnOpType
	key             string
	value           []byte
	expectedVersion metadata.Version
}

type commitOp struct {
	opType        txnOpType
	key           string
	value         []byte
	preState      txnRead
	responseIndex int
}

// Get retrieves a value within the transaction.
func (t *transaction) Get(key string) ([]byte, metadata.Version, error) {
	_, value, version, err := t.store.client.Get(t.ctx, key, oxiaclient.PartitionKey(t.scopeKey))
	if err != nil {
		if errors.Is(err, oxiaclient.ErrKeyNotFound) {
			t.reads[key] = txnRead{exists: false}
			return nil, 0, metadata.ErrKeyNotFound
		}
		return nil, 0, err
	}

	metaVersion := oxiaToMetadataVersion(version.VersionId)
	t.reads[key] = txnRead{
		value:   value,
		version: metaVersion,
		exists:  true,
	}
	return value, metaVersion, nil
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
func (t *transaction) commit() error {
	if len(t.ops) == 0 {
		return nil
	}

	shardID, err := t.store.txnCoordinator.shardForKey(t.scopeKey)
	if err != nil {
		return fmt.Errorf("oxia: transaction shard lookup failed: %w", err)
	}

	partitionKey := t.scopeKey
	puts := make([]*proto.PutRequest, 0, len(t.ops))
	deletes := make([]*proto.DeleteRequest, 0, len(t.ops))
	putOps := make([]commitOp, 0, len(t.ops))
	deleteOps := make([]commitOp, 0, len(t.ops))

	for _, op := range t.ops {
		state, err := t.readState(op.key)
		if err != nil {
			return err
		}

		switch op.opType {
		case txnOpPut:
			expectedVersionId := expectedVersionIdForPut(state)
			req := &proto.PutRequest{
				Key:               op.key,
				Value:             op.value,
				ExpectedVersionId: &expectedVersionId,
				PartitionKey:      &partitionKey,
			}
			puts = append(puts, req)
			putOps = append(putOps, commitOp{
				opType:        op.opType,
				key:           op.key,
				value:         op.value,
				preState:      state,
				responseIndex: len(puts) - 1,
			})

		case txnOpPutVersioned:
			expectedVersionId := expectedVersionIdForVersionedPut(op)
			req := &proto.PutRequest{
				Key:               op.key,
				Value:             op.value,
				ExpectedVersionId: &expectedVersionId,
				PartitionKey:      &partitionKey,
			}
			puts = append(puts, req)
			putOps = append(putOps, commitOp{
				opType:        op.opType,
				key:           op.key,
				value:         op.value,
				preState:      state,
				responseIndex: len(puts) - 1,
			})

		case txnOpDelete:
			if !state.exists {
				continue
			}
			expectedVersionId := metadataToOxiaVersion(state.version)
			req := &proto.DeleteRequest{
				Key:               op.key,
				ExpectedVersionId: &expectedVersionId,
			}
			deletes = append(deletes, req)
			deleteOps = append(deleteOps, commitOp{
				opType:        op.opType,
				key:           op.key,
				preState:      state,
				responseIndex: len(deletes) - 1,
			})

		case txnOpDeleteVersioned:
			if !state.exists {
				continue
			}
			expectedVersionId := expectedVersionIdForVersionedPut(op)
			req := &proto.DeleteRequest{
				Key:               op.key,
				ExpectedVersionId: &expectedVersionId,
			}
			deletes = append(deletes, req)
			deleteOps = append(deleteOps, commitOp{
				opType:        op.opType,
				key:           op.key,
				preState:      state,
				responseIndex: len(deletes) - 1,
			})
		}
	}

	if len(puts) == 0 && len(deletes) == 0 {
		return nil
	}

	response, err := t.store.txnCoordinator.write(t.ctx, shardID, &proto.WriteRequest{
		Puts:    puts,
		Deletes: deletes,
	})
	if err != nil {
		return fmt.Errorf("oxia: transaction commit failed: %w", err)
	}

	if len(response.Puts) != len(puts) || len(response.Deletes) != len(deletes) {
		return errors.New("oxia: transaction commit response mismatch")
	}

	conflict, rollbackReq, err := buildRollbackRequest(response, putOps, deleteOps, partitionKey)
	if err != nil {
		return err
	}
	if !conflict {
		return nil
	}

	if rollbackReq != nil {
		if _, rollbackErr := t.store.txnCoordinator.write(t.ctx, shardID, rollbackReq); rollbackErr != nil {
			return fmt.Errorf("%w: rollback failed: %v", metadata.ErrTxnConflict, rollbackErr)
		}
	}

	return metadata.ErrTxnConflict
}

func (t *transaction) readState(key string) (txnRead, error) {
	if read, ok := t.reads[key]; ok {
		return read, nil
	}

	_, value, version, err := t.store.client.Get(t.ctx, key, oxiaclient.PartitionKey(t.scopeKey))
	if err != nil {
		if errors.Is(err, oxiaclient.ErrKeyNotFound) {
			read := txnRead{exists: false}
			t.reads[key] = read
			return read, nil
		}
		return txnRead{}, err
	}

	read := txnRead{
		value:   value,
		version: oxiaToMetadataVersion(version.VersionId),
		exists:  true,
	}
	t.reads[key] = read
	return read, nil
}

func expectedVersionIdForPut(state txnRead) int64 {
	if state.exists {
		return metadataToOxiaVersion(state.version)
	}
	return oxiaclient.VersionIdNotExists
}

func expectedVersionIdForVersionedPut(op txnOp) int64 {
	if op.expectedVersion == 0 {
		return oxiaclient.VersionIdNotExists
	}
	return metadataToOxiaVersion(op.expectedVersion)
}

func buildRollbackRequest(response *proto.WriteResponse, putOps, deleteOps []commitOp, partitionKey string) (bool, *proto.WriteRequest, error) {
	conflict := false
	putStatuses := make([]proto.Status, len(putOps))
	deleteStatuses := make([]proto.Status, len(deleteOps))

	for i, op := range putOps {
		putResp := response.Puts[op.responseIndex]
		putStatuses[i] = putResp.Status
		if putResp.Status != proto.Status_OK {
			conflict = true
		}
	}

	for i, op := range deleteOps {
		deleteResp := response.Deletes[op.responseIndex]
		deleteStatuses[i] = deleteResp.Status
		if deleteResp.Status != proto.Status_OK {
			conflict = true
		}
	}

	if !conflict {
		return false, nil, nil
	}

	rollbackPuts := make([]*proto.PutRequest, 0, len(putOps)+len(deleteOps))
	rollbackDeletes := make([]*proto.DeleteRequest, 0, len(putOps))

	for i, op := range putOps {
		if putStatuses[i] != proto.Status_OK {
			continue
		}
		putResp := response.Puts[op.responseIndex]
		if putResp.Version == nil {
			return true, nil, errors.New("oxia: transaction commit returned empty version")
		}
		if op.preState.exists {
			expectedVersionId := putResp.Version.VersionId
			rollbackPuts = append(rollbackPuts, &proto.PutRequest{
				Key:               op.key,
				Value:             op.preState.value,
				ExpectedVersionId: &expectedVersionId,
				PartitionKey:      &partitionKey,
			})
		} else {
			expectedVersionId := putResp.Version.VersionId
			rollbackDeletes = append(rollbackDeletes, &proto.DeleteRequest{
				Key:               op.key,
				ExpectedVersionId: &expectedVersionId,
			})
		}
	}

	for i, op := range deleteOps {
		if deleteStatuses[i] != proto.Status_OK {
			continue
		}
		if !op.preState.exists {
			continue
		}
		expectedVersionId := oxiaclient.VersionIdNotExists
		rollbackPuts = append(rollbackPuts, &proto.PutRequest{
			Key:               op.key,
			Value:             op.preState.value,
			ExpectedVersionId: &expectedVersionId,
			PartitionKey:      &partitionKey,
		})
	}

	if len(rollbackPuts) == 0 && len(rollbackDeletes) == 0 {
		return true, nil, nil
	}

	return true, &proto.WriteRequest{
		Puts:    rollbackPuts,
		Deletes: rollbackDeletes,
	}, nil
}
