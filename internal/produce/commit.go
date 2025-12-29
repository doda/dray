package produce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/objectstore"
	"github.com/dray-io/dray/internal/wal"
)

// CommitterConfig configures the produce committer.
type CommitterConfig struct {
	// NumDomains is the number of MetaDomains for partitioning.
	NumDomains int
}

// Committer handles the atomic commit of WAL objects to metadata.
// It writes WAL objects to object storage and commits metadata atomically
// per spec section 9.2-9.4.
type Committer struct {
	objStore   objectstore.Store
	metaStore  metadata.MetadataStore
	calculator *metadata.DomainCalculator
}

// NewCommitter creates a new produce committer.
func NewCommitter(objStore objectstore.Store, metaStore metadata.MetadataStore, cfg CommitterConfig) *Committer {
	return &Committer{
		objStore:   objStore,
		metaStore:  metaStore,
		calculator: metadata.NewDomainCalculator(cfg.NumDomains),
	}
}

// CommitResult contains the result of a WAL commit.
type CommitResult struct {
	// StreamResults maps streamID to offset assignment.
	StreamResults map[uint64]*StreamResult
}

// StreamResult contains offset assignment for a single stream.
type StreamResult struct {
	// StartOffset is the first assigned offset (inclusive).
	StartOffset int64
	// EndOffset is the exclusive end offset (startOffset + recordCount).
	EndOffset int64
}

// WALObjectRecord is stored at /dray/v1/wal/objects/<metaDomain>/<walId>
// per spec section 9.7.
type WALObjectRecord struct {
	Path      string `json:"path"`
	RefCount  int32  `json:"refCount"`
	CreatedAt int64  `json:"createdAt"`
	SizeBytes int64  `json:"sizeBytes"`
}

// Commit commits a batch of pending requests for a single MetaDomain.
// It performs the following steps per spec section 9.3:
//  1. Create a WAL writer with staging marker support
//  2. Build chunks from pending requests
//  3. Flush WAL to object storage (with staging marker)
//  4. Execute atomic metadata commit transaction
//  5. Populate results with assigned offsets
func (c *Committer) Commit(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
	if len(requests) == 0 {
		return nil
	}

	// Create staging writer
	writer := wal.NewStagingWriter(c.objStore, c.metaStore, nil)
	defer writer.Close()

	// Build chunks from pending requests
	chunks := make(map[uint64]*pendingChunk)
	for _, req := range requests {
		pc, ok := chunks[req.StreamID]
		if !ok {
			pc = &pendingChunk{
				streamID:       req.StreamID,
				streamIDStr:    req.StreamIDStr,
				batches:        make([]wal.BatchEntry, 0),
				recordCount:    0,
				minTimestampMs: req.MinTimestampMs,
				maxTimestampMs: req.MaxTimestampMs,
				requests:       make([]*PendingRequest, 0),
			}
			chunks[req.StreamID] = pc
		}
		pc.batches = append(pc.batches, req.Batches...)
		pc.recordCount += req.RecordCount
		if req.MinTimestampMs < pc.minTimestampMs {
			pc.minTimestampMs = req.MinTimestampMs
		}
		if req.MaxTimestampMs > pc.maxTimestampMs {
			pc.maxTimestampMs = req.MaxTimestampMs
		}
		pc.requests = append(pc.requests, req)
	}

	// Add chunks to writer
	for streamID, pc := range chunks {
		chunk := wal.Chunk{
			StreamID:       streamID,
			Batches:        pc.batches,
			RecordCount:    pc.recordCount,
			MinTimestampMs: pc.minTimestampMs,
			MaxTimestampMs: pc.maxTimestampMs,
		}
		if err := writer.AddChunk(chunk, uint32(domain)); err != nil {
			return fmt.Errorf("produce: add chunk: %w", err)
		}
	}

	// Flush WAL to object storage
	writeResult, err := writer.Flush(ctx)
	if err != nil {
		return fmt.Errorf("produce: flush WAL: %w", err)
	}

	// Execute atomic metadata commit
	result, err := c.commitMetadata(ctx, domain, writeResult, chunks)
	if err != nil {
		return fmt.Errorf("produce: commit metadata: %w", err)
	}

	// Populate results in pending requests
	for streamID, sr := range result.StreamResults {
		if pc, ok := chunks[streamID]; ok {
			offsetPos := sr.StartOffset
			for _, req := range pc.requests {
				reqResult := &RequestResult{
					StartOffset: offsetPos,
					EndOffset:   offsetPos + int64(req.RecordCount),
				}
				req.Result = reqResult
				offsetPos = reqResult.EndOffset
			}
		}
	}

	return nil
}

// pendingChunk aggregates batches for a single stream.
type pendingChunk struct {
	streamID       uint64
	streamIDStr    string
	batches        []wal.BatchEntry
	recordCount    uint32
	minTimestampMs int64
	maxTimestampMs int64
	requests       []*PendingRequest
}

// commitMetadata executes the atomic metadata commit transaction per spec 9.2.
// This transaction:
//  1. Allocates offsets for each stream chunk (by reading/advancing hwm)
//  2. Writes corresponding index entries
//  3. Creates WAL object record
//  4. Deletes staging marker
func (c *Committer) commitMetadata(ctx context.Context, domain metadata.MetaDomain, writeResult *wal.StagingWriteResult, chunks map[uint64]*pendingChunk) (*CommitResult, error) {
	result := &CommitResult{
		StreamResults: make(map[uint64]*StreamResult),
	}

	scopeKey := keys.WALObjectsDomainPrefix(int(domain))
	err := c.metaStore.Txn(ctx, scopeKey, func(txn metadata.Txn) error {
		for _, chunkOffset := range writeResult.ChunkOffsets {
			streamID := chunkOffset.StreamID

			pc := chunks[streamID]
			if pc == nil {
				continue
			}

			streamIDStr := pc.streamIDStr

			var chunkSize int64
			for _, batch := range pc.batches {
				chunkSize += int64(len(batch.Data))
			}

			hwmKey := keys.HwmKeyPath(streamIDStr)

			hwmValue, hwmVersion, err := txn.Get(hwmKey)
			if err != nil {
				if errors.Is(err, metadata.ErrKeyNotFound) {
					return index.ErrStreamNotFound
				}
				return err
			}

			currentHWM, err := index.DecodeHWM(hwmValue)
			if err != nil {
				return err
			}

			startOffset := currentHWM
			endOffset := currentHWM + int64(chunkOffset.RecordCount)

			prevCumulativeSize := int64(0)
			startKey, err := keys.OffsetIndexStartKey(streamIDStr, currentHWM)
			if err != nil {
				return err
			}
			entries, err := c.metaStore.List(ctx, startKey, "", 1)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				var lastIndexEntry index.IndexEntry
				if err := json.Unmarshal(entries[0].Value, &lastIndexEntry); err != nil {
					return err
				}
				prevCumulativeSize = lastIndexEntry.CumulativeSize
			}

			cumulativeSize := prevCumulativeSize + chunkSize

			indexKey, err := keys.OffsetIndexKeyPath(streamIDStr, endOffset, cumulativeSize)
			if err != nil {
				return err
			}

			entry := index.IndexEntry{
				StreamID:       streamIDStr,
				StartOffset:    startOffset,
				EndOffset:      endOffset,
				CumulativeSize: cumulativeSize,
				CreatedAtMs:    writeResult.CreatedAtUnixMs,
				FileType:       index.FileTypeWAL,
				RecordCount:    chunkOffset.RecordCount,
				MessageCount:   chunkOffset.RecordCount,
				MinTimestampMs: chunkOffset.MinTimestampMs,
				MaxTimestampMs: chunkOffset.MaxTimestampMs,
				WalID:          writeResult.WalID.String(),
				WalPath:        writeResult.Path,
				ChunkOffset:    chunkOffset.ByteOffset,
				ChunkLength:    chunkOffset.ByteLength,
				BatchIndex:     nil,
			}

			batchIndex, err := buildBatchIndex(pc.batches)
			if err != nil {
				return err
			}
			entry.BatchIndex = batchIndex

			entryBytes, err := json.Marshal(entry)
			if err != nil {
				return err
			}

			txn.PutWithVersion(hwmKey, index.EncodeHWM(endOffset), hwmVersion)
			txn.Put(indexKey, entryBytes)

			result.StreamResults[streamID] = &StreamResult{
				StartOffset: startOffset,
				EndOffset:   endOffset,
			}
		}

		walObjectKey := keys.WALObjectKeyPath(int(domain), writeResult.WalID.String())
		walRecord := WALObjectRecord{
			Path:      writeResult.Path,
			RefCount:  int32(len(chunks)),
			CreatedAt: writeResult.CreatedAtUnixMs,
			SizeBytes: writeResult.Size,
		}
		walRecordBytes, err := json.Marshal(walRecord)
		if err != nil {
			return fmt.Errorf("marshal WAL record: %w", err)
		}

		txn.Put(walObjectKey, walRecordBytes)
		txn.Delete(writeResult.StagingKey)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// CreateFlushHandler creates a FlushHandler that uses the committer.
func (c *Committer) CreateFlushHandler() FlushHandler {
	return func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
		return c.Commit(ctx, domain, requests)
	}
}

// StreamCommitRequest contains parameters for committing a single stream's records.
type StreamCommitRequest struct {
	StreamID       string
	RecordCount    uint32
	ChunkSizeBytes int64
	CreatedAtMs    int64
	MinTimestampMs int64
	MaxTimestampMs int64
	WalID          string
	WalPath        string
	BatchIndex     []index.BatchIndexEntry
}

// CommitStream commits a single stream's records without a full WAL write.
// This is useful for testing or when the WAL has already been written.
func (c *Committer) CommitStream(ctx context.Context, req StreamCommitRequest) (*StreamResult, error) {
	hwmKey := keys.HwmKeyPath(req.StreamID)
	var sr StreamResult

	err := c.metaStore.Txn(ctx, hwmKey, func(txn metadata.Txn) error {
		hwmValue, hwmVersion, err := txn.Get(hwmKey)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				return index.ErrStreamNotFound
			}
			return err
		}

		currentHWM, err := index.DecodeHWM(hwmValue)
		if err != nil {
			return err
		}

		startOffset := currentHWM
		endOffset := currentHWM + int64(req.RecordCount)

		// Calculate cumulative size
		prevCumulativeSize := int64(0)
		prefix := keys.OffsetIndexPrefix(req.StreamID)
		entries, err := c.metaStore.List(ctx, prefix, "", 0)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			lastEntry := entries[len(entries)-1]
			var lastIndexEntry index.IndexEntry
			if err := json.Unmarshal(lastEntry.Value, &lastIndexEntry); err != nil {
				return err
			}
			prevCumulativeSize = lastIndexEntry.CumulativeSize
		}

		cumulativeSize := prevCumulativeSize + req.ChunkSizeBytes

		indexKey, err := keys.OffsetIndexKeyPath(req.StreamID, endOffset, cumulativeSize)
		if err != nil {
			return err
		}

		createdAt := req.CreatedAtMs
		if createdAt == 0 {
			createdAt = time.Now().UnixMilli()
		}

		entry := index.IndexEntry{
			StreamID:       req.StreamID,
			StartOffset:    startOffset,
			EndOffset:      endOffset,
			CumulativeSize: cumulativeSize,
			CreatedAtMs:    createdAt,
			FileType:       index.FileTypeWAL,
			RecordCount:    req.RecordCount,
			MessageCount:   req.RecordCount,
			MinTimestampMs: req.MinTimestampMs,
			MaxTimestampMs: req.MaxTimestampMs,
			WalID:          req.WalID,
			WalPath:        req.WalPath,
			BatchIndex:     req.BatchIndex,
		}

		entryBytes, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		txn.PutWithVersion(hwmKey, index.EncodeHWM(endOffset), hwmVersion)
		txn.Put(indexKey, entryBytes)

		sr = StreamResult{
			StartOffset: startOffset,
			EndOffset:   endOffset,
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &sr, nil
}
