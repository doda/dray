package produce

import (
	"encoding/binary"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/wal"
)

const (
	minBatchSize         = 61
	firstTimestampOffset = 27
	maxTimestampOffset   = 35
	recordCountOffset    = 57
	recordCountOffsetEnd = 61
)

func buildBatchIndex(batches []wal.BatchEntry) ([]index.BatchIndexEntry, error) {
	if len(batches) == 0 {
		return nil, nil
	}

	indexEntries := make([]index.BatchIndexEntry, 0, len(batches))
	var offsetDelta uint32
	var chunkOffset uint32

	for _, batch := range batches {
		data := batch.Data
		if len(data) < minBatchSize {
			return nil, nil
		}

		recordCount := binary.BigEndian.Uint32(data[recordCountOffset:recordCountOffsetEnd])
		if recordCount == 0 {
			return nil, nil
		}

		batchStartDelta := offsetDelta
		batchLastDelta := offsetDelta + recordCount - 1

		batchLength := uint32(len(data))
		batchOffset := chunkOffset + 4

		minTimestamp := int64(binary.BigEndian.Uint64(data[firstTimestampOffset : firstTimestampOffset+8]))
		maxTimestamp := int64(binary.BigEndian.Uint64(data[maxTimestampOffset : maxTimestampOffset+8]))

		indexEntries = append(indexEntries, index.BatchIndexEntry{
			BatchStartOffsetDelta: batchStartDelta,
			BatchLastOffsetDelta:  batchLastDelta,
			BatchOffsetInChunk:    batchOffset,
			BatchLength:           batchLength,
			MinTimestampMs:        minTimestamp,
			MaxTimestampMs:        maxTimestamp,
		})

		offsetDelta += recordCount
		chunkOffset += 4 + batchLength
	}

	return indexEntries, nil
}
