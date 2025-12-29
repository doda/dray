package fetch

import (
	"context"
	"fmt"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// TimestampScanner scans WAL or Parquet entries for timestamp-based offset lookup.
type TimestampScanner struct {
	wal     *WALReader
	parquet *ParquetReader
}

// NewTimestampScanner creates a timestamp scanner backed by the object store.
func NewTimestampScanner(store objectstore.Store) *TimestampScanner {
	return &TimestampScanner{
		wal:     NewWALReader(store),
		parquet: NewParquetReader(store),
	}
}

// ScanOffsetByTimestamp finds the first offset with timestamp >= the requested value.
func (s *TimestampScanner) ScanOffsetByTimestamp(ctx context.Context, entry *index.IndexEntry, timestamp int64) (int64, int64, bool, error) {
	switch entry.FileType {
	case index.FileTypeWAL:
		return s.wal.FindOffsetByTimestamp(ctx, entry, timestamp)
	case index.FileTypeParquet:
		return s.parquet.FindOffsetByTimestamp(ctx, entry, timestamp)
	default:
		return -1, -1, false, fmt.Errorf("fetch: unsupported file type %s", entry.FileType)
	}
}
