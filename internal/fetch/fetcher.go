package fetch

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// Source constants for metrics tracking.
const (
	SourceWAL     = "wal"
	SourceParquet = "parquet"
	SourceNone    = "none"

	// DefaultParquetFetchMaxBytes caps Parquet fetch size to keep long-range reads responsive.
	DefaultParquetFetchMaxBytes int64 = 1 * 1024 * 1024
)

// Fetcher handles fetching record batches from WAL or Parquet storage.
type Fetcher struct {
	walReader     *WALReader
	parquetReader *ParquetReader
	streamManager *index.StreamManager
	rangeCache    *ObjectRangeCache
}

// NewFetcher creates a new Fetcher.
func NewFetcher(store objectstore.Store, streamManager *index.StreamManager) *Fetcher {
	if store != nil && streamManager != nil {
		streamManager.SetTimestampScanner(NewTimestampScanner(store))
	}
	return &Fetcher{
		walReader:     NewWALReader(store),
		parquetReader: NewParquetReader(store),
		streamManager: streamManager,
	}
}

// NewFetcherWithCache creates a new Fetcher with an optional range cache for WAL reads.
// If cache is non-nil, WAL chunk reads will be cached for faster subsequent access.
func NewFetcherWithCache(store objectstore.Store, streamManager *index.StreamManager, cache *ObjectRangeCache) *Fetcher {
	if store != nil && streamManager != nil {
		streamManager.SetTimestampScanner(NewTimestampScanner(store))
	}
	return &Fetcher{
		walReader:     NewWALReaderWithCache(store, cache),
		parquetReader: NewParquetReader(store),
		streamManager: streamManager,
		rangeCache:    cache,
	}
}

// RangeCache returns the configured range cache, or nil if not configured.
func (f *Fetcher) RangeCache() *ObjectRangeCache {
	return f.rangeCache
}

// FetchRequest contains the parameters for a fetch operation.
type FetchRequest struct {
	// StreamID is the stream to fetch from.
	StreamID string
	// FetchOffset is the offset to start fetching from.
	FetchOffset int64
	// MaxBytes is the maximum bytes to return (0 = use default).
	MaxBytes int64
}

// FetchResponse contains the result of a fetch operation.
type FetchResponse struct {
	// Batches contains the record batch bytes with patched offsets.
	Batches [][]byte
	// TotalBytes is the total size of all batches.
	TotalBytes int64
	// HighWatermark is the current HWM for the stream.
	HighWatermark int64
	// StartOffset is the first offset in the returned batches.
	StartOffset int64
	// EndOffset is one past the last offset (exclusive).
	EndOffset int64
	// OffsetBeyondHWM is true if fetchOffset >= hwm.
	OffsetBeyondHWM bool
	// Source indicates where the data came from (wal, parquet, or none).
	Source string
}

// Fetch retrieves record batches for the given request.
// It:
//  1. Looks up the index entry for the requested offset
//  2. Reads data from WAL (or Parquet when supported)
//  3. Patches batch offsets to reflect assigned offsets
//  4. Returns batches up to maxBytes
func (f *Fetcher) Fetch(ctx context.Context, req *FetchRequest) (*FetchResponse, error) {
	// Look up the index entry for this offset
	lookupResult, err := f.streamManager.LookupOffset(ctx, req.StreamID, req.FetchOffset)
	if err != nil {
		if errors.Is(err, index.ErrStreamNotFound) {
			return nil, err
		}
		return nil, err
	}

	// If offset is beyond HWM, return empty response
	if lookupResult.OffsetBeyondHWM {
		return &FetchResponse{
			HighWatermark:   lookupResult.HWM,
			OffsetBeyondHWM: true,
			Source:          SourceNone,
		}, nil
	}

	// If no entry found (but not beyond HWM), there's a gap
	if !lookupResult.Found {
		return &FetchResponse{
			HighWatermark: lookupResult.HWM,
			Source:        SourceNone,
		}, nil
	}

	entry := lookupResult.Entry

	// Read batches from storage based on file type
	var fetchResult *FetchResult
	var source string
	switch entry.FileType {
	case index.FileTypeWAL:
		fetchResult, err = f.walReader.ReadBatches(ctx, entry, req.FetchOffset, req.MaxBytes)
		if err != nil {
			return nil, err
		}
		source = SourceWAL

	case index.FileTypeParquet:
		maxBytes := req.MaxBytes
		if maxBytes <= 0 || maxBytes > DefaultParquetFetchMaxBytes {
			maxBytes = DefaultParquetFetchMaxBytes
		}
		fetchResult, err = f.parquetReader.ReadBatches(ctx, entry, req.FetchOffset, maxBytes)
		if err != nil {
			return nil, err
		}
		source = SourceParquet

	default:
		return nil, errors.New("fetch: unknown file type")
	}

	// Patch batch offsets and validate integrity per spec 9.5
	// The batches from WAL have baseOffset=0, we need to set them to the assigned offsets
	// PatchBatchesWithValidation validates CRC, compression, and offset deltas
	_, err = PatchBatchesWithValidation(fetchResult.Batches, fetchResult.StartOffset)
	if err != nil {
		return nil, err
	}

	return &FetchResponse{
		Batches:       fetchResult.Batches,
		TotalBytes:    fetchResult.TotalBytes,
		HighWatermark: lookupResult.HWM,
		StartOffset:   fetchResult.StartOffset,
		EndOffset:     fetchResult.EndOffset,
		Source:        source,
	}, nil
}
