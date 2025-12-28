package fetch

import (
	"context"
	"errors"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/objectstore"
)

// Fetcher handles fetching record batches from WAL or Parquet storage.
type Fetcher struct {
	walReader     *WALReader
	parquetReader *ParquetReader
	streamManager *index.StreamManager
}

// NewFetcher creates a new Fetcher.
func NewFetcher(store objectstore.Store, streamManager *index.StreamManager) *Fetcher {
	return &Fetcher{
		walReader:     NewWALReader(store),
		parquetReader: NewParquetReader(store),
		streamManager: streamManager,
	}
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
		}, nil
	}

	// If no entry found (but not beyond HWM), there's a gap
	if !lookupResult.Found {
		return &FetchResponse{
			HighWatermark: lookupResult.HWM,
		}, nil
	}

	entry := lookupResult.Entry

	// Read batches from storage based on file type
	var fetchResult *FetchResult
	switch entry.FileType {
	case index.FileTypeWAL:
		fetchResult, err = f.walReader.ReadBatches(ctx, entry, req.FetchOffset, req.MaxBytes)
		if err != nil {
			return nil, err
		}

	case index.FileTypeParquet:
		fetchResult, err = f.parquetReader.ReadBatches(ctx, entry, req.FetchOffset, req.MaxBytes)
		if err != nil {
			return nil, err
		}

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
	}, nil
}
