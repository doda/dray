package property

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/google/uuid"
)

// AppendOp represents a random append operation for property testing.
type AppendOp struct {
	RecordCount    uint32
	ChunkSizeBytes int64
}

// Generate implements quick.Generator for AppendOp.
func (AppendOp) Generate(rand *rand.Rand, size int) reflect.Value {
	op := AppendOp{
		RecordCount:    uint32(rand.Intn(1000) + 1),   // 1-1000 records
		ChunkSizeBytes: int64(rand.Intn(100000) + 1), // 1-100000 bytes
	}
	return reflect.ValueOf(op)
}

// TestPropertyOffsetsMonotonic verifies that offsets are always monotonically
// increasing regardless of the sequence of append operations.
func TestPropertyOffsetsMonotonic(t *testing.T) {
	f := func(ops []AppendOp) bool {
		if len(ops) == 0 {
			return true
		}

		// Limit operations to avoid excessive test time
		if len(ops) > 100 {
			ops = ops[:100]
		}

		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, err := sm.CreateStream(ctx, "test-topic", 0)
		if err != nil {
			t.Logf("CreateStream failed: %v", err)
			return false
		}

		var prevEndOffset int64 = 0
		for i, op := range ops {
			// Filter invalid ops
			if op.RecordCount == 0 {
				op.RecordCount = 1
			}
			if op.ChunkSizeBytes <= 0 {
				op.ChunkSizeBytes = 1
			}

			result, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       streamID,
				RecordCount:    op.RecordCount,
				ChunkSizeBytes: op.ChunkSizeBytes,
				CreatedAtMs:    time.Now().UnixMilli(),
				WalID:          uuid.New().String(),
				WalPath:        "s3://bucket/wal/test.wo",
				ChunkOffset:    0,
				ChunkLength:    uint32(op.ChunkSizeBytes),
			})
			if err != nil {
				t.Logf("AppendIndexEntry %d failed: %v", i, err)
				return false
			}

			// Property 1: StartOffset must equal previous EndOffset
			if result.StartOffset != prevEndOffset {
				t.Logf("Monotonicity violation at op %d: StartOffset=%d, prevEndOffset=%d",
					i, result.StartOffset, prevEndOffset)
				return false
			}

			// Property 2: EndOffset must be strictly greater than StartOffset
			if result.EndOffset <= result.StartOffset {
				t.Logf("Invalid range at op %d: StartOffset=%d, EndOffset=%d",
					i, result.StartOffset, result.EndOffset)
				return false
			}

			// Property 3: EndOffset - StartOffset must equal RecordCount
			if result.EndOffset-result.StartOffset != int64(op.RecordCount) {
				t.Logf("Record count mismatch at op %d: expected %d, got %d",
					i, op.RecordCount, result.EndOffset-result.StartOffset)
				return false
			}

			prevEndOffset = result.EndOffset
		}

		// Final property: HWM must equal last EndOffset
		hwm, _, err := sm.GetHWM(ctx, streamID)
		if err != nil {
			t.Logf("GetHWM failed: %v", err)
			return false
		}
		if hwm != prevEndOffset {
			t.Logf("HWM mismatch: got %d, expected %d", hwm, prevEndOffset)
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}

// TestPropertyLookupsReturnCorrectEntries verifies that offset lookups
// always return the correct entry containing that offset.
func TestPropertyLookupsReturnCorrectEntries(t *testing.T) {
	f := func(recordCounts []uint8) bool {
		if len(recordCounts) == 0 {
			return true
		}

		// Limit and sanitize input
		if len(recordCounts) > 50 {
			recordCounts = recordCounts[:50]
		}

		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, err := sm.CreateStream(ctx, "test-topic", 0)
		if err != nil {
			return false
		}

		// Track all entries we create
		type entryInfo struct {
			startOffset int64
			endOffset   int64
		}
		var entries []entryInfo

		for _, rc := range recordCounts {
			recordCount := uint32(rc) + 1 // Ensure at least 1 record
			result, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       streamID,
				RecordCount:    recordCount,
				ChunkSizeBytes: int64(recordCount * 100),
				CreatedAtMs:    time.Now().UnixMilli(),
				WalID:          uuid.New().String(),
				WalPath:        "s3://bucket/wal/test.wo",
				ChunkOffset:    0,
				ChunkLength:    recordCount * 100,
			})
			if err != nil {
				return false
			}
			entries = append(entries, entryInfo{
				startOffset: result.StartOffset,
				endOffset:   result.EndOffset,
			})
		}

		if len(entries) == 0 {
			return true
		}

		hwm, _, _ := sm.GetHWM(ctx, streamID)

		// Test lookup for various offsets
		testOffsets := []int64{0}
		for _, e := range entries {
			testOffsets = append(testOffsets, e.startOffset, e.startOffset+1, e.endOffset-1)
		}
		testOffsets = append(testOffsets, hwm, hwm+1)

		for _, offset := range testOffsets {
			result, err := sm.LookupOffset(ctx, streamID, offset)
			if err != nil {
				return false
			}

			if offset >= hwm {
				// Should be beyond HWM
				if !result.OffsetBeyondHWM {
					t.Logf("Expected OffsetBeyondHWM for offset %d >= hwm %d", offset, hwm)
					return false
				}
				continue
			}

			// Should find an entry
			if !result.Found {
				t.Logf("Expected to find entry for offset %d < hwm %d", offset, hwm)
				return false
			}

			if result.Entry == nil {
				t.Logf("Entry is nil for offset %d", offset)
				return false
			}

			// Verify the entry actually contains the offset
			if offset < result.Entry.StartOffset || offset >= result.Entry.EndOffset {
				t.Logf("Entry [%d, %d) does not contain offset %d",
					result.Entry.StartOffset, result.Entry.EndOffset, offset)
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}

// TestPropertyCumulativeSizes verifies that cumulative sizes are always
// monotonically increasing and correctly computed.
func TestPropertyCumulativeSizes(t *testing.T) {
	f := func(chunkSizes []uint16) bool {
		if len(chunkSizes) == 0 {
			return true
		}

		if len(chunkSizes) > 50 {
			chunkSizes = chunkSizes[:50]
		}

		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, err := sm.CreateStream(ctx, "test-topic", 0)
		if err != nil {
			return false
		}

		var expectedCumulative int64 = 0
		for i, cs := range chunkSizes {
			chunkSize := int64(cs) + 1 // At least 1 byte
			expectedCumulative += chunkSize

			_, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       streamID,
				RecordCount:    10,
				ChunkSizeBytes: chunkSize,
				CreatedAtMs:    time.Now().UnixMilli(),
				WalID:          uuid.New().String(),
				WalPath:        "s3://bucket/wal/test.wo",
				ChunkOffset:    0,
				ChunkLength:    uint32(chunkSize),
			})
			if err != nil {
				t.Logf("Append %d failed: %v", i, err)
				return false
			}
		}

		// Verify cumulative sizes
		entries, err := sm.ListIndexEntries(ctx, streamID, 0)
		if err != nil {
			return false
		}

		if len(entries) != len(chunkSizes) {
			t.Logf("Entry count mismatch: got %d, expected %d", len(entries), len(chunkSizes))
			return false
		}

		var runningSum int64 = 0
		for i, entry := range entries {
			chunkSize := int64(chunkSizes[i]) + 1
			runningSum += chunkSize
			if entry.CumulativeSize != runningSum {
				t.Logf("Cumulative size mismatch at %d: got %d, expected %d",
					i, entry.CumulativeSize, runningSum)
				return false
			}

			// Cumulative sizes must be strictly increasing
			if i > 0 && entry.CumulativeSize <= entries[i-1].CumulativeSize {
				t.Logf("Cumulative sizes not monotonic at %d", i)
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}

// TestPropertyConcurrentAppendsPreserveInvariants tests that concurrent
// append operations preserve all offset index invariants.
func TestPropertyConcurrentAppendsPreserveInvariants(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		numWriters := rng.Intn(10) + 2     // 2-11 writers
		appendsPerWriter := rng.Intn(20) + 5 // 5-24 appends each
		recordsPerAppend := uint32(rng.Intn(50) + 1) // 1-50 records

		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, err := sm.CreateStream(ctx, "test-topic", 0)
		if err != nil {
			return false
		}

		type result struct {
			startOffset int64
			endOffset   int64
		}
		resultsCh := make(chan result, numWriters*appendsPerWriter)
		var wg sync.WaitGroup
		var successCount int64

		for w := 0; w < numWriters; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < appendsPerWriter; i++ {
					for retries := 0; retries < 200; retries++ {
						r, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
							StreamID:       streamID,
							RecordCount:    recordsPerAppend,
							ChunkSizeBytes: 1000,
							CreatedAtMs:    time.Now().UnixMilli(),
							WalID:          uuid.New().String(),
							WalPath:        "s3://bucket/wal/test.wo",
							ChunkOffset:    0,
							ChunkLength:    1000,
						})
						if err == nil {
							resultsCh <- result{r.StartOffset, r.EndOffset}
							atomic.AddInt64(&successCount, 1)
							break
						}
						if errors.Is(err, metadata.ErrVersionMismatch) || errors.Is(err, metadata.ErrTxnConflict) {
							continue
						}
						t.Logf("Unexpected error: %v", err)
						return
					}
				}
			}()
		}

		wg.Wait()
		close(resultsCh)

		expectedAppends := int64(numWriters * appendsPerWriter)
		if successCount != expectedAppends {
			t.Logf("Expected %d appends, got %d", expectedAppends, successCount)
			return false
		}

		// Collect and sort results
		var results []result
		for r := range resultsCh {
			results = append(results, r)
		}
		sort.Slice(results, func(i, j int) bool {
			return results[i].startOffset < results[j].startOffset
		})

		// Verify invariants
		if len(results) == 0 {
			return true
		}

		// First entry must start at 0
		if results[0].startOffset != 0 {
			t.Logf("First entry doesn't start at 0: %d", results[0].startOffset)
			return false
		}

		// All entries must be contiguous and non-overlapping
		for i := 1; i < len(results); i++ {
			if results[i].startOffset != results[i-1].endOffset {
				t.Logf("Gap or overlap at %d: prev.end=%d, curr.start=%d",
					i, results[i-1].endOffset, results[i].startOffset)
				return false
			}
			if results[i].endOffset-results[i].startOffset != int64(recordsPerAppend) {
				t.Logf("Wrong record count at %d", i)
				return false
			}
		}

		// HWM must match final offset
		expectedHWM := expectedAppends * int64(recordsPerAppend)
		hwm, _, _ := sm.GetHWM(ctx, streamID)
		if hwm != expectedHWM {
			t.Logf("HWM mismatch: got %d, expected %d", hwm, expectedHWM)
			return false
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 20}); err != nil {
		t.Error(err)
	}
}

// TestPropertyRandomLookups tests that random offset lookups always
// return consistent results.
func TestPropertyRandomLookups(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))

		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, err := sm.CreateStream(ctx, "test-topic", 0)
		if err != nil {
			return false
		}

		// Create random entries
		numEntries := rng.Intn(50) + 1
		var totalRecords int64 = 0
		for i := 0; i < numEntries; i++ {
			recordCount := uint32(rng.Intn(100) + 1)
			_, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       streamID,
				RecordCount:    recordCount,
				ChunkSizeBytes: int64(recordCount * 10),
				CreatedAtMs:    time.Now().UnixMilli(),
				WalID:          uuid.New().String(),
				WalPath:        "s3://bucket/wal/test.wo",
				ChunkOffset:    0,
				ChunkLength:    recordCount * 10,
			})
			if err != nil {
				return false
			}
			totalRecords += int64(recordCount)
		}

		// Perform random lookups
		numLookups := rng.Intn(100) + 10
		for i := 0; i < numLookups; i++ {
			// Random offset in extended range
			offset := rng.Int63n(totalRecords*2) - totalRecords/2

			result, err := sm.LookupOffset(ctx, streamID, offset)
			if err != nil {
				return false
			}

			if offset >= totalRecords {
				if !result.OffsetBeyondHWM {
					t.Logf("Offset %d >= HWM %d should be beyond", offset, totalRecords)
					return false
				}
			} else if offset >= 0 {
				if !result.Found {
					t.Logf("Offset %d should be found (HWM=%d)", offset, totalRecords)
					return false
				}
				if result.Entry == nil {
					return false
				}
				if offset < result.Entry.StartOffset || offset >= result.Entry.EndOffset {
					t.Logf("Entry [%d, %d) doesn't contain %d",
						result.Entry.StartOffset, result.Entry.EndOffset, offset)
					return false
				}
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 50}); err != nil {
		t.Error(err)
	}
}

// TestPropertyIndexEntriesAreOrdered verifies that listing index entries
// always returns them in offset order.
func TestPropertyIndexEntriesAreOrdered(t *testing.T) {
	f := func(recordCounts []uint8) bool {
		if len(recordCounts) == 0 {
			return true
		}
		if len(recordCounts) > 100 {
			recordCounts = recordCounts[:100]
		}

		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

		for _, rc := range recordCounts {
			recordCount := uint32(rc) + 1
			_, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       streamID,
				RecordCount:    recordCount,
				ChunkSizeBytes: int64(recordCount * 10),
				CreatedAtMs:    time.Now().UnixMilli(),
				WalID:          uuid.New().String(),
				WalPath:        "s3://bucket/wal/test.wo",
				ChunkOffset:    0,
				ChunkLength:    recordCount * 10,
			})
			if err != nil {
				return false
			}
		}

		entries, err := sm.ListIndexEntries(ctx, streamID, 0)
		if err != nil {
			return false
		}

		// Entries must be in ascending order by EndOffset
		for i := 1; i < len(entries); i++ {
			if entries[i].EndOffset <= entries[i-1].EndOffset {
				t.Logf("Entries not ordered at %d: %d <= %d",
					i, entries[i].EndOffset, entries[i-1].EndOffset)
				return false
			}
			if entries[i].StartOffset != entries[i-1].EndOffset {
				t.Logf("Entries not contiguous at %d", i)
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}

// TestPropertyHWMMatchesLastOffset verifies that HWM always matches
// the EndOffset of the last entry.
func TestPropertyHWMMatchesLastOffset(t *testing.T) {
	f := func(recordCounts []uint8) bool {
		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

		if len(recordCounts) == 0 {
			hwm, _, _ := sm.GetHWM(ctx, streamID)
			return hwm == 0
		}

		if len(recordCounts) > 50 {
			recordCounts = recordCounts[:50]
		}

		var lastEndOffset int64
		for _, rc := range recordCounts {
			recordCount := uint32(rc) + 1
			result, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       streamID,
				RecordCount:    recordCount,
				ChunkSizeBytes: int64(recordCount * 10),
				CreatedAtMs:    time.Now().UnixMilli(),
				WalID:          uuid.New().String(),
				WalPath:        "s3://bucket/wal/test.wo",
				ChunkOffset:    0,
				ChunkLength:    recordCount * 10,
			})
			if err != nil {
				return false
			}
			lastEndOffset = result.EndOffset

			// Check HWM after each append
			hwm, _, _ := sm.GetHWM(ctx, streamID)
			if hwm != lastEndOffset {
				t.Logf("HWM %d != lastEndOffset %d after append", hwm, lastEndOffset)
				return false
			}
		}

		return true
	}

	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}

// TestPropertyNoOffsetGaps verifies that there are never gaps in
// the offset sequence across all index entries.
func TestPropertyNoOffsetGaps(t *testing.T) {
	f := func(recordCounts []uint8) bool {
		if len(recordCounts) == 0 {
			return true
		}
		if len(recordCounts) > 100 {
			recordCounts = recordCounts[:100]
		}

		store := metadata.NewMockStore()
		sm := index.NewStreamManager(store)
		ctx := context.Background()

		streamID, _ := sm.CreateStream(ctx, "test-topic", 0)

		for _, rc := range recordCounts {
			recordCount := uint32(rc) + 1
			_, err := sm.AppendIndexEntry(ctx, index.AppendRequest{
				StreamID:       streamID,
				RecordCount:    recordCount,
				ChunkSizeBytes: int64(recordCount * 10),
				CreatedAtMs:    time.Now().UnixMilli(),
				WalID:          uuid.New().String(),
				WalPath:        "s3://bucket/wal/test.wo",
				ChunkOffset:    0,
				ChunkLength:    recordCount * 10,
			})
			if err != nil {
				return false
			}
		}

		entries, _ := sm.ListIndexEntries(ctx, streamID, 0)

		// First entry must start at 0
		if len(entries) > 0 && entries[0].StartOffset != 0 {
			t.Logf("First entry doesn't start at 0")
			return false
		}

		// All entries must be perfectly contiguous
		for i := 1; i < len(entries); i++ {
			if entries[i].StartOffset != entries[i-1].EndOffset {
				t.Logf("Gap at %d: [%d, %d) then [%d, %d)",
					i, entries[i-1].StartOffset, entries[i-1].EndOffset,
					entries[i].StartOffset, entries[i].EndOffset)
				return false
			}
		}

		// Every offset from 0 to HWM-1 must be findable
		hwm, _, _ := sm.GetHWM(ctx, streamID)
		for offset := int64(0); offset < hwm; offset++ {
			result, err := sm.LookupOffset(ctx, streamID, offset)
			if err != nil {
				return false
			}
			if !result.Found {
				t.Logf("Offset %d not found but HWM is %d", offset, hwm)
				return false
			}
		}

		return true
	}

	// Use fewer iterations since we check every offset
	if err := quick.Check(f, &quick.Config{MaxCount: 20}); err != nil {
		t.Error(err)
	}
}
