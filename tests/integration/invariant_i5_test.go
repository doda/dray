package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/compaction"
	"github.com/dray-io/dray/internal/compaction/worker"
	"github.com/dray-io/dray/internal/fetch"
	"github.com/dray-io/dray/internal/index"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/metadata/keys"
	"github.com/dray-io/dray/internal/produce"
	"github.com/dray-io/dray/internal/protocol"
	"github.com/dray-io/dray/internal/topics"
)

// TestInvariantI5_CompactionIndexSwapAtomicity tests invariant I5:
// Compaction index swap is atomic - readers never see partial index state.
//
// The test:
// 1. Produces records to create multiple WAL index entries
// 2. Starts concurrent fetch operations in background goroutines
// 3. Runs compaction to swap WAL entries for a Parquet entry
// 4. Verifies that fetch operations never see:
//   - Missing records (partial index where some WAL entries are gone but Parquet not yet inserted)
//   - Duplicate records (both WAL and Parquet entries visible)
//   - Incomplete data (fewer records than expected)
//
// 5. Verifies all records are returned correctly after compaction
func TestInvariantI5_CompactionIndexSwapAtomicity(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	// Create topic with one partition
	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i5-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i5-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1, // Immediate flush
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Step 1: Produce multiple batches to create multiple WAL index entries
	// This ensures compaction has multiple entries to swap atomically
	recordsPerBatch := []int{3, 4, 5, 3} // 15 total records across 4 batches
	var totalRecords int
	for _, count := range recordsPerBatch {
		totalRecords += count
	}

	for i, recordCount := range recordsPerBatch {
		produceReq := buildCompactionTestProduceRequest("invariant-i5-topic", 0, recordCount, i)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)

		partResp := produceResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			t.Fatalf("produce batch %d failed with error code %d", i, partResp.ErrorCode)
		}
		t.Logf("Produced batch %d: %d records, BaseOffset=%d", i, recordCount, partResp.BaseOffset)
	}

	// Verify we have multiple WAL index entries before compaction
	entries, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected multiple WAL entries for atomicity test, got %d", len(entries))
	}
	t.Logf("Created %d WAL index entries before compaction", len(entries))

	// Step 2: Start concurrent fetch operations
	// These will run continuously during compaction to check for partial states
	stopFetchers := make(chan struct{})
	var fetcherWg sync.WaitGroup
	var atomicityViolations int32
	var partialStateErrors []string
	var partialStatesMu sync.Mutex
	var fetchCount int32

	numFetchers := 5
	for i := 0; i < numFetchers; i++ {
		fetcherID := i
		fetcherWg.Add(1)
		go func() {
			defer fetcherWg.Done()
			for {
				select {
				case <-stopFetchers:
					return
				default:
				}

				// Fetch all records and verify we get either:
				// - All WAL-based records (before swap)
				// - All Parquet-based records (after swap)
				// Never a partial state
				records, err := fetchAllRecordsNoFatal(t, fetchHandler, ctx, "invariant-i5-topic", 0, 0)
				if err != nil {
					atomic.AddInt32(&atomicityViolations, 1)
					partialStatesMu.Lock()
					partialStateErrors = append(partialStateErrors, fmt.Sprintf(
						"fetcher %d: fetch error: %v",
						fetcherID, err))
					partialStatesMu.Unlock()
					continue
				}
				atomic.AddInt32(&fetchCount, 1)

				// Check for atomicity violation: wrong number of records
				if len(records) != 0 && len(records) != totalRecords {
					atomic.AddInt32(&atomicityViolations, 1)
					partialStatesMu.Lock()
					partialStateErrors = append(partialStateErrors, fmt.Sprintf(
						"fetcher %d: expected 0 or %d records, got %d (possible partial index state)",
						fetcherID, totalRecords, len(records)))
					partialStatesMu.Unlock()
				}

				// Verify offset continuity (no gaps in the returned records)
				if len(records) > 0 {
					for j := 1; j < len(records); j++ {
						if records[j].Offset != records[j-1].Offset+1 {
							atomic.AddInt32(&atomicityViolations, 1)
							partialStatesMu.Lock()
							partialStateErrors = append(partialStateErrors, fmt.Sprintf(
								"fetcher %d: offset gap at index %d: %d -> %d",
								fetcherID, j, records[j-1].Offset, records[j].Offset))
							partialStatesMu.Unlock()
						}
					}
				}

				// Small sleep to avoid overwhelming the system
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	// Let fetchers run for a bit before compaction
	time.Sleep(50 * time.Millisecond)

	// Step 3: Run compaction while fetchers are active
	t.Log("Running compaction while concurrent fetches are active...")
	runCompaction(t, ctx, streamID, metaStore, objStore, 0)
	t.Log("Compaction complete")

	// Let fetchers run a bit after compaction
	time.Sleep(50 * time.Millisecond)

	// Stop fetchers
	close(stopFetchers)
	fetcherWg.Wait()

	// Step 4: Check for atomicity violations
	violations := atomic.LoadInt32(&atomicityViolations)
	fetches := atomic.LoadInt32(&fetchCount)
	t.Logf("Completed %d fetch operations across %d concurrent fetchers", fetches, numFetchers)

	if violations > 0 {
		partialStatesMu.Lock()
		for _, err := range partialStateErrors {
			t.Errorf("Atomicity violation: %s", err)
		}
		partialStatesMu.Unlock()
		t.Fatalf("I5 VIOLATED: %d atomicity violations detected during compaction", violations)
	}

	// Step 5: Verify final state is correct
	entries, err = streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list index entries after compaction: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 Parquet entry after compaction, got %d", len(entries))
	}
	if entries[0].FileType != index.FileTypeParquet {
		t.Fatalf("expected Parquet entry, got %s", entries[0].FileType)
	}

	// Verify all records are still accessible
	finalRecords := fetchAllRecords(t, fetchHandler, ctx, "invariant-i5-topic", 0, 0)
	if len(finalRecords) != totalRecords {
		t.Fatalf("expected %d records after compaction, got %d", totalRecords, len(finalRecords))
	}

	// Verify offsets are correct (0 to totalRecords-1)
	for i, rec := range finalRecords {
		if rec.Offset != int64(i) {
			t.Errorf("record %d has offset %d, expected %d", i, rec.Offset, i)
		}
	}

	t.Logf("PASS: I5 invariant verified - %d concurrent fetches during compaction saw no partial states", fetches)
}

// TestInvariantI5_ConcurrentCompactionAndFetch tests that multiple fetches
// during compaction all see consistent states.
func TestInvariantI5_ConcurrentCompactionAndFetch(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i5-concurrent-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i5-concurrent-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Produce records in multiple batches
	totalRecords := 20
	batchSize := 4
	for batch := 0; batch*batchSize < totalRecords; batch++ {
		count := batchSize
		if (batch+1)*batchSize > totalRecords {
			count = totalRecords - batch*batchSize
		}
		produceReq := buildCompactionTestProduceRequest("invariant-i5-concurrent-topic", 0, count, batch)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)
		if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("produce batch %d failed", batch)
		}
	}

	// Track violations atomically
	var violations int32
	var fetchCount int32

	// Start fetchers
	numFetchers := 10
	compactionDone := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < numFetchers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-compactionDone:
					return
				default:
				}

				records, err := fetchAllRecordsNoFatal(t, fetchHandler, ctx, "invariant-i5-concurrent-topic", 0, 0)
				if err != nil {
					atomic.AddInt32(&violations, 1)
					continue
				}
				atomic.AddInt32(&fetchCount, 1)

				// Check for consistency: must see 0 or totalRecords
				if len(records) > 0 && len(records) != totalRecords {
					atomic.AddInt32(&violations, 1)
				}

				// Check offset continuity
				for j := 1; j < len(records); j++ {
					if records[j].Offset != records[j-1].Offset+1 {
						atomic.AddInt32(&violations, 1)
					}
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Run compaction
	time.Sleep(10 * time.Millisecond)
	runCompaction(t, ctx, streamID, metaStore, objStore, 0)
	time.Sleep(10 * time.Millisecond)

	close(compactionDone)
	wg.Wait()

	// Check for errors
	if violations := atomic.LoadInt32(&violations); violations > 0 {
		t.Errorf("Detected %d consistency violations", violations)
	}

	t.Logf("PASS: %d fetch operations completed without consistency violations", atomic.LoadInt32(&fetchCount))
}

// TestInvariantI5_IndexStateNeverPartial directly tests that the index
// lookup never returns a partial result during compaction.
func TestInvariantI5_IndexStateNeverPartial(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i5-index-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i5-index-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	// Produce 5 batches of records
	totalRecords := 25
	batchSize := 5
	for batch := 0; batch < 5; batch++ {
		produceReq := buildCompactionTestProduceRequest("invariant-i5-index-topic", 0, batchSize, batch)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)
		if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("produce batch %d failed", batch)
		}
	}

	// Verify we have 5 WAL entries before compaction
	entriesBefore, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list entries: %v", err)
	}
	if len(entriesBefore) != 5 {
		t.Fatalf("expected 5 WAL entries, got %d", len(entriesBefore))
	}

	// Concurrent goroutines check index state
	stopCheck := make(chan struct{})
	var checkWg sync.WaitGroup
	var partialStates int32

	numCheckers := 10
	for i := 0; i < numCheckers; i++ {
		checkWg.Add(1)
		go func() {
			defer checkWg.Done()
			for {
				select {
				case <-stopCheck:
					return
				default:
				}

				entries, err := streamManager.ListIndexEntries(ctx, streamID, 0)
				if err != nil {
					continue
				}

				// Calculate total records covered by entries
				var totalCovered int64
				for _, entry := range entries {
					totalCovered += entry.EndOffset - entry.StartOffset
				}

				// Should always cover all records or none (during init)
				if totalCovered != 0 && totalCovered != int64(totalRecords) {
					atomic.AddInt32(&partialStates, 1)
				}

				// Check for mix of WAL and Parquet covering same offsets
				if hasOverlappingEntries(entries) {
					atomic.AddInt32(&partialStates, 1)
				}
			}
		}()
	}

	// Let checkers run a bit
	time.Sleep(20 * time.Millisecond)

	// Run compaction
	runCompaction(t, ctx, streamID, metaStore, objStore, 0)

	// Let checkers run a bit more
	time.Sleep(20 * time.Millisecond)

	close(stopCheck)
	checkWg.Wait()

	if atomic.LoadInt32(&partialStates) > 0 {
		t.Fatalf("I5 VIOLATED: detected %d partial index states during compaction", partialStates)
	}

	// Verify final state
	entriesAfter, err := streamManager.ListIndexEntries(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("failed to list entries after compaction: %v", err)
	}
	if len(entriesAfter) != 1 {
		t.Fatalf("expected 1 Parquet entry after compaction, got %d", len(entriesAfter))
	}

	t.Logf("PASS: No partial index states observed during compaction")
}

// TestInvariantI5_FetchAllOffsetsReturnedCorrectly verifies that all records
// are accessible both before and after compaction with correct offsets.
func TestInvariantI5_FetchAllOffsetsReturnedCorrectly(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i5-offsets-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i5-offsets-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Produce records with identifiable content
	totalRecords := 15
	batchSize := 5
	for batch := 0; batch < 3; batch++ {
		produceReq := buildCompactionTestProduceRequest("invariant-i5-offsets-topic", 0, batchSize, batch)
		produceResp := produceHandler.Handle(ctx, 9, produceReq)
		if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("produce batch %d failed", batch)
		}
	}

	// Fetch all records before compaction
	recordsBefore := fetchAllRecords(t, fetchHandler, ctx, "invariant-i5-offsets-topic", 0, 0)
	if len(recordsBefore) != totalRecords {
		t.Fatalf("expected %d records before compaction, got %d", totalRecords, len(recordsBefore))
	}

	// Verify offsets before compaction
	for i, rec := range recordsBefore {
		if rec.Offset != int64(i) {
			t.Errorf("before compaction: record %d has offset %d", i, rec.Offset)
		}
	}

	// Run compaction
	t.Log("Running compaction...")
	runCompaction(t, ctx, streamID, metaStore, objStore, 0)

	// Fetch all records after compaction
	recordsAfter := fetchAllRecords(t, fetchHandler, ctx, "invariant-i5-offsets-topic", 0, 0)
	if len(recordsAfter) != totalRecords {
		t.Fatalf("expected %d records after compaction, got %d", totalRecords, len(recordsAfter))
	}

	// Verify offsets after compaction
	for i, rec := range recordsAfter {
		if rec.Offset != int64(i) {
			t.Errorf("after compaction: record %d has offset %d", i, rec.Offset)
		}
	}

	// Verify content matches (keys and values)
	for i := 0; i < totalRecords; i++ {
		if !bytes.Equal(recordsBefore[i].Key, recordsAfter[i].Key) {
			t.Errorf("record %d: key mismatch after compaction", i)
		}
		if !bytes.Equal(recordsBefore[i].Value, recordsAfter[i].Value) {
			t.Errorf("record %d: value mismatch after compaction", i)
		}
	}

	// Test fetching from middle offset
	midOffset := int64(7)
	recordsFromMid := fetchAllRecords(t, fetchHandler, ctx, "invariant-i5-offsets-topic", 0, midOffset)
	expectedFromMid := totalRecords - int(midOffset)
	if len(recordsFromMid) != expectedFromMid {
		t.Fatalf("expected %d records from offset %d, got %d", expectedFromMid, midOffset, len(recordsFromMid))
	}

	// Verify first record from mid-fetch has correct offset
	if recordsFromMid[0].Offset != midOffset {
		t.Errorf("expected first record at offset %d, got %d", midOffset, recordsFromMid[0].Offset)
	}

	t.Logf("PASS: All %d records returned correctly with proper offsets before and after compaction", totalRecords)
}

// TestInvariantI5_MultipleCompactions tests that multiple sequential compactions
// maintain atomicity.
func TestInvariantI5_MultipleCompactions(t *testing.T) {
	metaStore := metadata.NewMockStore()
	topicStore := topics.NewStore(metaStore)
	streamManager := index.NewStreamManager(metaStore)
	objStore := newCompactionTestObjectStore()
	ctx := context.Background()

	result, err := topicStore.CreateTopic(ctx, topics.CreateTopicRequest{
		Name:           "invariant-i5-multi-compact-topic",
		PartitionCount: 1,
		NowMs:          time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	streamID := result.Partitions[0].StreamID
	if err := streamManager.CreateStreamWithID(ctx, streamID, "invariant-i5-multi-compact-topic", 0); err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	committer := produce.NewCommitter(objStore, metaStore, produce.CommitterConfig{
		NumDomains: 4,
	})

	buffer := produce.NewBuffer(produce.BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1,
		NumDomains:     4,
		OnFlush:        committer.CreateFlushHandler(),
	})
	defer buffer.Close()

	produceHandler := protocol.NewProduceHandler(
		protocol.ProduceHandlerConfig{},
		topicStore,
		buffer,
	)

	fetcher := fetch.NewFetcher(objStore, streamManager)
	fetchHandler := protocol.NewFetchHandler(
		protocol.FetchHandlerConfig{MaxBytes: 1024 * 1024},
		topicStore,
		fetcher,
		streamManager,
	)

	// Produce, compact, produce more, compact again - multiple cycles
	cycles := 3
	recordsPerCycle := 6
	totalRecords := 0

	for cycle := 0; cycle < cycles; cycle++ {
		// Produce records for this cycle
		for batch := 0; batch < 2; batch++ {
			produceReq := buildCompactionTestProduceRequest("invariant-i5-multi-compact-topic", 0, 3, cycle*10+batch)
			produceResp := produceHandler.Handle(ctx, 9, produceReq)
			if produceResp.Topics[0].Partitions[0].ErrorCode != 0 {
				t.Fatalf("cycle %d batch %d: produce failed", cycle, batch)
			}
		}
		totalRecords += recordsPerCycle

		// Concurrent fetch during compaction
		stopFetch := make(chan struct{})
		var fetchWg sync.WaitGroup
		var violations int32

		fetchWg.Add(1)
		go func() {
			defer fetchWg.Done()
			for {
				select {
				case <-stopFetch:
					return
				default:
				}

				records, err := fetchAllRecordsNoFatal(t, fetchHandler, ctx, "invariant-i5-multi-compact-topic", 0, 0)
				if err != nil {
					atomic.AddInt32(&violations, 1)
					continue
				}
				// Should see either total from previous cycles, or total including this cycle
				expectedMin := totalRecords - recordsPerCycle
				if len(records) > 0 && len(records) != expectedMin && len(records) != totalRecords {
					atomic.AddInt32(&violations, 1)
				}

				time.Sleep(time.Millisecond)
			}
		}()

		// Run compaction for all WAL entries
		runCompactionWALOnly(t, ctx, streamID, metaStore, objStore, 0)

		close(stopFetch)
		fetchWg.Wait()

		if atomic.LoadInt32(&violations) > 0 {
			t.Errorf("cycle %d: detected %d atomicity violations", cycle, violations)
		}

		// Verify all records are accessible
		records := fetchAllRecords(t, fetchHandler, ctx, "invariant-i5-multi-compact-topic", 0, 0)
		if len(records) != totalRecords {
			t.Errorf("cycle %d: expected %d total records, got %d", cycle, totalRecords, len(records))
		}

		t.Logf("Cycle %d: produced %d records, total %d, compaction successful", cycle, recordsPerCycle, totalRecords)
	}

	// Final verification
	finalRecords := fetchAllRecords(t, fetchHandler, ctx, "invariant-i5-multi-compact-topic", 0, 0)
	if len(finalRecords) != totalRecords {
		t.Fatalf("expected %d total records after all cycles, got %d", totalRecords, len(finalRecords))
	}

	// Verify continuous offsets
	for i, rec := range finalRecords {
		if rec.Offset != int64(i) {
			t.Errorf("final record %d has offset %d", i, rec.Offset)
		}
	}

	t.Logf("PASS: %d compaction cycles completed, %d total records verified", cycles, totalRecords)
}

// hasOverlappingEntries checks if any index entries overlap in offset ranges.
func hasOverlappingEntries(entries []index.IndexEntry) bool {
	if len(entries) <= 1 {
		return false
	}

	// Sort by start offset
	sorted := make([]index.IndexEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartOffset < sorted[j].StartOffset
	})

	// Check for overlaps
	for i := 1; i < len(sorted); i++ {
		if sorted[i].StartOffset < sorted[i-1].EndOffset {
			return true
		}
	}

	return false
}

// fetchAllRecordsNoFatal is a goroutine-safe variant of fetchAllRecords.
func fetchAllRecordsNoFatal(t *testing.T, handler *protocol.FetchHandler, ctx context.Context, topic string, partition int32, startOffset int64) ([]ExtractedRecord, error) {
	t.Helper()

	var allRecords []ExtractedRecord
	currentOffset := startOffset

	for {
		fetchReq := buildFetchRequest(topic, partition, currentOffset)
		fetchResp := handler.Handle(ctx, 12, fetchReq)

		if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
			return nil, fmt.Errorf("unexpected empty fetch response")
		}

		partResp := fetchResp.Topics[0].Partitions[0]
		if partResp.ErrorCode != 0 {
			return nil, fmt.Errorf("fetch failed with error code %d at offset %d", partResp.ErrorCode, currentOffset)
		}

		// If no data returned, we've reached HWM
		if len(partResp.RecordBatches) == 0 {
			break
		}

		// Parse the record batches to extract logical records
		records := parseRecordBatches(t, partResp.RecordBatches)
		if len(records) == 0 {
			break
		}

		// Filter records to only those at or after startOffset
		for _, rec := range records {
			if rec.Offset >= startOffset {
				allRecords = append(allRecords, rec)
			}
		}
		currentOffset = records[len(records)-1].Offset + 1

		// Stop if we've reached HWM
		if currentOffset >= partResp.HighWatermark {
			break
		}
	}

	return allRecords, nil
}

// runCompactionWALOnly runs compaction only for WAL entries (ignores existing Parquet).
func runCompactionWALOnly(t *testing.T, ctx context.Context, streamID string, metaStore metadata.MetadataStore, objStore *compactionTestObjectStore, partition int32) {
	t.Helper()

	prefix := keys.OffsetIndexPrefix(streamID)
	kvs, err := metaStore.List(ctx, prefix, "", 0)
	if err != nil {
		t.Fatalf("failed to list index entries: %v", err)
	}

	var walEntries []*index.IndexEntry
	var walIndexKeys []string
	var minOffset, maxOffset int64 = 0, 0
	first := true

	for _, kv := range kvs {
		var entry index.IndexEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			t.Fatalf("failed to parse index entry: %v", err)
		}
		if entry.FileType == index.FileTypeWAL {
			walEntries = append(walEntries, &entry)
			walIndexKeys = append(walIndexKeys, kv.Key)
			if first || entry.StartOffset < minOffset {
				minOffset = entry.StartOffset
			}
			if entry.EndOffset > maxOffset {
				maxOffset = entry.EndOffset
			}
			first = false
		}
	}

	if len(walEntries) == 0 {
		t.Log("No WAL entries to compact")
		return
	}

	converter := worker.NewConverter(objStore, nil)
	convertResult, err := converter.Convert(ctx, walEntries, partition, "", nil)
	if err != nil {
		t.Fatalf("failed to convert WAL to Parquet: %v", err)
	}

	date := time.Now().Format("2006-01-02")
	parquetID := worker.GenerateParquetID()
	parquetPath := worker.GenerateParquetPath("test-topic", partition, date, parquetID)
	if err := converter.WriteParquetToStorage(ctx, parquetPath, convertResult.ParquetData); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	parquetEntry := index.IndexEntry{
		StreamID:         streamID,
		StartOffset:      minOffset,
		EndOffset:        maxOffset,
		FileType:         index.FileTypeParquet,
		RecordCount:      uint32(convertResult.RecordCount),
		MessageCount:     uint32(convertResult.RecordCount),
		CreatedAtMs:      time.Now().UnixMilli(),
		MinTimestampMs:   convertResult.Stats.MinTimestamp,
		MaxTimestampMs:   convertResult.Stats.MaxTimestamp,
		ParquetPath:      parquetPath,
		ParquetSizeBytes: uint64(len(convertResult.ParquetData)),
	}

	swapper := compaction.NewIndexSwapper(metaStore)
	_, err = swapper.Swap(ctx, compaction.SwapRequest{
		StreamID:     streamID,
		WALIndexKeys: walIndexKeys,
		ParquetEntry: parquetEntry,
		MetaDomain:   0,
	})
	if err != nil {
		t.Fatalf("failed to swap index: %v", err)
	}
}
