package produce

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/wal"
)

func TestNewBuffer(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := BufferConfig{
			MaxBufferBytes: 1024 * 1024,
			FlushSizeBytes: 64 * 1024,
			LingerMs:       100,
			NumDomains:     16,
			OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
				return nil
			},
		}
		buf := NewBuffer(cfg)
		if buf == nil {
			t.Fatal("expected buffer, got nil")
		}
	})

	t.Run("panics on zero NumDomains", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for NumDomains <= 0")
			}
		}()
		NewBuffer(BufferConfig{
			MaxBufferBytes: 1024,
			FlushSizeBytes: 512,
			NumDomains:     0,
			OnFlush:        func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error { return nil },
		})
	})

	t.Run("panics on zero MaxBufferBytes", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for MaxBufferBytes <= 0")
			}
		}()
		NewBuffer(BufferConfig{
			MaxBufferBytes: 0,
			FlushSizeBytes: 512,
			NumDomains:     16,
			OnFlush:        func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error { return nil },
		})
	})

	t.Run("panics on zero FlushSizeBytes", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for FlushSizeBytes <= 0")
			}
		}()
		NewBuffer(BufferConfig{
			MaxBufferBytes: 1024,
			FlushSizeBytes: 0,
			NumDomains:     16,
			OnFlush:        func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error { return nil },
		})
	})

	t.Run("panics on nil OnFlush", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for nil OnFlush")
			}
		}()
		NewBuffer(BufferConfig{
			MaxBufferBytes: 1024,
			FlushSizeBytes: 512,
			NumDomains:     16,
			OnFlush:        nil,
		})
	})
}

func TestBufferAddAndPending(t *testing.T) {
	var flushed atomic.Int32

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 64 * 1024, // Large threshold - won't trigger flush
		LingerMs:       0,         // No linger - manual flush only
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			flushed.Add(int32(len(requests)))
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	batches := []wal.BatchEntry{{Data: []byte("test data")}}

	// Add a request
	req, err := buf.Add(context.Background(), "stream-1", batches, 1, 1000, 2000)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if req == nil {
		t.Fatal("expected pending request")
	}

	// Check stats
	stats := buf.Stats()
	if stats.PendingCount != 1 {
		t.Errorf("expected 1 pending, got %d", stats.PendingCount)
	}
	if stats.TotalBytes != int64(len(batches[0].Data)) {
		t.Errorf("expected %d bytes, got %d", len(batches[0].Data), stats.TotalBytes)
	}

	// Manually flush
	buf.Flush(context.Background())

	// Wait for completion
	select {
	case <-req.Done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("request did not complete")
	}

	if req.Err != nil {
		t.Errorf("unexpected error: %v", req.Err)
	}

	if flushed.Load() != 1 {
		t.Errorf("expected 1 flush, got %d", flushed.Load())
	}
}

func TestBufferMetaDomainPartitioning(t *testing.T) {
	domainRequests := make(map[metadata.MetaDomain]int)
	var mu sync.Mutex

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 64 * 1024,
		LingerMs:       0,
		NumDomains:     4, // Small number to increase collision chance
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			mu.Lock()
			domainRequests[domain] += len(requests)
			mu.Unlock()
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	// Add requests for different streams
	streams := []string{"stream-a", "stream-b", "stream-c", "stream-d", "stream-e"}
	for _, s := range streams {
		_, err := buf.Add(context.Background(), s, []wal.BatchEntry{{Data: []byte("data")}}, 1, 0, 0)
		if err != nil {
			t.Fatalf("Add failed for %s: %v", s, err)
		}
	}

	// Flush all
	buf.Flush(context.Background())

	// Verify all requests were flushed
	mu.Lock()
	total := 0
	for _, count := range domainRequests {
		total += count
	}
	mu.Unlock()

	if total != len(streams) {
		t.Errorf("expected %d total flushed, got %d", len(streams), total)
	}
}

func TestBufferSizeThresholdFlush(t *testing.T) {
	flushCh := make(chan struct{}, 10)

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 100, // Small threshold
		LingerMs:       0,
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			flushCh <- struct{}{}
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	// Add data that exceeds threshold
	largeData := make([]byte, 150) // Exceeds 100 byte threshold
	_, err := buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: largeData}}, 1, 0, 0)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Should trigger automatic flush
	select {
	case <-flushCh:
		// Success - flush was triggered
	case <-time.After(time.Second):
		t.Error("expected automatic flush on size threshold")
	}
}

func TestBufferLingerTimeout(t *testing.T) {
	flushCh := make(chan struct{}, 10)

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1024 * 1024, // Very large - won't trigger on size
		LingerMs:       50,          // 50ms linger
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			flushCh <- struct{}{}
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	// Add small data that won't trigger size threshold
	_, err := buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: []byte("x")}}, 1, 0, 0)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Should trigger linger flush after ~50ms
	select {
	case <-flushCh:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Error("expected linger flush")
	}
}

func TestBufferMaxBufferBytesBlocking(t *testing.T) {
	flushCh := make(chan struct{})
	flushDone := make(chan struct{})

	cfg := BufferConfig{
		MaxBufferBytes: 100, // Very small
		FlushSizeBytes: 50,
		LingerMs:       0,
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			<-flushCh // Wait for signal to complete
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	// First add should succeed
	_, err := buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: make([]byte, 60)}}, 1, 0, 0)
	if err != nil {
		t.Fatalf("First Add failed: %v", err)
	}

	// Second add should block because buffer is full
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		_, err := buf.Add(ctx, "stream-1", []wal.BatchEntry{{Data: make([]byte, 60)}}, 1, 0, 0)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
		close(flushDone)
	}()

	// Wait for the add to timeout
	select {
	case <-flushDone:
		// Good - the add was blocked and timed out
	case <-time.After(time.Second):
		t.Error("Add did not timeout as expected")
	}

	// Cleanup
	close(flushCh)
}

func TestBufferClose(t *testing.T) {
	var flushed atomic.Int32

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 64 * 1024,
		LingerMs:       0,
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			flushed.Add(int32(len(requests)))
			return nil
		},
	}
	buf := NewBuffer(cfg)

	// Add some requests
	for i := 0; i < 5; i++ {
		_, err := buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: []byte("test")}}, 1, 0, 0)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	// Close should flush pending
	err := buf.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if flushed.Load() != 5 {
		t.Errorf("expected 5 flushed on close, got %d", flushed.Load())
	}

	// Add after close should fail
	_, err = buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: []byte("test")}}, 1, 0, 0)
	if err != ErrBufferClosed {
		t.Errorf("expected ErrBufferClosed, got %v", err)
	}
}

func TestBufferConcurrentAdds(t *testing.T) {
	var totalFlushed atomic.Int32

	cfg := BufferConfig{
		MaxBufferBytes: 10 * 1024 * 1024,
		FlushSizeBytes: 1024,
		LingerMs:       10,
		NumDomains:     8,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			totalFlushed.Add(int32(len(requests)))
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	// Concurrent adds
	var wg sync.WaitGroup
	numGoroutines := 10
	requestsPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < requestsPerGoroutine; i++ {
				streamID := "stream-" + string(rune('a'+id%26))
				_, err := buf.Add(context.Background(), streamID, []wal.BatchEntry{{Data: []byte("data")}}, 1, 0, 0)
				if err != nil {
					t.Errorf("Add failed: %v", err)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Flush remaining
	buf.Flush(context.Background())

	expected := int32(numGoroutines * requestsPerGoroutine)
	if totalFlushed.Load() != expected {
		t.Errorf("expected %d flushed, got %d", expected, totalFlushed.Load())
	}
}

func TestBufferStats(t *testing.T) {
	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 64 * 1024,
		LingerMs:       0,
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	// Initial stats
	stats := buf.Stats()
	if stats.TotalBytes != 0 {
		t.Errorf("expected 0 bytes initially, got %d", stats.TotalBytes)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending initially, got %d", stats.PendingCount)
	}
	if stats.MaxBufferSize != 1024*1024 {
		t.Errorf("expected max buffer size 1048576, got %d", stats.MaxBufferSize)
	}

	// Add some data
	data := make([]byte, 100)
	_, _ = buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: data}}, 1, 0, 0)
	_, _ = buf.Add(context.Background(), "stream-2", []wal.BatchEntry{{Data: data}}, 1, 0, 0)

	stats = buf.Stats()
	if stats.TotalBytes != 200 {
		t.Errorf("expected 200 bytes, got %d", stats.TotalBytes)
	}
	if stats.PendingCount != 2 {
		t.Errorf("expected 2 pending, got %d", stats.PendingCount)
	}
}

func TestBufferMultipleStreamsFlushTogether(t *testing.T) {
	// Verify that multiple streams in the same domain are flushed together
	var flushedRequests []*PendingRequest
	var mu sync.Mutex

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 64 * 1024,
		LingerMs:       0,
		NumDomains:     1, // Single domain - all streams go to same buffer
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			mu.Lock()
			flushedRequests = append(flushedRequests, requests...)
			mu.Unlock()
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	// Add multiple streams
	_, _ = buf.Add(context.Background(), "stream-a", []wal.BatchEntry{{Data: []byte("a")}}, 1, 100, 100)
	_, _ = buf.Add(context.Background(), "stream-b", []wal.BatchEntry{{Data: []byte("b")}}, 2, 200, 200)
	_, _ = buf.Add(context.Background(), "stream-c", []wal.BatchEntry{{Data: []byte("c")}}, 3, 300, 300)

	buf.Flush(context.Background())

	mu.Lock()
	count := len(flushedRequests)
	mu.Unlock()

	if count != 3 {
		t.Errorf("expected 3 requests flushed together, got %d", count)
	}
}

func TestBufferLingerDoesNotResetOnNewRequest(t *testing.T) {
	flushCh := make(chan time.Time, 10)

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 1024 * 1024,
		LingerMs:       100, // 100ms linger
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			flushCh <- time.Now()
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	start := time.Now()

	// First request starts linger timer
	_, _ = buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: []byte("x")}}, 1, 0, 0)

	// Wait 50ms then add another request to same domain
	time.Sleep(50 * time.Millisecond)
	_, _ = buf.Add(context.Background(), "stream-1", []wal.BatchEntry{{Data: []byte("y")}}, 1, 0, 0)

	// The flush should happen at ~100ms from start, not reset to 100ms from second add
	select {
	case flushTime := <-flushCh:
		elapsed := flushTime.Sub(start)
		if elapsed > 150*time.Millisecond {
			t.Errorf("linger timer was reset - elapsed %v, expected ~100ms", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("no flush received")
	}
}

func TestPendingRequestFields(t *testing.T) {
	var capturedReq *PendingRequest

	cfg := BufferConfig{
		MaxBufferBytes: 1024 * 1024,
		FlushSizeBytes: 64 * 1024,
		LingerMs:       0,
		NumDomains:     16,
		OnFlush: func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error {
			if len(requests) > 0 {
				capturedReq = requests[0]
			}
			return nil
		},
	}
	buf := NewBuffer(cfg)
	defer buf.Close()

	batches := []wal.BatchEntry{
		{Data: []byte("batch1")},
		{Data: []byte("batch2")},
	}

	_, _ = buf.Add(context.Background(), "test-stream", batches, 10, 1000, 2000)
	buf.Flush(context.Background())

	if capturedReq == nil {
		t.Fatal("request not captured")
	}

	if capturedReq.RecordCount != 10 {
		t.Errorf("expected RecordCount 10, got %d", capturedReq.RecordCount)
	}
	if capturedReq.MinTimestampMs != 1000 {
		t.Errorf("expected MinTimestampMs 1000, got %d", capturedReq.MinTimestampMs)
	}
	if capturedReq.MaxTimestampMs != 2000 {
		t.Errorf("expected MaxTimestampMs 2000, got %d", capturedReq.MaxTimestampMs)
	}
	expectedSize := int64(len("batch1") + len("batch2"))
	if capturedReq.Size != expectedSize {
		t.Errorf("expected Size %d, got %d", expectedSize, capturedReq.Size)
	}
	if len(capturedReq.Batches) != 2 {
		t.Errorf("expected 2 batches, got %d", len(capturedReq.Batches))
	}
}

func TestParseStreamID(t *testing.T) {
	// Test that same stream ID always produces same result
	id1 := parseStreamID("test-stream")
	id2 := parseStreamID("test-stream")
	if id1 != id2 {
		t.Error("parseStreamID should be deterministic")
	}

	// Different streams should (usually) produce different IDs
	id3 := parseStreamID("other-stream")
	if id1 == id3 {
		t.Log("warning: different streams produced same ID (collision)")
	}
}
