// Package produce implements produce buffering and commit logic.
// Buffers records by MetaDomain and commits to WAL + metadata atomically.
package produce

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dray-io/dray/internal/logging"
	"github.com/dray-io/dray/internal/metadata"
	"github.com/dray-io/dray/internal/wal"
)

// Common errors returned by Buffer operations.
var (
	// ErrBufferFull is returned when the buffer has reached max_buffer_bytes.
	ErrBufferFull = errors.New("produce: buffer full")

	// ErrBufferClosed is returned when operating on a closed buffer.
	ErrBufferClosed = errors.New("produce: buffer closed")
)

// PendingRequest represents a produce request waiting for flush completion.
type PendingRequest struct {
	// StreamID is the hashed stream identifier for WAL encoding.
	StreamID uint64

	// StreamIDStr is the original string stream ID for metadata lookups.
	StreamIDStr string

	// Batches are the Kafka record batches to write.
	Batches []wal.BatchEntry

	// RecordCount is the total number of records across all batches.
	RecordCount uint32

	// MinTimestampMs is the minimum timestamp across batches.
	MinTimestampMs int64

	// MaxTimestampMs is the maximum timestamp across batches.
	MaxTimestampMs int64

	// Size is the total size in bytes of all batches.
	Size int64

	// Done is closed when the request has been processed (flushed or failed).
	Done chan struct{}

	// Err contains the error if the request failed.
	Err error

	// Result contains the assigned offset information after successful flush.
	Result *RequestResult

	// Deadline is the request context deadline, if any.
	Deadline time.Time
	// HasDeadline indicates whether Deadline is set.
	HasDeadline bool
}

// RequestResult contains offset information for a successfully flushed request.
type RequestResult struct {
	// StartOffset is the first assigned offset for this request's records.
	StartOffset int64

	// EndOffset is the exclusive end offset (startOffset + recordCount).
	EndOffset int64
}

// domainBuffer is a buffer for a single MetaDomain.
type domainBuffer struct {
	mu       sync.Mutex
	domain   metadata.MetaDomain
	requests []*PendingRequest
	size     int64 // total bytes buffered
}

// BufferConfig configures the produce buffer.
type BufferConfig struct {
	// MaxBufferBytes is the maximum total size in bytes across all domain buffers.
	// When exceeded, new produce requests will block until space is available.
	MaxBufferBytes int64

	// FlushSizeBytes is the size threshold that triggers a flush.
	// When a domain buffer exceeds this size, it will be flushed.
	FlushSizeBytes int64

	// LingerMs is the maximum time to wait before flushing.
	// If > 0, buffers will be flushed after this duration even if not full.
	LingerMs int64

	// NumDomains is the number of MetaDomains (from config).
	NumDomains int

	// OnFlush is called when a buffer needs to be flushed.
	// It receives the domain and the pending requests to flush.
	// Returns results per request or an error.
	OnFlush FlushHandler
}

// FlushHandler processes a batch of pending requests for a domain.
// It is responsible for writing to WAL and committing metadata.
type FlushHandler func(ctx context.Context, domain metadata.MetaDomain, requests []*PendingRequest) error

// Buffer manages produce request buffering partitioned by MetaDomain.
// It bounds total buffer size and triggers flushes on size or linger thresholds.
type Buffer struct {
	config           BufferConfig
	domainCalculator *metadata.DomainCalculator

	mu      sync.RWMutex
	buffers map[metadata.MetaDomain]*domainBuffer
	closed  bool

	// lingerTimers tracks active linger timers per domain
	lingerTimers map[metadata.MetaDomain]*time.Timer
	timerMu      sync.Mutex

	// totalSize tracks the total bytes across all domain buffers
	totalSize int64
	sizeMu    sync.Mutex
	sizeCond  *sync.Cond
}

// NewBuffer creates a new produce buffer with the given configuration.
func NewBuffer(cfg BufferConfig) *Buffer {
	if cfg.NumDomains <= 0 {
		panic("produce: NumDomains must be positive")
	}
	if cfg.MaxBufferBytes <= 0 {
		panic("produce: MaxBufferBytes must be positive")
	}
	if cfg.FlushSizeBytes <= 0 {
		panic("produce: FlushSizeBytes must be positive")
	}
	if cfg.OnFlush == nil {
		panic("produce: OnFlush handler is required")
	}

	b := &Buffer{
		config:           cfg,
		domainCalculator: metadata.NewDomainCalculator(cfg.NumDomains),
		buffers:          make(map[metadata.MetaDomain]*domainBuffer),
		lingerTimers:     make(map[metadata.MetaDomain]*time.Timer),
	}
	b.sizeCond = sync.NewCond(&b.sizeMu)
	return b
}

// Add adds a produce request to the appropriate domain buffer.
// It may block if the total buffer size exceeds MaxBufferBytes.
// Returns the PendingRequest that can be waited on for completion.
func (b *Buffer) Add(ctx context.Context, streamID string, batches []wal.BatchEntry, recordCount uint32, minTs, maxTs int64) (*PendingRequest, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrBufferClosed
	}
	b.mu.RUnlock()

	// Calculate total size
	var size int64
	for _, batch := range batches {
		size += int64(len(batch.Data))
	}

	// Wait for space if buffer is full
	if err := b.waitForSpace(ctx, size); err != nil {
		return nil, err
	}

	// Calculate domain and get/create buffer
	domain := b.domainCalculator.Calculate(streamID)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrBufferClosed
	}

	db, ok := b.buffers[domain]
	if !ok {
		db = &domainBuffer{
			domain:   domain,
			requests: make([]*PendingRequest, 0),
		}
		b.buffers[domain] = db
	}
	b.mu.Unlock()

	// Create the pending request
	deadline, hasDeadline := ctx.Deadline()
	req := &PendingRequest{
		StreamID:       parseStreamID(streamID),
		StreamIDStr:    streamID,
		Batches:        batches,
		RecordCount:    recordCount,
		MinTimestampMs: minTs,
		MaxTimestampMs: maxTs,
		Size:           size,
		Done:           make(chan struct{}),
		Deadline:       deadline,
		HasDeadline:    hasDeadline,
	}

	// Add to domain buffer
	db.mu.Lock()
	db.requests = append(db.requests, req)
	db.size += size
	domainSize := db.size
	db.mu.Unlock()

	// Update total size
	b.sizeMu.Lock()
	b.totalSize += size
	b.sizeMu.Unlock()

	// Check if we should flush (size threshold exceeded)
	if domainSize >= b.config.FlushSizeBytes {
		go b.flushDomain(context.Background(), domain)
	} else if b.config.LingerMs > 0 {
		// Start/reset linger timer if not already running
		b.startLingerTimer(domain)
	}

	return req, nil
}

// waitForSpace blocks until there's space in the buffer or context is canceled.
func (b *Buffer) waitForSpace(ctx context.Context, size int64) error {
	b.sizeMu.Lock()
	defer b.sizeMu.Unlock()

	if size > b.config.MaxBufferBytes {
		return ErrBufferFull
	}

	for b.totalSize+size > b.config.MaxBufferBytes {
		// Check context before waiting
		if err := ctx.Err(); err != nil {
			return err
		}

		// Check if buffer is closed
		b.mu.RLock()
		closed := b.closed
		b.mu.RUnlock()
		if closed {
			return ErrBufferClosed
		}

		// Wait for space - use a goroutine to wake on context cancellation
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				b.sizeCond.Broadcast()
			case <-done:
			}
		}()

		b.sizeCond.Wait()
		close(done)
	}
	return nil
}

// startLingerTimer starts or resets the linger timer for a domain.
func (b *Buffer) startLingerTimer(domain metadata.MetaDomain) {
	b.timerMu.Lock()
	defer b.timerMu.Unlock()

	// If timer already exists for this domain, don't reset
	if _, ok := b.lingerTimers[domain]; ok {
		return
	}

	timer := time.AfterFunc(time.Duration(b.config.LingerMs)*time.Millisecond, func() {
		b.timerMu.Lock()
		delete(b.lingerTimers, domain)
		b.timerMu.Unlock()

		b.flushDomain(context.Background(), domain)
	})
	b.lingerTimers[domain] = timer
}

// cancelLingerTimer cancels any pending linger timer for a domain.
func (b *Buffer) cancelLingerTimer(domain metadata.MetaDomain) {
	b.timerMu.Lock()
	defer b.timerMu.Unlock()

	if timer, ok := b.lingerTimers[domain]; ok {
		timer.Stop()
		delete(b.lingerTimers, domain)
	}
}

// flushDomain flushes all pending requests for a domain.
func (b *Buffer) flushDomain(ctx context.Context, domain metadata.MetaDomain) {
	// Cancel linger timer
	b.cancelLingerTimer(domain)

	// Get the domain buffer
	b.mu.RLock()
	db, ok := b.buffers[domain]
	b.mu.RUnlock()

	if !ok {
		return
	}

	// Extract pending requests
	db.mu.Lock()
	if len(db.requests) == 0 {
		db.mu.Unlock()
		return
	}

	requests := db.requests
	size := db.size
	db.requests = make([]*PendingRequest, 0)
	db.size = 0
	db.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	flushCtx := ctx
	if deadline, ok := earliestDeadline(requests); ok {
		if existing, ok := ctx.Deadline(); !ok || deadline.Before(existing) {
			var cancel context.CancelFunc
			flushCtx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
	}

	// Call the flush handler first - space is only freed after flush completes
	err := b.config.OnFlush(flushCtx, domain, requests)
	if err != nil {
		logging.FromCtx(flushCtx).Warnf("produce flush failed", map[string]any{
			"domain":     domain,
			"requests":   len(requests),
			"sizeBytes":  size,
			"error":      err.Error(),
			"hasTimeout": flushCtx.Err() != nil,
		})
	}

	// Update total size after flush completes
	b.sizeMu.Lock()
	b.totalSize -= size
	b.sizeMu.Unlock()

	// Signal that space is available
	b.sizeCond.Broadcast()

	// Complete all pending requests
	for _, req := range requests {
		if err != nil {
			req.Err = err
		}
		close(req.Done)
	}
}

func earliestDeadline(requests []*PendingRequest) (time.Time, bool) {
	var earliest time.Time
	for _, req := range requests {
		if !req.HasDeadline {
			continue
		}
		if earliest.IsZero() || req.Deadline.Before(earliest) {
			earliest = req.Deadline
		}
	}
	if earliest.IsZero() {
		return time.Time{}, false
	}
	return earliest, true
}

// Flush forces a flush of all domain buffers.
func (b *Buffer) Flush(ctx context.Context) {
	b.mu.RLock()
	domains := make([]metadata.MetaDomain, 0, len(b.buffers))
	for domain := range b.buffers {
		domains = append(domains, domain)
	}
	b.mu.RUnlock()

	var wg sync.WaitGroup
	for _, domain := range domains {
		wg.Add(1)
		go func(d metadata.MetaDomain) {
			defer wg.Done()
			b.flushDomain(ctx, d)
		}(domain)
	}
	wg.Wait()
}

// Close closes the buffer, flushing all pending requests.
func (b *Buffer) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	// Cancel all linger timers
	b.timerMu.Lock()
	for _, timer := range b.lingerTimers {
		timer.Stop()
	}
	b.lingerTimers = nil
	b.timerMu.Unlock()

	// Flush all pending requests
	b.Flush(context.Background())

	// Wake up any blocked waiters
	b.sizeCond.Broadcast()

	return nil
}

// Stats returns current buffer statistics.
func (b *Buffer) Stats() BufferStats {
	b.sizeMu.Lock()
	totalSize := b.totalSize
	b.sizeMu.Unlock()

	b.mu.RLock()
	domainCount := len(b.buffers)
	var totalRequests int
	for _, db := range b.buffers {
		db.mu.Lock()
		totalRequests += len(db.requests)
		db.mu.Unlock()
	}
	b.mu.RUnlock()

	return BufferStats{
		TotalBytes:    totalSize,
		DomainCount:   domainCount,
		PendingCount:  totalRequests,
		MaxBufferSize: b.config.MaxBufferBytes,
	}
}

// BufferStats contains buffer statistics.
type BufferStats struct {
	// TotalBytes is the total size of all buffered data.
	TotalBytes int64

	// DomainCount is the number of domains with buffered data.
	DomainCount int

	// PendingCount is the total number of pending requests.
	PendingCount int

	// MaxBufferSize is the configured maximum buffer size.
	MaxBufferSize int64
}

// parseStreamID converts a string stream ID to uint64.
// In production this would use proper stream ID parsing.
func parseStreamID(streamID string) uint64 {
	// Simple hash for now - in production this would parse the UUID
	var h uint64
	for _, c := range streamID {
		h = h*31 + uint64(c)
	}
	return h
}
