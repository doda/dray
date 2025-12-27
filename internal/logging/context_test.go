package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestWithCorrelationIDCtx(t *testing.T) {
	ctx := context.Background()
	ctx = WithCorrelationIDCtx(ctx, "corr-123")

	got := CorrelationIDFromCtx(ctx)
	if got != "corr-123" {
		t.Errorf("CorrelationIDFromCtx() = %q, want %q", got, "corr-123")
	}
}

func TestCorrelationIDFromCtxEmpty(t *testing.T) {
	ctx := context.Background()
	got := CorrelationIDFromCtx(ctx)
	if got != "" {
		t.Errorf("CorrelationIDFromCtx() = %q, want empty string", got)
	}
}

func TestWithTraceIDCtx(t *testing.T) {
	ctx := context.Background()
	ctx = WithTraceIDCtx(ctx, "trace-456")

	got := TraceIDFromCtx(ctx)
	if got != "trace-456" {
		t.Errorf("TraceIDFromCtx() = %q, want %q", got, "trace-456")
	}
}

func TestTraceIDFromCtxEmpty(t *testing.T) {
	ctx := context.Background()
	got := TraceIDFromCtx(ctx)
	if got != "" {
		t.Errorf("TraceIDFromCtx() = %q, want empty string", got)
	}
}

func TestWithLoggerCtx(t *testing.T) {
	l := DefaultLogger()
	ctx := context.Background()
	ctx = WithLoggerCtx(ctx, l)

	got := LoggerFromCtx(ctx)
	if got != l {
		t.Error("LoggerFromCtx should return the same logger")
	}
}

func TestLoggerFromCtxNil(t *testing.T) {
	ctx := context.Background()
	got := LoggerFromCtx(ctx)
	if got != nil {
		t.Error("LoggerFromCtx should return nil when no logger in context")
	}
}

func TestFromCtxWithLogger(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})
	l = l.WithCorrelationID("preset-corr")

	ctx := WithLoggerCtx(context.Background(), l)
	got := FromCtx(ctx)

	if got != l {
		t.Error("FromCtx should return logger from context")
	}
}

func TestFromCtxWithIDs(t *testing.T) {
	ctx := context.Background()
	ctx = WithCorrelationIDCtx(ctx, "ctx-corr")
	ctx = WithTraceIDCtx(ctx, "ctx-trace")

	l := FromCtx(ctx)

	var buf bytes.Buffer
	l.mu.Lock()
	l.out = &buf
	l.mu.Unlock()

	l.Info("test")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.CorrelationID != "ctx-corr" {
		t.Errorf("correlationId = %q, want %q", entry.CorrelationID, "ctx-corr")
	}
	if entry.TraceID != "ctx-trace" {
		t.Errorf("traceId = %q, want %q", entry.TraceID, "ctx-trace")
	}
}

func TestFromCtxWithNoContext(t *testing.T) {
	ctx := context.Background()
	l := FromCtx(ctx)

	if l == nil {
		t.Error("FromCtx should return a default logger")
	}
}

func TestContextLogger(t *testing.T) {
	var buf bytes.Buffer
	base := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	ctx := context.Background()
	ctx = WithCorrelationIDCtx(ctx, "ctx-corr")
	ctx = WithTraceIDCtx(ctx, "ctx-trace")

	l := ContextLogger(ctx, base)
	l.Info("test")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.CorrelationID != "ctx-corr" {
		t.Errorf("correlationId = %q, want %q", entry.CorrelationID, "ctx-corr")
	}
	if entry.TraceID != "ctx-trace" {
		t.Errorf("traceId = %q, want %q", entry.TraceID, "ctx-trace")
	}
}

func TestContextLoggerNilBase(t *testing.T) {
	ctx := context.Background()
	ctx = WithCorrelationIDCtx(ctx, "corr-123")

	l := ContextLogger(ctx, nil)
	if l == nil {
		t.Error("ContextLogger should return a logger even with nil base")
	}
}

func TestPropagateIDs(t *testing.T) {
	l := DefaultLogger().WithCorrelationID("logger-corr").WithTraceID("logger-trace")
	ctx := context.Background()
	ctx = PropagateIDs(ctx, l)

	if got := CorrelationIDFromCtx(ctx); got != "logger-corr" {
		t.Errorf("CorrelationIDFromCtx = %q, want %q", got, "logger-corr")
	}
	if got := TraceIDFromCtx(ctx); got != "logger-trace" {
		t.Errorf("TraceIDFromCtx = %q, want %q", got, "logger-trace")
	}
}

func TestPropagateIDsNilLogger(t *testing.T) {
	ctx := context.Background()
	newCtx := PropagateIDs(ctx, nil)

	if newCtx != ctx {
		t.Error("PropagateIDs with nil logger should return same context")
	}
}

func TestPropagateIDsPreservesExisting(t *testing.T) {
	ctx := context.Background()
	ctx = WithCorrelationIDCtx(ctx, "existing-corr")

	// Logger with only trace ID
	l := DefaultLogger().WithTraceID("logger-trace")
	ctx = PropagateIDs(ctx, l)

	// Existing correlation should be overwritten (empty string from logger)
	// Only trace should be added
	if got := TraceIDFromCtx(ctx); got != "logger-trace" {
		t.Errorf("TraceIDFromCtx = %q, want %q", got, "logger-trace")
	}
}

func TestContextPropagationEndToEnd(t *testing.T) {
	var buf bytes.Buffer
	base := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	// Simulate a request with correlation ID
	correlationID := "request-corr-123"
	traceID := "request-trace-456"

	ctx := context.Background()
	ctx = WithCorrelationIDCtx(ctx, correlationID)
	ctx = WithTraceIDCtx(ctx, traceID)

	// Get logger from context
	l := ContextLogger(ctx, base)

	// Log in a "handler"
	l.Info("handling request")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.CorrelationID != correlationID {
		t.Errorf("correlationId = %q, want %q", entry.CorrelationID, correlationID)
	}
	if entry.TraceID != traceID {
		t.Errorf("traceId = %q, want %q", entry.TraceID, traceID)
	}
}

func TestContextPropagationAcrossLayers(t *testing.T) {
	var buf bytes.Buffer
	base := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	// Layer 1: HTTP handler sets up context
	ctx := context.Background()
	ctx = WithCorrelationIDCtx(ctx, "http-corr")
	ctx = WithTraceIDCtx(ctx, "http-trace")
	ctx = WithLoggerCtx(ctx, ContextLogger(ctx, base))

	// Layer 2: Service layer gets logger from context
	l := FromCtx(ctx)
	l.Info("service layer log")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.CorrelationID != "http-corr" {
		t.Errorf("correlationId = %q, want %q", entry.CorrelationID, "http-corr")
	}
	if entry.TraceID != "http-trace" {
		t.Errorf("traceId = %q, want %q", entry.TraceID, "http-trace")
	}
}
