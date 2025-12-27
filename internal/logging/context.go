package logging

import (
	"context"
)

// contextKey is a type for context keys to avoid collisions.
type contextKey int

const (
	correlationIDKey contextKey = iota
	traceIDKey
	loggerKey
)

// WithCorrelationIDCtx returns a new context with the correlation ID set.
func WithCorrelationIDCtx(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationIDFromCtx extracts the correlation ID from the context.
func CorrelationIDFromCtx(ctx context.Context) string {
	if id, ok := ctx.Value(correlationIDKey).(string); ok {
		return id
	}
	return ""
}

// WithTraceIDCtx returns a new context with the trace ID set.
func WithTraceIDCtx(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey, id)
}

// TraceIDFromCtx extracts the trace ID from the context.
func TraceIDFromCtx(ctx context.Context) string {
	if id, ok := ctx.Value(traceIDKey).(string); ok {
		return id
	}
	return ""
}

// WithLoggerCtx returns a new context with the logger attached.
func WithLoggerCtx(ctx context.Context, l *Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// FromCtx returns a logger from the context. If none is found, returns
// a logger configured from the context's correlation and trace IDs using
// the global logger.
func FromCtx(ctx context.Context) *Logger {
	if l, ok := ctx.Value(loggerKey).(*Logger); ok {
		return l
	}

	// Build a logger with IDs from context
	l := Global()
	if id := CorrelationIDFromCtx(ctx); id != "" {
		l = l.WithCorrelationID(id)
	}
	if id := TraceIDFromCtx(ctx); id != "" {
		l = l.WithTraceID(id)
	}
	return l
}

// LoggerFromCtx returns the logger from context, or nil if not set.
func LoggerFromCtx(ctx context.Context) *Logger {
	l, _ := ctx.Value(loggerKey).(*Logger)
	return l
}

// ContextLogger returns a logger configured with any correlation and trace IDs
// from the context. If a logger is already in the context, it returns that
// logger updated with any additional IDs from the context.
func ContextLogger(ctx context.Context, base *Logger) *Logger {
	l := LoggerFromCtx(ctx)
	if l == nil {
		l = base
	}
	if l == nil {
		l = Global()
	}

	correlationID := CorrelationIDFromCtx(ctx)
	traceID := TraceIDFromCtx(ctx)

	if correlationID != "" {
		l = l.WithCorrelationID(correlationID)
	}
	if traceID != "" {
		l = l.WithTraceID(traceID)
	}

	return l
}

// PropagateIDs returns a new context with correlation and trace IDs propagated
// from the logger to the context.
func PropagateIDs(ctx context.Context, l *Logger) context.Context {
	if l == nil {
		return ctx
	}

	l.mu.Lock()
	correlationID := l.correlationID
	traceID := l.traceID
	l.mu.Unlock()

	if correlationID != "" {
		ctx = WithCorrelationIDCtx(ctx, correlationID)
	}
	if traceID != "" {
		ctx = WithTraceIDCtx(ctx, traceID)
	}
	return ctx
}
