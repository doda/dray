// Package logging provides structured logging with correlation ID propagation.
package logging

import (
	"encoding/json"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

// Level represents the severity of a log message.
type Level int

const (
	// LevelDebug is for detailed debugging information.
	LevelDebug Level = iota
	// LevelInfo is for general information messages.
	LevelInfo
	// LevelWarn is for warning messages.
	LevelWarn
	// LevelError is for error messages.
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

// ParseLevel converts a string to a Level.
func ParseLevel(s string) Level {
	switch s {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// Format represents the output format for log messages.
type Format int

const (
	// FormatJSON outputs logs as JSON objects.
	FormatJSON Format = iota
	// FormatText outputs logs as human-readable text.
	FormatText
)

// ParseFormat converts a string to a Format.
func ParseFormat(s string) Format {
	switch s {
	case "json":
		return FormatJSON
	case "text":
		return FormatText
	default:
		return FormatJSON
	}
}

// Entry represents a single log entry.
type Entry struct {
	Timestamp     time.Time         `json:"timestamp"`
	Level         string            `json:"level"`
	Message       string            `json:"message"`
	CorrelationID string            `json:"correlationId,omitempty"`
	TraceID       string            `json:"traceId,omitempty"`
	File          string            `json:"file,omitempty"`
	Line          int               `json:"line,omitempty"`
	Fields        map[string]any    `json:"fields,omitempty"`
}

// Logger provides structured logging with configurable levels and formats.
type Logger struct {
	mu            sync.Mutex
	out           io.Writer
	level         Level
	format        Format
	addCaller     bool
	callerSkip    int
	fields        map[string]any
	correlationID string
	traceID       string
}

// Config holds configuration for a Logger.
type Config struct {
	Level      Level
	Format     Format
	Output     io.Writer
	AddCaller  bool
	CallerSkip int
}

// New creates a new Logger with the given configuration.
func New(cfg Config) *Logger {
	out := cfg.Output
	if out == nil {
		out = os.Stderr
	}
	return &Logger{
		out:        out,
		level:      cfg.Level,
		format:     cfg.Format,
		addCaller:  cfg.AddCaller,
		callerSkip: cfg.CallerSkip,
		fields:     make(map[string]any),
	}
}

// DefaultLogger returns a logger with default settings.
func DefaultLogger() *Logger {
	return New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: os.Stderr,
	})
}

// SetLevel updates the minimum logging level.
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current logging level.
func (l *Logger) GetLevel() Level {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// SetFormat updates the output format.
func (l *Logger) SetFormat(format Format) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.format = format
}

// SetAddCaller enables or disables caller info (file/line).
func (l *Logger) SetAddCaller(add bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.addCaller = add
}

// With returns a new Logger with the given fields added.
func (l *Logger) With(fields map[string]any) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newFields := make(map[string]any, len(l.fields)+len(fields))
	for k, v := range l.fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}

	return &Logger{
		out:           l.out,
		level:         l.level,
		format:        l.format,
		addCaller:     l.addCaller,
		callerSkip:    l.callerSkip,
		fields:        newFields,
		correlationID: l.correlationID,
		traceID:       l.traceID,
	}
}

// WithCorrelationID returns a new Logger with the correlation ID set.
func (l *Logger) WithCorrelationID(id string) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newFields := make(map[string]any, len(l.fields))
	for k, v := range l.fields {
		newFields[k] = v
	}

	return &Logger{
		out:           l.out,
		level:         l.level,
		format:        l.format,
		addCaller:     l.addCaller,
		callerSkip:    l.callerSkip,
		fields:        newFields,
		correlationID: id,
		traceID:       l.traceID,
	}
}

// WithTraceID returns a new Logger with the trace ID set.
func (l *Logger) WithTraceID(id string) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()

	newFields := make(map[string]any, len(l.fields))
	for k, v := range l.fields {
		newFields[k] = v
	}

	return &Logger{
		out:           l.out,
		level:         l.level,
		format:        l.format,
		addCaller:     l.addCaller,
		callerSkip:    l.callerSkip,
		fields:        newFields,
		correlationID: l.correlationID,
		traceID:       id,
	}
}

// Debug logs a debug message.
func (l *Logger) Debug(msg string) {
	l.log(LevelDebug, msg, nil)
}

// Debugf logs a debug message with fields.
func (l *Logger) Debugf(msg string, fields map[string]any) {
	l.log(LevelDebug, msg, fields)
}

// Info logs an info message.
func (l *Logger) Info(msg string) {
	l.log(LevelInfo, msg, nil)
}

// Infof logs an info message with fields.
func (l *Logger) Infof(msg string, fields map[string]any) {
	l.log(LevelInfo, msg, fields)
}

// Warn logs a warning message.
func (l *Logger) Warn(msg string) {
	l.log(LevelWarn, msg, nil)
}

// Warnf logs a warning message with fields.
func (l *Logger) Warnf(msg string, fields map[string]any) {
	l.log(LevelWarn, msg, fields)
}

// Error logs an error message.
func (l *Logger) Error(msg string) {
	l.log(LevelError, msg, nil)
}

// Errorf logs an error message with fields.
func (l *Logger) Errorf(msg string, fields map[string]any) {
	l.log(LevelError, msg, fields)
}

func (l *Logger) log(level Level, msg string, extraFields map[string]any) {
	l.mu.Lock()
	currentLevel := l.level
	format := l.format
	addCaller := l.addCaller
	callerSkip := l.callerSkip
	correlationID := l.correlationID
	traceID := l.traceID
	fields := l.fields
	out := l.out
	l.mu.Unlock()

	if level < currentLevel {
		return
	}

	entry := Entry{
		Timestamp:     time.Now().UTC(),
		Level:         level.String(),
		Message:       msg,
		CorrelationID: correlationID,
		TraceID:       traceID,
	}

	if addCaller {
		_, file, line, ok := runtime.Caller(2 + callerSkip)
		if ok {
			entry.File = file
			entry.Line = line
		}
	}

	if len(fields) > 0 || len(extraFields) > 0 {
		entry.Fields = make(map[string]any, len(fields)+len(extraFields))
		for k, v := range fields {
			entry.Fields[k] = v
		}
		for k, v := range extraFields {
			entry.Fields[k] = v
		}
	}

	var data []byte
	switch format {
	case FormatJSON:
		data, _ = json.Marshal(entry)
		data = append(data, '\n')
	case FormatText:
		data = formatText(entry)
	}

	l.mu.Lock()
	_, _ = out.Write(data)
	l.mu.Unlock()
}

func formatText(e Entry) []byte {
	buf := make([]byte, 0, 256)
	buf = append(buf, e.Timestamp.Format(time.RFC3339)...)
	buf = append(buf, ' ')
	buf = append(buf, '[')
	buf = append(buf, e.Level...)
	buf = append(buf, ']')
	buf = append(buf, ' ')
	buf = append(buf, e.Message...)

	if e.CorrelationID != "" {
		buf = append(buf, " correlationId="...)
		buf = append(buf, e.CorrelationID...)
	}
	if e.TraceID != "" {
		buf = append(buf, " traceId="...)
		buf = append(buf, e.TraceID...)
	}
	if e.File != "" {
		buf = append(buf, " file="...)
		buf = append(buf, e.File...)
		buf = append(buf, ':')
		buf = append(buf, itoa(e.Line)...)
	}
	for k, v := range e.Fields {
		buf = append(buf, ' ')
		buf = append(buf, k...)
		buf = append(buf, '=')
		switch val := v.(type) {
		case string:
			buf = append(buf, val...)
		default:
			data, _ := json.Marshal(v)
			buf = append(buf, data...)
		}
	}
	buf = append(buf, '\n')
	return buf
}

func itoa(i int) string {
	if i < 10 {
		return string(rune('0' + i))
	}
	// Simple int-to-string for line numbers
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
