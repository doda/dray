package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestParseLevelValid(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"info", LevelInfo},
		{"warn", LevelWarn},
		{"error", LevelError},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := ParseLevel(tc.input)
			if got != tc.expected {
				t.Errorf("ParseLevel(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestParseLevelInvalid(t *testing.T) {
	got := ParseLevel("invalid")
	if got != LevelInfo {
		t.Errorf("ParseLevel(\"invalid\") = %v, want %v (default)", got, LevelInfo)
	}
}

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "debug"},
		{LevelInfo, "info"},
		{LevelWarn, "warn"},
		{LevelError, "error"},
		{Level(99), "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			if got := tc.level.String(); got != tc.expected {
				t.Errorf("Level.String() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected Format
	}{
		{"json", FormatJSON},
		{"text", FormatText},
		{"invalid", FormatJSON}, // default
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := ParseFormat(tc.input)
			if got != tc.expected {
				t.Errorf("ParseFormat(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestLoggerJSONOutput(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Info("test message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON log output: %v", err)
	}

	if entry.Message != "test message" {
		t.Errorf("message = %q, want %q", entry.Message, "test message")
	}
	if entry.Level != "info" {
		t.Errorf("level = %q, want %q", entry.Level, "info")
	}
	if entry.Timestamp.IsZero() {
		t.Error("timestamp should not be zero")
	}
}

func TestLoggerTimestampPresent(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Info("test")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Timestamp.IsZero() {
		t.Error("expected timestamp to be present")
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelWarn,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Debug("debug msg")
	l.Info("info msg")
	if buf.Len() > 0 {
		t.Error("debug/info should be filtered at warn level")
	}

	l.Warn("warn msg")
	if buf.Len() == 0 {
		t.Error("warn should be logged at warn level")
	}
}

func TestLoggerSetLevel(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelError,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Info("should not appear")
	if buf.Len() > 0 {
		t.Error("info should be filtered at error level")
	}

	l.SetLevel(LevelInfo)
	l.Info("should appear")
	if buf.Len() == 0 {
		t.Error("info should be logged after SetLevel(Info)")
	}
}

func TestLoggerGetLevel(t *testing.T) {
	l := New(Config{Level: LevelDebug})
	if got := l.GetLevel(); got != LevelDebug {
		t.Errorf("GetLevel() = %v, want %v", got, LevelDebug)
	}
}

func TestLoggerWithFields(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l2 := l.With(map[string]any{"key": "value"})
	l2.Info("with fields")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Fields["key"] != "value" {
		t.Errorf("fields[key] = %v, want %q", entry.Fields["key"], "value")
	}
}

func TestLoggerWithCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l2 := l.WithCorrelationID("corr-123")
	l2.Info("with correlation id")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.CorrelationID != "corr-123" {
		t.Errorf("correlationId = %q, want %q", entry.CorrelationID, "corr-123")
	}
}

func TestLoggerWithTraceID(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l2 := l.WithTraceID("trace-456")
	l2.Info("with trace id")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.TraceID != "trace-456" {
		t.Errorf("traceId = %q, want %q", entry.TraceID, "trace-456")
	}
}

func TestLoggerWithCorrelationAndTraceID(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l2 := l.WithCorrelationID("corr-123").WithTraceID("trace-456")
	l2.Info("with both ids")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.CorrelationID != "corr-123" {
		t.Errorf("correlationId = %q, want %q", entry.CorrelationID, "corr-123")
	}
	if entry.TraceID != "trace-456" {
		t.Errorf("traceId = %q, want %q", entry.TraceID, "trace-456")
	}
}

func TestLoggerFileLineInDebugMode(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:     LevelDebug,
		Format:    FormatJSON,
		Output:    &buf,
		AddCaller: true,
	})

	l.Debug("with caller info")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.File == "" {
		t.Error("expected file to be present when AddCaller is true")
	}
	if entry.Line == 0 {
		t.Error("expected line to be non-zero when AddCaller is true")
	}
	if !strings.HasSuffix(entry.File, "logger_test.go") {
		t.Errorf("file = %q, expected to end with logger_test.go", entry.File)
	}
}

func TestLoggerNoCallerByDefault(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Debug("without caller info")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.File != "" {
		t.Errorf("expected file to be empty when AddCaller is false, got %q", entry.File)
	}
	if entry.Line != 0 {
		t.Errorf("expected line to be 0 when AddCaller is false, got %d", entry.Line)
	}
}

func TestLoggerTextFormat(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatText,
		Output: &buf,
	})

	l.Info("text message")

	output := buf.String()
	if !strings.Contains(output, "[info]") {
		t.Errorf("text output should contain [info], got %q", output)
	}
	if !strings.Contains(output, "text message") {
		t.Errorf("text output should contain message, got %q", output)
	}
}

func TestLoggerTextFormatWithIDs(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatText,
		Output: &buf,
	})

	l2 := l.WithCorrelationID("corr-123").WithTraceID("trace-456")
	l2.Info("with ids")

	output := buf.String()
	if !strings.Contains(output, "correlationId=corr-123") {
		t.Errorf("text output should contain correlationId, got %q", output)
	}
	if !strings.Contains(output, "traceId=trace-456") {
		t.Errorf("text output should contain traceId, got %q", output)
	}
}

func TestLoggerInfof(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Infof("with extra fields", map[string]any{"extra": "value"})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Fields["extra"] != "value" {
		t.Errorf("fields[extra] = %v, want %q", entry.Fields["extra"], "value")
	}
}

func TestLoggerDebugf(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Debugf("debug with fields", map[string]any{"key": "val"})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "debug" {
		t.Errorf("level = %q, want debug", entry.Level)
	}
}

func TestLoggerWarnf(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Warnf("warning msg", map[string]any{"alert": true})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "warn" {
		t.Errorf("level = %q, want warn", entry.Level)
	}
}

func TestLoggerErrorf(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l.Errorf("error msg", map[string]any{"err": "something failed"})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "error" {
		t.Errorf("level = %q, want error", entry.Level)
	}
}

func TestDefaultLogger(t *testing.T) {
	l := DefaultLogger()
	if l == nil {
		t.Fatal("DefaultLogger() returned nil")
	}
	if l.GetLevel() != LevelInfo {
		t.Errorf("default level = %v, want info", l.GetLevel())
	}
}

func TestLoggerSetFormat(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	l.SetFormat(FormatText)
	l.Info("text output")

	output := buf.String()
	if strings.HasPrefix(output, "{") {
		t.Error("expected text format, got JSON")
	}
	if !strings.Contains(output, "[info]") {
		t.Errorf("expected text format with [info], got %q", output)
	}
}

func TestLoggerSetAddCaller(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})

	l.SetAddCaller(true)
	l.Debug("with caller")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.File == "" {
		t.Error("expected file after SetAddCaller(true)")
	}
}

func TestLoggerWithDoesNotMutateOriginal(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	_ = l.With(map[string]any{"added": "field"})
	l.Info("original logger")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if len(entry.Fields) > 0 {
		t.Error("original logger should not have added fields")
	}
}

func TestLoggerWithCorrelationIDDoesNotMutateOriginal(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	_ = l.WithCorrelationID("corr-123")
	l.Info("original logger")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.CorrelationID != "" {
		t.Error("original logger should not have correlation ID")
	}
}

func TestItoaSmall(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{9, "9"},
	}
	for _, tc := range tests {
		if got := itoa(tc.input); got != tc.expected {
			t.Errorf("itoa(%d) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestItoaLarge(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{10, "10"},
		{42, "42"},
		{123, "123"},
		{9999, "9999"},
	}
	for _, tc := range tests {
		if got := itoa(tc.input); got != tc.expected {
			t.Errorf("itoa(%d) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}
