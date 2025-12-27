package logging

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestSetGlobalAndGlobal(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})

	SetGlobal(l)
	got := Global()

	if got != l {
		t.Error("Global() should return the logger set by SetGlobal")
	}
}

func TestConfigure(t *testing.T) {
	l := Configure("debug", "json")

	if l.GetLevel() != LevelDebug {
		t.Errorf("Configure level = %v, want debug", l.GetLevel())
	}

	got := Global()
	if got != l {
		t.Error("Configure should set global logger")
	}
}

func TestConfigureEnablesCallerAtDebug(t *testing.T) {
	var buf bytes.Buffer
	l := Configure("debug", "json")
	l.mu.Lock()
	l.out = &buf
	l.mu.Unlock()

	l.Debug("test")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.File == "" {
		t.Error("Configure at debug level should enable caller info")
	}
}

func TestConfigureNoCallerAtInfo(t *testing.T) {
	var buf bytes.Buffer
	l := Configure("info", "json")
	l.mu.Lock()
	l.out = &buf
	l.mu.Unlock()

	l.Info("test")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.File != "" {
		t.Error("Configure at info level should not enable caller info")
	}
}

func TestGlobalDebug(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Debug("global debug")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "debug" {
		t.Errorf("level = %q, want debug", entry.Level)
	}
}

func TestGlobalDebugf(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelDebug,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Debugf("global debugf", map[string]any{"key": "val"})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "debug" {
		t.Errorf("level = %q, want debug", entry.Level)
	}
	if entry.Fields["key"] != "val" {
		t.Errorf("fields[key] = %v, want val", entry.Fields["key"])
	}
}

func TestGlobalInfo(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Info("global info")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "info" {
		t.Errorf("level = %q, want info", entry.Level)
	}
}

func TestGlobalInfof(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Infof("global infof", map[string]any{"k": "v"})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Fields["k"] != "v" {
		t.Errorf("fields[k] = %v, want v", entry.Fields["k"])
	}
}

func TestGlobalWarn(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Warn("global warn")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "warn" {
		t.Errorf("level = %q, want warn", entry.Level)
	}
}

func TestGlobalWarnf(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Warnf("global warnf", map[string]any{"alert": true})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "warn" {
		t.Errorf("level = %q, want warn", entry.Level)
	}
}

func TestGlobalError(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Error("global error")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "error" {
		t.Errorf("level = %q, want error", entry.Level)
	}
}

func TestGlobalErrorf(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{
		Level:  LevelInfo,
		Format: FormatJSON,
		Output: &buf,
	})
	SetGlobal(l)

	Errorf("global errorf", map[string]any{"err": "failed"})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Level != "error" {
		t.Errorf("level = %q, want error", entry.Level)
	}
	if entry.Fields["err"] != "failed" {
		t.Errorf("fields[err] = %v, want failed", entry.Fields["err"])
	}
}

func TestGlobalLoggerInitialized(t *testing.T) {
	// Reset to ensure global is default
	SetGlobal(DefaultLogger())

	l := Global()
	if l == nil {
		t.Fatal("Global() should never return nil")
	}
}
