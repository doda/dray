package logging

import (
	"os"
	"sync"
)

var (
	globalLogger *Logger
	globalMu     sync.RWMutex
)

func init() {
	globalLogger = DefaultLogger()
}

// SetGlobal sets the global logger.
func SetGlobal(l *Logger) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalLogger = l
}

// Global returns the global logger.
func Global() *Logger {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalLogger
}

// Configure creates and sets a global logger from config values.
// This is typically called during application startup.
func Configure(level, format string) *Logger {
	l := New(Config{
		Level:     ParseLevel(level),
		Format:    ParseFormat(format),
		Output:    os.Stderr,
		AddCaller: ParseLevel(level) == LevelDebug,
	})
	SetGlobal(l)
	return l
}

// Debug logs a debug message to the global logger.
func Debug(msg string) {
	Global().Debug(msg)
}

// Debugf logs a debug message with fields to the global logger.
func Debugf(msg string, fields map[string]any) {
	Global().Debugf(msg, fields)
}

// Info logs an info message to the global logger.
func Info(msg string) {
	Global().Info(msg)
}

// Infof logs an info message with fields to the global logger.
func Infof(msg string, fields map[string]any) {
	Global().Infof(msg, fields)
}

// Warn logs a warning message to the global logger.
func Warn(msg string) {
	Global().Warn(msg)
}

// Warnf logs a warning message with fields to the global logger.
func Warnf(msg string, fields map[string]any) {
	Global().Warnf(msg, fields)
}

// Error logs an error message to the global logger.
func Error(msg string) {
	Global().Error(msg)
}

// Errorf logs an error message with fields to the global logger.
func Errorf(msg string, fields map[string]any) {
	Global().Errorf(msg, fields)
}
