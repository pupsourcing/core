package es_test

import (
	"context"
	"testing"

	"github.com/pupsourcing/core/es"
)

// TestNoOpLogger verifies the NoOpLogger doesn't panic.
func TestNoOpLogger(_ *testing.T) {
	ctx := context.Background()
	logger := es.NoOpLogger{}

	// These should not panic
	logger.Debug(ctx, "debug message", "key", "value")
	logger.Info(ctx, "info message", "key", "value")
	logger.Error(ctx, "error message", "key", "value")
}

// TestLoggerInterface verifies NoOpLogger implements Logger.
func TestLoggerInterface(_ *testing.T) {
	var _ es.Logger = es.NoOpLogger{}
}

// mockLogger is a simple logger for testing that records calls.
type mockLogger struct {
	lastMsg    string
	debugCalls int
	infoCalls  int
	errorCalls int
}

func (m *mockLogger) Debug(_ context.Context, msg string, _ ...interface{}) {
	m.debugCalls++
	m.lastMsg = msg
}

func (m *mockLogger) Info(_ context.Context, msg string, _ ...interface{}) {
	m.infoCalls++
	m.lastMsg = msg
}

func (m *mockLogger) Error(_ context.Context, msg string, _ ...interface{}) {
	m.errorCalls++
	m.lastMsg = msg
}

func TestMockLogger(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}

	logger.Debug(ctx, "debug", "key", "value")
	if logger.debugCalls != 1 {
		t.Errorf("expected 1 debug call, got %d", logger.debugCalls)
	}
	if logger.lastMsg != "debug" {
		t.Errorf("expected last message 'debug', got %s", logger.lastMsg)
	}

	logger.Info(ctx, "info", "key", "value")
	if logger.infoCalls != 1 {
		t.Errorf("expected 1 info call, got %d", logger.infoCalls)
	}
	if logger.lastMsg != "info" {
		t.Errorf("expected last message 'info', got %s", logger.lastMsg)
	}

	logger.Error(ctx, "error", "key", "value")
	if logger.errorCalls != 1 {
		t.Errorf("expected 1 error call, got %d", logger.errorCalls)
	}
	if logger.lastMsg != "error" {
		t.Errorf("expected last message 'error', got %s", logger.lastMsg)
	}
}
