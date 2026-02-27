package runner

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
)

// mockConsumer implements consumer.Consumer for testing
type mockConsumer struct {
	name        string
	handleCount int32
	shouldFail  bool
}

func (m *mockConsumer) Name() string {
	return m.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (m *mockConsumer) Handle(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
	atomic.AddInt32(&m.handleCount, 1)

	if m.shouldFail {
		return errors.New("mock consumer error")
	}
	return nil
}

// mockProcessor implements consumer.ProcessorRunner for testing
type mockProcessor struct {
	projName   string
	runCalled  bool
	shouldFail bool
}

func (m *mockProcessor) Run(_ context.Context, proj consumer.Consumer) error {
	m.runCalled = true
	m.projName = proj.Name()

	if m.shouldFail {
		return errors.New("mock processor error")
	}
	return nil
}

func TestRunner_Run_NoConsumers(t *testing.T) {
	runner := New()

	err := runner.Run(context.Background(), []ConsumerRunner{})
	if !errors.Is(err, ErrNoConsumers) {
		t.Errorf("Expected ErrNoConsumers, got %v", err)
	}
}

func TestRunner_Run_NilConsumer(t *testing.T) {
	runner := New()

	runners := []ConsumerRunner{
		{
			Consumer:  nil,
			Processor: &mockProcessor{},
		},
	}

	err := runner.Run(context.Background(), runners)
	if err == nil || err.Error() != "consumer at index 0 is nil" {
		t.Errorf("Expected nil consumer error, got %v", err)
	}
}

func TestRunner_Run_NilProcessor(t *testing.T) {
	runner := New()

	runners := []ConsumerRunner{
		{
			Consumer:  &mockConsumer{name: "test"},
			Processor: nil,
		},
	}

	err := runner.Run(context.Background(), runners)
	if err == nil || err.Error() != "processor at index 0 is nil" {
		t.Errorf("Expected nil processor error, got %v", err)
	}
}

func TestRunner_Run_ContextCancellation(t *testing.T) {
	runner := New()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	runners := []ConsumerRunner{
		{
			Consumer:  &mockConsumer{name: "test"},
			Processor: &mockProcessor{},
		},
	}

	err := runner.Run(ctx, runners)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestNew(t *testing.T) {
	runner := New()
	if runner == nil {
		t.Fatal("New returned nil")
	}
}
