// Package runner provides optional tooling for running multiple consumers and scaling them safely.
// This package is designed to be explicit, deterministic, and CLI-friendly without imposing
// framework behavior or automatic scheduling.
package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pupsourcing/core/es/consumer"
)

var (
	// ErrNoConsumers indicates that no consumers were provided to run.
	ErrNoConsumers = errors.New("no consumers provided")

	// ErrInvalidPartitionConfig indicates invalid partition configuration.
	ErrInvalidPartitionConfig = errors.New("invalid partition configuration")
)

// ConsumerRunner pairs a consumer with its processor.
// The processor is adapter-specific (postgres.Processor, mysql.Processor, etc.)
// and knows how to manage transactions and checkpoints for that storage type.
type ConsumerRunner struct {
	Consumer  consumer.Consumer
	Processor consumer.ProcessorRunner
}

// Runner orchestrates multiple consumers concurrently.
// It is storage-agnostic and works with any processor implementation.
//
// Example with PostgreSQL:
//
// store := postgres.NewStore(postgres.DefaultStoreConfig())
// processor1 := postgres.NewProcessor(db, store, &config1)
// processor2 := postgres.NewProcessor(db, store, &config2)
//
// runner := runner.New()
//
//	err := runner.Run(ctx, []runner.ConsumerRunner{
//	   {Consumer: &MyConsumer{}, Processor: processor1},
//	   {Consumer: &MyOtherConsumer{}, Processor: processor2},
//	})
type Runner struct{}

// New creates a new consumer runner.
func New() *Runner {
	return &Runner{}
}

// Run runs multiple consumers concurrently until the context is canceled.
// Each consumer runs in its own goroutine with its processor.
// Returns when the context is canceled or when any consumer returns an error.
//
// If a consumer returns an error, all other consumers are canceled and the error
// is returned. This ensures fail-fast behavior.
//
// This method is safe to call from CLIs and does not assume single-process ownership.
// Coordination happens via the processor's checkpoint management.
func (r *Runner) Run(ctx context.Context, runners []ConsumerRunner) error {
	if len(runners) == 0 {
		return ErrNoConsumers
	}

	// Validate configurations
	for i, runner := range runners {
		if runner.Consumer == nil {
			return fmt.Errorf("consumer at index %d is nil", i)
		}
		if runner.Processor == nil {
			return fmt.Errorf("processor at index %d is nil", i)
		}
	}

	// Create a context that we can cancel if any consumer fails
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errChan := make(chan error, len(runners))

	// Start each consumer in its own goroutine
	for _, runner := range runners {
		wg.Add(1)
		go func(pr ConsumerRunner) {
			defer wg.Done()

			err := pr.Processor.Run(ctx, pr.Consumer)

			// Only report errors that aren't from context cancellation
			if err != nil && !errors.Is(err, context.Canceled) {
				errChan <- fmt.Errorf("consumer %q failed: %w", pr.Consumer.Name(), err)
			}
		}(runner)
	}

	// Wait for all consumers to complete or for an error
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Return the first error, or nil if context was canceled
	select {
	case err := <-errChan:
		if err != nil {
			cancel() // Cancel all other consumers
			return err
		}
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
