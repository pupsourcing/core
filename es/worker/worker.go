// Package worker provides a high-level orchestrator that wraps SegmentProcessor, Dispatcher,
// and Runner into a single Worker.Run(ctx, consumers...) call for simplified deployment.
package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/consumer/runner"
	"github.com/pupsourcing/core/es/store"
)

// Config configures the Worker orchestrator.
type Config struct {
	// PartitionStrategy determines which events each segment processes.
	// Default: consumer.HashPartitionStrategy{}
	PartitionStrategy consumer.PartitionStrategy

	// Logger is an optional logger for observability.
	// Default: nil
	Logger es.Logger

	// HeartbeatInterval is how often each worker updates its heartbeat timestamp.
	// Default: 5s
	HeartbeatInterval time.Duration

	// StaleThreshold is how long a segment can go without a heartbeat before
	// it is considered abandoned and eligible for reclaim by other workers.
	// Default: 30s
	StaleThreshold time.Duration

	// RebalanceInterval is how often workers check segment distribution
	// and perform fair-share rebalancing.
	// Default: 10s
	RebalanceInterval time.Duration

	// PollInterval is the duration to wait when no events are available.
	// Default: 100ms
	PollInterval time.Duration

	// MaxPollInterval is the upper bound for idle fallback polling when backoff is enabled.
	// Default: 5s
	MaxPollInterval time.Duration

	// WakeupJitter is the random delay applied after receiving a wake signal.
	// Default: 25ms
	WakeupJitter time.Duration

	// DispatcherPollInterval controls how often the dispatcher checks for new events.
	// Default: 200ms
	DispatcherPollInterval time.Duration

	// PollBackoffFactor controls exponential backoff growth for idle fallback polling.
	// Default: 2.0
	PollBackoffFactor float64

	// TotalSegments is the number of segments to pre-create for each consumer.
	// This is the upper bound on parallelism per consumer.
	// Default: 16
	TotalSegments int

	// BatchSize is the number of events to read per batch.
	// Default: 100
	BatchSize int

	// EnableDispatcher enables the optional dispatcher for best-effort wake signals.
	// Default: true
	EnableDispatcher bool

	// RunMode determines how the processor handles event processing.
	// Default: RunModeContinuous (processes events indefinitely)
	// Set to RunModeOneOff for integration tests (processes all events and exits).
	RunMode consumer.RunMode
}

// DefaultConfig returns the default Worker configuration.
func DefaultConfig() *Config {
	return &Config{
		TotalSegments:          16,
		HeartbeatInterval:      5 * time.Second,
		StaleThreshold:         30 * time.Second,
		RebalanceInterval:      10 * time.Second,
		BatchSize:              100,
		PollInterval:           100 * time.Millisecond,
		MaxPollInterval:        5 * time.Second,
		PollBackoffFactor:      2.0,
		WakeupJitter:           25 * time.Millisecond,
		PartitionStrategy:      consumer.HashPartitionStrategy{},
		EnableDispatcher:       true,
		DispatcherPollInterval: 200 * time.Millisecond,
		Logger:                 nil,
	}
}

// Option is a functional option for configuring a Worker.
type Option func(*Config)

// WithTotalSegments sets the total number of segments per consumer.
func WithTotalSegments(n int) Option {
	return func(c *Config) {
		c.TotalSegments = n
	}
}

// WithHeartbeatInterval sets the heartbeat interval.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatInterval = d
	}
}

// WithStaleThreshold sets the stale threshold.
func WithStaleThreshold(d time.Duration) Option {
	return func(c *Config) {
		c.StaleThreshold = d
	}
}

// WithRebalanceInterval sets the rebalance interval.
func WithRebalanceInterval(d time.Duration) Option {
	return func(c *Config) {
		c.RebalanceInterval = d
	}
}

// WithBatchSize sets the batch size.
func WithBatchSize(n int) Option {
	return func(c *Config) {
		c.BatchSize = n
	}
}

// WithPollInterval sets the poll interval.
func WithPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.PollInterval = d
	}
}

// WithMaxPollInterval sets the max poll interval.
func WithMaxPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.MaxPollInterval = d
	}
}

// WithPollBackoffFactor sets the poll backoff factor.
func WithPollBackoffFactor(f float64) Option {
	return func(c *Config) {
		c.PollBackoffFactor = f
	}
}

// WithWakeupJitter sets the wakeup jitter.
func WithWakeupJitter(d time.Duration) Option {
	return func(c *Config) {
		c.WakeupJitter = d
	}
}

// WithPartitionStrategy sets the partition strategy.
func WithPartitionStrategy(s consumer.PartitionStrategy) Option {
	return func(c *Config) {
		c.PartitionStrategy = s
	}
}

// WithDispatcher enables or disables the dispatcher.
func WithDispatcher(enabled bool) Option {
	return func(c *Config) {
		c.EnableDispatcher = enabled
	}
}

// WithDispatcherPollInterval sets the dispatcher poll interval.
func WithDispatcherPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.DispatcherPollInterval = d
	}
}

// WithLogger sets the logger.
func WithLogger(l es.Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

// WithRunMode sets the run mode (Continuous or OneOff).
func WithRunMode(mode consumer.RunMode) Option {
	return func(c *Config) {
		c.RunMode = mode
	}
}

// ProcessorFactory creates a ProcessorRunner from a SegmentProcessorConfig.
// This abstraction allows the Worker to be storage-agnostic.
type ProcessorFactory func(config *consumer.SegmentProcessorConfig) consumer.ProcessorRunner

// Worker orchestrates SegmentProcessor, Dispatcher, and Runner into a single Run call.
type Worker struct {
	db        *sql.DB
	posReader store.GlobalPositionReader
	factory   ProcessorFactory
	config    *Config
}

// New creates a new Worker with the given options.
func New(db *sql.DB, posReader store.GlobalPositionReader, factory ProcessorFactory, opts ...Option) *Worker {
	config := DefaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	return &Worker{
		db:        db,
		posReader: posReader,
		factory:   factory,
		config:    config,
	}
}

// Run runs the worker with the given consumers until the context is canceled.
// It sets up the dispatcher (if enabled), creates segment processors for each consumer,
// and runs them all concurrently using the runner package.
func (w *Worker) Run(ctx context.Context, consumers ...consumer.Consumer) error {
	if len(consumers) == 0 {
		return fmt.Errorf("at least one consumer is required")
	}

	var dispatcher *consumer.Dispatcher
	if w.config.EnableDispatcher {
		dispatcherCfg := &consumer.DispatcherConfig{
			PollInterval: w.config.DispatcherPollInterval,
			Logger:       w.config.Logger,
		}
		dispatcher = consumer.NewDispatcher(w.db, w.posReader, dispatcherCfg)
	}

	runners := make([]runner.ConsumerRunner, 0, len(consumers))
	for _, c := range consumers {
		segCfg := &consumer.SegmentProcessorConfig{
			PartitionStrategy: w.config.PartitionStrategy,
			Logger:            w.config.Logger,
			HeartbeatInterval: w.config.HeartbeatInterval,
			StaleThreshold:    w.config.StaleThreshold,
			RebalanceInterval: w.config.RebalanceInterval,
			PollInterval:      w.config.PollInterval,
			MaxPollInterval:   w.config.MaxPollInterval,
			WakeupJitter:      w.config.WakeupJitter,
			PollBackoffFactor: w.config.PollBackoffFactor,
			TotalSegments:     w.config.TotalSegments,
			BatchSize:         w.config.BatchSize,
			RunMode:           w.config.RunMode,
		}
		if dispatcher != nil {
			segCfg.WakeupSource = dispatcher
		}

		processor := w.factory(segCfg)
		runners = append(runners, runner.ConsumerRunner{
			Consumer:  c,
			Processor: processor,
		})
	}

	// Start dispatcher if enabled
	// Use a derived context so we can stop the dispatcher when the runner finishes.
	internalCtx, internalCancel := context.WithCancel(ctx)
	defer internalCancel()

	var dispatcherErrCh chan error
	if dispatcher != nil {
		dispatcherErrCh = make(chan error, 1)
		go func() {
			dispatcherErrCh <- dispatcher.Run(internalCtx)
		}()
	}

	// Run all consumers
	runnerErr := runner.New().Run(internalCtx, runners)

	// Cancel internal context to stop the dispatcher
	internalCancel()

	// Wait for dispatcher to finish if it was started
	var dispatcherErr error
	if dispatcherErrCh != nil {
		dispatcherErr = <-dispatcherErrCh
	}

	// Return the first non-context error, prioritizing runner errors
	if runnerErr != nil && !errors.Is(runnerErr, context.Canceled) && !errors.Is(runnerErr, context.DeadlineExceeded) {
		return runnerErr
	}
	if dispatcherErr != nil && !errors.Is(dispatcherErr, context.Canceled) &&
		!errors.Is(dispatcherErr, context.DeadlineExceeded) {
		return dispatcherErr
	}

	return runnerErr
}
