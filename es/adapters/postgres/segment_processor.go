package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/pupsourcing/core/es/consumer"
)

// SegmentProcessor processes events for consumers using segment-based auto-scaling.
// This processor manages multiple segments dynamically with fair-share rebalancing,
// enabling horizontal scaling across multiple worker instances.

var _ consumer.ProcessorRunner = (*SegmentProcessor)(nil)

var (
	errNoEventsInBatch = errors.New("no events in batch")
	// ErrConsumerStopped indicates the consumer was stopped due to an error.
	ErrConsumerStopped = errors.New("consumer stopped")
)

const (
	minAdaptivePostBatchPause       = 5 * time.Millisecond
	initialTransientBatchRetryDelay = 25 * time.Millisecond
	maxTransientBatchRetryDelay     = 500 * time.Millisecond
	pgSQLStateDeadlockDetected      = "40P01"
	pgSQLStateSerializationFailure  = "40001"
)

type SegmentProcessor struct {
	db      *sql.DB
	store   *Store
	config  *consumer.SegmentProcessorConfig
	ownerID string
}

// NewSegmentProcessor creates a new segment-based consumer processor.
// The processor manages segment claiming, heartbeats, rebalancing, and per-segment processing.
func NewSegmentProcessor(db *sql.DB, s *Store, config *consumer.SegmentProcessorConfig) *SegmentProcessor {
	return &SegmentProcessor{
		db:      db,
		store:   s,
		config:  config,
		ownerID: uuid.New().String(),
	}
}

// Run processes events for the given consumer until the context is canceled.
// It manages segment claiming, heartbeating, rebalancing, and per-segment processing goroutines.
func (p *SegmentProcessor) Run(ctx context.Context, cons consumer.Consumer) error {
	plan := newSegmentProcessPlan(cons)

	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "segment processor starting",
			"consumer", plan.name,
			"owner_id", p.ownerID,
			"total_segments", p.config.TotalSegments,
			"batch_size", p.config.BatchSize)
	}

	// Initialize segments (idempotent)
	if err := p.store.InitializeSegments(ctx, p.db, plan.name, p.config.TotalSegments); err != nil {
		return fmt.Errorf("failed to initialize segments: %w", err)
	}

	// Register this worker in the registry (makes it visible for fair-share calculations)
	if err := p.store.RegisterWorker(ctx, p.db, plan.name, p.ownerID); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	// Track segment workers
	var workersMu sync.Mutex
	workers := make(map[int]*segmentWorker)

	// Error channel for segment worker failures
	workerErrCh := make(chan error, 1)

	// Start heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		p.heartbeatLoop(heartbeatCtx, plan.name)
	}()

	healthDone := make(chan struct{})
	close(healthDone)
	cancelHealthAudit := func() {}

	if p.config.RunMode != consumer.RunModeOneOff {
		healthAuditInterval := p.healthAuditInterval()
		if healthAuditInterval > 0 {
			healthAuditCtx, cancel := context.WithCancel(ctx)
			cancelHealthAudit = cancel
			healthDone = make(chan struct{})
			go func() {
				defer close(healthDone)
				p.healthAuditLoop(healthAuditCtx, plan.name, healthAuditInterval)
			}()
		}
	}

	stopBackgroundLoops := func() {
		cancelHeartbeat()
		cancelHealthAudit()
		<-heartbeatDone
		<-healthDone
	}

	// Handle RunModeOneOff
	if p.config.RunMode == consumer.RunModeOneOff {
		err := p.runOneOff(ctx, plan, &workersMu, workers, workerErrCh)
		stopBackgroundLoops()
		p.deregisterWorker(ctx, plan.name)
		return err
	}

	// Main rebalance loop
	rebalanceTicker := time.NewTicker(p.config.RebalanceInterval)
	defer rebalanceTicker.Stop()

	// Perform initial rebalance immediately
	if err := p.rebalance(ctx, plan, &workersMu, workers, workerErrCh); err != nil {
		stopBackgroundLoops()
		p.stopAllWorkers(&workersMu, workers)
		p.deregisterWorker(ctx, plan.name)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			if p.config.Logger != nil {
				p.config.Logger.Info(ctx, "segment processor stopping",
					"consumer", plan.name,
					"reason", ctx.Err())
			}
			stopBackgroundLoops()
			p.stopAllWorkers(&workersMu, workers)
			p.releaseAllSegments(context.Background(), plan.name)
			p.deregisterWorker(context.Background(), plan.name)
			return ctx.Err()

		case err := <-workerErrCh:
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "segment worker error",
					"consumer", plan.name,
					"error", err)
			}
			stopBackgroundLoops()
			p.stopAllWorkers(&workersMu, workers)
			p.releaseAllSegments(context.Background(), plan.name)
			p.deregisterWorker(context.Background(), plan.name)
			return fmt.Errorf("%w: %v", ErrConsumerStopped, err)

		case <-rebalanceTicker.C:
			if err := p.rebalance(ctx, plan, &workersMu, workers, workerErrCh); err != nil {
				stopBackgroundLoops()
				p.stopAllWorkers(&workersMu, workers)
				p.releaseAllSegments(context.Background(), plan.name)
				p.deregisterWorker(context.Background(), plan.name)
				return err
			}
		}
	}
}
