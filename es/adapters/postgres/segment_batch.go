package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/pupsourcing/core/es/consumer"
)

// processSegment is the main event processing loop for a single segment.
//
//nolint:gocyclo // Intentional control flow: idle wait, batch processing, and adaptive throttling coordination
func (p *SegmentProcessor) processSegment(
	ctx context.Context,
	plan *segmentProcessPlan,
	segmentID int,
) error {
	var wakeupCh <-chan struct{}
	unsubscribe := func() {}
	if p.config.WakeupSource != nil {
		wakeupCh, unsubscribe = p.config.WakeupSource.Subscribe()
	}
	defer unsubscribe()

	idleDelay := p.config.PollInterval
	postBatchPause := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			if p.config.Logger != nil {
				p.config.Logger.Info(ctx, "segment worker stopped",
					"consumer", plan.name,
					"segment_id", segmentID,
					"reason", ctx.Err())
			}
			return ctx.Err()
		default:
		}

		// Process batch
		batchSize, err := p.executeBatchWithRetry(ctx, plan.name, segmentID, func() (int, error) {
			return p.processSegmentBatch(ctx, plan, segmentID)
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errNoEventsInBatch) {
				postBatchPause = 0

				// Handle idle cycle
				if p.config.RunMode == consumer.RunModeOneOff {
					if p.config.Logger != nil {
						p.config.Logger.Info(ctx, "segment worker caught up (one-off mode)",
							"consumer", plan.name,
							"segment_id", segmentID)
					}
					return nil
				}

				if p.config.Logger != nil {
					p.config.Logger.Debug(ctx, "segment idle polling",
						"consumer", plan.name,
						"segment_id", segmentID,
						"idle_delay", idleDelay)
				}

				wokeBySignal, waitErr := p.waitForNextBatch(ctx, plan.name, segmentID, wakeupCh, idleDelay)
				if waitErr != nil {
					return waitErr
				}

				if wokeBySignal {
					idleDelay = p.config.PollInterval
				} else {
					idleDelay = p.nextPollDelay(idleDelay)
				}
				continue
			}

			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "segment worker error",
					"consumer", plan.name,
					"segment_id", segmentID,
					"error", err)
			}
			return err
		}

		// Reset idle delay on successful batch
		if idleDelay != p.config.PollInterval {
			idleDelay = p.config.PollInterval
		}

		postBatchPause = p.nextAdaptivePostBatchPause(postBatchPause, batchSize)
		if waitErr := p.waitPostBatchPause(ctx, plan.name, segmentID, postBatchPause); waitErr != nil {
			return waitErr
		}
	}
}

func (p *SegmentProcessor) executeBatchWithRetry(ctx context.Context, consumerName string, segmentID int, run func() (int, error)) (int, error) {
	retriesUsed := 0

	for {
		batchSize, err := run()
		if err == nil {
			return batchSize, nil
		}

		if !p.shouldRetryTransientBatchError(err, retriesUsed) {
			return 0, err
		}

		delay := nextTransientRetryDelay(retriesUsed)
		if p.config.Logger != nil {
			p.config.Logger.Info(ctx, "segment batch transient error, retrying",
				"consumer", consumerName,
				"segment_id", segmentID,
				"retry_attempt", retriesUsed+1,
				"max_retry_attempts", p.config.TransientErrorRetryMaxAttempts,
				"retry_delay", delay,
				"error", err)
		}

		if err := waitForRetryDelay(ctx, delay); err != nil {
			return 0, err
		}

		retriesUsed++
	}
}

// processSegmentBatch processes one batch of events for a segment.
//
//nolint:gocyclo // Intentional control flow: ownership checks, event filtering, handler errors, and checkpointing
func (p *SegmentProcessor) processSegmentBatch(
	ctx context.Context,
	plan *segmentProcessPlan,
	segmentID int,
) (int, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		tx.Rollback()
	}()

	// Get segment checkpoint
	checkpoint, err := p.store.getOwnedSegmentCheckpoint(ctx, tx, plan.name, segmentID, p.ownerID)
	if err != nil {
		if errors.Is(err, ErrSegmentNotOwned) {
			return 0, err
		}
		return 0, fmt.Errorf("failed to get segment checkpoint: %w", err)
	}

	// Read events
	events, err := p.store.ReadEventsWithScope(ctx, tx, checkpoint, p.config.BatchSize, plan.readScope)
	if err != nil {
		return 0, fmt.Errorf("failed to read events: %w", err)
	}

	if len(events) == 0 {
		return 0, errNoEventsInBatch
	}

	// Process events with filtering
	var lastPosition int64
	var processedCount int
	var skippedCount int
	for i := range events {
		event := events[i]

		// Check if this segment should process this event.
		if !p.shouldProcessEventForSegment(event, segmentID) {
			lastPosition = event.GlobalPosition
			skippedCount++
			continue
		}

		// Handle event
		handlerErr := plan.consumer.Handle(ctx, tx, event)
		if handlerErr != nil {
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "consumer handler error",
					"consumer", plan.name,
					"segment_id", segmentID,
					"position", event.GlobalPosition,
					"aggregate_type", event.AggregateType,
					"aggregate_id", event.AggregateID,
					"event_type", event.EventType,
					"error", handlerErr)
			}
			return 0, fmt.Errorf("consumer handler error at position %d: %w", event.GlobalPosition, handlerErr)
		}

		lastPosition = event.GlobalPosition
		processedCount++
	}

	// Update segment checkpoint
	if lastPosition > 0 {
		err = p.store.updateOwnedSegmentCheckpoint(ctx, tx, plan.name, segmentID, p.ownerID, lastPosition)
		if err != nil {
			if errors.Is(err, ErrSegmentNotOwned) {
				return 0, err
			}
			return 0, fmt.Errorf("failed to update segment checkpoint: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	if p.config.Logger != nil {
		if processedCount > 0 {
			p.config.Logger.Info(ctx, "segment events processed",
				"consumer", plan.name,
				"segment_id", segmentID,
				"processed", processedCount,
				"skipped", skippedCount,
				"checkpoint", lastPosition)
		} else {
			p.config.Logger.Debug(ctx, "segment batch processed",
				"consumer", plan.name,
				"segment_id", segmentID,
				"processed", processedCount,
				"skipped", skippedCount,
				"checkpoint", lastPosition)
		}
	}

	return len(events), nil
}

func (p *SegmentProcessor) shouldRetryTransientBatchError(err error, retriesUsed int) bool {
	if p.config == nil || p.config.TransientErrorRetryMaxAttempts <= 0 {
		return false
	}
	if retriesUsed >= p.config.TransientErrorRetryMaxAttempts {
		return false
	}

	return isRetryablePostgresTransactionError(err)
}

func isRetryablePostgresTransactionError(err error) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return false
	}

	switch pqErr.SQLState() {
	case pgSQLStateDeadlockDetected, pgSQLStateSerializationFailure:
		return true
	default:
		return false
	}
}

func nextTransientRetryDelay(retriesUsed int) time.Duration {
	delay := initialTransientBatchRetryDelay
	for i := 0; i < retriesUsed; i++ {
		if delay >= maxTransientBatchRetryDelay/2 {
			return maxTransientBatchRetryDelay
		}
		delay *= 2
	}
	return delay
}

func waitForRetryDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// waitForNextBatch waits for the next batch using wakeup signals or timer.
func (p *SegmentProcessor) waitForNextBatch(ctx context.Context, consumerName string, segmentID int, wakeupCh <-chan struct{}, delay time.Duration) (bool, error) {
	if wakeupCh == nil {
		if delay <= 0 {
			return false, nil
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(delay):
			return false, nil
		}
	}

	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-wakeupCh:
			return true, p.applyWakeupJitter(ctx, consumerName, segmentID)
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-wakeupCh:
		return true, p.applyWakeupJitter(ctx, consumerName, segmentID)
	case <-timer.C:
		return false, nil
	}
}

// applyWakeupJitter applies a random delay after wakeup to smooth concurrent wake-ups.
func (p *SegmentProcessor) applyWakeupJitter(ctx context.Context, consumerName string, segmentID int) error {
	if p.config.WakeupJitter <= 0 {
		return nil
	}

	jitter := time.Duration((time.Now().UnixNano() + int64(segmentID+1)) % int64(p.config.WakeupJitter))
	if jitter <= 0 {
		return nil
	}

	if p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "segment wakeup jitter waiting",
			"consumer", consumerName,
			"segment_id", segmentID,
			"jitter_delay", jitter)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(jitter):
		return nil
	}
}

// nextAdaptivePostBatchPause calculates the next post-batch pause based on observed batch size.
func (p *SegmentProcessor) nextAdaptivePostBatchPause(current time.Duration, batchSize int) time.Duration {
	maxPause := p.config.MaxPostBatchPause
	if p.config.RunMode == consumer.RunModeOneOff || maxPause <= 0 || batchSize <= 0 {
		return 0
	}

	if current > maxPause {
		current = maxPause
	}

	fullBatch := p.config.BatchSize > 0 && batchSize >= p.config.BatchSize
	if fullBatch {
		if current <= 0 {
			if maxPause < minAdaptivePostBatchPause {
				return maxPause
			}
			return minAdaptivePostBatchPause
		}

		next := current * 2
		if next > maxPause {
			return maxPause
		}
		return next
	}

	if current <= 0 {
		return 0
	}

	next := current / 2
	if next < minAdaptivePostBatchPause {
		return 0
	}
	return next
}

// waitPostBatchPause blocks between successful batches to reduce sustained DB pressure.
func (p *SegmentProcessor) waitPostBatchPause(ctx context.Context, consumerName string, segmentID int, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	if p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "segment adaptive post-batch pause",
			"consumer", consumerName,
			"segment_id", segmentID,
			"pause", delay)
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// nextPollDelay calculates the next poll delay with exponential backoff.
func (p *SegmentProcessor) nextPollDelay(current time.Duration) time.Duration {
	if p.config.PollInterval <= 0 {
		return 0
	}

	maxDelay := p.config.MaxPollInterval
	if maxDelay <= 0 {
		maxDelay = p.config.PollInterval
	}

	if current <= 0 {
		current = p.config.PollInterval
	}

	factor := p.config.PollBackoffFactor
	if factor <= 1 {
		if current > maxDelay {
			return maxDelay
		}
		return current
	}

	next := time.Duration(float64(current) * factor)
	if next > maxDelay {
		return maxDelay
	}

	return next
}
