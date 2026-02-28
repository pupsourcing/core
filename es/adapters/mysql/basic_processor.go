package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
)

// BasicProcessor processes events for consumers using MySQL for checkpointing.
// It provides a simple, single-process consumer loop without segment-based auto-scaling.
// Use this for integration tests (with RunModeOneOff) or simple single-instance deployments.
// For production multi-worker deployments, use SegmentProcessor via the Worker API instead.
type BasicProcessor struct {
	db     *sql.DB
	store  *Store
	config *consumer.BasicProcessorConfig
}

// NewBasicProcessor creates a new MySQL consumer processor.
// The processor manages SQL transactions internally and coordinates checkpointing with event processing.
func NewBasicProcessor(db *sql.DB, store *Store, config *consumer.BasicProcessorConfig) *BasicProcessor {
	return &BasicProcessor{
		db:     db,
		store:  store,
		config: config,
	}
}

// checkpointName returns the checkpoint name for this processor.
// When partitioning is enabled (TotalPartitions > 1), it appends the partition key to ensure
// each partition tracks its checkpoint independently.
func (p *BasicProcessor) checkpointName(consumerName string) string {
	if p.config.TotalPartitions > 1 {
		return fmt.Sprintf("%s_p%d", consumerName, p.config.PartitionKey)
	}
	return consumerName
}

// Run processes events for the given consumer until the context is canceled.
// It reads events in batches, applies partition and aggregate type filters, and updates checkpoints.
// Returns an error if the consumer handler fails.
func (p *BasicProcessor) Run(ctx context.Context, proj consumer.Consumer) error {
	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "consumer processor starting",
			"consumer", proj.Name(),
			"partition_key", p.config.PartitionKey,
			"total_partitions", p.config.TotalPartitions,
			"batch_size", p.config.BatchSize)
	}

	var wakeupCh <-chan struct{}
	unsubscribe := func() {}
	if p.config.WakeupSource != nil {
		wakeupCh, unsubscribe = p.config.WakeupSource.Subscribe()
	}
	defer unsubscribe()

	idleDelay := p.config.PollInterval

	aggregateTypeFilter := buildAggregateTypeFilter(proj)
	boundedContextFilter := buildBoundedContextFilter(proj)

	for {
		select {
		case <-ctx.Done():
			if p.config.Logger != nil {
				p.config.Logger.Info(ctx, "consumer processor stopped",
					"consumer", proj.Name(),
					"reason", ctx.Err())
			}
			return ctx.Err()
		default:
		}

		err := p.processBatch(ctx, proj, aggregateTypeFilter, boundedContextFilter)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || err.Error() == "no events in batch" {
				nextIdleDelay, shouldStop, waitErr := p.handleIdleCycle(ctx, proj, wakeupCh, idleDelay)
				if waitErr != nil {
					return waitErr
				}

				if shouldStop {
					return nil
				}

				idleDelay = nextIdleDelay
				continue
			}
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "consumer processor error",
					"consumer", proj.Name(),
					"error", err)
			}
			return fmt.Errorf("%w: %v", ErrConsumerStopped, err)
		}

		idleDelay = p.resetIdleDelay(ctx, proj, idleDelay)
	}
}

func (p *BasicProcessor) handleIdleCycle(
	ctx context.Context, proj consumer.Consumer, wakeupCh <-chan struct{}, idleDelay time.Duration,
) (time.Duration, bool, error) {
	if p.config.RunMode == consumer.RunModeOneOff {
		if p.config.Logger != nil {
			p.config.Logger.Info(ctx, "consumer processor caught up (one-off mode)",
				"consumer", proj.Name())
		}
		return idleDelay, true, nil
	}

	if p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "consumer idle polling",
			"consumer", proj.Name(),
			"partition_key", p.config.PartitionKey,
			"total_partitions", p.config.TotalPartitions,
			"idle_delay", idleDelay,
			"wakeup_source_enabled", wakeupCh != nil)
	}

	wokeBySignal, waitErr := p.waitForNextBatch(ctx, proj.Name(), wakeupCh, idleDelay)
	if waitErr != nil {
		return idleDelay, false, waitErr
	}

	waitReason := "fallback_timer"
	nextIdleDelay := p.nextPollDelay(idleDelay)
	if wokeBySignal {
		waitReason = "signal"
		nextIdleDelay = p.config.PollInterval
	}

	if p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "consumer idle wait completed",
			"consumer", proj.Name(),
			"partition_key", p.config.PartitionKey,
			"total_partitions", p.config.TotalPartitions,
			"wait_reason", waitReason,
			"idle_delay", idleDelay,
			"next_idle_delay", nextIdleDelay,
			"wakeup_source_enabled", wakeupCh != nil)
	}

	return nextIdleDelay, false, nil
}

func (p *BasicProcessor) resetIdleDelay(
	ctx context.Context, proj consumer.Consumer, currentDelay time.Duration,
) time.Duration {
	if currentDelay != p.config.PollInterval && p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "consumer idle backoff reset",
			"consumer", proj.Name(),
			"partition_key", p.config.PartitionKey,
			"total_partitions", p.config.TotalPartitions,
			"previous_idle_delay", currentDelay,
			"next_idle_delay", p.config.PollInterval)
	}

	return p.config.PollInterval
}

func (p *BasicProcessor) waitForNextBatch(
	ctx context.Context, consumerName string, wakeupCh <-chan struct{}, delay time.Duration,
) (bool, error) {
	if p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "consumer idle wait starting",
			"consumer", consumerName,
			"partition_key", p.config.PartitionKey,
			"total_partitions", p.config.TotalPartitions,
			"idle_delay", delay,
			"wakeup_source_enabled", wakeupCh != nil)
	}

	if wakeupCh == nil {
		if delay <= 0 {
			if p.config.Logger != nil {
				p.config.Logger.Debug(ctx, "consumer idle wait skipped",
					"consumer", consumerName,
					"partition_key", p.config.PartitionKey,
					"total_partitions", p.config.TotalPartitions,
					"reason", "busy_poll_no_delay")
			}
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
			return true, p.applyWakeupJitter(ctx, consumerName)
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-wakeupCh:
		return true, p.applyWakeupJitter(ctx, consumerName)
	case <-timer.C:
		return false, nil
	}
}

func (p *BasicProcessor) applyWakeupJitter(ctx context.Context, consumerName string) error {
	if p.config.WakeupJitter <= 0 {
		if p.config.Logger != nil {
			p.config.Logger.Debug(ctx, "consumer wakeup jitter skipped",
				"consumer", consumerName,
				"partition_key", p.config.PartitionKey,
				"total_partitions", p.config.TotalPartitions,
				"reason", "jitter_disabled")
		}
		return nil
	}

	jitter := time.Duration(
		(time.Now().UnixNano() + int64(p.config.PartitionKey+1)) % int64(p.config.WakeupJitter),
	)
	if jitter <= 0 {
		if p.config.Logger != nil {
			p.config.Logger.Debug(ctx, "consumer wakeup jitter skipped",
				"consumer", consumerName,
				"partition_key", p.config.PartitionKey,
				"total_partitions", p.config.TotalPartitions,
				"reason", "computed_zero_jitter")
		}
		return nil
	}

	if p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "consumer wakeup jitter waiting",
			"consumer", consumerName,
			"partition_key", p.config.PartitionKey,
			"total_partitions", p.config.TotalPartitions,
			"jitter_delay", jitter)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(jitter):
		return nil
	}
}

func (p *BasicProcessor) nextPollDelay(current time.Duration) time.Duration {
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

// shouldProcessEvent checks if an event should be processed based on partition, aggregate type,
// and bounded context filters.
//
//nolint:gocritic // hugeParam: Intentionally pass by value to match event processing pattern
func (p *BasicProcessor) shouldProcessEvent(
	event es.PersistedEvent, aggregateTypeFilter, boundedContextFilter map[string]bool,
) bool {
	if !p.config.PartitionStrategy.ShouldProcess(
		event.AggregateID,
		p.config.PartitionKey,
		p.config.TotalPartitions,
	) {
		return false
	}

	if aggregateTypeFilter != nil && !aggregateTypeFilter[event.AggregateType] {
		return false
	}

	if boundedContextFilter != nil && !boundedContextFilter[event.BoundedContext] {
		return false
	}

	return true
}

func (p *BasicProcessor) processBatch(
	ctx context.Context,
	proj consumer.Consumer,
	aggregateTypeFilter, boundedContextFilter map[string]bool,
) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		tx.Rollback()
	}()

	checkpointName := p.checkpointName(proj.Name())
	checkpoint, err := p.store.GetCheckpoint(ctx, tx, checkpointName)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint: %w", err)
	}

	events, err := p.store.ReadEvents(ctx, tx, checkpoint, p.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to read events: %w", err)
	}

	if len(events) == 0 {
		return errors.New("no events in batch")
	}

	var lastPosition int64
	var processedCount int
	var skippedCount int
	for i := range events {
		event := events[i]

		if !p.shouldProcessEvent(event, aggregateTypeFilter, boundedContextFilter) {
			lastPosition = event.GlobalPosition
			skippedCount++
			continue
		}

		handlerErr := proj.Handle(ctx, tx, event)
		if handlerErr != nil {
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "consumer handler error",
					"consumer", proj.Name(),
					"position", event.GlobalPosition,
					"aggregate_type", event.AggregateType,
					"aggregate_id", event.AggregateID,
					"event_type", event.EventType,
					"error", handlerErr)
			}
			return fmt.Errorf("consumer handler error at position %d: %w", event.GlobalPosition, handlerErr)
		}

		lastPosition = event.GlobalPosition
		processedCount++
	}

	if lastPosition > 0 {
		err = p.store.UpdateCheckpoint(ctx, tx, checkpointName, lastPosition)
		if err != nil {
			return fmt.Errorf("failed to update checkpoint: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if p.config.Logger != nil {
		if processedCount > 0 {
			p.config.Logger.Info(ctx, "events processed",
				"consumer", proj.Name(),
				"processed", processedCount,
				"skipped", skippedCount,
				"checkpoint", lastPosition)
		} else {
			p.config.Logger.Debug(ctx, "batch processed",
				"consumer", proj.Name(),
				"processed", processedCount,
				"skipped", skippedCount,
				"checkpoint", lastPosition)
		}
	}

	return nil
}
