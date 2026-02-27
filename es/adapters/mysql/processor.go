package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/consumer"
)

var (
	// ErrConsumerStopped indicates the consumer was stopped due to an error.
	ErrConsumerStopped = errors.New("consumer stopped")
)

// Processor processes events for consumers using MySQL for checkpointing.
// This is the MySQL-specific implementation that manages transactions internally.
type Processor struct {
	db     *sql.DB
	store  *Store
	config *consumer.ProcessorConfig
}

// NewProcessor creates a new MySQL consumer processor.
// The processor manages SQL transactions internally and coordinates checkpointing with event processing.
func NewProcessor(db *sql.DB, store *Store, config *consumer.ProcessorConfig) *Processor {
	return &Processor{
		db:     db,
		store:  store,
		config: config,
	}
}

// checkpointName returns the checkpoint name for this processor.
// When partitioning is enabled (TotalPartitions > 1), it appends the partition key to ensure
// each partition tracks its checkpoint independently.
func (p *Processor) checkpointName(consumerName string) string {
	if p.config.TotalPartitions > 1 {
		return fmt.Sprintf("%s_p%d", consumerName, p.config.PartitionKey)
	}
	return consumerName
}

// Run processes events for the given consumer until the context is canceled.
// It reads events in batches, applies partition and aggregate type filters, and updates checkpoints.
// Returns an error if the consumer handler fails.
func (p *Processor) Run(ctx context.Context, proj consumer.Consumer) error {
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

	// Build aggregate type and bounded context filters once for the consumer (not per batch)
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

		// Process batch in transaction
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

func (p *Processor) handleIdleCycle(ctx context.Context, proj consumer.Consumer, wakeupCh <-chan struct{}, idleDelay time.Duration) (time.Duration, bool, error) {
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

func (p *Processor) resetIdleDelay(ctx context.Context, proj consumer.Consumer, currentDelay time.Duration) time.Duration {
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

func (p *Processor) waitForNextBatch(ctx context.Context, consumerName string, wakeupCh <-chan struct{}, delay time.Duration) (bool, error) {
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

func (p *Processor) applyWakeupJitter(ctx context.Context, consumerName string) error {
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

	jitter := time.Duration((time.Now().UnixNano() + int64(p.config.PartitionKey+1)) % int64(p.config.WakeupJitter))
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

func (p *Processor) nextPollDelay(current time.Duration) time.Duration {
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

// buildAggregateTypeFilter builds a filter map for scoped consumers.
// Returns nil if the consumer is not scoped or has an empty aggregate types list.
func buildAggregateTypeFilter(proj consumer.Consumer) map[string]bool {
	scopedProj, ok := proj.(consumer.ScopedConsumer)
	if !ok {
		return nil
	}

	types := scopedProj.AggregateTypes()
	if len(types) == 0 {
		return nil
	}

	filter := make(map[string]bool, len(types))
	for _, aggType := range types {
		filter[aggType] = true
	}
	return filter
}

// buildBoundedContextFilter builds a filter map for scoped consumers with bounded contexts.
// Returns nil if the consumer is not scoped or has an empty bounded contexts list.
func buildBoundedContextFilter(proj consumer.Consumer) map[string]bool {
	scopedProj, ok := proj.(consumer.ScopedConsumer)
	if !ok {
		return nil
	}

	contexts := scopedProj.BoundedContexts()
	if len(contexts) == 0 {
		return nil
	}

	filter := make(map[string]bool, len(contexts))
	for _, ctx := range contexts {
		filter[ctx] = true
	}
	return filter
}

// shouldProcessEvent checks if an event should be processed based on partition, aggregate type, and bounded context filters.
//
//nolint:gocritic // hugeParam: Intentionally pass by value to match event processing pattern
func (p *Processor) shouldProcessEvent(event es.PersistedEvent, aggregateTypeFilter, boundedContextFilter map[string]bool) bool {
	// Apply partition filter
	if !p.config.PartitionStrategy.ShouldProcess(
		event.AggregateID,
		p.config.PartitionKey,
		p.config.TotalPartitions,
	) {
		return false
	}

	// Apply aggregate type filter if consumer is scoped
	if aggregateTypeFilter != nil && !aggregateTypeFilter[event.AggregateType] {
		return false
	}

	// Apply bounded context filter if consumer is scoped
	if boundedContextFilter != nil && !boundedContextFilter[event.BoundedContext] {
		return false
	}

	return true
}

func (p *Processor) processBatch(ctx context.Context, proj consumer.Consumer, aggregateTypeFilter, boundedContextFilter map[string]bool) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		tx.Rollback()
	}()

	// Get current checkpoint - use partition-aware name
	checkpointName := p.checkpointName(proj.Name())
	checkpoint, err := p.store.GetCheckpoint(ctx, tx, checkpointName)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint: %w", err)
	}

	// Read events
	events, err := p.store.ReadEvents(ctx, tx, checkpoint, p.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to read events: %w", err)
	}

	if len(events) == 0 {
		return errors.New("no events in batch")
	}

	// Process events with partition filter and aggregate type filter
	// Note: Events are passed by value to consumer handlers to enforce immutability.
	// This creates a 232-byte copy per event, but large data (Payload, Metadata) is not deep-copied
	// since slices share references to their backing arrays. The immutability guarantee
	// is more valuable than the minimal copy cost in event processing workloads.
	var lastPosition int64
	var processedCount int
	var skippedCount int
	for i := range events {
		event := events[i]

		// Check if event should be processed
		if !p.shouldProcessEvent(event, aggregateTypeFilter, boundedContextFilter) {
			lastPosition = event.GlobalPosition
			skippedCount++
			continue
		}

		// Handle event - consumer can use the transaction for atomic updates
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

	// Update checkpoint - use partition-aware name
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
		// Log at Info level when events are actually processed, Debug for skipped-only batches
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
