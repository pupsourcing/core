package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/store"
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

// segmentWorker tracks a running segment processing goroutine.
type segmentWorker struct {
	cancel    context.CancelFunc
	done      chan error
	segmentID int
}

// Run processes events for the given consumer until the context is canceled.
// It manages segment claiming, heartbeating, rebalancing, and per-segment processing goroutines.
func (p *SegmentProcessor) Run(ctx context.Context, cons consumer.Consumer) error {
	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "segment processor starting",
			"consumer", cons.Name(),
			"owner_id", p.ownerID,
			"total_segments", p.config.TotalSegments,
			"batch_size", p.config.BatchSize)
	}

	// Initialize segments (idempotent)
	if err := p.initializeSegments(ctx, cons.Name()); err != nil {
		return fmt.Errorf("failed to initialize segments: %w", err)
	}

	// Register this worker in the registry (makes it visible for fair-share calculations)
	if err := p.store.RegisterWorker(ctx, p.db, cons.Name(), p.ownerID); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	// Build filters once
	aggregateTypeFilter := buildAggregateTypeFilter(cons)
	boundedContextFilter := buildBoundedContextFilter(cons)

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
		p.heartbeatLoop(heartbeatCtx, cons.Name())
	}()

	// Handle RunModeOneOff
	if p.config.RunMode == consumer.RunModeOneOff {
		err := p.runOneOff(ctx, cons, aggregateTypeFilter, boundedContextFilter, &workersMu, workers, workerErrCh)
		cancelHeartbeat()
		<-heartbeatDone
		p.deregisterWorker(ctx, cons.Name())
		return err
	}

	// Main rebalance loop
	rebalanceTicker := time.NewTicker(p.config.RebalanceInterval)
	defer rebalanceTicker.Stop()

	// Perform initial rebalance immediately
	if err := p.rebalance(ctx, cons, aggregateTypeFilter, boundedContextFilter, &workersMu, workers, workerErrCh); err != nil {
		cancelHeartbeat()
		<-heartbeatDone
		p.stopAllWorkers(&workersMu, workers)
		p.deregisterWorker(ctx, cons.Name())
		return err
	}

	for {
		select {
		case <-ctx.Done():
			if p.config.Logger != nil {
				p.config.Logger.Info(ctx, "segment processor stopping",
					"consumer", cons.Name(),
					"reason", ctx.Err())
			}
			cancelHeartbeat()
			<-heartbeatDone
			p.stopAllWorkers(&workersMu, workers)
			p.releaseAllSegments(context.Background(), cons.Name())
			p.deregisterWorker(context.Background(), cons.Name())
			return ctx.Err()

		case err := <-workerErrCh:
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "segment worker error",
					"consumer", cons.Name(),
					"error", err)
			}
			cancelHeartbeat()
			<-heartbeatDone
			p.stopAllWorkers(&workersMu, workers)
			p.releaseAllSegments(context.Background(), cons.Name())
			p.deregisterWorker(context.Background(), cons.Name())
			return fmt.Errorf("%w: %v", ErrConsumerStopped, err)

		case <-rebalanceTicker.C:
			if err := p.rebalance(ctx, cons, aggregateTypeFilter, boundedContextFilter, &workersMu, workers, workerErrCh); err != nil {
				cancelHeartbeat()
				<-heartbeatDone
				p.stopAllWorkers(&workersMu, workers)
				p.releaseAllSegments(context.Background(), cons.Name())
				p.deregisterWorker(context.Background(), cons.Name())
				return err
			}
		}
	}
}

// runOneOff handles RunModeOneOff by claiming all segments, processing to completion, and releasing.
func (p *SegmentProcessor) runOneOff(ctx context.Context, cons consumer.Consumer, aggregateTypeFilter, boundedContextFilter map[string]bool, workersMu *sync.Mutex, workers map[int]*segmentWorker, workerErrCh chan error) error {
	// Claim all available segments
	for {
		seg, err := p.claimSegment(ctx, cons.Name())
		if err != nil {
			return fmt.Errorf("failed to claim segment: %w", err)
		}
		if seg == nil {
			break
		}

		p.startSegmentWorker(ctx, cons, seg.SegmentID, aggregateTypeFilter, boundedContextFilter, workersMu, workers, workerErrCh)
	}

	workersMu.Lock()
	claimed := len(workers)
	workersMu.Unlock()

	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "one-off mode: all segments claimed",
			"consumer", cons.Name(),
			"segments_claimed", claimed)
	}

	// Wait for all workers to complete
	workersMu.Lock()
	for len(workers) > 0 {
		// Get a worker to wait on
		var w *segmentWorker
		for _, worker := range workers {
			w = worker
			break
		}
		workersMu.Unlock()

		// Wait for this worker
		select {
		case err := <-w.done:
			if err != nil {
				p.stopAllWorkers(workersMu, workers)
				p.releaseAllSegments(context.Background(), cons.Name())
				return err
			}
		case <-ctx.Done():
			p.stopAllWorkers(workersMu, workers)
			p.releaseAllSegments(context.Background(), cons.Name())
			return ctx.Err()
		}

		workersMu.Lock()
	}
	workersMu.Unlock()

	// Release all segments
	p.releaseAllSegments(context.Background(), cons.Name())

	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "one-off mode: processing complete",
			"consumer", cons.Name())
	}

	return nil
}

// heartbeatLoop periodically refreshes the worker registry entry.
func (p *SegmentProcessor) heartbeatLoop(ctx context.Context, consumerName string) {
	ticker := time.NewTicker(p.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.store.RegisterWorker(ctx, p.db, consumerName, p.ownerID); err != nil {
				if p.config.Logger != nil {
					p.config.Logger.Error(ctx, "worker registry heartbeat failed",
						"consumer", consumerName,
						"owner_id", p.ownerID,
						"error", err)
				}
			}
		}
	}
}

// rebalance performs fair-share rebalancing: purge stale workers, reclaim stale segments, release excess, claim available.
func (p *SegmentProcessor) rebalance(ctx context.Context, cons consumer.Consumer, aggregateTypeFilter, boundedContextFilter map[string]bool, workersMu *sync.Mutex, workers map[int]*segmentWorker, workerErrCh chan error) error {
	// Purge stale worker registry entries
	if err := p.store.PurgeStaleWorkers(ctx, p.db, cons.Name(), p.config.StaleThreshold); err != nil {
		return fmt.Errorf("failed to purge stale workers: %w", err)
	}

	// Reclaim stale segments
	if err := p.reclaimStale(ctx, cons.Name()); err != nil {
		return err
	}

	// Calculate fair share
	mySegments, fairShare, err := p.calculateFairShare(ctx, cons.Name())
	if err != nil {
		return err
	}

	// Release excess segments
	if err := p.releaseExcess(ctx, cons.Name(), mySegments, fairShare, workersMu, workers); err != nil {
		return err
	}

	// Claim available segments (up to fair share)
	return p.claimAvailable(ctx, cons, fairShare, aggregateTypeFilter, boundedContextFilter, workersMu, workers, workerErrCh)
}

// reclaimStale reclaims stale segments from failed workers.
func (p *SegmentProcessor) reclaimStale(ctx context.Context, consumerName string) error {
	reclaimed, err := p.store.ReclaimStaleSegments(ctx, p.db, consumerName, p.config.StaleThreshold)
	if err != nil {
		return fmt.Errorf("failed to reclaim stale segments: %w", err)
	}

	if reclaimed > 0 && p.config.Logger != nil {
		p.config.Logger.Info(ctx, "stale segments reclaimed",
			"consumer", consumerName,
			"count", reclaimed)
	}

	return nil
}

// calculateFairShare determines the fair share of segments for this worker.
// calculateFairShare determines the fair share of segments for this worker using rank-based assignment.
// Each worker finds its position in a sorted list of active workers and deterministically computes
// whether it gets floor(total/workers) or floor(total/workers)+1 segments. The first (total % workers)
// workers in sorted order get the extra segment.
func (p *SegmentProcessor) calculateFairShare(ctx context.Context, consumerName string) ([]*store.Segment, int, error) {
	segments, err := p.store.GetSegments(ctx, p.db, consumerName)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get segments: %w", err)
	}

	mySegments := make([]*store.Segment, 0)
	for i := range segments {
		seg := &segments[i]
		if seg.OwnerID != nil && *seg.OwnerID == p.ownerID {
			mySegments = append(mySegments, seg)
		}
	}

	activeWorkers, err := p.store.ListActiveWorkers(ctx, p.db, consumerName, p.config.StaleThreshold)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list active workers: %w", err)
	}
	if len(activeWorkers) == 0 {
		activeWorkers = []string{p.ownerID}
	}

	// Find my rank in the sorted worker list
	myRank := -1
	for i, wid := range activeWorkers {
		if wid == p.ownerID {
			myRank = i
			break
		}
	}
	if myRank == -1 {
		// Not in registry yet (shouldn't happen, but be safe)
		myRank = len(activeWorkers)
		activeWorkers = append(activeWorkers, p.ownerID)
	}

	// Rank-based fair share: first (remainder) workers get floor+1, rest get floor
	floor := p.config.TotalSegments / len(activeWorkers)
	remainder := p.config.TotalSegments % len(activeWorkers)

	fairShare := floor
	if myRank < remainder {
		fairShare = floor + 1
	}

	if p.config.Logger != nil {
		p.config.Logger.Debug(ctx, "rebalance check",
			"consumer", consumerName,
			"my_segments", len(mySegments),
			"active_workers", len(activeWorkers),
			"my_rank", myRank,
			"fair_share", fairShare)
	}

	return mySegments, fairShare, nil
}

// releaseExcess releases segments beyond fair share.
func (p *SegmentProcessor) releaseExcess(ctx context.Context, consumerName string, mySegments []*store.Segment, fairShare int, workersMu *sync.Mutex, workers map[int]*segmentWorker) error {
	if len(mySegments) <= fairShare {
		return nil
	}

	excess := len(mySegments) - fairShare
	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "releasing excess segments",
			"consumer", consumerName,
			"count", excess)
	}

	for i := 0; i < excess; i++ {
		seg := mySegments[len(mySegments)-1-i]
		p.stopSegmentWorker(seg.SegmentID, workersMu, workers)
		if err := p.store.ReleaseSegment(ctx, p.db, consumerName, seg.SegmentID, p.ownerID); err != nil {
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "failed to release segment",
					"consumer", consumerName,
					"segment_id", seg.SegmentID,
					"error", err)
			}
		}
	}

	return nil
}

// claimAvailable claims available segments up to fair share.
func (p *SegmentProcessor) claimAvailable(ctx context.Context, cons consumer.Consumer, fairShare int, aggregateTypeFilter, boundedContextFilter map[string]bool, workersMu *sync.Mutex, workers map[int]*segmentWorker, workerErrCh chan error) error {
	workersMu.Lock()
	currentCount := len(workers)
	workersMu.Unlock()

	for currentCount < fairShare {
		seg, err := p.claimSegment(ctx, cons.Name())
		if err != nil {
			return fmt.Errorf("failed to claim segment: %w", err)
		}
		if seg == nil {
			break
		}

		p.startSegmentWorker(ctx, cons, seg.SegmentID, aggregateTypeFilter, boundedContextFilter, workersMu, workers, workerErrCh)

		workersMu.Lock()
		currentCount = len(workers)
		workersMu.Unlock()
	}

	return nil
}

// startSegmentWorker starts a processing goroutine for a segment.
func (p *SegmentProcessor) startSegmentWorker(ctx context.Context, cons consumer.Consumer, segmentID int, aggregateTypeFilter, boundedContextFilter map[string]bool, workersMu *sync.Mutex, workers map[int]*segmentWorker, workerErrCh chan error) {
	workersMu.Lock()
	defer workersMu.Unlock()

	// Check if already running
	if _, exists := workers[segmentID]; exists {
		return
	}

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)

	worker := &segmentWorker{
		segmentID: segmentID,
		cancel:    cancel,
		done:      done,
	}

	workers[segmentID] = worker

	if p.config.Logger != nil {
		p.config.Logger.Info(workerCtx, "segment worker starting",
			"consumer", cons.Name(),
			"segment_id", segmentID,
			"owner_id", p.ownerID)
	}

	go func() {
		err := p.processSegment(workerCtx, cons, segmentID, aggregateTypeFilter, boundedContextFilter)
		done <- err
		close(done)

		// Remove from workers map
		workersMu.Lock()
		delete(workers, segmentID)
		workersMu.Unlock()

		// Report error to main loop (context cancellation is expected during rebalance/shutdown)
		if err != nil && !errors.Is(err, context.Canceled) {
			select {
			case workerErrCh <- err:
			default:
				// Main loop already has an error
			}
		}
	}()
}

// stopSegmentWorker stops a segment processing goroutine.
func (p *SegmentProcessor) stopSegmentWorker(segmentID int, workersMu *sync.Mutex, workers map[int]*segmentWorker) {
	workersMu.Lock()
	worker, exists := workers[segmentID]
	if !exists {
		workersMu.Unlock()
		return
	}
	workersMu.Unlock()

	worker.cancel()
	<-worker.done
}

// stopAllWorkers stops all segment processing goroutines.
func (p *SegmentProcessor) stopAllWorkers(workersMu *sync.Mutex, workers map[int]*segmentWorker) {
	workersMu.Lock()
	workerList := make([]*segmentWorker, 0, len(workers))
	for _, w := range workers {
		workerList = append(workerList, w)
	}
	workersMu.Unlock()

	for _, w := range workerList {
		w.cancel()
	}

	for _, w := range workerList {
		<-w.done
	}
}

// initializeSegments creates segments if they don't exist (idempotent).
func (p *SegmentProcessor) initializeSegments(ctx context.Context, consumerName string) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		tx.Rollback()
	}()

	if err := p.store.InitializeSegments(ctx, tx, consumerName, p.config.TotalSegments); err != nil {
		return err
	}

	return tx.Commit()
}

// claimSegment atomically claims one unclaimed segment.
// MySQL ClaimSegment requires a transaction.
func (p *SegmentProcessor) claimSegment(ctx context.Context, consumerName string) (*store.Segment, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		tx.Rollback()
	}()

	seg, err := p.store.ClaimSegment(ctx, tx, consumerName, p.ownerID)
	if err != nil {
		return nil, err
	}

	if seg == nil {
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit claim: %w", err)
	}

	return seg, nil
}

// releaseAllSegments releases all owned segments.
func (p *SegmentProcessor) releaseAllSegments(ctx context.Context, consumerName string) {
	// Query DB for owned segments instead of relying on the workers map,
	// which may be empty after stopAllWorkers due to goroutine self-cleanup.
	segments, err := p.store.GetSegments(ctx, p.db, consumerName)
	if err != nil {
		if p.config.Logger != nil {
			p.config.Logger.Error(ctx, "failed to query segments for release",
				"consumer", consumerName,
				"error", err)
		}
		return
	}

	released := 0
	for _, seg := range segments {
		if seg.OwnerID == nil || *seg.OwnerID != p.ownerID {
			continue
		}
		if err := p.store.ReleaseSegment(ctx, p.db, consumerName, seg.SegmentID, p.ownerID); err != nil {
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "failed to release segment during shutdown",
					"consumer", consumerName,
					"segment_id", seg.SegmentID,
					"error", err)
			}
		} else {
			released++
		}
	}

	if p.config.Logger != nil && released > 0 {
		p.config.Logger.Info(ctx, "segments released",
			"consumer", consumerName,
			"count", released)
	}
}

// deregisterWorker removes this worker from the registry during shutdown.
func (p *SegmentProcessor) deregisterWorker(ctx context.Context, consumerName string) {
	if err := p.store.DeregisterWorker(ctx, p.db, consumerName, p.ownerID); err != nil {
		if p.config.Logger != nil {
			p.config.Logger.Error(ctx, "failed to deregister worker",
				"consumer", consumerName,
				"owner_id", p.ownerID,
				"error", err)
		}
	}
}

// processSegment is the main event processing loop for a single segment.
func (p *SegmentProcessor) processSegment(ctx context.Context, cons consumer.Consumer, segmentID int, aggregateTypeFilter, boundedContextFilter map[string]bool) error {
	var wakeupCh <-chan struct{}
	unsubscribe := func() {}
	if p.config.WakeupSource != nil {
		wakeupCh, unsubscribe = p.config.WakeupSource.Subscribe()
	}
	defer unsubscribe()

	idleDelay := p.config.PollInterval

	for {
		select {
		case <-ctx.Done():
			if p.config.Logger != nil {
				p.config.Logger.Info(ctx, "segment worker stopped",
					"consumer", cons.Name(),
					"segment_id", segmentID,
					"reason", ctx.Err())
			}
			return ctx.Err()
		default:
		}

		// Process batch
		err := p.processSegmentBatch(ctx, cons, segmentID, aggregateTypeFilter, boundedContextFilter)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errNoEventsInBatch) {
				// Handle idle cycle
				if p.config.RunMode == consumer.RunModeOneOff {
					if p.config.Logger != nil {
						p.config.Logger.Info(ctx, "segment worker caught up (one-off mode)",
							"consumer", cons.Name(),
							"segment_id", segmentID)
					}
					return nil
				}

				if p.config.Logger != nil {
					p.config.Logger.Debug(ctx, "segment idle polling",
						"consumer", cons.Name(),
						"segment_id", segmentID,
						"idle_delay", idleDelay)
				}

				wokeBySignal, waitErr := p.waitForNextBatch(ctx, cons.Name(), segmentID, wakeupCh, idleDelay)
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
					"consumer", cons.Name(),
					"segment_id", segmentID,
					"error", err)
			}
			return err
		}

		// Reset idle delay on successful batch
		if idleDelay != p.config.PollInterval {
			idleDelay = p.config.PollInterval
		}
	}
}

// processSegmentBatch processes one batch of events for a segment.
func (p *SegmentProcessor) processSegmentBatch(ctx context.Context, cons consumer.Consumer, segmentID int, aggregateTypeFilter, boundedContextFilter map[string]bool) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		tx.Rollback()
	}()

	// Get segment checkpoint
	checkpoint, err := p.store.GetSegmentCheckpoint(ctx, tx, cons.Name(), segmentID)
	if err != nil {
		return fmt.Errorf("failed to get segment checkpoint: %w", err)
	}

	// Read events
	events, err := p.store.ReadEvents(ctx, tx, checkpoint, p.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to read events: %w", err)
	}

	if len(events) == 0 {
		return errNoEventsInBatch
	}

	// Process events with filtering
	var lastPosition int64
	var processedCount int
	var skippedCount int
	for i := range events {
		event := events[i]

		// Check if this segment should process this event
		if !p.shouldProcessEvent(event, segmentID, aggregateTypeFilter, boundedContextFilter) {
			lastPosition = event.GlobalPosition
			skippedCount++
			continue
		}

		// Handle event
		handlerErr := cons.Handle(ctx, tx, event)
		if handlerErr != nil {
			if p.config.Logger != nil {
				p.config.Logger.Error(ctx, "consumer handler error",
					"consumer", cons.Name(),
					"segment_id", segmentID,
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

	// Update segment checkpoint
	if lastPosition > 0 {
		err = p.store.UpdateSegmentCheckpoint(ctx, tx, cons.Name(), segmentID, lastPosition)
		if err != nil {
			return fmt.Errorf("failed to update segment checkpoint: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if p.config.Logger != nil {
		if processedCount > 0 {
			p.config.Logger.Info(ctx, "segment events processed",
				"consumer", cons.Name(),
				"segment_id", segmentID,
				"processed", processedCount,
				"skipped", skippedCount,
				"checkpoint", lastPosition)
		} else {
			p.config.Logger.Debug(ctx, "segment batch processed",
				"consumer", cons.Name(),
				"segment_id", segmentID,
				"processed", processedCount,
				"skipped", skippedCount,
				"checkpoint", lastPosition)
		}
	}

	return nil
}

// shouldProcessEvent checks if this segment should process the event based on partition strategy and filters.
//
//nolint:gocritic // hugeParam: Intentionally pass by value to match event processing pattern
func (p *SegmentProcessor) shouldProcessEvent(event es.PersistedEvent, segmentID int, aggregateTypeFilter, boundedContextFilter map[string]bool) bool {
	// Apply partition filter using segment as partition key
	if !p.config.PartitionStrategy.ShouldProcess(
		event.AggregateID,
		segmentID,
		p.config.TotalSegments,
	) {
		return false
	}

	// Apply aggregate type filter
	if aggregateTypeFilter != nil && !aggregateTypeFilter[event.AggregateType] {
		return false
	}

	// Apply bounded context filter
	if boundedContextFilter != nil && !boundedContextFilter[event.BoundedContext] {
		return false
	}

	return true
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

// buildAggregateTypeFilter builds a filter map for scoped consumers with aggregate types.
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
