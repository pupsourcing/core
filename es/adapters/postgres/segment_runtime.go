package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// segmentWorker tracks a running segment processing goroutine.
type segmentWorker struct {
	cancel    context.CancelFunc
	done      chan error
	segmentID int
}

// runOneOff handles RunModeOneOff by claiming all segments, processing to completion, and releasing.
func (p *SegmentProcessor) runOneOff(
	ctx context.Context,
	plan *segmentProcessPlan,
	workersMu *sync.Mutex,
	workers map[int]*segmentWorker,
	workerErrCh chan error,
) error {
	// Claim all available segments
	for {
		seg, err := p.store.ClaimSegment(ctx, p.db, plan.name, p.ownerID)
		if err != nil {
			return fmt.Errorf("failed to claim segment: %w", err)
		}
		if seg == nil {
			break
		}

		p.startSegmentWorker(ctx, plan, seg.SegmentID, workersMu, workers, workerErrCh)
	}

	if p.config.Logger != nil {
		workersMu.Lock()
		claimed := len(workers)
		workersMu.Unlock()
		p.config.Logger.Info(ctx, "one-off mode: all segments claimed",
			"consumer", plan.name,
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
				p.releaseAllSegments(context.Background(), plan.name)
				return err
			}
		case <-ctx.Done():
			p.stopAllWorkers(workersMu, workers)
			p.releaseAllSegments(context.Background(), plan.name)
			return ctx.Err()
		}

		workersMu.Lock()
	}
	workersMu.Unlock()

	// Release all segments
	p.releaseAllSegments(context.Background(), plan.name)

	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "one-off mode: processing complete",
			"consumer", plan.name)
	}

	return nil
}

// startSegmentWorker starts a processing goroutine for a segment.
func (p *SegmentProcessor) startSegmentWorker(
	ctx context.Context,
	plan *segmentProcessPlan,
	segmentID int,
	workersMu *sync.Mutex,
	workers map[int]*segmentWorker,
	workerErrCh chan error,
) {
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
			"consumer", plan.name,
			"segment_id", segmentID,
			"owner_id", p.ownerID)
	}

	go func() {
		err := p.processSegment(workerCtx, plan, segmentID)
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
