package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pupsourcing/core/es/store"
)

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

func (p *SegmentProcessor) healthAuditLoop(ctx context.Context, consumerName string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			active, err := p.store.isWorkerActive(ctx, p.db, consumerName, p.ownerID, p.config.StaleThreshold)
			if err != nil {
				if p.config.Logger != nil {
					p.config.Logger.Error(ctx, "worker health audit failed",
						"consumer", consumerName,
						"owner_id", p.ownerID,
						"error", err)
				}
				continue
			}

			if active {
				continue
			}

			if p.config.Logger != nil {
				p.config.Logger.Info(ctx, "worker health audit restoring registry entry",
					"consumer", consumerName,
					"owner_id", p.ownerID)
			}

			if err := p.store.RegisterWorker(ctx, p.db, consumerName, p.ownerID); err != nil {
				if p.config.Logger != nil {
					p.config.Logger.Error(ctx, "worker health audit restore failed",
						"consumer", consumerName,
						"owner_id", p.ownerID,
						"error", err)
				}
				continue
			}

			if p.config.Logger != nil {
				p.config.Logger.Info(ctx, "worker health audit restored registry entry",
					"consumer", consumerName,
					"owner_id", p.ownerID)
			}
		}
	}
}

func (p *SegmentProcessor) healthAuditInterval() time.Duration {
	if p.config == nil {
		return 0
	}

	switch {
	case p.config.HealthAuditInterval < 0:
		return 0
	case p.config.HealthAuditInterval > 0:
		return p.config.HealthAuditInterval
	case p.config.StaleThreshold <= 0:
		return 0
	}

	interval := p.config.StaleThreshold / 2
	if interval <= 0 {
		return p.config.StaleThreshold
	}

	return interval
}

// rebalance performs fair-share rebalancing: purge stale workers, reclaim stale segments, release excess, claim available.
func (p *SegmentProcessor) rebalance(
	ctx context.Context,
	plan *segmentProcessPlan,
	workersMu *sync.Mutex,
	workers map[int]*segmentWorker,
	workerErrCh chan error,
) error {
	// Purge stale worker registry entries
	if err := p.store.PurgeStaleWorkers(ctx, p.db, plan.name, p.config.StaleThreshold); err != nil {
		return fmt.Errorf("failed to purge stale workers: %w", err)
	}

	// Reclaim stale segments
	if err := p.reclaimStale(ctx, plan.name); err != nil {
		return err
	}

	// Calculate fair share
	mySegments, fairShare, err := p.calculateFairShare(ctx, plan.name)
	if err != nil {
		return err
	}

	// Release excess segments and compute current DB ownership after release attempts.
	currentOwned, err := p.releaseExcess(ctx, plan.name, mySegments, fairShare, workersMu, workers)
	if err != nil {
		return err
	}

	// Claim available segments (up to fair share)
	return p.claimAvailable(ctx, plan, currentOwned, fairShare, workersMu, workers, workerErrCh)
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
// It returns the number of segments still owned in DB after release attempts.
func (p *SegmentProcessor) releaseExcess(
	ctx context.Context,
	consumerName string,
	mySegments []*store.Segment,
	fairShare int,
	workersMu *sync.Mutex,
	workers map[int]*segmentWorker,
) (int, error) {
	if len(mySegments) <= fairShare {
		return len(mySegments), nil
	}

	excess := len(mySegments) - fairShare
	if p.config.Logger != nil {
		p.config.Logger.Info(ctx, "releasing excess segments",
			"consumer", consumerName,
			"count", excess)
	}

	released := 0
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
			continue
		}
		released++
	}

	return len(mySegments) - released, nil
}

// claimAvailable claims available segments up to fair share.
func (p *SegmentProcessor) claimAvailable(
	ctx context.Context,
	plan *segmentProcessPlan,
	currentOwned int,
	fairShare int,
	workersMu *sync.Mutex,
	workers map[int]*segmentWorker,
	workerErrCh chan error,
) error {
	ownedCount := currentOwned
	for ownedCount < fairShare {
		seg, err := p.store.ClaimSegment(ctx, p.db, plan.name, p.ownerID)
		if err != nil {
			return fmt.Errorf("failed to claim segment: %w", err)
		}
		if seg == nil {
			break
		}

		p.startSegmentWorker(ctx, plan, seg.SegmentID, workersMu, workers, workerErrCh)
		ownedCount++
	}

	return nil
}
