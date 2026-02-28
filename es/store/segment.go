// Segment store types for consumer coordination and horizontal auto-scaling.
//
// # Overview
//
// SegmentStore is consumer coordination infrastructure, not event storage.
// While adapters implement it alongside EventStore (to share DB connections),
// segments solve a fundamentally different problem: distributing work across
// consumer instances without restarts or manual reconfiguration.
//
// # How Segments Enable Horizontal Scaling
//
// A logical consumer (e.g., "order-processor") can be split into N segments.
// Each segment:
//   - Maps to a partition of the event space via PartitionStrategy
//   - Can be claimed by any worker instance
//   - Tracks its own checkpoint independently
//   - Can be released, reclaimed, or rebalanced dynamically
//
// This allows M workers to process N segments with automatic work distribution,
// failure recovery, and elastic scaling without changing configuration or restarting.
//
// # Relationship to Partition Strategies
//
// The segment count (totalSegments) should match the partition count in your
// PartitionStrategy. For example:
//   - Hash-based partitioning with 16 buckets → 16 segments
//   - Each segment processes events where (hash % 16) == segmentID
//   - Workers claim segments dynamically; segments can move between workers
//
// The partition strategy is configurable and independent of the segment store.
// See the partition package for available strategies.
//
// # Why Segments Live in store/
//
// Segments are defined here (alongside EventStore interfaces) for pragmatic reasons:
//   - Adapters implement both EventStore and SegmentStore
//   - Both share the same underlying database connection
//   - Both are persistence contracts that adapters must fulfill
//   - The store/ package serves as the "all adapter contracts" package
//
// This co-location does not imply conceptual coupling. Segments coordinate consumers;
// EventStore persists events. They're separate concerns with a shared adapter boundary.
package store

import (
	"context"
	"time"

	"github.com/pupsourcing/core/es"
)

// Segment represents a consumer segment in a distributed event processing system.
// Each segment tracks ownership, processing position, and heartbeat status for
// a partition of the event stream.
type Segment struct {
	OwnerID       *string    // nil = unclaimed
	LastHeartbeat *time.Time // nil if never heartbeated
	ConsumerName  string
	Checkpoint    int64
	SegmentID     int
	TotalSegments int
}

// SegmentStore defines the interface for managing consumer segments.
// Segments enable multiple consumer instances to coordinate processing of
// events from a single logical consumer, with automatic claim management
// and stale segment reclamation.
type SegmentStore interface {
	// InitializeSegments pre-creates N segments for a consumer.
	// This operation is idempotent — segments that already exist are skipped.
	// All segments are created unclaimed (OwnerID = nil) with checkpoint = 0.
	//
	// Parameters:
	// - consumerName: the logical consumer name (e.g., "order-processor")
	// - totalSegments: the number of segments to create (must be > 0)
	//
	// This should typically be called once during consumer deployment or
	// when scaling up the number of segments. Existing segments are preserved.
	InitializeSegments(ctx context.Context, tx es.DBTX, consumerName string, totalSegments int) error

	// ClaimSegment atomically claims one unclaimed segment for this owner.
	// Returns the claimed segment, or nil if no unclaimed segments are available.
	//
	// The claim operation:
	// - Finds an unclaimed segment (OwnerID = nil)
	// - Atomically sets OwnerID to the provided ownerID
	// - Returns the segment with its current checkpoint and metadata
	//
	// Parameters:
	// - consumerName: the logical consumer name
	// - ownerID: unique identifier for the worker instance claiming the segment
	//
	// Returns:
	// - The claimed Segment if successful
	// - nil if no unclaimed segments are available (all segments are claimed)
	// - error if the operation fails
	//
	// Workers should call this method periodically to acquire additional segments
	// when scaling up or when segments become available.
	ClaimSegment(ctx context.Context, tx es.DBTX, consumerName, ownerID string) (*Segment, error)

	// ReleaseSegment releases a specific segment owned by this owner.
	// This operation is a no-op if the segment is not owned by this owner.
	//
	// The release operation:
	// - Sets OwnerID to nil for the specified segment
	// - Only succeeds if the current OwnerID matches the provided ownerID
	// - Preserves the segment's checkpoint for the next owner
	//
	// Parameters:
	// - consumerName: the logical consumer name
	// - segmentID: the specific segment to release
	// - ownerID: the owner releasing the segment (must match current owner)
	//
	// Workers should call this during graceful shutdown or when rebalancing segments.
	ReleaseSegment(ctx context.Context, tx es.DBTX, consumerName string, segmentID int, ownerID string) error

	// Heartbeat updates last_heartbeat for all segments owned by this owner.
	// This prevents the segments from being reclaimed as stale by other workers.
	//
	// Parameters:
	// - consumerName: the logical consumer name
	// - ownerID: the owner sending the heartbeat
	//
	// Workers should call this method periodically (e.g., every 5-10 seconds)
	// to maintain ownership of their claimed segments.
	Heartbeat(ctx context.Context, tx es.DBTX, consumerName, ownerID string) error

	// ReclaimStaleSegments releases segments whose heartbeat is older than the threshold.
	// Returns the number of segments reclaimed.
	//
	// The reclaim operation:
	// - Finds segments where (now - last_heartbeat) > staleThreshold
	// - Sets OwnerID to nil for those segments
	// - Preserves checkpoints so processing can resume
	//
	// Parameters:
	// - consumerName: the logical consumer name
	// - staleThreshold: age threshold for considering a segment stale (e.g., 30 seconds)
	//
	// Returns:
	// - The count of segments that were reclaimed
	// - error if the operation fails
	//
	// Workers should call this method periodically (e.g., every 10-15 seconds)
	// to detect and reclaim segments from failed or unresponsive instances.
	ReclaimStaleSegments(ctx context.Context, tx es.DBTX, consumerName string, staleThreshold time.Duration) (int, error)

	// GetSegments returns all segments for a consumer.
	// This is primarily used for monitoring, debugging, and fair-share calculations
	// to determine if a worker should release segments to other workers.
	//
	// Parameters:
	// - consumerName: the logical consumer name
	//
	// Returns:
	// - A slice of all segments for the consumer, ordered by segment_id
	// - Empty slice if no segments exist (consumer not initialized)
	GetSegments(ctx context.Context, tx es.DBTX, consumerName string) ([]Segment, error)

	// GetSegmentCheckpoint returns the checkpoint for a specific segment.
	// The checkpoint indicates the last successfully processed global position
	// for this segment.
	//
	// Parameters:
	// - consumerName: the logical consumer name
	// - segmentID: the segment identifier
	//
	// Returns:
	// - The checkpoint position (0 if segment doesn't exist or hasn't processed events)
	// - error if the operation fails
	GetSegmentCheckpoint(ctx context.Context, tx es.DBTX, consumerName string, segmentID int) (int64, error)

	// UpdateSegmentCheckpoint updates the checkpoint for a specific segment.
	// This records the last successfully processed global position for the segment.
	//
	// Parameters:
	// - consumerName: the logical consumer name
	// - segmentID: the segment identifier
	// - position: the new checkpoint position (must be >= current checkpoint)
	//
	// Workers should call this method after successfully processing each batch
	// of events to enable resumption from the correct position after restarts.
	UpdateSegmentCheckpoint(ctx context.Context, tx es.DBTX, consumerName string, segmentID int, position int64) error
}
