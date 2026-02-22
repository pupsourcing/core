// Package projection provides projection processing capabilities.
package projection

import (
	"context"
	"database/sql"
	"hash/fnv"
	"time"

	"github.com/getpup/pupsourcing/es"
)

// Projection defines the interface for event projection handlers.
// Projections are storage-agnostic and can write to any destination
// (SQL databases, NoSQL stores, message brokers, search engines, etc.).
type Projection interface {
	// Name returns the unique name of this projection.
	// This name is used for checkpoint tracking.
	Name() string

	// Handle processes a single event.
	// Return an error to stop projection processing.
	//
	// The tx parameter is the processor's transaction used for checkpoint management.
	// SQL projections can use this transaction to ensure atomic updates of both
	// the read model and the checkpoint. This eliminates inconsistencies where
	// a projection succeeds but the checkpoint update fails (or vice versa).
	//
	// The transaction will be committed by the processor after Handle returns successfully.
	// Projections should NEVER call Commit() or Rollback() on the provided transaction.
	//
	// For non-SQL projections (Elasticsearch, Redis, message brokers), the tx parameter
	// should be ignored and projections should manage their own connections as before.
	//
	// Event is passed by value to enforce immutability (events are value objects).
	// Large data (Payload, Metadata byte slices) share references to their backing arrays,
	// so the actual payload/metadata data is not deep-copied.
	//
	//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
	Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error
}

// ScopedProjection is an optional interface that projections can implement to filter
// events by aggregate type and/or bounded context. This is useful for read model projections
// that only care about specific aggregate types within specific bounded contexts.
//
// By default, projections implementing only the Projection interface receive all events.
// This ensures that global projections (e.g., integration publishers, audit logs) continue
// to work without modification.
//
// Example - Read model projection scoped to User aggregate in Identity context:
//
// type UserReadModelProjection struct {}
//
//	func (p *UserReadModelProjection) Name() string {
//	   return "user_read_model"
//	}
//
//	func (p *UserReadModelProjection) AggregateTypes() []string {
//	   return []string{"User"}
//	}
//
//	func (p *UserReadModelProjection) BoundedContexts() []string {
//	   return []string{"Identity"}
//	}
//
//	func (p *UserReadModelProjection) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
//	   // Only receives User aggregate events from Identity bounded context
//	   // Use tx for atomic read model updates with checkpoint
//	   return nil
//	}
//
// Example - Read model projection scoped to multiple contexts:
//
// type OrderRevenueProjection struct {}
//
//	func (p *OrderRevenueProjection) Name() string {
//	   return "order_revenue"
//	}
//
//	func (p *OrderRevenueProjection) AggregateTypes() []string {
//	   return []string{"Order"}
//	}
//
//	func (p *OrderRevenueProjection) BoundedContexts() []string {
//	   return []string{"Sales", "Billing"}
//	}
//
//	func (p *OrderRevenueProjection) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
//	   // Receives Order events from both Sales and Billing contexts
//	   return nil
//	}
//
// Example - Global integration publisher:
//
// type WatermillPublisher struct {}
//
//	func (p *WatermillPublisher) Name() string {
//	   return "system.integration.watermill.v1"
//	}
//
//	func (p *WatermillPublisher) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
//	   // Receives ALL events from all contexts for publishing to message broker
//	   // Ignore tx parameter - use message broker client
//	   _ = tx
//	   return nil
//	}
type ScopedProjection interface {
	Projection
	// AggregateTypes returns the list of aggregate types this projection cares about.
	// If empty, the projection receives events from all aggregate types (still filtered by BoundedContexts if specified).
	// If non-empty, only events matching one of these aggregate types are passed to Handle.
	AggregateTypes() []string

	// BoundedContexts returns the list of bounded contexts this projection cares about.
	// If empty, the projection receives events from all bounded contexts (still filtered by AggregateTypes if specified).
	// If non-empty, only events matching one of these bounded contexts are passed to Handle.
	BoundedContexts() []string
}

// PartitionStrategy defines how events are partitioned across projection instances.
type PartitionStrategy interface {
	// ShouldProcess returns true if this projection instance should process the given event.
	// aggregateID is the aggregate ID of the event.
	// partitionKey identifies this projection instance (e.g., "0" for first of 4 workers).
	// totalPartitions is the total number of projection instances.
	ShouldProcess(aggregateID string, partitionKey int, totalPartitions int) bool
}

// HashPartitionStrategy implements deterministic hash-based partitioning.
// Events are distributed across partitions based on a hash of the aggregate ID.
// This ensures:
// - All events for the same aggregate go to the same partition
// - Even distribution across partitions
// - Deterministic assignment (same aggregate always goes to same partition)
//
// This strategy enables horizontal scaling of projection processing while
// maintaining ordering guarantees within each aggregate.
type HashPartitionStrategy struct{}

// ShouldProcess implements PartitionStrategy using FNV-1a hashing.
func (HashPartitionStrategy) ShouldProcess(aggregateID string, partitionKey, totalPartitions int) bool {
	if totalPartitions <= 1 {
		return true
	}

	h := fnv.New32a()
	h.Write([]byte(aggregateID))
	partition := int(h.Sum32()) % totalPartitions
	return partition == partitionKey
}

// RunMode determines how the processor handles event processing.
type RunMode int

const (
	// RunModeContinuous runs forever, continuously polling for new events.
	// This is the default mode for production use.
	RunModeContinuous RunMode = iota

	// RunModeOneOff processes all available events and exits cleanly.
	// This mode is useful for:
	// - Integration tests that need synchronous projection processing
	// - One-time catch-up operations
	// - Backfilling projections
	RunModeOneOff
)

// WakeupSource provides best-effort wake signals to projection processors.
// Signals are intentionally lossy/coalesced and should only be used as an
// optimization hint. Correctness must always rely on checkpoint-based pulling.
type WakeupSource interface {
	// Subscribe registers a projection processor for wake signals.
	// Returns a receive-only signal channel and an unsubscribe function.
	Subscribe() (signals <-chan struct{}, unsubscribe func())
}

// ProcessorConfig configures a projection processor.
type ProcessorConfig struct {
	// PartitionStrategy determines which events this processor handles
	PartitionStrategy PartitionStrategy

	// Logger is an optional logger for observability.
	// If nil, logging is disabled (zero overhead).
	Logger es.Logger

	// WakeupSource is an optional best-effort signal source.
	// If nil, processors rely purely on PollInterval fallback polling.
	WakeupSource WakeupSource

	// PollBackoffFactor controls exponential backoff growth for idle fallback polling.
	// Values <= 1 disable growth (constant PollInterval fallback).
	// Default is 2.0.
	PollBackoffFactor float64

	// PollInterval is the duration to wait when no events are available.
	// This prevents tight polling loops that consume excessive CPU.
	// A value of 0 means no delay (busy polling - not recommended).
	// Default is 100ms, which provides a good balance between latency and CPU usage.
	PollInterval time.Duration

	// MaxPollInterval is the upper bound for idle fallback polling when backoff is enabled.
	// Default is 5s.
	MaxPollInterval time.Duration

	// WakeupJitter is the random delay applied after receiving a wake signal.
	// This helps smooth spikes when many projections wake at once.
	// Default is 25ms.
	WakeupJitter time.Duration

	// BatchSize is the number of events to read per batch
	BatchSize int

	// PartitionKey identifies this processor instance (0-indexed)
	PartitionKey int

	// TotalPartitions is the total number of processor instances
	TotalPartitions int

	// RunMode determines processing behavior.
	// Default: RunModeContinuous
	RunMode RunMode
}

// DefaultProcessorConfig returns the default configuration.
func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		BatchSize:         100,
		PartitionKey:      0,
		TotalPartitions:   1,
		PartitionStrategy: HashPartitionStrategy{},
		Logger:            nil,                    // No logging by default
		PollInterval:      100 * time.Millisecond, // Prevent CPU spinning
		MaxPollInterval:   5 * time.Second,        // Bound idle backoff
		PollBackoffFactor: 2.0,                    // Exponential backoff by default
		WakeupJitter:      25 * time.Millisecond,  // Smooth wake-up spikes
		WakeupSource:      nil,                    // No dispatcher by default
		RunMode:           RunModeContinuous,      // Continuous mode by default
	}
}

// ProcessorRunner is the interface that adapter-specific processors must implement.
// This allows the Runner to orchestrate projections regardless of the underlying
// storage implementation (SQL, NoSQL, message brokers, etc.).
type ProcessorRunner interface {
	// Run processes events for the given projection until the context is canceled.
	// Returns an error if the projection handler fails.
	Run(ctx context.Context, projection Projection) error
}
