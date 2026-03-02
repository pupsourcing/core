// Package store defines persistence contracts for event sourcing and consumer coordination.
//
// This package contains two categories of interfaces:
//
// Event Storage: EventStore, EventReader, AggregateStreamReader - for persisting and
// reading domain events.
//
// Consumer Infrastructure: SegmentStore - for coordinating distributed
// consumers and tracking processing progress.
//
// Adapters (e.g., postgres.Store) implement both categories, sharing the underlying
// database connection for transactional consistency and operational simplicity.
package store

import (
	"context"
	"errors"

	"github.com/pupsourcing/core/es"
)

var (
	// ErrOptimisticConcurrency indicates a version conflict during append.
	ErrOptimisticConcurrency = errors.New("optimistic concurrency conflict")

	// ErrNoEvents indicates an attempt to append zero events.
	ErrNoEvents = errors.New("no events to append")
)

// EventStore defines the interface for appending events.
type EventStore interface {
	// Append atomically appends one or more events within the provided transaction.
	// Events must all belong to the same aggregate instance.
	// Returns an AppendResult containing the persisted events with assigned versions
	// and their global positions, or an error.
	//
	// The expectedVersion parameter controls optimistic concurrency:
	// - Any(): No version check - always succeeds if no other errors
	// - NoStream(): Aggregate must not exist - used for aggregate creation
	// - Exact(N): Aggregate must be at version N - used for normal updates
	//
	// The store automatically assigns AggregateVersion to each event:
	// - Fetches the current version from the aggregate_heads table (O(1) lookup)
	// - Validates against expectedVersion
	// - Assigns consecutive versions starting from (current + 1)
	// - Updates aggregate_heads with the new version
	// - The database unique constraint on (aggregate_type, aggregate_id, aggregate_version)
	//   enforces optimistic concurrency as a last safety net
	//
	// Returns ErrOptimisticConcurrency if expectedVersion validation fails or if
	// another transaction commits conflicting events between the version check and insert
	// (detected via unique constraint violation).
	// Returns ErrNoEvents if events slice is empty.
	//
	// After a successful append:
	// - Use result.ToVersion() to get the new aggregate version
	// - Use result.Events to access the persisted events with all fields populated
	// - Use result.GlobalPositions to get the assigned global positions
	Append(ctx context.Context, tx es.DBTX, expectedVersion es.ExpectedVersion, events []es.Event) (es.AppendResult, error)
}

// EventReader defines the interface for reading events sequentially.
type EventReader interface {
	// ReadEvents reads events starting from the given global position.
	// Returns up to limit events.
	// Events are ordered by global_position ascending.
	ReadEvents(ctx context.Context, tx es.DBTX, fromPosition int64, limit int) ([]es.PersistedEvent, error)
}

// GlobalPositionReader defines the interface for reading the latest global event position.
// This is useful for lightweight "new events available" checks without loading full batches.
type GlobalPositionReader interface {
	// GetLatestGlobalPosition returns the highest global_position currently present in the event log.
	// Returns 0 when no events exist.
	GetLatestGlobalPosition(ctx context.Context, tx es.DBTX) (int64, error)
}

// AggregateStreamReader defines the interface for reading events for a specific aggregate.
type AggregateStreamReader interface {
	// ReadAggregateStream reads all events for a specific aggregate instance and returns
	// them as a Stream containing the aggregate's full history.
	// Events are ordered by aggregate_version ascending.
	//
	// Parameters:
	// - boundedContext: the bounded context of the aggregate (e.g., "Billing", "Identity")
	// - aggregateType: the type of aggregate (e.g., "User", "Order")
	// - aggregateID: the unique identifier of the aggregate instance (can be UUID string, email, etc.)
	// - fromVersion: optional minimum version (inclusive). Pass nil to read from the beginning.
	// - toVersion: optional maximum version (inclusive). Pass nil to read to the end.
	//
	// Examples:
	// - ReadAggregateStream(ctx, tx, "Identity", "User", "550e8400-e29b-41d4-a716-446655440000", nil, nil) - read all events
	// - ReadAggregateStream(ctx, tx, "Identity", "User", id, ptr(5), nil) - read from version 5 onwards
	// - ReadAggregateStream(ctx, tx, "Identity", "EmailReservation", "user@example.com", nil, nil) - read reservation events
	// - ReadAggregateStream(ctx, tx, "Billing", "User", id, nil, ptr(10)) - read up to version 10
	// - ReadAggregateStream(ctx, tx, "Identity", "User", id, ptr(5), ptr(10)) - read versions 5-10
	//
	// Returns a Stream with an empty Events slice if no events match the criteria.
	// Use stream.Version() to get the current aggregate version.
	// Use stream.IsEmpty() to check if any events were found.
	ReadAggregateStream(ctx context.Context, tx es.DBTX, boundedContext, aggregateType string, aggregateID string, fromVersion, toVersion *int64) (es.Stream, error)
}
