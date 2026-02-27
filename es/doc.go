// Package es provides core event sourcing infrastructure.
//
// # Overview
//
// This package defines the fundamental types and interfaces for event sourcing:
//   - Event: immutable domain events
//   - DBTX: database transaction abstraction
//   - EventStore: event persistence interface
//   - Consumer: event processing interface
//
// # Design Philosophy
//
// Clean Architecture: Core interfaces are database-agnostic. Infrastructure
// concerns (like PostgreSQL) are isolated in adapter packages.
//
// Transaction Control: The library uses DBTX instead of managing transactions.
// This gives you full control over transaction boundaries and allows combining
// event operations with other database work atomically.
//
// Immutability: Events are value objects. They don't have identity until
// persisted and assigned a global_position by the event store.
//
// # Quick Start
//
// 1. Generate database migrations:
//
//	go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
//
// 2. Apply migrations to your database
//
// 3. Create an event store:
//
//	import (
//	    "github.com/pupsourcing/core/es"
//	    "github.com/pupsourcing/core/es/adapters/postgres"
//	)
//
//	store := postgres.NewStore(postgres.DefaultStoreConfig())
//
// 4. Append events:
//
//	tx, _ := db.BeginTx(ctx, nil)
//	defer tx.Rollback()
//
//	events := []es.Event{
//	    {
//	        AggregateType:    "Order",
//	        AggregateID:      orderID,
//	        EventID:          uuid.New(),
//	        EventType:        "OrderCreated",
//	        EventVersion:     1,
//	        Payload:          payload,
//	        Metadata:         []byte(`{}`),
//	        CreatedAt:        time.Now(),
//	    },
//	}
//
//	result, err := store.Append(ctx, tx, es.NoStream(), events)
//	if err != nil {
//	    return err
//	}
//
//	tx.Commit()
//
// 5. Process events with consumers:
//
//	import "github.com/pupsourcing/core/es/consumer"
//
//	type MyProjection struct {}
//
//	func (p *MyProjection) Name() string { return "my_projection" }
//
//	func (p *MyProjection) Handle(ctx context.Context, tx es.DBTX, event es.PersistedEvent) error {
//	    // Process event
//	    return nil
//	}
//
//	config := consumer.DefaultProcessorConfig()
//	processor := consumer.NewProcessor(db, store, &config)
//	processor.Run(ctx, &MyProjection{})
//
// # Optimistic Concurrency
//
// The library enforces optimistic concurrency via aggregate_version.
// When appending events:
//   - The first event's version must be current_version + 1
//   - Subsequent events must have sequential versions
//   - Version conflicts return ErrOptimisticConcurrency
//
// This prevents race conditions when multiple processes modify the same aggregate.
//
// # Consumers
//
// Consumers process events sequentially and track their progress via checkpoints.
// They can be:
//   - Long-running (endless processing with context cancellation)
//   - Horizontally scaled (via deterministic hash partitioning)
//   - Resumed after failure (from last checkpoint)
//
// See the consumer package for details.
//
// # Database Schema
//
// Events are stored in a table with:
//   - global_position: BIGSERIAL primary key for global ordering
//   - aggregate_type, aggregate_id: identify the aggregate
//   - aggregate_version: for optimistic concurrency
//   - event_id: unique identifier (UUID)
//   - payload: BYTEA for flexible serialization
//   - trace_id, correlation_id, causation_id: for distributed tracing
//   - metadata: JSONB for additional context
//
// Checkpoints are stored separately per consumer.
//
// # Design Decisions
//
// BYTEA for payload: Supports any serialization (JSON, Protobuf, Avro).
// Users choose their encoding.
//
// DBTX interface: Works with *sql.DB and *sql.Tx. No transaction management
// in the library keeps it focused on event sourcing.
//
// Pull-based consumers: Consumers read events in batches. This is simpler
// than push-based and works well with checkpoint-based resumption.
//
// Hash-based partitioning: Events for the same aggregate go to the same
// partition. This maintains ordering while enabling horizontal scaling.
package es
