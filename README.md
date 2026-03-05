# pupsourcing

[![CI](https://github.com/pupsourcing/core/actions/workflows/ci.yml/badge.svg)](https://github.com/pupsourcing/core/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/pupsourcing/core)](https://goreportcard.com/report/github.com/pupsourcing/core)
[![GoDoc](https://godoc.org/github.com/pupsourcing/core?status.svg)](https://godoc.org/github.com/pupsourcing/core)

<div>
<img width="300" height="300" alt="response" src="https://github.com/user-attachments/assets/113ffeea-b8e3-497a-b0df-3ef903a70540" align="center" />
</div>

A production-ready Event Sourcing library for Go.

## Overview

pupsourcing provides minimal, reliable infrastructure for event sourcing in Go applications. State changes are stored as an immutable sequence of events, providing complete audit trails and enabling event replay, temporal queries, and flexible read models.

## Key Features

- **Clean Architecture** - No "infrastructure creep" into your domain model (no annotations, no framework-specific base structs)
- **PostgreSQL Adapter** - Production-ready PostgreSQL implementation
- **Bounded Context Support** - Events scoped to bounded contexts for domain-driven design alignment
- **Optimistic Concurrency** - Automatic conflict detection via database constraints
- **Auto-Scaling Workers** - Segment-based consumer processing. Deploy N instances, they self-organize
- **Code Generation** - Optional tool for strongly-typed domain event mapping
- **Minimal Dependencies** - Go standard library plus database driver

## Installation

```bash
go get github.com/pupsourcing/core
```

Choose your database driver:
```bash
# PostgreSQL
go get github.com/lib/pq
```

## Quick Start

### 1. Generate Database Schema

```bash
go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
```

Apply the generated migrations using your preferred migration tool.

### 2. (Optional) Generate Event Mapping Code

If you want type-safe mapping between domain events and event sourcing types:

```bash
go run github.com/pupsourcing/core/cmd/eventmap-gen \
  -input internal/domain/events \
  -output internal/infrastructure/generated
```

See [Event Mapping Documentation](https://pupsourcing.gopup.dev/eventmap-gen) for details.

### 3. Append Events

```go
import (
    "github.com/pupsourcing/core/es"
    "github.com/pupsourcing/core/es/adapters/postgres"
    "github.com/google/uuid"
)

// Create store
store := postgres.NewStore(postgres.DefaultStoreConfig())

// Create event with bounded context
event := es.Event{
    BoundedContext: "Identity",       // Required: bounded context for the event
    AggregateType:  "User",
    AggregateID:    uuid.New().String(),  // String-based ID for flexibility
    EventID:        uuid.New(),
    EventType:      "UserCreated",
    EventVersion:   1,
    Payload:        []byte(`{"email":"alice@example.com","name":"Alice"}`),
    Metadata:       []byte(`{}`),
    CreatedAt:      time.Now(),
}

// Append within transaction with optimistic concurrency control
tx, _ := db.BeginTx(ctx, nil)
// Use NoStream() for creating a new aggregate
result, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}
tx.Commit()

// Access the result
fmt.Printf("Events appended at positions: %v\n", result.GlobalPositions)
fmt.Printf("Aggregate is now at version: %d\n", result.ToVersion())
```

### 4. Read Aggregate Streams

```go
// Read all events for an aggregate (with bounded context)
aggregateID := uuid.New().String()
stream, err := store.ReadAggregateStream(ctx, tx, "Identity", "User", aggregateID, nil, nil)

// Access stream information
fmt.Printf("Stream has %d events\n", stream.Len())
fmt.Printf("Current aggregate version: %d\n", stream.Version())
fmt.Printf("Bounded context: %s\n", stream.BoundedContext)

// Read from a specific version
fromVersion := int64(5)
stream, err = store.ReadAggregateStream(ctx, tx, "Identity", "User", aggregateID, &fromVersion, nil)

// Process events
for _, event := range stream.Events {
    // Handle event - all events have the same bounded context
    fmt.Printf("Event %s in context %s\n", event.EventType, event.BoundedContext)
}
```

### 5. Run Consumers

Use the **Worker API** for production consumer workloads. The Worker manages segment-based processing with automatic scaling and an internal dispatcher for wakeup optimization.

```go
import (
    "github.com/pupsourcing/core/es"
    "github.com/pupsourcing/core/es/adapters/postgres"
)

// Define a projection - scoped to specific aggregate types and bounded contexts
type UserProjection struct{}

func (p *UserProjection) Name() string { return "user_projection" }

func (p *UserProjection) AggregateTypes() []string {
    return []string{"User"}  // Only receives User events
}

func (p *UserProjection) BoundedContexts() []string {
    return []string{"Identity"}  // Only receives Identity context events
}

func (p *UserProjection) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
    // Update your read model here
    // IMPORTANT: Do NOT commit or rollback the transaction - the worker manages that
    return nil
}

// Run with auto-scaling
store := postgres.NewStore(postgres.DefaultStoreConfig())
w := postgres.NewWorker(db, store)
err := w.Run(ctx, &UserProjection{})
```

The Worker manages segment-based processing with an internal dispatcher for wakeup optimization. Deploy the same binary multiple times — workers automatically claim and rebalance segments.

**Worker Configuration:**

```go
import "github.com/pupsourcing/core/es/worker"

w := postgres.NewWorker(db, store,
    worker.WithTotalSegments(32),        // More segments = more parallelism
    worker.WithLogger(myLogger),         // Optional observability
    worker.WithBatchSize(200),           // Events per batch
    worker.WithPollInterval(50*time.Millisecond),
    worker.WithMaxPostBatchPause(100*time.Millisecond), // Max adaptive post-batch pause (default)
)

// Run multiple consumers in the same worker
err := w.Run(ctx, &UserProjection{}, &OrderProjection{}, &NotificationConsumer{})
```

Adaptive post-batch throttling is enabled by default (`MaxPostBatchPause = 100ms`). In continuous mode, the
pause is applied only after successful non-empty batches; it grows under sustained full batches, shrinks/resets
as load cools, and is capped by `MaxPostBatchPause`. Use `worker.WithMaxPostBatchPause(...)` to configure it,
and set `<= 0` to disable.

**LISTEN/NOTIFY (zero idle polling):**

Replace the built-in polling dispatcher with Postgres LISTEN/NOTIFY for instant event wake-ups and zero idle overhead:

```go
import "github.com/pupsourcing/core/es/worker"

// 1. Configure the store to NOTIFY on event append
store := postgres.NewStore(postgres.NewStoreConfig(
    postgres.WithNotifyChannel("pupsourcing_events"),
))

// 2. Create a NotifyDispatcher (uses pq.NewListener under the hood)
nd := postgres.NewNotifyDispatcher(connStr, &postgres.NotifyDispatcherConfig{
    Channel:          "pupsourcing_events", // Must match WithNotifyChannel
    FallbackInterval: 30 * time.Second,     // Safety net for missed notifications
})

// 3. Pass it to the Worker — replaces internal polling dispatcher
w := postgres.NewWorker(db, store,
    worker.WithWakeupSource(nd),
    worker.WithTotalSegments(4),
)
err := w.Run(ctx, &UserProjection{})
```

The `NotifyDispatcher` requires a Postgres connection string (not `*sql.DB`) because `pq.NewListener` manages its own dedicated connection with automatic reconnect. Notifications are transactional — they fire only when the `Append()` transaction commits, guaranteeing no phantom wakes.

## Scaling

Workers auto-scale via segment claiming. Each worker claims a fair share of segments and rebalances when workers join or leave. No coordinator needed — workers self-organize using database-level atomic operations.

**To scale horizontally:** Deploy more instances of the same binary. Each worker automatically claims segments and processes its fair share of the event stream.

**To scale down:** Stop instances — remaining workers reclaim orphaned segments after the stale threshold (default: 30 seconds).

The number of segments (default: 16 per consumer) determines the upper bound on parallelism. With 16 segments and 4 workers, each worker processes ~4 segments. With 8 workers, each processes ~2 segments.

## Documentation

Comprehensive documentation is available at **[https://pupsourcing.gopup.dev](https://pupsourcing.gopup.dev)**:

- **[Getting Started](https://pupsourcing.gopup.dev/getting-started)** - Installation, setup, and first steps
- **[Core Concepts](https://pupsourcing.gopup.dev/core-concepts)** - Understanding event sourcing principles
- **[Database Adapters](https://pupsourcing.gopup.dev/adapters)** - Choosing the right database
- **[Consumers & Scaling](https://pupsourcing.gopup.dev/scaling)** - Horizontal scaling and production patterns
- **[Event Mapping Code Generation](https://pupsourcing.gopup.dev/eventmap-gen)** - Type-safe domain event mapping
- **[Observability](https://pupsourcing.gopup.dev/observability)** - Logging, tracing, and monitoring
- **[API Reference](https://pupsourcing.gopup.dev/api-reference)** - Complete API documentation

## Examples

Complete runnable examples are available in the [`examples/`](./examples) directory:

- **[basic](./examples/basic/)** - Basic event appending and reading
- **[worker](./examples/worker/)** - Recommended consumer pattern with auto-scaling (coming soon)
- **[scoped-projections](./examples/scoped-projections/)** - Filtering events by bounded context and aggregate type
- **[cockroachdb-basic](./examples/cockroachdb-basic/)** - CockroachDB compatibility
- **[integration-testing](./examples/integration-testing/)** - Testing patterns
- **[with-logging](./examples/with-logging/)** - Observability
- **[stop-resume](./examples/stop-resume/)** - Checkpoint persistence
- **[eventmap-codegen](./examples/eventmap-codegen/)** - Code generation

See the [examples README](./examples/README.md) for more details.

## Development

### Running Tests

**Unit tests:**
```bash
make test-unit
```

**Integration tests locally (requires Docker):**
```bash
make test-integration-local
```

This command automatically:
1. Starts a PostgreSQL container via `docker compose`
2. Runs all integration tests
3. Cleans up containers

**Manual integration testing:**
```bash
# Start databases
docker compose up -d

# Run integration tests
make test-integration

# Stop databases
docker compose down
```

## Contributing

Contributions are welcome! Please submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
