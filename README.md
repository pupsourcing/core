# pupsourcing

[![CI](https://github.com/pupsourcing/core/actions/workflows/ci.yml/badge.svg)](https://github.com/pupsourcing/core/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/pupsourcing/core)](https://goreportcard.com/report/github.com/pupsourcing/core)
[![GoDoc](https://godoc.org/github.com/pupsourcing/core?status.svg)](https://godoc.org/github.com/pupsourcing/core)

<div>
<img width="300" height="300" alt="response" src="https://github.com/user-attachments/assets/113ffeea-b8e3-497a-b0df-3ef903a70540" align="center" />
</div>

A production-ready Event Sourcing library for Go.

## Overview

pupsourcing provides minimal, reliable infrastructure for event sourcing in Go applications. State changes are stored as an immutable sequence of events, providing a complete audit trail and enabling event replay, temporal queries, and flexible read models.

## Key Features

- **Clean Architecture** - no "infrastructure creep" into your domain model (no annotations, no framework-specific base structs)
- **Multiple Database Adapters** - PostgreSQL, SQLite, and MySQL/MariaDB
- **Bounded Context Support** - Events are scoped to bounded contexts for domain-driven design alignment
- **Optimistic Concurrency** - Automatic conflict detection via database constraints
- **Consumer System** - Pull-based event processing with checkpoints, supporting both global and context-scoped consumers
- **Horizontal Scaling** - Hash-based partitioning for consumer workers
- **Code Generation** - Optional tool for strongly-typed domain event mapping
- **Minimal Dependencies** - Go standard library plus database driver

## Installation

```bash
go get github.com/pupsourcing/core
```

Choose your database driver:
```bash
# PostgreSQL (recommended for production)
go get github.com/lib/pq

# SQLite (embedded, ideal for testing)
go get modernc.org/sqlite

# MySQL/MariaDB
go get github.com/go-sql-driver/mysql
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

Consumers process events from the event store. Use **scoped consumers** for read models that only care about specific aggregate types and/or bounded contexts, or **global consumers** for integration publishers that need all events.

```go
import "github.com/pupsourcing/core/es/consumer"

// Scoped projection - only receives User aggregate events from Identity context
type UserReadModelProjection struct{}

func (p *UserReadModelProjection) Name() string {
    return "user_read_model"
}

// AggregateTypes filters events by aggregate type
func (p *UserReadModelProjection) AggregateTypes() []string {
    return []string{"User"}  // Only receives User events
}

// BoundedContexts filters events by bounded context
func (p *UserReadModelProjection) BoundedContexts() []string {
    return []string{"Identity"}  // Only receives events from Identity context
}

func (p *UserReadModelProjection) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
    // Update read model using the provided transaction for atomic consistency
    // The transaction is committed by the processor after successful handling
    // IMPORTANT: Do NOT commit or rollback the transaction - the processor manages that
    return nil
}

// Run projection with adapter-specific processor
store := postgres.NewStore(postgres.DefaultStoreConfig())
config := consumer.DefaultProcessorConfig()
processor := postgres.NewProcessor(db, store, &config)
err := processor.Run(ctx, &UserReadModelProjection{})
```

Optional optimization for many consumers in one process (best-effort wake signals with explicit lifecycle):

```go
runCtx, cancel := context.WithCancel(ctx)
defer cancel()

dispatcher := consumer.NewDispatcher(db, store, nil) // uses DefaultDispatcherConfig
dispatcherErrCh := make(chan error, 1)
go func() {
    dispatcherErrCh <- dispatcher.Run(runCtx)
}()

consumersSlice := []consumer.Consumer{
    &UserReadModelProjection{},
    &AnotherConsumer{},
}

runners := make([]runner.ConsumerRunner, len(consumersSlice))
for i, c := range consumersSlice {
    cfg := consumer.DefaultProcessorConfig()
    cfg.WakeupSource = dispatcher // correctness still relies on checkpoints + fallback polling
    cfg.PollInterval = 500 * time.Millisecond
    cfg.MaxPollInterval = 8 * time.Second

    runners[i] = runner.ConsumerRunner{
        Consumer: c,
        Processor:  postgres.NewProcessor(db, store, &cfg),
    }
}

runErr := runner.New().Run(runCtx, runners)
cancel() // ensure dispatcher stops even if runner exits first
dispatcherErr := <-dispatcherErrCh

if runErr != nil && !errors.Is(runErr, context.Canceled) {
    return runErr
}
if dispatcherErr != nil &&
    !errors.Is(dispatcherErr, context.Canceled) &&
    !errors.Is(dispatcherErr, context.DeadlineExceeded) {
    // Optional: log warning; consumers remain correct via fallback polling.
}
```

See the full runnable version in [`examples/dispatcher-runner`](./examples/dispatcher-runner/).

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

- **[Single Worker](./examples/single-worker/)** - Basic consumer pattern
- **[Multiple Consumers](./examples/multiple-projections/)** - Running different consumers concurrently
- **[Dispatcher + Runner](./examples/dispatcher-runner/)** - Safe dispatcher lifecycle with wakeup optimization
- **[Worker Pool](./examples/worker-pool/)** - Multiple workers in the same process
- **[Partitioned](./examples/partitioned/)** - Horizontal scaling across processes
- **[With Logging](./examples/with-logging/)** - Observability and debugging

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
1. Starts PostgreSQL and MySQL containers via `docker compose`
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
