# pupsourcing Examples

Comprehensive, runnable examples demonstrating pupsourcing patterns and use cases.

## Overview

Each example is self-contained and demonstrates specific patterns. Start with the `worker` example to understand the recommended approach for running consumers, then explore database-specific and projection patterns.

## Consumer Examples

### [Worker](./worker/)
**Difficulty:** Beginner  
**Best for:** Getting started, recommended approach for running consumers

Demonstrates the high-level Worker API that wraps segment-based processor, dispatcher, and runner into a single entry point. This is the recommended way to run projections with automatic scaling support.

**What you'll learn:**
- Using the Worker API (recommended approach)
- Running multiple projections concurrently
- Automatic segment-based scaling
- Graceful shutdown

**Run it:**
```bash
cd worker
go run main.go
```

## Database Examples

### [Basic](./basic/)
**Database:** PostgreSQL  
**Difficulty:** Beginner  
**Best for:** Understanding the basics with PostgreSQL

Demonstrates basic event appending and reading with PostgreSQL.

**Prerequisites:**
```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_example postgres:16
```

**Run it:**
```bash
cd basic
go run main.go
```

### [CockroachDB Basic](./cockroachdb-basic/)
**Database:** CockroachDB  
**Difficulty:** Beginner  
**Best for:** CockroachDB infrastructure, globally distributed systems
Demonstrates event sourcing with CockroachDB (distributed SQL).

**Prerequisites:**
```bash
docker run -d -p 26257:26257 -p 8080:8080 cockroachdb/cockroach:latest start-single-node --insecure
```

**Run it:**
```bash
cd cockroachdb-basic
go run main.go
```

## Projection Examples

### [Stop and Resume](./stop-resume/)
**Difficulty:** Beginner  
**Best for:** Understanding checkpoint reliability

Shows that projections can be stopped and resumed without data loss.

**What you'll learn:**
- Checkpoint persistence
- Graceful shutdown
- Resumption behavior
- Status checking

**Run it:**
```bash
cd stop-resume

# Append events
go run main.go --mode=append --events=20

# Process some events, then Ctrl+C
go run main.go --mode=process

# Check status
go run main.go --mode=status

# Resume processing
go run main.go --mode=process
```

### [Custom Logging](./with-logging/)
**Difficulty:** Beginner  
**Best for:** Production observability integration

Shows how to integrate custom logging with pupsourcing.

**What you'll learn:**
- Implementing the `es.Logger` interface
- Integrating with structured logging libraries
- Observability hooks for debugging
- Production logging patterns

**Run it:**
```bash
cd with-logging
go run main.go
```

## Advanced Examples

### [Integration Testing](./integration-testing/)
**Difficulty:** Intermediate  
**Best for:** Testing event sourcing applications

Demonstrates patterns for integration testing event sourcing applications.

**What you'll learn:**
- Setting up test databases
- Testing event appending
- Testing projections
- Test isolation patterns

**Run it:**
```bash
cd integration-testing
go test -v
```

### [Event Mapping Code Generation](./eventmap-codegen/)
**Difficulty:** Advanced  
**Best for:** Clean architecture, domain-driven design

Demonstrates type-safe mapping between domain events and event sourcing types using code generation.

**What you'll learn:**
- Pure domain events (no infrastructure dependencies)
- Versioned events (v1, v2, etc.)
- Code generation for type-safe mappings
- Schema evolution patterns

**Run it:**
```bash
cd eventmap-codegen
go generate
go run main.go
```

## Quick Start

### Prerequisites

Most examples require:
- Go 1.23 or later
- PostgreSQL 12+ running locally

Start PostgreSQL:
```bash
docker run -d -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=pupsourcing_example \
  postgres:16
```

### Running Examples

1. Navigate to an example directory
2. Generate and apply migrations (first time only):
   ```bash
   cd basic
   go generate
   psql -h localhost -U postgres -d pupsourcing_example -f ../../migrations/init.sql
   ```
3. Run the example:
   ```bash
   go run main.go
   ```

## Learning Path

**New to event sourcing?**
1. Start with [Worker](./worker/) to see the recommended approach
2. Read [Core Concepts](https://pupsourcing.gopup.dev/core-concepts)
3. Try [Stop and Resume](./stop-resume/) to understand checkpoints

**Need to scale projections?**
1. The [Worker](./worker/) example uses segment-based scaling (recommended)
2. For custom scaling strategies, explore the consumer package directly

**Building a CQRS application?**
1. Start with [Worker](./worker/) for running multiple projections
2. Read [Deployment Guide](https://pupsourcing.gopup.dev/deployment)

## Key Concepts

### Events are Immutable

Events are value objects that become immutable once persisted. They don't have identity until the store assigns a `global_position`.

### Transaction Control

You control transaction boundaries. This allows you to combine event appending with other database operations atomically.

### Optimistic Concurrency

Version conflicts are detected automatically via database constraints. If concurrent modifications occur, one will fail with `ErrOptimisticConcurrency`.

### Projections

Projections read events sequentially and maintain progress via checkpoints. They can be stopped and resumed without losing position.

**Transaction Safety**: Each projection receives a `*sql.Tx` transaction in its `Handle` method. Use this transaction for atomic updates to your read model. The processor commits the transaction after successful handling, updating both your read model and the checkpoint atomically. Never commit or rollback the transaction yourself - the processor manages that.

For non-SQL integrations (e.g., message brokers), you can ignore the transaction parameter - the checkpoint will still be tracked atomically.

### Horizontal Scaling

The Worker API provides segment-based auto-scaling that allows multiple worker processes to
claim and process segments dynamically. Events for the same aggregate always go to the same
segment, maintaining ordering. For custom scaling strategies, use the consumer package directly.

### Checkpoints

Each projection maintains its own checkpoint in the database. Checkpoints are updated atomically with event processing, ensuring exactly-once semantics.

## Common Patterns

### Pattern: Graceful Shutdown

All examples demonstrate graceful shutdown:
```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

go func() {
    <-sigChan
    cancel()
}()

err := processor.Run(ctx, projection)
```

### Pattern: CLI Configuration

Examples show CLI-friendly configuration:
```go
partitionKey := flag.Int("partition-key", -1, "Partition key")
totalPartitions := flag.Int("total-partitions", 4, "Total partitions")
flag.Parse()

// Also support env vars
if *partitionKey == -1 {
    if envKey := os.Getenv("PARTITION_KEY"); envKey != "" {
        *partitionKey, _ = strconv.Atoi(envKey)
    }
}
```

### Pattern: Idempotent Projections

Make projections safe for reprocessing:
```go
func (p *Projection) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
    // Use the provided transaction for atomic updates to your read model
    // The processor commits the transaction after successful handling
    _, err := tx.ExecContext(ctx,
        "INSERT INTO read_model (id, data) VALUES ($1, $2)"+
        "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
        id, data)
    return err
}
```

## Troubleshooting

### PostgreSQL Connection Errors

Ensure PostgreSQL is running:
```bash
docker ps  # Check if container is running
psql -h localhost -U postgres -d pupsourcing_example -c "SELECT 1"
```

### Migrations Not Applied

Generate and apply migrations:
```bash
cd basic && go generate
psql -h localhost -U postgres -d pupsourcing_example -f ../../migrations/init.sql
```

### Projection Not Processing

Check events exist:
```sql
SELECT COUNT(*) FROM events;
```

Check consumer checkpoint:
```sql
SELECT * FROM consumer_segments;
```

## Next Steps

- Read [Getting Started Guide](https://pupsourcing.gopup.dev/getting-started)
- Study [Core Concepts](https://pupsourcing.gopup.dev/core-concepts)
- Explore the [Deployment Guide](https://pupsourcing.gopup.dev/deployment)

## Contributing

Found a bug or have an example idea? Please open an issue or submit a PR!
