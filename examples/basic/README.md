# Basic Example

This example demonstrates the fundamental usage of the pupsourcing library, showing how to append events to an event store and process them with a projection.

## What It Does

- Connects to a PostgreSQL database
- Creates an event store
- Appends a `UserCreated` event to the store
- Runs a simple projection to process the events
- Demonstrates the complete event sourcing workflow

## Prerequisites

1. PostgreSQL running on localhost:5432
2. Database named `pupsourcing_example`

## Running the Example

### Step 1: Start PostgreSQL

```bash
docker run -d -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=pupsourcing_example \
  postgres:16
```

### Step 2: Generate and Apply Migrations

This example includes a `go:generate` directive to create the database schema:

```bash
cd examples/basic
go generate
```

This will generate `init.sql` in the `migrations` directory at the repository root. Apply the migration to your database:

```bash
# Using psql
psql -h localhost -U postgres -d pupsourcing_example < ../../migrations/init.sql

# Or using docker exec if running in container
docker exec -i <container-id> psql -U postgres -d pupsourcing_example < migrations/init.sql
```

### Step 3: Run the Example

```bash
go run main.go
```

## Expected Output

```
Appending events...
Events appended at positions: [1]
Aggregate is now at version: 1

Running projection...
Projection processed: User created - Alice Smith (alice@example.com)

Projection result - Users: [alice@example.com]
```

## Key Concepts Demonstrated

### Event Store

The example creates an event store with PostgreSQL:

```go
store := postgres.NewStore(postgres.DefaultStoreConfig())
```

### Appending Events

Events are appended within a transaction:

```go
events := []es.Event{
    {
        BoundedContext: "Identity",  // Required: scope to bounded context
        AggregateType:  "User",
        AggregateID:    aggregateID,
        EventID:        uuid.New(),
        EventType:      "UserCreated",
        EventVersion:   1,
        Payload:        payload,
        Metadata:       []byte(`{}`),
        CreatedAt:      time.Now(),
    },
}

tx, _ := db.BeginTx(ctx, nil)
result, err := store.Append(ctx, tx, es.NoStream(), events)
tx.Commit()
```

**Note:** Use `es.NoStream()` when creating a new aggregate (first event).

### Projections

A projection processes events to build read models:

```go
type UserProjection struct {
    users []string
}

func (p *UserProjection) Name() string {
    return "user_list"
}

func (p *UserProjection) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
    // The processor provides a transaction for atomic updates
    // In this example we use in-memory state, but typically you'd use tx for database operations
    if event.EventType == "UserCreated" {
        var payload UserCreated
        json.Unmarshal(event.Payload, &payload)
        p.users = append(p.users, payload.Email)
    }
    return nil
}
```

### Running Projections

Create a processor and run the projection:

```go
config := consumer.DefaultSegmentProcessorConfig()
config.TotalSegments = 1  // Single segment for simple examples
processor := postgres.NewSegmentProcessor(db, store, config)
processor.Run(ctx, proj)
```

## Database Schema

The migration generates four tables:

1. **`events`** - Stores all events in order
   - `global_position` - Monotonic sequence for ordering
   - `aggregate_id` + `aggregate_version` - For optimistic concurrency

2. **`aggregate_heads`** - Tracks current version of each aggregate
   - Used for fast version lookups (O(1) instead of O(n))

3. **`consumer_checkpoints`** - Tracks projection progress
   - Each projection maintains its own checkpoint
   - Allows projections to resume after restart

4. **`consumer_segments`** - Manages segment-based processing
   - Enables horizontal scaling via segment claiming
   - Used for worker-based auto-scaling

## Transaction Control

The library is **transaction-agnostic** - you control when to commit:

```go
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()  // Safe to call even after commit

// Append events
store.Append(ctx, tx, es.NoStream(), events)

// Commit when ready
tx.Commit()
```

This gives you full control over transaction boundaries and allows you to combine event appending with other database operations.

## Next Steps

- **More Examples**: See other examples for advanced patterns
  - `../worker` - Recommended approach using Worker API for auto-scaling
  - `../stop-resume` - Checkpoint persistence and resumption
  - `../with-logging` - Custom logger integration
- **Read the Docs**: Check the main README for API documentation
- **Production Setup**: Use proper connection pooling and configuration management

## See Also

- `../with-logging` - Same example with custom logger integration
- `../mysql-basic` - Basic example using MySQL adapter
- `../sqlite-basic` - Basic example using SQLite adapter
