# SQLite Basic Example

This example demonstrates basic event sourcing with SQLite, showing how to use the SQLite adapter to append and read events.

## What It Does

- Creates an SQLite database file
- Generates and applies database migrations programmatically
- Demonstrates three operations:
  1. Appending events to the event store
  2. Reading all events from the store
  3. Reading events for a specific aggregate
- Saves the database to a local file for inspection

## Prerequisites

None! SQLite is embedded and requires no separate database server.

## Running the Example

```bash
cd examples/sqlite-basic
go run main.go
```

The example automatically:
- Creates `pupsourcing_example.db` in the current directory
- Generates and applies the schema
- Runs all demonstrations

## Expected Output

```
Setting up database schema...

=== Example 1: Appending Events ===
Appending UserCreated event for Alice Smith...
✓ Event appended at global position: 1
Appending UserEmailChanged event...
✓ Event appended at global position: 2

=== Example 2: Reading All Events ===
Reading all events from the store...
Event 1: UserCreated (position: 1, version: 1)
Event 2: UserEmailChanged (position: 2, version: 2)

=== Example 3: Reading Aggregate Stream ===
Reading stream for aggregate: <aggregate-id>
  Current version: 2
  Total events: 2
Event 1: UserCreated (version: 1)
Event 2: UserEmailChanged (version: 2)

✓ Example completed successfully!
Database saved to: pupsourcing_example.db
```

## Inspecting the Database

After running, you can inspect the database with the SQLite CLI:

```bash
# Install sqlite3 if needed
# macOS: brew install sqlite
# Ubuntu: sudo apt-get install sqlite3

sqlite3 pupsourcing_example.db

# View tables
.tables

# View events
SELECT * FROM events;

# View aggregate heads
SELECT * FROM aggregate_heads;

# Exit
.quit
```

## Key Features

### SQLite Adapter

The example uses the SQLite-specific adapter:

```go
import "github.com/getpup/pupsourcing/es/adapters/sqlite"
import _ "modernc.org/sqlite"  // Pure Go SQLite driver

db, _ := sql.Open("sqlite", "pupsourcing_example.db")
store := sqlite.NewStore(sqlite.DefaultStoreConfig())
```

### WAL Mode

The example enables Write-Ahead Logging for better concurrency:

```go
db.Exec("PRAGMA journal_mode = WAL;")
```

This allows:
- Multiple concurrent readers
- One writer at a time
- Better performance for read-heavy workloads

### Automatic Schema Setup

Migrations are generated and applied programmatically:

```go
import "github.com/getpup/pupsourcing/es/migrations"

config := migrations.Config{
    OutputFolder:        "/tmp",
    OutputFilename:      "init.sql",
    EventsTable:         "events",
    CheckpointsTable:    "consumer_checkpoints",
    AggregateHeadsTable: "aggregate_heads",
}
migrations.GenerateSQLite(&config)

migrationSQL, _ := os.ReadFile("/tmp/init.sql")
db.Exec(string(migrationSQL))
```

### Appending Events

```go
events := []es.Event{
    {
        AggregateType: "User",
        AggregateID:   aggregateID,
        EventID:       uuid.New(),
        EventType:     "UserCreated",
        EventVersion:  1,
        Payload:       payload,
        Metadata:      []byte(`{}`),
        CreatedAt:     time.Now(),
    },
}

tx, _ := db.BeginTx(ctx, nil)
result, err := store.Append(ctx, tx, es.NoStream(), events)
tx.Commit()
```

### Reading All Events

```go
fromPosition := int64(0)
batchSize := 100

stream, err := store.ReadForward(ctx, db, fromPosition, batchSize)
for _, event := range stream.Events {
    // Process event
}
```

### Reading Aggregate Stream

```go
stream, err := store.ReadAggregateStream(ctx, db, "User", aggregateID, nil, nil)

// Check if aggregate exists
if stream.IsEmpty() {
    log.Println("Aggregate not found")
}

// Get current version
currentVersion := stream.Version()

// Process events
for _, event := range stream.Events {
    // Handle event
}
```

## SQLite-Specific Considerations

### File-Based Storage

SQLite stores everything in a single file. This makes it:
- ✅ Easy to backup (just copy the file)
- ✅ Easy to inspect (use sqlite3 CLI)
- ✅ Portable (move the file anywhere)
- ⚠️ Limited to single-server deployments

### Concurrency Model

SQLite supports:
- **Multiple readers** simultaneously
- **One writer** at a time
- Writer blocks readers (briefly)

This works well for:
- Low to moderate write volumes
- Read-heavy workloads
- Single-server applications

### When to Use SQLite

**Good for:**
- Development and testing
- Single-server applications
- Edge computing / embedded systems
- Desktop applications
- Low to moderate traffic (< 100 writes/sec)
- Prototyping before scaling to PostgreSQL/MySQL

**Not recommended for:**
- High write volumes
- Multiple application servers
- Network-attached storage (NAS)
- Horizontal scaling requirements

### Performance Tips

1. **Use WAL mode** (shown in example)
2. **Batch writes** when possible
3. **Use transactions** for multiple operations
4. **Regular VACUUM** for long-running databases
5. **Monitor database size** - consider archiving old events

## Transitioning to PostgreSQL

If your application outgrows SQLite, you can migrate to PostgreSQL:

1. Export events from SQLite:
   ```sql
   .mode insert
   .output events.sql
   SELECT * FROM events;
   ```

2. Import to PostgreSQL (adjust SQL as needed)
3. Change adapter from `sqlite` to `postgres`
4. Update connection string

The application code remains the same!

## Next Steps

- See `../basic` for PostgreSQL version
- See `../mysql-basic` for MySQL version
- Explore projection examples for processing events
- Check main README for SQLite adapter configuration options

## See Also

- `../basic` - Basic example with PostgreSQL
- `../mysql-basic` - Basic example with MySQL
- SQLite Adapter Documentation in `es/adapters/sqlite`
