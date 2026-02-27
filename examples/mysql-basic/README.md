# MySQL Basic Example

This example demonstrates basic event sourcing with MySQL/MariaDB, showing how to use the MySQL adapter to append and read events.

## What It Does

- Connects to a MySQL database
- Generates and applies database migrations programmatically
- Demonstrates three operations:
  1. Appending events to the event store
  2. Reading all events from the store
  3. Reading events for a specific aggregate

## Prerequisites

MySQL 8.0 or MariaDB 10.5+ running on localhost:3306

## Running the Example

### Step 1: Start MySQL

```bash
docker run -d -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=pupsourcing_example \
  mysql:8
```

Or for MariaDB:

```bash
docker run -d -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=pupsourcing_example \
  mariadb:11
```

### Step 2: Run the Example

```bash
cd examples/mysql-basic
go run main.go
```

The example automatically handles schema setup - no manual migration needed!

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
```

## Key Features

### MySQL Adapter

The example uses the MySQL-specific adapter:

```go
import "github.com/getpup/pupsourcing/es/adapters/mysql"

store := mysql.NewStore(mysql.DefaultStoreConfig())
```

### Automatic Schema Setup

The example programmatically generates and applies migrations:

```go
import "github.com/getpup/pupsourcing/es/migrations"

// Generate migration
config := migrations.Config{
    OutputFolder:        "./migrations",
    OutputFilename:      "init.sql",
    EventsTable:         "events",
    CheckpointsTable:    "consumer_checkpoints",
    AggregateHeadsTable: "aggregate_heads",
}
migrations.GenerateMySQL(&config)

// Read and apply the migration
migrationSQL, _ := os.ReadFile("./migrations/init.sql")
// Split by ";" and execute each statement
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

## MySQL-Specific Considerations

### AUTO_INCREMENT vs SERIAL

MySQL uses `AUTO_INCREMENT` for the `global_position` column, equivalent to PostgreSQL's `SERIAL` type.

### Transaction Isolation

The example uses MySQL's default transaction isolation level. For production, consider using `REPEATABLE READ` or `SERIALIZABLE` for stronger consistency guarantees.

### Statement Execution

MySQL requires executing DDL statements individually. The example splits the migration SQL by semicolons and executes each statement separately:

```go
statements := strings.Split(migrationSQL, ";")
for _, stmt := range statements {
    if strings.TrimSpace(stmt) != "" {
        db.Exec(stmt)
    }
}
```

## Connection String Format

The example uses MySQL's DSN format:

```
root:password@tcp(localhost:3306)/pupsourcing_example?parseTime=true
```

**Important:** Include `parseTime=true` to properly handle time columns.

## Next Steps

- See `../basic` for PostgreSQL version
- See `../sqlite-basic` for SQLite version
- Explore projection examples for processing events
- Check main README for MySQL adapter configuration options

## See Also

- `../basic` - Basic example with PostgreSQL
- `../sqlite-basic` - Basic example with SQLite
- MySQL Adapter Documentation in `es/adapters/mysql`
