# CockroachDB Basic Example

This example demonstrates using pupsourcing with CockroachDB, leveraging PostgreSQL wire protocol compatibility.

## Overview

CockroachDB is a distributed SQL database that implements the PostgreSQL wire protocol. This means you can use the existing PostgreSQL adapter from pupsourcing with CockroachDB directly.

## Key Features Demonstrated

- Using the PostgreSQL adapter with CockroachDB
- Event appending with optimistic concurrency
- Reading aggregate streams
- Automatic retry logic for distributed transactions

## Prerequisites

- CockroachDB installed locally or via Docker
- Go 1.24 or later

## Setup

### Option 1: Docker (Recommended)

```bash
# Start CockroachDB in a Docker container
docker run -d \
  --name cockroach \
  -p 26257:26257 \
  -p 8080:8080 \
  cockroachdb/cockroach:latest \
  start-single-node --insecure

# Create database
docker exec -it cockroach cockroach sql --insecure -e "CREATE DATABASE pupsourcing"
```

### Option 2: Local Installation

```bash
# Install CockroachDB (macOS)
brew install cockroachdb/tap/cockroach

# Start single-node cluster
cockroach start-single-node --insecure --listen-addr=localhost:26257

# Create database
cockroach sql --insecure -e "CREATE DATABASE pupsourcing"
```

## Running the Example

1. Ensure CockroachDB is running (see Setup above)

2. Generate the database schema:
```bash
go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
```

3. Apply the migration:
```bash
# Using CockroachDB SQL client
cockroach sql --insecure --database=pupsourcing < migrations/[timestamp]_init_event_sourcing.sql
```

4. Run the example:
```bash
cd examples/cockroachdb-basic
go run main.go
```

## Connection String

The example uses the standard PostgreSQL connection string format:

```
postgresql://root@localhost:26257/pupsourcing?sslmode=disable
```

**Key differences from PostgreSQL:**
- Default port is `26257` (not `5432`)
- Default user is `root` (in insecure mode)
- Single-node clusters use `sslmode=disable` for local development

## What This Example Shows

1. **Direct PostgreSQL Adapter Usage**: Uses `postgres.NewStore()` - no special CockroachDB adapter needed
2. **Event Appending**: Creates and appends events to aggregates
3. **Optimistic Concurrency**: Demonstrates version conflict detection
4. **Aggregate Stream Reading**: Reads all events for an aggregate
5. **Transaction Retry Logic**: Shows how to handle serialization errors

## Performance Considerations

CockroachDB is optimized for distributed workloads. For best performance:

1. **Batch Event Appends**: Append multiple events in a single transaction
2. **Use Connection Pooling**: Reuse database connections
3. **Enable Clock Synchronization**: Ensure NTP is configured (production)
4. **Monitor Transaction Retries**: CockroachDB may retry transactions more frequently than single-node PostgreSQL

## Scaling to Multiple Nodes

To run a multi-node cluster:

```bash
# Node 1
cockroach start --insecure --listen-addr=localhost:26257 --join=localhost:26257,localhost:26258,localhost:26259

# Node 2
cockroach start --insecure --listen-addr=localhost:26258 --join=localhost:26257,localhost:26258,localhost:26259

# Node 3
cockroach start --insecure --listen-addr=localhost:26259 --join=localhost:26257,localhost:26258,localhost:26259

# Initialize cluster
cockroach init --insecure --host=localhost:26257
```

Connect to any node using the same connection string (CockroachDB handles routing).

## Web UI

CockroachDB provides a web UI at http://localhost:8080 where you can:
- Monitor cluster health
- View SQL queries
- Inspect data distribution
- Check transaction metrics

## Troubleshooting

### Connection Refused
- Ensure CockroachDB is running: `cockroach node status --insecure`
- Check port 26257 is accessible

### Transaction Retry Errors
- Normal in distributed systems
- Implement exponential backoff retry logic
- Consider reducing `TotalPartitions` to reduce contention

### Clock Skew Warnings
- CockroachDB requires synchronized clocks (<500ms skew)
- Configure NTP on all nodes
- Monitor with `cockroach node status --insecure`

## Further Reading

- [CockroachDB Documentation](https://www.cockroachlabs.com/docs/)
- [PostgreSQL Compatibility](https://www.cockroachlabs.com/docs/stable/postgresql-compatibility.html)
- [Transaction Retry Logic](https://www.cockroachlabs.com/docs/stable/transaction-retry-error-reference.html)
- [pupsourcing PostgreSQL Adapter](../../es/adapters/postgres/)
- [Full CockroachDB Research](../../COCKROACHDB_RESEARCH.md)
