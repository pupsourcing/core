# Worker Example

The recommended way to run event sourcing consumers with auto-scaling.

## What This Shows

- **Two scoped consumers** that filter events by aggregate type and bounded context
- **One global consumer** that receives all events for cross-cutting concerns
- **Zero-config usage** with production-ready defaults
- **Graceful shutdown** via signal handling
- **Auto-scaling**: deploy multiple instances, workers automatically claim and rebalance segments

## Usage

```bash
# Ensure PostgreSQL is running with migrations applied
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/pupsourcing_test?sslmode=disable"
go run .
```

## Scaling Horizontally

Deploy multiple instances of this binary — workers automatically claim and rebalance segments. No coordinator needed.

```bash
# Terminal 1
go run .

# Terminal 2 (auto-scales — segments will rebalance)
go run .

# Terminal 3 (auto-scales further)
go run .
```

Workers use fair-share rebalancing to distribute segments evenly. When a worker dies, its segments are automatically reclaimed by surviving workers.

## Customization

For production deployments, you may want to tune configuration:

```go
w := postgres.NewWorker(db, store,
    worker.WithTotalSegments(32),              // More segments = higher parallelism ceiling
    worker.WithBatchSize(200),                 // Larger batches = fewer DB roundtrips
    worker.WithHeartbeatInterval(3*time.Second), // Faster heartbeats = quicker failure detection
    worker.WithLogger(myLogger),               // For observability
)
```

See `es/worker/worker.go` for all available options.
