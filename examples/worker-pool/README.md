# Worker Pool Example

This example demonstrates the "poor man's worker pool" - running N partitions of a single projection within the same process using goroutines.

## What It Does

- Runs a single projection across multiple goroutines (workers)
- Each worker handles a disjoint subset of events based on aggregate ID hashing
- All workers share the same database connection pool
- All workers run in the same process

## Running the Example

1. Start PostgreSQL:
```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_example postgres:16
```

2. Run migrations (from the basic example directory):
```bash
cd ../basic
go generate
# Apply the generated migration to your database
```

3. Run with default 4 workers:
```bash
go run main.go
```

4. Or specify number of workers:
```bash
go run main.go --workers=8
```

## Key Concepts

### In-Process Partitioning

This pattern uses goroutines instead of separate processes:
- Lower overhead than multiple processes
- Shared memory (useful for some use cases, risky for others)
- Single deployment unit
- All workers stop together

### When to Use

**✅ Use this pattern when:**
- You need moderate parallelism (2-8 workers)
- All workers fit comfortably in one process
- You want simple deployment
- You need shared in-memory state

**❌ Avoid this pattern when:**
- You need high parallelism (>8 workers)
- Workers need to scale independently
- You need better failure isolation
- You want to spread load across multiple machines

## Comparison with Other Patterns

### vs. Single Worker
- **Pro**: Better throughput, parallel processing
- **Con**: More complex, requires thread-safe state

### vs. Separate Processes
- **Pro**: Simpler deployment, shared memory
- **Con**: Less isolation, harder to scale beyond one machine

### vs. Multiple Projections
- **Pro**: All workers process the same events
- **Con**: Can only scale one projection this way

## Thread Safety Considerations

Since all workers share the same projection instance, any state must be thread-safe:

```go
type ThreadSafeProjection struct {
    mu    sync.Mutex
    state map[string]int
}

// OR use atomic operations (as shown in this example)
type AtomicProjection struct {
    count int64  // Use atomic.AddInt64, atomic.LoadInt64
}
```

**Important**: If your projection has non-thread-safe state, either:
1. Use proper synchronization (mutexes, atomic operations, channels)
2. Make the projection stateless (only update database tables)
3. Use separate processes with `partitioned` example instead

## Performance Characteristics

### Resource Usage
- **Memory**: Lower than separate processes (shared connection pool)
- **CPU**: Efficient goroutine scheduling
- **Database Connections**: Shared pool across all workers

### Scaling Limits
- Practical limit: 8-16 workers per process
- Beyond that, consider separate processes
- Limited by single machine resources

## Configuration

### Via Command Line
```bash
go run main.go --workers=4
```

### Programmatically
```go
// Create runners for each partition
var runners []runner.ConsumerRunner
for i := 0; i < numWorkers; i++ {
    config := consumer.DefaultProcessorConfig()
    config.PartitionKey = i
    config.TotalPartitions = numWorkers
    processor := postgres.NewProcessor(db, store, &config)
    runners = append(runners, runner.ConsumerRunner{
        Consumer: proj,
        Processor:  processor,
    })
}

r := runner.New()
err := r.Run(ctx, runners)
```

## Production Considerations

1. **Monitoring**: All workers share the same process metrics
2. **Error Handling**: One worker error stops all workers
3. **Resource Limits**: All workers compete for CPU/memory in the same process
4. **Deployment**: Single binary, simple to deploy

## Scaling Strategy

**Start small, scale up:**
1. Start with 1 worker (simplest)
2. If CPU bound: Add more workers (up to CPU cores)
3. If DB bound: Consider optimizing queries or batch size
4. If process bound: Move to separate processes

## Advanced: Custom Worker Configuration

For more control, configure workers manually:

```go
configs := make([]runner.ProjectionConfig, numWorkers)
for i := 0; i < numWorkers; i++ {
    config := consumer.DefaultProcessorConfig()
    config.PartitionKey = i
    config.TotalPartitions = numWorkers
    config.BatchSize = 100  // Customize per worker if needed
    
    configs[i] = runner.ProjectionConfig{
        Consumer:      projection,
        ProcessorConfig: config,
    }
}

err := runner.RunMultipleProjections(ctx, db, store, configs)
```

## See Also

- `../single-worker` - Baseline (1 worker)
- `../partitioned` - Multiple processes instead of goroutines
- `../multiple-projections` - Multiple different projections
- `../scaling` - Demonstrates scaling from 1→N
