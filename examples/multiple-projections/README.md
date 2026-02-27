# Multiple Projections Example

This example demonstrates running multiple different projections concurrently in the same process using the runner package.

## What It Does

- Runs three different projections simultaneously:
  - **UserCounterProjection**: Counts user creation events
  - **RevenueProjection**: Tracks total revenue from orders
  - **ActivityLogProjection**: Logs all events regardless of type
- Each projection maintains its own checkpoint
- Each projection can have different batch sizes and configurations
- All projections share the same database connection pool

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

3. Run this example:
```bash
go run main.go
```

## Key Concepts

### Independent Checkpoints

Each projection maintains its own checkpoint in the database. This means:
- Projections can process events at different speeds
- A slow projection doesn't block fast projections
- Each projection can be stopped and restarted independently
- Adding a new projection doesn't affect existing ones

### Shared Resources

All projections share:
- Database connection pool
- Event store reader
- Process resources (memory, CPU)

### Configuration Per Projection

Each projection can have:
- Different batch sizes (optimize for throughput vs. latency)
- Different partition configurations
- Different error handling strategies

## When to Use This Pattern

**Advantages:**
- Simple deployment (single binary)
- Efficient resource usage (shared connections)
- Easy local development

**Use when:**
- Projections have similar resource requirements
- Total CPU/memory usage fits in one process
- You want simple deployment

**Avoid when:**
- Projections have very different resource needs
- One projection is much slower than others
- You need independent scaling per projection
- You need better failure isolation

## Trade-offs

### Single Process Benefits
✅ Simpler deployment
✅ Shared connection pool
✅ Lower infrastructure costs
✅ Easier local development

### Single Process Drawbacks
❌ One projection failure affects all
❌ Resource contention between projections
❌ Cannot scale projections independently
❌ Difficult to isolate performance issues

## Configuration Examples

### Fast vs. Slow Projections

```go
// Create projections
fastProjection := &FastProjection{}
slowProjection := &SlowProjection{}

// Configure processors independently
var runners []runner.ConsumerRunner

config1 := consumer.ProcessorConfig{
    BatchSize:         1000, // Large batches for throughput
    PartitionKey:      0,
    TotalPartitions:   1,
    PartitionStrategy: consumer.HashPartitionStrategy{},
}
processor1 := postgres.NewProcessor(db, store, &config1)
runners = append(runners, runner.ConsumerRunner{
    Consumer: fastProjection,
    Processor:  processor1,
})

config2 := consumer.ProcessorConfig{
    BatchSize:         10, // Small batches to avoid blocking
    PartitionKey:      0,
    TotalPartitions:   1,
    PartitionStrategy: consumer.HashPartitionStrategy{},
}
processor2 := postgres.NewProcessor(db, store, &config2)
runners = append(runners, runner.ConsumerRunner{
    Consumer: slowProjection,
    Processor:  processor2,
})

// Run all projections
r := runner.New()
err := r.Run(ctx, runners)
```

### Mixed Partitioning

```go
// Create projections
noPartProj := &NoPartitionProjection{}
partitionedProj := &PartitionedProjection{}

var runners []runner.ConsumerRunner

config1 := consumer.ProcessorConfig{
    PartitionKey:      0,
    TotalPartitions:   1, // Single worker
    BatchSize:         100,
    PartitionStrategy: consumer.HashPartitionStrategy{},
}
processor1 := postgres.NewProcessor(db, store, &config1)
runners = append(runners, runner.ConsumerRunner{
    Consumer: noPartProj,
    Processor:  processor1,
})

config2 := consumer.ProcessorConfig{
    PartitionKey:      workerID,
    TotalPartitions:   4, // Scaled across 4 workers
    BatchSize:         100,
    PartitionStrategy: consumer.HashPartitionStrategy{},
}
processor2 := postgres.NewProcessor(db, store, &config2)
runners = append(runners, runner.ConsumerRunner{
    Consumer: partitionedProj,
    Processor:  processor2,
})

// Run all projections
r := runner.New()
err := r.Run(ctx, runners)
```

## Production Considerations

1. **Monitoring**: Log projection names with metrics to identify bottlenecks
2. **Error Handling**: One projection error stops all (fail-fast by design)
3. **Memory**: Total memory usage is sum of all projections
4. **CPU**: Projections run concurrently (use goroutines efficiently)

## Scaling Strategy

**To scale individual projections:**
1. Move the slow projection to its own process
2. Partition it across multiple workers
3. Keep fast projections together

See:
- `../partitioned` - Scale a projection across processes
- `../worker-pool` - Scale a projection within a process

## See Also

- `../single-worker` - Simplest pattern (one projection)
- `../worker-pool` - Multiple partitions of the same projection
- `../scaling` - Demonstrates adding projections dynamically
