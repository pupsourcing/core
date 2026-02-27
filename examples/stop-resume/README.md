# Stop and Resume Example

This example demonstrates that projections can be stopped at any time and will resume exactly where they left off, with no data loss.

## What It Demonstrates

- Checkpoint persistence across restarts
- Graceful shutdown with progress preservation
- No data loss when stopping/restarting
- Status checking between runs

## Running the Example

### Step 1: Prepare Database

```bash
# Start PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_example postgres:16

# Apply migrations (from ../basic)
cd ../basic && go generate
```

### Step 2: Append Events

```bash
cd ../stop-resume
go run main.go --mode=append --events=20
```

This creates 20 events in the event store.

### Step 3: Check Status (Optional)

```bash
go run main.go --mode=status
```

Output:
```
Current checkpoint position: 0
Total events in store: 20
Status: Not started (will process all 20 events)
```

### Step 4: Start Processing

```bash
go run main.go --mode=process
```

You'll see:
```
Starting fresh (no checkpoint found)
Processing events... (Press Ctrl+C to stop)
✓ Processed event #1 (position: 1, aggregate: 3f8a2b1c): User 1
✓ Processed event #2 (position: 2, aggregate: 7a9c4d2e): User 2
...
```

**Stop it with Ctrl+C after a few events.**

### Step 5: Check Status Again

```bash
go run main.go --mode=status
```

Output:
```
Current checkpoint position: 5
Total events in store: 20
Status: Behind by 15 events (25.0% complete)
```

### Step 6: Resume Processing

```bash
go run main.go --mode=process
```

You'll see:
```
Resuming from checkpoint: position 5
Processing events... (Press Ctrl+C to stop)
✓ Processed event #1 (position: 6, aggregate: a2b3c4d5): User 6
✓ Processed event #2 (position: 7, aggregate: b3c4d5e6): User 7
...
```

**Notice:** It continues from event #6, not event #1!

### Step 7: Process Everything

Let it run until it processes all events, then Ctrl+C:

```bash
go run main.go --mode=status
```

Output:
```
Current checkpoint position: 20
Total events in store: 20
Status: Caught up (all events processed)
```

## Key Concepts

### Checkpoint Reliability

The checkpoint is persisted in the database **atomically** with event processing:
- Each batch of events is processed in a transaction
- The checkpoint is updated in the same transaction
- Either both succeed or both roll back
- No partial updates, no data loss

### Graceful Shutdown

When you press Ctrl+C:
1. Context is cancelled
2. Current batch completes (if any)
3. Checkpoint is saved
4. Process exits cleanly

**Important:** Don't kill the process (kill -9) during a batch - let it shut down gracefully.

### Resumption Guarantee

When restarted:
1. Reads checkpoint from database
2. Resumes from that position
3. Never reprocesses events (unless projection changed)
4. Never skips events

## Modes

### Append Mode

Creates events in the event store:
```bash
go run main.go --mode=append --events=10
```

### Process Mode

Runs the projection (can be stopped/resumed):
```bash
go run main.go --mode=process
```

### Status Mode

Shows current checkpoint and lag:
```bash
go run main.go --mode=status
```

## Production Scenarios

### Deployment

When deploying a new version:
1. Stop the current projection (Ctrl+C or SIGTERM)
2. Wait for graceful shutdown
3. Deploy new binary
4. Start new version - it resumes from checkpoint

### Maintenance

During database maintenance:
1. Stop all projections
2. Perform maintenance
3. Restart projections - they resume automatically

### Crash Recovery

If process crashes (kill -9, OOM, etc.):
- Checkpoint from last successful batch is preserved
- May need to reprocess the incomplete batch (usually small)
- Projection must be idempotent to handle reprocessing

## Idempotency Considerations

Since the same events might be reprocessed (in rare crash scenarios), projections should be idempotent:

### Good: Idempotent Operations

```go
// Update read model (idempotent)
_, err := tx.ExecContext(ctx,
    "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)"+
    "ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name",
    user.ID, user.Email, user.Name)
```

### Bad: Non-Idempotent Operations

```go
// Increment counter (NOT idempotent)
_, err := tx.ExecContext(ctx,
    "UPDATE stats SET user_count = user_count + 1")

// Better: Store event ID to track processed events
_, err := tx.ExecContext(ctx,
    "INSERT INTO stats_events (event_id, increment) VALUES ($1, 1)"+
    "ON CONFLICT (event_id) DO NOTHING")
```

## Monitoring Checkpoints

### SQL Query - Check All Projections

```sql
SELECT 
    consumer_name,
    last_global_position,
    (SELECT MAX(global_position) FROM events) - last_global_position as lag,
    updated_at
FROM consumer_checkpoints
ORDER BY lag DESC;
```

### SQL Query - Projection Health

```sql
SELECT 
    consumer_name,
    last_global_position,
    updated_at,
    CASE 
        WHEN updated_at < NOW() - INTERVAL '5 minutes' THEN 'STALE'
        WHEN last_global_position >= (SELECT MAX(global_position) FROM events) THEN 'CAUGHT_UP'
        ELSE 'PROCESSING'
    END as status
FROM consumer_checkpoints;
```

## Common Questions

### Q: What if I restart immediately?
**A:** No problem. Checkpoint was saved before shutdown.

### Q: What if power fails during processing?
**A:** Last complete batch was checkpointed. Incomplete batch will be reprocessed.

### Q: Can I run multiple instances?
**A:** Yes, with partitioning. Each partition has its own checkpoint. See `../partitioned`.

### Q: How often is checkpoint saved?
**A:** After every batch (default 100 events). Configurable via `BatchSize`.

### Q: What if checkpoint is lost?
**A:** Projection reprocesses from position 0. Must be idempotent.

## See Also

- `../single-worker` - Basic projection pattern
- `../scaling` - Adding workers dynamically
- `../partitioned` - Multiple workers with independent checkpoints
