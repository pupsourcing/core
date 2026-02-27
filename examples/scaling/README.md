# Scaling Example - From 1 to N Workers

This example demonstrates how to safely scale a projection from 1 worker to N workers without data loss or downtime.

## What It Demonstrates

- Adding workers incrementally (1 → 2 → 3 → 4)
- Each worker independently catches up from its checkpoint
- No coordination needed between workers
- Safe to add/remove workers at any time

## Running the Example

### Step 1: Prepare Database and Events

```bash
# Start PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_example postgres:16

# Apply migrations (from ../basic)
cd ../basic && go generate

# Append sample events (one-time)
cd ../scaling
go run main.go --worker-id=0 --append
```

### Step 2: Start with 1 Worker

**Terminal 1:**
```bash
WORKER_ID=0 go run main.go
```

Watch it process events. It handles 100% of events since it's the only worker.

### Step 3: Add Second Worker

**Terminal 2:**
```bash
WORKER_ID=1 go run main.go
```

Now you have 2 workers:
- Worker 0: Processes ~50% of events (its partition)
- Worker 1: Catches up and processes ~50% of events (its partition)

### Step 4: Add Third Worker

**Terminal 3:**
```bash
WORKER_ID=2 go run main.go
```

Now you have 3 workers, each handling ~33% of events.

### Step 5: Add Fourth Worker

**Terminal 4:**
```bash
WORKER_ID=3 go run main.go
```

Now you have 4 workers, each handling ~25% of events.

## Key Observations

### 1. Independent Catchup

Each new worker:
- Starts from its own checkpoint (position 0 initially)
- Reads events from the beginning
- Skips events not in its partition
- Catches up to real-time independently

### 2. No Coordination Required

- No need to pause existing workers
- No need to reconfigure existing workers
- No distributed locks or leader election
- Just start the new worker

### 3. Load Distribution

Watch the logs to see:
- Which aggregate IDs go to which worker (deterministic)
- Each worker processes approximately equal events
- Same aggregate always goes to the same worker

## Scaling Patterns

### Scaling Up (1 → N)

```bash
# Start with 1 worker
WORKER_ID=0 TOTAL_WORKERS=4 go run main.go

# Add more workers as needed
WORKER_ID=1 TOTAL_WORKERS=4 go run main.go
WORKER_ID=2 TOTAL_WORKERS=4 go run main.go
WORKER_ID=3 TOTAL_WORKERS=4 go run main.go
```

### Scaling Down (N → M)

Simply stop workers. Their checkpoints remain in the database.

```bash
# Stop worker 3 (Ctrl+C)
# Stop worker 2 (Ctrl+C)
# Workers 0 and 1 continue processing their partitions
```

### Changing Partition Count

**⚠️ Important**: Changing the total number of partitions requires reconfiguring ALL workers:

```bash
# From 4 partitions to 8 partitions
# 1. Stop all workers
# 2. Start 8 new workers with --total-workers=8
# 3. Each will reprocess events (idempotent projections required)
```

## Production Deployment

### Kubernetes Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: projection-worker
spec:
  replicas: 4
  template:
    spec:
      containers:
      - name: worker
        image: myapp:latest
        env:
        - name: WORKER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.labels['statefulset.kubernetes.io/pod-name']
        - name: TOTAL_WORKERS
          value: "4"
        command: ["./myapp", "projection-worker"]
```

### Docker Compose Example

```yaml
version: '3.8'
services:
  worker-0:
    image: myapp:latest
    environment:
      WORKER_ID: "0"
      TOTAL_WORKERS: "4"
    command: ["./myapp", "projection-worker"]
  
  worker-1:
    image: myapp:latest
    environment:
      WORKER_ID: "1"
      TOTAL_WORKERS: "4"
    command: ["./myapp", "projection-worker"]
  
  # ... worker-2, worker-3
```

### Systemd Example

```ini
# /etc/systemd/system/projection-worker@.service
[Unit]
Description=Projection Worker %i
After=network.target postgresql.service

[Service]
Type=simple
User=myapp
Environment="WORKER_ID=%i"
Environment="TOTAL_WORKERS=4"
ExecStart=/usr/local/bin/myapp projection-worker
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start workers
sudo systemctl enable projection-worker@0
sudo systemctl enable projection-worker@1
sudo systemctl enable projection-worker@2
sudo systemctl enable projection-worker@3
sudo systemctl start projection-worker@0
sudo systemctl start projection-worker@1
sudo systemctl start projection-worker@2
sudo systemctl start projection-worker@3
```

## Monitoring

### What to Monitor

1. **Checkpoint Lag**: How far behind real-time each worker is
2. **Events Processed**: Rate of event processing per worker
3. **Worker Health**: Is each worker running and processing events?

### Example Query - Check Lag

```sql
SELECT 
    consumer_name,
    last_global_position,
    (SELECT MAX(global_position) FROM events) - last_global_position as lag
FROM consumer_checkpoints
WHERE consumer_name = 'scalable_projection';
```

## Common Scenarios

### Adding Capacity

Your projection is falling behind:
1. Check current worker count
2. Add more workers (double the count is common)
3. Watch lag decrease as new workers catch up

### Removing Capacity

Your projection is over-provisioned:
1. Stop unnecessary workers
2. Remaining workers continue processing their partitions
3. Checkpoints for stopped workers remain (safe to restart later)

### Worker Failure

A worker crashes:
1. Other workers continue unaffected
2. Restart the crashed worker
3. It resumes from its checkpoint automatically

## See Also

- `../partitioned` - The pattern this example demonstrates
- `../worker-pool` - Similar but in a single process
- `../stop-resume` - Demonstrates checkpoint reliability
