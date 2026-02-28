# Custom Logging Example

This example demonstrates how to integrate custom logging with pupsourcing using the `es.Logger` interface.

## What It Does

- Shows how to implement the `es.Logger` interface
- Demonstrates logging integration with both event store and projection processor
- Provides visibility into internal operations for debugging and monitoring
- Shows the type of information logged at different levels (DEBUG, INFO, ERROR)

## Prerequisites

1. PostgreSQL running on localhost:5432
2. Database named `pupsourcing_example`
3. Schema migrations applied (see `../basic` example)

## Running the Example

### Step 1: Start PostgreSQL

```bash
docker run -d -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=pupsourcing_example \
  postgres:16
```

### Step 2: Apply Migrations

```bash
cd ../basic
go generate
psql -h localhost -U postgres -d pupsourcing_example < ../../migrations/init.sql
```

### Step 3: Run the Example

```bash
cd ../with-logging
go run main.go
```

## Expected Output

```
=== Pupsourcing with Logging Example ===

--- Appending Events ---
[DEBUG] [Store] appending events count=2
[INFO] [Store] events appended global_positions=[1,2] aggregate_version=2
✓ Events appended at positions: [1 2]
✓ Aggregate is now at version: 2

--- Running Projection with Logging ---
[INFO] [Projection] starting projection consumer_name=user_list_with_logging
[DEBUG] [Projection] loading checkpoint consumer_name=user_list_with_logging
[DEBUG] [Projection] reading events from_position=0 batch_size=100
[INFO] [Projection] processing batch consumer_name=user_list_with_logging event_count=2
✓ Projection processed: User created - Alice Smith (alice@example.com)
✓ Projection processed: User created - Bob Jones (bob@example.com)
[DEBUG] [Projection] saving checkpoint consumer_name=user_list_with_logging position=2

✓ Projection result - Users: [alice@example.com bob@example.com]

=== Example Complete ===

Note: Check the log output above to see the observability hooks in action.
In production, integrate with your preferred logging library (zap, zerolog, logrus, slog, etc.)
```

## Implementing the Logger Interface

The example provides a simple implementation:

```go
type SimpleLogger struct {
    prefix string
}

func (l *SimpleLogger) Debug(ctx context.Context, msg string, keyvals ...interface{}) {
    l.log("DEBUG", msg, keyvals...)
}

func (l *SimpleLogger) Info(ctx context.Context, msg string, keyvals ...interface{}) {
    l.log("INFO", msg, keyvals...)
}

func (l *SimpleLogger) Error(ctx context.Context, msg string, keyvals ...interface{}) {
    l.log("ERROR", msg, keyvals...)
}

func (l *SimpleLogger) log(level, msg string, keyvals ...interface{}) {
    var kvStr string
    for i := 0; i < len(keyvals); i += 2 {
        if i+1 < len(keyvals) {
            kvStr += fmt.Sprintf(" %v=%v", keyvals[i], keyvals[i+1])
        }
    }
    log.Printf("[%s] %s%s%s", level, l.prefix, msg, kvStr)
}
```

## Integrating with Event Store

Pass your logger to the store configuration:

```go
storeConfig := postgres.DefaultStoreConfig()
storeConfig.Logger = storeLogger
store := postgres.NewStore(storeConfig)
```

The event store logs:
- **DEBUG**: Detailed operation information (queries, parameters)
- **INFO**: High-level operations (events appended, positions)
- **ERROR**: Failures and errors

## Integrating with Projection Processor

Pass your logger to the processor configuration:

```go
processorConfig := consumer.DefaultBasicProcessorConfig()
processorConfig.Logger = projectionLogger
processor := postgres.NewBasicProcessor(db, store, &processorConfig)
```

The processor logs:
- **DEBUG**: Checkpoint loads/saves, batch reading
- **INFO**: Projection lifecycle, batch processing
- **ERROR**: Event handling failures, checkpoint errors

## Production Integration

### With Structured Logging (zerolog)

```go
import "github.com/rs/zerolog"

type ZerologAdapter struct {
    logger zerolog.Logger
}

func (a *ZerologAdapter) Debug(ctx context.Context, msg string, keyvals ...interface{}) {
    event := a.logger.Debug()
    for i := 0; i < len(keyvals); i += 2 {
        if i+1 < len(keyvals) {
            event = event.Interface(fmt.Sprint(keyvals[i]), keyvals[i+1])
        }
    }
    event.Msg(msg)
}

func (a *ZerologAdapter) Info(ctx context.Context, msg string, keyvals ...interface{}) {
    event := a.logger.Info()
    for i := 0; i < len(keyvals); i += 2 {
        if i+1 < len(keyvals) {
            event = event.Interface(fmt.Sprint(keyvals[i]), keyvals[i+1])
        }
    }
    event.Msg(msg)
}

func (a *ZerologAdapter) Error(ctx context.Context, msg string, keyvals ...interface{}) {
    event := a.logger.Error()
    for i := 0; i < len(keyvals); i += 2 {
        if i+1 < len(keyvals) {
            event = event.Interface(fmt.Sprint(keyvals[i]), keyvals[i+1])
        }
    }
    event.Msg(msg)
}
```

### With slog (Go 1.21+)

```go
import "log/slog"

type SlogAdapter struct {
    logger *slog.Logger
}

func (a *SlogAdapter) Debug(ctx context.Context, msg string, keyvals ...interface{}) {
    a.logger.DebugContext(ctx, msg, keyvals...)
}

func (a *SlogAdapter) Info(ctx context.Context, msg string, keyvals ...interface{}) {
    a.logger.InfoContext(ctx, msg, keyvals...)
}

func (a *SlogAdapter) Error(ctx context.Context, msg string, keyvals ...interface{}) {
    a.logger.ErrorContext(ctx, msg, keyvals...)
}
```

### With zap

```go
import "go.uber.org/zap"

type ZapAdapter struct {
    logger *zap.Logger
}

func (a *ZapAdapter) Debug(ctx context.Context, msg string, keyvals ...interface{}) {
    fields := make([]zap.Field, 0, len(keyvals)/2)
    for i := 0; i < len(keyvals); i += 2 {
        if i+1 < len(keyvals) {
            fields = append(fields, zap.Any(fmt.Sprint(keyvals[i]), keyvals[i+1]))
        }
    }
    a.logger.Debug(msg, fields...)
}

func (a *ZapAdapter) Info(ctx context.Context, msg string, keyvals ...interface{}) {
    fields := make([]zap.Field, 0, len(keyvals)/2)
    for i := 0; i < len(keyvals); i += 2 {
        if i+1 < len(keyvals) {
            fields = append(fields, zap.Any(fmt.Sprint(keyvals[i]), keyvals[i+1]))
        }
    }
    a.logger.Info(msg, fields...)
}

func (a *ZapAdapter) Error(ctx context.Context, msg string, keyvals ...interface{}) {
    fields := make([]zap.Field, 0, len(keyvals)/2)
    for i := 0; i < len(keyvals); i += 2 {
        if i+1 < len(keyvals) {
            fields = append(fields, zap.Any(fmt.Sprint(keyvals[i]), keyvals[i+1]))
        }
    }
    a.logger.Error(msg, fields...)
}
```

## Key-Value Pairs

The logger interface uses key-value pairs for structured logging:

```go
logger.Info(ctx, "events appended", 
    "aggregate_id", aggregateID,
    "version", 5,
    "event_count", len(events))
```

These are passed as variadic arguments and should be processed in pairs:
- Even indices (0, 2, 4...) are keys (typically strings)
- Odd indices (1, 3, 5...) are values (any type)

## Benefits of Logging Integration

1. **Debugging**: See exactly what's happening inside the library
2. **Performance Monitoring**: Track batch sizes, processing times
3. **Audit Trail**: Log all event appends and checkpoint updates
4. **Error Tracking**: Capture and alert on failures
5. **Metrics**: Extract metrics from log events

## Common Log Patterns

### Store Operations
```
[DEBUG] appending events count=5
[INFO] events appended global_positions=[101,102,103,104,105] aggregate_version=10
```

### Projection Processing
```
[INFO] starting projection consumer_name=user_counter
[DEBUG] loading checkpoint consumer_name=user_counter
[INFO] processing batch consumer_name=user_counter event_count=100
[DEBUG] saving checkpoint consumer_name=user_counter position=150
```

### Error Scenarios
```
[ERROR] failed to append events error="optimistic concurrency violation"
[ERROR] projection handler failed consumer_name=user_counter event_id=123 error="..."
```

## Next Steps

- See `../basic` for the same example without logging
- Integrate with your production logging framework
- Add metrics collection based on log events
- Configure log levels per environment (verbose dev, quiet prod)

## See Also

- `../basic` - Basic example without logging
- `es.Logger` interface in `es/logger.go`
- Popular Go logging libraries: zap, zerolog, logrus, slog
