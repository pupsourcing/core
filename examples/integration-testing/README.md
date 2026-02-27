# Integration Testing with One-Off Projection Mode

This example demonstrates how to use `RunModeOneOff` for synchronous projection testing in integration tests.

## Problem

In production, projections run continuously (`RunModeContinuous`), polling for new events indefinitely. This creates challenges for integration tests:

- Tests need to manage concurrent goroutines
- Hard to know when projection processing has completed
- Difficult to assert final state without timing issues
- Tests may be flaky due to race conditions

## Solution

Use `RunModeOneOff` to process projections synchronously in tests:

```go
config := consumer.DefaultProcessorConfig()
config.RunMode = consumer.RunModeOneOff  // Exit after catching up

processor := postgres.NewProcessor(db, store, &config)

// Append test events
appendTestEvents(ctx, db, store, testEvents)

// Process all events synchronously - exits when caught up
err := processor.Run(ctx, myProjection)
if err != nil {
    t.Fatal(err)
}

// Now safely assert projection state
assertProjectionState(t, myProjection)
```

## Benefits

1. **Deterministic**: Process events synchronously without timing issues
2. **Simple**: No goroutines, channels, or context cancellation needed
3. **Fast**: Tests run as fast as possible without polling delays
4. **Clear**: Explicit about test behavior vs production behavior

## Example Test

See `main_test.go` for a complete working example that:
- Appends test events to an event store
- Processes them synchronously using `RunModeOneOff`
- Asserts the final projection state
- Verifies checkpoint was saved correctly

## Running the Example

```bash
# Run the test
go test -v ./examples/integration-testing/

# With integration tag (if needed for your setup)
go test -tags=integration -v ./examples/integration-testing/
```

## Use Cases

- **Integration tests**: Validate projection logic with known event sequences
- **Catch-up operations**: Process historical events once and exit
- **Backfilling**: Rebuild projections from existing event store
- **CI/CD pipelines**: Fast, deterministic tests without timing issues

## Production vs Testing

| Mode | Use Case | Behavior |
|------|----------|----------|
| `RunModeContinuous` | Production | Runs forever, polling for new events |
| `RunModeOneOff` | Testing/Catch-up | Processes available events, then exits cleanly |

## Notes

- `RunModeOneOff` exits with `nil` error when caught up (not an error condition)
- Checkpoints are saved correctly in one-off mode
- Works with all adapters: postgres, mysql, sqlite
- Partition configuration is respected in one-off mode
- Scoped projections work normally in one-off mode
