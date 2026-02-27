# GitHub Copilot Instructions for pupsourcing

## Project Overview

`pupsourcing` is a professional-grade Event Sourcing library for Go, designed with clean architecture principles. The library provides minimal, production-ready infrastructure for event sourcing in Go applications, where state changes are stored as a sequence of events.

## Architecture & Design Principles

### Core Concepts
- **Clean Architecture**: Core interfaces are datastore-agnostic
- **Transaction-Agnostic**: Callers control transaction boundaries using the `DBTX` interface
- **Optimistic Concurrency**: Built-in version conflict detection via database constraints
- **Immutable Events**: Events are value objects until persisted
- **Pull-Based Consumers**: Sequential event processing by global position

### Package Structure
```
es/                      # Core event sourcing packages
├── event.go            # Core event types (Event, PersistedEvent)
├── dbtx.go             # Database transaction abstraction (DBTX interface)
├── store/              # Event store interfaces
├── consumer/          # Consumer processing with checkpoints
├── adapters/
│   └── postgres/       # PostgreSQL implementation
└── migrations/         # Migration generation utilities
cmd/
└── migrate-gen/        # CLI tool for generating database migrations
pkg/                    # Public API entry point
examples/               # Example applications
```

## Development Guidelines

### Language & Versions
- **Go Version**: 1.23 or later (go.mod specifies 1.24.11)
- **PostgreSQL**: Version 12+ (for integration tests)
- **Dependencies**: Minimal - primarily Go standard library, `github.com/google/uuid`, and `github.com/lib/pq`

### Code Style & Conventions

1. **Interface Design**
   - Keep interfaces minimal and focused (e.g., `DBTX` has only 3 methods)
   - Design for both `*sql.DB` and `*sql.Tx` compatibility
   - Use `context.Context` as first parameter for all database operations

2. **Error Handling**
   - Return clear, specific errors (e.g., `ErrOptimisticConcurrency`)
   - Use sentinel errors for expected error cases
   - Always check and propagate errors appropriately

3. **Naming Conventions**
   - Event types: Use descriptive names (e.g., `UserCreated`, `OrderPlaced`)
   - Aggregate types: Singular nouns (e.g., `User`, `Order`)
   - Package names: Short, lowercase, no underscores

4. **Concurrency**
   - Use optimistic concurrency via database constraints
   - Track aggregate versions in the `aggregate_heads` table for O(1) lookups
   - Version conflicts return `ErrOptimisticConcurrency` for retry

5. **Database Schema**
   - Use `BYTEA` for event payloads (supports any serialization format)
   - Use `BIGSERIAL` for `global_position` to ensure ordered event log
   - Use `UUID` for `event_id` and `aggregate_id`
   - Maintain `aggregate_heads` table for efficient version tracking

### Testing Practices

#### Unit Tests
- Run with: `go test ./...`
- Include table-driven tests for comprehensive coverage
- Test files should be named `*_test.go`
- Use `testing.T` for standard tests
- Example: `es/consumer/consumer_test.go`

#### Integration Tests
- Located in `integration_test/` subdirectories
- Require PostgreSQL: `docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_test postgres:16`
- Run with: `go test -tags=integration ./es/adapters/postgres/integration_test/... ./es/consumer/integration_test/...`
- Use `-p 1` flag to run tests sequentially to avoid database conflicts
- Set environment variables: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`

#### Test Commands
```bash
# Unit tests only
go test ./...

# Unit tests with race detection and coverage
go test -v -race -coverprofile=coverage.out ./...

# Integration tests
go test -p 1 -v -tags=integration ./es/adapters/postgres/integration_test/... ./es/consumer/integration_test/...
```

### Linting & Code Quality

- **Linter**: golangci-lint (version 2 config in `.golangci.yml`)
- **Command**: `golangci-lint run` or `golangci-lint run --timeout=5m`
- **Auto-fix**: Use `golangci-lint run --fix` to automatically fix simple linting issues
- **Workflow**: Always run the linter after making code changes, and use `--fix` to resolve fixable issues efficiently
- **MANDATORY**: After fixing any linting issues, **ALWAYS** run the linter globally (`golangci-lint run --timeout=5m`) to ensure no new issues were introduced elsewhere
- **Enabled Linters**: gocritic, gocyclo, gosec, misspell, revive
- **Formatters**: gofmt, goimports (with local prefix `github.com/pupsourcing/core`)
- **Complexity**: Maximum cyclomatic complexity of 15
- **Line Length**: 120 characters max
- **Exclusions**: Test files, examples, and third-party code have relaxed rules

### Building

```bash
# Build all packages
go build -v ./...

# Download dependencies
go mod download

# Verify dependencies
go mod verify

# Generate migrations
go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
```

### Migration Generation

Use the `migrate-gen` tool to create database migrations:
```bash
go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
```

Or add to your code:
```go
//go:generate go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
```

This generates:
- Event store table with proper indexes
- Aggregate heads table for version tracking
- Consumer checkpoint table
- All necessary constraints

## CI/CD

The repository uses GitHub Actions with the following jobs:

1. **Unit Tests** - Runs on Go 1.23, 1.24, 1.25 with race detection
2. **Integration Tests** - Runs on Go 1.25 with PostgreSQL 16 service
3. **Lint** - Runs golangci-lint with 5-minute timeout
4. **Build** - Verifies all packages build successfully

All jobs run on pull requests and pushes to the `master` branch.

## Common Patterns

### Event Appending
```go
// Create events with all required fields
events := []es.Event{
    {
        AggregateType: "User",
        AggregateID:   uuid.New(),
        EventID:       uuid.New(),
        EventType:     "UserCreated",
        EventVersion:  1,
        Payload:       []byte(`{"email":"user@example.com"}`),
        Metadata:      []byte(`{}`),
        CreatedAt:     time.Now(),
    },
}

// Append within a transaction
tx, _ := db.BeginTx(ctx, nil)
result, err := store.Append(ctx, tx, es.NoStream(), events)
if err != nil {
    tx.Rollback()
    return err
}
tx.Commit()

// Access result information
currentVersion := result.ToVersion()
```

### Reading Aggregate Streams
```go
// Read all events for an aggregate
stream, err := store.ReadAggregateStream(ctx, tx, "User", aggregateID, nil, nil)

// Access stream information
currentVersion := stream.Version()
if stream.IsEmpty() {
    // Aggregate doesn't exist
}

// Read from a specific version onwards
fromVersion := int64(5)
stream, err := store.ReadAggregateStream(ctx, tx, "User", aggregateID, &fromVersion, nil)

// Read a version range
toVersion := int64(10)
stream, err := store.ReadAggregateStream(ctx, tx, "User", aggregateID, &fromVersion, &toVersion)

// Process events
for _, event := range stream.Events {
    // Handle event
}
```

### Implementing Consumers
```go
type MyProjection struct {}

func (p *MyProjection) Name() string {
    return "my_projection"
}

func (p *MyProjection) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
    // Process event - update read model, send notifications, etc.
    // Use tx for atomic SQL updates, or ignore for non-SQL integrations
    return nil
}
```

### Horizontal Scaling
```go
config := consumer.DefaultProcessorConfig()
config.TotalPartitions = 4  // Total number of workers
config.PartitionKey = 0     // This worker's partition (0-3)

processor := consumer.NewProcessor(db, store, config)
```

## Best Practices for Contributors

1. **Zero Dependencies**: Avoid adding external dependencies unless absolutely necessary
2. **Clean Interfaces**: Keep interfaces minimal and focused on single responsibilities
3. **Transaction Control**: Never commit/rollback transactions within library code - let callers control boundaries
4. **Immutability**: Events should be immutable once created
5. **Version Assignment**: Let the store assign `AggregateVersion` and `GlobalPosition` automatically
6. **Testing**: Always include both unit tests and integration tests for new features
7. **Documentation**: Update README.md examples when adding new features
8. **Security**: Run `gosec` linter to catch security issues

## Security Considerations

- Use parameterized queries to prevent SQL injection
- Validate input data before persisting events
- Be cautious with payload/metadata content - they're stored as bytes
- Consider data sensitivity when logging events
- Use proper database permissions in production

## Future Roadmap

Current focus (v1):
- Event store with PostgreSQL ✅
- Consumer processing ✅
- Optimistic concurrency ✅
- Horizontal scaling support ✅
- Read API for aggregate streams ✅

Future considerations:
- Snapshots for long-lived aggregates
- Additional database adapters (MySQL, SQLite)
- Observability hooks and metrics
