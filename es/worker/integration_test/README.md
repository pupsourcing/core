# Worker Integration Tests

This directory contains integration tests for the `es/worker` package. These tests verify the end-to-end behavior of the Worker orchestrator with a real PostgreSQL database.

## Prerequisites

- PostgreSQL instance running and accessible
- Database created for testing (default: `pupsourcing_test`)

## Running the Tests

### Using Docker Compose (Recommended)

From the repository root:

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Run the tests
go test -tags=integration -v ./es/worker/integration_test/...

# Stop PostgreSQL
docker-compose down
```

### Using Custom PostgreSQL Instance

Configure the connection using environment variables:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=pupsourcing_test

go test -tags=integration -v ./es/worker/integration_test/...
```

### Quick Test (Single Test)

Run a specific test:

```bash
go test -tags=integration -v -run TestWorkerProcessesEvents ./es/worker/integration_test/...
```

## Test Coverage

### Basic Functionality
- **TestWorkerProcessesEvents**: Basic end-to-end event processing
- **TestWorkerWithOptions**: Custom configuration options (segments, batch size)
- **TestWorkerNoConsumers**: Error handling when no consumers provided

### Multiple Consumers
- **TestWorkerMultipleConsumers**: Multiple consumers processing events independently
- **TestWorkerScopedConsumer**: Scoped consumers with bounded context and aggregate type filtering

### Error Handling & Resilience
- **TestWorkerHandlerError**: Worker stops when consumer handler returns error
- **TestWorkerContextCancellation**: Worker respects context cancellation
- **TestWorkerResumesFromCheckpoint**: Worker resumes from last checkpoint after restart

## Test Structure

Each test follows this pattern:

1. **Setup**: Create clean database tables
2. **Arrange**: Insert test events into event store
3. **Act**: Run worker with test consumers
4. **Assert**: Verify expected events were processed
5. **Cleanup**: Context cancellation stops worker gracefully

## Important Notes

- Tests use the build tag `//go:build integration` to separate them from unit tests
- Each test creates its own clean database schema
- Tests use polling loops to wait for async event processing
- Consumer checkpoints are tested for proper resumption behavior
- Scoped consumers are tested to ensure filtering works correctly

## Troubleshooting

**Connection refused errors**:
- Ensure PostgreSQL is running: `docker-compose ps`
- Verify connection settings match your PostgreSQL instance

**Test timeouts**:
- Increase test timeout: `go test -tags=integration -timeout 30s`
- Check PostgreSQL logs for errors: `docker-compose logs postgres`

**Permission errors**:
- Ensure the test database exists and user has permissions
- Create database: `createdb pupsourcing_test` (if using local PostgreSQL)
