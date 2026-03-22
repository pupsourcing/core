// Package postgres provides a PostgreSQL adapter for event sourcing.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/store"
)

// StoreConfig contains configuration for the Postgres event store.
// Configuration is immutable after construction.
type StoreConfig struct {
	// Logger is an optional logger for observability.
	// If nil, logging is disabled (zero overhead).
	Logger es.Logger

	// EventsTable is the name of the events table
	EventsTable string

	// AggregateHeadsTable is the name of the aggregate version tracking table
	AggregateHeadsTable string

	// SegmentsTable is the name of the consumer segments table
	SegmentsTable string

	// WorkerRegistryTable is the name of the worker registry table
	WorkerRegistryTable string

	// NotifyChannel is the Postgres NOTIFY channel name for event append notifications.
	// When set, Append() executes pg_notify within the same transaction, so the
	// notification fires only when the transaction commits.
	// Leave empty to disable notifications.
	NotifyChannel string
}

// DefaultStoreConfig returns the default configuration.
func DefaultStoreConfig() *StoreConfig {
	return &StoreConfig{
		EventsTable:         "events",
		AggregateHeadsTable: "aggregate_heads",
		SegmentsTable:       "consumer_segments",
		WorkerRegistryTable: "consumer_workers",
		Logger:              nil, // No logging by default
	}
}

// StoreOption is a functional option for configuring a Store.
type StoreOption func(*StoreConfig)

// WithLogger sets a logger for the store.
func WithLogger(logger es.Logger) StoreOption {
	return func(c *StoreConfig) {
		c.Logger = logger
	}
}

// WithEventsTable sets a custom events table name.
func WithEventsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.EventsTable = tableName
	}
}

// WithAggregateHeadsTable sets a custom aggregate heads table name.
func WithAggregateHeadsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.AggregateHeadsTable = tableName
	}
}

// WithSegmentsTable sets a custom consumer segments table name.
func WithSegmentsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.SegmentsTable = tableName
	}
}

// WithWorkerRegistryTable sets a custom worker registry table name.
func WithWorkerRegistryTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.WorkerRegistryTable = tableName
	}
}

// WithNotifyChannel sets the Postgres NOTIFY channel for event append notifications.
// When configured, each Append() call issues pg_notify within the same transaction,
// so the notification fires only when the transaction commits.
func WithNotifyChannel(channel string) StoreOption {
	return func(c *StoreConfig) {
		c.NotifyChannel = channel
	}
}

// NewStoreConfig creates a new store configuration with functional options.
// It starts with the default configuration and applies the given options.
//
// Example:
//
//	config := postgres.NewStoreConfig(
//	    postgres.WithLogger(myLogger),
//	    postgres.WithEventsTable("custom_events"),
//	)
func NewStoreConfig(opts ...StoreOption) *StoreConfig {
	config := DefaultStoreConfig()
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// Store is a PostgreSQL-backed event store implementation.
type Store struct {
	config StoreConfig
}

// ReadEventsScope applies optional bounded context and aggregate type filters
// to sequential event reads. Empty slices disable the corresponding filter.
type ReadEventsScope struct {
	AggregateTypes  []string
	BoundedContexts []string
}

// NewStore creates a new Postgres event store with the given configuration.
func NewStore(config *StoreConfig) *Store {
	return &Store{
		config: *config,
	}
}

// Append implements store.EventStore.
// It automatically assigns aggregate versions using the aggregate_heads table for O(1) lookup.
// The expectedVersion parameter controls optimistic concurrency validation.
// The database constraint on (aggregate_type, aggregate_id, aggregate_version) enforces
// optimistic concurrency as a safety net - if another transaction commits between our version
// check and insert, the insert will fail with a unique constraint violation.
//
//nolint:gocyclo // Cyclomatic complexity is acceptable here - comes from necessary logging and validation checks
func (s *Store) Append(ctx context.Context, tx es.DBTX, expectedVersion es.ExpectedVersion, events []es.Event) (es.AppendResult, error) {
	if len(events) == 0 {
		return es.AppendResult{}, store.ErrNoEvents
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "append starting",
			"event_count", len(events),
			"expected_version", expectedVersion.String())
	}

	// Validate all events belong to same aggregate and have BoundedContext
	firstEvent := events[0]
	if firstEvent.BoundedContext == "" {
		return es.AppendResult{}, fmt.Errorf("bounded context is required")
	}
	for i := range events {
		e := &events[i]
		if e.BoundedContext == "" {
			return es.AppendResult{}, fmt.Errorf("event %d: bounded context is required", i)
		}
		if e.BoundedContext != firstEvent.BoundedContext {
			return es.AppendResult{}, fmt.Errorf("event %d: bounded context mismatch", i)
		}
		if e.AggregateType != firstEvent.AggregateType {
			return es.AppendResult{}, fmt.Errorf("event %d: aggregate type mismatch", i)
		}
		if e.AggregateID != firstEvent.AggregateID {
			return es.AppendResult{}, fmt.Errorf("event %d: aggregate ID mismatch", i)
		}
	}

	// Fetch current version from aggregate_heads table
	var currentVersion sql.NullInt64
	query := fmt.Sprintf(`
		SELECT aggregate_version 
		FROM %s 
		WHERE bounded_context = $1 AND aggregate_type = $2 AND aggregate_id = $3
	`, s.config.AggregateHeadsTable)

	err := tx.QueryRowContext(ctx, query, firstEvent.BoundedContext, firstEvent.AggregateType, firstEvent.AggregateID).Scan(&currentVersion)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return es.AppendResult{}, fmt.Errorf("failed to check current version: %w", err)
	}

	// Validate expected version
	if !expectedVersion.IsAny() {
		if expectedVersion.IsNoStream() {
			// NoStream: aggregate must not exist
			if currentVersion.Valid {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "expected version validation failed: aggregate already exists",
						"aggregate_type", firstEvent.AggregateType,
						"aggregate_id", firstEvent.AggregateID,
						"current_version", currentVersion.Int64,
						"expected_version", expectedVersion.String())
				}
				return es.AppendResult{}, store.ErrOptimisticConcurrency
			}
		} else if expectedVersion.IsExact() {
			// Exact: aggregate must exist at exact version
			if !currentVersion.Valid {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "expected version validation failed: aggregate does not exist",
						"aggregate_type", firstEvent.AggregateType,
						"aggregate_id", firstEvent.AggregateID,
						"expected_version", expectedVersion.String())
				}
				return es.AppendResult{}, store.ErrOptimisticConcurrency
			}
			if currentVersion.Int64 != expectedVersion.Value() {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "expected version validation failed: version mismatch",
						"aggregate_type", firstEvent.AggregateType,
						"aggregate_id", firstEvent.AggregateID,
						"current_version", currentVersion.Int64,
						"expected_version", expectedVersion.String())
				}
				return es.AppendResult{}, store.ErrOptimisticConcurrency
			}
		}
	}

	// Determine starting version for new events
	var nextVersion int64
	if currentVersion.Valid {
		nextVersion = currentVersion.Int64 + 1
	} else {
		nextVersion = 1 // First event for this aggregate
	}

	if s.config.Logger != nil {
		if currentVersion.Valid {
			s.config.Logger.Debug(ctx, "version calculated",
				"aggregate_type", firstEvent.AggregateType,
				"aggregate_id", firstEvent.AggregateID,
				"current_version", currentVersion.Int64,
				"next_version", nextVersion)
		} else {
			s.config.Logger.Debug(ctx, "version calculated",
				"aggregate_type", firstEvent.AggregateType,
				"aggregate_id", firstEvent.AggregateID,
				"current_version", "none",
				"next_version", nextVersion)
		}
	}

	// Insert events with auto-assigned versions and collect global positions and persisted events
	globalPositions := make([]int64, len(events))
	persistedEvents := make([]es.PersistedEvent, len(events))
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			bounded_context, aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING global_position
	`, s.config.EventsTable)

	for i := range events {
		event := &events[i]
		aggregateVersion := nextVersion + int64(i)

		var globalPos int64
		err = tx.QueryRowContext(ctx, insertQuery,
			event.BoundedContext,
			event.AggregateType,
			event.AggregateID,
			aggregateVersion,
			event.EventID,
			event.EventType,
			event.EventVersion,
			event.Payload,
			event.TraceID,
			event.CorrelationID,
			event.CausationID,
			event.Metadata,
			event.CreatedAt,
		).Scan(&globalPos)

		if err != nil {
			// Check if this is a unique constraint violation (optimistic concurrency failure)
			if IsUniqueViolation(err) {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "optimistic concurrency conflict",
						"aggregate_type", event.AggregateType,
						"aggregate_id", event.AggregateID,
						"aggregate_version", aggregateVersion)
				}
				return es.AppendResult{}, store.ErrOptimisticConcurrency
			}
			return es.AppendResult{}, fmt.Errorf("failed to insert event %d: %w", i, err)
		}
		globalPositions[i] = globalPos

		// Build persisted event
		persistedEvents[i] = es.PersistedEvent{
			GlobalPosition:   globalPos,
			BoundedContext:   event.BoundedContext,
			AggregateType:    event.AggregateType,
			AggregateID:      event.AggregateID,
			AggregateVersion: aggregateVersion,
			EventID:          event.EventID,
			EventType:        event.EventType,
			EventVersion:     event.EventVersion,
			Payload:          event.Payload,
			TraceID:          event.TraceID,
			CorrelationID:    event.CorrelationID,
			CausationID:      event.CausationID,
			Metadata:         event.Metadata,
			CreatedAt:        event.CreatedAt,
		}
	}

	// Update aggregate_heads with the new version (UPSERT pattern)
	latestVersion := nextVersion + int64(len(events)) - 1
	upsertQuery := fmt.Sprintf(`
		INSERT INTO %s (bounded_context, aggregate_type, aggregate_id, aggregate_version, updated_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (bounded_context, aggregate_type, aggregate_id)
		DO UPDATE SET aggregate_version = $4, updated_at = NOW()
	`, s.config.AggregateHeadsTable)

	_, err = tx.ExecContext(ctx, upsertQuery, firstEvent.BoundedContext, firstEvent.AggregateType, firstEvent.AggregateID, latestVersion)
	if err != nil {
		return es.AppendResult{}, fmt.Errorf("failed to update aggregate head: %w", err)
	}

	// Send transactional NOTIFY — fires only when the caller commits the TX
	if s.config.NotifyChannel != "" {
		lastPos := globalPositions[len(globalPositions)-1]
		_, err = tx.ExecContext(ctx, "SELECT pg_notify($1, $2)", s.config.NotifyChannel, fmt.Sprintf("%d", lastPos))
		if err != nil {
			return es.AppendResult{}, fmt.Errorf("failed to send notify: %w", err)
		}
	}

	if s.config.Logger != nil {
		s.config.Logger.Info(ctx, "events appended",
			"aggregate_type", firstEvent.AggregateType,
			"aggregate_id", firstEvent.AggregateID,
			"event_count", len(events),
			"version_range", fmt.Sprintf("%d-%d", nextVersion, latestVersion),
			"positions", globalPositions)
	}

	return es.AppendResult{
		Events:          persistedEvents,
		GlobalPositions: globalPositions,
	}, nil
}

const uniqueViolationSQLState = "23505"

// IsUniqueViolation checks if an error is a PostgreSQL unique constraint violation.
// This is exported for testing purposes.
// Driver-agnostic: works with pq, pgx, and database/sql.
func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	// pgx driver: check for SQLState() method
	type pgxError interface {
		SQLState() string
	}
	var pgxErr pgxError
	if errors.As(err, &pgxErr) {
		return pgxErr.SQLState() == uniqueViolationSQLState
	}

	// pq driver: check for Code field
	var pqErr interface {
		Code() string
	}
	if errors.As(err, &pqErr) {
		return pqErr.Code() == uniqueViolationSQLState
	}

	// Fallback: check error message for common patterns (for wrapped or custom errors)
	errMsg := fmt.Sprintf("%v", err)
	return containsString(errMsg, "duplicate key") || containsString(errMsg, "unique constraint")
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ReadEvents implements store.EventReader.
func (s *Store) ReadEvents(ctx context.Context, tx es.DBTX, fromPosition int64, limit int) ([]es.PersistedEvent, error) {
	return s.readEvents(ctx, tx, fromPosition, limit, ReadEventsScope{})
}

// ReadEventsWithScope reads events sequentially with optional SQL-level scope filters.
// Empty AggregateTypes or BoundedContexts mean "no filter" for that dimension.
func (s *Store) ReadEventsWithScope(ctx context.Context, tx es.DBTX, fromPosition int64, limit int, scope ReadEventsScope) ([]es.PersistedEvent, error) {
	return s.readEvents(ctx, tx, fromPosition, limit, scope)
}

func (s *Store) readEvents(ctx context.Context, tx es.DBTX, fromPosition int64, limit int, scope ReadEventsScope) ([]es.PersistedEvent, error) {
	if s.config.Logger != nil {
		keyvals := []interface{}{"from_position", fromPosition, "limit", limit}
		if len(scope.BoundedContexts) > 0 {
			keyvals = append(keyvals, "bounded_context_filters", len(scope.BoundedContexts))
		}
		if len(scope.AggregateTypes) > 0 {
			keyvals = append(keyvals, "aggregate_type_filters", len(scope.AggregateTypes))
		}
		s.config.Logger.Debug(ctx, "reading events", keyvals...)
	}

	query, args := buildReadEventsQuery(s.config.EventsTable, fromPosition, limit, scope)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []es.PersistedEvent
	for rows.Next() {
		var e es.PersistedEvent
		err := rows.Scan(
			&e.GlobalPosition,
			&e.BoundedContext,
			&e.AggregateType,
			&e.AggregateID,
			&e.AggregateVersion,
			&e.EventID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.TraceID,
			&e.CorrelationID,
			&e.CausationID,
			&e.Metadata,
			&e.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "events read", "count", len(events))
	}

	return events, nil
}

func buildReadEventsQuery(
	eventsTable string,
	fromPosition int64,
	limit int,
	scope ReadEventsScope,
) (query string, args []interface{}) {
	query = fmt.Sprintf(`
		SELECT 
			global_position, bounded_context, aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		FROM %s
		WHERE global_position > $1
	`, eventsTable)

	args = []interface{}{fromPosition}
	nextParam := 2

	if len(scope.BoundedContexts) > 0 {
		query += fmt.Sprintf("\n\t\tAND bounded_context = ANY($%d)", nextParam)
		args = append(args, pq.Array(scope.BoundedContexts))
		nextParam++
	}

	if len(scope.AggregateTypes) > 0 {
		query += fmt.Sprintf("\n\t\tAND aggregate_type = ANY($%d)", nextParam)
		args = append(args, pq.Array(scope.AggregateTypes))
		nextParam++
	}

	query += fmt.Sprintf("\n\t\tORDER BY global_position ASC\n\t\tLIMIT $%d\n\t", nextParam)
	args = append(args, limit)

	return query, args
}

// GetLatestGlobalPosition implements store.GlobalPositionReader.
func (s *Store) GetLatestGlobalPosition(ctx context.Context, tx es.DBTX) (int64, error) {
	query := fmt.Sprintf(`
		SELECT global_position
		FROM %s
		ORDER BY global_position DESC
		LIMIT 1
	`, s.config.EventsTable)

	var position int64
	err := tx.QueryRowContext(ctx, query).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return position, nil
}

// ReadAggregateStream implements store.AggregateStreamReader.
func (s *Store) ReadAggregateStream(ctx context.Context, tx es.DBTX, boundedContext, aggregateType, aggregateID string, fromVersion, toVersion *int64) (es.Stream, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "reading aggregate stream",
			"bounded_context", boundedContext,
			"aggregate_type", aggregateType,
			"aggregate_id", aggregateID,
			"from_version", fromVersion,
			"to_version", toVersion)
	}

	// Build the query dynamically based on optional version parameters
	baseQuery := fmt.Sprintf(`
		SELECT 
			global_position, bounded_context, aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		FROM %s
		WHERE bounded_context = $1 AND aggregate_type = $2 AND aggregate_id = $3
	`, s.config.EventsTable)

	var args []interface{}
	args = append(args, boundedContext, aggregateType, aggregateID)
	paramIndex := 4

	// Add version range filters if specified
	if fromVersion != nil {
		baseQuery += fmt.Sprintf(" AND aggregate_version >= $%d", paramIndex)
		args = append(args, *fromVersion)
		paramIndex++
	}

	if toVersion != nil {
		baseQuery += fmt.Sprintf(" AND aggregate_version <= $%d", paramIndex)
		args = append(args, *toVersion)
	}

	// Always order by aggregate_version ASC
	baseQuery += " ORDER BY aggregate_version ASC"

	rows, err := tx.QueryContext(ctx, baseQuery, args...)
	if err != nil {
		return es.Stream{}, fmt.Errorf("failed to query aggregate stream: %w", err)
	}
	defer rows.Close()

	var events []es.PersistedEvent
	for rows.Next() {
		var e es.PersistedEvent
		err := rows.Scan(
			&e.GlobalPosition,
			&e.BoundedContext,
			&e.AggregateType,
			&e.AggregateID,
			&e.AggregateVersion,
			&e.EventID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.TraceID,
			&e.CorrelationID,
			&e.CausationID,
			&e.Metadata,
			&e.CreatedAt,
		)
		if err != nil {
			return es.Stream{}, fmt.Errorf("failed to scan event: %w", err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return es.Stream{}, fmt.Errorf("rows error: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "aggregate stream read",
			"bounded_context", boundedContext,
			"aggregate_type", aggregateType,
			"aggregate_id", aggregateID,
			"event_count", len(events))
	}

	return es.Stream{
		BoundedContext: boundedContext,
		AggregateType:  aggregateType,
		AggregateID:    aggregateID,
		Events:         events,
	}, nil
}
