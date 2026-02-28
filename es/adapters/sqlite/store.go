// Package sqlite provides a SQLite adapter for event sourcing.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/store"
)

const (
	// sqliteDateTimeFormat is the format used for timestamp storage/parsing in SQLite
	sqliteDateTimeFormat = "2006-01-02 15:04:05.999999"
)

// StoreConfig contains configuration for the SQLite event store.
// Configuration is immutable after construction.
type StoreConfig struct {
	// Logger is an optional logger for observability.
	// If nil, logging is disabled (zero overhead).
	Logger es.Logger

	// EventsTable is the name of the events table
	EventsTable string

	// CheckpointsTable is the name of the consumer checkpoints table
	CheckpointsTable string

	// AggregateHeadsTable is the name of the aggregate version tracking table
	AggregateHeadsTable string
}

// DefaultStoreConfig returns the default configuration.
func DefaultStoreConfig() *StoreConfig {
	return &StoreConfig{
		EventsTable:         "events",
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
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

// WithCheckpointsTable sets a custom consumer checkpoints table name.
func WithCheckpointsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.CheckpointsTable = tableName
	}
}

// WithAggregateHeadsTable sets a custom aggregate heads table name.
func WithAggregateHeadsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.AggregateHeadsTable = tableName
	}
}

// NewStoreConfig creates a new store configuration with functional options.
// It starts with the default configuration and applies the given options.
//
// Example:
//
//	config := sqlite.NewStoreConfig(
//	    sqlite.WithLogger(myLogger),
//	    sqlite.WithEventsTable("custom_events"),
//	)
func NewStoreConfig(opts ...StoreOption) *StoreConfig {
	config := DefaultStoreConfig()
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// Store is a SQLite-backed event store implementation.
type Store struct {
	config StoreConfig
}

// NewStore creates a new SQLite event store with the given configuration.
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
		WHERE bounded_context = ? AND aggregate_type = ? AND aggregate_id = ?
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.config.EventsTable)

	for i := range events {
		event := &events[i]
		aggregateVersion := nextVersion + int64(i)

		// Handle nullable strings for SQLite
		var traceID, correlationID, causationID interface{}
		if event.TraceID.Valid {
			traceID = event.TraceID.String
		}
		if event.CorrelationID.Valid {
			correlationID = event.CorrelationID.String
		}
		if event.CausationID.Valid {
			causationID = event.CausationID.String
		}

		result, execErr := tx.ExecContext(ctx, insertQuery,
			event.BoundedContext,
			event.AggregateType,
			event.AggregateID,
			aggregateVersion,
			event.EventID.String(),
			event.EventType,
			event.EventVersion,
			event.Payload,
			traceID,
			correlationID,
			causationID,
			event.Metadata,
			event.CreatedAt.Format(sqliteDateTimeFormat),
		)

		if execErr != nil {
			// Check if this is a unique constraint violation (optimistic concurrency failure)
			if IsUniqueViolation(execErr) {
				if s.config.Logger != nil {
					s.config.Logger.Error(ctx, "optimistic concurrency conflict",
						"aggregate_type", event.AggregateType,
						"aggregate_id", event.AggregateID,
						"aggregate_version", aggregateVersion)
				}
				return es.AppendResult{}, store.ErrOptimisticConcurrency
			}
			return es.AppendResult{}, fmt.Errorf("failed to insert event %d: %w", i, execErr)
		}

		var globalPos int64
		globalPos, err = result.LastInsertId()
		if err != nil {
			return es.AppendResult{}, fmt.Errorf("failed to get last insert id: %w", err)
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

	// Update aggregate_heads with the new version (UPSERT pattern for SQLite)
	latestVersion := nextVersion + int64(len(events)) - 1
	upsertQuery := fmt.Sprintf(`
		INSERT INTO %s (bounded_context, aggregate_type, aggregate_id, aggregate_version, updated_at)
		VALUES (?, ?, ?, ?, datetime('now'))
		ON CONFLICT (bounded_context, aggregate_type, aggregate_id)
		DO UPDATE SET aggregate_version = ?, updated_at = datetime('now')
	`, s.config.AggregateHeadsTable)

	_, err = tx.ExecContext(ctx, upsertQuery, firstEvent.BoundedContext, firstEvent.AggregateType, firstEvent.AggregateID, latestVersion, latestVersion)
	if err != nil {
		return es.AppendResult{}, fmt.Errorf("failed to update aggregate head: %w", err)
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

// IsUniqueViolation checks if an error is a SQLite unique constraint violation.
// This is exported for testing purposes.
func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	// SQLite error messages for unique constraint violations
	errMsg := err.Error()
	return strings.Contains(errMsg, "UNIQUE constraint failed") ||
		strings.Contains(errMsg, "unique constraint") ||
		strings.Contains(errMsg, "constraint failed")
}

// ReadEvents implements store.EventReader.
//
//nolint:gocyclo // Complexity comes from necessary UUID parsing and error handling
func (s *Store) ReadEvents(ctx context.Context, tx es.DBTX, fromPosition int64, limit int) ([]es.PersistedEvent, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "reading events", "from_position", fromPosition, "limit", limit)
	}

	query := fmt.Sprintf(`
		SELECT 
			global_position, bounded_context, aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, trace_id, correlation_id, causation_id,
			metadata, created_at
		FROM %s
		WHERE global_position > ?
		ORDER BY global_position ASC
		LIMIT ?
	`, s.config.EventsTable)

	rows, err := tx.QueryContext(ctx, query, fromPosition, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []es.PersistedEvent
	for rows.Next() {
		var e es.PersistedEvent
		var aggregateID, eventID string
		var createdAt string

		err := rows.Scan(
			&e.GlobalPosition,
			&e.BoundedContext,
			&e.AggregateType,
			&aggregateID,
			&e.AggregateVersion,
			&eventID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.TraceID,
			&e.CorrelationID,
			&e.CausationID,
			&e.Metadata,
			&createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}

		// Aggregate ID is now a string
		e.AggregateID = aggregateID

		// Parse EventID
		e.EventID, err = uuid.Parse(eventID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse event ID: %w", err)
		}

		// Parse timestamp
		e.CreatedAt, err = parseTimestamp(createdAt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse created_at: %w", err)
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
//
//nolint:gocyclo // Complexity comes from necessary UUID parsing and error handling
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
		WHERE bounded_context = ? AND aggregate_type = ? AND aggregate_id = ?
	`, s.config.EventsTable)

	var args []interface{}
	args = append(args, boundedContext, aggregateType, aggregateID)

	// Add version range filters if specified
	if fromVersion != nil {
		baseQuery += " AND aggregate_version >= ?"
		args = append(args, *fromVersion)
	}

	if toVersion != nil {
		baseQuery += " AND aggregate_version <= ?"
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
		var aggID, eventID string
		var createdAt string

		err := rows.Scan(
			&e.GlobalPosition,
			&e.BoundedContext,
			&e.AggregateType,
			&aggID,
			&e.AggregateVersion,
			&eventID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.TraceID,
			&e.CorrelationID,
			&e.CausationID,
			&e.Metadata,
			&createdAt,
		)
		if err != nil {
			return es.Stream{}, fmt.Errorf("failed to scan event: %w", err)
		}

		// Aggregate ID is now a string
		e.AggregateID = aggID

		// Parse EventID
		e.EventID, err = uuid.Parse(eventID)
		if err != nil {
			return es.Stream{}, fmt.Errorf("failed to parse event ID: %w", err)
		}

		// Parse timestamp
		e.CreatedAt, err = parseTimestamp(createdAt)
		if err != nil {
			return es.Stream{}, fmt.Errorf("failed to parse created_at: %w", err)
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

// sqliteDateTimeFormats lists common SQLite datetime formats for parsing
var sqliteDateTimeFormats = []string{
	sqliteDateTimeFormat,
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999Z",
	"2006-01-02T15:04:05Z",
	time.RFC3339,
	time.RFC3339Nano,
}

// parseTimestamp parses SQLite datetime strings to time.Time
func parseTimestamp(s string) (time.Time, error) {
	for _, format := range sqliteDateTimeFormats {
		t, err := time.Parse(format, s)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}

// GetCheckpoint implements store.CheckpointStore.
func (s *Store) GetCheckpoint(ctx context.Context, tx es.DBTX, consumerName string) (int64, error) {
	query := fmt.Sprintf(`
		SELECT last_global_position 
		FROM %s 
		WHERE consumer_name = ?
	`, s.config.CheckpointsTable)

	var checkpoint int64
	err := tx.QueryRowContext(ctx, query, consumerName).Scan(&checkpoint)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return checkpoint, nil
}

// UpdateCheckpoint implements store.CheckpointStore.
func (s *Store) UpdateCheckpoint(ctx context.Context, tx es.DBTX, consumerName string, position int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (consumer_name, last_global_position, updated_at)
		VALUES (?, ?, datetime('now'))
		ON CONFLICT (consumer_name)
		DO UPDATE SET 
			last_global_position = excluded.last_global_position,
			updated_at = excluded.updated_at
	`, s.config.CheckpointsTable)

	_, err := tx.ExecContext(ctx, query, consumerName, position)
	return err
}
