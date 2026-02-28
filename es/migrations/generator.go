// Package migrations provides SQL migration generation for event sourcing infrastructure.
package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config configures migration generation.
type Config struct {
	// OutputFolder is the directory where the migration file will be written
	OutputFolder string

	// OutputFilename is the name of the migration file
	OutputFilename string

	// EventsTable is the name of the events table
	EventsTable string

	// CheckpointsTable is the name of the consumer checkpoints table
	CheckpointsTable string

	// AggregateHeadsTable is the name of the aggregate version tracking table
	AggregateHeadsTable string

	// SegmentsTable is the name of the consumer segments table
	SegmentsTable string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	timestamp := time.Now().Format("20060102150405")
	return Config{
		OutputFolder:        "migrations",
		OutputFilename:      fmt.Sprintf("%s_init_event_sourcing.sql", timestamp),
		EventsTable:         "events",
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
		SegmentsTable:       "consumer_segments",
	}
}

// GeneratePostgres generates a PostgreSQL migration file.
func GeneratePostgres(config *Config) error {
	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generatePostgresSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generatePostgresSQL(config *Config) string {
	return fmt.Sprintf(`-- Event Sourcing Infrastructure Migration
-- Generated: %s

-- Events table stores all domain events in append-only fashion
CREATE TABLE IF NOT EXISTS %s (
    global_position BIGSERIAL PRIMARY KEY,
    bounded_context TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    event_version INT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL,
    trace_id TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Ensure version uniqueness per aggregate within bounded context
    UNIQUE (bounded_context, aggregate_type, aggregate_id, aggregate_version)
);

-- Index for aggregate stream reads
CREATE INDEX IF NOT EXISTS idx_%s_aggregate 
    ON %s (bounded_context, aggregate_type, aggregate_id, aggregate_version);

-- Index for event type queries
CREATE INDEX IF NOT EXISTS idx_%s_event_type 
    ON %s (event_type, global_position);

-- Index for correlation tracking
CREATE INDEX IF NOT EXISTS idx_%s_correlation 
    ON %s (correlation_id) WHERE correlation_id IS NOT NULL;

-- Aggregate heads table tracks the current version of each aggregate
-- Provides O(1) version lookup for event append operations
-- Primary key (bounded_context, aggregate_type, aggregate_id) ensures one row per aggregate
CREATE TABLE IF NOT EXISTS %s (
    bounded_context TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (bounded_context, aggregate_type, aggregate_id)
);

-- Index for observability
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at);

-- Consumer checkpoints table tracks progress of each consumer
CREATE TABLE IF NOT EXISTS %s (
    consumer_name TEXT PRIMARY KEY,
    last_global_position BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for checkpoints
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at);

-- Consumer segments table for auto-scaling event processing
-- Each segment tracks ownership and processing position for distributed consumers
CREATE TABLE IF NOT EXISTS %s (
    consumer_name TEXT NOT NULL,
    segment_id INT NOT NULL,
    total_segments INT NOT NULL,
    owner_id TEXT,
    checkpoint BIGINT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, segment_id)
);

-- Index for stale segment cleanup and fair-share queries
CREATE INDEX IF NOT EXISTS idx_%s_owner
    ON %s (consumer_name, owner_id);
`,
		time.Now().Format(time.RFC3339),
		config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.AggregateHeadsTable,
		config.AggregateHeadsTable, config.AggregateHeadsTable,
		config.CheckpointsTable,
		config.CheckpointsTable, config.CheckpointsTable,
		config.SegmentsTable,
		config.SegmentsTable, config.SegmentsTable,
	)
}

// GenerateSQLite generates a SQLite migration file.
func GenerateSQLite(config *Config) error {
	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generateSQLiteSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generateSQLiteSQL(config *Config) string {
	return fmt.Sprintf(`-- Event Sourcing Infrastructure Migration for SQLite
-- Generated: %s

-- Events table stores all domain events in append-only fashion
CREATE TABLE IF NOT EXISTS %s (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    bounded_context TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    payload BLOB NOT NULL,
    trace_id TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    metadata TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    
    -- Ensure version uniqueness per aggregate within bounded context
    UNIQUE (bounded_context, aggregate_type, aggregate_id, aggregate_version)
);

-- Index for aggregate stream reads
CREATE INDEX IF NOT EXISTS idx_%s_aggregate 
    ON %s (bounded_context, aggregate_type, aggregate_id, aggregate_version);

-- Index for event type queries
CREATE INDEX IF NOT EXISTS idx_%s_event_type 
    ON %s (event_type, global_position);

-- Index for correlation tracking
CREATE INDEX IF NOT EXISTS idx_%s_correlation 
    ON %s (correlation_id) WHERE correlation_id IS NOT NULL;

-- Aggregate heads table tracks the current version of each aggregate
-- Provides O(1) version lookup for event append operations
-- Primary key (bounded_context, aggregate_type, aggregate_id) ensures one row per aggregate
CREATE TABLE IF NOT EXISTS %s (
    bounded_context TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    
    PRIMARY KEY (bounded_context, aggregate_type, aggregate_id)
);

-- Index for observability
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at);

-- Consumer checkpoints table tracks progress of each consumer
CREATE TABLE IF NOT EXISTS %s (
    consumer_name TEXT PRIMARY KEY,
    last_global_position INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Index for checkpoints
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at);

-- Consumer segments table for auto-scaling event processing
-- Each segment tracks ownership and processing position for distributed consumers
CREATE TABLE IF NOT EXISTS %s (
    consumer_name TEXT NOT NULL,
    segment_id INTEGER NOT NULL,
    total_segments INTEGER NOT NULL,
    owner_id TEXT,
    checkpoint INTEGER NOT NULL DEFAULT 0,
    last_heartbeat TEXT,
    PRIMARY KEY (consumer_name, segment_id)
);

-- Index for stale segment cleanup and fair-share queries
CREATE INDEX IF NOT EXISTS idx_%s_owner
    ON %s (consumer_name, owner_id);
`,
		time.Now().Format(time.RFC3339),
		config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.AggregateHeadsTable,
		config.AggregateHeadsTable, config.AggregateHeadsTable,
		config.CheckpointsTable,
		config.CheckpointsTable, config.CheckpointsTable,
		config.SegmentsTable,
		config.SegmentsTable, config.SegmentsTable,
	)
}

// GenerateMySQL generates a MySQL/MariaDB migration file.
func GenerateMySQL(config *Config) error {
	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generateMySQLSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generateMySQLSQL(config *Config) string {
	return fmt.Sprintf(`-- Event Sourcing Infrastructure Migration for MySQL/MariaDB
-- Generated: %s

-- Events table stores all domain events in append-only fashion
CREATE TABLE IF NOT EXISTS %s (
    global_position BIGINT AUTO_INCREMENT PRIMARY KEY,
    bounded_context VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    event_id BINARY(16) NOT NULL UNIQUE,
    event_type VARCHAR(255) NOT NULL,
    event_version INT NOT NULL DEFAULT 1,
    payload BLOB NOT NULL,
    trace_id VARCHAR(255),
    correlation_id VARCHAR(255),
    causation_id VARCHAR(255),
    metadata JSON,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    
    -- Ensure version uniqueness per aggregate within bounded context
    UNIQUE KEY unique_aggregate_version (bounded_context, aggregate_type, aggregate_id, aggregate_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for aggregate stream reads
CREATE INDEX idx_%s_aggregate 
    ON %s (bounded_context, aggregate_type, aggregate_id, aggregate_version);

-- Index for event type queries
CREATE INDEX idx_%s_event_type 
    ON %s (event_type, global_position);

-- Index for correlation tracking
CREATE INDEX idx_%s_correlation 
    ON %s (correlation_id);

-- Aggregate heads table tracks the current version of each aggregate
-- Provides O(1) version lookup for event append operations
-- Primary key (bounded_context, aggregate_type, aggregate_id) ensures one row per aggregate
CREATE TABLE IF NOT EXISTS %s (
    bounded_context VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    PRIMARY KEY (bounded_context, aggregate_type, aggregate_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for observability
CREATE INDEX idx_%s_updated 
    ON %s (updated_at);

-- Consumer checkpoints table tracks progress of each consumer
CREATE TABLE IF NOT EXISTS %s (
    consumer_name VARCHAR(255) PRIMARY KEY,
    last_global_position BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for checkpoints
CREATE INDEX idx_%s_updated 
    ON %s (updated_at);

-- Consumer segments table for auto-scaling event processing
-- Each segment tracks ownership and processing position for distributed consumers
CREATE TABLE IF NOT EXISTS %s (
    consumer_name VARCHAR(255) NOT NULL,
    segment_id INT NOT NULL,
    total_segments INT NOT NULL,
    owner_id VARCHAR(255),
    checkpoint BIGINT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMP(6) NULL,
    PRIMARY KEY (consumer_name, segment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for stale segment cleanup and fair-share queries
CREATE INDEX idx_%s_owner
    ON %s (consumer_name, owner_id);
`,
		time.Now().Format(time.RFC3339),
		config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.EventsTable, config.EventsTable,
		config.AggregateHeadsTable,
		config.AggregateHeadsTable, config.AggregateHeadsTable,
		config.CheckpointsTable,
		config.CheckpointsTable, config.CheckpointsTable,
		config.SegmentsTable,
		config.SegmentsTable, config.SegmentsTable,
	)
}
