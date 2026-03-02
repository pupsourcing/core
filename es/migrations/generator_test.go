package migrations

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratePostgres(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "test_migration.sql",
		EventsTable:         "events",
		SegmentsTable:       "consumer_segments",
		AggregateHeadsTable: "aggregate_heads",
		WorkerRegistryTable: "consumer_workers",
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify essential components are present
	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS events",
		"global_position BIGSERIAL PRIMARY KEY",
		"aggregate_type TEXT NOT NULL",
		"aggregate_id TEXT NOT NULL",
		"aggregate_version BIGINT NOT NULL",
		"event_id UUID NOT NULL UNIQUE",
		"event_type TEXT NOT NULL",
		"event_version INT NOT NULL DEFAULT 1",
		"payload BYTEA NOT NULL",
		"trace_id TEXT",
		"correlation_id TEXT",
		"causation_id TEXT",
		"metadata JSONB",
		"created_at TIMESTAMPTZ NOT NULL",
		"CREATE TABLE IF NOT EXISTS consumer_segments",
		"CREATE TABLE IF NOT EXISTS aggregate_heads",
		"CREATE TABLE IF NOT EXISTS consumer_workers",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing required string: %s", required)
		}
	}

	// Verify indexes are created
	requiredIndexes := []string{
		"idx_events_aggregate",
		"idx_events_event_type",
		"idx_events_correlation",
		"idx_consumer_segments_owner",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}

	// Verify consumer_checkpoints is NOT present (removed)
	if strings.Contains(sql, "consumer_checkpoints") {
		t.Error("Generated SQL should not contain consumer_checkpoints (removed)")
	}
}

func TestGeneratePostgres_CustomTableNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "custom_migration.sql",
		EventsTable:         "custom_events",
		SegmentsTable:       "custom_segments",
		AggregateHeadsTable: "custom_aggregate_heads",
		WorkerRegistryTable: "custom_workers",
	}

	err := GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify custom table names are used
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_events") {
		t.Error("Custom events table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_segments") {
		t.Error("Custom segments table name not used")
	}
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_workers") {
		t.Error("Custom worker registry table name not used")
	}
}
