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
		OutputFolder:     tmpDir,
		OutputFilename:   "test_migration.sql",
		EventsTable:      "events",
		CheckpointsTable: "consumer_checkpoints",
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
		"CREATE TABLE IF NOT EXISTS consumer_checkpoints",
		"consumer_name TEXT PRIMARY KEY",
		"last_global_position BIGINT NOT NULL",
		"updated_at TIMESTAMPTZ NOT NULL",
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
		"idx_consumer_checkpoints_updated",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}

	// Verify essential tables are present
	if !strings.Contains(sql, "consumer_checkpoints") {
		t.Error("Missing consumer_checkpoints table")
	}
}

func TestGeneratePostgres_CustomTableNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "custom_migration.sql",
		EventsTable:      "custom_events",
		CheckpointsTable: "custom_checkpoints",
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
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_checkpoints") {
		t.Error("Custom checkpoints table name not used")
	}
}

func TestGenerateMySQL(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "test_migration.sql",
		EventsTable:      "events",
		CheckpointsTable: "consumer_checkpoints",
	}

	err := GenerateMySQL(&config)
	if err != nil {
		t.Fatalf("GenerateMySQL failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify essential components are present for MySQL
	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS events",
		"global_position BIGINT AUTO_INCREMENT PRIMARY KEY",
		"aggregate_type VARCHAR(255) NOT NULL",
		"aggregate_id VARCHAR(255) NOT NULL",
		"aggregate_version BIGINT NOT NULL",
		"event_id BINARY(16) NOT NULL UNIQUE",
		"event_type VARCHAR(255) NOT NULL",
		"event_version INT NOT NULL DEFAULT 1",
		"payload BLOB NOT NULL",
		"trace_id VARCHAR(255)",
		"correlation_id VARCHAR(255)",
		"causation_id VARCHAR(255)",
		"metadata JSON",
		"created_at TIMESTAMP(6) NOT NULL",
		"CREATE TABLE IF NOT EXISTS consumer_checkpoints",
		"consumer_name VARCHAR(255) PRIMARY KEY",
		"last_global_position BIGINT NOT NULL",
		"updated_at TIMESTAMP(6) NOT NULL",
		"ENGINE=InnoDB",
		"CHARSET=utf8mb4",
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
		"idx_consumer_checkpoints_updated",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}
}

func TestGenerateSQLite(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "test_migration.sql",
		EventsTable:      "events",
		CheckpointsTable: "consumer_checkpoints",
	}

	err := GenerateSQLite(&config)
	if err != nil {
		t.Fatalf("GenerateSQLite failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)

	// Verify essential components are present for SQLite
	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS events",
		"global_position INTEGER PRIMARY KEY AUTOINCREMENT",
		"aggregate_type TEXT NOT NULL",
		"aggregate_id TEXT NOT NULL",
		"aggregate_version INTEGER NOT NULL",
		"event_id TEXT NOT NULL UNIQUE",
		"event_type TEXT NOT NULL",
		"event_version INTEGER NOT NULL DEFAULT 1",
		"payload BLOB NOT NULL",
		"trace_id TEXT",
		"correlation_id TEXT",
		"causation_id TEXT",
		"metadata TEXT",
		"created_at TEXT NOT NULL",
		"CREATE TABLE IF NOT EXISTS consumer_checkpoints",
		"consumer_name TEXT PRIMARY KEY",
		"last_global_position INTEGER NOT NULL",
		"updated_at TEXT NOT NULL",
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
		"idx_consumer_checkpoints_updated",
	}

	for _, idx := range requiredIndexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("Generated SQL missing index: %s", idx)
		}
	}
}

func TestGenerateMySQL_CustomTableNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "custom_migration.sql",
		EventsTable:      "custom_events",
		CheckpointsTable: "custom_checkpoints",
	}

	err := GenerateMySQL(&config)
	if err != nil {
		t.Fatalf("GenerateMySQL failed: %v", err)
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
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_checkpoints") {
		t.Error("Custom checkpoints table name not used")
	}
}

func TestGenerateSQLite_CustomTableNames(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:     tmpDir,
		OutputFilename:   "custom_migration.sql",
		EventsTable:      "custom_events",
		CheckpointsTable: "custom_checkpoints",
	}

	err := GenerateSQLite(&config)
	if err != nil {
		t.Fatalf("GenerateSQLite failed: %v", err)
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
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_checkpoints") {
		t.Error("Custom checkpoints table name not used")
	}
}
