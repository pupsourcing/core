package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/sqlite"
	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/migrations"
)

func TestProjection_OneOffMode(t *testing.T) {
	// Setup
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// Arrange: Append test events (each user is a separate aggregate)
	eventCount := 10
	for i := 0; i < eventCount; i++ {
		event := es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    fmt.Sprintf("user-%d", i+1),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"id":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}

		tx, txErr := db.BeginTx(ctx, nil)
		if txErr != nil {
			t.Fatalf("Failed to begin transaction for event %d: %v", i+1, txErr)
		}
		_, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			//nolint:errcheck // rollback error is not critical in test
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i+1, err)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			t.Fatalf("Failed to commit event %d: %v", i+1, commitErr)
		}
	}

	// Act: Process with one-off mode
	proj := &UserProjection{}
	config := consumer.DefaultProcessorConfig()
	config.RunMode = consumer.RunModeOneOff
	config.BatchSize = 5 // Process in multiple batches

	processor := sqlite.NewProcessor(db, store, &config)

	// This will process all events and exit cleanly
	err := processor.Run(ctx, proj)

	// Assert: Verify results
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode, got: %v", err)
	}

	if proj.GetCount() != eventCount {
		t.Errorf("Expected %d events processed, got %d", eventCount, proj.GetCount())
	}

	// Verify checkpoint was saved
	tx2, txErr := db.BeginTx(ctx, nil)
	if txErr != nil {
		t.Fatalf("Failed to begin transaction for checkpoint check: %v", txErr)
	}
	checkpoint, err := store.GetCheckpoint(ctx, tx2, proj.Name())
	if commitErr := tx2.Commit(); commitErr != nil {
		t.Fatalf("Failed to commit checkpoint check: %v", commitErr)
	}

	if err != nil {
		t.Fatalf("Failed to get checkpoint: %v", err)
	}

	if checkpoint <= 0 {
		t.Error("Expected checkpoint to be saved")
	}

	t.Logf("✓ Processed %d events in one-off mode", proj.GetCount())
	t.Logf("✓ Checkpoint saved at position %d", checkpoint)
}

func TestProjection_OneOffMode_EmptyStore(t *testing.T) {
	// Setup
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// No events appended - empty store

	// Process with one-off mode
	proj := &UserProjection{}
	config := consumer.DefaultProcessorConfig()
	config.RunMode = consumer.RunModeOneOff

	processor := sqlite.NewProcessor(db, store, &config)

	// Should exit immediately with no error
	err := processor.Run(ctx, proj)

	// Assert
	if err != nil {
		t.Fatalf("Expected nil error with empty store, got: %v", err)
	}

	if proj.GetCount() != 0 {
		t.Errorf("Expected 0 events processed, got %d", proj.GetCount())
	}

	t.Log("✓ Handled empty store correctly in one-off mode")
}

func TestProjection_ContinuousMode_StillWorks(t *testing.T) {
	// This test verifies backward compatibility
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// Append test events (each user is a separate aggregate)
	for i := 0; i < 3; i++ {
		event := es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    fmt.Sprintf("user-%d", i+1),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"id":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}

		tx, txErr := db.BeginTx(ctx, nil)
		if txErr != nil {
			t.Fatalf("Failed to begin transaction for event %d: %v", i+1, txErr)
		}
		_, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			//nolint:errcheck // rollback error is not critical in test
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i+1, err)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			t.Fatalf("Failed to commit event %d: %v", i+1, commitErr)
		}
	}

	// Process with continuous mode (default)
	proj := &UserProjection{}
	config := consumer.DefaultProcessorConfig()
	// RunMode defaults to RunModeContinuous

	processor := sqlite.NewProcessor(db, store, &config)

	// Run with timeout - should continue until context cancellation
	ctx2, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, proj)

	// Should get context.DeadlineExceeded (continuous mode doesn't exit on its own)
	if err == nil {
		t.Error("Expected timeout error in continuous mode")
	}

	if proj.GetCount() < 3 {
		t.Errorf("Expected at least 3 events processed, got %d", proj.GetCount())
	}

	t.Log("✓ Continuous mode still works (backward compatible)")
}

// setupTestDB creates a clean test database with tables
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbFile := fmt.Sprintf("/tmp/pupsourcing_test_%d.db", time.Now().UnixNano())
	t.Cleanup(func() {
		os.Remove(dbFile)
	})

	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	_, err = db.Exec("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")
	if err != nil {
		t.Fatalf("Failed to configure database: %v", err)
	}

	// Generate and execute migrations
	tmpDir := t.TempDir()
	config := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "test.sql",
		EventsTable:         "events",
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
	}

	if genErr := migrations.GenerateSQLite(&config); genErr != nil {
		t.Fatalf("Failed to generate migration: %v", genErr)
	}

	migrationSQL, readErr := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, config.OutputFilename))
	if readErr != nil {
		t.Fatalf("Failed to read migration: %v", readErr)
	}

	_, execErr := db.Exec(string(migrationSQL))
	if execErr != nil {
		t.Fatalf("Failed to execute migration: %v", execErr)
	}

	return db
}
