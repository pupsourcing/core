//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/migrations"
	"github.com/pupsourcing/core/es/worker"
)

func TestConsumer_OneOffMode(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

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

	// Act: Process with one-off mode using the Worker API
	proj := &UserProjection{}
	w := postgres.NewWorker(db, store,
		worker.WithTotalSegments(1),
		worker.WithRunMode(consumer.RunModeOneOff),
		worker.WithBatchSize(5),
	)

	err := w.Run(ctx, proj)

	// Assert
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode, got: %v", err)
	}

	if proj.GetCount() != eventCount {
		t.Errorf("Expected %d events processed, got %d", eventCount, proj.GetCount())
	}

	// Verify checkpoint was saved (in consumer_segments table)
	var checkpoint int64
	err = db.QueryRow(
		"SELECT checkpoint FROM consumer_segments WHERE consumer_name = $1 AND segment_id = 0",
		proj.Name(),
	).Scan(&checkpoint)
	if err != nil {
		t.Fatalf("Failed to get checkpoint: %v", err)
	}

	if checkpoint <= 0 {
		t.Error("Expected checkpoint to be saved")
	}

	t.Logf("✓ Processed %d events in one-off mode", proj.GetCount())
	t.Logf("✓ Checkpoint saved at position %d", checkpoint)
}

func TestConsumer_OneOffMode_EmptyStore(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()

	proj := &UserProjection{}
	store := postgres.NewStore(postgres.DefaultStoreConfig())
	w := postgres.NewWorker(db, store,
		worker.WithTotalSegments(1),
		worker.WithRunMode(consumer.RunModeOneOff),
	)

	err := w.Run(ctx, proj)

	if err != nil {
		t.Fatalf("Expected nil error with empty store, got: %v", err)
	}

	if proj.GetCount() != 0 {
		t.Errorf("Expected 0 events processed, got %d", proj.GetCount())
	}

	t.Log("✓ Handled empty store correctly in one-off mode")
}

func TestConsumer_ContinuousMode(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

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

	// Process with continuous mode (default) — runs until context cancellation
	proj := &UserProjection{}
	w := postgres.NewWorker(db, store, worker.WithTotalSegments(1))

	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := w.Run(ctx2, proj)

	// Should get context.DeadlineExceeded
	if err == nil {
		t.Error("Expected timeout error in continuous mode")
	}

	if proj.GetCount() < 3 {
		t.Errorf("Expected at least 3 events processed, got %d", proj.GetCount())
	}

	t.Log("✓ Continuous mode works correctly")
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	dbname := os.Getenv("POSTGRES_DB")
	if dbname == "" {
		dbname = "pupsourcing_test"
	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Drop and recreate tables
	_, err = db.Exec(`
DROP TABLE IF EXISTS consumer_workers CASCADE;
DROP TABLE IF EXISTS consumer_segments CASCADE;
DROP TABLE IF EXISTS aggregate_heads CASCADE;
DROP TABLE IF EXISTS events CASCADE;
`)
	if err != nil {
		t.Fatalf("Failed to drop tables: %v", err)
	}

	tmpDir := t.TempDir()
	config := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "test.sql",
		EventsTable:         "events",
		AggregateHeadsTable: "aggregate_heads",
		SegmentsTable:       "consumer_segments",
		WorkerRegistryTable: "consumer_workers",
	}

	if genErr := migrations.GeneratePostgres(&config); genErr != nil {
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
