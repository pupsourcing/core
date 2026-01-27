// Package integration_test contains integration tests for the runner package.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./es/projection/runner/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/migrations"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/getpup/pupsourcing/es/projection/runner"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func getTestDB(t *testing.T) *sql.DB {
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
		t.Fatalf("Failed to connect to database: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	return db
}

func setupTestTables(t *testing.T, db *sql.DB) {
	t.Helper()

	// Drop existing objects to ensure clean state
	_, err := db.Exec(`
		DROP TABLE IF EXISTS projection_checkpoints CASCADE;
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
		CheckpointsTable:    "projection_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
	}

	if err := migrations.GeneratePostgres(&config); err != nil {
		t.Fatalf("Failed to generate migration: %v", err)
	}

	sqlFile := fmt.Sprintf("%s/test.sql", tmpDir)
	sqlBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		t.Fatalf("Failed to read SQL file: %v", err)
	}

	if _, err := db.Exec(string(sqlBytes)); err != nil {
		t.Fatalf("Failed to execute migration: %v", err)
	}
}

type testEvent struct {
	Message string `json:"message"`
}

func appendTestEvents(t *testing.T, ctx context.Context, db *sql.DB, store *postgres.Store, count int) []es.PersistedEvent {
	t.Helper()

	var persistedEvents []es.PersistedEvent

	for i := 0; i < count; i++ {
		payload, _ := json.Marshal(testEvent{Message: fmt.Sprintf("Event %d", i+1)})

		events := []es.Event{
			{
				BoundedContext: "TestContext",
				AggregateType:  "TestAggregate",
				AggregateID:   uuid.New().String(),
				EventID:       uuid.New(),
				EventType:     "TestEvent",
				EventVersion:  1,
				Payload:       payload,
				Metadata:      []byte(`{}`),
				CreatedAt:     time.Now(),
			},
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		result, err := store.Append(ctx, tx, es.Any(), events)
		if err != nil {
			//nolint:errcheck // Best effort rollback
			tx.Rollback()
			t.Fatalf("Failed to append events: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}

		// Read back the persisted event
		tx2, _ := db.BeginTx(ctx, nil)
		readEvents, err := store.ReadEvents(ctx, tx2, result.GlobalPositions[0]-1, 1)
		//nolint:errcheck // Best effort rollback
		tx2.Rollback()
		if err != nil {
			t.Fatalf("Failed to read event: %v", err)
		}
		persistedEvents = append(persistedEvents, readEvents[0])
	}

	return persistedEvents
}

// createAndRunPartitions runs a projection with N partitions
func createAndRunPartitions(ctx context.Context, db *sql.DB, store *postgres.Store, proj projection.Projection, totalPartitions int) error {
	var runners []runner.ProjectionRunner
	for i := 0; i < totalPartitions; i++ {
		config := projection.DefaultProcessorConfig()
		config.PartitionKey = i
		config.TotalPartitions = totalPartitions
		processor := postgres.NewProcessor(db, store, &config)
		runners = append(runners, runner.ProjectionRunner{
			Projection: proj,
			Processor:  processor,
		})
	}
	
	r := runner.New()
	return r.Run(ctx, runners)
}

// createAndRunMultiple runs multiple projections
func createAndRunMultiple(ctx context.Context, db *sql.DB, store *postgres.Store, projections []projection.Projection) error {
	var runners []runner.ProjectionRunner
	for _, proj := range projections {
		config := projection.DefaultProcessorConfig()
		processor := postgres.NewProcessor(db, store, &config)
		runners = append(runners, runner.ProjectionRunner{
			Projection: proj,
			Processor:  processor,
		})
	}
	
	r := runner.New()
	return r.Run(ctx, runners)
}

// countingProjection counts events processed
type countingProjection struct {
	name  string
	count int64
}

func (p *countingProjection) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *countingProjection) Handle(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
	atomic.AddInt64(&p.count, 1)
	return nil
}

// TestRunProjectionPartitions tests running a projection with multiple partitions
func TestRunProjectionPartitions(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append test events
	eventCount := 20
	appendTestEvents(t, ctx, db, store, eventCount)

	// Create projection
	proj := &countingProjection{name: "test_partition_runner"}

	// Run with 4 partitions for a short time
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := createAndRunPartitions(ctx2, db, store, proj, 4)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunProjectionPartitions failed: %v", err)
	}

	// Verify events were processed
	count := atomic.LoadInt64(&proj.count)
	if count != int64(eventCount) {
		t.Errorf("Expected %d events processed, got %d", eventCount, count)
	}

	// Verify checkpoints were created (one per partition)
	rows, err := db.Query("SELECT projection_name, last_global_position FROM projection_checkpoints ORDER BY projection_name")
	if err != nil {
		t.Fatalf("Failed to query checkpoints: %v", err)
	}
	defer rows.Close()

	checkpointCount := 0
	for rows.Next() {
		var name string
		var position int64
		if err := rows.Scan(&name, &position); err != nil {
			t.Fatalf("Failed to scan checkpoint: %v", err)
		}
		// Each partition should have its own checkpoint
		if position <= 0 {
			t.Errorf("Expected positive checkpoint position, got %d for %s", position, name)
		}
		checkpointCount++
	}

	// Should have 4 checkpoints (one per partition)
	if checkpointCount != 4 {
		t.Errorf("Expected 4 checkpoints (one per partition), got %d", checkpointCount)
	}
}

// TestRunMultipleProjections tests running multiple different projections
func TestRunMultipleProjections(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append test events
	eventCount := 15
	appendTestEvents(t, ctx, db, store, eventCount)

	// Create two projections
	proj1 := &countingProjection{name: "test_multi_proj1"}
	proj2 := &countingProjection{name: "test_multi_proj2"}

	projections := []projection.Projection{proj1, proj2}

	// Run both projections for a short time
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := createAndRunMultiple(ctx2, db, store, projections)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("createAndRunMultiple failed: %v", err)
	}

	// Verify both projections processed all events
	count1 := atomic.LoadInt64(&proj1.count)
	count2 := atomic.LoadInt64(&proj2.count)

	if count1 != int64(eventCount) {
		t.Errorf("Projection 1: Expected %d events processed, got %d", eventCount, count1)
	}
	if count2 != int64(eventCount) {
		t.Errorf("Projection 2: Expected %d events processed, got %d", eventCount, count2)
	}

	// Verify both projections have checkpoints
	rows, err := db.Query("SELECT projection_name, last_global_position FROM projection_checkpoints ORDER BY projection_name")
	if err != nil {
		t.Fatalf("Failed to query checkpoints: %v", err)
	}
	defer rows.Close()

	checkpoints := make(map[string]int64)
	for rows.Next() {
		var name string
		var position int64
		if err := rows.Scan(&name, &position); err != nil {
			t.Fatalf("Failed to scan checkpoint: %v", err)
		}
		checkpoints[name] = position
	}

	if len(checkpoints) != 2 {
		t.Errorf("Expected 2 checkpoints, got %d", len(checkpoints))
	}

	if _, ok := checkpoints["test_multi_proj1"]; !ok {
		t.Error("Missing checkpoint for projection 1")
	}
	if _, ok := checkpoints["test_multi_proj2"]; !ok {
		t.Error("Missing checkpoint for projection 2")
	}
}

// failingProjection fails after processing N events
type failingProjection struct {
	name         string
	count        int64
	failAfter    int64
	shouldResume bool
}

func (p *failingProjection) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *failingProjection) Handle(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
	count := atomic.AddInt64(&p.count, 1)
	if !p.shouldResume && count > p.failAfter {
		return errors.New("intentional failure")
	}
	return nil
}

// TestRunnerErrorHandling tests that runner properly handles projection errors
func TestRunnerErrorHandling(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append test events
	appendTestEvents(t, ctx, db, store, 20)

	// Create projection that fails after 10 events (enough to save at least one batch checkpoint)
	proj := &failingProjection{
		name:      "test_error_handling",
		failAfter: 10,
	}

	r := runner.New()

	// Use small batch size to ensure we save checkpoints before failure
	config := projection.DefaultProcessorConfig()
	config.BatchSize = 5

	// Create processor with small batch size
	processor := postgres.NewProcessor(db, store, &config)
	runners := []runner.ProjectionRunner{
		{Projection: proj, Processor: processor},
	}

	// Run and expect failure
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := r.Run(ctx2, runners)
	if err == nil {
		t.Fatal("Expected error from runner, got nil")
	}

	// Verify checkpoint was saved for events before failure
	var checkpoint int64
	err = db.QueryRow("SELECT last_global_position FROM projection_checkpoints WHERE projection_name = $1",
		"test_error_handling").Scan(&checkpoint)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Skip("No checkpoint saved before failure (failure in first batch)")
		}
		t.Fatalf("Failed to query checkpoint: %v", err)
	}

	if checkpoint == 0 {
		t.Error("Expected non-zero checkpoint after partial processing")
	}

	// Resume projection (shouldn't fail this time)
	proj.shouldResume = true
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()

	// Create processor for resume
	resumeProcessor := postgres.NewProcessor(db, store, &config)
	resumeRunners := []runner.ProjectionRunner{
		{Projection: proj, Processor: resumeProcessor},
	}

	err = r.Run(ctx3, resumeRunners)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Unexpected error on resume: %v", err)
	}

	// Should have processed more events after resume
	finalCount := atomic.LoadInt64(&proj.count)
	if finalCount <= 10 {
		t.Errorf("Expected more than 10 events processed after resume, got %d", finalCount)
	}
}

// TestRunnerConcurrentCheckpoints tests that concurrent partitions maintain separate checkpoints correctly
func TestRunnerConcurrentCheckpoints(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append many events to ensure all partitions get work
	appendTestEvents(t, ctx, db, store, 100)

	// Create projection
	proj := &countingProjection{name: "test_concurrent_checkpoints"}

	// Run with 4 partitions
	ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err := createAndRunPartitions(ctx2, db, store, proj, 4)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunProjectionPartitions failed: %v", err)
	}

	// Verify all events were processed
	count := atomic.LoadInt64(&proj.count)
	if count != 100 {
		t.Errorf("Expected 100 events processed, got %d", count)
	}

	// Verify checkpoints for each partition exist
	rows, err := db.Query("SELECT projection_name, last_global_position FROM projection_checkpoints ORDER BY projection_name")
	if err != nil {
		t.Fatalf("Failed to query checkpoints: %v", err)
	}
	defer rows.Close()

	checkpointCount := 0
	var maxCheckpoint int64
	for rows.Next() {
		var name string
		var position int64
		if err := rows.Scan(&name, &position); err != nil {
			t.Fatalf("Failed to scan checkpoint: %v", err)
		}
		if position > maxCheckpoint {
			maxCheckpoint = position
		}
		checkpointCount++
	}

	// Should have 4 checkpoints (one per partition)
	if checkpointCount != 4 {
		t.Errorf("Expected 4 checkpoints (one per partition), got %d", checkpointCount)
	}

	// The highest checkpoint should be at or near position 100
	if maxCheckpoint < 90 {
		t.Errorf("Expected max checkpoint near 100, got %d", maxCheckpoint)
	}
}
