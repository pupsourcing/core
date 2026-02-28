// Package integration_test contains integration tests for the runner package.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./es/consumer/runner/integration_test/...
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

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/consumer/runner"
	"github.com/pupsourcing/core/es/migrations"
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
		DROP TABLE IF EXISTS consumer_checkpoints CASCADE;
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
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
		SegmentsTable:       "consumer_segments",
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
				AggregateID:    uuid.New().String(),
				EventID:        uuid.New(),
				EventType:      "TestEvent",
				EventVersion:   1,
				Payload:        payload,
				Metadata:       []byte(`{}`),
				CreatedAt:      time.Now(),
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

// createAndRunPartitions runs a consumer with N partitions
func createAndRunPartitions(ctx context.Context, db *sql.DB, store *postgres.Store, proj consumer.Consumer, totalPartitions int) error {
	var runners []runner.ConsumerRunner
	for i := 0; i < totalPartitions; i++ {
		config := consumer.DefaultProcessorConfig()
		config.PartitionKey = i
		config.TotalPartitions = totalPartitions
		processor := postgres.NewProcessor(db, store, &config)
		runners = append(runners, runner.ConsumerRunner{
			Consumer:  proj,
			Processor: processor,
		})
	}

	r := runner.New()
	return r.Run(ctx, runners)
}

// createAndRunMultiple runs multiple consumers
func createAndRunMultiple(ctx context.Context, db *sql.DB, store *postgres.Store, consumers []consumer.Consumer) error {
	var runners []runner.ConsumerRunner
	for _, proj := range consumers {
		config := consumer.DefaultProcessorConfig()
		processor := postgres.NewProcessor(db, store, &config)
		runners = append(runners, runner.ConsumerRunner{
			Consumer:  proj,
			Processor: processor,
		})
	}

	r := runner.New()
	return r.Run(ctx, runners)
}

// countingConsumer counts events processed
type countingConsumer struct {
	name  string
	count int64
}

func (p *countingConsumer) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *countingConsumer) Handle(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
	atomic.AddInt64(&p.count, 1)
	return nil
}

// TestRunConsumerPartitions tests running a consumer with multiple partitions
func TestRunConsumerPartitions(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append test events
	eventCount := 20
	appendTestEvents(t, ctx, db, store, eventCount)

	// Create consumer
	proj := &countingConsumer{name: "test_partition_runner"}

	// Run with 4 partitions for a short time
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := createAndRunPartitions(ctx2, db, store, proj, 4)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunConsumerPartitions failed: %v", err)
	}

	// Verify events were processed
	count := atomic.LoadInt64(&proj.count)
	if count != int64(eventCount) {
		t.Errorf("Expected %d events processed, got %d", eventCount, count)
	}

	// Verify checkpoints were created (one per partition)
	rows, err := db.Query("SELECT consumer_name, last_global_position FROM consumer_checkpoints ORDER BY consumer_name")
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

// TestRunMultipleConsumers tests running multiple different consumers
func TestRunMultipleConsumers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append test events
	eventCount := 15
	appendTestEvents(t, ctx, db, store, eventCount)

	// Create two consumers
	proj1 := &countingConsumer{name: "test_multi_proj1"}
	proj2 := &countingConsumer{name: "test_multi_proj2"}

	consumers := []consumer.Consumer{proj1, proj2}

	// Run both consumers for a short time
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := createAndRunMultiple(ctx2, db, store, consumers)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("createAndRunMultiple failed: %v", err)
	}

	// Verify both consumers processed all events
	count1 := atomic.LoadInt64(&proj1.count)
	count2 := atomic.LoadInt64(&proj2.count)

	if count1 != int64(eventCount) {
		t.Errorf("Consumer 1: Expected %d events processed, got %d", eventCount, count1)
	}
	if count2 != int64(eventCount) {
		t.Errorf("Consumer 2: Expected %d events processed, got %d", eventCount, count2)
	}

	// Verify both consumers have checkpoints
	rows, err := db.Query("SELECT consumer_name, last_global_position FROM consumer_checkpoints ORDER BY consumer_name")
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
		t.Error("Missing checkpoint for consumer 1")
	}
	if _, ok := checkpoints["test_multi_proj2"]; !ok {
		t.Error("Missing checkpoint for consumer 2")
	}
}

// failingConsumer fails after processing N events
type failingConsumer struct {
	name         string
	count        int64
	failAfter    int64
	shouldResume bool
}

func (p *failingConsumer) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *failingConsumer) Handle(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
	count := atomic.AddInt64(&p.count, 1)
	if !p.shouldResume && count > p.failAfter {
		return errors.New("intentional failure")
	}
	return nil
}

// TestRunnerErrorHandling tests that runner properly handles consumer errors
func TestRunnerErrorHandling(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append test events
	appendTestEvents(t, ctx, db, store, 20)

	// Create consumer that fails after 10 events (enough to save at least one batch checkpoint)
	proj := &failingConsumer{
		name:      "test_error_handling",
		failAfter: 10,
	}

	r := runner.New()

	// Use small batch size to ensure we save checkpoints before failure
	config := consumer.DefaultProcessorConfig()
	config.BatchSize = 5

	// Create processor with small batch size
	processor := postgres.NewProcessor(db, store, &config)
	runners := []runner.ConsumerRunner{
		{Consumer: proj, Processor: processor},
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
	err = db.QueryRow("SELECT last_global_position FROM consumer_checkpoints WHERE consumer_name = $1",
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

	// Resume consumer (shouldn't fail this time)
	proj.shouldResume = true
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()

	// Create processor for resume
	resumeProcessor := postgres.NewProcessor(db, store, &config)
	resumeRunners := []runner.ConsumerRunner{
		{Consumer: proj, Processor: resumeProcessor},
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

	// Create consumer
	proj := &countingConsumer{name: "test_concurrent_checkpoints"}

	// Run with 4 partitions
	ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err := createAndRunPartitions(ctx2, db, store, proj, 4)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunConsumerPartitions failed: %v", err)
	}

	// Verify all events were processed
	count := atomic.LoadInt64(&proj.count)
	if count != 100 {
		t.Errorf("Expected 100 events processed, got %d", count)
	}

	// Verify checkpoints for each partition exist
	rows, err := db.Query("SELECT consumer_name, last_global_position FROM consumer_checkpoints ORDER BY consumer_name")
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
