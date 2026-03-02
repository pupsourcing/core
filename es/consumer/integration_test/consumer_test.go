// Package integration_test contains integration tests for consumers.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./es/consumer/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
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

	if err := migrations.GeneratePostgres(&config); err != nil {
		t.Fatalf("Failed to generate migration: %v", err)
	}

	migrationSQL, err := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("Failed to read migration: %v", err)
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute migration: %v", err)
	}
}

// testConsumer is a simple consumer for testing
type testConsumer struct {
	name   string
	events []es.PersistedEvent
	mu     sync.Mutex
	errors []error
}

func newTestConsumer(name string) *testConsumer {
	return &testConsumer{
		name:   name,
		events: make([]es.PersistedEvent, 0),
		errors: make([]error, 0),
	}
}

func (p *testConsumer) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *testConsumer) Handle(ctx context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *testConsumer) EventCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func (p *testConsumer) GetEvents() []es.PersistedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]es.PersistedEvent{}, p.events...)
}

func TestConsumer_BasicProcessing(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append some test events
	aggregateID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "EventCreated",
			EventVersion:   1,
			Payload:        []byte(`{"id":1}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "EventUpdated",
			EventVersion:   1,
			Payload:        []byte(`{"id":2}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// Run consumer
	proj := newTestConsumer("test_consumer")
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run for a short time - increased timeout for CI environment
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err = processor.Run(ctx2, proj)
	// Accept no error, consumer stopped, deadline exceeded (possibly wrapped without %w)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, postgres.ErrConsumerStopped) || strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			// acceptable
		} else {
			t.Fatalf("Unexpected error from processor: %v, %T", err, err)
		}
	}

	// Verify events were processed - allow some time for processing
	time.Sleep(100 * time.Millisecond)
	if proj.EventCount() != 2 {
		t.Errorf("Expected 2 events processed, got %d", proj.EventCount())
	}
}

func TestConsumer_Checkpoint(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events
	aggregateID := uuid.New().String()
	allEvents := make([]es.Event, 5)
	for i := 0; i < 5; i++ {
		allEvents[i] = es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      fmt.Sprintf("Event%d", i+1),
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"num":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), allEvents)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// First run processes some events
	proj1 := newTestConsumer("checkpoint_test")
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	config.BatchSize = 2
	processor1 := postgres.NewSegmentProcessor(db, store, config)

	ctx1, cancel1 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel1()

	processor1.Run(ctx1, proj1)

	firstRunCount := proj1.EventCount()
	if firstRunCount == 0 {
		t.Fatal("First run processed no events")
	}

	// Second run should resume from checkpoint
	proj2 := newTestConsumer("checkpoint_test")
	processor2 := postgres.NewSegmentProcessor(db, store, config)

	ctx2, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()

	processor2.Run(ctx2, proj2)

	// Total events should equal sum of both runs
	totalProcessed := firstRunCount + proj2.EventCount()
	if totalProcessed < 5 {
		t.Errorf("Expected at least 5 total events processed, got %d", totalProcessed)
	}

	// Second run should not reprocess first run's events
	if proj2.EventCount() > 0 {
		firstEvent := proj2.GetEvents()[0]
		lastEventFirstRun := proj1.GetEvents()[len(proj1.GetEvents())-1]
		if firstEvent.GlobalPosition <= lastEventFirstRun.GlobalPosition {
			t.Error("Second run reprocessed events from first run")
		}
	}
}

func TestConsumer_ErrorHandling(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events
	aggregateID := uuid.New().String()
	event := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "ErrorEvent",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	tx, _ := db.BeginTx(ctx, nil)
	store.Append(ctx, tx, es.Any(), []es.Event{event})
	tx.Commit()

	// Create consumer that returns error
	errorProj := &errorConsumer{name: "error_test"}
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	processor := postgres.NewSegmentProcessor(db, store, config)

	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, errorProj)
	if err == nil {
		t.Error("Expected error from consumer processor")
	}
	if err != nil && err != postgres.ErrConsumerStopped && err != context.DeadlineExceeded {
		// Should be wrapped ErrConsumerStopped
		t.Logf("Got error: %v", err)
	}
}

type errorConsumer struct {
	name string
}

func (p *errorConsumer) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *errorConsumer) Handle(ctx context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	return fmt.Errorf("intentional error")
}

func TestProcessor_RunModeOneOff(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append known number of events
	eventCount := 25
	aggregateID := uuid.New().String()
	events := make([]es.Event, eventCount)
	for i := 0; i < eventCount; i++ {
		events[i] = es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      fmt.Sprintf("Event%d", i+1),
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"num":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// Create test consumer
	proj := newTestConsumer("test_oneoff")

	// Configure processor in one-off mode
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	config.RunMode = consumer.RunModeOneOff
	config.BatchSize = 10

	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run should exit cleanly after processing all events
	err = processor.Run(ctx, proj)
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode, got: %v", err)
	}

	// Verify all events were processed
	if proj.EventCount() != eventCount {
		t.Errorf("Expected %d events processed, got %d", eventCount, proj.EventCount())
	}

	// Verify checkpoint was saved at the end (segment checkpoint, not consumer checkpoint)
	tx2, _ := db.BeginTx(ctx, nil)
	checkpoint, err := store.GetSegmentCheckpoint(ctx, tx2, proj.Name(), 0)
	tx2.Commit()
	if err != nil {
		t.Fatalf("Failed to get checkpoint: %v", err)
	}
	if checkpoint <= 0 {
		t.Error("Expected checkpoint to be saved")
	}
}

func TestProcessor_RunModeOneOff_EmptyStore(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// No events appended - empty store

	// Create test consumer
	proj := newTestConsumer("test_oneoff_empty")

	// Configure processor in one-off mode
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	config.RunMode = consumer.RunModeOneOff

	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run should exit cleanly immediately with no events
	err := processor.Run(ctx, proj)
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode with empty store, got: %v", err)
	}

	// Verify no events were processed
	if proj.EventCount() != 0 {
		t.Errorf("Expected 0 events processed, got %d", proj.EventCount())
	}
}

func TestProcessor_RunModeOneOff_PartialBatch(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events less than one batch
	eventCount := 5
	aggregateID := uuid.New().String()
	events := make([]es.Event, eventCount)
	for i := 0; i < eventCount; i++ {
		events[i] = es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      fmt.Sprintf("Event%d", i+1),
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"num":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// Create test consumer
	proj := newTestConsumer("test_oneoff_partial")

	// Configure processor in one-off mode with batch size larger than events
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	config.RunMode = consumer.RunModeOneOff
	config.BatchSize = 100

	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run should exit cleanly after processing partial batch
	err = processor.Run(ctx, proj)
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode, got: %v", err)
	}

	// Verify all events were processed
	if proj.EventCount() != eventCount {
		t.Errorf("Expected %d events processed, got %d", eventCount, proj.EventCount())
	}
}

func TestProcessor_RunModeContinuous_StillWorks(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append test events
	aggregateID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event1",
			EventVersion:   1,
			Payload:        []byte(`{"id":1}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// Create test consumer
	proj := newTestConsumer("test_continuous")

	// Configure processor in continuous mode (default)
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	// RunMode defaults to RunModeContinuous

	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run with timeout - should continue until context cancellation
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err = processor.Run(ctx2, proj)
	// Should get context.DeadlineExceeded or wrapped version
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Errorf("Expected context.DeadlineExceeded, got: %v", err)
		}
	}

	// Verify events were processed
	if proj.EventCount() < 1 {
		t.Errorf("Expected at least 1 event processed, got %d", proj.EventCount())
	}
}
