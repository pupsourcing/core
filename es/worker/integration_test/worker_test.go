// Package integration_test contains integration tests for the Worker package.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./es/worker/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/migrations"
	"github.com/pupsourcing/core/es/worker"
)

// getTestDB creates a connection to the test database.
func getTestDB(t *testing.T) *sql.DB {
	t.Helper()

	// Default to localhost, but allow override via env var for CI
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

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	return db
}

// setupTestTables drops and recreates all tables for a clean test environment.
func setupTestTables(t *testing.T, db *sql.DB) {
	t.Helper()

	// Drop existing objects to ensure clean state
	tables := []string{"consumer_workers", "consumer_segments", "aggregate_heads", "events"}
	for _, table := range tables {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", table))
		if err != nil {
			t.Fatalf("Failed to drop table %s: %v", table, err)
		}
	}

	// Generate and execute migration
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

// makeEvent is a helper to create test events.
func makeEvent(boundedContext, aggType, aggID string) es.Event {
	return es.Event{
		BoundedContext: boundedContext,
		AggregateType:  aggType,
		AggregateID:    aggID,
		EventID:        uuid.New(),
		EventType:      aggType + "Created",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}
}

// countingConsumer is a test consumer that counts how many events it processes.
type countingConsumer struct {
	name    string
	count   atomic.Int64
	mu      sync.Mutex
	events  []es.PersistedEvent
	handler func(context.Context, *sql.Tx, es.PersistedEvent) error
}

func newCountingConsumer(name string) *countingConsumer {
	return &countingConsumer{
		name:   name,
		events: make([]es.PersistedEvent, 0),
	}
}

func (c *countingConsumer) Name() string {
	return c.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to match Consumer interface
func (c *countingConsumer) Handle(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
	if c.handler != nil {
		if err := c.handler(ctx, tx, event); err != nil {
			return err
		}
	}

	c.count.Add(1)
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
	return nil
}

func (c *countingConsumer) Count() int64 {
	return c.count.Load()
}

func (c *countingConsumer) Events() []es.PersistedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]es.PersistedEvent, len(c.events))
	copy(result, c.events)
	return result
}

// scopedCountingConsumer is a test consumer that filters by aggregate types and bounded contexts.
type scopedCountingConsumer struct {
	*countingConsumer
	aggregateTypes  []string
	boundedContexts []string
}

func newScopedCountingConsumer(name string, aggTypes []string, boundedContexts []string) *scopedCountingConsumer {
	return &scopedCountingConsumer{
		countingConsumer: newCountingConsumer(name),
		aggregateTypes:   aggTypes,
		boundedContexts:  boundedContexts,
	}
}

func (c *scopedCountingConsumer) AggregateTypes() []string {
	return c.aggregateTypes
}

func (c *scopedCountingConsumer) BoundedContexts() []string {
	return c.boundedContexts
}

// TestWorkerProcessesEvents tests basic end-to-end event processing.
func TestWorkerProcessesEvents(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append 5 events - each to a different aggregate
	// (store.Append requires same AggregateID in a batch)
	numEvents := 5
	for i := 0; i < numEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("TestContext", "TestAggregate", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit event %d: %v", i, err)
		}
	}

	// Create a counting consumer
	cons := newCountingConsumer("test-worker-basic")

	// Create Worker
	w := postgres.NewWorker(db, store)

	// Run with timeout
	workerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(workerCtx, cons)
	}()

	// Poll until all events are processed or timeout
	timeout := time.After(8 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			cancel()
			t.Fatalf("Timeout waiting for events to be processed. Got %d/%d events", cons.Count(), numEvents)
		case <-ticker.C:
			if cons.Count() == int64(numEvents) {
				cancel()
				// Wait for worker to finish
				if err := <-errCh; err != nil && err != context.Canceled {
					t.Fatalf("Worker returned error: %v", err)
				}
				return
			}
		}
	}
}

// TestWorkerMultipleConsumers tests that multiple consumers can run concurrently.
func TestWorkerMultipleConsumers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append 5 events
	numEvents := 5
	for i := 0; i < numEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("TestContext", "TestAggregate", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit event %d: %v", i, err)
		}
	}

	// Create 2 consumers with different names
	cons1 := newCountingConsumer("test-worker-multi-1")
	cons2 := newCountingConsumer("test-worker-multi-2")

	// Create Worker
	w := postgres.NewWorker(db, store)

	// Run with timeout
	workerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(workerCtx, cons1, cons2)
	}()

	// Poll until both consumers have processed all events or timeout
	timeout := time.After(8 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			cancel()
			t.Fatalf("Timeout waiting for events. Consumer1: %d/%d, Consumer2: %d/%d",
				cons1.Count(), numEvents, cons2.Count(), numEvents)
		case <-ticker.C:
			if cons1.Count() == int64(numEvents) && cons2.Count() == int64(numEvents) {
				cancel()
				// Wait for worker to finish
				if err := <-errCh; err != nil && err != context.Canceled {
					t.Fatalf("Worker returned error: %v", err)
				}

				// Verify both consumers processed all events independently
				if cons1.Count() != int64(numEvents) {
					t.Errorf("Consumer1 expected %d events, got %d", numEvents, cons1.Count())
				}
				if cons2.Count() != int64(numEvents) {
					t.Errorf("Consumer2 expected %d events, got %d", numEvents, cons2.Count())
				}
				return
			}
		}
	}
}

// TestWorkerScopedConsumer tests that scoped consumers only receive filtered events.
func TestWorkerScopedConsumer(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events from 2 different bounded contexts
	// Identity context with User aggregates
	numIdentityEvents := 3
	for i := 0; i < numIdentityEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("Identity", "User", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append Identity event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit Identity event %d: %v", i, err)
		}
	}

	// Sales context with Order aggregates
	numSalesEvents := 4
	for i := 0; i < numSalesEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("Sales", "Order", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append Sales event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit Sales event %d: %v", i, err)
		}
	}

	totalEvents := numIdentityEvents + numSalesEvents

	// Create a scoped consumer that only processes Identity/User events
	scopedCons := newScopedCountingConsumer(
		"test-worker-scoped",
		[]string{"User"},
		[]string{"Identity"},
	)

	// Create a global consumer that processes all events
	globalCons := newCountingConsumer("test-worker-global")

	// Create Worker
	w := postgres.NewWorker(db, store)

	// Run with timeout
	workerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(workerCtx, scopedCons, globalCons)
	}()

	// Poll until consumers have processed their expected events or timeout
	timeout := time.After(8 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			cancel()
			t.Fatalf("Timeout waiting for events. Scoped: %d/%d, Global: %d/%d",
				scopedCons.Count(), numIdentityEvents, globalCons.Count(), totalEvents)
		case <-ticker.C:
			if scopedCons.Count() == int64(numIdentityEvents) && globalCons.Count() == int64(totalEvents) {
				cancel()
				// Wait for worker to finish
				if err := <-errCh; err != nil && err != context.Canceled {
					t.Fatalf("Worker returned error: %v", err)
				}

				// Verify scoped consumer only got Identity events
				if scopedCons.Count() != int64(numIdentityEvents) {
					t.Errorf("Scoped consumer expected %d events, got %d", numIdentityEvents, scopedCons.Count())
				}

				// Verify all Identity events are User aggregates
				events := scopedCons.Events()
				for _, event := range events {
					if event.BoundedContext != "Identity" {
						t.Errorf("Scoped consumer received event from wrong context: %s", event.BoundedContext)
					}
					if event.AggregateType != "User" {
						t.Errorf("Scoped consumer received event from wrong aggregate: %s", event.AggregateType)
					}
				}

				// Verify global consumer got all events
				if globalCons.Count() != int64(totalEvents) {
					t.Errorf("Global consumer expected %d events, got %d", totalEvents, globalCons.Count())
				}

				return
			}
		}
	}
}

// TestWorkerWithOptions tests that custom configuration options work correctly.
func TestWorkerWithOptions(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append 10 events to test batch size
	numEvents := 10
	for i := 0; i < numEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("TestContext", "TestAggregate", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit event %d: %v", i, err)
		}
	}

	// Create consumer
	cons := newCountingConsumer("test-worker-options")

	// Create Worker with custom options
	w := postgres.NewWorker(
		db,
		store,
		worker.WithTotalSegments(4),
		worker.WithBatchSize(2),
		worker.WithPollInterval(50*time.Millisecond),
	)

	// Run with timeout
	workerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(workerCtx, cons)
	}()

	// Poll until all events are processed or timeout
	timeout := time.After(8 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			cancel()
			t.Fatalf("Timeout waiting for events to be processed. Got %d/%d events", cons.Count(), numEvents)
		case <-ticker.C:
			if cons.Count() == int64(numEvents) {
				cancel()
				// Wait for worker to finish
				if err := <-errCh; err != nil && err != context.Canceled {
					t.Fatalf("Worker returned error: %v", err)
				}

				// Verify all events were processed
				if cons.Count() != int64(numEvents) {
					t.Errorf("Consumer expected %d events, got %d", numEvents, cons.Count())
				}
				return
			}
		}
	}
}

// TestWorkerNoConsumers tests that worker returns an error when no consumers are provided.
func TestWorkerNoConsumers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create Worker
	w := postgres.NewWorker(db, store)

	// Run with no consumers - should return error
	err := w.Run(ctx)
	if err == nil {
		t.Fatal("Expected error when running worker with no consumers, got nil")
	}

	expectedErr := "at least one consumer is required"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}

// TestWorkerHandlerError tests that worker stops on handler error.
func TestWorkerHandlerError(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append 5 events
	numEvents := 5
	for i := 0; i < numEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("TestContext", "TestAggregate", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit event %d: %v", i, err)
		}
	}

	// Create consumer that fails after processing 2 events
	cons := newCountingConsumer("test-worker-error")
	expectedErr := fmt.Errorf("test error")
	cons.handler = func(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
		if cons.Count() >= 1 { // Will have incremented by the time this check runs
			return expectedErr
		}
		return nil
	}

	// Create Worker
	w := postgres.NewWorker(db, store)

	// Run
	err := w.Run(ctx, cons)

	// Should get an error
	if err == nil {
		t.Fatal("Expected error from handler, got nil")
	}

	// Worker should have processed at least 1 event before erroring
	if cons.Count() < 1 {
		t.Errorf("Expected at least 1 event processed before error, got %d", cons.Count())
	}

	// Should not have processed all events
	if cons.Count() >= int64(numEvents) {
		t.Errorf("Expected fewer than %d events processed due to error, got %d", numEvents, cons.Count())
	}
}

// TestWorkerContextCancellation tests that worker respects context cancellation.
func TestWorkerContextCancellation(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append many events
	numEvents := 20
	for i := 0; i < numEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("TestContext", "TestAggregate", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit event %d: %v", i, err)
		}
	}

	// Create consumer with slow handler
	cons := newCountingConsumer("test-worker-cancel")
	cons.handler = func(ctx context.Context, tx *sql.Tx, event es.PersistedEvent) error {
		// Slow down processing to ensure we don't finish before cancel
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	// Create Worker with batch size 1 so context is checked between each event
	w := postgres.NewWorker(db, store, worker.WithBatchSize(1))

	// Run with context that will be canceled
	workerCtx, cancel := context.WithCancel(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(workerCtx, cons)
	}()

	// Wait a bit for processing to start, then cancel
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Wait for worker to finish
	err := <-errCh

	// Should get context.Canceled error
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}

	// Should have processed some but not all events
	count := cons.Count()
	if count == 0 {
		t.Error("Expected to process at least some events before cancellation")
	}
	if count >= int64(numEvents) {
		t.Errorf("Expected to process fewer than %d events before cancellation, got %d", numEvents, count)
	}
}

func TestWorkerReclaimsDetachedSegments(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	consumerName := "test-worker-reclaim-detached"
	cons := newCountingConsumer(consumerName)

	w := postgres.NewWorker(db, store,
		worker.WithTotalSegments(4),
		worker.WithRebalanceInterval(200*time.Millisecond),
		worker.WithHeartbeatInterval(200*time.Millisecond),
		worker.WithStaleThreshold(5*time.Second),
		worker.WithPollInterval(50*time.Millisecond),
		worker.WithMaxPollInterval(200*time.Millisecond),
	)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(workerCtx, cons)
	}()

	// Wait for initial segment claims and capture owner ID.
	var ownerID string
	{
		deadline := time.After(6 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

	initialClaims:
		for {
			select {
			case <-deadline:
				cancel()
				t.Fatal("timeout waiting for worker to claim all segments")
			case <-ticker.C:
				segments, err := store.GetSegments(ctx, db, consumerName)
				if err != nil || len(segments) != 4 {
					continue
				}

				allOwned := true
				sameOwner := true
				var candidate string
				for i, seg := range segments {
					if seg.OwnerID == nil {
						allOwned = false
						break
					}
					if i == 0 {
						candidate = *seg.OwnerID
						continue
					}
					if *seg.OwnerID != candidate {
						sameOwner = false
						break
					}
				}

				if allOwned && sameOwner && candidate != "" {
					ownerID = candidate
					break initialClaims
				}
			}
		}
	}
	// Simulate detached ownership for two segments while worker goroutines keep running.
	_, err := db.ExecContext(ctx, `
		UPDATE consumer_segments
		SET owner_id = NULL
		WHERE consumer_name = $1
		  AND segment_id IN (0, 1)
	`, consumerName)
	if err != nil {
		cancel()
		t.Fatalf("failed to detach segment ownership: %v", err)
	}

	// Worker should reclaim the detached segments on the next rebalance cycle.
	{
		deadline := time.After(6 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-deadline:
				cancel()
				t.Fatal("timeout waiting for detached segments to be reclaimed")
			case <-ticker.C:
				segments, err := store.GetSegments(ctx, db, consumerName)
				if err != nil || len(segments) != 4 {
					continue
				}

				allReclaimed := true
				for _, seg := range segments {
					if seg.OwnerID == nil || *seg.OwnerID != ownerID {
						allReclaimed = false
						break
					}
				}

				if allReclaimed {
					cancel()
					runErr := <-errCh
					if runErr != nil && runErr != context.Canceled {
						t.Fatalf("worker returned unexpected error: %v", runErr)
					}
					return
				}
			}
		}
	}
}

// getTestConnStr returns a Postgres connection string in DSN format (for pq.NewListener).
func getTestConnStr() string {
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

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}

// TestWorkerWithNotifyDispatcher tests that LISTEN/NOTIFY wakes the worker instantly
// instead of waiting for the polling interval.
func TestWorkerWithNotifyDispatcher(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	notifyChannel := "pupsourcing_test_events"
	store := postgres.NewStore(
		postgres.NewStoreConfig(
			postgres.WithNotifyChannel(notifyChannel),
		),
	)

	// Create a counting consumer
	cons := newCountingConsumer("test-notify-dispatcher")

	// Create NotifyDispatcher with a short fallback interval for test safety
	nd := postgres.NewNotifyDispatcher(getTestConnStr(), &postgres.NotifyDispatcherConfig{
		Channel:          notifyChannel,
		FallbackInterval: 10 * time.Second,
	})

	// Create Worker with NotifyDispatcher as WakeupSource and a VERY long poll interval.
	// If events are processed within the timeout, it proves LISTEN/NOTIFY woke the worker,
	// not the polling interval.
	w := postgres.NewWorker(db, store,
		worker.WithWakeupSource(nd),
		worker.WithPollInterval(5*time.Second),
		worker.WithMaxPollInterval(30*time.Second),
	)

	// Start worker
	workerCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(workerCtx, cons)
	}()

	// Give the worker time to start and the LISTEN connection to establish
	time.Sleep(2 * time.Second)

	// Append 5 events AFTER the worker is running — they should be picked up via NOTIFY
	numEvents := 5
	for i := 0; i < numEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("TestContext", "TestAggregate", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit event %d: %v", i, err)
		}
	}

	// Events should be processed very quickly via NOTIFY (within ~2s, not 5s+ from polling)
	deadline := time.After(4 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("Timeout waiting for NOTIFY-driven processing. Got %d/%d events. "+
				"If polling interval is 5s and this failed at 4s, LISTEN/NOTIFY is not working.",
				cons.Count(), numEvents)
		case <-ticker.C:
			if cons.Count() == int64(numEvents) {
				cancel()
				if err := <-errCh; err != nil && err != context.Canceled {
					t.Fatalf("Worker returned error: %v", err)
				}
				t.Logf("All %d events processed via NOTIFY-driven wake", numEvents)
				return
			}
		}
	}
}

// TestWorkerResumesFromCheckpoint tests that worker resumes from last checkpoint.
func TestWorkerResumesFromCheckpoint(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append 10 events
	numEvents := 10
	for i := 0; i < numEvents; i++ {
		aggID := uuid.New().String()
		event := makeEvent("TestContext", "TestAggregate", aggID)

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event %d: %v", i, err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit event %d: %v", i, err)
		}
	}

	consumerName := "test-worker-resume"
	// Keep this test single-threaded and per-event transactional to avoid
	// nondeterministic over-processing from multi-segment concurrency.
	workerOpts := []worker.Option{
		worker.WithTotalSegments(1),
		worker.WithBatchSize(1),
	}

	// First run - process first 4 events, error on 5th
	firstRunExpected := 4
	{
		cons := newCountingConsumer(consumerName)
		cons.handler = func(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
			if cons.Count() >= 4 {
				return fmt.Errorf("stop processing")
			}
			return nil
		}

		w := postgres.NewWorker(db, store, workerOpts...)
		_ = w.Run(ctx, cons) // Expect error

		firstRunCount := cons.Count()
		if firstRunCount != int64(firstRunExpected) {
			t.Fatalf("Expected first run to process %d events, got %d", firstRunExpected, firstRunCount)
		}
	}

	// Second run - should resume from checkpoint and process remaining events
	{
		cons := newCountingConsumer(consumerName)

		w := postgres.NewWorker(db, store, workerOpts...)

		workerCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- w.Run(workerCtx, cons)
		}()

		// Poll until remaining events are processed or timeout
		expectedRemaining := int64(numEvents - firstRunExpected)
		timeout := time.After(10 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				cancel()
				t.Fatalf("Timeout waiting for remaining events. Got %d/%d", cons.Count(), expectedRemaining)
			case <-ticker.C:
				if cons.Count() == expectedRemaining {
					cancel()
					// Wait for worker to finish
					if err := <-errCh; err != nil && err != context.Canceled {
						t.Fatalf("Worker returned error: %v", err)
					}

					// Verify only remaining events were processed in second run
					if cons.Count() != expectedRemaining {
						t.Errorf("Second run expected to process %d events, got %d", expectedRemaining, cons.Count())
					}
					return
				}
			}
		}
	}
}
