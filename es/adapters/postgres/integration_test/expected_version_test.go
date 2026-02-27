//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/store"
)

// TestExpectedVersion_NoStream tests that NoStream() enforces aggregate doesn't exist
func TestExpectedVersion_NoStream(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	event := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventCreated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	// First append with NoStream() should succeed
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, es.NoStream(), []es.Event{event})
	if err != nil {
		t.Fatalf("First append with NoStream() should succeed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Second append with NoStream() should fail (aggregate already exists)
	event2 := event
	event2.EventID = uuid.New()
	event2.EventType = "TestEventUpdated"

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, es.NoStream(), []es.Event{event2})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Second append with NoStream() should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx2.Rollback()
}

// TestExpectedVersion_Exact tests that Exact(N) enforces exact version match
func TestExpectedVersion_Exact(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	// Create aggregate with version 1
	event1 := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventCreated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, es.NoStream(), []es.Event{event1})
	if err != nil {
		t.Fatalf("First append should succeed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Append with Exact(1) should succeed
	event2 := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventUpdated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, es.Exact(1), []es.Event{event2})
	if err != nil {
		t.Fatalf("Append with Exact(1) should succeed: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Append with Exact(1) should now fail (version is now 2)
	event3 := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventUpdated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	tx3, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx3, es.Exact(1), []es.Event{event3})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Append with Exact(1) should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx3.Rollback()

	// Append with Exact(2) should succeed
	tx4, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx4, es.Exact(2), []es.Event{event3})
	if err != nil {
		t.Fatalf("Append with Exact(2) should succeed: %v", err)
	}
	if err := tx4.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
}

// TestExpectedVersion_Exact_NonExistent tests that Exact(N) fails for non-existent aggregates
func TestExpectedVersion_Exact_NonExistent(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	event := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventCreated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	// Append with Exact(1) should fail (aggregate doesn't exist)
	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, es.Exact(1), []es.Event{event})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Append with Exact(1) on non-existent aggregate should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx.Rollback()
}

// TestExpectedVersion_Exact_Zero tests that Exact(0) can be used to create new aggregates
func TestExpectedVersion_Exact_Zero(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	event := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventCreated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	// Append with Exact(0) should succeed when aggregate is at version 0 (new aggregate)
	tx1, _ := db.BeginTx(ctx, nil)
	result, err := pgStore.Append(ctx, tx1, es.Exact(0), []es.Event{event})
	if err != nil {
		t.Fatalf("First append with Exact(0) should succeed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify aggregate now has version 1
	if result.ToVersion() != 1 {
		t.Errorf("Expected version 1 after first append, got %d", result.ToVersion())
	}

	// Append with Exact(0) should now fail (aggregate is at version 1, not 0)
	event2 := event
	event2.EventID = uuid.New()
	event2.EventType = "TestEventUpdated"

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, es.Exact(0), []es.Event{event2})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Second append with Exact(0) should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx2.Rollback()
}

// TestExpectedVersion_Any tests that Any() allows appends regardless of version
func TestExpectedVersion_Any(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	// First append with Any() on new aggregate should succeed
	event1 := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventCreated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, es.Any(), []es.Event{event1})
	if err != nil {
		t.Fatalf("First append with Any() should succeed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Second append with Any() on existing aggregate should also succeed
	event2 := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventUpdated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, es.Any(), []es.Event{event2})
	if err != nil {
		t.Fatalf("Second append with Any() should succeed: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
}

// TestExpectedVersion_UniquenessPattern tests the reservation aggregate pattern
func TestExpectedVersion_UniquenessPattern(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Use email as aggregate ID (reservation pattern)
	email := "user@example.com"

	event := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "EmailReservation",
		AggregateID:    email,
		EventID:        uuid.New(),
		EventType:      "EmailReserved",
		EventVersion:   1,
		Payload:        []byte(`{"email":"user@example.com"}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	// First reservation should succeed
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, es.NoStream(), []es.Event{event})
	if err != nil {
		t.Fatalf("First email reservation should succeed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Second reservation of same email should fail (enforces uniqueness)
	event2 := event
	event2.EventID = uuid.New()

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, es.NoStream(), []es.Event{event2})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Second email reservation should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx2.Rollback()
}
