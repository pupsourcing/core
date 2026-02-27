// Package main demonstrates using pupsourcing with CockroachDB.
//
// CockroachDB implements the PostgreSQL wire protocol, so we can use the
// existing PostgreSQL adapter without any modifications.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
)

const (
	// Default CockroachDB connection string for local development
	defaultConnStr = "postgresql://root@localhost:26257/pupsourcing?sslmode=disable"
)

func main() {
	ctx := context.Background()

	// Get connection string from environment or use default
	connStr := os.Getenv("COCKROACH_URL")
	if connStr == "" {
		connStr = defaultConnStr
		log.Printf("Using default connection string. Set COCKROACH_URL to override.")
	}

	log.Printf("Connecting to CockroachDB at %s", connStr)

	// Connect to CockroachDB using the PostgreSQL driver
	// This works because CockroachDB implements the PostgreSQL wire protocol
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("✓ Connected to CockroachDB successfully")

	// Create store using the PostgreSQL adapter
	// No special CockroachDB adapter is needed!
	store := postgres.NewStore(postgres.DefaultStoreConfig())
	log.Println("✓ PostgreSQL adapter initialized for CockroachDB")

	// Example 1: Create a new aggregate with events
	if err := exampleCreateAggregate(ctx, db, store); err != nil {
		log.Fatalf("Example 1 failed: %v", err)
	}

	// Example 2: Append more events to existing aggregate
	if err := exampleAppendEvents(ctx, db, store); err != nil {
		log.Fatalf("Example 2 failed: %v", err)
	}

	// Example 3: Read aggregate stream
	if err := exampleReadAggregate(ctx, db, store); err != nil {
		log.Fatalf("Example 3 failed: %v", err)
	}

	// Example 4: Demonstrate optimistic concurrency
	if err := exampleOptimisticConcurrency(ctx, db, store); err != nil {
		log.Fatalf("Example 4 failed: %v", err)
	}

	log.Println("\n✓ All examples completed successfully!")
	log.Println("\nCockroachDB Web UI: http://localhost:8080")
	log.Println("Check the UI to see your events in the database")
}

// exampleCreateAggregate shows creating a new aggregate with its first events
func exampleCreateAggregate(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	log.Println("\n--- Example 1: Create New Aggregate ---")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create user aggregate events
	userID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "Identity",
			AggregateType:  "User",
			AggregateID:    userID,
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"email":"alice@example.com","name":"Alice"}`),
			Metadata:       []byte(`{"ip":"192.168.1.1"}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "Identity",
			AggregateType:  "User",
			AggregateID:    userID,
			EventID:        uuid.New(),
			EventType:      "EmailVerified",
			EventVersion:   1,
			Payload:        []byte(`{"verified_at":"2026-01-01T20:00:00Z"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Append events - use NoStream() to indicate this is a new aggregate
	result, err := store.Append(ctx, tx, es.NoStream(), events)
	if err != nil {
		return fmt.Errorf("failed to append events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("✓ Created user aggregate: %s", userID)
	log.Printf("  Events appended at global positions: %v", result.GlobalPositions)
	log.Printf("  Aggregate version: %d", result.ToVersion())

	return nil
}

// exampleAppendEvents shows appending events to an existing aggregate
func exampleAppendEvents(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	log.Println("\n--- Example 2: Append to Existing Aggregate ---")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create and append events
	orderID := uuid.New().String()

	// First event - create order
	events1 := []es.Event{
		{
			BoundedContext: "Sales",
			AggregateType:  "Order",
			AggregateID:    orderID,
			EventID:        uuid.New(),
			EventType:      "OrderCreated",
			EventVersion:   1,
			Payload:        []byte(`{"total":99.99,"items":["item1","item2"]}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	result1, err := store.Append(ctx, tx, es.NoStream(), events1)
	if err != nil {
		return fmt.Errorf("failed to append first event: %w", err)
	}

	log.Printf("✓ Created order: %s at version %d", orderID, result1.ToVersion())

	// Second event - add items (expecting version 1)
	events2 := []es.Event{
		{
			BoundedContext: "Sales",
			AggregateType:  "Order",
			AggregateID:    orderID,
			EventID:        uuid.New(),
			EventType:      "OrderItemAdded",
			EventVersion:   1,
			Payload:        []byte(`{"item":"item3","price":29.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Use ExactVersion to enforce optimistic concurrency
	result2, err := store.Append(ctx, tx, es.Exact(1), events2)
	if err != nil {
		return fmt.Errorf("failed to append second event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("✓ Added item to order: version %d -> %d", result1.ToVersion(), result2.ToVersion())

	return nil
}

// exampleReadAggregate shows reading all events for an aggregate
func exampleReadAggregate(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	log.Println("\n--- Example 3: Read Aggregate Stream ---")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create a test aggregate
	productID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "Catalog",
			AggregateType:  "Product",
			AggregateID:    productID,
			EventID:        uuid.New(),
			EventType:      "ProductCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Widget","price":19.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "Catalog",
			AggregateType:  "Product",
			AggregateID:    productID,
			EventID:        uuid.New(),
			EventType:      "PriceUpdated",
			EventVersion:   1,
			Payload:        []byte(`{"old_price":19.99,"new_price":24.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	if _, err := store.Append(ctx, tx, es.NoStream(), events); err != nil {
		return fmt.Errorf("failed to append events: %w", err)
	}

	// Read the aggregate stream
	stream, err := store.ReadAggregateStream(ctx, tx, "Catalog", "Product", productID, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to read aggregate stream: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("✓ Read aggregate stream for product: %s", productID)
	log.Printf("  Stream length: %d events", stream.Len())
	log.Printf("  Current version: %d", stream.Version())
	log.Printf("  Bounded context: %s", stream.BoundedContext)

	for i, event := range stream.Events {
		log.Printf("  Event %d: %s (v%d) at position %d",
			i+1, event.EventType, event.AggregateVersion, event.GlobalPosition)
	}

	return nil
}

// exampleOptimisticConcurrency demonstrates version conflict detection
func exampleOptimisticConcurrency(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	log.Println("\n--- Example 4: Optimistic Concurrency Control ---")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create an account
	accountID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "Banking",
			AggregateType:  "Account",
			AggregateID:    accountID,
			EventID:        uuid.New(),
			EventType:      "AccountOpened",
			EventVersion:   1,
			Payload:        []byte(`{"balance":1000.00}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	if _, err := store.Append(ctx, tx, es.NoStream(), events); err != nil {
		return fmt.Errorf("failed to create account: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("✓ Created account: %s", accountID)

	// Try to append with wrong expected version
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx2.Rollback()

	wrongVersionEvents := []es.Event{
		{
			BoundedContext: "Banking",
			AggregateType:  "Account",
			AggregateID:    accountID,
			EventID:        uuid.New(),
			EventType:      "MoneyDeposited",
			EventVersion:   1,
			Payload:        []byte(`{"amount":100.00}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// This will fail because we expect version 999, but actual version is 1
	_, err = store.Append(ctx, tx2, es.Exact(999), wrongVersionEvents)
	if err != nil {
		log.Printf("✓ Optimistic concurrency check caught version mismatch")
		log.Printf("  Error: %v", err)
		return nil // This error is expected
	}

	return fmt.Errorf("expected optimistic concurrency error but didn't get one")
}
