// Package main demonstrates basic event sourcing with SQLite.
// This example shows how to use the SQLite adapter to append and read events.
//
// Run this example:
//  1. Run: go run main.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/sqlite"
	"github.com/getpup/pupsourcing/es/migrations"
)

// UserCreated is a sample event payload
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

func main() {
	// Create SQLite database
	dbFile := "pupsourcing_example.db"
	// Clean up old database for fresh start
	os.Remove(dbFile)

	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Enable WAL mode for better concurrency
	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	if err != nil {
		//nolint:gocritic // it's fine for examples
		log.Fatalf("Failed to configure database: %v", err)
	}

	ctx := context.Background()

	// Generate and apply migrations
	log.Println("Setting up database schema...")
	if err := setupSchema(db); err != nil {
		log.Fatalf("Failed to setup schema: %v", err)
	}

	// Create event store
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// Example 1: Append events
	log.Println("\n=== Example 1: Appending Events ===")
	aggregateID := uuid.New().String()
	if err := appendUserEvents(ctx, db, store, aggregateID); err != nil {
		log.Fatalf("Failed to append events: %v", err)
	}

	// Example 2: Read all events
	log.Println("\n=== Example 2: Reading All Events ===")
	if err := readAllEvents(ctx, db, store); err != nil {
		log.Fatalf("Failed to read events: %v", err)
	}

	// Example 3: Read aggregate stream
	log.Println("\n=== Example 3: Reading Aggregate Stream ===")
	if err := readAggregateStream(ctx, db, store, aggregateID); err != nil {
		log.Fatalf("Failed to read aggregate stream: %v", err)
	}

	log.Println("\n✓ Example completed successfully!")
	log.Printf("Database saved to: %s", dbFile)
}

func setupSchema(db *sql.DB) error {
	// Generate migration
	tmpDir := "/tmp"
	config := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "init.sql",
		EventsTable:         "events",
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
	}

	if err := migrations.GenerateSQLite(&config); err != nil {
		return fmt.Errorf("failed to generate migration: %w", err)
	}

	migrationSQL, err := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, config.OutputFilename))
	if err != nil {
		return fmt.Errorf("failed to read migration: %w", err)
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		return fmt.Errorf("failed to execute migration: %w", err)
	}

	return nil
}

func appendUserEvents(ctx context.Context, db *sql.DB, store *sqlite.Store, aggregateID string) error {
	users := []UserCreated{
		{Email: "alice@example.com", Name: "Alice Smith"},
		{Email: "bob@example.com", Name: "Bob Johnson"},
		{Email: "carol@example.com", Name: "Carol Williams"},
	}

	for _, user := range users {
		payload, err := json.Marshal(user)
		if err != nil {
			return fmt.Errorf("failed to marshal user: %w", err)
		}

		events := []es.Event{
			{
				BoundedContext: "Identity",
				AggregateType:  "User",
				AggregateID:    aggregateID,
				EventID:        uuid.New(),
				EventType:      "UserCreated",
				EventVersion:   1,
				Payload:        payload,
				Metadata:       []byte(`{}`),
				CreatedAt:      time.Now(),
			},
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		result, err := store.Append(ctx, tx, es.NoStream(), events)
		if err != nil {
			//nolint:errcheck // Rollback error ignored: transaction already failed
			tx.Rollback()
			return fmt.Errorf("failed to append events: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}

		log.Printf("✓ Appended event for %s at position %d", user.Name, result.GlobalPositions[0])
	}

	return nil
}

func readAllEvents(ctx context.Context, db *sql.DB, store *sqlite.Store) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	//nolint:errcheck // Rollback is allowed to fail in defer
	defer tx.Rollback()

	events, err := store.ReadEvents(ctx, tx, 0, 100)
	if err != nil {
		return fmt.Errorf("failed to read events: %w", err)
	}

	log.Printf("Found %d events:", len(events))
	//nolint:gocritic // Iterating by value is acceptable for example code
	for _, event := range events {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			log.Printf("  - Position %d: %s (failed to parse payload)", event.GlobalPosition, event.EventType)
			continue
		}
		log.Printf("  - Position %d: %s - %s (%s)", event.GlobalPosition, event.EventType, payload.Name, payload.Email)
	}

	return nil
}

func readAggregateStream(ctx context.Context, db *sql.DB, store *sqlite.Store, aggregateID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	//nolint:errcheck // Rollback is allowed to fail in defer
	defer tx.Rollback()

	stream, err := store.ReadAggregateStream(ctx, tx, "Identity", "User", aggregateID, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to read aggregate stream: %w", err)
	}

	log.Printf("Found %d events for aggregate %s:", stream.Len(), aggregateID)
	//nolint:gocritic // Iterating by value is acceptable for example code
	for _, event := range stream.Events {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			log.Printf("  - Version %d: %s (failed to parse payload)", event.AggregateVersion, event.EventType)
			continue
		}
		log.Printf("  - Version %d: %s - %s (%s)", event.AggregateVersion, event.EventType, payload.Name, payload.Email)
	}

	return nil
}
