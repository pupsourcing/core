// Package main demonstrates basic event sourcing with MySQL/MariaDB.
// This example shows how to use the MySQL adapter to append and read events.
//
// Run this example:
//  1. Start MySQL: docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=pupsourcing_example mysql:8
//  2. Run: go run main.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/mysql"
	"github.com/pupsourcing/core/es/migrations"
)

// UserCreated is a sample event payload
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

func main() {
	// Connection string - use environment variable if available
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "root:password@tcp(localhost:3306)/pupsourcing_example?parseTime=true"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test connection
	if pingErr := db.Ping(); pingErr != nil {
		//nolint:gocritic // it's fine for examples
		log.Fatalf("Failed to ping database: %v", pingErr)
	}

	ctx := context.Background()

	// Generate and apply migrations
	log.Println("Setting up database schema...")
	if err := setupSchema(db); err != nil {
		log.Fatalf("Failed to setup schema: %v", err)
	}

	// Create event store
	store := mysql.NewStore(mysql.DefaultStoreConfig())

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
}

func setupSchema(db *sql.DB) error {
	// Drop existing tables for clean start
	// MySQL requires separate Exec calls for each statement
	_, err := db.Exec(`DROP TABLE IF EXISTS consumer_checkpoints`)
	if err != nil {
		return fmt.Errorf("failed to drop consumer_checkpoints table: %w", err)
	}
	_, err = db.Exec(`DROP TABLE IF EXISTS aggregate_heads`)
	if err != nil {
		return fmt.Errorf("failed to drop aggregate_heads table: %w", err)
	}
	_, err = db.Exec(`DROP TABLE IF EXISTS events`)
	if err != nil {
		return fmt.Errorf("failed to drop events table: %w", err)
	}

	// Generate migration
	tmpDir := "/tmp"
	config := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "init.sql",
		EventsTable:         "events",
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
	}

	//nolint:govet // Shadow is acceptable here - err is properly scoped
	if err := migrations.GenerateMySQL(&config); err != nil {
		return fmt.Errorf("failed to generate migration: %w", err)
	}

	migrationSQL, err := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, config.OutputFilename))
	if err != nil {
		return fmt.Errorf("failed to read migration: %w", err)
	}

	// MySQL requires executing statements separately
	// Split by semicolon and execute each statement
	statements := strings.Split(string(migrationSQL), ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Check if this is a comment-only statement
		lines := strings.Split(stmt, "\n")
		hasNonComment := false
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "--") {
				hasNonComment = true
				break
			}
		}

		if !hasNonComment {
			continue
		}

		_, err = db.Exec(stmt)
		if err != nil {
			return fmt.Errorf("failed to execute migration statement: %w\nStatement: %s", err, stmt)
		}
	}

	return nil
}

func appendUserEvents(ctx context.Context, db *sql.DB, store *mysql.Store, aggregateID string) error {
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

func readAllEvents(ctx context.Context, db *sql.DB, store *mysql.Store) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	//nolint:errcheck // cleanup
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

func readAggregateStream(ctx context.Context, db *sql.DB, store *mysql.Store, aggregateID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	//nolint:errcheck // cleanup
	defer tx.Rollback()

	stream, err := store.ReadAggregateStream(ctx, tx, "Identity", "User", aggregateID, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to read aggregate stream: %w", err)
	}

	log.Printf("Found %d events for aggregate %s:", stream.Len(), aggregateID)
	//nolint:gocritic // Iterating by value is acceptable for example code
	for _, event := range stream.Events {
		var payload UserCreated
		if err = json.Unmarshal(event.Payload, &payload); err != nil {
			log.Printf("  - Version %d: %s (failed to parse payload)", event.AggregateVersion, event.EventType)
			continue
		}
		log.Printf("  - Version %d: %s - %s (%s)", event.AggregateVersion, event.EventType, payload.Name, payload.Email)
	}

	return nil
}
