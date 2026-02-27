// Package main demonstrates a single projection running on a single worker.
// This is the simplest possible setup - one projection, one process, no partitioning.
//
// Run this example:
//  1. Start PostgreSQL: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_example postgres:16
//  2. Run migrations from the basic example
//  3. Run this example: go run main.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/consumer"
)

// UserCreated is a sample event payload
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// SimpleProjection maintains a count of users created
type SimpleProjection struct {
	count int
}

func (p *SimpleProjection) Name() string {
	return "simple_user_counter"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *SimpleProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.count++
		log.Printf("User created: %s (%s) - Total: %d", payload.Name, payload.Email, p.count)
	}
	return nil
}

func main() {
	// Connection string - use environment variable if available
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "host=localhost port=5432 user=postgres password=postgres dbname=pupsourcing_example sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Test connection
	if pingErr := db.Ping(); pingErr != nil {
		log.Fatalf("Failed to ping database: %v", pingErr)
	}
	defer db.Close()

	ctx := context.Background()

	// Create event store
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append some sample events to demonstrate the projection
	log.Println("Appending sample events...")
	if appendErr := appendSampleEvents(ctx, db, store); appendErr != nil {
		log.Printf("Warning: Failed to append sample events: %v", appendErr)
	}

	// Create projection
	proj := &SimpleProjection{}

	// Create processor with default configuration
	config := consumer.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nReceived shutdown signal, stopping projection...")
		cancel()
	}()

	// Run projection
	log.Println("Starting single-worker projection...")
	log.Println("Press Ctrl+C to stop")
	err = processor.Run(ctx, proj)
	if err != nil && !errors.Is(err, context.Canceled) {
		//nolint:gocritic // it's just an example code
		log.Fatalf("Projection error: %v", err)
	}

	log.Printf("Projection stopped. Final count: %d", proj.count)
}

func appendSampleEvents(ctx context.Context, db *sql.DB, store *postgres.Store) error {
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
				AggregateID:    uuid.New().String(),
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

		_, err = store.Append(ctx, tx, es.NoStream(), events)
		if err != nil {
			//nolint:errcheck // Rollback error ignored: transaction already failed
			tx.Rollback()
			return fmt.Errorf("failed to append events: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
	}

	return nil
}
