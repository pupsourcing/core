// Package main demonstrates basic usage of the pupsourcing library.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
)

//go:generate go run ../../cmd/migrate-gen -output ../../migrations -filename init.sql

// UserCreated is a sample event payload
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// UserProjection is an example projection that maintains a list of users
type UserProjection struct {
	users []string
}

func (p *UserProjection) Name() string {
	return "user_list"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *UserProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.users = append(p.users, payload.Email)
		fmt.Printf("Projection processed: User created - %s (%s)\n", payload.Name, payload.Email)
	}
	return nil
}

func main() {
	// This is a demonstration - in production, use proper connection management
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=pupsourcing_example sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create event store
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append some events
	fmt.Println("Appending events...")
	aggregateID := uuid.New().String()

	payload1, err := json.Marshal(UserCreated{
		Email: "alice@example.com",
		Name:  "Alice Smith",
	})
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		return
	}

	events := []es.Event{
		{
			BoundedContext: "Identity",
			AggregateType:  "User",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        payload1,
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		tx.Rollback()
	}()

	// Use NoStream() for creating a new aggregate
	result, err := store.Append(ctx, tx, es.NoStream(), events)
	if err != nil {
		log.Printf("Failed to append events: %v", err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit: %v", err)
		return
	}

	fmt.Printf("Events appended at positions: %v\n", result.GlobalPositions)
	fmt.Printf("Aggregate is now at version: %d\n", result.ToVersion())

	// Process events with projection
	fmt.Println("\nRunning projection...")
	proj := &UserProjection{users: []string{}}
	config := projection.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

	// Run projection for a short time
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := processor.Run(ctx2, proj); err != nil && err != context.DeadlineExceeded {
		log.Printf("Projection error: %v", err)
	}

	fmt.Printf("\nProjection result - Users: %v\n", proj.users)
}
