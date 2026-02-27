// Package main demonstrates using a custom logger with pupsourcing.
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

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
)

//go:generate go run ../../cmd/migrate-gen -output ../../migrations -filename init.sql

// SimpleLogger is a basic logger implementation for demonstration.
// In production, you would typically integrate with your preferred logging library
// such as zap, zerolog, logrus, or slog.
type SimpleLogger struct {
	prefix string
}

// Debug implements es.Logger.
func (l *SimpleLogger) Debug(_ context.Context, msg string, keyvals ...interface{}) {
	l.log("DEBUG", msg, keyvals...)
}

// Info implements es.Logger.
func (l *SimpleLogger) Info(_ context.Context, msg string, keyvals ...interface{}) {
	l.log("INFO", msg, keyvals...)
}

// Error implements es.Logger.
func (l *SimpleLogger) Error(_ context.Context, msg string, keyvals ...interface{}) {
	l.log("ERROR", msg, keyvals...)
}

func (l *SimpleLogger) log(level, msg string, keyvals ...interface{}) {
	// Format key-value pairs
	var kvStr string
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			kvStr += fmt.Sprintf(" %v=%v", keyvals[i], keyvals[i+1])
		}
	}
	log.Printf("[%s] %s%s%s", level, l.prefix, msg, kvStr)
}

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
	return "user_list_with_logging"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *UserProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.users = append(p.users, payload.Email)
		fmt.Printf("✓ Projection processed: User created - %s (%s)\n", payload.Name, payload.Email)
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

	// Create logger instances
	storeLogger := &SimpleLogger{prefix: "[Store] "}
	projectionLogger := &SimpleLogger{prefix: "[Projection] "}

	fmt.Println("=== Pupsourcing with Logging Example ===")
	fmt.Println()

	// Create event store with logger
	storeConfig := postgres.DefaultStoreConfig()
	storeConfig.Logger = storeLogger
	store := postgres.NewStore(storeConfig)

	// Append some events
	fmt.Println("--- Appending Events ---")
	aggregateID := uuid.New().String()

	payload1, err := json.Marshal(UserCreated{
		Email: "alice@example.com",
		Name:  "Alice Smith",
	})
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		return
	}

	payload2, err := json.Marshal(UserCreated{
		Email: "bob@example.com",
		Name:  "Bob Jones",
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
		{
			BoundedContext: "Identity",
			AggregateType:  "User",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        payload2,
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

	result, err := store.Append(ctx, tx, es.NoStream(), events)
	if err != nil {
		log.Printf("Failed to append events: %v", err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit: %v", err)
		return
	}

	fmt.Printf("\n✓ Events appended at positions: %v\n", result.GlobalPositions)
	fmt.Printf("✓ Aggregate is now at version: %d\n", result.ToVersion())
	fmt.Println()

	// Process events with projection (with logging)
	fmt.Println("--- Running Projection with Logging ---")
	proj := &UserProjection{users: []string{}}

	processorConfig := consumer.DefaultProcessorConfig()
	processorConfig.Logger = projectionLogger
	processor := postgres.NewProcessor(db, store, &processorConfig)

	// Run projection for a short time
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := processor.Run(ctx2, proj); err != nil && err != context.DeadlineExceeded {
		log.Printf("Projection error: %v", err)
	}

	fmt.Printf("\n✓ Projection result - Users: %v\n", proj.users)
	fmt.Println()
	fmt.Println("=== Example Complete ===")
	fmt.Println()
	fmt.Println("Note: Check the log output above to see the observability hooks in action.")
	fmt.Println("In production, integrate with your preferred logging library (zap, zerolog, logrus, slog, etc.)")
}
