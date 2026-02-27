// Package main demonstrates running multiple projections with the runner package.
// This shows how to run different projections with different configurations in the same process.
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
	"github.com/getpup/pupsourcing/es/consumer/runner"
)

// UserCreated event
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// OrderPlaced event
type OrderPlaced struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
}

// UserCounterProjection counts users
type UserCounterProjection struct {
	count int
}

func (p *UserCounterProjection) Name() string {
	return "user_counter"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *UserCounterProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.count++
		log.Printf("[UserCounter] User created: %s - Total: %d", payload.Name, p.count)
	}
	return nil
}

// RevenueProjection tracks total revenue
type RevenueProjection struct {
	total float64
}

func (p *RevenueProjection) Name() string {
	return "revenue_tracker"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *RevenueProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "OrderPlaced" {
		var payload OrderPlaced
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.total += payload.Amount
		log.Printf("[Revenue] Order placed: $%.2f - Total: $%.2f", payload.Amount, p.total)
	}
	return nil
}

// ActivityLogProjection logs all events
type ActivityLogProjection struct {
	eventCount int
}

func (p *ActivityLogProjection) Name() string {
	return "activity_log"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *ActivityLogProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.eventCount++
	log.Printf("[Activity] Event #%d: %s (aggregate: %s)", p.eventCount, event.EventType, event.AggregateType)
	return nil
}

func main() {
	// Connection string
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "host=localhost port=5432 user=postgres password=postgres dbname=pupsourcing_example sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		//nolint:gocritic // it's just an example code
		log.Fatalf("Failed to ping database: %v", err)
	}

	ctx := context.Background()

	// Create event store
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append sample events
	log.Println("Appending sample events...")
	if err = appendSampleEvents(ctx, db, store); err != nil {
		log.Printf("Warning: Failed to append sample events: %v", err)
	}

	// Create projections
	userCounter := &UserCounterProjection{}
	revenueTracker := &RevenueProjection{}
	activityLog := &ActivityLogProjection{}

	// Configure each projection independently and create processors
	var runners []runner.ConsumerRunner

	config1 := consumer.ProcessorConfig{
		BatchSize:         100,
		PartitionKey:      0,
		TotalPartitions:   1,
		PartitionStrategy: consumer.HashPartitionStrategy{},
	}
	processor1 := postgres.NewProcessor(db, store, &config1)
	runners = append(runners, runner.ConsumerRunner{
		Consumer:  userCounter,
		Processor: processor1,
	})

	config2 := consumer.ProcessorConfig{
		BatchSize:         50, // Different batch size
		PartitionKey:      0,
		TotalPartitions:   1,
		PartitionStrategy: consumer.HashPartitionStrategy{},
	}
	processor2 := postgres.NewProcessor(db, store, &config2)
	runners = append(runners, runner.ConsumerRunner{
		Consumer:  revenueTracker,
		Processor: processor2,
	})

	config3 := consumer.ProcessorConfig{
		BatchSize:         200, // Larger batch for logging
		PartitionKey:      0,
		TotalPartitions:   1,
		PartitionStrategy: consumer.HashPartitionStrategy{},
	}
	processor3 := postgres.NewProcessor(db, store, &config3)
	runners = append(runners, runner.ConsumerRunner{
		Consumer:  activityLog,
		Processor: processor3,
	})

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nReceived shutdown signal, stopping all projections...")
		cancel()
	}()

	// Run all projections concurrently using the runner
	log.Println("Starting multiple projections...")
	log.Println("Press Ctrl+C to stop")
	r := runner.New()
	err = r.Run(ctx, runners)
	if err != nil && !errors.Is(err, context.Canceled) {
		//nolint:gocritic // it's just an example code
		log.Fatalf("Runner error: %v", err)
	}

	log.Println("All projections stopped.")
	log.Printf("Final stats - Users: %d, Revenue: $%.2f, Total events: %d",
		userCounter.count, revenueTracker.total, activityLog.eventCount)
}

func appendSampleEvents(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	// Create some users
	users := []struct {
		user UserCreated
		id   string
	}{
		{UserCreated{Email: "alice@example.com", Name: "Alice"}, uuid.New().String()},
		{UserCreated{Email: "bob@example.com", Name: "Bob"}, uuid.New().String()},
		{UserCreated{Email: "carol@example.com", Name: "Carol"}, uuid.New().String()},
	}

	for _, u := range users {
		payload, err := json.Marshal(u.user)
		if err != nil {
			return err
		}

		events := []es.Event{
			{
				BoundedContext: "Identity",
				AggregateType:  "User",
				AggregateID:    u.id,
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
			return err
		}

		if _, err := store.Append(ctx, tx, es.NoStream(), events); err != nil {
			//nolint:errcheck // Rollback error ignored: transaction already failed
			tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	// Create some orders
	orders := []OrderPlaced{
		{UserID: users[0].id, Amount: 99.99},
		{UserID: users[1].id, Amount: 149.99},
		{UserID: users[0].id, Amount: 49.99},
	}

	for _, order := range orders {
		payload, err := json.Marshal(order)
		if err != nil {
			return err
		}

		events := []es.Event{
			{
				BoundedContext: "Sales",
				AggregateType:  "Order",
				AggregateID:    uuid.New().String(),
				EventID:        uuid.New(),
				EventType:      "OrderPlaced",
				EventVersion:   1,
				Payload:        payload,
				Metadata:       []byte(`{}`),
				CreatedAt:      time.Now(),
			},
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if _, err := store.Append(ctx, tx, es.NoStream(), events); err != nil {
			//nolint:errcheck // Rollback error ignored: transaction already failed
			tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
