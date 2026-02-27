// Package main demonstrates running a single projection with N partitions in the same process.
// This is a "poor man's worker pool" - simple horizontal scaling without multiple processes.
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
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing/es/consumer"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/consumer/runner"
)

// UserCreated event
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// WorkerPoolProjection processes events across multiple goroutines
type WorkerPoolProjection struct {
	totalCount int64 // Use atomic for thread-safe counting
}

func (p *WorkerPoolProjection) Name() string {
	return "worker_pool_projection"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *WorkerPoolProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}

		count := atomic.AddInt64(&p.totalCount, 1)
		log.Printf("[Worker Pool] User created: %s - Total: %d (Partition determined by aggregate ID hash)",
			payload.Name, count)
	}
	return nil
}

func main() {
	// Parse command-line flags
	numWorkers := flag.Int("workers", 4, "Number of worker goroutines")
	flag.Parse()

	if *numWorkers < 1 {
		log.Fatal("workers must be >= 1")
	}

	// Connection string
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "host=localhost port=5432 user=postgres password=postgres dbname=pupsourcing_example sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	if pingErr := db.Ping(); pingErr != nil {
		log.Fatalf("Failed to ping database: %v", pingErr)
	}
	defer db.Close()

	ctx := context.Background()

	// Create event store
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append sample events
	log.Println("Appending sample events...")
	if appendErr := appendSampleEvents(ctx, db, store); appendErr != nil {
		log.Printf("Warning: Failed to append sample events: %v", appendErr)
	}

	// Create a single projection instance
	// Note: We use the same instance for all workers, so state must be thread-safe
	proj := &WorkerPoolProjection{}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nReceived shutdown signal, stopping all workers...")
		cancel()
	}()

	// Run projection with N workers
	log.Printf("Starting projection with %d workers in a single process...", *numWorkers)
	log.Println("Each worker handles events for different aggregate IDs")
	log.Println("Press Ctrl+C to stop")

	// Create runners for each partition
	var runners []runner.ConsumerRunner
	for i := 0; i < *numWorkers; i++ {
		config := consumer.DefaultProcessorConfig()
		config.PartitionKey = i
		config.TotalPartitions = *numWorkers
		processor := postgres.NewProcessor(db, store, &config)
		runners = append(runners, runner.ConsumerRunner{
			Consumer:  proj,
			Processor: processor,
		})
	}

	r := runner.New()
	err = r.Run(ctx, runners)
	if err != nil && !errors.Is(err, context.Canceled) {
		//nolint:gocritic // it's just an example code
		log.Fatalf("Runner error: %v", err)
	}

	log.Printf("All workers stopped. Total events processed: %d", atomic.LoadInt64(&proj.totalCount))
}

func appendSampleEvents(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	// Create many events to demonstrate parallel processing
	users := []UserCreated{
		{Email: "user1@example.com", Name: "User 1"},
		{Email: "user2@example.com", Name: "User 2"},
		{Email: "user3@example.com", Name: "User 3"},
		{Email: "user4@example.com", Name: "User 4"},
		{Email: "user5@example.com", Name: "User 5"},
		{Email: "user6@example.com", Name: "User 6"},
		{Email: "user7@example.com", Name: "User 7"},
		{Email: "user8@example.com", Name: "User 8"},
		{Email: "user9@example.com", Name: "User 9"},
		{Email: "user10@example.com", Name: "User 10"},
		{Email: "user11@example.com", Name: "User 11"},
		{Email: "user12@example.com", Name: "User 12"},
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
