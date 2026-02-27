// Package main demonstrates how to safely scale projections from 1 → N workers.
// This example shows that you can add workers dynamically and they will catch up independently.
//
// Run this example in stages:
//  1. Start with worker 0: WORKER_ID=0 go run main.go
//  2. Add worker 1: WORKER_ID=1 go run main.go (in a new terminal)
//  3. Add worker 2: WORKER_ID=2 go run main.go (in a new terminal)
//  4. Add worker 3: WORKER_ID=3 go run main.go (in a new terminal)
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
)

// UserCreated event
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// ScalableProjection tracks which worker processed each event
type ScalableProjection struct {
	workerID int
	count    int
}

func (p *ScalableProjection) Name() string {
	// All workers share the same projection name
	// but each partition maintains its own checkpoint
	return "scalable_projection"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *ScalableProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.count++
		log.Printf("[Worker %d] Processing: %s (aggregate: %s) - Worker total: %d",
			p.workerID, payload.Name, event.AggregateID[:8], p.count)
	}
	return nil
}

func main() {
	// Configuration
	workerID := flag.Int("worker-id", -1, "Worker ID (0-based)")
	totalWorkers := flag.Int("total-workers", 4, "Total number of workers")
	appendEvents := flag.Bool("append", false, "Append sample events (use with first worker only)")
	flag.Parse()

	// Allow worker ID from environment
	if *workerID == -1 {
		if envID := os.Getenv("WORKER_ID"); envID != "" {
			parsed, err := strconv.Atoi(envID)
			if err != nil {
				log.Fatalf("Invalid WORKER_ID: %v", err)
			}
			*workerID = parsed
		}
	}

	if *workerID < 0 {
		log.Fatal("worker-id is required (use --worker-id flag or WORKER_ID env var)")
	}
	if *workerID >= *totalWorkers {
		log.Fatalf("worker-id (%d) must be less than total-workers (%d)", *workerID, *totalWorkers)
	}

	// Connection
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

	// Optionally append events
	if *appendEvents {
		log.Println("Appending sample events...")
		if appendErr := appendSampleEvents(ctx, db, store, 20); appendErr != nil {
			//nolint:gocritic // it's just an example code
			log.Fatalf("Failed to append sample events: %v", appendErr)
		}
		log.Println("Sample events appended. Start workers now.")
		return
	}

	// Create projection
	proj := &ScalableProjection{workerID: *workerID}

	// Configure with partition info
	config := consumer.DefaultProcessorConfig()
	config.PartitionKey = *workerID
	config.TotalPartitions = *totalWorkers

	processor := postgres.NewProcessor(db, store, &config)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Printf("\n[Worker %d] Received shutdown signal, stopping...", *workerID)
		cancel()
	}()

	// Run projection
	log.Printf("═══════════════════════════════════════════════════════")
	log.Printf("Worker %d of %d starting...", *workerID, *totalWorkers)
	log.Printf("This worker processes events where hash(aggregate_id) %% %d == %d", *totalWorkers, *workerID)
	log.Printf("Press Ctrl+C to stop")
	log.Printf("═══════════════════════════════════════════════════════")

	err = processor.Run(ctx, proj)
	if err != nil && err != context.Canceled {
		log.Fatalf("Projection error: %v", err)
	}

	log.Printf("[Worker %d] Stopped. Processed %d events.", *workerID, proj.count)
}

func appendSampleEvents(ctx context.Context, db *sql.DB, store *postgres.Store, count int) error {
	for i := 1; i <= count; i++ {
		user := UserCreated{
			Email: fmt.Sprintf("user%d@example.com", i),
			Name:  fmt.Sprintf("User %d", i),
		}

		payload, err := json.Marshal(user)
		if err != nil {
			return err
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

	log.Printf("Appended %d sample events", count)
	return nil
}
