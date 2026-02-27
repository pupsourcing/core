// Package main demonstrates horizontal scaling with partitioning across multiple processes.
// This shows how to safely run the same binary multiple times with different partition keys.
//
// Run this example:
//  1. Start PostgreSQL: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_example postgres:16
//  2. Run migrations from the basic example
//  3. Run multiple instances with different partition keys:
//     Terminal 1: PARTITION_KEY=0 go run main.go
//     Terminal 2: PARTITION_KEY=1 go run main.go
//     Terminal 3: PARTITION_KEY=2 go run main.go
//     Terminal 4: PARTITION_KEY=3 go run main.go
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
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
)

// UserCreated is a sample event payload
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// PartitionedProjection processes events for a specific partition
type PartitionedProjection struct {
	partitionKey int
	count        int
}

func (p *PartitionedProjection) Name() string {
	return "partitioned_user_counter"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *PartitionedProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.count++
		log.Printf("[Partition %d] User created: %s (%s) - Partition total: %d",
			p.partitionKey, payload.Name, payload.Email, p.count)
	}
	return nil
}

func main() {
	// Parse command-line flags
	partitionKey := flag.Int("partition-key", -1, "Partition key (0-based index)")
	totalPartitions := flag.Int("total-partitions", 4, "Total number of partitions")
	flag.Parse()

	// Allow partition key to be set via environment variable
	if *partitionKey == -1 {
		if envKey := os.Getenv("PARTITION_KEY"); envKey != "" {
			parsed, err := strconv.Atoi(envKey)
			if err != nil {
				log.Fatalf("Invalid PARTITION_KEY: %v", err)
			}
			*partitionKey = parsed
		}
	}

	// Validate configuration
	if *partitionKey < 0 {
		log.Fatal("partition-key is required (use --partition-key flag or PARTITION_KEY env var)")
	}
	if *partitionKey >= *totalPartitions {
		log.Fatalf("partition-key (%d) must be less than total-partitions (%d)", *partitionKey, *totalPartitions)
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

	// Append some sample events (only from partition 0 to avoid duplicates)
	if *partitionKey == 0 {
		log.Println("Appending sample events...")
		if appendErr := appendSampleEvents(ctx, db, store); appendErr != nil {
			log.Printf("Warning: Failed to append sample events: %v", appendErr)
		}
	}

	// Create projection for this partition
	proj := &PartitionedProjection{partitionKey: *partitionKey}

	// Configure processor with partitioning
	config := consumer.DefaultProcessorConfig()
	config.PartitionKey = *partitionKey
	config.TotalPartitions = *totalPartitions

	processor := postgres.NewProcessor(db, store, &config)

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Printf("\n[Partition %d] Received shutdown signal, stopping...", *partitionKey)
		cancel()
	}()

	// Run projection
	log.Printf("Starting projection on partition %d of %d...", *partitionKey, *totalPartitions)
	log.Println("Press Ctrl+C to stop")
	err = processor.Run(ctx, proj)
	if err != nil && !errors.Is(err, context.Canceled) {
		//nolint:gocritic // it's just an example code
		log.Fatalf("Projection error: %v", err)
	}

	log.Printf("[Partition %d] Stopped. Processed %d events on this partition.", *partitionKey, proj.count)
}

func appendSampleEvents(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	// Create more events to better demonstrate partitioning
	users := []UserCreated{
		{Email: "alice@example.com", Name: "Alice Smith"},
		{Email: "bob@example.com", Name: "Bob Johnson"},
		{Email: "carol@example.com", Name: "Carol Williams"},
		{Email: "dave@example.com", Name: "Dave Brown"},
		{Email: "eve@example.com", Name: "Eve Davis"},
		{Email: "frank@example.com", Name: "Frank Miller"},
		{Email: "grace@example.com", Name: "Grace Wilson"},
		{Email: "henry@example.com", Name: "Henry Moore"},
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
