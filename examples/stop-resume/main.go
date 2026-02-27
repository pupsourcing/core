// Package main demonstrates that projections can be stopped and resumed without data loss.
// This shows checkpoint reliability - the projection always resumes from where it left off.
//
// Run this example:
//  1. Start PostgreSQL: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_example postgres:16
//  2. Run migrations from the basic example
//  3. Run this example: go run main.go --mode=append (creates events)
//  4. Run this example: go run main.go --mode=process (processes some events, then stop with Ctrl+C)
//  5. Run this example again: go run main.go --mode=process (resumes from checkpoint)
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
	Email     string `json:"email"`
	Name      string `json:"name"`
	Timestamp string `json:"timestamp"`
}

// ReliableProjection tracks checkpoint position explicitly
type ReliableProjection struct {
	lastEventTime time.Time
	count         int
	lastPosition  int64
}

func (p *ReliableProjection) Name() string {
	return "reliable_checkpoint_projection"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *ReliableProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.count++
		p.lastPosition = event.GlobalPosition
		p.lastEventTime = event.CreatedAt

		log.Printf("✓ Processed event #%d (position: %d, aggregate: %s): %s",
			p.count, event.GlobalPosition, event.AggregateID[:8], payload.Name)
	}
	return nil
}

func main() {
	// Configuration
	mode := flag.String("mode", "", "Mode: 'append' to create events, 'process' to run projection, 'status' to check checkpoint")
	numEvents := flag.Int("events", 10, "Number of events to append (used with append mode)")
	flag.Parse()

	if *mode == "" {
		log.Fatal("mode is required: use --mode=append, --mode=process, or --mode=status")
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
	defer db.Close()

	if err = db.Ping(); err != nil {
		//nolint:gocritic // it's just an example code
		log.Fatalf("Failed to ping database: %v", err)
	}

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	switch *mode {
	case "append":
		handleAppendMode(ctx, db, store, *numEvents)
	case "process":
		handleProcessMode(ctx, db, store)
	case "status":
		handleStatusMode(ctx, db)
	default:
		log.Fatalf("Invalid mode: %s (use append, process, or status)", *mode)
	}
}

func handleAppendMode(ctx context.Context, db *sql.DB, store *postgres.Store, numEvents int) {
	log.Printf("Appending %d events...", numEvents)

	for i := 1; i <= numEvents; i++ {
		user := UserCreated{
			Email:     fmt.Sprintf("user%d@example.com", i),
			Name:      fmt.Sprintf("User %d", i),
			Timestamp: time.Now().Format(time.RFC3339),
		}

		payload, err := json.Marshal(user)
		if err != nil {
			log.Fatalf("Failed to marshal: %v", err)
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
			log.Fatalf("Failed to begin tx: %v", err)
		}

		result, err := store.Append(ctx, tx, es.NoStream(), events)
		if err != nil {
			//nolint:errcheck // Rollback error ignored: transaction already failed
			tx.Rollback()
			log.Fatalf("Failed to append: %v", err)
		}

		if err := tx.Commit(); err != nil {
			log.Fatalf("Failed to commit: %v", err)
		}

		log.Printf("  → Event %d appended at position %d", i, result.GlobalPositions[0])
		time.Sleep(100 * time.Millisecond) // Small delay to make it more visible
	}

	log.Printf("\n✓ Successfully appended %d events", numEvents)
	log.Println("\nNow run: go run main.go --mode=status   (to check checkpoint)")
	log.Println("     or: go run main.go --mode=process  (to start processing)")
}

func handleProcessMode(ctx context.Context, db *sql.DB, store *postgres.Store) {
	proj := &ReliableProjection{}
	config := consumer.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

	// Check current checkpoint before starting
	checkpoint := getCurrentCheckpoint(ctx, db, proj.Name())
	if checkpoint > 0 {
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("Resuming from checkpoint: position %d", checkpoint)
		log.Printf("═══════════════════════════════════════════════════════\n")
	} else {
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("Starting fresh (no checkpoint found)")
		log.Printf("═══════════════════════════════════════════════════════\n")
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\n\n═══════════════════════════════════════════════════════")
		log.Println("Received Ctrl+C - Stopping gracefully...")
		log.Println("Current progress will be saved to checkpoint")
		log.Println("═══════════════════════════════════════════════════════")
		cancel()
	}()
	defer cancel()

	// Run projection
	log.Println("Processing events... (Press Ctrl+C to stop)")
	err := processor.Run(ctx, proj)
	if err != nil && !errors.Is(err, context.Canceled) {
		cancel()
		//nolint:gocritic // it's just an example code
		log.Fatalf("Projection error: %v", err)
	}

	// Show final state
	log.Printf("\n═══════════════════════════════════════════════════════")
	log.Printf("Stopped gracefully")
	log.Printf("  Events processed in this run: %d", proj.count)
	if proj.count > 0 {
		log.Printf("  Last position: %d", proj.lastPosition)
		log.Printf("  Last event time: %s", proj.lastEventTime.Format(time.RFC3339))
	}
	log.Printf("═══════════════════════════════════════════════════════")
	log.Println("\nCheckpoint saved! You can safely:")
	log.Println("  1. Append more events: go run main.go --mode=append")
	log.Println("  2. Resume processing: go run main.go --mode=process")
	log.Println("  3. Check status: go run main.go --mode=status")
}

func handleStatusMode(ctx context.Context, db *sql.DB) {
	projectionName := "reliable_checkpoint_projection"

	// Get checkpoint
	checkpoint := getCurrentCheckpoint(ctx, db, projectionName)

	// Get total events
	var totalEvents int64
	err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(global_position), 0) FROM events").Scan(&totalEvents)
	if err != nil {
		log.Fatalf("Failed to get total events: %v", err)
	}

	// Display status
	log.Printf("═══════════════════════════════════════════════════════")
	log.Printf("Projection Status: %s", projectionName)
	log.Printf("═══════════════════════════════════════════════════════")
	log.Printf("Current checkpoint position: %d", checkpoint)
	log.Printf("Total events in store: %d", totalEvents)

	switch {
	case checkpoint == 0:
		log.Printf("Status: Not started (will process all %d events)", totalEvents)
	case checkpoint >= totalEvents:
		log.Printf("Status: Caught up (all events processed)")
	default:
		lag := totalEvents - checkpoint
		log.Printf("Status: Behind by %d events (%.1f%% complete)",
			lag, float64(checkpoint)/float64(totalEvents)*100)
	}
	log.Printf("═══════════════════════════════════════════════════════")
}

func getCurrentCheckpoint(ctx context.Context, db *sql.DB, projectionName string) int64 {
	var checkpoint int64
	err := db.QueryRowContext(ctx,
		"SELECT last_global_position FROM consumer_checkpoints WHERE consumer_name = $1",
		projectionName).Scan(&checkpoint)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalf("Failed to get checkpoint: %v", err)
	}
	return checkpoint
}
