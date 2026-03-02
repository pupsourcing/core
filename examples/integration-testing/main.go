package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/migrations"
	"github.com/pupsourcing/core/es/worker"
)

// UserProjection is a simple consumer that counts user events.
type UserProjection struct {
	userCount int
}

func (p *UserProjection) Name() string {
	return "user_projection"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *UserProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.AggregateType == "User" && event.EventType == "UserCreated" {
		p.userCount++
	}
	return nil
}

func (p *UserProjection) GetCount() int {
	return p.userCount
}

func main() {
	fmt.Println("Integration Testing Example - One-Off Consumer Mode")
	fmt.Println("====================================================")
	fmt.Println()
	fmt.Println("This example shows how to use RunModeOneOff for synchronous consumer testing.")
	fmt.Println("See main_test.go for a complete integration test example.")
	fmt.Println()

	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println()
	fmt.Println("Run 'go test -v' to see the full integration test example.")
}

func run() error {
	db, err := setupDatabase()
	if err != nil {
		return fmt.Errorf("failed to setup database: %w", err)
	}
	defer db.Close()

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	fmt.Println("Appending 5 test events...")

	if err := appendTestEvents(ctx, db, store, 5); err != nil {
		return fmt.Errorf("failed to append events: %w", err)
	}

	// Process with one-off mode using the Worker API
	proj := &UserProjection{}
	w := postgres.NewWorker(db, store,
		worker.WithTotalSegments(1),
		worker.WithRunMode(consumer.RunModeOneOff),
	)

	fmt.Println("Processing events in one-off mode...")
	if err := w.Run(ctx, proj); err != nil {
		return fmt.Errorf("consumer processing failed: %w", err)
	}

	fmt.Printf("✓ Processed %d user events\n", proj.GetCount())
	fmt.Println("✓ Consumer exited cleanly after catching up")

	return nil
}

func setupDatabase() (*sql.DB, error) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "host=localhost port=5432 user=postgres password=postgres dbname=pupsourcing_test sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Generate and execute migrations
	tmpDir := os.TempDir()
	migrationFilename := fmt.Sprintf("migration_%d.sql", time.Now().UnixNano())

	config := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      migrationFilename,
		EventsTable:         "events",
		AggregateHeadsTable: "aggregate_heads",
		SegmentsTable:       "consumer_segments",
		WorkerRegistryTable: "consumer_workers",
	}

	if genErr := migrations.GeneratePostgres(&config); genErr != nil {
		return nil, genErr
	}

	migrationFile := fmt.Sprintf("%s/%s", tmpDir, migrationFilename)
	migrationSQL, readErr := os.ReadFile(migrationFile)
	if readErr != nil {
		return nil, readErr
	}

	_, execErr := db.Exec(string(migrationSQL))
	if execErr != nil {
		return nil, execErr
	}

	//nolint:errcheck // cleanup error is not critical
	os.Remove(migrationFile)

	return db, nil
}

func appendTestEvents(ctx context.Context, db *sql.DB, store *postgres.Store, count int) error {
	for i := 0; i < count; i++ {
		event := es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    fmt.Sprintf("user-%d", i+1),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"id":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}

		tx, txErr := db.BeginTx(ctx, nil)
		if txErr != nil {
			return fmt.Errorf("failed to begin transaction for event %d: %w", i+1, txErr)
		}
		_, appendErr := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if appendErr != nil {
			//nolint:errcheck // rollback error is not critical here
			tx.Rollback()
			return fmt.Errorf("failed to append event %d: %w", i+1, appendErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return fmt.Errorf("failed to commit event %d: %w", i+1, commitErr)
		}
	}
	return nil
}
