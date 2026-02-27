// Package main demonstrates dispatcher + runner integration with bounded execution.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/sqlite"
	"github.com/getpup/pupsourcing/es/consumer"
	"github.com/getpup/pupsourcing/es/consumer/runner"
	"github.com/getpup/pupsourcing/es/migrations"
)

type countingProjection struct {
	name      string
	eventType string
	count     int
	mu        sync.Mutex
}

func (p *countingProjection) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *countingProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == p.eventType {
		p.mu.Lock()
		p.count++
		p.mu.Unlock()
	}

	return nil
}

func (p *countingProjection) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

func main() {
	if err := run(1200 * time.Millisecond); err != nil {
		fmt.Printf("Dispatcher example failed: %v\n", err)
		os.Exit(1)
	}
}

func run(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 1200 * time.Millisecond
	}

	db, cleanup, err := setupDatabase()
	if err != nil {
		return fmt.Errorf("setup database: %w", err)
	}
	defer cleanup()

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	if err := appendSampleEvents(ctx, db, store); err != nil {
		return fmt.Errorf("append sample events: %w", err)
	}

	userProjection := &countingProjection{
		name:      "dispatcher_example.user_counter",
		eventType: "UserCreated",
	}

	dispatcher := consumer.NewDispatcher(db, store, nil)

	config1 := consumer.DefaultProcessorConfig()
	config1.WakeupSource = dispatcher
	config1.PollInterval = 250 * time.Millisecond

	runners := []runner.ConsumerRunner{
		{
			Consumer:  userProjection,
			Processor: sqlite.NewProcessor(db, store, &config1),
		},
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dispatcherErrChan := make(chan error, 1)
	go func() {
		dispatcherErrChan <- dispatcher.Run(runCtx)
	}()

	r := runner.New()
	runnerErr := r.Run(runCtx, runners)
	cancel() // Stop dispatcher promptly if runner exits early.
	dispatcherErr := <-dispatcherErrChan

	if !isExpectedStopError(runnerErr) {
		return fmt.Errorf("runner failed: %w", runnerErr)
	}

	if !isExpectedStopError(dispatcherErr) {
		return fmt.Errorf("dispatcher failed: %w", dispatcherErr)
	}

	if userProjection.Count() == 0 {
		return fmt.Errorf("projection did not process expected events (users=%d)", userProjection.Count())
	}

	fmt.Println("Dispatcher + runner example completed successfully.")
	fmt.Printf("User events processed: %d\n", userProjection.Count())
	fmt.Println("Both runner and dispatcher exited cleanly after timeout.")

	return nil
}

func isExpectedStopError(err error) bool {
	if err == nil {
		return true
	}

	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func setupDatabase() (*sql.DB, func(), error) {
	tmpDir, err := os.MkdirTemp("", "pupsourcing-dispatcher-example-*")
	if err != nil {
		return nil, nil, err
	}

	dbFile := filepath.Join(tmpDir, "example.db")
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		//nolint:errcheck // best-effort cleanup
		os.RemoveAll(tmpDir)
		return nil, nil, err
	}

	cleanup := func() {
		db.Close()
		//nolint:errcheck // best-effort cleanup
		os.RemoveAll(tmpDir)
	}

	_, err = db.Exec("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	config := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "init.sql",
		EventsTable:         "events",
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
	}

	err = migrations.GenerateSQLite(&config)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	migrationSQL, err := os.ReadFile(filepath.Join(tmpDir, config.OutputFilename))
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	return db, cleanup, nil
}

func appendSampleEvents(ctx context.Context, db *sql.DB, store *sqlite.Store) error {
	events := []es.Event{
		{
			BoundedContext: "Identity",
			AggregateType:  "User",
			AggregateID:    "user-1",
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Alice"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "Identity",
			AggregateType:  "User",
			AggregateID:    "user-2",
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Bob"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "Sales",
			AggregateType:  "Order",
			AggregateID:    "order-1",
			EventID:        uuid.New(),
			EventType:      "OrderPlaced",
			EventVersion:   1,
			Payload:        []byte(`{"amount":42}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "Sales",
			AggregateType:  "Order",
			AggregateID:    "order-2",
			EventID:        uuid.New(),
			EventType:      "OrderPlaced",
			EventVersion:   1,
			Payload:        []byte(`{"amount":99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	for i := range events {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for event %d: %w", i, err)
		}

		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{events[i]})
		if err != nil {
			//nolint:errcheck // rollback best effort on append failure
			tx.Rollback()
			return fmt.Errorf("append event %d: %w", i, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit event %d: %w", i, err)
		}
	}

	return nil
}
