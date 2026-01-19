// Package integration_test contains integration tests for the SQLite adapter.
// These tests require SQLite (which is embedded).
//
// Run with: go test -tags=integration ./es/adapters/sqlite/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/sqlite"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/google/uuid"
)

// testProjection is a simple projection for testing
type testProjection struct {
	name   string
	events []es.PersistedEvent
	mu     sync.Mutex
}

func newTestProjection(name string) *testProjection {
	return &testProjection{
		name:   name,
		events: make([]es.PersistedEvent, 0),
	}
}

func (p *testProjection) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *testProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *testProjection) EventCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func TestProcessor_RunModeOneOff(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// Append known number of events
	eventCount := 25
	aggregateID := uuid.New().String()
	events := make([]es.Event, eventCount)
	for i := 0; i < eventCount; i++ {
		events[i] = es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      fmt.Sprintf("Event%d", i+1),
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"num":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// Create test projection
	proj := newTestProjection("test_oneoff")

	// Configure processor in one-off mode
	config := projection.DefaultProcessorConfig()
	config.RunMode = projection.RunModeOneOff
	config.BatchSize = 10

	processor := sqlite.NewProcessor(db, store, &config)

	// Run should exit cleanly after processing all events
	err = processor.Run(ctx, proj)
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode, got: %v", err)
	}

	// Verify all events were processed
	if proj.EventCount() != eventCount {
		t.Errorf("Expected %d events processed, got %d", eventCount, proj.EventCount())
	}

	// Verify checkpoint was saved at the end
	tx2, _ := db.BeginTx(ctx, nil)
	checkpoint, err := store.GetCheckpoint(ctx, tx2, proj.Name())
	tx2.Commit()
	if err != nil {
		t.Fatalf("Failed to get checkpoint: %v", err)
	}
	if checkpoint <= 0 {
		t.Error("Expected checkpoint to be saved")
	}
}

func TestProcessor_RunModeOneOff_EmptyStore(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// No events appended - empty store

	// Create test projection
	proj := newTestProjection("test_oneoff_empty")

	// Configure processor in one-off mode
	config := projection.DefaultProcessorConfig()
	config.RunMode = projection.RunModeOneOff

	processor := sqlite.NewProcessor(db, store, &config)

	// Run should exit cleanly immediately with no events
	err := processor.Run(ctx, proj)
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode with empty store, got: %v", err)
	}

	// Verify no events were processed
	if proj.EventCount() != 0 {
		t.Errorf("Expected 0 events processed, got %d", proj.EventCount())
	}
}

func TestProcessor_RunModeOneOff_PartialBatch(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// Append events less than one batch
	eventCount := 5
	aggregateID := uuid.New().String()
	events := make([]es.Event, eventCount)
	for i := 0; i < eventCount; i++ {
		events[i] = es.Event{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      fmt.Sprintf("Event%d", i+1),
			EventVersion:   1,
			Payload:        []byte(fmt.Sprintf(`{"num":%d}`, i+1)),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		}
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// Create test projection
	proj := newTestProjection("test_oneoff_partial")

	// Configure processor in one-off mode with batch size larger than events
	config := projection.DefaultProcessorConfig()
	config.RunMode = projection.RunModeOneOff
	config.BatchSize = 100

	processor := sqlite.NewProcessor(db, store, &config)

	// Run should exit cleanly after processing partial batch
	err = processor.Run(ctx, proj)
	if err != nil {
		t.Fatalf("Expected nil error in one-off mode, got: %v", err)
	}

	// Verify all events were processed
	if proj.EventCount() != eventCount {
		t.Errorf("Expected %d events processed, got %d", eventCount, proj.EventCount())
	}
}

func TestProcessor_RunModeContinuous_StillWorks(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := sqlite.NewStore(sqlite.DefaultStoreConfig())

	// Append test events
	aggregateID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event1",
			EventVersion:   1,
			Payload:        []byte(`{"id":1}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}
	tx.Commit()

	// Create test projection
	proj := newTestProjection("test_continuous")

	// Configure processor in continuous mode (default)
	config := projection.DefaultProcessorConfig()
	// RunMode defaults to RunModeContinuous

	processor := sqlite.NewProcessor(db, store, &config)

	// Run with timeout - should continue until context cancellation
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err = processor.Run(ctx2, proj)
	// Should get context.DeadlineExceeded or wrapped version
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Errorf("Expected context.DeadlineExceeded, got: %v", err)
		}
	}

	// Verify events were processed
	if proj.EventCount() < 1 {
		t.Errorf("Expected at least 1 event processed, got %d", proj.EventCount())
	}
}
