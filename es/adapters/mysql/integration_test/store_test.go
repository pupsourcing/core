// Package integration_test contains integration tests for the MySQL adapter.
// These tests require a running MySQL/MariaDB instance.
//
// Start MySQL: docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=pupsourcing_test mysql:8
// Run with: go test -tags=integration ./es/adapters/mysql/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/mysql"
	"github.com/pupsourcing/core/es/migrations"
)

func getTestDB(t *testing.T) *sql.DB {
	t.Helper()

	// Default to localhost, but allow override via env var for CI
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("MYSQL_PORT")
	if port == "" {
		port = "3306"
	}

	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}

	password := os.Getenv("MYSQL_PASSWORD")
	if password == "" {
		password = "password"
	}

	dbname := os.Getenv("MYSQL_DATABASE")
	if dbname == "" {
		dbname = "pupsourcing_test"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		user, password, host, port, dbname)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	return db
}

func setupTestTables(t *testing.T, db *sql.DB) {
	t.Helper()

	// Drop existing objects to ensure clean state
	// MySQL requires separate Exec calls for each statement
	_, err := db.Exec(`DROP TABLE IF EXISTS consumer_checkpoints`)
	if err != nil {
		t.Fatalf("Failed to drop consumer_checkpoints table: %v", err)
	}
	_, err = db.Exec(`DROP TABLE IF EXISTS aggregate_heads`)
	if err != nil {
		t.Fatalf("Failed to drop aggregate_heads table: %v", err)
	}
	_, err = db.Exec(`DROP TABLE IF EXISTS events`)
	if err != nil {
		t.Fatalf("Failed to drop events table: %v", err)
	}

	// Generate and execute migration
	tmpDir := t.TempDir()
	config := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "test.sql",
		EventsTable:         "events",
		CheckpointsTable:    "consumer_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
	}

	if err := migrations.GenerateMySQL(&config); err != nil {
		t.Fatalf("Failed to generate migration: %v", err)
	}

	migrationSQL, err := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("Failed to read migration: %v", err)
	}

	// MySQL requires executing statements separately
	// Split by semicolon and execute each statement
	statements := strings.Split(string(migrationSQL), ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Check if this is a comment-only statement
		lines := strings.Split(stmt, "\n")
		hasNonComment := false
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "--") {
				hasNonComment = true
				break
			}
		}

		if !hasNonComment {
			continue
		}

		_, err = db.Exec(stmt)
		if err != nil {
			t.Fatalf("Failed to execute migration statement: %v\nStatement: %s", err, stmt)
		}
	}
}

func TestAppendEvents(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())

	// Create test events
	aggregateID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "TestEventCreated",
			EventVersion:   1,
			Payload:        []byte(`{"test":"data"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "TestEventUpdated",
			EventVersion:   1,
			Payload:        []byte(`{"test":"updated"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	result, err := store.Append(ctx, tx, es.Any(), events)
	if err != nil {
		t.Fatalf("Failed to append events: %v", err)
	}

	if len(result.GlobalPositions) != len(events) {
		t.Errorf("Expected %d positions, got %d", len(events), len(result.GlobalPositions))
	}

	// Verify positions are sequential
	for i := 1; i < len(result.GlobalPositions); i++ {
		if result.GlobalPositions[i] != result.GlobalPositions[i-1]+1 {
			t.Errorf("Positions not sequential: %v", result.GlobalPositions)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
}

func TestAppendEvents_OptimisticConcurrency(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	str := mysql.NewStore(mysql.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	event1 := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventCreated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	// First, append an event successfully to establish version 1
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := str.Append(ctx, tx1, es.NoStream(), []es.Event{event1})
	if err != nil {
		t.Fatalf("First append failed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("First transaction commit failed: %v", err)
	}

	// Now try to manually insert a duplicate version to simulate optimistic concurrency conflict
	event2 := es.Event{
		BoundedContext: "TestContext",
		AggregateType:  "TestAggregate",
		AggregateID:    aggregateID,
		EventID:        uuid.New(),
		EventType:      "TestEventUpdated",
		EventVersion:   1,
		Payload:        []byte(`{}`),
		Metadata:       []byte(`{}`),
		CreatedAt:      time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback() //nolint:errcheck // cleanup

	// Convert EventID to binary
	eventIDBytes, _ := event2.EventID.MarshalBinary()

	// Manually insert with version=1 (which already exists) to trigger unique constraint violation
	_, err = tx2.ExecContext(ctx, `
		INSERT INTO events (
			bounded_context, aggregate_type, aggregate_id, aggregate_version,
			event_id, event_type, event_version,
			payload, metadata, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event2.BoundedContext, event2.AggregateType, event2.AggregateID, int64(1), // Use version 1 which already exists
		eventIDBytes, event2.EventType, event2.EventVersion,
		event2.Payload, event2.Metadata, event2.CreatedAt)

	// The insert should fail immediately with unique constraint violation
	if err == nil {
		t.Fatal("Expected unique constraint violation, got nil")
	}

	// Verify it's the right kind of error
	if !mysql.IsUniqueViolation(err) {
		t.Errorf("Expected unique violation error, got: %v", err)
	}
}

func TestReadEvents(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	mysqlStore := mysql.NewStore(mysql.DefaultStoreConfig())

	// Append some events
	aggregateID1 := uuid.New().String()
	aggregateID2 := uuid.New().String()

	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID1,
			EventID:        uuid.New(),
			EventType:      "Event1",
			EventVersion:   1,
			Payload:        []byte(`{}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID2,
			EventID:        uuid.New(),
			EventType:      "Event2",
			EventVersion:   1,
			Payload:        []byte(`{}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx, _ := db.BeginTx(ctx, nil)
	_, err := mysqlStore.Append(ctx, tx, es.Any(), events[:1])
	if err != nil {
		t.Fatalf("Failed to append first event: %v", err)
	}
	_, err = mysqlStore.Append(ctx, tx, es.Any(), events[1:])
	if err != nil {
		t.Fatalf("Failed to append second event: %v", err)
	}
	tx.Commit()

	// Read events
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback()

	readEvents, err := mysqlStore.ReadEvents(ctx, tx2, 0, 10)
	if err != nil {
		t.Fatalf("Failed to read events: %v", err)
	}

	if len(readEvents) != 2 {
		t.Errorf("Expected 2 events, got %d", len(readEvents))
	}

	// Verify ordering
	if readEvents[0].GlobalPosition >= readEvents[1].GlobalPosition {
		t.Error("Events not ordered by global position")
	}
}

func TestReadAggregateStream_FullStream(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())

	// Create test events for one aggregate
	aggregateID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event1",
			EventVersion:   1,
			Payload:        []byte(`{"data":"1"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event2",
			EventVersion:   1,
			Payload:        []byte(`{"data":"2"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event3",
			EventVersion:   1,
			Payload:        []byte(`{"data":"3"}`),
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

	// Read full stream
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback()

	stream, err := store.ReadAggregateStream(ctx, tx2, "TestContext", "TestAggregate", aggregateID, nil, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if stream.Len() != 3 {
		t.Errorf("Expected 3 events, got %d", stream.Len())
	}

	// Verify events are ordered by aggregate_version
	for i, event := range stream.Events {
		expectedVersion := int64(i + 1)
		if event.AggregateVersion != expectedVersion {
			t.Errorf("Event %d: expected version %d, got %d", i, expectedVersion, event.AggregateVersion)
		}
		if event.AggregateID != aggregateID {
			t.Errorf("Event %d: wrong aggregate ID", i)
		}
		if event.AggregateType != "TestAggregate" {
			t.Errorf("Event %d: wrong aggregate type", i)
		}
	}
}

func TestReadAggregateStream_WithFromVersion(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())

	// Create test events
	aggregateID := uuid.New().String()
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event1",
			EventVersion:   1,
			Payload:        []byte(`{}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event2",
			EventVersion:   1,
			Payload:        []byte(`{}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event3",
			EventVersion:   1,
			Payload:        []byte(`{}`),
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

	// Read from version 2 onwards
	tx2, _ := db.BeginTx(ctx, nil)
	defer tx2.Rollback()

	fromVersion := int64(2)
	stream, err := store.ReadAggregateStream(ctx, tx2, "TestContext", "TestAggregate", aggregateID, &fromVersion, nil)
	if err != nil {
		t.Fatalf("Failed to read aggregate stream: %v", err)
	}

	if stream.Len() != 2 {
		t.Errorf("Expected 2 events, got %d", stream.Len())
	}

	// Verify we got versions 2 and 3
	if len(stream.Events) > 0 && stream.Events[0].AggregateVersion != 2 {
		t.Errorf("First event: expected version 2, got %d", stream.Events[0].AggregateVersion)
	}
	if len(stream.Events) > 1 && stream.Events[1].AggregateVersion != 3 {
		t.Errorf("Second event: expected version 3, got %d", stream.Events[1].AggregateVersion)
	}
}

func TestAggregateVersionTracking(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := mysql.NewStore(mysql.DefaultStoreConfig())

	aggregateID := uuid.New().String()

	// Append first batch of events
	events1 := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event1",
			EventVersion:   1,
			Payload:        []byte(`{}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event2",
			EventVersion:   1,
			Payload:        []byte(`{}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := store.Append(ctx, tx1, es.Any(), events1)
	if err != nil {
		t.Fatalf("First append failed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Verify aggregate_heads has correct version
	var aggVersion int64
	err = db.QueryRowContext(ctx, `
		SELECT aggregate_version 
		FROM aggregate_heads 
		WHERE aggregate_type = ? AND aggregate_id = ?
	`, "TestAggregate", aggregateID).Scan(&aggVersion)
	if err != nil {
		t.Fatalf("Failed to query aggregate_heads: %v", err)
	}
	if aggVersion != 2 {
		t.Errorf("Expected aggregate version 2, got %d", aggVersion)
	}

	// Append second batch of events
	events2 := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "TestAggregate",
			AggregateID:    aggregateID,
			EventID:        uuid.New(),
			EventType:      "Event3",
			EventVersion:   1,
			Payload:        []byte(`{}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = store.Append(ctx, tx2, es.Any(), events2)
	if err != nil {
		t.Fatalf("Second append failed: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Second commit failed: %v", err)
	}

	// Verify aggregate_heads was updated
	err = db.QueryRowContext(ctx, `
		SELECT aggregate_version 
		FROM aggregate_heads 
		WHERE aggregate_type = ? AND aggregate_id = ?
	`, "TestAggregate", aggregateID).Scan(&aggVersion)
	if err != nil {
		t.Fatalf("Failed to query aggregate_heads: %v", err)
	}
	if aggVersion != 3 {
		t.Errorf("Expected aggregate version 3, got %d", aggVersion)
	}
}
