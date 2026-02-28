// Package integration_test contains integration tests for consumers.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./es/consumer/integration_test/...
//
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
	"github.com/pupsourcing/core/es/consumer"
)

// globalConsumer receives all events
type globalConsumer struct {
	name           string
	mu             sync.Mutex
	receivedEvents []es.PersistedEvent
}

func (p *globalConsumer) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *globalConsumer) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

func (p *globalConsumer) getReceivedEvents() []es.PersistedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]es.PersistedEvent, len(p.receivedEvents))
	copy(result, p.receivedEvents)
	return result
}

// scopedConsumer only receives specified aggregate types and bounded contexts
type scopedConsumer struct {
	name            string
	aggregateTypes  []string
	boundedContexts []string
	mu              sync.Mutex
	receivedEvents  []es.PersistedEvent
}

func (p *scopedConsumer) Name() string {
	return p.name
}

func (p *scopedConsumer) AggregateTypes() []string {
	return p.aggregateTypes
}

func (p *scopedConsumer) BoundedContexts() []string {
	return p.boundedContexts
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *scopedConsumer) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

func (p *scopedConsumer) getReceivedEvents() []es.PersistedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]es.PersistedEvent, len(p.receivedEvents))
	copy(result, p.receivedEvents)
	return result
}

func TestScopedConsumer_GlobalReceivesAllEvents(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events from different aggregate types
	// Each event must be appended separately since they belong to different aggregates
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Alice"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "OrderPlaced",
			EventVersion:   1,
			Payload:        []byte(`{"amount":99.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "ProductAdded",
			EventVersion:   1,
			Payload:        []byte(`{"sku":"ABC123"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Append each event separately since they belong to different aggregates
	for _, event := range events {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
		tx.Commit()
	}

	// Create global consumer
	globalCons := &globalConsumer{name: "global_test"}
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run consumer for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, globalCons)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify all events were received
	receivedEvents := globalCons.getReceivedEvents()
	if len(receivedEvents) != 3 {
		t.Errorf("Expected 3 events, got %d", len(receivedEvents))
	}

	// Verify we got events from all aggregate types
	aggregateTypes := make(map[string]bool)
	for _, event := range receivedEvents {
		aggregateTypes[event.AggregateType] = true
	}

	expectedTypes := []string{"User", "Order", "Product"}
	for _, expectedType := range expectedTypes {
		if !aggregateTypes[expectedType] {
			t.Errorf("Missing events from aggregate type: %s", expectedType)
		}
	}
}

func TestScopedConsumer_OnlyReceivesMatchingAggregates(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events from different aggregate types
	// Each event must be appended separately since they belong to different aggregates
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Alice"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "UserUpdated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Bob"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "OrderPlaced",
			EventVersion:   1,
			Payload:        []byte(`{"amount":99.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "ProductAdded",
			EventVersion:   1,
			Payload:        []byte(`{"sku":"ABC123"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Append each event separately since they belong to different aggregates
	for _, event := range events {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
		tx.Commit()
	}

	// Create scoped consumer that only cares about User events
	scopedCons := &scopedConsumer{
		name:            "user_scoped_test",
		aggregateTypes:  []string{"User"},
		boundedContexts: nil, // No filtering by context
	}
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run consumer for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, scopedCons)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify only User events were received
	receivedEvents := scopedCons.getReceivedEvents()
	if len(receivedEvents) != 2 {
		t.Errorf("Expected 2 User events, got %d", len(receivedEvents))
	}

	// Verify all received events are User events
	for _, event := range receivedEvents {
		if event.AggregateType != "User" {
			t.Errorf("Expected only User events, got %s", event.AggregateType)
		}
	}
}

func TestScopedConsumer_MultipleAggregateTypes(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events from different aggregate types
	// Each event must be appended separately since they belong to different aggregates
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Alice"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "OrderPlaced",
			EventVersion:   1,
			Payload:        []byte(`{"amount":99.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "ProductAdded",
			EventVersion:   1,
			Payload:        []byte(`{"sku":"ABC123"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Inventory",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "InventoryAdjusted",
			EventVersion:   1,
			Payload:        []byte(`{"quantity":100}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Append each event separately since they belong to different aggregates
	for _, event := range events {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
		tx.Commit()
	}

	// Create scoped consumer that cares about User and Order events
	scopedCons := &scopedConsumer{
		name:            "user_order_scoped_test",
		aggregateTypes:  []string{"User", "Order"},
		boundedContexts: nil, // No filtering by context
	}
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run consumer for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, scopedCons)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify only User and Order events were received
	receivedEvents := scopedCons.getReceivedEvents()
	if len(receivedEvents) != 2 {
		t.Errorf("Expected 2 events (User and Order), got %d", len(receivedEvents))
	}

	// Verify all received events are User or Order events
	for _, event := range receivedEvents {
		if event.AggregateType != "User" && event.AggregateType != "Order" {
			t.Errorf("Expected only User or Order events, got %s", event.AggregateType)
		}
	}
}

func TestScopedConsumer_EmptyAggregateTypesReceivesAll(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events from different aggregate types
	// Each event must be appended separately since they belong to different aggregates
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Alice"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "OrderPlaced",
			EventVersion:   1,
			Payload:        []byte(`{"amount":99.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Append each event separately since they belong to different aggregates
	for _, event := range events {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
		tx.Commit()
	}

	// Create scoped consumer with empty aggregate types list
	scopedCons := &scopedConsumer{
		name:            "empty_scoped_test",
		aggregateTypes:  []string{},
		boundedContexts: nil, // No filtering by context
	}
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	processor := postgres.NewSegmentProcessor(db, store, config)

	// Run consumer for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, scopedCons)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify all events were received (empty list means no filtering)
	receivedEvents := scopedCons.getReceivedEvents()
	if len(receivedEvents) != 2 {
		t.Errorf("Expected 2 events (no filtering), got %d", len(receivedEvents))
	}
}

func TestScopedConsumer_MixedConsumersWorkCorrectly(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events from different aggregate types
	// Each event must be appended separately since they belong to different aggregates
	events := []es.Event{
		{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Alice"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "OrderPlaced",
			EventVersion:   1,
			Payload:        []byte(`{"amount":99.99}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "ProductAdded",
			EventVersion:   1,
			Payload:        []byte(`{"sku":"ABC123"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Append each event separately since they belong to different aggregates
	for _, event := range events {
		tx, _ := db.BeginTx(ctx, nil)
		_, err := store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			t.Fatalf("Failed to append event: %v", err)
		}
		tx.Commit()
	}

	// Create both global and scoped consumers
	globalCons := &globalConsumer{name: "mixed_global_test"}
	scopedCons := &scopedConsumer{
		name:            "mixed_scoped_test",
		aggregateTypes:  []string{"User"},
		boundedContexts: nil, // No filtering by context
	}

	// Run global consumer
	config1 := consumer.DefaultSegmentProcessorConfig()
	config1.TotalSegments = 1
	processor1 := postgres.NewSegmentProcessor(db, store, config1)

	ctx1, cancel1 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel1()

	err := processor1.Run(ctx1, globalCons)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error from global consumer: %v", err)
	}

	// Run scoped consumer
	config2 := consumer.DefaultSegmentProcessorConfig()
	config2.TotalSegments = 1
	processor2 := postgres.NewSegmentProcessor(db, store, config2)

	ctx2, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()

	err = processor2.Run(ctx2, scopedCons)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error from scoped consumer: %v", err)
	}

	// Verify global consumer received all events
	globalEvents := globalCons.getReceivedEvents()
	if len(globalEvents) != 3 {
		t.Errorf("Global consumer: expected 3 events, got %d", len(globalEvents))
	}

	// Verify scoped consumer only received User events
	scopedEvents := scopedCons.getReceivedEvents()
	if len(scopedEvents) != 1 {
		t.Errorf("Scoped consumer: expected 1 User event, got %d", len(scopedEvents))
	}

	if len(scopedEvents) > 0 && scopedEvents[0].AggregateType != "User" {
		t.Errorf("Scoped consumer: expected User event, got %s", scopedEvents[0].AggregateType)
	}
}

func TestScopedConsumer_FilterByBoundedContext(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append events from different bounded contexts and aggregate types
	events := []es.Event{
		// Identity context - User
		{
			BoundedContext: "Identity",
			AggregateType:  "User",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "UserCreated",
			EventVersion:   1,
			Payload:        []byte(`{"email":"user1@example.com"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		// Identity context - Profile
		{
			BoundedContext: "Identity",
			AggregateType:  "Profile",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "ProfileCreated",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Alice"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		// Billing context - Subscription
		{
			BoundedContext: "Billing",
			AggregateType:  "Subscription",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "SubscriptionCreated",
			EventVersion:   1,
			Payload:        []byte(`{"plan":"premium"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
		// Catalog context - Product
		{
			BoundedContext: "Catalog",
			AggregateType:  "Product",
			AggregateID:    uuid.New().String(),
			EventID:        uuid.New(),
			EventType:      "ProductAdded",
			EventVersion:   1,
			Payload:        []byte(`{"name":"Widget"}`),
			Metadata:       []byte(`{}`),
			CreatedAt:      time.Now(),
		},
	}

	// Append each event separately since they belong to different aggregates
	for _, event := range events {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		_, err = store.Append(ctx, tx, es.NoStream(), []es.Event{event})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to append event: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	}

	// Create scoped consumer that only receives Identity context events
	identityProj := &scopedConsumer{
		name:            "identity_consumer",
		aggregateTypes:  nil, // Accept all aggregate types
		boundedContexts: []string{"Identity"},
		receivedEvents:  make([]es.PersistedEvent, 0),
	}

	// Create scoped consumer for Identity context + User aggregate type
	identityUserProj := &scopedConsumer{
		name:            "identity_user_consumer",
		aggregateTypes:  []string{"User"},
		boundedContexts: []string{"Identity"},
		receivedEvents:  make([]es.PersistedEvent, 0),
	}

	// Create global consumer that receives everything
	globalCons := &globalConsumer{
		name:           "global_consumer",
		receivedEvents: make([]es.PersistedEvent, 0),
	}

	// Run consumers
	config := consumer.DefaultSegmentProcessorConfig()
	config.TotalSegments = 1
	processor := postgres.NewSegmentProcessor(db, store, config)

	projCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		_ = processor.Run(projCtx, identityProj)
	}()

	go func() {
		defer wg.Done()
		_ = processor.Run(projCtx, identityUserProj)
	}()

	go func() {
		defer wg.Done()
		_ = processor.Run(projCtx, globalCons)
	}()

	wg.Wait()

	// Verify global consumer received all 4 events
	globalEvents := globalCons.getReceivedEvents()
	if len(globalEvents) != 4 {
		t.Errorf("Global consumer: expected 4 events, got %d", len(globalEvents))
	}

	// Verify Identity-scoped consumer received only Identity context events (2 events)
	identityEvents := identityProj.getReceivedEvents()
	if len(identityEvents) != 2 {
		t.Errorf("Identity consumer: expected 2 Identity events, got %d", len(identityEvents))
	}
	for _, event := range identityEvents {
		if event.BoundedContext != "Identity" {
			t.Errorf("Identity consumer: expected Identity context, got %s", event.BoundedContext)
		}
	}

	// Verify Identity+User scoped consumer received only 1 event
	identityUserEvents := identityUserProj.getReceivedEvents()
	if len(identityUserEvents) != 1 {
		t.Errorf("Identity+User consumer: expected 1 event, got %d", len(identityUserEvents))
	}
	if len(identityUserEvents) > 0 {
		if identityUserEvents[0].BoundedContext != "Identity" {
			t.Errorf("Identity+User consumer: expected Identity context, got %s", identityUserEvents[0].BoundedContext)
		}
		if identityUserEvents[0].AggregateType != "User" {
			t.Errorf("Identity+User consumer: expected User aggregate, got %s", identityUserEvents[0].AggregateType)
		}
	}
}
