// Package integration_test contains integration tests for projections.
// These tests require a running PostgreSQL instance.
//
// Run with: go test -tags=integration ./es/projection/integration_test/...
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

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/google/uuid"
)

// globalProjection receives all events
type globalProjection struct {
	name           string
	mu             sync.Mutex
	receivedEvents []es.PersistedEvent
}

func (p *globalProjection) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *globalProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

func (p *globalProjection) getReceivedEvents() []es.PersistedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]es.PersistedEvent, len(p.receivedEvents))
	copy(result, p.receivedEvents)
	return result
}

// scopedProjection only receives specified aggregate types and bounded contexts
type scopedProjection struct {
	name            string
	aggregateTypes  []string
	boundedContexts []string
	mu              sync.Mutex
	receivedEvents  []es.PersistedEvent
}

func (p *scopedProjection) Name() string {
	return p.name
}

func (p *scopedProjection) AggregateTypes() []string {
	return p.aggregateTypes
}

func (p *scopedProjection) BoundedContexts() []string {
	return p.boundedContexts
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *scopedProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

func (p *scopedProjection) getReceivedEvents() []es.PersistedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]es.PersistedEvent, len(p.receivedEvents))
	copy(result, p.receivedEvents)
	return result
}

func TestScopedProjection_GlobalReceivesAllEvents(t *testing.T) {
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
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "UserCreated",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Alice"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "OrderPlaced",
			EventVersion:  1,
			Payload:       []byte(`{"amount":99.99}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "ProductAdded",
			EventVersion:  1,
			Payload:       []byte(`{"sku":"ABC123"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
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

	// Create global projection
	globalProj := &globalProjection{name: "global_test"}
	config := projection.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

	// Run projection for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, globalProj)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify all events were received
	receivedEvents := globalProj.getReceivedEvents()
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

func TestScopedProjection_OnlyReceivesMatchingAggregates(t *testing.T) {
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
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "UserCreated",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Alice"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "User",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "UserUpdated",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Bob"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "OrderPlaced",
			EventVersion:  1,
			Payload:       []byte(`{"amount":99.99}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "ProductAdded",
			EventVersion:  1,
			Payload:       []byte(`{"sku":"ABC123"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
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

	// Create scoped projection that only cares about User events
	scopedProj := &scopedProjection{
		name:            "user_scoped_test",
		aggregateTypes:  []string{"User"},
		boundedContexts: nil, // No filtering by context
	}
	config := projection.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

	// Run projection for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, scopedProj)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify only User events were received
	receivedEvents := scopedProj.getReceivedEvents()
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

func TestScopedProjection_MultipleAggregateTypes(t *testing.T) {
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
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "UserCreated",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Alice"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "OrderPlaced",
			EventVersion:  1,
			Payload:       []byte(`{"amount":99.99}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "ProductAdded",
			EventVersion:  1,
			Payload:       []byte(`{"sku":"ABC123"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Inventory",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "InventoryAdjusted",
			EventVersion:  1,
			Payload:       []byte(`{"quantity":100}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
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

	// Create scoped projection that cares about User and Order events
	scopedProj := &scopedProjection{
		name:            "user_order_scoped_test",
		aggregateTypes:  []string{"User", "Order"},
		boundedContexts: nil, // No filtering by context
	}
	config := projection.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

	// Run projection for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, scopedProj)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify only User and Order events were received
	receivedEvents := scopedProj.getReceivedEvents()
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

func TestScopedProjection_EmptyAggregateTypesReceivesAll(t *testing.T) {
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
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "UserCreated",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Alice"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "OrderPlaced",
			EventVersion:  1,
			Payload:       []byte(`{"amount":99.99}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
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

	// Create scoped projection with empty aggregate types list
	scopedProj := &scopedProjection{
		name:            "empty_scoped_test",
		aggregateTypes:  []string{},
		boundedContexts: nil, // No filtering by context
	}
	config := projection.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

	// Run projection for a short time
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err := processor.Run(ctx2, scopedProj)
	// Accept context deadline or cancellation
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify all events were received (empty list means no filtering)
	receivedEvents := scopedProj.getReceivedEvents()
	if len(receivedEvents) != 2 {
		t.Errorf("Expected 2 events (no filtering), got %d", len(receivedEvents))
	}
}

func TestScopedProjection_MixedProjectionsWorkCorrectly(t *testing.T) {
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
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "UserCreated",
			EventVersion:  1,
			Payload:       []byte(`{"name":"Alice"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Order",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "OrderPlaced",
			EventVersion:  1,
			Payload:       []byte(`{"amount":99.99}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
		},
		{
			BoundedContext: "TestContext",
			AggregateType:  "Product",
			AggregateID:   uuid.New().String(),
			EventID:       uuid.New(),
			EventType:     "ProductAdded",
			EventVersion:  1,
			Payload:       []byte(`{"sku":"ABC123"}`),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now(),
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

	// Create both global and scoped projections
	globalProj := &globalProjection{name: "mixed_global_test"}
	scopedProj := &scopedProjection{
		name:            "mixed_scoped_test",
		aggregateTypes:  []string{"User"},
		boundedContexts: nil, // No filtering by context
	}

	// Run global projection
	config1 := projection.DefaultProcessorConfig()
	processor1 := postgres.NewProcessor(db, store, &config1)

	ctx1, cancel1 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel1()

	err := processor1.Run(ctx1, globalProj)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error from global projection: %v", err)
	}

	// Run scoped projection
	config2 := projection.DefaultProcessorConfig()
	processor2 := postgres.NewProcessor(db, store, &config2)

	ctx2, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()

	err = processor2.Run(ctx2, scopedProj)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Unexpected error from scoped projection: %v", err)
	}

	// Verify global projection received all events
	globalEvents := globalProj.getReceivedEvents()
	if len(globalEvents) != 3 {
		t.Errorf("Global projection: expected 3 events, got %d", len(globalEvents))
	}

	// Verify scoped projection only received User events
	scopedEvents := scopedProj.getReceivedEvents()
	if len(scopedEvents) != 1 {
		t.Errorf("Scoped projection: expected 1 User event, got %d", len(scopedEvents))
	}

	if len(scopedEvents) > 0 && scopedEvents[0].AggregateType != "User" {
		t.Errorf("Scoped projection: expected User event, got %s", scopedEvents[0].AggregateType)
	}
}

func TestScopedProjection_FilterByBoundedContext(t *testing.T) {
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

	// Create scoped projection that only receives Identity context events
	identityProj := &scopedProjection{
		name:           "identity_projection",
		aggregateTypes: nil, // Accept all aggregate types
		boundedContexts: []string{"Identity"},
		receivedEvents: make([]es.PersistedEvent, 0),
	}

	// Create scoped projection for Identity context + User aggregate type
	identityUserProj := &scopedProjection{
		name:            "identity_user_projection",
		aggregateTypes:  []string{"User"},
		boundedContexts: []string{"Identity"},
		receivedEvents:  make([]es.PersistedEvent, 0),
	}

	// Create global projection that receives everything
	globalProj := &globalProjection{
		name:           "global_projection",
		receivedEvents: make([]es.PersistedEvent, 0),
	}

	// Run projections
	config := projection.DefaultProcessorConfig()
	processor := postgres.NewProcessor(db, store, &config)

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
		_ = processor.Run(projCtx, globalProj)
	}()

	wg.Wait()

	// Verify global projection received all 4 events
	globalEvents := globalProj.getReceivedEvents()
	if len(globalEvents) != 4 {
		t.Errorf("Global projection: expected 4 events, got %d", len(globalEvents))
	}

	// Verify Identity-scoped projection received only Identity context events (2 events)
	identityEvents := identityProj.getReceivedEvents()
	if len(identityEvents) != 2 {
		t.Errorf("Identity projection: expected 2 Identity events, got %d", len(identityEvents))
	}
	for _, event := range identityEvents {
		if event.BoundedContext != "Identity" {
			t.Errorf("Identity projection: expected Identity context, got %s", event.BoundedContext)
		}
	}

	// Verify Identity+User scoped projection received only 1 event
	identityUserEvents := identityUserProj.getReceivedEvents()
	if len(identityUserEvents) != 1 {
		t.Errorf("Identity+User projection: expected 1 event, got %d", len(identityUserEvents))
	}
	if len(identityUserEvents) > 0 {
		if identityUserEvents[0].BoundedContext != "Identity" {
			t.Errorf("Identity+User projection: expected Identity context, got %s", identityUserEvents[0].BoundedContext)
		}
		if identityUserEvents[0].AggregateType != "User" {
			t.Errorf("Identity+User projection: expected User aggregate, got %s", identityUserEvents[0].AggregateType)
		}
	}
}
