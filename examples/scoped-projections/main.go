// Package main demonstrates the use of scoped projections vs global projections.
//
// This example shows:
// 1. A scoped read model projection that only processes User events
// 2. A scoped read model projection that only processes Order events
// 3. A global integration projection that receives all events (e.g., for Watermill)
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
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/getpup/pupsourcing/es/projection/runner"
)

// UserCreated event
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// OrderPlaced event
type OrderPlaced struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
}

// ProductAdded event
type ProductAdded struct {
	SKU   string  `json:"sku"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

// UserReadModelProjection is a SCOPED projection that only processes User events.
// It builds a read model for user data, ignoring all other aggregate types.
type UserReadModelProjection struct {
	userCount int
}

func (p *UserReadModelProjection) Name() string {
	return "user_read_model"
}

// AggregateTypes implements ScopedProjection to filter events by aggregate type
func (p *UserReadModelProjection) AggregateTypes() []string {
	return []string{"User"}
}

// BoundedContexts implements ScopedProjection to filter events by bounded context
func (p *UserReadModelProjection) BoundedContexts() []string {
	return []string{"Identity"}
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *UserReadModelProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.userCount++
		log.Printf("[UserReadModel] User created: %s - Total users: %d", payload.Name, p.userCount)
	}
	return nil
}

// OrderReadModelProjection is a SCOPED projection that only processes Order events.
// It builds a read model for order data and revenue tracking.
type OrderReadModelProjection struct {
	orderCount   int
	totalRevenue float64
}

func (p *OrderReadModelProjection) Name() string {
	return "order_read_model"
}

// AggregateTypes implements ScopedProjection to filter events by aggregate type
func (p *OrderReadModelProjection) AggregateTypes() []string {
	return []string{"Order"}
}

// BoundedContexts implements ScopedProjection to filter events by bounded context
func (p *OrderReadModelProjection) BoundedContexts() []string {
	return []string{"Sales"}
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *OrderReadModelProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "OrderPlaced" {
		var payload OrderPlaced
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.orderCount++
		p.totalRevenue += payload.Amount
		log.Printf("[OrderReadModel] Order placed: $%.2f - Total orders: %d, Revenue: $%.2f",
			payload.Amount, p.orderCount, p.totalRevenue)
	}
	return nil
}

// WatermillIntegrationProjection is a GLOBAL projection that receives ALL events.
// This pattern is used for integration with message brokers like Watermill, RabbitMQ, Kafka, etc.
// It does NOT implement ScopedProjection, so it receives every event regardless of aggregate type.
type WatermillIntegrationProjection struct {
	publishedCount int
}

func (p *WatermillIntegrationProjection) Name() string {
	// Convention: system.integration.{integration-name}.v{version}
	return "system.integration.watermill.v1"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *WatermillIntegrationProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	// In a real implementation, this would publish to a message broker
	p.publishedCount++
	log.Printf("[Watermill] Publishing event to message broker: %s/%s (count: %d)",
		event.AggregateType, event.EventType, p.publishedCount)
	// Here you would typically:
	// - Marshal the event to JSON or Protobuf
	// - Publish to Watermill/RabbitMQ/Kafka/etc.
	// - Handle errors and retries
	return nil
}

func main() {
	// Connection string
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
		log.Printf("Failed to ping database: %v", err)
		return
	}

	ctx := context.Background()

	// Create event store
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Append sample events to demonstrate filtering
	log.Println("Appending sample events...")
	if err = appendSampleEvents(ctx, db, store); err != nil {
		log.Printf("Warning: Failed to append sample events: %v", err)
	}

	// Create projections
	userReadModel := &UserReadModelProjection{}
	orderReadModel := &OrderReadModelProjection{}
	watermillIntegration := &WatermillIntegrationProjection{}

	// Configure each projection independently and create processors
	var runners []runner.ProjectionRunner

	config1 := projection.ProcessorConfig{
		BatchSize:         100,
		PartitionKey:      0,
		TotalPartitions:   1,
		PartitionStrategy: projection.HashPartitionStrategy{},
	}
	processor1 := postgres.NewProcessor(db, store, &config1)
	runners = append(runners, runner.ProjectionRunner{
		Projection: userReadModel, // Scoped projection - only receives User events
		Processor:  processor1,
	})

	config2 := projection.ProcessorConfig{
		BatchSize:         100,
		PartitionKey:      0,
		TotalPartitions:   1,
		PartitionStrategy: projection.HashPartitionStrategy{},
	}
	processor2 := postgres.NewProcessor(db, store, &config2)
	runners = append(runners, runner.ProjectionRunner{
		Projection: orderReadModel, // Scoped projection - only receives Order events
		Processor:  processor2,
	})

	config3 := projection.ProcessorConfig{
		BatchSize:         100,
		PartitionKey:      0,
		TotalPartitions:   1,
		PartitionStrategy: projection.HashPartitionStrategy{},
	}
	processor3 := postgres.NewProcessor(db, store, &config3)
	runners = append(runners, runner.ProjectionRunner{
		Projection: watermillIntegration, // Global projection - receives ALL events
		Processor:  processor3,
	})

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nReceived shutdown signal, stopping all projections...")
		cancel()
	}()

	// Run all projections concurrently using the runner
	log.Println("\nStarting projections...")
	log.Println("- UserReadModel: SCOPED to User events only")
	log.Println("- OrderReadModel: SCOPED to Order events only")
	log.Println("- WatermillIntegration: GLOBAL, receives ALL events")
	log.Println("\nPress Ctrl+C to stop")

	r := runner.New()
	err = r.Run(ctx, runners)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("Runner error: %v", err)
		return
	}

	log.Println("\nAll projections stopped.")
	log.Printf("Final stats:")
	log.Printf("  - Users processed: %d (scoped projection)", userReadModel.userCount)
	log.Printf("  - Orders processed: %d (scoped projection)", orderReadModel.orderCount)
	log.Printf("  - Revenue: $%.2f (scoped projection)", orderReadModel.totalRevenue)
	log.Printf("  - Events published: %d (global projection - should equal total events)", watermillIntegration.publishedCount)
}

//nolint:gocyclo // example code with multiple similar operations
func appendSampleEvents(ctx context.Context, db *sql.DB, store *postgres.Store) error {
	// Create some users
	users := []struct {
		user UserCreated
		id   string
	}{
		{UserCreated{Email: "alice@example.com", Name: "Alice"}, uuid.New().String()},
		{UserCreated{Email: "bob@example.com", Name: "Bob"}, uuid.New().String()},
		{UserCreated{Email: "carol@example.com", Name: "Carol"}, uuid.New().String()},
	}

	for _, u := range users {
		payload, err := json.Marshal(u.user)
		if err != nil {
			return err
		}

		events := []es.Event{
			{
				BoundedContext: "Identity",
				AggregateType:  "User",
				AggregateID:    u.id,
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

	// Create some orders
	orders := []OrderPlaced{
		{UserID: users[0].id, Amount: 99.99},
		{UserID: users[1].id, Amount: 149.99},
		{UserID: users[0].id, Amount: 49.99},
	}

	for _, order := range orders {
		payload, err := json.Marshal(order)
		if err != nil {
			return err
		}

		events := []es.Event{
			{
				BoundedContext: "Sales",
				AggregateType:  "Order",
				AggregateID:    uuid.New().String(),
				EventID:        uuid.New(),
				EventType:      "OrderPlaced",
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

	// Create some products (these should only be seen by the global projection)
	products := []ProductAdded{
		{SKU: "WIDGET-001", Name: "Blue Widget", Price: 29.99},
		{SKU: "GADGET-001", Name: "Red Gadget", Price: 39.99},
	}

	for _, product := range products {
		payload, err := json.Marshal(product)
		if err != nil {
			return err
		}

		events := []es.Event{
			{
				BoundedContext: "Catalog",
				AggregateType:  "Product",
				AggregateID:    uuid.New().String(),
				EventID:        uuid.New(),
				EventType:      "ProductAdded",
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

	return nil
}
