// Package main demonstrates the recommended consumer pattern using the Worker API.
//
// This example shows:
// 1. Two scoped consumers (UserProjection for Identity context, OrderProjection for Sales context)
// 2. One global consumer (AuditLog that processes ALL events)
// 3. Graceful shutdown via signal.NotifyContext
// 4. Zero-config worker usage with production-ready defaults
// 5. Auto-scaling: deploy N instances of this binary, workers automatically claim and rebalance segments
//
// Run this example:
//  1. Start PostgreSQL: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pupsourcing_test postgres:16
//  2. Run migrations from the basic example
//  3. Run this example: DATABASE_URL="postgres://..." go run .
//  4. Scale up: Run multiple instances in separate terminals
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/adapters/postgres"
)

// UserCreated event payload
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// OrderPlaced event payload
type OrderPlaced struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
}

// UserProjection processes User events from the Identity context.
// This is a SCOPED consumer — it only receives events matching its filters.
type UserProjection struct {
	userCount int
}

func (p *UserProjection) Name() string {
	return "user_projection"
}

// AggregateTypes implements consumer.ScopedConsumer to filter by aggregate type
func (p *UserProjection) AggregateTypes() []string {
	return []string{"User"}
}

// BoundedContexts implements consumer.ScopedConsumer to filter by bounded context
func (p *UserProjection) BoundedContexts() []string {
	return []string{"Identity"}
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *UserProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "UserCreated" {
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.userCount++
		log.Printf("[UserProjection] User created: %s (total: %d)", payload.Name, p.userCount)
	}
	return nil
}

// OrderProjection processes Order events from the Sales context.
// This is a SCOPED consumer — it only receives events matching its filters.
type OrderProjection struct {
	orderCount   int
	totalRevenue float64
}

func (p *OrderProjection) Name() string {
	return "order_projection"
}

// AggregateTypes implements consumer.ScopedConsumer to filter by aggregate type
func (p *OrderProjection) AggregateTypes() []string {
	return []string{"Order"}
}

// BoundedContexts implements consumer.ScopedConsumer to filter by bounded context
func (p *OrderProjection) BoundedContexts() []string {
	return []string{"Sales"}
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *OrderProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	if event.EventType == "OrderPlaced" {
		var payload OrderPlaced
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}
		p.orderCount++
		p.totalRevenue += payload.Amount
		log.Printf("[OrderProjection] Order placed: $%.2f (total orders: %d, revenue: $%.2f)",
			payload.Amount, p.orderCount, p.totalRevenue)
	}
	return nil
}

// AuditLog processes ALL events across all contexts.
// This is a GLOBAL consumer — it does NOT implement ScopedConsumer methods.
// Use this pattern for cross-cutting concerns like audit logs, message broker integration, etc.
type AuditLog struct {
	eventCount int
}

func (p *AuditLog) Name() string {
	return "audit_log"
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *AuditLog) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.eventCount++
	log.Printf("[AuditLog] Event %d: %s/%s/%s", p.eventCount,
		event.BoundedContext, event.AggregateType, event.EventType)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Read connection string from environment
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/pupsourcing_test?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	// Create event store
	store := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create a worker with production-ready defaults.
	// Deploy this binary N times — workers auto-scale via segment claiming.
	w := postgres.NewWorker(db, store)

	// For custom configuration, use functional options:
	// w := postgres.NewWorker(db, store,
	//     worker.WithTotalSegments(32),            // More segments = higher parallelism ceiling
	//     worker.WithBatchSize(200),               // Larger batches = fewer DB roundtrips
	//     worker.WithHeartbeatInterval(3*time.Second),
	//     worker.WithLogger(myLogger),             // For observability
	// )

	// Set up graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Create consumers
	userProj := &UserProjection{}
	orderProj := &OrderProjection{}
	auditLog := &AuditLog{}

	fmt.Println("Worker started. Press Ctrl+C to stop.")
	fmt.Println("- UserProjection: processes User events from Identity context")
	fmt.Println("- OrderProjection: processes Order events from Sales context")
	fmt.Println("- AuditLog: processes ALL events (global consumer)")
	fmt.Println()

	// Run all consumers concurrently with automatic segment claiming and rebalancing
	if err := w.Run(ctx, userProj, orderProj, auditLog); err != nil {
		return fmt.Errorf("worker: %w", err)
	}

	fmt.Println("\nWorker stopped gracefully.")
	fmt.Printf("Final stats:\n")
	fmt.Printf("  - Users processed: %d\n", userProj.userCount)
	fmt.Printf("  - Orders processed: %d (revenue: $%.2f)\n", orderProj.orderCount, orderProj.totalRevenue)
	fmt.Printf("  - Events audited: %d\n", auditLog.eventCount)

	return nil
}
