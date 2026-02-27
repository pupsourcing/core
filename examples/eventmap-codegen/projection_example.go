package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/pupsourcing/core/es"
	v1 "github.com/pupsourcing/core/examples/eventmap-codegen/domain/user/events/v1"
	v2 "github.com/pupsourcing/core/examples/eventmap-codegen/domain/user/events/v2"
	"github.com/pupsourcing/core/examples/eventmap-codegen/infrastructure/persistence"
)

// UserProjection is an example projection that demonstrates using FromESEvent
// to convert persisted events to domain events in projection handlers.
type UserProjection struct {
	registeredCount int
	emailChanges    int
	deletedCount    int
}

func (p *UserProjection) Name() string {
	return "user_projection_example"
}

// Handle demonstrates the recommended pattern for using FromESEvent in projections.
// This allows you to work with strongly-typed domain events instead of raw persisted events.
// nolint:gocritic // it's fine
func (p *UserProjection) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	// Convert the persisted event to a domain event using the generated FromESEvent function
	domainEvent, err := persistence.FromESEvent(event)
	if err != nil {
		return fmt.Errorf("failed to convert event: %w", err)
	}

	// Handle the specific event type with type safety
	switch e := domainEvent.(type) {
	case v1.UserRegistered:
		p.registeredCount++
		log.Printf("[v1] User registered: %s (%s) - Total registrations: %d",
			e.Name, e.Email, p.registeredCount)

	case v2.UserRegistered:
		p.registeredCount++
		log.Printf("[v2] User registered: %s (%s) from %s - Total registrations: %d",
			e.Name, e.Email, e.Country, p.registeredCount)

	case v1.UserEmailChanged:
		p.emailChanges++
		log.Printf("[v1] Email changed: %s -> %s - Total changes: %d",
			e.OldEmail, e.NewEmail, p.emailChanges)

	case v1.UserDeleted:
		p.deletedCount++
		log.Printf("[v1] User deleted (reason: %s) - Total deletions: %d",
			e.Reason, p.deletedCount)

	default:
		// Ignore unknown events or log them
		log.Printf("Unknown event type: %T", e)
	}

	return nil
}

// Stats returns the current projection statistics
func (p *UserProjection) Stats() string {
	return fmt.Sprintf("Registrations: %d, Email Changes: %d, Deletions: %d",
		p.registeredCount, p.emailChanges, p.deletedCount)
}
