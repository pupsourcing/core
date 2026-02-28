// Package pupsourcing provides event sourcing capabilities for Go applications.
//
// This package serves as the main entry point for the pupsourcing library.
// For the core event sourcing functionality, see the es package and its subpackages:
//
//	es           - Core types and interfaces
//	es/store     - Event store abstractions
//	es/consumer  - Consumer processing
//	es/worker    - High-level Worker API (recommended for running consumers)
//	es/adapters/postgres - PostgreSQL implementation
//	es/migrations - Migration generation
//
// Quick Start:
//
//  1. Generate migrations:
//     go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
//
//  2. Create store and append events:
//     store := postgres.NewStore(postgres.DefaultStoreConfig())
//     tx, _ := db.BeginTx(ctx, nil)
//     positions, err := store.Append(ctx, tx, events)
//     tx.Commit()
//
//  3. Process events with Worker (recommended):
//     w := postgres.NewWorker(db, store)
//     w.Run(ctx, &MyProjection{}, &OtherProjection{})
//
//     Or use the lower-level Processor API for custom workflows:
//     processor := consumer.NewProcessor(db, store, consumer.DefaultProcessorConfig())
//     processor.Run(ctx, myConsumer)
//
// See the examples directory for complete working examples.
package pupsourcing

// Version returns the current version of the library.
func Version() string {
	return "0.1.0-dev"
}
