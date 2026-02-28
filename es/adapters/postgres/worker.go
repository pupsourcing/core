package postgres

import (
	"database/sql"

	"github.com/pupsourcing/core/es/consumer"
	"github.com/pupsourcing/core/es/worker"
)

// NewWorker creates a Worker backed by a PostgreSQL SegmentProcessor.
// The Worker manages segment-based consumer processing with automatic
// dispatcher lifecycle. Deploy the same binary N times for auto-scaling.
func NewWorker(db *sql.DB, store *Store, opts ...worker.Option) *worker.Worker {
	return worker.New(db, store, func(cfg *consumer.SegmentProcessorConfig) consumer.ProcessorRunner {
		return NewSegmentProcessor(db, store, cfg)
	}, opts...)
}
