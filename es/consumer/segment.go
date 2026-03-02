package consumer

import (
	"time"

	"github.com/pupsourcing/core/es"
)

// DefaultSegments is the default number of segments for segment-based consumer processing.
const DefaultSegments = 16

// SegmentProcessorConfig configures a segment-based consumer processor.
// Segment processors enable auto-scaling consumer processing by pre-creating a fixed
// number of segments that workers dynamically claim and rebalance across instances.
type SegmentProcessorConfig struct {
	// --- Processing ---

	// PartitionStrategy determines which events a segment processes.
	// Default: HashPartitionStrategy{}
	PartitionStrategy PartitionStrategy

	// Logger is an optional logger for observability.
	// If nil, logging is disabled (zero overhead).
	Logger es.Logger

	// WakeupSource is an optional best-effort signal source.
	// If nil, processors rely purely on PollInterval fallback polling.
	WakeupSource WakeupSource

	// --- Segment management ---

	// HeartbeatInterval is how often each worker updates its heartbeat timestamp.
	// Must be significantly less than StaleThreshold.
	// Default: 5s
	HeartbeatInterval time.Duration

	// StaleThreshold is how long a worker's registry entry can go without a
	// heartbeat refresh before it is considered crashed and its segments are
	// eligible for reclaim by other workers. Must be significantly greater
	// than HeartbeatInterval.
	// Default: 30s
	StaleThreshold time.Duration

	// RebalanceInterval is how often workers check segment distribution
	// and perform fair-share rebalancing (claim unclaimed, release excess).
	// Default: 10s
	RebalanceInterval time.Duration

	// PollInterval is the duration to wait when no events are available.
	// Default: 100ms
	PollInterval time.Duration

	// MaxPollInterval is the upper bound for idle fallback polling when backoff is enabled.
	// Default: 5s
	MaxPollInterval time.Duration

	// WakeupJitter is the random delay applied after receiving a wake signal.
	// Default: 25ms
	WakeupJitter time.Duration

	// PollBackoffFactor controls exponential backoff growth for idle fallback polling.
	// Values <= 1 disable growth (constant PollInterval fallback).
	// Default: 2.0
	PollBackoffFactor float64

	// TotalSegments is the number of segments to pre-create for a consumer.
	// This is the upper bound on parallelism. Workers claim subsets of these segments.
	// Default: 16
	TotalSegments int

	// BatchSize is the number of events to read per batch.
	// Default: 100
	BatchSize int

	// RunMode determines processing behavior.
	// Default: RunModeContinuous
	RunMode RunMode
}

// DefaultSegmentProcessorConfig returns the default segment processor configuration.
func DefaultSegmentProcessorConfig() *SegmentProcessorConfig {
	return &SegmentProcessorConfig{
		// Segment management
		TotalSegments:     DefaultSegments,
		HeartbeatInterval: 5 * time.Second,
		StaleThreshold:    30 * time.Second,
		RebalanceInterval: 10 * time.Second,

		// Processing
		BatchSize:         100,
		PartitionStrategy: HashPartitionStrategy{},
		Logger:            nil,
		PollInterval:      100 * time.Millisecond,
		MaxPollInterval:   5 * time.Second,
		PollBackoffFactor: 2.0,
		WakeupJitter:      25 * time.Millisecond,
		WakeupSource:      nil,
		RunMode:           RunModeContinuous,
	}
}
