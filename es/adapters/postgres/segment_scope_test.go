package postgres

import (
	"context"
	"database/sql"
	"slices"
	"testing"

	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
)

type noopTestConsumer struct {
	name string
}

func (c *noopTestConsumer) Name() string {
	return c.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to match Consumer interface
func (c *noopTestConsumer) Handle(_ context.Context, _ *sql.Tx, _ es.PersistedEvent) error {
	return nil
}

type scopedPlanTestConsumer struct {
	*noopTestConsumer
	aggregateTypes      []string
	boundedContexts     []string
	aggregateTypeCalls  int
	boundedContextCalls int
}

func (c *scopedPlanTestConsumer) AggregateTypes() []string {
	c.aggregateTypeCalls++
	return c.aggregateTypes
}

func (c *scopedPlanTestConsumer) BoundedContexts() []string {
	c.boundedContextCalls++
	return c.boundedContexts
}

type partitionStrategySpy struct {
	result          bool
	callCount       int
	aggregateID     string
	partitionKey    int
	totalPartitions int
}

func (s *partitionStrategySpy) ShouldProcess(aggregateID string, partitionKey, totalPartitions int) bool {
	s.callCount++
	s.aggregateID = aggregateID
	s.partitionKey = partitionKey
	s.totalPartitions = totalPartitions
	return s.result
}

func TestNewSegmentProcessPlan_UnscopedConsumer(t *testing.T) {
	t.Parallel()

	cons := &noopTestConsumer{name: "plain"}

	plan := newSegmentProcessPlan(cons)

	if plan.name != cons.name {
		t.Fatalf("plan.name = %q, want %q", plan.name, cons.name)
	}
	if plan.consumer != cons {
		t.Fatalf("plan.consumer did not keep the original consumer")
	}
	if len(plan.readScope.AggregateTypes) != 0 {
		t.Fatalf("plan.readScope.AggregateTypes = %v, want empty", plan.readScope.AggregateTypes)
	}
	if len(plan.readScope.BoundedContexts) != 0 {
		t.Fatalf("plan.readScope.BoundedContexts = %v, want empty", plan.readScope.BoundedContexts)
	}
}

func TestNewSegmentProcessPlan_ScopedConsumerBuildsScopeOnce(t *testing.T) {
	t.Parallel()

	cons := &scopedPlanTestConsumer{
		noopTestConsumer: &noopTestConsumer{name: "scoped"},
		aggregateTypes:   []string{"Order", "Invoice"},
		boundedContexts:  []string{"Billing", "Ledger"},
	}

	plan := newSegmentProcessPlan(cons)

	if cons.aggregateTypeCalls != 1 {
		t.Fatalf("AggregateTypes() calls = %d, want 1", cons.aggregateTypeCalls)
	}
	if cons.boundedContextCalls != 1 {
		t.Fatalf("BoundedContexts() calls = %d, want 1", cons.boundedContextCalls)
	}
	if !slices.Equal(plan.readScope.AggregateTypes, cons.aggregateTypes) {
		t.Fatalf("AggregateTypes scope = %v, want %v", plan.readScope.AggregateTypes, cons.aggregateTypes)
	}
	if !slices.Equal(plan.readScope.BoundedContexts, cons.boundedContexts) {
		t.Fatalf("BoundedContexts scope = %v, want %v", plan.readScope.BoundedContexts, cons.boundedContexts)
	}

	cons.aggregateTypes[0] = "Mutated"
	cons.boundedContexts[0] = "Changed"

	if !slices.Equal(plan.readScope.AggregateTypes, []string{"Order", "Invoice"}) {
		t.Fatalf("AggregateTypes scope was not copied: %v", plan.readScope.AggregateTypes)
	}
	if !slices.Equal(plan.readScope.BoundedContexts, []string{"Billing", "Ledger"}) {
		t.Fatalf("BoundedContexts scope was not copied: %v", plan.readScope.BoundedContexts)
	}
}

func TestShouldProcessEventForSegmentUsesPartitionStrategyOnly(t *testing.T) {
	t.Parallel()

	strategy := &partitionStrategySpy{result: true}
	proc := &SegmentProcessor{
		config: &consumer.SegmentProcessorConfig{
			PartitionStrategy: strategy,
			TotalSegments:     8,
		},
	}

	event := es.PersistedEvent{
		AggregateID:    "agg-42",
		AggregateType:  "IgnoredBySQLScope",
		BoundedContext: "AlsoIgnoredInMemory",
	}

	if !proc.shouldProcessEventForSegment(event, 3) {
		t.Fatalf("shouldProcessEventForSegment() = false, want true")
	}
	if strategy.callCount != 1 {
		t.Fatalf("PartitionStrategy.ShouldProcess() calls = %d, want 1", strategy.callCount)
	}
	if strategy.aggregateID != event.AggregateID {
		t.Fatalf("aggregateID = %q, want %q", strategy.aggregateID, event.AggregateID)
	}
	if strategy.partitionKey != 3 {
		t.Fatalf("partitionKey = %d, want 3", strategy.partitionKey)
	}
	if strategy.totalPartitions != 8 {
		t.Fatalf("totalPartitions = %d, want 8", strategy.totalPartitions)
	}

	strategy.result = false
	if proc.shouldProcessEventForSegment(event, 3) {
		t.Fatalf("shouldProcessEventForSegment() = true, want false")
	}
}
