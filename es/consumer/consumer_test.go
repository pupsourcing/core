package consumer

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"

	"github.com/pupsourcing/core/es"
)

// mockGlobalConsumer is a consumer that receives all events
type mockGlobalConsumer struct {
	name           string
	receivedEvents []es.PersistedEvent
}

func (p *mockGlobalConsumer) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *mockGlobalConsumer) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

// mockScopedConsumer is a consumer that only receives specific aggregate types
type mockScopedConsumer struct {
	name            string
	aggregateTypes  []string
	boundedContexts []string
	receivedEvents  []es.PersistedEvent
}

func (p *mockScopedConsumer) Name() string {
	return p.name
}

func (p *mockScopedConsumer) AggregateTypes() []string {
	return p.aggregateTypes
}

func (p *mockScopedConsumer) BoundedContexts() []string {
	return p.boundedContexts
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *mockScopedConsumer) Handle(_ context.Context, _ *sql.Tx, event es.PersistedEvent) error {
	p.receivedEvents = append(p.receivedEvents, event)
	return nil
}

func TestScopedConsumer_Interface(_ *testing.T) {
	// Test that mockScopedConsumer implements both interfaces
	var _ Consumer = &mockScopedConsumer{}
	var _ ScopedConsumer = &mockScopedConsumer{}

	// Test that mockGlobalConsumer implements only Consumer
	var _ Consumer = &mockGlobalConsumer{}
}

func TestScopedConsumer_TypeAssertion(t *testing.T) {
	globalProj := &mockGlobalConsumer{name: "global"}
	scopedProj := &mockScopedConsumer{name: "scoped", aggregateTypes: []string{"User"}}

	// Global consumer should not be a ScopedConsumer
	if _, ok := Consumer(globalProj).(ScopedConsumer); ok {
		t.Error("Global consumer should not implement ScopedConsumer")
	}

	// Scoped consumer should be a ScopedConsumer
	if _, ok := Consumer(scopedProj).(ScopedConsumer); !ok {
		t.Error("Scoped consumer should implement ScopedConsumer")
	}
}

func TestScopedConsumer_EmptyAggregateTypes(t *testing.T) {
	// Test that empty aggregate types list is valid
	scopedProj := &mockScopedConsumer{
		name:           "scoped_empty",
		aggregateTypes: []string{},
	}

	types := scopedProj.AggregateTypes()
	if types == nil {
		t.Error("AggregateTypes should not return nil")
	}
	if len(types) != 0 {
		t.Errorf("Expected empty slice, got %v", types)
	}
}

func TestHashPartitionStrategy_SinglePartition(t *testing.T) {
	strategy := HashPartitionStrategy{}

	// With single partition, all events should be processed
	aggregateID := uuid.New().String()

	if !strategy.ShouldProcess(aggregateID, 0, 1) {
		t.Error("Single partition should process all events")
	}
}

func TestHashPartitionStrategy_MultiplePartitions(t *testing.T) {
	strategy := HashPartitionStrategy{}
	totalPartitions := 4

	// Test that each aggregate ID maps to exactly one partition
	for i := 0; i < 100; i++ {
		aggregateID := uuid.New().String()
		processedBy := 0

		for partition := 0; partition < totalPartitions; partition++ {
			if strategy.ShouldProcess(aggregateID, partition, totalPartitions) {
				processedBy++
			}
		}

		if processedBy != 1 {
			t.Errorf("Aggregate %s processed by %d partitions, expected 1", aggregateID, processedBy)
		}
	}
}

func TestHashPartitionStrategy_Deterministic(t *testing.T) {
	strategy := HashPartitionStrategy{}
	aggregateID := uuid.New().String()
	totalPartitions := 4

	// First call
	var assignedPartition int
	for partition := 0; partition < totalPartitions; partition++ {
		if strategy.ShouldProcess(aggregateID, partition, totalPartitions) {
			assignedPartition = partition
			break
		}
	}

	// Subsequent calls should return same result
	for i := 0; i < 10; i++ {
		if !strategy.ShouldProcess(aggregateID, assignedPartition, totalPartitions) {
			t.Error("Partition assignment is not deterministic")
		}

		// Other partitions should not process this aggregate
		for partition := 0; partition < totalPartitions; partition++ {
			if partition == assignedPartition {
				continue
			}
			if strategy.ShouldProcess(aggregateID, partition, totalPartitions) {
				t.Errorf("Aggregate assigned to multiple partitions")
			}
		}
	}
}

func TestHashPartitionStrategy_Distribution(t *testing.T) {
	strategy := HashPartitionStrategy{}
	totalPartitions := 4
	iterations := 1000

	// Count assignments per partition
	counts := make([]int, totalPartitions)

	for i := 0; i < iterations; i++ {
		aggregateID := uuid.New().String()
		for partition := 0; partition < totalPartitions; partition++ {
			if strategy.ShouldProcess(aggregateID, partition, totalPartitions) {
				counts[partition]++
			}
		}
	}

	// Check that distribution is reasonably even
	// Each partition should get roughly 25% (250 ± 75 for 1000 iterations)
	expectedCount := iterations / totalPartitions
	tolerance := expectedCount / 3 // 33% tolerance

	for partition, count := range counts {
		if count < expectedCount-tolerance || count > expectedCount+tolerance {
			t.Logf("Partition distribution: %v", counts)
			t.Errorf("Partition %d has %d assignments, expected %d ± %d",
				partition, count, expectedCount, tolerance)
		}
	}
}

func TestRunMode_Constants(t *testing.T) {
	// Ensure constants are defined correctly
	if RunModeContinuous != 0 {
		t.Error("RunModeContinuous should be 0 (default)")
	}
	if RunModeOneOff != 1 {
		t.Error("RunModeOneOff should be 1")
	}
}

func TestDefaultSegmentProcessorConfig_RunMode(t *testing.T) {
	config := DefaultSegmentProcessorConfig()
	if config.RunMode != RunModeContinuous {
		t.Errorf("Expected default RunMode to be RunModeContinuous, got %v", config.RunMode)
	}
}
