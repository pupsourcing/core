package postgres

import (
	"github.com/pupsourcing/core/es"
	"github.com/pupsourcing/core/es/consumer"
)

// segmentProcessPlan captures per-consumer processing settings that stay constant
// for the lifetime of a processor run.
type segmentProcessPlan struct {
	consumer  consumer.Consumer
	name      string
	readScope ReadEventsScope
}

func newSegmentProcessPlan(cons consumer.Consumer) *segmentProcessPlan {
	return &segmentProcessPlan{
		consumer:  cons,
		name:      cons.Name(),
		readScope: buildReadEventsScope(cons),
	}
}

func buildReadEventsScope(cons consumer.Consumer) ReadEventsScope {
	scopedConsumer, ok := cons.(consumer.ScopedConsumer)
	if !ok {
		return ReadEventsScope{}
	}

	scope := ReadEventsScope{}
	if types := scopedConsumer.AggregateTypes(); len(types) > 0 {
		scope.AggregateTypes = append([]string(nil), types...)
	}
	if contexts := scopedConsumer.BoundedContexts(); len(contexts) > 0 {
		scope.BoundedContexts = append([]string(nil), contexts...)
	}

	return scope
}

// shouldProcessEventForSegment applies only partition routing.
// Aggregate-type and bounded-context scope is already pushed into SQL reads.
//
//nolint:gocritic // hugeParam: Intentionally pass by value to match event processing pattern
func (p *SegmentProcessor) shouldProcessEventForSegment(event es.PersistedEvent, segmentID int) bool {
	return p.config.PartitionStrategy.ShouldProcess(
		event.AggregateID,
		segmentID,
		p.config.TotalSegments,
	)
}
