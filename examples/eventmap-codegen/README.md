# Event Mapping Code Generation Example

This example demonstrates how to use the `eventmap-gen` tool to generate type-safe mapping code between domain events and event sourcing types.

## Directory Structure

```
eventmap-codegen/
├── domain/
│   └── user/
│       └── events/
│           ├── v1/
│           │   └── user_events.go    # Initial event versions
│           └── v2/
│               └── user_events.go    # Evolved UserRegistered event
├── infrastructure/
│   └── persistence/
│       └── (generated code goes here)
└── main.go                           # Example usage
```

## Running the Example

### 1. Generate the Mapping Code

From the repository root:

```bash
go run ./cmd/eventmap-gen \
  -input ./examples/eventmap-codegen/domain/user/events \
  -output ./examples/eventmap-codegen/infrastructure/persistence \
  -package persistence \
  -module github.com/pupsourcing/core/examples/eventmap-codegen/domain/user/events
```

This will create `event_mapping.gen.go` in the infrastructure/persistence directory.

### 2. Run the Example

```bash
go run ./examples/eventmap-codegen
```

## What This Example Shows

1. **Pure Domain Events** - Events in `domain/user/events` have no dependencies on infrastructure
2. **Versioned Events** - `UserRegistered` exists in both v1 and v2 with different schemas
3. **Type-Safe Mapping with Generics** - Generated code provides compile-time safety with generic type parameters
4. **Round-Trip Conversion** - Domain → ES → Domain conversion maintains data integrity
5. **Options Pattern** - Metadata injection (CausationID, CorrelationID, TraceID)
6. **Automatic Test Generation** - Unit tests are generated alongside the mapping code

## Key Takeaways

- Domain events are pure Go structs with JSON tags
- Version directories (v1/, v2/) determine EventVersion
- Event type (e.g., "UserRegistered") is constant across versions
- Generated code handles serialization, deserialization, and version routing
- No runtime reflection - everything is explicit

## Clean Architecture

This example follows clean architecture principles:

- **Domain Layer**: Pure business logic, no infrastructure dependencies
- **Infrastructure Layer**: Generated mapping code, depends on both domain and pupsourcing
- **Clear Boundaries**: Domain events remain testable in isolation

## Schema Evolution

The example shows how to handle breaking changes:

1. Create new version directory (v2/)
2. Add evolved event struct with new fields
3. Regenerate mapping code
4. Both versions remain available for replaying historical events

Old events (v1) and new events (v2) can coexist in the same event stream!
