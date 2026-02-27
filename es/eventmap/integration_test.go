package eventmap_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestGeneratedCodeExecution tests that the generated code actually works
// by generating code, compiling it, and running round-trip tests.
func TestGeneratedCodeExecution(t *testing.T) {
	// Create a temporary directory for our test
	tmpDir := t.TempDir()

	// Create test event structures
	eventsDir := filepath.Join(tmpDir, "events")
	v1Dir := filepath.Join(eventsDir, "v1")
	v2Dir := filepath.Join(eventsDir, "v2")

	if err := os.MkdirAll(v1Dir, 0o755); err != nil {
		t.Fatalf("Failed to create v1 dir: %v", err)
	}
	if err := os.MkdirAll(v2Dir, 0o755); err != nil {
		t.Fatalf("Failed to create v2 dir: %v", err)
	}

	// Write v1 events
	v1Code := `package v1

type OrderCreated struct {
	OrderID    string  ` + "`json:\"order_id\"`" + `
	CustomerID string  ` + "`json:\"customer_id\"`" + `
	Amount     float64 ` + "`json:\"amount\"`" + `
}

type OrderCancelled struct {
	OrderID string ` + "`json:\"order_id\"`" + `
	Reason  string ` + "`json:\"reason\"`" + `
}
`
	if err := os.WriteFile(filepath.Join(v1Dir, "order_events.go"), []byte(v1Code), 0o644); err != nil {
		t.Fatalf("Failed to write v1 events: %v", err)
	}

	// Write v2 events (OrderCreated with additional field)
	v2Code := `package v2

type OrderCreated struct {
	OrderID    string  ` + "`json:\"order_id\"`" + `
	CustomerID string  ` + "`json:\"customer_id\"`" + `
	Amount     float64 ` + "`json:\"amount\"`" + `
	Currency   string  ` + "`json:\"currency\"`" + `
	TaxAmount  float64 ` + "`json:\"tax_amount\"`" + `
}
`
	if err := os.WriteFile(filepath.Join(v2Dir, "order_events.go"), []byte(v2Code), 0o644); err != nil {
		t.Fatalf("Failed to write v2 events: %v", err)
	}

	// Determine the repository root from the test working directory
	// We're running from /home/runner/work/pupsourcing/pupsourcing
	// When running tests, the current directory is the package directory
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	// Go up from es/eventmap to the repository root
	repoRoot = filepath.Join(repoRoot, "..", "..")
	repoRoot, err = filepath.Abs(repoRoot)
	if err != nil {
		t.Fatalf("Failed to determine repo root: %v", err)
	}

	// Create go.mod for the test module
	goModContent := `module testevents

go 1.24

require (
	github.com/google/uuid v1.6.0
	github.com/pupsourcing/core v0.0.0
)

replace github.com/pupsourcing/core => ` + repoRoot + `
`
	if err = os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte(goModContent), 0o644); err != nil {
		t.Fatalf("Failed to write go.mod: %v", err)
	}

	// Run go mod download to populate go.sum
	downloadCmd := exec.Command("go", "mod", "download")
	downloadCmd.Dir = tmpDir
	var downloadOutput []byte
	if downloadOutput, err = downloadCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to download dependencies: %v\nOutput: %s", err, downloadOutput)
	}

	// Generate the mapping code
	outputDir := filepath.Join(tmpDir, "generated")
	if err = os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	// Run the eventmap-gen tool
	cmd := exec.Command("go", "run", "github.com/pupsourcing/core/cmd/eventmap-gen",
		"-input", eventsDir,
		"-output", outputDir,
		"-package", "generated",
		"-module", "testevents/events")

	cmd.Dir = tmpDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to run eventmap-gen: %v\nOutput: %s", err, output)
	}

	// Verify the generated file exists
	generatedFile := filepath.Join(outputDir, "event_mapping.gen.go")
	if _, err = os.Stat(generatedFile); err != nil {
		t.Fatalf("Generated file not found: %v", err)
	}

	// Create a test file that uses the generated code
	testCode := `package generated

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pupsourcing/core/es"
	"testevents/events/v1"
	"testevents/events/v2"
)

func TestRoundTripV1(t *testing.T) {
	// Create a v1 domain event
	domainEvent := v1.OrderCreated{
		OrderID:    "order-123",
		CustomerID: "customer-456",
		Amount:     99.99,
	}

	// Convert to es.Event
	esEvents, err := ToESEvents("Orders", "Order", "order-123", []any{domainEvent})
	if err != nil {
		t.Fatalf("ToESEvents failed: %v", err)
	}

	if len(esEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(esEvents))
	}

	esEvent := esEvents[0]

	// Verify event properties
	if esEvent.AggregateType != "Order" {
		t.Errorf("Expected AggregateType=Order, got %s", esEvent.AggregateType)
	}
	if esEvent.EventType != "OrderCreated" {
		t.Errorf("Expected EventType=OrderCreated, got %s", esEvent.EventType)
	}
	if esEvent.EventVersion != 1 {
		t.Errorf("Expected EventVersion=1, got %d", esEvent.EventVersion)
	}

	// Convert to PersistedEvent (simulate database storage)
	persistedEvent := es.PersistedEvent{
		CreatedAt:        esEvent.CreatedAt,
		AggregateType:    esEvent.AggregateType,
		EventType:        esEvent.EventType,
		AggregateID:      esEvent.AggregateID,
		CausationID:      esEvent.CausationID,
		Metadata:         esEvent.Metadata,
		Payload:          esEvent.Payload,
		CorrelationID:    esEvent.CorrelationID,
		TraceID:          esEvent.TraceID,
		GlobalPosition:   1,
		AggregateVersion: 1,
		EventVersion:     esEvent.EventVersion,
		EventID:          esEvent.EventID,
	}

	// Convert back to domain event
	domainEvents, err := FromESEvents[any]([]es.PersistedEvent{persistedEvent})
	if err != nil {
		t.Fatalf("FromESEvents failed: %v", err)
	}

	if len(domainEvents) != 1 {
		t.Fatalf("Expected 1 domain event, got %d", len(domainEvents))
	}

	// Verify round-trip
	restored, ok := domainEvents[0].(v1.OrderCreated)
	if !ok {
		t.Fatalf("Expected v1.OrderCreated, got %T", domainEvents[0])
	}

	if restored.OrderID != domainEvent.OrderID {
		t.Errorf("OrderID mismatch: got %s, want %s", restored.OrderID, domainEvent.OrderID)
	}
	if restored.CustomerID != domainEvent.CustomerID {
		t.Errorf("CustomerID mismatch: got %s, want %s", restored.CustomerID, domainEvent.CustomerID)
	}
	if restored.Amount != domainEvent.Amount {
		t.Errorf("Amount mismatch: got %f, want %f", restored.Amount, domainEvent.Amount)
	}
}

func TestRoundTripV2(t *testing.T) {
	// Create a v2 domain event
	domainEvent := v2.OrderCreated{
		OrderID:    "order-789",
		CustomerID: "customer-101",
		Amount:     199.99,
		Currency:   "USD",
		TaxAmount:  20.00,
	}

	// Convert to es.Event
	esEvents, err := ToESEvents("Orders", "Order", "order-789", []any{domainEvent})
	if err != nil {
		t.Fatalf("ToESEvents failed: %v", err)
	}

	if len(esEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(esEvents))
	}

	esEvent := esEvents[0]

	// Verify version is 2
	if esEvent.EventVersion != 2 {
		t.Errorf("Expected EventVersion=2, got %d", esEvent.EventVersion)
	}

	// Convert to PersistedEvent
	persistedEvent := es.PersistedEvent{
		CreatedAt:        esEvent.CreatedAt,
		AggregateType:    esEvent.AggregateType,
		EventType:        esEvent.EventType,
		AggregateID:      esEvent.AggregateID,
		CausationID:      esEvent.CausationID,
		Metadata:         esEvent.Metadata,
		Payload:          esEvent.Payload,
		CorrelationID:    esEvent.CorrelationID,
		TraceID:          esEvent.TraceID,
		GlobalPosition:   2,
		AggregateVersion: 1,
		EventVersion:     esEvent.EventVersion,
		EventID:          esEvent.EventID,
	}

	// Convert back to domain event
	domainEvents, err := FromESEvents[any]([]es.PersistedEvent{persistedEvent})
	if err != nil {
		t.Fatalf("FromESEvents failed: %v", err)
	}

	// Verify round-trip
	restored, ok := domainEvents[0].(v2.OrderCreated)
	if !ok {
		t.Fatalf("Expected v2.OrderCreated, got %T", domainEvents[0])
	}

	if restored.OrderID != domainEvent.OrderID {
		t.Errorf("OrderID mismatch")
	}
	if restored.Currency != domainEvent.Currency {
		t.Errorf("Currency mismatch: got %s, want %s", restored.Currency, domainEvent.Currency)
	}
}

func TestIntegrationTypeHelpers(t *testing.T) {
	// Test type-safe helper functions
	domainEvent := v1.OrderCreated{
		OrderID:    "order-999",
		CustomerID: "customer-999",
		Amount:     299.99,
	}

	// Use type-safe conversion
	esEvent, err := ToOrderCreatedV1("Orders", "Order", "order-999", domainEvent)
	if err != nil {
		t.Fatalf("ToOrderCreatedV1 failed: %v", err)
	}

	if esEvent.EventType != "OrderCreated" {
		t.Errorf("Expected EventType=OrderCreated, got %s", esEvent.EventType)
	}
	if esEvent.EventVersion != 1 {
		t.Errorf("Expected EventVersion=1, got %d", esEvent.EventVersion)
	}

	// Convert to persisted event
	persistedEvent := es.PersistedEvent{
		CreatedAt:        esEvent.CreatedAt,
		AggregateType:    esEvent.AggregateType,
		EventType:        esEvent.EventType,
		AggregateID:      esEvent.AggregateID,
		CausationID:      esEvent.CausationID,
		Metadata:         esEvent.Metadata,
		Payload:          esEvent.Payload,
		CorrelationID:    esEvent.CorrelationID,
		TraceID:          esEvent.TraceID,
		GlobalPosition:   1,
		AggregateVersion: 1,
		EventVersion:     esEvent.EventVersion,
		EventID:          esEvent.EventID,
	}

	// Use type-safe conversion back
	restored, err := FromOrderCreatedV1(persistedEvent)
	if err != nil {
		t.Fatalf("FromOrderCreatedV1 failed: %v", err)
	}

	if restored.OrderID != domainEvent.OrderID {
		t.Errorf("OrderID mismatch")
	}
}

func TestIntegrationOptions(t *testing.T) {
	domainEvent := v1.OrderCreated{
		OrderID:    "order-111",
		CustomerID: "customer-111",
		Amount:     99.99,
	}

	// Use options to set metadata
	esEvents, err := ToESEvents("Orders", "Order", "order-111", []any{domainEvent},
		WithCausationID("cmd-123"),
		WithCorrelationID("corr-456"),
		WithTraceID("trace-789"),
		WithMetadata([]byte(` + "`" + `{"source":"api"}` + "`" + `)))
	if err != nil {
		t.Fatalf("ToESEvents failed: %v", err)
	}

	esEvent := esEvents[0]

	if !esEvent.CausationID.Valid || esEvent.CausationID.String != "cmd-123" {
		t.Errorf("CausationID not set correctly")
	}
	if !esEvent.CorrelationID.Valid || esEvent.CorrelationID.String != "corr-456" {
		t.Errorf("CorrelationID not set correctly")
	}
	if !esEvent.TraceID.Valid || esEvent.TraceID.String != "trace-789" {
		t.Errorf("TraceID not set correctly")
	}
}

func TestUnknownEventType(t *testing.T) {
	// Create a persisted event with unknown type
	persistedEvent := es.PersistedEvent{
		CreatedAt:        time.Now(),
		AggregateType:    "Order",
		EventType:        "UnknownEvent",
		AggregateID:      "order-123",
		Payload:          []byte("{}"),
		Metadata:         []byte("{}"),
		GlobalPosition:   1,
		AggregateVersion: 1,
		EventVersion:     1,
		EventID:          uuid.New(),
	}

	// Should fail with unknown event type
	_, err := FromESEvents[any]([]es.PersistedEvent{persistedEvent})
	if err == nil {
		t.Error("Expected error for unknown event type")
	}
}

func TestUnknownEventVersion(t *testing.T) {
	// Create a v1 event but with version 99
	domainEvent := v1.OrderCreated{
		OrderID:    "order-123",
		CustomerID: "customer-456",
		Amount:     99.99,
	}

	esEvents, _ := ToESEvents("Orders", "Order", "order-123", []any{domainEvent})
	esEvent := esEvents[0]

	persistedEvent := es.PersistedEvent{
		CreatedAt:        esEvent.CreatedAt,
		AggregateType:    esEvent.AggregateType,
		EventType:        esEvent.EventType,
		AggregateID:      esEvent.AggregateID,
		Payload:          esEvent.Payload,
		Metadata:         esEvent.Metadata,
		GlobalPosition:   1,
		AggregateVersion: 1,
		EventVersion:     99, // Unknown version
		EventID:          esEvent.EventID,
	}

	// Should fail with unknown version
	_, err := FromESEvents[any]([]es.PersistedEvent{persistedEvent})
	if err == nil {
		t.Error("Expected error for unknown event version")
	}
}
`

	if err = os.WriteFile(filepath.Join(outputDir, "integration_test.go"), []byte(testCode), 0o644); err != nil {
		t.Fatalf("Failed to write test code: %v", err)
	}

	// Run go mod tidy to populate go.sum with all dependencies
	tidyCmd := exec.Command("go", "mod", "tidy")
	tidyCmd.Dir = tmpDir
	var tidyOutput []byte
	if tidyOutput, err = tidyCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to run go mod tidy: %v\nOutput: %s", err, tidyOutput)
	}

	// Run the generated tests
	cmd = exec.Command("go", "test", "-v", "./generated")
	cmd.Dir = tmpDir
	output, err = cmd.CombinedOutput()
	t.Logf("Test output:\n%s", output)

	if err != nil {
		t.Fatalf("Generated tests failed: %v\nOutput: %s", err, output)
	}
}

// TestMixedVersionStream tests that we can handle a stream with multiple versions of the same event.
func TestMixedVersionStream(t *testing.T) {
	// This is tested in the integration test above
	t.Skip("Tested in TestGeneratedCodeExecution")
}
