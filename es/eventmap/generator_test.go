package eventmap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerator_Discover(t *testing.T) {
	config := Config{
		InputDir:    "testdata/events",
		OutputDir:   "testdata/output",
		OutputFile:  "event_mapping.gen.go",
		PackageName: "generated",
		ModulePath:  "github.com/pupsourcing/core/es/eventmap/testdata/events",
	}

	gen := NewGenerator(&config)
	err := gen.Discover()
	if err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Verify events were discovered
	if len(gen.events) == 0 {
		t.Fatal("No events discovered")
	}

	// Check that we found multiple versions
	eventsByName := make(map[string][]EventInfo)
	for _, event := range gen.events {
		eventsByName[event.Name] = append(eventsByName[event.Name], event)
	}

	// Should have UserRegistered in v1 and v2
	if len(eventsByName["UserRegistered"]) < 2 {
		t.Errorf("Expected UserRegistered in multiple versions, got %d", len(eventsByName["UserRegistered"]))
	}

	// Should have UserEmailChanged in v1
	if len(eventsByName["UserEmailChanged"]) < 1 {
		t.Error("Expected UserEmailChanged to be discovered")
	}

	// Verify versions are correctly extracted
	for _, event := range gen.events {
		if event.Version < 1 {
			t.Errorf("Event %s has invalid version %d", event.Name, event.Version)
		}
	}
}

func TestGenerator_ExtractVersion(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected int
	}{
		{
			name:     "version 1 directory",
			path:     "/some/path/v1/event.go",
			expected: 1,
		},
		{
			name:     "version 2 directory",
			path:     "/some/path/v2/event.go",
			expected: 2,
		},
		{
			name:     "version 10 directory",
			path:     "/some/path/v10/event.go",
			expected: 10,
		},
		{
			name:     "no version directory",
			path:     "/some/path/event.go",
			expected: 1,
		},
		{
			name:     "nested version directory",
			path:     "/some/path/domain/v3/events/event.go",
			expected: 3,
		},
	}

	config := Config{}
	gen := NewGenerator(&config)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := gen.extractVersion(tt.path)
			if version != tt.expected {
				t.Errorf("extractVersion(%q) = %d, want %d", tt.path, version, tt.expected)
			}
		})
	}
}

func TestGenerator_Generate(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		InputDir:    "testdata/events",
		OutputDir:   tmpDir,
		OutputFile:  "event_mapping.gen.go",
		PackageName: "generated",
		ModulePath:  "github.com/pupsourcing/core/es/eventmap/testdata/events",
	}

	gen := NewGenerator(&config)

	// Discover events
	if err := gen.Discover(); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Generate code
	if err := gen.Generate(); err != nil {
		t.Fatalf("Generate() failed: %v", err)
	}

	// Verify file was created
	outputPath := filepath.Join(tmpDir, config.OutputFile)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	generatedCode := string(content)

	// Verify essential components are present
	requiredStrings := []string{
		"package generated",
		"func EventTypeOf(e any) (string, error)",
		"func ToESEvents[T any](boundedContext string, aggregateType string, aggregateID string, events []T, opts ...Option)",
		"func FromESEvents[T any](events []es.PersistedEvent) ([]T, error)",
		"UserRegistered",
		"UserEmailChanged",
		"WithCausationID",
		"WithCorrelationID",
		"WithTraceID",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(generatedCode, required) {
			t.Errorf("Generated code missing required string: %s", required)
		}
	}

	// Verify version-specific functions are generated
	versionFunctions := []string{
		"ToUserRegisteredV1",
		"FromUserRegisteredV1",
		"ToUserRegisteredV2",
		"FromUserRegisteredV2",
		"ToUserEmailChangedV1",
		"FromUserEmailChangedV1",
	}

	for _, fn := range versionFunctions {
		if !strings.Contains(generatedCode, fn) {
			t.Errorf("Generated code missing version-specific function: %s", fn)
		}
	}

	// Verify imports are present
	requiredImports := []string{
		`"encoding/json"`,
		`"github.com/google/uuid"`,
		`"github.com/pupsourcing/core/es"`,
	}

	for _, imp := range requiredImports {
		if !strings.Contains(generatedCode, imp) {
			t.Errorf("Generated code missing import: %s", imp)
		}
	}
}

func TestGenerator_GenerateNoEvents(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		InputDir:    tmpDir, // Empty directory
		OutputDir:   tmpDir,
		OutputFile:  "event_mapping.gen.go",
		PackageName: "generated",
	}

	gen := NewGenerator(&config)

	// Discover should succeed but find nothing
	if err := gen.Discover(); err != nil {
		t.Fatalf("Discover() failed: %v", err)
	}

	// Generate should fail with no events
	err := gen.Generate()
	if err == nil {
		t.Error("Generate() should fail when no events are discovered")
	}
	if !strings.Contains(err.Error(), "no events discovered") {
		t.Errorf("Expected 'no events discovered' error, got: %v", err)
	}
}

func TestGenerator_TypeToString(t *testing.T) {
	// This is tested implicitly through the Discover test,
	// but we can add explicit tests if needed
	t.Skip("Tested implicitly through integration tests")
}
