package eventmap

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// EventInfo represents a discovered domain event struct.
type EventInfo struct {
	Name        string
	PackageName string
	ImportPath  string
	Fields      []FieldInfo
	Version     int
}

// FieldInfo represents a struct field.
type FieldInfo struct {
	Name     string
	Type     string
	JSONTag  string
	Optional bool
}

// Config configures the code generation.
type Config struct {
	InputDir    string // Directory containing domain events
	OutputDir   string // Directory where generated code will be written
	OutputFile  string // Name of the generated file (default: event_mapping.gen.go)
	PackageName string // Package name for generated code
	ModulePath  string // Go module path for generating import paths
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		OutputFile:  "event_mapping.gen.go",
		PackageName: "generated",
	}
}

// Generator generates event mapping code.
type Generator struct {
	config Config
	events []EventInfo
}

// NewGenerator creates a new generator with the given configuration.
func NewGenerator(config *Config) *Generator {
	return &Generator{
		config: *config,
		events: make([]EventInfo, 0),
	}
}

// Discover walks the input directory and discovers all domain event structs.
func (g *Generator) Discover() error {
	return filepath.WalkDir(g.config.InputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip non-Go files
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		// Determine version from directory structure
		version := g.extractVersion(path)

		// Parse the Go file
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", path, err)
		}

		// Extract package name and import path
		packageName := file.Name.Name
		importPath := g.buildImportPath(path)

		// Find all exported struct declarations
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}

			for _, spec := range genDecl.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok || !typeSpec.Name.IsExported() {
					continue
				}

				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}

				// Extract fields
				fields := g.extractFields(structType)

				event := EventInfo{
					Name:        typeSpec.Name.Name,
					PackageName: packageName,
					ImportPath:  importPath,
					Version:     version,
					Fields:      fields,
				}

				g.events = append(g.events, event)
			}
		}

		return nil
	})
}

// extractVersion extracts the version number from the directory path.
// Returns 1 if no version directory is found or if parsing fails.
func (g *Generator) extractVersion(path string) int {
	versionRegex := regexp.MustCompile(`/v(\d+)/`)
	matches := versionRegex.FindStringSubmatch(path)
	if len(matches) > 1 {
		var version int
		_, err := fmt.Sscanf(matches[1], "%d", &version)
		if err != nil || version < 1 {
			return 1 // Default version on parse error
		}
		return version
	}
	return 1 // Default version
}

// buildImportPath builds the import path for a given file path.
func (g *Generator) buildImportPath(filePath string) string {
	relPath, err := filepath.Rel(g.config.InputDir, filepath.Dir(filePath))
	if err != nil {
		relPath = filepath.Dir(filePath)
	}

	if g.config.ModulePath != "" {
		return filepath.Join(g.config.ModulePath, relPath)
	}

	// Try to determine from input directory
	absInput, err := filepath.Abs(g.config.InputDir)
	if err != nil {
		return filepath.ToSlash(relPath)
	}
	absFile, err := filepath.Abs(filePath)
	if err != nil {
		return filepath.ToSlash(relPath)
	}
	relPath, err = filepath.Rel(absInput, filepath.Dir(absFile))
	if err != nil {
		return filepath.ToSlash(relPath)
	}

	return filepath.ToSlash(relPath)
}

// extractFields extracts field information from a struct type.
func (g *Generator) extractFields(structType *ast.StructType) []FieldInfo {
	fields := make([]FieldInfo, 0)

	for _, field := range structType.Fields.List {
		if len(field.Names) == 0 {
			continue // Skip embedded fields
		}

		for _, name := range field.Names {
			if !name.IsExported() {
				continue
			}

			fieldInfo := FieldInfo{
				Name: name.Name,
				Type: g.typeToString(field.Type),
			}

			// Extract JSON tag if present
			if field.Tag != nil {
				tag := field.Tag.Value
				tag = strings.Trim(tag, "`")
				if strings.Contains(tag, "json:") {
					jsonTagRegex := regexp.MustCompile(`json:"([^"]+)"`)
					matches := jsonTagRegex.FindStringSubmatch(tag)
					if len(matches) > 1 {
						fieldInfo.JSONTag = strings.Split(matches[1], ",")[0]
						fieldInfo.Optional = strings.Contains(matches[1], "omitempty")
					}
				}
			}

			fields = append(fields, fieldInfo)
		}
	}

	return fields
}

// typeToString converts an AST type to a string representation.
func (g *Generator) typeToString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + g.typeToString(t.X)
	case *ast.ArrayType:
		return "[]" + g.typeToString(t.Elt)
	case *ast.MapType:
		return "map[" + g.typeToString(t.Key) + "]" + g.typeToString(t.Value)
	case *ast.SelectorExpr:
		return g.typeToString(t.X) + "." + t.Sel.Name
	default:
		return "interface{}"
	}
}

// Generate generates the mapping code and writes it to the output file.
func (g *Generator) Generate() error {
	if len(g.events) == 0 {
		return fmt.Errorf("no events discovered in %s", g.config.InputDir)
	}

	// Sort events by name and version for deterministic output
	sort.Slice(g.events, func(i, j int) bool {
		if g.events[i].Name != g.events[j].Name {
			return g.events[i].Name < g.events[j].Name
		}
		return g.events[i].Version < g.events[j].Version
	})

	// Ensure output directory exists
	if err := os.MkdirAll(g.config.OutputDir, 0o755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate code
	code := g.generateCode()

	// Write to file
	outputPath := filepath.Join(g.config.OutputDir, g.config.OutputFile)
	if err := os.WriteFile(outputPath, []byte(code), 0o600); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	// Generate test file
	testCode := g.generateTestCode()
	testOutputPath := filepath.Join(g.config.OutputDir, g.getTestFileName())
	if err := os.WriteFile(testOutputPath, []byte(testCode), 0o600); err != nil {
		return fmt.Errorf("failed to write test file: %w", err)
	}

	return nil
}

// generateCode generates the complete mapping code.
func (g *Generator) generateCode() string {
	var sb strings.Builder

	// File header
	sb.WriteString(g.generateHeader())
	sb.WriteString("\n\n")

	// Imports
	sb.WriteString(g.generateImports())
	sb.WriteString("\n\n")

	// Option type for metadata injection
	sb.WriteString(g.generateOptionsType())
	sb.WriteString("\n\n")

	// EventTypeOf function
	sb.WriteString(g.generateEventTypeOf())
	sb.WriteString("\n\n")

	// ToESEvents function
	sb.WriteString(g.generateToESEvents())
	sb.WriteString("\n\n")

	// FromESEvents function
	sb.WriteString(g.generateFromESEvents())
	sb.WriteString("\n\n")

	// Type-safe helpers
	sb.WriteString(g.generateTypeHelpers())

	return sb.String()
}

// generateHeader generates the file header.
func (g *Generator) generateHeader() string {
	return fmt.Sprintf(`// Code generated by eventmap-gen. DO NOT EDIT.

package %s`, g.config.PackageName)
}

// generateImports generates the import statements.
func (g *Generator) generateImports() string {
	var sb strings.Builder

	sb.WriteString("import (\n")
	sb.WriteString("\t\"encoding/json\"\n")
	sb.WriteString("\t\"fmt\"\n")
	sb.WriteString("\t\"time\"\n")
	sb.WriteString("\n")
	sb.WriteString("\t\"github.com/google/uuid\"\n")
	sb.WriteString("\t\"github.com/pupsourcing/core/es\"\n")

	// Add imports for domain event packages
	importPaths := make(map[string]string)
	for _, event := range g.events {
		if event.ImportPath != "" {
			// Use package name as alias
			importPaths[event.ImportPath] = event.PackageName
		}
	}

	if len(importPaths) > 0 {
		sb.WriteString("\n")
		// Sort import paths for deterministic output
		paths := make([]string, 0, len(importPaths))
		for path := range importPaths {
			paths = append(paths, path)
		}
		sort.Strings(paths)

		for _, path := range paths {
			alias := importPaths[path]
			sb.WriteString(fmt.Sprintf("\t%s %q\n", alias, path))
		}
	}

	sb.WriteString(")")

	return sb.String()
}

// generateOptionsType generates the Option type for dependency injection.
func (g *Generator) generateOptionsType() string {
	return `// Option is a functional option for configuring event metadata.
type Option func(*eventOptions)

type eventOptions struct {
	causationID   es.NullString
	correlationID es.NullString
	traceID       es.NullString
	metadata      []byte
}

// WithCausationID sets the causation ID for the event.
func WithCausationID(id string) Option {
	return func(o *eventOptions) {
		o.causationID = es.NullString{String: id, Valid: true}
	}
}

// WithCorrelationID sets the correlation ID for the event.
func WithCorrelationID(id string) Option {
	return func(o *eventOptions) {
		o.correlationID = es.NullString{String: id, Valid: true}
	}
}

// WithTraceID sets the trace ID for the event.
func WithTraceID(id string) Option {
	return func(o *eventOptions) {
		o.traceID = es.NullString{String: id, Valid: true}
	}
}

// WithMetadata sets custom metadata for the event.
func WithMetadata(metadata []byte) Option {
	return func(o *eventOptions) {
		o.metadata = metadata
	}
}`
}

// generateEventTypeOf generates the EventTypeOf function.
func (g *Generator) generateEventTypeOf() string {
	var sb strings.Builder

	sb.WriteString(`// EventTypeOf returns the event type string for a given domain event.
// The event type is the struct name without version information.
func EventTypeOf(e any) (string, error) {
	switch e.(type) {
`)

	// Generate switch cases
	for _, event := range g.events {
		sb.WriteString(fmt.Sprintf("\tcase %s.%s, *%s.%s:\n",
			event.PackageName, event.Name, event.PackageName, event.Name))
		sb.WriteString(fmt.Sprintf("\t\treturn %q, nil\n", event.Name))
	}

	sb.WriteString(`	default:
		return "", fmt.Errorf("unknown event type: %T", e)
	}
}`)

	return sb.String()
}

// generateToESEvents generates the ToESEvents function.
func (g *Generator) generateToESEvents() string {
	var sb strings.Builder

	sb.WriteString(`// ToESEvents converts domain events to es.Event instances.
// Each domain event is marshaled to JSON and wrapped in an es.Event.
// The generic type T allows for type-safe event slices instead of []any.
func ToESEvents[T any](boundedContext string, aggregateType string, aggregateID string, events []T, opts ...Option) ([]es.Event, error) {
	options := &eventOptions{}
	for _, opt := range opts {
		opt(options)
	}

	result := make([]es.Event, 0, len(events))

	for _, e := range events {
		eventType, err := EventTypeOf(e)
		if err != nil {
			return nil, err
		}

		payload, err := json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal event %s: %w", eventType, err)
		}

		metadata := options.metadata
		if metadata == nil {
			metadata = []byte("{}")
		}

		version := getEventVersion(e)

		event := es.Event{
			BoundedContext: boundedContext,
			AggregateType: aggregateType,
			AggregateID:   aggregateID,
			EventType:     eventType,
			EventVersion:  version,
			EventID:       uuid.New(),
			Payload:       payload,
			Metadata:      metadata,
			CausationID:   options.causationID,
			CorrelationID: options.correlationID,
			TraceID:       options.traceID,
			CreatedAt:     time.Now(),
		}

		result = append(result, event)
	}

	return result, nil
}

// getEventVersion returns the version for a given domain event.
func getEventVersion(e any) int {
	switch e.(type) {
`)

	// Generate version lookup cases
	for _, event := range g.events {
		sb.WriteString(fmt.Sprintf("\tcase %s.%s, *%s.%s:\n",
			event.PackageName, event.Name, event.PackageName, event.Name))
		sb.WriteString(fmt.Sprintf("\t\treturn %d\n", event.Version))
	}

	sb.WriteString(`	default:
		return 1
	}
}`)

	return sb.String()
}

// generateFromESEvents generates the FromESEvents function with generics.
func (g *Generator) generateFromESEvents() string {
	var sb strings.Builder

	sb.WriteString(`// FromESEvents converts persisted events back to domain events.
// The function uses generics to return a strongly-typed slice.
// T must be 'any' or a common interface implemented by all domain events.
func FromESEvents[T any](events []es.PersistedEvent) ([]T, error) {
	result := make([]T, 0, len(events))

	for i, pe := range events {
		domainEvent, err := FromESEvent(pe)
		if err != nil {
			return nil, fmt.Errorf("failed to convert event at index %d: %w", i, err)
		}

		// Type assertion
		typedEvent, ok := domainEvent.(T)
		if !ok {
			return nil, fmt.Errorf("event at index %d is not of expected type: got %T", i, domainEvent)
		}

		result = append(result, typedEvent)
	}

	return result, nil
}

// FromESEvent converts a single persisted event to a domain event.
// This is useful for consumer handlers that need to convert individual events.
func FromESEvent(pe es.PersistedEvent) (any, error) {
	switch pe.EventType {
`)

	// Group events by type
	eventsByType := make(map[string][]EventInfo)
	for _, event := range g.events {
		eventsByType[event.Name] = append(eventsByType[event.Name], event)
	}

	// Generate switch cases for each event type
	for eventType, versions := range eventsByType {
		sb.WriteString(fmt.Sprintf("\tcase %q:\n", eventType))
		sb.WriteString("\t\tswitch pe.EventVersion {\n")

		for _, event := range versions {
			sb.WriteString(fmt.Sprintf("\t\tcase %d:\n", event.Version))
			sb.WriteString(fmt.Sprintf("\t\t\tvar e %s.%s\n", event.PackageName, event.Name))
			sb.WriteString("\t\t\tif err := json.Unmarshal(pe.Payload, &e); err != nil {\n")
			sb.WriteString(fmt.Sprintf("\t\t\t\treturn nil, fmt.Errorf(\"failed to unmarshal %s v%d: %%w\", err)\n",
				event.Name, event.Version))
			sb.WriteString("\t\t\t}\n")
			sb.WriteString("\t\t\treturn e, nil\n")
		}

		sb.WriteString("\t\tdefault:\n")
		sb.WriteString(fmt.Sprintf("\t\t\treturn nil, fmt.Errorf(\"unknown version %%d for event type %s\", pe.EventVersion)\n",
			eventType))
		sb.WriteString("\t\t}\n")
	}

	sb.WriteString(`	default:
		return nil, fmt.Errorf("unknown event type: %s", pe.EventType)
	}
}`)

	return sb.String()
}

// generateTypeHelpers generates type-safe per-event helper functions.
func (g *Generator) generateTypeHelpers() string {
	var sb strings.Builder

	for _, event := range g.events {
		// ToXXXVN function
		sb.WriteString(fmt.Sprintf(`// To%sV%d converts a domain event to an es.Event.
func To%sV%d(boundedContext string, aggregateType string, aggregateID string, e %s.%s, opts ...Option) (es.Event, error) {
	options := &eventOptions{}
	for _, opt := range opts {
		opt(options)
	}

	payload, err := json.Marshal(e)
	if err != nil {
		return es.Event{}, fmt.Errorf("failed to marshal %s: %%w", err)
	}

	metadata := options.metadata
	if metadata == nil {
		metadata = []byte("{}")
	}

	return es.Event{
		BoundedContext: boundedContext,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     %q,
		EventVersion:  %d,
		EventID:       uuid.New(),
		Payload:       payload,
		Metadata:      metadata,
		CausationID:   options.causationID,
		CorrelationID: options.correlationID,
		TraceID:       options.traceID,
		CreatedAt:     time.Now(),
	}, nil
}

`,
			event.Name, event.Version,
			event.Name, event.Version, event.PackageName, event.Name,
			event.Name,
			event.Name, event.Version))

		// FromXXXVN function
		sb.WriteString(fmt.Sprintf(`// From%sV%d converts a persisted event to a domain event.
// Returns an error if the event type or version doesn't match.
func From%sV%d(pe es.PersistedEvent) (%s.%s, error) {
	if pe.EventType != %q {
		return %s.%s{}, fmt.Errorf("expected event type %s, got %%s", pe.EventType)
	}
	if pe.EventVersion != %d {
		return %s.%s{}, fmt.Errorf("expected event version %d, got %%d", pe.EventVersion)
	}

	var e %s.%s
	if err := json.Unmarshal(pe.Payload, &e); err != nil {
		return %s.%s{}, fmt.Errorf("failed to unmarshal %s v%d: %%w", err)
	}

	return e, nil
}

`,
			event.Name, event.Version,
			event.Name, event.Version, event.PackageName, event.Name,
			event.Name,
			event.PackageName, event.Name, event.Name,
			event.Version,
			event.PackageName, event.Name, event.Version,
			event.PackageName, event.Name,
			event.PackageName, event.Name, event.Name, event.Version))
	}

	return sb.String()
}

// getTestFileName returns the test file name based on the output file name.
func (g *Generator) getTestFileName() string {
	// Replace .gen.go with .gen_test.go or .go with _test.go
	if strings.HasSuffix(g.config.OutputFile, ".gen.go") {
		return strings.TrimSuffix(g.config.OutputFile, ".gen.go") + ".gen_test.go"
	}
	if strings.HasSuffix(g.config.OutputFile, ".go") {
		return strings.TrimSuffix(g.config.OutputFile, ".go") + "_test.go"
	}
	return g.config.OutputFile + "_test.go"
}

// generateTestCode generates comprehensive unit tests for the generated code.
func (g *Generator) generateTestCode() string {
	var sb strings.Builder

	// File header
	sb.WriteString(fmt.Sprintf(`// Code generated by eventmap-gen. DO NOT EDIT.

package %s

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pupsourcing/core/es"
`, g.config.PackageName))

	// Add imports for domain event packages
	importPaths := make(map[string]string)
	for _, event := range g.events {
		if event.ImportPath != "" {
			importPaths[event.ImportPath] = event.PackageName
		}
	}

	if len(importPaths) > 0 {
		sb.WriteString("\n")
		// Sort import paths for deterministic output
		paths := make([]string, 0, len(importPaths))
		for path := range importPaths {
			paths = append(paths, path)
		}
		sort.Strings(paths)

		for _, path := range paths {
			alias := importPaths[path]
			sb.WriteString(fmt.Sprintf("\t%s %q\n", alias, path))
		}
	}

	sb.WriteString(")\n\n")

	// Test EventTypeOf
	sb.WriteString(g.generateTestEventTypeOf())
	sb.WriteString("\n\n")

	// Test ToESEvents with generics
	sb.WriteString(g.generateTestToESEvents())
	sb.WriteString("\n\n")

	// Test FromESEvents
	sb.WriteString(g.generateTestFromESEvents())
	sb.WriteString("\n\n")

	// Test options
	sb.WriteString(g.generateTestOptions())
	sb.WriteString("\n\n")

	// Test type-specific helpers
	sb.WriteString(g.generateTestTypeHelpers())
	sb.WriteString("\n\n")

	// Test error cases
	sb.WriteString(g.generateTestErrorCases())

	return sb.String()
}

// generateTestEventTypeOf generates tests for EventTypeOf function.
func (g *Generator) generateTestEventTypeOf() string {
	var sb strings.Builder

	sb.WriteString(`// TestEventTypeOf tests the EventTypeOf function.
func TestEventTypeOf(t *testing.T) {
	tests := []struct {
		name      string
		event     any
		wantType  string
		wantError bool
	}{
`)

	// Generate test cases for each event
	for _, event := range g.events {
		sb.WriteString("\t\t{\n")
		sb.WriteString(fmt.Sprintf("\t\t\tname:      %q,\n", event.Name+"V"+fmt.Sprint(event.Version)))
		sb.WriteString(fmt.Sprintf("\t\t\tevent:     %s.%s{},\n", event.PackageName, event.Name))
		sb.WriteString(fmt.Sprintf("\t\t\twantType:  %q,\n", event.Name))
		sb.WriteString("\t\t\twantError: false,\n")
		sb.WriteString("\t\t},\n")

		// Test pointer variant
		sb.WriteString("\t\t{\n")
		sb.WriteString(fmt.Sprintf("\t\t\tname:      %q,\n", event.Name+"V"+fmt.Sprint(event.Version)+"Pointer"))
		sb.WriteString(fmt.Sprintf("\t\t\tevent:     &%s.%s{},\n", event.PackageName, event.Name))
		sb.WriteString(fmt.Sprintf("\t\t\twantType:  %q,\n", event.Name))
		sb.WriteString("\t\t\twantError: false,\n")
		sb.WriteString("\t\t},\n")
	}

	// Test unknown type
	sb.WriteString(`		{
			name:      "UnknownType",
			event:     struct{}{},
			wantType:  "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, err := EventTypeOf(tt.event)
			if (err != nil) != tt.wantError {
				t.Errorf("EventTypeOf() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if gotType != tt.wantType {
				t.Errorf("EventTypeOf() = %v, want %v", gotType, tt.wantType)
			}
		})
	}
}`)

	return sb.String()
}

// generateTestToESEvents generates tests for ToESEvents function.
func (g *Generator) generateTestToESEvents() string {
	if len(g.events) == 0 {
		return ""
	}

	// Pick the first event for testing
	event := g.events[0]

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`// TestToESEvents tests the ToESEvents function with generics.
func TestToESEvents(t *testing.T) {
	boundedContext := "TestContext"
	aggregateType := "TestAggregate"
	aggregateID := uuid.New().String()

	// Create a domain event
	domainEvent := %s.%s{}

	// Test with slice of specific type (not []any)
	events := []%s.%s{domainEvent}
	
	esEvents, err := ToESEvents(boundedContext, aggregateType, aggregateID, events)
	if err != nil {
		t.Fatalf("ToESEvents() failed: %%v", err)
	}

	if len(esEvents) != 1 {
		t.Fatalf("Expected 1 event, got %%d", len(esEvents))
	}

	esEvent := esEvents[0]

	// Verify event properties
	if esEvent.BoundedContext != boundedContext {
		t.Errorf("BoundedContext = %%s, want %%s", esEvent.BoundedContext, boundedContext)
	}
	if esEvent.AggregateType != aggregateType {
		t.Errorf("AggregateType = %%s, want %%s", esEvent.AggregateType, aggregateType)
	}
	if esEvent.AggregateID != aggregateID {
		t.Errorf("AggregateID = %%s, want %%s", esEvent.AggregateID, aggregateID)
	}
	if esEvent.EventType != %q {
		t.Errorf("EventType = %%s, want %%s", esEvent.EventType, %q)
	}
	if esEvent.EventVersion != %d {
		t.Errorf("EventVersion = %%d, want %%d", esEvent.EventVersion, %d)
	}
	if esEvent.EventID == uuid.Nil {
		t.Error("EventID should not be nil")
	}
	if esEvent.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}`, event.PackageName, event.Name, event.PackageName, event.Name,
		event.Name, event.Name, event.Version, event.Version))

	return sb.String()
}

// generateTestFromESEvents generates tests for FromESEvents function.
func (g *Generator) generateTestFromESEvents() string {
	if len(g.events) == 0 {
		return ""
	}

	// Pick the first event for testing
	event := g.events[0]

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`// TestFromESEvents tests the FromESEvents function.
func TestFromESEvents(t *testing.T) {
	// Create a persisted event
	persistedEvent := es.PersistedEvent{
		CreatedAt:        time.Now(),
		AggregateType:    "TestAggregate",
		EventType:        %q,
		AggregateID:      uuid.New().String(),
		Payload:          []byte("{}"),
		Metadata:         []byte("{}"),
		GlobalPosition:   1,
		AggregateVersion: 1,
		EventVersion:     %d,
		EventID:          uuid.New(),
	}

	// Convert to domain events
	domainEvents, err := FromESEvents[any]([]es.PersistedEvent{persistedEvent})
	if err != nil {
		t.Fatalf("FromESEvents() failed: %%v", err)
	}

	if len(domainEvents) != 1 {
		t.Fatalf("Expected 1 domain event, got %%d", len(domainEvents))
	}

	// Verify type
	_, ok := domainEvents[0].(%s.%s)
	if !ok {
		t.Errorf("Expected %%T, got %%T", %s.%s{}, domainEvents[0])
	}
}`, event.Name, event.Version, event.PackageName, event.Name, event.PackageName, event.Name))

	return sb.String()
}

// generateTestOptions generates tests for the Options pattern.
func (g *Generator) generateTestOptions() string {
	if len(g.events) == 0 {
		return ""
	}

	event := g.events[0]

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`// TestOptions tests the Options pattern.
func TestOptions(t *testing.T) {
	boundedContext := "TestContext"
	aggregateType := "TestAggregate"
	aggregateID := uuid.New().String()

	domainEvent := %s.%s{}
	
	// Use options
	esEvents, err := ToESEvents(
		boundedContext,
		aggregateType,
		aggregateID,
		[]%s.%s{domainEvent},
		WithCausationID("causation-123"),
		WithCorrelationID("correlation-456"),
		WithTraceID("trace-789"),
		WithMetadata([]byte("{\"key\":\"value\"}")),
	)
	if err != nil {
		t.Fatalf("ToESEvents() failed: %%v", err)
	}

	if len(esEvents) != 1 {
		t.Fatalf("Expected 1 event, got %%d", len(esEvents))
	}

	esEvent := esEvents[0]

	// Verify options were applied
	if !esEvent.CausationID.Valid || esEvent.CausationID.String != "causation-123" {
		t.Errorf("CausationID not set correctly: got %%v", esEvent.CausationID)
	}
	if !esEvent.CorrelationID.Valid || esEvent.CorrelationID.String != "correlation-456" {
		t.Errorf("CorrelationID not set correctly: got %%v", esEvent.CorrelationID)
	}
	if !esEvent.TraceID.Valid || esEvent.TraceID.String != "trace-789" {
		t.Errorf("TraceID not set correctly: got %%v", esEvent.TraceID)
	}
	if string(esEvent.Metadata) != "{\"key\":\"value\"}" {
		t.Errorf("Metadata not set correctly: got %%s", string(esEvent.Metadata))
	}
}`, event.PackageName, event.Name, event.PackageName, event.Name))

	return sb.String()
}

// generateTestTypeHelpers generates tests for type-specific helper functions.
func (g *Generator) generateTestTypeHelpers() string {
	if len(g.events) == 0 {
		return ""
	}

	event := g.events[0]

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`// TestTypeHelpers tests type-specific helper functions.
func TestTypeHelpers(t *testing.T) {
	boundedContext := "TestContext"
	aggregateType := "TestAggregate"
	aggregateID := uuid.New().String()

	domainEvent := %s.%s{}

	// Test To helper
	esEvent, err := To%sV%d(boundedContext, aggregateType, aggregateID, domainEvent)
	if err != nil {
		t.Fatalf("To%sV%d() failed: %%v", err)
	}

	if esEvent.EventType != %q {
		t.Errorf("EventType = %%s, want %%s", esEvent.EventType, %q)
	}
	if esEvent.EventVersion != %d {
		t.Errorf("EventVersion = %%d, want %%d", esEvent.EventVersion, %d)
	}

	// Convert to persisted event
	persistedEvent := es.PersistedEvent{
		CreatedAt:        esEvent.CreatedAt,
		BoundedContext:   esEvent.BoundedContext,
		AggregateType:    esEvent.AggregateType,
		EventType:        esEvent.EventType,
		AggregateID:      esEvent.AggregateID,
		Payload:          esEvent.Payload,
		Metadata:         esEvent.Metadata,
		CausationID:      esEvent.CausationID,
		CorrelationID:    esEvent.CorrelationID,
		TraceID:          esEvent.TraceID,
		GlobalPosition:   1,
		AggregateVersion: 1,
		EventVersion:     esEvent.EventVersion,
		EventID:          esEvent.EventID,
	}

	// Test From helper
	restored, err := From%sV%d(persistedEvent)
	if err != nil {
		t.Fatalf("From%sV%d() failed: %%v", err)
	}

	// Verify type
	_ = restored // Type assertion happens in function call
}`, event.PackageName, event.Name,
		event.Name, event.Version, event.Name, event.Version,
		event.Name, event.Name,
		event.Version, event.Version,
		event.Name, event.Version, event.Name, event.Version))

	return sb.String()
}

// generateTestErrorCases generates tests for error handling.
func (g *Generator) generateTestErrorCases() string {
	var sb strings.Builder

	sb.WriteString(`// TestErrorCases tests error handling.
func TestErrorCases(t *testing.T) {
	t.Run("UnknownEventType", func(t *testing.T) {
		persistedEvent := es.PersistedEvent{
			CreatedAt:        time.Now(),
			AggregateType:    "TestAggregate",
			EventType:        "UnknownEvent",
			AggregateID:      uuid.New().String(),
			Payload:          []byte("{}"),
			Metadata:         []byte("{}"),
			GlobalPosition:   1,
			AggregateVersion: 1,
			EventVersion:     1,
			EventID:          uuid.New(),
		}

		_, err := FromESEvents[any]([]es.PersistedEvent{persistedEvent})
		if err == nil {
			t.Error("Expected error for unknown event type")
		}
	})

	t.Run("InvalidJSON", func(t *testing.T) {`)

	if len(g.events) > 0 {
		event := g.events[0]
		sb.WriteString(fmt.Sprintf(`
		persistedEvent := es.PersistedEvent{
			CreatedAt:        time.Now(),
			AggregateType:    "TestAggregate",
			EventType:        %q,
			AggregateID:      uuid.New().String(),
			Payload:          []byte("invalid json"),
			Metadata:         []byte("{}"),
			GlobalPosition:   1,
			AggregateVersion: 1,
			EventVersion:     %d,
			EventID:          uuid.New(),
		}

		_, err := FromESEvents[any]([]es.PersistedEvent{persistedEvent})
		if err == nil {
			t.Error("Expected error for invalid JSON")
		}`, event.Name, event.Version))
	}

	sb.WriteString(`
	})
}`)

	return sb.String()
}
