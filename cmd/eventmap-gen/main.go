// Command eventmap-gen generates mapping code between domain events and event sourcing types.
//
// Usage:
//
//	go run github.com/pupsourcing/core/cmd/eventmap-gen \
//	  -input internal/core/component/user/domain/user/events \
//	  -output internal/infrastructure/component/user/persistence/es/generated
//
// The tool discovers domain event structs in the input directory and generates
// strongly-typed mapping functions for converting between domain events and
// pupsourcing core types (es.Event and es.PersistedEvent).
//
// # Versioned Events
//
// The tool supports event versioning through directory structure. Subdirectories
// named v1, v2, v3, etc. indicate the event version:
//
//	events/
//	  v1/
//	    user_registered.go
//	    user_email_changed.go
//	  v2/
//	    user_registered.go  // New version with different schema
//
// If no version directory exists, the default version is 1.
//
// # Generated Code
//
// The tool generates:
//   - EventTypeOf(e any) - Resolves event type string from domain event
//   - ToESEvents(boundedContext, aggregateType, aggregateID, events, opts...) - Converts domain events to es.Event
//   - FromESEvents[T](events) - Converts es.PersistedEvent to domain events (using generics)
//   - Type-safe helpers per event (ToUserRegisteredV1, FromUserRegisteredV1, etc.)
//
// # Clean Architecture
//
// This tool maintains clean architecture boundaries:
//   - Domain events remain pure (no dependency on pupsourcing)
//   - Generated code lives in the infrastructure layer
//   - Generated code may depend on pupsourcing
//   - No runtime reflection or magic
//   - All mapping is explicit and inspectable
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pupsourcing/core/es/eventmap"
)

func main() {
	var (
		inputDir    = flag.String("input", "", "Input directory containing domain events (required)")
		outputDir   = flag.String("output", "", "Output directory for generated code (required)")
		outputFile  = flag.String("filename", "event_mapping.gen.go", "Output filename")
		packageName = flag.String("package", "generated", "Package name for generated code")
		modulePath  = flag.String("module", "", "Go module path for import generation (auto-detected if empty)")
	)

	flag.Parse()

	// Validate required flags
	if *inputDir == "" {
		fmt.Fprintf(os.Stderr, "Error: -input flag is required\n")
		flag.Usage()
		os.Exit(1)
	}

	if *outputDir == "" {
		fmt.Fprintf(os.Stderr, "Error: -output flag is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Convert to absolute paths
	absInputDir, err := filepath.Abs(*inputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid input directory: %v\n", err)
		os.Exit(1)
	}

	absOutputDir, err := filepath.Abs(*outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid output directory: %v\n", err)
		os.Exit(1)
	}

	// Auto-detect module path if not provided
	detectedModulePath := *modulePath
	if detectedModulePath == "" {
		detectedModulePath, err = detectModulePath(absInputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not auto-detect module path: %v\n", err)
			fmt.Fprintf(os.Stderr, "Using relative import paths. Consider using -module flag.\n")
		}
	}

	// Configure generator
	config := eventmap.Config{
		InputDir:    absInputDir,
		OutputDir:   absOutputDir,
		OutputFile:  *outputFile,
		PackageName: *packageName,
		ModulePath:  detectedModulePath,
	}

	// Create generator
	generator := eventmap.NewGenerator(&config)

	// Discover domain events
	fmt.Printf("Discovering events in %s...\n", absInputDir)
	if err := generator.Discover(); err != nil {
		fmt.Fprintf(os.Stderr, "Error discovering events: %v\n", err)
		os.Exit(1)
	}

	// Generate code
	fmt.Printf("Generating mapping code...\n")
	if err := generator.Generate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error generating code: %v\n", err)
		os.Exit(1)
	}

	outputPath := filepath.Join(absOutputDir, *outputFile)
	fmt.Printf("Successfully generated: %s\n", outputPath)
}

// detectModulePath attempts to detect the Go module path from go.mod file.
func detectModulePath(startDir string) (string, error) {
	dir := startDir
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			// Found go.mod, read module path
			content, err := os.ReadFile(goModPath)
			if err != nil {
				return "", fmt.Errorf("failed to read go.mod: %w", err)
			}

			// Extract module path from first line
			lines := string(content)
			if len(lines) > 7 && lines[:7] == "module " {
				endIdx := 7
				for endIdx < len(lines) && lines[endIdx] != '\n' && lines[endIdx] != '\r' {
					endIdx++
				}
				modulePath := lines[7:endIdx]

				// Build import path relative to module root
				relPath, err := filepath.Rel(dir, startDir)
				if err != nil {
					return modulePath, nil
				}

				if relPath == "." {
					return modulePath, nil
				}

				return filepath.Join(modulePath, filepath.ToSlash(relPath)), nil
			}

			return "", fmt.Errorf("invalid go.mod format")
		}

		// Move up one directory
		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached root
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("go.mod not found")
}
