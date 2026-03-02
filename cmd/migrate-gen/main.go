// Command migrate-gen generates SQL migration files for event sourcing.
//
// Usage:
//
// go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations -filename init.sql
//
// Or with go generate:
//
// //go:generate go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pupsourcing/core/es/migrations"
)

func main() {
	var (
		outputFolder   = flag.String("output", "migrations", "Output folder for migration file")
		outputFilename = flag.String("filename", "", "Output filename (default: timestamp-based)")
		eventsTable    = flag.String("events-table", "events", "Name of events table")
	)

	flag.Parse()

	config := migrations.DefaultConfig()
	config.OutputFolder = *outputFolder
	config.EventsTable = *eventsTable

	if *outputFilename != "" {
		config.OutputFilename = *outputFilename
	}

	err := migrations.GeneratePostgres(&config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating migration: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated PostgreSQL migration: %s/%s\n", config.OutputFolder, config.OutputFilename)
}
