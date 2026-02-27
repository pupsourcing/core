// Command migrate-gen generates SQL migration files for event sourcing.
//
// Usage:
//
//	go run github.com/getpup/pupsourcing/cmd/migrate-gen -output migrations -filename init.sql
//
// Or with go generate:
//
//	//go:generate go run github.com/getpup/pupsourcing/cmd/migrate-gen -output migrations
//
// Generate migrations for different database adapters:
//
//	go run github.com/getpup/pupsourcing/cmd/migrate-gen -adapter postgres -output migrations
//	go run github.com/getpup/pupsourcing/cmd/migrate-gen -adapter mysql -output migrations
//	go run github.com/getpup/pupsourcing/cmd/migrate-gen -adapter sqlite -output migrations
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/getpup/pupsourcing/es/migrations"
)

func main() {
	var (
		adapter          = flag.String("adapter", "postgres", "Database adapter: postgres, mysql, or sqlite")
		outputFolder     = flag.String("output", "migrations", "Output folder for migration file")
		outputFilename   = flag.String("filename", "", "Output filename (default: timestamp-based)")
		eventsTable      = flag.String("events-table", "events", "Name of events table")
		checkpointsTable = flag.String("checkpoints-table", "consumer_checkpoints", "Name of checkpoints table")
	)

	flag.Parse()

	config := migrations.DefaultConfig()
	config.OutputFolder = *outputFolder
	config.EventsTable = *eventsTable
	config.CheckpointsTable = *checkpointsTable

	if *outputFilename != "" {
		config.OutputFilename = *outputFilename
	}

	var err error
	switch *adapter {
	case "postgres":
		err = migrations.GeneratePostgres(&config)
	case "mysql":
		err = migrations.GenerateMySQL(&config)
	case "sqlite":
		err = migrations.GenerateSQLite(&config)
	default:
		fmt.Fprintf(os.Stderr, "Error: unsupported adapter '%s'. Supported adapters are: postgres, mysql, sqlite\n", *adapter)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating migration: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated %s migration: %s/%s\n", *adapter, config.OutputFolder, config.OutputFilename)
}
