// Package migrations provides SQL migration generation.
//
// To generate migrations, use the migrate-gen command:
//
//	go run github.com/pupsourcing/core/cmd/migrate-gen -output migrations
//
// Or add a go generate directive to your code:
//
//	//go:generate go run github.com/pupsourcing/core/cmd/migrate-gen -output ../../migrations
//
// Then run:
//
//	go generate ./...
package migrations

//go:generate go run ../../cmd/migrate-gen -output example_migrations -filename example.sql
