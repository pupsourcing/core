.PHONY: help test test-unit test-integration test-integration-local lint fmt build

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: test-unit ## Run all tests

test-unit: ## Run unit tests
	go test -v -race -coverprofile=coverage.out ./...

test-integration: ## Run integration tests (requires databases)
	go test -p 1 -v -tags=integration ./...

test-integration-local: ## Start databases and run integration tests locally
	docker compose up -d
	@until docker compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do \
		sleep 1; \
	done
	@until docker compose exec -T mysql mysql -h localhost -u root -proot -e "SELECT 1" > /dev/null 2>&1; do \
		sleep 1; \
	done
	@until docker compose exec -T mysql mysql -h localhost -u root -proot -e "USE pupsourcing_test; SELECT 1" > /dev/null 2>&1; do \
		echo "MySQL database not ready - sleeping"; \
		sleep 1; \
	done
	POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_USER=postgres POSTGRES_PASSWORD=postgres POSTGRES_DB=pupsourcing_test \
	MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_USER=test MYSQL_PASSWORD=test MYSQL_DB=pupsourcing_test \
	go test -p 1 -v -tags=integration ./... || true
	docker compose down

lint: ## Run linter
	golangci-lint run --timeout=5m

fmt: ## Format code
	gofmt -w -s .
	goimports -w -local github.com/pupsourcing/core .

build: ## Build all packages
	go build -v ./...
