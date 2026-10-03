POSTGRES_PORT ?= 5544
DATABASE_URL  ?= postgres://queue:queue-local-only@127.0.0.1:$(POSTGRES_PORT)/durable_queue?sslmode=disable
export POSTGRES_PORT

.DEFAULT_GOAL := help
.PHONY: help up down test test-unit test-integration simulate lint clean

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{ printf "  make %-17s %s\n", $$1, $$2 }'

up: ## Start PostgreSQL 16 and wait until it is ready
	docker compose up -d --wait

down: ## Stop PostgreSQL (keeps nothing: no volume is declared)
	docker compose down

test-unit: ## Run unit tests (no database needed)
	go test ./internal/...

test-integration: up ## Run integration tests against PostgreSQL
	TEST_DATABASE_URL="$(DATABASE_URL)" go test -count=1 -p 1 ./tests/...

test: test-unit test-integration ## Run all tests

simulate: up ## Drain a 300-order batch against a degraded fake upstream and print metrics
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/simulate

lint: ## Run go vet
	go vet ./...

clean: down ## Stop the database and remove build artifacts
	go clean ./...
