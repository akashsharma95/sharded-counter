.PHONY: help test test-unit test-integration docker-up docker-down docker-logs clean lint

# Default target
help:
	@echo "Available targets:"
	@echo "  make test              - Run all tests (unit + integration)"
	@echo "  make test-unit         - Run unit tests only"
	@echo "  make test-integration  - Run integration tests with LocalStack"
	@echo "  make docker-up         - Start LocalStack container"
	@echo "  make docker-down       - Stop and remove LocalStack container"
	@echo "  make docker-logs       - Show LocalStack logs"
	@echo "  make clean             - Clean test artifacts and stop containers"
	@echo "  make lint              - Run golangci-lint"
	@echo "  make deps              - Install required dependencies"

# Run all tests
test: test-unit test-integration

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	go test -v -race -coverprofile=coverage-unit.out -covermode=atomic ./... -short

# Run integration tests with LocalStack
test-integration: docker-up
	@echo "Waiting for LocalStack to be ready..."
	@sleep 5
	@echo "Running integration tests..."
	go test -v -race -tags=integration -coverprofile=coverage-integration.out -covermode=atomic -timeout 5m ./...

# Start LocalStack
docker-up:
	@echo "Starting LocalStack..."
	docker-compose up -d
	@echo "Waiting for LocalStack to be healthy..."
	@timeout 60 sh -c 'until docker-compose ps | grep -q "healthy"; do sleep 2; done' || \
		(echo "LocalStack failed to become healthy" && docker-compose logs && exit 1)
	@echo "LocalStack is ready!"

# Stop LocalStack
docker-down:
	@echo "Stopping LocalStack..."
	docker-compose down -v

# Show LocalStack logs
docker-logs:
	docker-compose logs -f localstack

# Clean up
clean: docker-down
	@echo "Cleaning up..."
	rm -f coverage-*.out
	rm -rf .localstack
	go clean -testcache

# Run linter
lint:
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --timeout=5m ./...; \
	else \
		echo "golangci-lint not installed. Install from: https://golangci-lint.run/welcome/install/"; \
		exit 1; \
	fi

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy
	@echo "Dependencies installed!"

# Quick check - runs unit tests and linting
check: test-unit lint
	@echo "Quick check passed!"

# CI target - for continuous integration
ci: deps test
	@echo "CI pipeline completed!"

# Development workflow - start LocalStack in background
dev-up: docker-up
	@echo "Development environment ready!"
	@echo "Run 'make test-integration' to test or 'make docker-down' to stop"

# Coverage report
coverage: test
	@echo "Generating coverage report..."
	go tool cover -html=coverage-integration.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Benchmark
benchmark: docker-up
	@echo "Running benchmarks..."
	@sleep 5
	go test -bench=. -benchmem -tags=integration -run=^$$ ./...

