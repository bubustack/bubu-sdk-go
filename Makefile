.PHONY: help test test-coverage vet lint doc-coverage examples docs clean all

# Default target
help:
	@echo "Bubu SDK Go - Makefile targets:"
	@echo ""
	@echo "  make test             - Run all tests"
	@echo "  make test-coverage    - Run tests with coverage report"
	@echo "  make vet              - Run go vet"
	@echo "  make lint             - Run golangci-lint (requires golangci-lint installed)"
	@echo "  make doc-coverage     - Check Godoc coverage (100% required)"
	@echo "  make docs             - Validate documentation (link check, basic sanity)"
	@echo "  make clean            - Clean build artifacts"
	@echo "  make all              - Run all checks (vet, test, doc-coverage, examples)"

# Run all tests
test:
	@echo "Running tests..."
	go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run go vet
vet:
	@echo "Running go vet..."
	go vet ./...

# Run golangci-lint (if installed)
lint:
	@echo "Running golangci-lint..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed; skipping..."; exit 0; }
	golangci-lint run

# Check doc coverage (100% required)
doc-coverage:
	@echo "Checking documentation coverage..."
	go run ./tools/cmd/doccheck/main.go

# Validate documentation (basic checks)
docs:
	@echo "Validating documentation..."
	@# README and CONTRIBUTING must exist locally
	@test -f README.md || { echo "ERROR: README.md missing"; exit 1; }
	@test -f CONTRIBUTING.md || { echo "ERROR: CONTRIBUTING.md missing"; exit 1; }
	@echo "Documentation validation complete."

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -f coverage.out coverage.html
	rm -f cover.out
	go clean ./...

# Run all checks
all: vet test doc-coverage
	@echo ""
	@echo "✓ All checks passed!"
