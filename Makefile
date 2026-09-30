BINARY      := distributed-drive
CMD         := ./cmd/server
BIN_DIR     := bin
COVER_FILE  := coverage.txt
LINT_VERSION := v2.13.0

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

## --- build ---------------------------------------------------------------

.PHONY: build
build: ## Build the server binary into ./bin
	@mkdir -p $(BIN_DIR)
	go build -trimpath -o $(BIN_DIR)/$(BINARY) $(CMD)

.PHONY: run
run: ## Run the server (reads .env if present)
	go run $(CMD)

.PHONY: tidy
tidy: ## Sync go.mod / go.sum
	go mod tidy

.PHONY: clean
clean: ## Remove build and coverage artifacts
	rm -rf $(BIN_DIR) $(COVER_FILE) coverage.html

## --- verify --------------------------------------------------------------

.PHONY: fmt
fmt: ## Format code
	gofmt -w -s .

.PHONY: fmt-check
fmt-check: ## Fail if code is not gofmt'd
	@unformatted=$$(gofmt -l -s .); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: test
test: ## Run unit tests
	go test ./...

.PHONY: test-race
test-race: ## Run unit tests with the race detector
	go test -race ./...

.PHONY: cover
cover: ## Run tests and report coverage
	go test -covermode=atomic -coverprofile=$(COVER_FILE) ./...
	go tool cover -func=$(COVER_FILE) | tail -n 1

.PHONY: lint
lint: ## Run golangci-lint (skips with a hint if not installed)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed. Install with:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(LINT_VERSION)"; \
		exit 1; \
	fi

.PHONY: check
check: fmt-check vet test ## Run every gate required before a phase is considered done

## --- docker --------------------------------------------------------------

.PHONY: docker-build
docker-build: ## Build the backend image
	docker build -t $(BINARY):dev .

.PHONY: docker-up
docker-up: ## Start the backend with docker compose
	docker compose up --build

.PHONY: docker-down
docker-down: ## Stop the compose stack
	docker compose down -v
