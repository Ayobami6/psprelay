GOPATH ?= $(shell go env GOPATH)
export PATH := $(GOPATH)/bin:$(PATH)

AIR := $(shell which air 2>/dev/null || echo $(GOPATH)/bin/air)
COVERAGE_FILE ?= coverage.out
BIN_DIR ?= bin

.PHONY: run dev build test test-cov test-cov-html lint security ci docker-build docker-up docker-down

run:
	@go run cmd/main.go

dev:
	@which air > /dev/null 2>&1 || [ -x "$(AIR)" ] || (echo "Air is not in PATH. Install via: go install github.com/air-verse/air@latest" && exit 1)
	@$(AIR)

build:
	@mkdir -p $(BIN_DIR)
	@CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/psprelay cmd/main.go

test:
	@go test -v -race ./...

test-cov:
	@go test -race -coverpkg=./... -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	@go tool cover -func=$(COVERAGE_FILE)

test-cov-html: test-cov
	@go tool cover -html=$(COVERAGE_FILE) -o coverage.html

lint:
	@golangci-lint run --timeout 5m ./...

security:
	@go run golang.org/x/vuln/cmd/govulncheck@latest ./...

ci: lint test build

docker-build:
	@docker build -t psprelay:latest .

docker-up:
	@docker compose up -d

docker-down:
	@docker compose down
