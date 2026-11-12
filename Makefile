GOPATH ?= $(shell go env GOPATH)
export PATH := $(GOPATH)/bin:$(PATH)

AIR := $(shell which air 2>/dev/null || echo $(GOPATH)/bin/air)

.PHONY: run dev build test lint docker-build docker-up docker-down

run:
	@go run cmd/main.go

dev:
	@which air > /dev/null 2>&1 || [ -x "$(AIR)" ] || (echo "Air is not in PATH. Install via: go install github.com/air-verse/air@latest" && exit 1)
	@$(AIR)

build:
	@go build -v -o bin/psprelay cmd/main.go

test:
	@go test -v -race ./...

lint:
	@golangci-lint run ./...

docker-build:
	@docker build -t psprelay:latest .

docker-up:
	@docker compose up -d

docker-down:
	@docker compose down
