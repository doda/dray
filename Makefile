.PHONY: all build test lint clean fmt vet deps

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"

all: deps lint test build

deps:
	go mod download
	go mod tidy

build:
	go build $(LDFLAGS) -o bin/drayd ./cmd/drayd

test:
	go test -race -v ./...

lint: vet
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping lint"; \
	fi

vet:
	go vet ./...

fmt:
	gofmt -s -w .
	goimports -w -local github.com/dray-io/dray .

clean:
	rm -rf bin/
	go clean -cache -testcache

# Run the broker
run: build
	./bin/drayd

# Generate coverage report
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
