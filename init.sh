#!/bin/bash
set -e

echo "=== Dray Project Initialization ==="

# Ensure Go is in PATH
export PATH="$PATH:/usr/local/go/bin"

# Verify Go is available
if ! command -v go &> /dev/null; then
    echo "ERROR: Go is not installed or not in PATH"
    exit 1
fi

echo "Go version: $(go version)"

# Change to project directory
cd "$(dirname "$0")"

echo ""
echo "=== Downloading dependencies ==="
go mod download
go mod tidy

echo ""
echo "=== Running go vet ==="
go vet ./...

echo ""
echo "=== Building project ==="
go build -o bin/drayd ./cmd/drayd

echo ""
echo "=== Running tests ==="
go test -v ./...

echo ""
echo "=== Smoke test: running drayd --version ==="
./bin/drayd -version

echo ""
echo "=== Project initialization complete ==="
echo "Run 'make' for full build with linting"
