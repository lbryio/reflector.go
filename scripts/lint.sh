#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."

if ! command -v golangci-lint &> /dev/null; then
    echo "Installing golangci-lint..."
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
else
    echo "Ensuring golangci-lint is up to date..."
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
fi

echo "Running golangci-lint..."
golangci-lint run --config .golangci.yml ./...