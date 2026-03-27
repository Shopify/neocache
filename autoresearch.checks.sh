#!/bin/bash
set -euo pipefail

cd /Users/samobeid/src/github.com/shopify-playground/s3dashmap

# Run all tests — suppress success output, only show errors
cargo test 2>&1 | grep -E '(FAILED|error|panicked|test result:)' || true

# Check for test failures
cargo test 2>&1 | tail -1 | grep -q "test result: ok" || {
    echo "TESTS FAILED"
    cargo test 2>&1 | tail -20
    exit 1
}
