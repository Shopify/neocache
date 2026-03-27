#!/bin/bash
set -euo pipefail

cd /Users/samobeid/src/github.com/shopify-playground/s3dashmap

# Run all tests — cargo test exits non-zero on failure, which set -e catches
cargo test 2>&1 | tail -5
