#!/bin/bash
set -euo pipefail

BENCH_DIR="/Users/samobeid/src/github.com/shopify-playground/rust-cache-benchmarks"
S3DASH_DIR="/Users/samobeid/src/github.com/shopify-playground/s3dashmap"

# Quick syntax check — fails fast on compile errors
cd "$S3DASH_DIR"
cargo check 2>&1 | tail -5
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "COMPILE ERROR"
    exit 1
fi

# Run benchmark (rebuilds s3dashmap automatically via path dep)
cd "$BENCH_DIR"
OUTPUT=$(cargo run --release -- --caches s3dashmap 2>&1)

# Parse the s3dashmap result line
# Format: s3dashmap       25667941  10.0%     0.17     1.67     1.50     84.9%    21785144
LINE=$(echo "$OUTPUT" | grep -E '^\s*s3dashmap\s')

if [ -z "$LINE" ]; then
    echo "ERROR: no s3dashmap result found"
    echo "$OUTPUT"
    exit 1
fi

# Extract fields
OPS_SEC=$(echo "$LINE" | awk '{print $2}')
CV_PCT=$(echo "$LINE" | awk '{gsub(/%/,""); print $3}')
P50_US=$(echo "$LINE" | awk '{print $4}')
P99_US=$(echo "$LINE" | awk '{print $5}')
TAIL_US=$(echo "$LINE" | awk '{print $6}')
HIT_PCT=$(echo "$LINE" | awk '{gsub(/%/,""); print $7}')
EFF_OPS=$(echo "$LINE" | awk '{print $8}')

echo "METRIC ops_sec=$OPS_SEC"
echo "METRIC hit_pct=$HIT_PCT"
echo "METRIC p50_us=$P50_US"
echo "METRIC p99_us=$P99_US"
echo "METRIC cv_pct=$CV_PCT"
echo "METRIC eff_ops_sec=$EFF_OPS"
echo "METRIC tail_us=$TAIL_US"

echo ""
echo "Result: $LINE"
