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

cd "$BENCH_DIR"

# Run benchmark 3 times for stability, take median eff_ops_sec
declare -a EFF_OPS_RUNS=()
declare -a OPS_SEC_RUNS=()
declare -a LINES=()

for run in 1 2 3 4 5; do
    OUTPUT=$(cargo run --release -- --caches s3dashmap 2>&1)
    LINE=$(echo "$OUTPUT" | grep -E '^\s*s3dashmap\s')
    if [ -z "$LINE" ]; then
        echo "ERROR: no s3dashmap result found in run $run"
        echo "$OUTPUT"
        exit 1
    fi
    LINES+=("$LINE")
    EFF=$(echo "$LINE" | awk '{print $8}')
    OPS=$(echo "$LINE" | awk '{print $2}')
    EFF_OPS_RUNS+=("$EFF")
    OPS_SEC_RUNS+=("$OPS")
done

# Sort and pick the middle value (index 2 for 5 runs)
SORTED_EFF=($(printf '%s\n' "${EFF_OPS_RUNS[@]}" | sort -n))
MEDIAN_EFF="${SORTED_EFF[2]}"

# Find the run that produced the median eff_ops
for i in 0 1 2 3 4; do
    if [ "${EFF_OPS_RUNS[$i]}" = "$MEDIAN_EFF" ]; then
        LINE="${LINES[$i]}"
        break
    fi
done

# Extract fields from the median run
OPS_SEC=$(echo "$LINE" | awk '{print $2}')
CV_PCT=$(echo "$LINE" | awk '{gsub(/%/,""); print $3}')
P50_US=$(echo "$LINE" | awk '{print $4}')
P99_US=$(echo "$LINE" | awk '{print $5}')
TAIL_US=$(echo "$LINE" | awk '{print $6}')
HIT_PCT=$(echo "$LINE" | awk '{gsub(/%/,""); print $7}')
EFF_OPS=$(echo "$LINE" | awk '{print $8}')

echo "METRIC eff_ops_sec=$EFF_OPS"
echo "METRIC ops_sec=$OPS_SEC"
echo "METRIC hit_pct=$HIT_PCT"
echo "METRIC p50_us=$P50_US"
echo "METRIC p99_us=$P99_US"
echo "METRIC cv_pct=$CV_PCT"
echo "METRIC tail_us=$TAIL_US"

echo ""
echo "5 runs: eff_ops = ${EFF_OPS_RUNS[0]} / ${EFF_OPS_RUNS[1]} / ${EFF_OPS_RUNS[2]} / ${EFF_OPS_RUNS[3]} / ${EFF_OPS_RUNS[4]}"
echo "Median run: $LINE"
