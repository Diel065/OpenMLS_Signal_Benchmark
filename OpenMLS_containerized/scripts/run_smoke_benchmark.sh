#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="${SMOKE_OUTPUT_DIR:-$REPO_ROOT/benchmark_output/smoke_coverage}"
RUN_ID="${SMOKE_RUN_ID:-smoke-coverage-$(date +%Y%m%d_%H%M%S)}"

echo "[smoke] Repo root: $REPO_ROOT"
echo "[smoke] Output dir: $OUTPUT_DIR"
echo "[smoke] Run ID: $RUN_ID"

# Build the benchmark runner binary if not already built
if [ ! -f "$REPO_ROOT/target/debug/benchmark_runner_http_staircase_local" ]; then
    echo "[smoke] Building benchmark runner binary..."
    cargo build --manifest-path "$REPO_ROOT/Cargo.toml" --bin benchmark_runner_http_staircase_local
fi

# Exercise multiple N and k values while remaining small enough for local validation.
echo "[smoke] Running smoke benchmark..."
cargo run --manifest-path "$REPO_ROOT/Cargo.toml" --bin benchmark_runner_http_staircase_local -- \
    --spawn-local-workers 16 \
    --min-size 2 \
    --max-size 16 \
    --step-size 7 \
    --roundtrips 1 \
    --update-rounds 0 \
    --max-update-samples-per-plateau 0 \
    --app-rounds 0 \
    --payload-sizes "32" \
    --run-id "$RUN_ID" \
    --scenario "smoke-coverage-test" \
    --scenario-seed 1 \
    --output-dir "$OUTPUT_DIR" \
    --max-commit-receive-samples-per-plateau 8 \
    --commit-receive-sampling-seed 42 \
    2>&1

echo "[smoke] Benchmark run complete. Output in $OUTPUT_DIR/$RUN_ID"

# Check output files
RUN_DIR="$OUTPUT_DIR/$RUN_ID"
echo "[smoke] Checking output files..."
ls -la "$RUN_DIR/"

# Check JSONL for commit_receive_protocol
echo "[smoke] Checking JSONL for commit_receive_protocol..."
JSONL_COUNT=$(grep -h -c '"op":"commit_receive_protocol"' "$RUN_DIR"/client-*.jsonl | awk '{s+=$1} END {print s}' || echo "0")
echo "[smoke] commit_receive_protocol rows in JSONL: $JSONL_COUNT"

# Check CSV for commit_receive_protocol
if [ -f "$RUN_DIR/events.csv" ]; then
    CSV_COUNT=$(grep -c 'commit_receive_protocol' "$RUN_DIR/events.csv" || echo "0")
    echo "[smoke] commit_receive_protocol rows in CSV: $CSV_COUNT"
    echo "[smoke] Total CSV rows: $(tail -n +2 "$RUN_DIR/events.csv" | wc -l)"
else
    echo "[smoke] WARNING: events.csv not found"
    CSV_COUNT="0"
fi

# The strict validator is part of the smoke contract; any failure aborts the script.
echo "[smoke] Running validator..."
python3 "$REPO_ROOT/scripts/validate_benchmark_outputs.py" \
    "$RUN_DIR" \
    --require-k-values 1
echo "[smoke] Validator PASSED"

# Report results
echo ""
echo "=== SMOKE BENCHMARK RESULTS ==="
echo "JSONL commit_receive_protocol rows: $JSONL_COUNT"
echo "CSV commit_receive_protocol rows: $CSV_COUNT"
echo "Output directory: $RUN_DIR"

if [ "$JSONL_COUNT" -gt 0 ] && [ "$CSV_COUNT" -gt 0 ]; then
    echo "STATUS: PASS - commit_receive_protocol rows present in both JSONL and CSV"
    exit 0
else
    echo "STATUS: FAIL - commit_receive_protocol rows missing"
    exit 1
fi
