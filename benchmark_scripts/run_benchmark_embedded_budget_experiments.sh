#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPENMLS_DIR="$(cd "$ROOT_DIR/../OpenMLS_containerized" && pwd)"

MODE="${MODE:-smoke}"
OUTPUT_DIR="${OUTPUT_DIR:-benchmark_output}"
WORKERS="${WORKERS:-4}"
BASE_WORKER_PORT="${BASE_WORKER_PORT:-18081}"
DS_PORT="${DS_PORT:-13000}"
RELAY_PORT="${RELAY_PORT:-14000}"
CPU_AFFINITY_SAMPLE_SECONDS="${CPU_AFFINITY_SAMPLE_SECONDS:-0}"
EMBEDDED_DOCKER_MEMORY="${EMBEDDED_DOCKER_MEMORY:-256m}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"

if [[ "$MODE" == "smoke" ]]; then
  EMBEDDED_HEAP_BUDGETS="${EMBEDDED_HEAP_BUDGETS:-1k,2m}"
  EMBEDDED_CPU_FRACTIONS="${EMBEDDED_CPU_FRACTIONS:-1.00,0.10}"
  PROFILE_INDICES=(${PROFILE_INDICES:-0 3})
  MIN_SIZE="${MIN_SIZE:-2}"
  MAX_SIZE="${MAX_SIZE:-4}"
  STEP_SIZE="${STEP_SIZE:-2}"
  ROUNDTRIPS="${ROUNDTRIPS:-1}"
  UPDATE_ROUNDS="${UPDATE_ROUNDS:-0}"
  APP_ROUNDS="${APP_ROUNDS:-0}"
elif [[ "$MODE" == "production" ]]; then
  EMBEDDED_HEAP_BUDGETS="${EMBEDDED_HEAP_BUDGETS:-32k,64k,128k,256k,512k,1m,2m}"
  EMBEDDED_CPU_FRACTIONS="${EMBEDDED_CPU_FRACTIONS:-1.00,0.50,0.25,0.10,0.05}"
  PROFILE_INDICES=(${PROFILE_INDICES:-0 1 2 3 4 5 6 7 8 9})
  MIN_SIZE="${MIN_SIZE:-2}"
  MAX_SIZE="${MAX_SIZE:-32}"
  STEP_SIZE="${STEP_SIZE:-2}"
  ROUNDTRIPS="${ROUNDTRIPS:-3}"
  UPDATE_ROUNDS="${UPDATE_ROUNDS:-1}"
  APP_ROUNDS="${APP_ROUNDS:-1}"
else
  echo "MODE must be smoke or production, got: $MODE" >&2
  exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available on PATH; embedded-budget experiments require Docker." >&2
  exit 127
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not available; embedded-budget experiments require Docker." >&2
  exit 127
fi

mkdir -p "$OPENMLS_DIR/$OUTPUT_DIR"

echo "[embedded-budget] mode=$MODE workers=$WORKERS profiles=${PROFILE_INDICES[*]}"
echo "[embedded-budget] heap_budgets=$EMBEDDED_HEAP_BUDGETS cpu_fractions=$EMBEDDED_CPU_FRACTIONS docker_memory=$EMBEDDED_DOCKER_MEMORY build_images=$BUILD_IMAGES"

for PROFILE_INDEX in "${PROFILE_INDICES[@]}"; do
  TS="$(date -u +%Y%m%d_%H%M%S)"
  RUN_ID="embedded_budget_${MODE}_p${PROFILE_INDEX}_${TS}"
  LOG_PATH="$OPENMLS_DIR/$OUTPUT_DIR/${RUN_ID}.log"

  echo "[embedded-budget] starting run_id=$RUN_ID profile_index=$PROFILE_INDEX"
  set +e
  (
    cd "$OPENMLS_DIR"
    BUILD_IMAGE_ARGS=()
    if [[ "$BUILD_IMAGES" == "1" || "$BUILD_IMAGES" == "true" || "$BUILD_IMAGES" == "yes" ]]; then
      BUILD_IMAGE_ARGS+=(--build-images)
    fi
    python3 scripts/run_compose_benchmark.py \
      "${BUILD_IMAGE_ARGS[@]}" \
      --run-id "$RUN_ID" \
      --output-dir "$OUTPUT_DIR" \
      --workers "$WORKERS" \
      --worker-layout-mode hybrid \
      --singleton-min-count 2 \
      --singleton-fraction 0.50 \
      --packed-clients-per-container 4 \
      --profiled-singleton-count 1 \
      --resource-experiment embedded-budget-singleton \
      --resource-profile-index "$PROFILE_INDEX" \
      --resource-failure-policy stop-on-profiled-failure \
      --embedded-heap-budgets "$EMBEDDED_HEAP_BUDGETS" \
      --embedded-cpu-fractions "$EMBEDDED_CPU_FRACTIONS" \
      --embedded-cpu-cores 1 \
      --embedded-docker-memory "$EMBEDDED_DOCKER_MEMORY" \
      --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE_SECONDS" \
      --min-size "$MIN_SIZE" \
      --max-size "$MAX_SIZE" \
      --step-size "$STEP_SIZE" \
      --roundtrips "$ROUNDTRIPS" \
      --update-rounds "$UPDATE_ROUNDS" \
      --app-rounds "$APP_ROUNDS" \
      --payload-sizes 16 \
      --base-worker-port "$BASE_WORKER_PORT" \
      --ds-port "$DS_PORT" \
      --relay-port "$RELAY_PORT" \
      --wipe-run-dir
  ) 2>&1 | tee "$LOG_PATH"
  BENCH_STATUS=${PIPESTATUS[0]}
  set -e
  if [[ "$BENCH_STATUS" -ne 0 ]]; then
    echo "[embedded-budget] benchmark exited with status $BENCH_STATUS; validating sidecars before deciding outcome"
  fi

  RUN_DIR="$OPENMLS_DIR/$OUTPUT_DIR/$RUN_ID"
  echo "[embedded-budget] validating $RUN_DIR"
  python3 "$OPENMLS_DIR/scripts/validate_resource_experiment_outputs.py" "$RUN_DIR"
  echo "[embedded-budget] validation passed for run_id=$RUN_ID"
done

echo "[embedded-budget] completed mode=$MODE"
