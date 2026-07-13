#!/usr/bin/env bash
#
# Calibration run 02: unconstrained container baseline.
# Produces events.csv for every run.
#
# PROTOCOL env: openmls | signal | both (default: openmls)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"
SIGNAL_DIR="$REPO_ROOT/Signal_containerized"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

PROTOCOL="${PROTOCOL:-openmls}"

N="${N:-3}"
STRICT_CPUSET="${STRICT_CPUSET:-1}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"

WORKERS="${WORKERS:-1024}"
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-10}"
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-10}"
SINGLETON_FRACTION="${SINGLETON_FRACTION:-0.000000001}"
PACKED_PER_CONTAINER="${PACKED_PER_CONTAINER:-64}"
PACKED_INTERNAL_PARALLELISM="${PACKED_INTERNAL_PARALLELISM:-16}"
BRIDGE_COUNT="${BRIDGE_COUNT:-2}"

MAX_GROUP_SIZE="${MAX_GROUP_SIZE:-1024}"
STEP_SIZE="${STEP_SIZE:-16}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
UPDATE_ROUNDS="${UPDATE_ROUNDS:-4}"
APP_ROUNDS="${APP_ROUNDS:-4}"
PLATEAU_ORDER="${PLATEAU_ORDER:-staircase}"
MAX_UPDATE_SAMPLES_PER_PLATEAU="${MAX_UPDATE_SAMPLES_PER_PLATEAU:-4}"
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-4}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-32,256,512,2048}"

CPU_AFFINITY_SAMPLE="${CPU_AFFINITY_SAMPLE:-20}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"

# Signal-specific defaults (smaller sizes, pairwise semantics)
SIGNAL_MAX_CONVERSATION_SIZE="${SIGNAL_MAX_CONVERSATION_SIZE:-256}"
SIGNAL_STEP_SIZE="${SIGNAL_STEP_SIZE:-16}"
SIGNAL_ROUNDTRIPS="${SIGNAL_ROUNDTRIPS:-1}"
SIGNAL_APP_ROUNDS="${SIGNAL_APP_ROUNDS:-4}"
SIGNAL_WORKERS="${SIGNAL_WORKERS:-256}"
SIGNAL_PACKED_PER_CONTAINER="${SIGNAL_PACKED_PER_CONTAINER:-64}"

export PATH="$HOME/.cargo/bin:$PATH"

raise_nofile_limit() {
  ulimit -n "$NOFILE_LIMIT" 2>/dev/null || {
    echo "WARN: could not raise nofile limit to $NOFILE_LIMIT; current limit is $(ulimit -n)" >&2
  }
}

python_bin() {
  [ -x "$OPENMLS_DIR/.venv/bin/python" ] && printf '%s\n' "$OPENMLS_DIR/.venv/bin/python" || printf '%s\n' python3
}

python_bin_signal() {
  [ -x "$SIGNAL_DIR/.venv/bin/python" ] && printf '%s\n' "$SIGNAL_DIR/.venv/bin/python" || printf '%s\n' python3
}

cleanup_docker() {
  docker compose -f "$OPENMLS_DIR/docker-compose.yml" down --timeout 2 2>/dev/null || true
  for f in "$OPENMLS_DIR"/docker-compose_benchmark_*.yml "$OPENMLS_DIR"/docker-compose.*.generated.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=mls-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
}

cleanup_docker_signal() {
  for f in "$SIGNAL_DIR"/docker-compose.*.generated.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  docker container ls -aq --filter "name=signal-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=signal-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
}

require_events_csv() {
  local run_dir="$1"
  test -s "$run_dir/events.csv" || {
    echo "ERROR: missing or empty events.csv in $run_dir" >&2
    return 1
  }
}

affinity_mode() {
  local cpus
  cpus="$(nproc 2>/dev/null || echo 0)"
  if [ "$cpus" -ge "$PROFILED_SINGLETON_COUNT" ]; then
    printf '%s\n' "profiled-nor-background"
    return
  fi
  if [ "$STRICT_CPUSET" = "1" ]; then
    echo "ERROR: need >= $PROFILED_SINGLETON_COUNT online CPUs, have $cpus" >&2
    return 1
  fi
  echo "WARN: only $cpus CPUs; running without strict profiled-core affinity" >&2
  printf '%s\n' "none"
}

run_baseline() {
  local iter="$1"
  local run_id="cal02_unconstrained_container_baseline_i${iter}_${DATE_TAG}"
  local run_dir="$OPENMLS_DIR/benchmark_output/$run_id"
  local scenario_seed singleton_selection_seed mode py
  local -a image_args

  scenario_seed="$(shuf -i 1-2147483647 -n 1)"
  singleton_selection_seed="$(shuf -i 1-2147483647 -n 1)"
  mode="$(affinity_mode)"
  py="$(python_bin)"
  image_args=()
  if [ "$BUILD_IMAGES" = "1" ]; then
    image_args=(--build-images)
  fi

  echo "===== [02/baseline] iteration $iter/$N run_id=$run_id ====="
  cd "$OPENMLS_DIR"
  "$py" scripts/run_compose_benchmark.py \
    --workers "$WORKERS" \
    --run-id "$run_id" \
    --scenario-seed "$scenario_seed" \
    --singleton-selection-seed "$singleton_selection_seed" \
    --output-dir benchmark_output \
    --worker-layout-mode hybrid \
    --singleton-min-count "$SINGLETON_MIN_COUNT" \
    --singleton-fraction "$SINGLETON_FRACTION" \
    --packed-clients-per-container "$PACKED_PER_CONTAINER" \
    --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM" \
    --bridge-count "$BRIDGE_COUNT" \
    --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
    --cpu-affinity-mode "$mode" \
    --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE" \
    --health-timeout-seconds "$HEALTH_TIMEOUT" \
    --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT" \
    --health-poll-seconds 0.5 \
    --worker-health-poll-ms 250 \
    --min-size 2 \
    --max-size "$MAX_GROUP_SIZE" \
    --step-size "$STEP_SIZE" \
    --plateau-order "$PLATEAU_ORDER" \
    --roundtrips "$ROUNDTRIPS" \
    --update-rounds "$UPDATE_ROUNDS" \
    --app-rounds "$APP_ROUNDS" \
    --max-update-samples-per-plateau "$MAX_UPDATE_SAMPLES_PER_PLATEAU" \
    --add-batch-extremes-only \
    --max-app-samples-per-payload "$MAX_APP_SAMPLES_PER_PAYLOAD" \
    --payload-sizes "$PAYLOAD_SIZES" \
    --http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-outbound-http-permits "$WORKER_OUTBOUND_PERMITS" \
    --max-fanout-parallelism "$FANOUT_PARALLELISM" \
    --min-fanout-parallelism "$FANOUT_MIN" \
    --force-cleanup-mls-ports \
    --runner-in-docker \
    --no-aggregate \
    "${image_args[@]}"
  require_events_csv "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker
}

run_signal_baseline() {
  local iter="$1"
  local run_id="cal02_signal_unconstrained_baseline_i${iter}_${DATE_TAG}"
  local run_dir="$SIGNAL_DIR/benchmark_output/$run_id"
  local singleton_selection_seed py
  local -a image_args

  singleton_selection_seed="$(shuf -i 1-2147483647 -n 1)"
  py="$(python_bin_signal)"
  image_args=()
  if [ "$BUILD_IMAGES" = "1" ]; then
    image_args=(--build-images)
  fi

  echo "===== [02/Signal-baseline] iteration $iter/$N run_id=$run_id ====="
  cd "$SIGNAL_DIR"
  "$py" scripts/run_compose_benchmark.py \
    --workers "$SIGNAL_WORKERS" \
    --run-id "$run_id" \
    --singleton-selection-seed "$singleton_selection_seed" \
    --output-dir benchmark_output \
    --worker-layout-mode hybrid \
    --singleton-min-count "$SINGLETON_MIN_COUNT" \
    --singleton-fraction "$SINGLETON_FRACTION" \
    --packed-clients-per-container "$SIGNAL_PACKED_PER_CONTAINER" \
    --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM" \
    --bridge-count "$BRIDGE_COUNT" \
    --health-timeout-seconds "$HEALTH_TIMEOUT" \
    --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT" \
    --health-poll-seconds 0.5 \
    --worker-health-poll-ms 250 \
    --min-size 2 \
    --max-size "$SIGNAL_MAX_CONVERSATION_SIZE" \
    --step-size "$SIGNAL_STEP_SIZE" \
    --roundtrips "$SIGNAL_ROUNDTRIPS" \
    --app-rounds "$SIGNAL_APP_ROUNDS" \
    --max-app-samples-per-payload "$MAX_APP_SAMPLES_PER_PAYLOAD" \
    --payload-sizes "$PAYLOAD_SIZES" \
    --http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-outbound-http-permits "$WORKER_OUTBOUND_PERMITS" \
    --max-fanout-parallelism "$FANOUT_PARALLELISM" \
    --force-cleanup-signal-ports \
    --runner-in-docker \
    "${image_args[@]}"
  require_events_csv "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker_signal
}

raise_nofile_limit

# ── OpenMLS baseline ────────────────────────────────────────────────────────
if [ "$PROTOCOL" = "openmls" ] || [ "$PROTOCOL" = "both" ]; then
  cleanup_docker
  for iter in $(seq 1 "$N"); do
    run_baseline "$iter"
  done
  echo "All unconstrained container OpenMLS baseline runs completed."
fi

# ── Signal baseline ─────────────────────────────────────────────────────────
if [ "$PROTOCOL" = "signal" ] || [ "$PROTOCOL" = "both" ]; then
  cleanup_docker_signal
  for iter in $(seq 1 "$N"); do
    run_signal_baseline "$iter"
  done
  echo "All unconstrained container Signal baseline runs completed."
fi

echo "All unconstrained container baseline runs completed."
