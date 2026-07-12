#!/usr/bin/env bash
#
# Calibration run 01: constrained container RAM/CPU sweeps.
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
OUTPUT_ROOT="${OUTPUT_ROOT:-}"

N="${N:-3}"
SWEEP="${SWEEP:-both}"
STRICT_CPUSET="${STRICT_CPUSET:-1}"
RESOURCE_OUTPUT_VALIDATION="${RESOURCE_OUTPUT_VALIDATION:-1}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"

WORKERS="${WORKERS:-1024}"
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-10}"
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-10}"
SINGLETON_FRACTION="${SINGLETON_FRACTION:-0.000000001}"
PACKED_PER_CONTAINER="${PACKED_PER_CONTAINER:-256}"
PACKED_INTERNAL_PARALLELISM="${PACKED_INTERNAL_PARALLELISM:-16}"
BRIDGE_COUNT="${BRIDGE_COUNT:-2}"

MAX_GROUP_SIZE="${MAX_GROUP_SIZE:-1024}"
STEP_SIZE="${STEP_SIZE:-16}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
UPDATE_ROUNDS="${UPDATE_ROUNDS:-2}"
APP_ROUNDS="${APP_ROUNDS:-2}"
PLATEAU_ORDER="${PLATEAU_ORDER:-staircase}"
MAX_UPDATE_SAMPLES_PER_PLATEAU="${MAX_UPDATE_SAMPLES_PER_PLATEAU:-2}"
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-2}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-32,2048}"

CPU_AFFINITY_SAMPLE="${CPU_AFFINITY_SAMPLE:-20}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"
CPU_SWEEP_FRACTIONS="${CPU_SWEEP_FRACTIONS:-1.00,0.75,0.50,0.25,0.10,0.05,0.04,0.03,0.02,0.01}"
OPENMLS_RAM_SWEEP_VALUES="${OPENMLS_RAM_SWEEP_VALUES:-32k,64k,128k,512k,1m,2m,8m,32m,256m,1g}"
SIGNAL_RAM_SWEEP_VALUES="${SIGNAL_RAM_SWEEP_VALUES:-8m,16m,32m,64m,128m,256m,512m,1g,2g,3g}"
CPU_THROTTLED_PERIOD_THRESHOLD="${CPU_THROTTLED_PERIOD_THRESHOLD:-0.05}"

# Signal-specific defaults (smaller sizes, pairwise semantics)
SIGNAL_MAX_CONVERSATION_SIZE="${SIGNAL_MAX_CONVERSATION_SIZE:-256}"
SIGNAL_STEP_SIZE="${SIGNAL_STEP_SIZE:-16}"
SIGNAL_ROUNDTRIPS="${SIGNAL_ROUNDTRIPS:-1}"
SIGNAL_APP_ROUNDS="${SIGNAL_APP_ROUNDS:-2}"
SIGNAL_WORKERS="${SIGNAL_WORKERS:-256}"
SIGNAL_PACKED_PER_CONTAINER="${SIGNAL_PACKED_PER_CONTAINER:-64}"

export PATH="$HOME/.cargo/bin:$PATH"
export OPENMLS_CPU_THROTTLED_PERIOD_THRESHOLD="$CPU_THROTTLED_PERIOD_THRESHOLD"

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

output_dir_arg_for() {
  local base_dir="$1"
  if [ -n "$OUTPUT_ROOT" ]; then
    python3 - "$OUTPUT_ROOT" "$base_dir" <<'PY'
import os, sys
print(os.path.relpath(sys.argv[1], sys.argv[2]))
PY
  else
    printf '%s\n' benchmark_output
  fi
}

run_dir_for() {
  local base_dir="$1" run_id="$2"
  if [ -n "$OUTPUT_ROOT" ]; then
    printf '%s/%s\n' "$OUTPUT_ROOT" "$run_id"
  else
    printf '%s/benchmark_output/%s\n' "$base_dir" "$run_id"
  fi
}

require_events_csv() {
  local run_dir="$1"
  test -s "$run_dir/events.csv" || {
    echo "ERROR: missing or empty events.csv in $run_dir" >&2
    return 1
  }
}

validate_resource_output() {
  local run_dir="$1"
  [ "$RESOURCE_OUTPUT_VALIDATION" = "1" ] || return 0
  python3 "$OPENMLS_DIR/scripts/validate_resource_experiment_outputs.py" "$run_dir"
}

validate_resource_output_signal() {
  local run_dir="$1"
  [ "$RESOURCE_OUTPUT_VALIDATION" = "1" ] || return 0
  local validator="$SIGNAL_DIR/scripts/validate_resource_experiment_outputs.py"
  if [ -f "$validator" ]; then
    python3 "$validator" "$run_dir"
  else
    echo "WARN: Signal validator not found at $validator; skipping" >&2
  fi
}

online_cpus() {
  nproc 2>/dev/null || echo 0
}

affinity_mode() {
  local cpus
  cpus="$(online_cpus)"
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

run_sweep() {
  local iter="$1"
  local sweep_type="$2"
  local label="$3"
  local run_id="cal01_${sweep_type}_i${iter}_${DATE_TAG}"
  local run_dir output_arg
  local scenario_seed singleton_selection_seed mode py
  local -a image_args validation_args

  scenario_seed="$(shuf -i 1-2147483647 -n 1)"
  singleton_selection_seed="$(shuf -i 1-2147483647 -n 1)"
  mode="$(affinity_mode)"
  py="$(python_bin)"
  output_arg="$(output_dir_arg_for "$OPENMLS_DIR")"
  run_dir="$(run_dir_for "$OPENMLS_DIR" "$run_id")"
  image_args=()
  if [ "$BUILD_IMAGES" = "1" ]; then
    image_args=(--build-images)
  fi
  validation_args=()
  if [ "$RESOURCE_OUTPUT_VALIDATION" != "1" ]; then
    validation_args=(--no-resource-output-validation)
  fi

  echo "===== [01/$label] iteration $iter/$N run_id=$run_id ====="
  cd "$OPENMLS_DIR"
  "$py" scripts/run_compose_benchmark.py \
    --workers "$WORKERS" \
    --run-id "$run_id" \
    --scenario-seed "$scenario_seed" \
    --singleton-selection-seed "$singleton_selection_seed" \
    --output-dir "$output_arg" \
    --worker-layout-mode hybrid \
    --singleton-min-count "$SINGLETON_MIN_COUNT" \
    --singleton-fraction "$SINGLETON_FRACTION" \
    --packed-clients-per-container "$PACKED_PER_CONTAINER" \
    --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM" \
    --bridge-count "$BRIDGE_COUNT" \
    --resource-experiment "$sweep_type" \
    --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
    --resource-failure-policy remove-and-continue \
    --cpu-affinity-mode "$mode" \
    --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE" \
    --embedded-docker-memory 4g \
    --cpu-sweep-fractions "$CPU_SWEEP_FRACTIONS" \
    --ram-app-heap-sweep-values "$OPENMLS_RAM_SWEEP_VALUES" \
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
    "${validation_args[@]}" \
    "${image_args[@]}"
  require_events_csv "$run_dir"
  validate_resource_output "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker
}

run_signal_sweep() {
  local iter="$1"
  local sweep_type="$2"
  local label="$3"
  local run_id="cal01_signal_${sweep_type}_i${iter}_${DATE_TAG}"
  local run_dir output_arg singleton_selection_seed py
  local -a image_args

  singleton_selection_seed="$(shuf -i 1-2147483647 -n 1)"
  py="$(python_bin_signal)"
  output_arg="$(output_dir_arg_for "$SIGNAL_DIR")"
  run_dir="$(run_dir_for "$SIGNAL_DIR" "$run_id")"
  image_args=()
  if [ "$BUILD_IMAGES" = "1" ]; then
    image_args=(--build-images)
  fi

  echo "===== [01/Signal-$label] iteration $iter/$N run_id=$run_id ====="
  cd "$SIGNAL_DIR"

  case "$sweep_type" in
    cpu-quota-sweep)
      "$py" scripts/run_compose_benchmark.py \
        --workers "$SIGNAL_WORKERS" \
        --run-id "$run_id" \
        --singleton-selection-seed "$singleton_selection_seed" \
        --output-dir "$output_arg" \
        --worker-layout-mode hybrid \
        --singleton-min-count "$SINGLETON_MIN_COUNT" \
        --singleton-fraction "$SINGLETON_FRACTION" \
        --packed-clients-per-container "$SIGNAL_PACKED_PER_CONTAINER" \
        --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM" \
        --bridge-count "$BRIDGE_COUNT" \
        --resource-experiment cpu-quota-sweep \
        --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
        --resource-failure-policy stop-on-profiled-failure \
        --strict-cpuset \
        --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE" \
        --embedded-docker-memory 4g \
        --cpu-sweep-fractions "$CPU_SWEEP_FRACTIONS" \
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
        --min-fanout-parallelism "$FANOUT_MIN" \
        --force-cleanup-signal-ports \
        --runner-in-docker \
        "${image_args[@]}"
      ;;
    ram-docker-cgroup-sweep)
      "$py" scripts/run_compose_benchmark.py \
        --workers "$SIGNAL_WORKERS" \
        --run-id "$run_id" \
        --singleton-selection-seed "$singleton_selection_seed" \
        --output-dir "$output_arg" \
        --worker-layout-mode hybrid \
        --singleton-min-count "$SINGLETON_MIN_COUNT" \
        --singleton-fraction "$SINGLETON_FRACTION" \
        --packed-clients-per-container "$SIGNAL_PACKED_PER_CONTAINER" \
        --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM" \
        --bridge-count "$BRIDGE_COUNT" \
        --resource-experiment ram-docker-cgroup-sweep \
        --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
        --resource-failure-policy stop-on-profiled-failure \
        --strict-cpuset \
        --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE" \
        --embedded-docker-memory 4g \
        --ram-sweep-values "$SIGNAL_RAM_SWEEP_VALUES" \
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
        --min-fanout-parallelism "$FANOUT_MIN" \
        --force-cleanup-signal-ports \
        --runner-in-docker \
        "${image_args[@]}"
      ;;
    *)
      echo "ERROR: unknown signal sweep type '$sweep_type'" >&2
      return 1
      ;;
  esac

  require_events_csv "$run_dir"
  validate_resource_output_signal "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker_signal
}

raise_nofile_limit

# ── OpenMLS sweeps ──────────────────────────────────────────────────────────
if [ "$PROTOCOL" = "openmls" ] || [ "$PROTOCOL" = "both" ]; then
  cleanup_docker
  for iter in $(seq 1 "$N"); do
    case "$SWEEP" in
      ram) run_sweep "$iter" "ram-app-heap-sweep" "RAM app-heap sweep" ;;
      cpu) run_sweep "$iter" "cpu-quota-sweep" "CPU quota sweep" ;;
      both)
        run_sweep "$iter" "ram-app-heap-sweep" "RAM app-heap sweep"
        run_sweep "$iter" "cpu-quota-sweep" "CPU quota sweep"
        ;;
      *) echo "ERROR: SWEEP must be ram, cpu, or both" >&2; exit 1 ;;
    esac
  done
  echo "All constrained container OpenMLS calibration sweeps completed."
fi

# ── Signal sweeps ───────────────────────────────────────────────────────────
if [ "$PROTOCOL" = "signal" ] || [ "$PROTOCOL" = "both" ]; then
  cleanup_docker_signal
  for iter in $(seq 1 "$N"); do
    case "$SWEEP" in
      ram) run_signal_sweep "$iter" "ram-docker-cgroup-sweep" "RAM cgroup sweep" ;;
      cpu) run_signal_sweep "$iter" "cpu-quota-sweep" "CPU quota sweep" ;;
      both)
        run_signal_sweep "$iter" "cpu-quota-sweep" "CPU quota sweep"
        run_signal_sweep "$iter" "ram-docker-cgroup-sweep" "RAM cgroup sweep"
        ;;
      *) echo "ERROR: SWEEP must be ram, cpu, or both" >&2; exit 1 ;;
    esac
  done
  echo "All constrained container Signal calibration sweeps completed."
fi

echo "All constrained container calibration sweeps completed."
