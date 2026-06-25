#!/usr/bin/env bash
#
# Calibration run 01: constrained container RAM/CPU sweeps.
# Produces events.csv for every run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

N="${N:-3}"
SWEEP="${SWEEP:-both}"
STRICT_CPUSET="${STRICT_CPUSET:-1}"
RESOURCE_OUTPUT_VALIDATION="${RESOURCE_OUTPUT_VALIDATION:-1}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"

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
PAYLOAD_SIZES="${PAYLOAD_SIZES:-32,256,2048}"

CPU_AFFINITY_SAMPLE="${CPU_AFFINITY_SAMPLE:-20}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"
CPU_SWEEP_FRACTIONS="${CPU_SWEEP_FRACTIONS:-1.00,0.75,0.50,0.25,0.10,0.05,0.04,0.03,0.02,0.01}"

export PATH="$HOME/.cargo/bin:$PATH"

python_bin() {
  [ -x "$OPENMLS_DIR/.venv/bin/python" ] && printf '%s\n' "$OPENMLS_DIR/.venv/bin/python" || printf '%s\n' python3
}

cleanup_docker() {
  docker compose -f "$OPENMLS_DIR/docker-compose.yml" down --timeout 2 2>/dev/null || true
  for f in "$OPENMLS_DIR"/docker-compose_benchmark_*.yml "$OPENMLS_DIR"/docker-compose.*.generated.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=mls-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
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
  local run_dir="$OPENMLS_DIR/benchmark_output/$run_id"
  local scenario_seed singleton_selection_seed mode py
  local -a image_args validation_args

  scenario_seed="$(shuf -i 1-2147483647 -n 1)"
  singleton_selection_seed="$(shuf -i 1-2147483647 -n 1)"
  mode="$(affinity_mode)"
  py="$(python_bin)"
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
    --output-dir benchmark_output \
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
    --no-aggregate \
    "${validation_args[@]}" \
    "${image_args[@]}"
  require_events_csv "$run_dir"
  validate_resource_output "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker
}

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
echo "All constrained container calibration sweeps completed."
