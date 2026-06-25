#!/usr/bin/env bash
#
# Calibration run 03: external-device calibration.
# Docker containers are unprofiled; external devices are profiled.
# Produces events.csv for every run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

N="${N:-3}"
DEVICES_FILE="${DEVICES_FILE:-devices.yaml}"
EXTERNAL_DEVICES="${EXTERNAL_DEVICES:-luckfox-pico-plus-01 raspberry-pi-01}"
BUILD_EXTERNAL_BINARIES="${BUILD_EXTERNAL_BINARIES:-1}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"

WORKERS="${WORKERS:-256}"
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-1}"
SINGLETON_FRACTION="${SINGLETON_FRACTION:-0.000000001}"
PACKED_PER_CONTAINER="${PACKED_PER_CONTAINER:-64}"
PACKED_INTERNAL_PARALLELISM="${PACKED_INTERNAL_PARALLELISM:-16}"
BRIDGE_COUNT="${BRIDGE_COUNT:-2}"

MAX_GROUP_SIZE="${MAX_GROUP_SIZE:-256}"
STEP_SIZE="${STEP_SIZE:-16}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
UPDATE_ROUNDS="${UPDATE_ROUNDS:-4}"
APP_ROUNDS="${APP_ROUNDS:-4}"
PLATEAU_ORDER="${PLATEAU_ORDER:-ascending}"
MAX_UPDATE_SAMPLES_PER_PLATEAU="${MAX_UPDATE_SAMPLES_PER_PLATEAU:-4}"
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-4}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-32,256,2048}"

HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"

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

external_device_args() {
  local device
  for device in $EXTERNAL_DEVICES; do
    printf '%s\n' "--external-device"
    printf '%s\n' "$device"
  done
}

run_external() {
  local iter="$1"
  local run_id="cal03_external_device_calibration_i${iter}_${DATE_TAG}"
  local run_dir="$OPENMLS_DIR/benchmark_output/$run_id"
  local scenario_seed singleton_selection_seed py
  local -a build_args device_args image_args

  scenario_seed="$(shuf -i 1-2147483647 -n 1)"
  singleton_selection_seed="$(shuf -i 1-2147483647 -n 1)"
  py="$(python_bin)"
  build_args=()
  if [ "$BUILD_EXTERNAL_BINARIES" = "1" ] && command -v cargo >/dev/null 2>&1; then
    build_args=(--build-external-binaries)
  fi
  image_args=()
  if [ "$BUILD_IMAGES" = "1" ]; then
    image_args=(--build-images)
  fi
  mapfile -t device_args < <(external_device_args)

  echo "===== [03/external] iteration $iter/$N run_id=$run_id ====="
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
    --disable-container-profiling \
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
    "${image_args[@]}" \
    "${build_args[@]}" \
    --runner-in-docker \
    --devices-file "$DEVICES_FILE" \
    --enable-external-devices \
    "${device_args[@]}" \
    --external-coverage-lane \
    --wipe-device-run-dirs
  require_events_csv "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker
}

cleanup_docker
for iter in $(seq 1 "$N"); do
  run_external "$iter"
done
echo "All external-device calibration runs completed."
