#!/usr/bin/env bash
#
# run_06_external_devices_remove_rejoin_openmls_5x.sh
#
# External-device-only OpenMLS RemoveCommit / JoinFromWelcome
# ("remove-rejoin") benchmark for all three devices. The historical filename
# is retained for compatibility; production uses one run with a coverage floor.
#
# This is run_05 with the remove-rejoin mode enabled:
#   * --remove-rejoin: at every plateau a profiled member is removed
#     (RemoveCommit, measured on the removing actor) and immediately re-added
#     (Welcome -> JoinFromWelcome, measured on the returning victim). The
#     update and application phases are skipped by the runner in this mode.
#   * ONLY external workers are profiled (--disable-container-profiling +
#     --external-coverage-lane), so the profiled set is exactly the three
#     external devices. Therefore remove-rejoin's victim AND actor are always
#     external devices, i.e. both RemoveCommit and JoinFromWelcome are measured
#     on real hardware. Docker workers remain pure virtual group-size increasers.
#   * No Docker resource experiment, affinity, or container profiling is run.
#   * A failed external device is evicted and recorded so the remaining devices
#     continue; this is attrition handling, not a resource-failure experiment.
#
# All other workload parameters match the run_05 campaign (workers=1024,
# max_size=1024, step_size=32, roundtrips=1, singleton_min_count=10,
# singleton_fraction=1e-9, stratified-random, packed=64/16, bridge_count=2, same
# health/pool/fanout timeouts) EXCEPT plateau order: this run is ASCENDING only
# (no staircase descent, which is wasted work for RemoveCommit/JoinFromWelcome).
# Outputs go to the SAME OUTPUT_ROOT on /dev/sda2 (distinct cal06_ run-id prefix).
#
# Every parameter is overridable via the environment.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

export PATH="$HOME/.cargo/bin:$PATH"

# ---- output location ------------------------------------------------------
# Same target disk as run_05: /dev/sda2 (the root filesystem, which has the
# free space) rather than the small /dev/shm tmpfs. The generated
# docker-compose bind-mounts "./<output_dir>" relative to the stack dir, so
# --output-dir is passed as a path RELATIVE to OpenMLS_containerized.
OUTPUT_ROOT="${OUTPUT_ROOT:-/home/diel/openmls_external_benchmark_output}"

# ---- run control ----------------------------------------------------------
N="${N:-1}"
DEVICES_FILE="${DEVICES_FILE:-devices.yaml}"
EXTERNAL_DEVICES="${EXTERNAL_DEVICES:-luckfox-pico-plus-01 raspberry-pi-01 raspberry-pi-3bplus-01}"
# Worker (client) ids of the external devices, from devices.yaml worker.id.
EXTERNAL_WORKER_IDS="${EXTERNAL_WORKER_IDS:-pico-plus-00001 raspi5-00001 raspi3bp-00001}"
ATTRITION_ALLOWED_WORKER_IDS="${ATTRITION_ALLOWED_WORKER_IDS:-pico-plus-00001}"
SCENARIO="${SCENARIO:-external-device-openmls-remove-rejoin}"
# Keep the benchmark running when a profiled (external) device fails: evict it
# and continue rather than aborting the run.
RESOURCE_FAILURE_POLICY="${RESOURCE_FAILURE_POLICY:-remove-and-continue}"
# Build fresh images + external worker binaries once (first iteration) so the
# runner and the pushed device workers match the current source tree.
BUILD_IMAGES="${BUILD_IMAGES:-1}"
BUILD_EXTERNAL_BINARIES="${BUILD_EXTERNAL_BINARIES:-1}"
NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"

# ---- workload parameters (identical to the active run_05 campaign) --------
WORKERS="${WORKERS:-1024}"
MAX_SIZE="${MAX_SIZE:-1024}"
MIN_SIZE="${MIN_SIZE:-4}"
STEP_SIZE="${STEP_SIZE:-32}"
PLATEAU_SIZES="${PLATEAU_SIZES:-4,64,256,512,1024}"
# ASCENDING only: for RemoveCommit / JoinFromWelcome the staircase descent phase
# is wasted work. Size 4 is the smallest configured plateau that includes the
# leader and all three external devices, so every plateau gets all device roles.
PLATEAU_ORDER="${PLATEAU_ORDER:-ascending}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
# remove-rejoin mode: the runner skips update/app phases, so these are zeroed.
UPDATE_ROUNDS="${UPDATE_ROUNDS:-0}"
APP_ROUNDS="${APP_ROUNDS:-0}"
MAX_UPDATE_SAMPLES_PER_PLATEAU="${MAX_UPDATE_SAMPLES_PER_PLATEAU:-0}"
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-0}"
MIN_EXTERNAL_SAMPLES_PER_OPERATION="${MIN_EXTERNAL_SAMPLES_PER_OPERATION:-${MIN_PROFILED_SAMPLES_PER_OPERATION:-10}}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-32}"
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-10}"
SINGLETON_FRACTION="${SINGLETON_FRACTION:-0.000000001}"
SINGLETON_SELECTION_STRATEGY="${SINGLETON_SELECTION_STRATEGY:-stratified-random}"
PACKED_PER_CONTAINER="${PACKED_PER_CONTAINER:-64}"
PACKED_INTERNAL_PARALLELISM="${PACKED_INTERNAL_PARALLELISM:-16}"
BRIDGE_COUNT="${BRIDGE_COUNT:-2}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"

python_bin() {
  [ -x "$OPENMLS_DIR/.venv/bin/python" ] && printf '%s\n' "$OPENMLS_DIR/.venv/bin/python" || printf '%s\n' python3
}

relpath_from_stack() {
  python3 - "$1" "$OPENMLS_DIR" <<'PY'
import os, sys
print(os.path.relpath(sys.argv[1], sys.argv[2]))
PY
}

mkdir -p "$OUTPUT_ROOT"
# Path to OUTPUT_ROOT expressed relative to the stack dir, for --output-dir and
# the compose "./<output_dir>:/results" bind mount.
OUTPUT_DIR_ARG="$(relpath_from_stack "$OUTPUT_ROOT")"

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
  test -s "$run_dir/events.csv" || { echo "ERROR: missing or empty events.csv in $run_dir" >&2; return 1; }
}

verify_external_metrics() {
  local run_dir="$1"
  local worker_id
  local -a summary_args
  summary_args=()
  for worker_id in $EXTERNAL_WORKER_IDS; do
    summary_args+=(--expected-worker "$worker_id")
  done
  for worker_id in $ATTRITION_ALLOWED_WORKER_IDS; do
    summary_args+=(--attrition-allowed-worker "$worker_id")
  done
  "$(python_bin)" "$OPENMLS_DIR/scripts/summarize_external_device_run.py" \
    "$run_dir" "${summary_args[@]}" \
    --minimum-observations "$MIN_EXTERNAL_SAMPLES_PER_OPERATION"
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
  local run_id="cal06_ext_remove_rejoin_openmls_i${iter}_${DATE_TAG}"
  local run_dir="$OUTPUT_ROOT/$run_id"
  local py scenario_seed singleton_selection_seed
  local -a device_args build_args image_args

  py="$(python_bin)"
  scenario_seed="$(shuf -i 1-2147483647 -n 1)"
  singleton_selection_seed="$(shuf -i 1-2147483647 -n 1)"

  # Build images + external binaries only on the first iteration.
  image_args=()
  build_args=()
  if [ "$iter" -eq 1 ]; then
    [ "$BUILD_IMAGES" = "1" ] && image_args=(--build-images)
    if [ "$BUILD_EXTERNAL_BINARIES" = "1" ] && command -v cargo >/dev/null 2>&1; then
      build_args=(--build-external-binaries)
    fi
  fi
  mapfile -t device_args < <(external_device_args)

  echo ""
  echo "===== [06/external-remove-rejoin] OpenMLS iteration $iter/$N run_id=$run_id ====="
  echo "  workers=$WORKERS max_size=$MAX_SIZE step_size=$STEP_SIZE mode=remove-rejoin"
  echo "  external_devices=$EXTERNAL_DEVICES"
  echo "  profiling=external-only (Docker profiles and resource experiments off)"
  echo "  failure_policy=$RESOURCE_FAILURE_POLICY (device failure -> evict and continue)"
  echo "  output_root=$OUTPUT_ROOT (--output-dir=$OUTPUT_DIR_ARG)"
  echo ""

  cd "$OPENMLS_DIR"
  ( ulimit -n "$NOFILE_LIMIT" 2>/dev/null || true
    "$py" scripts/run_compose_benchmark.py \
      --workers "$WORKERS" \
      --run-id "$run_id" \
      --scenario "$SCENARIO" \
      --scenario-seed "$scenario_seed" \
      --singleton-selection-seed "$singleton_selection_seed" \
      --singleton-selection-strategy "$SINGLETON_SELECTION_STRATEGY" \
      --output-dir "$OUTPUT_DIR_ARG" \
      --worker-layout-mode hybrid \
      --singleton-min-count "$SINGLETON_MIN_COUNT" \
      --singleton-fraction "$SINGLETON_FRACTION" \
      --packed-clients-per-container "$PACKED_PER_CONTAINER" \
      --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM" \
      --bridge-count "$BRIDGE_COUNT" \
      --disable-container-profiling \
      --cpu-affinity-mode none \
      --resource-failure-policy "$RESOURCE_FAILURE_POLICY" \
      --health-timeout-seconds "$HEALTH_TIMEOUT" \
      --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT" \
      --health-poll-seconds 0.5 \
      --worker-health-poll-ms 250 \
      --min-size "$MIN_SIZE" \
      --max-size "$MAX_SIZE" \
      --step-size "$STEP_SIZE" \
      --plateau-sizes "$PLATEAU_SIZES" \
      --plateau-order "$PLATEAU_ORDER" \
      --roundtrips "$ROUNDTRIPS" \
      --update-rounds "$UPDATE_ROUNDS" \
      --app-rounds "$APP_ROUNDS" \
      --max-update-samples-per-plateau "$MAX_UPDATE_SAMPLES_PER_PLATEAU" \
      --max-app-samples-per-payload "$MAX_APP_SAMPLES_PER_PAYLOAD" \
      --min-external-samples-per-operation "$MIN_EXTERNAL_SAMPLES_PER_OPERATION" \
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
      --remove-rejoin \
      --wipe-device-run-dirs )

  require_events_csv "$run_dir"
  echo "----- external-device metric verification (alloc_bytes + cpu_process_ns + remove-rejoin ops) -----"
  if ! verify_external_metrics "$run_dir"; then
    cd "$REPO_ROOT"
    cleanup_docker
    return 1
  fi
  cd "$REPO_ROOT"
  cleanup_docker
}

STATUS=0
cleanup_docker
echo "===== OpenMLS external-device remove-rejoin campaign: $N run(s) ====="
echo "  output_root=$OUTPUT_ROOT"
echo "  free_space=$(df -h "$OUTPUT_ROOT" | awk 'NR==2 {print $4" avail on "$6" ("$1")"}')"
for iter in $(seq 1 "$N"); do
  if ! run_external "$iter"; then
    STATUS=1
    echo "===== OpenMLS remove-rejoin run $iter/$N FAILED =====" >&2
    cleanup_docker || true
  fi
done

if [ "$STATUS" -eq 0 ]; then
  echo ""
  echo "All $N OpenMLS remove-rejoin runs completed; external-device RemoveCommit/JoinFromWelcome metrics verified."
else
  echo ""
  echo "One or more OpenMLS remove-rejoin runs failed; see output above." >&2
fi
exit "$STATUS"
