#!/usr/bin/env bash
#
# run_07_external_devices_signal_1x.sh
#
# One Signal benchmark run that includes ALL THREE external devices
# (Luckfox Pico Plus, Raspberry Pi 5, Raspberry Pi 3B+).
#
# Design:
#   * Signal pairwise workload: session establishment + application messages.
#     There is no MLS self-update/remove-rejoin phase, so update rounds are 0.
#   * Required workload: workers=1024, max_size=1024, step 64 then 256 after
#     size 256, plateau_order=ascending, payload=512, N=1.
#   * ONLY the three external devices are profiled. Docker workers are virtual
#     group-size increasers and run with profiling and cgroup monitoring off.
#   * External-device runs force --no-aggregate internally; the Signal
#     orchestrator pulls device JSONL files and runs standalone aggregation.
#   * After the run, hard-verify every canonical total for every real device at
#     every expected plateau: session establishment, application create, and
#     application receive. Eligible rows must have positive alloc_bytes and
#     cpu_process_ns. Only authoritative Luckfox attrition may truncate its
#     expected plateau list; the last completed operation and size are recorded.
#
# Every parameter is overridable via the environment.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SIGNAL_DIR="$REPO_ROOT/Signal_containerized"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

export PATH="$HOME/.cargo/bin:$PATH"

# ---- output location ------------------------------------------------------
# Same output root as the completed OpenMLS external-device campaign. The
# generated compose file bind-mounts "./<output_dir>" relative to
# Signal_containerized, so --output-dir is passed as a path relative to that
# stack directory.
OUTPUT_ROOT="${OUTPUT_ROOT:-/home/diel/openmls_external_benchmark_output}"

# ---- run control ----------------------------------------------------------
N="${N:-1}"
if [ "$N" != "1" ]; then
  echo "ERROR: this Signal external-device benchmark is intentionally exactly one run; got N=$N" >&2
  exit 2
fi
DEVICES_FILE="${DEVICES_FILE:-devices.yaml}"
EXTERNAL_DEVICES="${EXTERNAL_DEVICES:-luckfox-pico-plus-01 raspberry-pi-01 raspberry-pi-3bplus-01}"
# Worker/client ids of the external devices, from Signal_containerized/devices.yaml.
EXTERNAL_WORKER_IDS="${EXTERNAL_WORKER_IDS:-pico-plus-00001 raspi5-00001 raspi3bp-00001}"
SCENARIO="${SCENARIO:-external-device-signal-pairwise-baseline}"
# Keep the (single) run going if a profiled external device fails: evict it and
# continue instead of aborting the whole run.
RESOURCE_FAILURE_POLICY="${RESOURCE_FAILURE_POLICY:-remove-and-continue}"
# Build fresh images + external worker binaries on this run so the runner,
# Docker workers, and pushed device workers all match current source.
BUILD_IMAGES="${BUILD_IMAGES:-1}"
BUILD_EXTERNAL_BINARIES="${BUILD_EXTERNAL_BINARIES:-1}"
NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"

# ---- required Signal workload parameters ---------------------------------
WORKERS="${WORKERS:-1024}"
MAX_SIZE="${MAX_SIZE:-1024}"
MIN_SIZE="${MIN_SIZE:-4}"
STEP_SIZE="${STEP_SIZE:-64}"
PLATEAU_SIZES="${PLATEAU_SIZES:-4,64,256,512,1024}"
STEP_SIZE_SWITCH_AT="${STEP_SIZE_SWITCH_AT:-256}"
STEP_SIZE_AFTER_SWITCH="${STEP_SIZE_AFTER_SWITCH:-256}"
PLATEAU_ORDER="${PLATEAU_ORDER:-ascending}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
UPDATE_ROUNDS="${UPDATE_ROUNDS:-0}"
APP_ROUNDS="${APP_ROUNDS:-4}"
MAX_UPDATE_SAMPLES_PER_PLATEAU="${MAX_UPDATE_SAMPLES_PER_PLATEAU:-0}"
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-4}"
MIN_EXTERNAL_SAMPLES_PER_OPERATION="${MIN_EXTERNAL_SAMPLES_PER_OPERATION:-${MIN_PROFILED_SAMPLES_PER_OPERATION:-10}}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-512}"

# One unprofiled singleton is retained for the hybrid layout. Every Docker
# client is unprofiled; only real-device layout entries emit measurements.
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-0}"
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-1}"
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
  [ -x "$SIGNAL_DIR/.venv/bin/python" ] && printf '%s\n' "$SIGNAL_DIR/.venv/bin/python" || printf '%s\n' python3
}

relpath_from_stack() {
  python3 - "$1" "$SIGNAL_DIR" <<'PY'
import os, sys
print(os.path.relpath(sys.argv[1], sys.argv[2]))
PY
}

random_seed() {
  python3 - <<'PY'
import random
print(random.randint(1, 2147483647))
PY
}

mkdir -p "$OUTPUT_ROOT"
OUTPUT_DIR_ARG="$(relpath_from_stack "$OUTPUT_ROOT")"

cleanup_docker() {
  docker compose -f "$SIGNAL_DIR/docker-compose.yml" down --timeout 2 2>/dev/null || true
  for f in "$SIGNAL_DIR"/docker-compose_benchmark_*.yml "$SIGNAL_DIR"/docker-compose.*.generated.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  docker container ls -aq --filter "name=signal-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=signal-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
}

assert_no_openmls_campaign_active() {
  local proc_matches container_matches
  proc_matches="$(pgrep -af run_compose_benchmark 2>/dev/null | grep -E 'OpenMLS_containerized|openmls' || true)"
  container_matches="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '^mls-' || true)"
  if [ -n "$proc_matches" ] || [ -n "$container_matches" ]; then
    echo "ERROR: OpenMLS benchmark activity appears to be active; refusing to start Signal." >&2
    if [ -n "$proc_matches" ]; then
      echo "OpenMLS-like run_compose_benchmark processes:" >&2
      echo "$proc_matches" >&2
    fi
    if [ -n "$container_matches" ]; then
      echo "Running mls-* containers:" >&2
      echo "$container_matches" >&2
    fi
    echo "Wait for the OpenMLS campaign to finish before running this script." >&2
    exit 2
  fi
}

require_events_csv() {
  local run_dir="$1"
  test -s "$run_dir/events.csv" || { echo "ERROR: missing or empty events.csv in $run_dir" >&2; return 1; }
}

verify_signal_external_metrics() {
  local run_dir="$1"
  local worker_id
  local -a worker_args
  worker_args=()
  for worker_id in $EXTERNAL_WORKER_IDS; do
    worker_args+=(--external-worker-id "$worker_id")
  done

  "$(python_bin)" "$SIGNAL_DIR/scripts/validate_external_device_coverage.py" \
    "$run_dir/events.csv" \
    --layout "$run_dir/worker_layout.json" \
    --runner-events "$run_dir/runner-events.jsonl" \
    "${worker_args[@]}" \
    --allow-luckfox-attrition \
    --min-size "$MIN_SIZE" \
    --max-size "$MAX_SIZE" \
    --step-size "$STEP_SIZE" \
    --switch-at "$STEP_SIZE_SWITCH_AT" \
    --step-after-switch "$STEP_SIZE_AFTER_SWITCH" \
    --plateau-sizes "$PLATEAU_SIZES" \
    --payload-sizes "$PAYLOAD_SIZES" \
    --expected-profiled-docker 0 \
    --minimum-observations "$MIN_EXTERNAL_SAMPLES_PER_OPERATION" \
    --summary "$run_dir/external_device_coverage_summary.json"
}

external_device_args() {
  local device
  for device in $EXTERNAL_DEVICES; do
    printf '%s\n' "--external-device"
    printf '%s\n' "$device"
  done
}

run_external_signal() {
  local iter="$1"
  local run_id="cal07_ext_signal_pairwise_i${iter}_${DATE_TAG}"
  local run_dir="$OUTPUT_ROOT/$run_id"
  local py scenario_seed singleton_selection_seed
  local -a device_args build_args image_args

  py="$(python_bin)"
  scenario_seed="$(random_seed)"
  singleton_selection_seed="$(random_seed)"

  image_args=()
  build_args=()
  [ "$BUILD_IMAGES" = "1" ] && image_args=(--build-images)
  if [ "$BUILD_EXTERNAL_BINARIES" = "1" ]; then
    if ! command -v cargo >/dev/null 2>&1; then
      echo "ERROR: BUILD_EXTERNAL_BINARIES=1 requires cargo on PATH." >&2
      exit 2
    fi
    build_args=(--build-external-binaries)
  fi
  mapfile -t device_args < <(external_device_args)

  echo ""
  echo "===== [07/external-signal] Signal iteration $iter/$N run_id=$run_id ====="
  echo "  workers=$WORKERS max_size=$MAX_SIZE step=$STEP_SIZE switch_at=$STEP_SIZE_SWITCH_AT after_switch=$STEP_SIZE_AFTER_SWITCH plateau_order=$PLATEAU_ORDER"
  echo "  update_rounds=$UPDATE_ROUNDS app_rounds=$APP_ROUNDS payloads=$PAYLOAD_SIZES"
  echo "  external_devices=$EXTERNAL_DEVICES"
  echo "  profiling=external devices only (all Docker clients unprofiled)"
  echo "  output_root=$OUTPUT_ROOT (--output-dir=$OUTPUT_DIR_ARG)"
  echo ""

  cd "$SIGNAL_DIR"
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
      --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
      --profile-only-singletons \
      --disable-container-profiling \
      --cpu-affinity-mode none \
      --resource-failure-policy "$RESOURCE_FAILURE_POLICY" \
      --no-resource-monitor \
      --health-timeout-seconds "$HEALTH_TIMEOUT" \
      --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT" \
      --health-poll-seconds 0.5 \
      --worker-health-poll-ms 250 \
      --min-size "$MIN_SIZE" \
      --max-size "$MAX_SIZE" \
      --step-size "$STEP_SIZE" \
      --plateau-sizes "$PLATEAU_SIZES" \
      --step-size-switch-at "$STEP_SIZE_SWITCH_AT" \
      --step-size-after-switch "$STEP_SIZE_AFTER_SWITCH" \
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
      --force-cleanup-signal-ports \
      "${image_args[@]}" \
      "${build_args[@]}" \
      --runner-in-docker \
      --devices-file "$DEVICES_FILE" \
      --enable-external-devices \
      "${device_args[@]}" \
      --wipe-device-run-dirs )

  require_events_csv "$run_dir"
  echo "----- Signal external-device verification (alloc_bytes + cpu_process_ns + coverage) -----"
  if ! verify_signal_external_metrics "$run_dir"; then
    cd "$REPO_ROOT"
    cleanup_docker
    return 1
  fi
  cd "$REPO_ROOT"
  cleanup_docker
}

STATUS=0
assert_no_openmls_campaign_active
cleanup_docker
echo "===== Signal external-device pairwise campaign: $N run(s) ====="
echo "  output_root=$OUTPUT_ROOT"
echo "  free_space=$(df -h "$OUTPUT_ROOT" | awk 'NR==2 {print $4" avail on "$6" ("$1")"}')"
for iter in $(seq 1 "$N"); do
  if ! run_external_signal "$iter"; then
    STATUS=1
    echo "===== Signal external run $iter/$N FAILED =====" >&2
    cleanup_docker || true
  fi
done

if [ "$STATUS" -eq 0 ]; then
  echo ""
  echo "Signal external-device run completed; alloc_bytes + cpu_process_ns verified for all three devices."
else
  echo ""
  echo "One or more Signal external-device runs failed verification." >&2
fi
exit "$STATUS"
