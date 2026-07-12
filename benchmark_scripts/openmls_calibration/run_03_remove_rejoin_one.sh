#!/usr/bin/env bash
# One OpenMLS run with --remove-rejoin: a profiled singleton is removed
# and immediately re-added at every plateau to produce clean RemoveCommit
# and ProcessWelcome scaling data.  No update or application phases.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"

OUTPUT_ROOT="${OUTPUT_ROOT:-/tmp/MLS_container_no_constraints}"

WORKERS="${WORKERS:-1024}"
MAX_SIZE="${MAX_SIZE:-1024}"
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-10}"
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-10}"
SINGLETON_FRACTION="${SINGLETON_FRACTION:-0.000000001}"
PACKED_PER_CONTAINER="${PACKED_PER_CONTAINER:-64}"
PACKED_INTERNAL_PARALLELISM="${PACKED_INTERNAL_PARALLELISM:-16}"
BRIDGE_COUNT="${BRIDGE_COUNT:-2}"
STEP_SIZE="${STEP_SIZE:-8}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
CPU_AFFINITY_SAMPLE="${CPU_AFFINITY_SAMPLE:-20}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"
NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"

DATE_TAG="$(date +%Y%m%d_%H%M%S)"
RUN_TOKEN="remove_rejoin_${DATE_TAG}_pid$$"
RUN_ID="${RUN_TOKEN}_openmls"
RUN_DIR="$OUTPUT_ROOT/$RUN_ID"
REPORT="$OUTPUT_ROOT/${RUN_TOKEN}_report.txt"

log() { printf '%s\n' "$*" | tee -a "$REPORT"; }

random_seed() { python3 -c 'import random; print(random.randint(1,2147483647))'; }

python_bin_for() {
  local dir="$1"
  if [ -x "$dir/.venv/bin/python" ]; then printf '%s\n' "$dir/.venv/bin/python"
  else printf '%s\n' python3; fi
}

relpath_from() { python3 -c "import os,sys; print(os.path.relpath(sys.argv[1],sys.argv[2]))" "$1" "$2"; }

cleanup() {
  if [ -f "$RUN_DIR/docker-compose.generated.yml" ]; then
    docker compose -f "$RUN_DIR/docker-compose.generated.yml" down --timeout 2 >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

: > "$REPORT"

log "remove-rejoin one-run benchmark"
log "repo=$REPO_ROOT"
log "git_commit=$(git -C "$REPO_ROOT" rev-parse HEAD)"
log "output_root=$OUTPUT_ROOT run_dir=$RUN_DIR"

export OPENMLS_MEMORY_MODEL="${OPENMLS_MEMORY_MODEL:-app-heap-budget}"
export OPENMLS_APP_HEAP_BUDGET="${OPENMLS_APP_HEAP_BUDGET:-1024g}"
export OPENMLS_APP_HEAP_BUDGET_BYTES="${OPENMLS_APP_HEAP_BUDGET_BYTES:-1099511627776}"

py="$(python_bin_for "$OPENMLS_DIR")"
output_arg="$(relpath_from "$OUTPUT_ROOT" "$OPENMLS_DIR")"
scenario_seed="$(random_seed)"
singleton_seed="$(random_seed)"

image_args=()
if [ "$BUILD_IMAGES" = "1" ]; then image_args=(--build-images); fi

cmd=(
  "$py" scripts/run_compose_benchmark.py
  --workers "$WORKERS"
  --run-id "$RUN_ID"
  --scenario tmp-two-run-unconstrained-container-baseline
  --scenario-seed "$scenario_seed"
  --singleton-selection-seed "$singleton_seed"
  --output-dir "$output_arg"
  --worker-layout-mode hybrid
  --singleton-min-count "$SINGLETON_MIN_COUNT"
  --singleton-fraction "$SINGLETON_FRACTION"
  --packed-clients-per-container "$PACKED_PER_CONTAINER"
  --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM"
  --bridge-count "$BRIDGE_COUNT"
  --profiled-singleton-count "$PROFILED_SINGLETON_COUNT"
  --cpu-affinity-mode profiled-nor-background
  --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE"
  --health-timeout-seconds "$HEALTH_TIMEOUT"
  --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT"
  --health-poll-seconds 0.5
  --worker-health-poll-ms 250
  --min-size 2
  --max-size "$MAX_SIZE"
  --step-size "$STEP_SIZE"
  --plateau-order staircase
  --roundtrips "$ROUNDTRIPS"
  --update-rounds 0
  --app-rounds 0
  --max-update-samples-per-plateau 0
  --max-app-samples-per-payload 0
  --payload-sizes "32"
  --http-pool-max-idle-per-host "$WORKER_HTTP_POOL"
  --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL"
  --worker-outbound-http-permits "$WORKER_OUTBOUND_PERMITS"
  --max-fanout-parallelism "$FANOUT_PARALLELISM"
  --min-fanout-parallelism "$FANOUT_MIN"
  --force-cleanup-mls-ports
  --runner-in-docker
  --keep-stack-up
  --keep-stack-up-on-failure
  --remove-rejoin
  "${image_args[@]}"
)

log "command=$(printf '%q ' "${cmd[@]}")"
set +e
(cd "$OPENMLS_DIR" || exit; ulimit -n "$NOFILE_LIMIT" 2>/dev/null || true; "${cmd[@]}")
rc=$?
set -e
log "exit_code=$rc"

if [ "$rc" -eq 0 ]; then log "status=pass"; else log "status=fail"; exit "$rc"; fi
