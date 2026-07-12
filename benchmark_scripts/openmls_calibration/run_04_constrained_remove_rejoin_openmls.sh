#!/usr/bin/env bash
#
# Calibration run 04: constrained Remove-Rejoin (RemoveCommit + ProcessWelcome)
# under RFC-7228bis-aligned RAM sweep and standard CPU quota sweep profiles.
# OpenMLS only.  Reuses the same resource limits as Phase 1
# (run_01_constrained_container_sweeps.sh).
#
# Phase A: RAM sweep with --remove-rejoin (10 heap budgets × 10 runs)
# Phase B: CPU sweep with --remove-rejoin (10 fractions × 10 runs)
#
# At every plateau, a random profiled singleton is removed and immediately
# re-added.  All other clients commit-to-update the group.  No update or
# application phases.  The per-operation heap budget failures (especially
# process_welcome and create_commit at join time) are recorded in
# worker_failures.csv and events.csv.
#
# PROTOCOL env not used (OpenMLS only).
# env N – number of iterations per sweep (default 10)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"

OUTPUT_ROOT="${OUTPUT_ROOT:-/tmp}"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

N="${N:-10}"
OPENMLS_RAM_SWEEP_VALUES="${OPENMLS_RAM_SWEEP_VALUES:-10k,50k,100k,500k,1m,5m,10m,50m,100m,500m}"
CPU_SWEEP_FRACTIONS="${CPU_SWEEP_FRACTIONS:-1.00,0.75,0.50,0.25,0.10,0.05,0.04,0.03,0.02,0.01}"

WORKERS="${WORKERS:-1024}"
MAX_GROUP_SIZE="${MAX_GROUP_SIZE:-1024}"
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-10}"
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-10}"
SINGLETON_FRACTION="${SINGLETON_FRACTION:-0.000000001}"
PACKED_PER_CONTAINER="${PACKED_PER_CONTAINER:-64}"
PACKED_INTERNAL_PARALLELISM="${PACKED_INTERNAL_PARALLELISM:-16}"
BRIDGE_COUNT="${BRIDGE_COUNT:-2}"
STEP_SIZE="${STEP_SIZE:-8}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
CPU_AFFINITY_SAMPLE="${CPU_AFFINITY_SAMPLE:-20}"
CPU_THROTTLED_PERIOD_THRESHOLD="${CPU_THROTTLED_PERIOD_THRESHOLD:-0.05}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"
NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"
RESOURCE_OUTPUT_VALIDATION="${RESOURCE_OUTPUT_VALIDATION:-1}"

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

cleanup_docker() {
  docker compose -f "$OPENMLS_DIR/docker-compose.yml" down --timeout 2 2>/dev/null || true
  for f in "$OPENMLS_DIR"/docker-compose_benchmark_*.yml "$OPENMLS_DIR"/docker-compose.*.generated.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=mls-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
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
  echo "ERROR: need >= $PROFILED_SINGLETON_COUNT online CPUs, have $cpus" >&2
  return 1
}

# ── Phase A: RAM sweep with remove-rejoin ────────────────────────────
run_ram_remove_rejoin() {
  local iter="$1"
  local run_id="cal04_remove_rejoin_ram_i${iter}_${DATE_TAG}"
  local run_dir output_arg scenario_seed singleton_selection_seed mode py
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

  echo "===== [04/remove-rejoin RAM] iteration $iter/$N run_id=$run_id ====="
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
    --resource-experiment ram-app-heap-sweep \
    --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
    --resource-failure-policy remove-and-continue \
    --cpu-affinity-mode "$mode" \
    --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE" \
    --embedded-docker-memory 4g \
    --ram-app-heap-sweep-values "$OPENMLS_RAM_SWEEP_VALUES" \
    --health-timeout-seconds "$HEALTH_TIMEOUT" \
    --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT" \
    --health-poll-seconds 0.5 \
    --worker-health-poll-ms 250 \
    --min-size 2 \
    --max-size "$MAX_GROUP_SIZE" \
    --step-size "$STEP_SIZE" \
    --plateau-order staircase \
    --roundtrips "$ROUNDTRIPS" \
    --update-rounds 0 \
    --app-rounds 0 \
    --max-update-samples-per-plateau 0 \
    --max-app-samples-per-payload 0 \
    --payload-sizes 32 \
    --http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-outbound-http-permits "$WORKER_OUTBOUND_PERMITS" \
    --max-fanout-parallelism "$FANOUT_PARALLELISM" \
    --min-fanout-parallelism "$FANOUT_MIN" \
    --force-cleanup-mls-ports \
    --runner-in-docker \
    --keep-stack-up \
    --keep-stack-up-on-failure \
    --remove-rejoin \
    --no-aggregate \
    "${validation_args[@]}" \
    "${image_args[@]}"
  require_events_csv "$run_dir"
  validate_resource_output "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker
}

# ── Phase B: CPU sweep with remove-rejoin ────────────────────────────
run_cpu_remove_rejoin() {
  local iter="$1"
  local run_id="cal04_remove_rejoin_cpu_i${iter}_${DATE_TAG}"
  local run_dir output_arg scenario_seed singleton_selection_seed mode py
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

  echo "===== [04/remove-rejoin CPU] iteration $iter/$N run_id=$run_id ====="
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
    --resource-experiment cpu-quota-sweep \
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
    --plateau-order staircase \
    --roundtrips "$ROUNDTRIPS" \
    --update-rounds 0 \
    --app-rounds 0 \
    --max-update-samples-per-plateau 0 \
    --max-app-samples-per-payload 0 \
    --payload-sizes 32 \
    --http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL" \
    --worker-outbound-http-permits "$WORKER_OUTBOUND_PERMITS" \
    --max-fanout-parallelism "$FANOUT_PARALLELISM" \
    --min-fanout-parallelism "$FANOUT_MIN" \
    --force-cleanup-mls-ports \
    --runner-in-docker \
    --keep-stack-up \
    --keep-stack-up-on-failure \
    --remove-rejoin \
    --no-aggregate \
    "${validation_args[@]}" \
    "${image_args[@]}"
  require_events_csv "$run_dir"
  validate_resource_output "$run_dir"
  cd "$REPO_ROOT"
  cleanup_docker
}

# ── Main ─────────────────────────────────────────────────────────────
raise_nofile_limit

echo "===== Calibration run 04: constrained remove-rejoin ====="
echo "protocol=openmls"
echo "N=$N"
echo "output_root=$OUTPUT_ROOT"
echo "ram_values=$OPENMLS_RAM_SWEEP_VALUES"
echo "cpu_fractions=$CPU_SWEEP_FRACTIONS"
echo "workers=$WORKERS max_size=$MAX_GROUP_SIZE"
echo ""

# Phase A: RAM sweep with remove-rejoin
cleanup_docker
for iter in $(seq 1 "$N"); do
  run_ram_remove_rejoin "$iter"
done
echo "All constrained remove-rejoin RAM sweeps completed."

# Phase B: CPU sweep with remove-rejoin
cleanup_docker
for iter in $(seq 1 "$N"); do
  run_cpu_remove_rejoin "$iter"
done
echo "All constrained remove-rejoin CPU sweeps completed."

echo "All constrained remove-rejoin calibration sweeps completed."
