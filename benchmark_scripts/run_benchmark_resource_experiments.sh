#!/usr/bin/env bash
#
# Resource experiment benchmark runner — Scientific threshold mode
# ---------------------------------------------------------------------------
# Tests exactly ONE resource profile per benchmark invocation.
# One profiled singleton container; all other clients are densely packed.
# Loops over every configured RAM sweep and CPU matrix profile.
#
# Usage:
#   chmod +x run_benchmark_resource_experiments.sh
#   bash run_benchmark_resource_experiments.sh
#
# Run from the repository root (parent of *_containerized/).
# Requires Docker and Python 3.

# NOTE: set -e is DISABLED here intentionally. Individual run failures
# (e.g., strict output validation, profiled singleton OOM, preflight failure)
# are collected and reported in the summary, but they must not abort the
# entire sweep. More detail in each run's worker_failures.csv / run_status.csv.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"
SUMMARY_FILE="${SCRIPT_DIR}/resource_experiment_summary_${DATE_TAG}.txt"

export PATH="$HOME/.cargo/bin:$PATH"

python_for() {
  local stack_dir="$1"
  if [ -x "$stack_dir/.venv/bin/python" ]; then
    printf '%s\n' "$stack_dir/.venv/bin/python"
  else
    printf '%s\n' "python3"
  fi
}

cleanup_generated_compose() {
  local dir="$1"
  for f in \
    "$dir"/docker-compose_benchmark_*.yml \
    "$dir"/docker-compose.*.generated.yml
  do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
}

cleanup_docker() {
  if [ -f "$OPENMLS_DIR/docker-compose.yml" ]; then
    docker compose -f "$OPENMLS_DIR/docker-compose.yml" down --timeout 2 2>/dev/null || true
  fi
  cleanup_generated_compose "$OPENMLS_DIR"
  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=mls-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
}

PYTHON_BIN="$(python_for "$SCRIPT_DIR/../OpenMLS_containerized")"
OPENMLS_DIR="$(cd "$SCRIPT_DIR/../OpenMLS_containerized" && pwd)"

# Scientific threshold mode: one profiled singleton per run.
# The runner requires a positive singleton fraction. The default is 1/4096;
# override SINGLETON_FRACTION when changing LOGICAL_WORKERS if needed.
HOST_CPU_COUNT="$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '1')"
DEFAULT_RAM_CPU_COUNT=$((HOST_CPU_COUNT / 2))
[ "$DEFAULT_RAM_CPU_COUNT" -lt 1 ] && DEFAULT_RAM_CPU_COUNT=1
[ "$DEFAULT_RAM_CPU_COUNT" -gt 10 ] && DEFAULT_RAM_CPU_COUNT=10

LOGICAL_WORKERS="${LOGICAL_WORKERS:-4096}"
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-1}"
SINGLETON_FRACTION="${SINGLETON_FRACTION:-0.000244140625}"
PACKED_CLIENTS_PER_CONTAINER="${PACKED_CLIENTS_PER_CONTAINER:-192}"
PACKED_WORKER_INTERNAL_PARALLELISM="${PACKED_WORKER_INTERNAL_PARALLELISM:-32}"
AFFINITY_SAMPLE_SECONDS="${AFFINITY_SAMPLE_SECONDS:-20}"
RAM_SWEEP_CPU_COUNT="${RAM_SWEEP_CPU_COUNT:-$DEFAULT_RAM_CPU_COUNT}"
RAM_SWEEP_VALUES="${RAM_SWEEP_VALUES:-32m,64m,128m,256m,512m,1g}"
CPU_MATRIX_CORE_COUNTS="${CPU_MATRIX_CORE_COUNTS:-1,2,4}"
CPU_MATRIX_CAPACITY_FRACTIONS="${CPU_MATRIX_CAPACITY_FRACTIONS:-0.25,0.50,0.75,1.00}"
MIN_SIZE="${MIN_SIZE:-2}"
MAX_SIZE="${MAX_SIZE:-$LOGICAL_WORKERS}"
STEP_SIZE="${STEP_SIZE:-[1,32]}"
PLATEAU_ORDER="${PLATEAU_ORDER:-randomized}"
ROUNDTRIPS="${ROUNDTRIPS:-2}"
UPDATE_ROUNDS="${UPDATE_ROUNDS:-8}"
APP_ROUNDS="${APP_ROUNDS:-8}"
MAX_UPDATE_SAMPLES="${MAX_UPDATE_SAMPLES:-8}"
MAX_APP_SAMPLES="${MAX_APP_SAMPLES:-8}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-[16,4096]}"
POST_STARTUP_SETTLE_SECONDS="${POST_STARTUP_SETTLE_SECONDS:-10}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-240}"
WORKER_HEALTH_TIMEOUT_SECONDS="${WORKER_HEALTH_TIMEOUT_SECONDS:-600}"
RUNNER_HTTP_REQUEST_TIMEOUT_MS="${RUNNER_HTTP_REQUEST_TIMEOUT_MS:-120000}"
WORKER_HTTP_REQUEST_TIMEOUT_MS="${WORKER_HTTP_REQUEST_TIMEOUT_MS:-45000}"
BUILD_IMAGES_NEXT_RUN="${BUILD_IMAGES_NEXT_RUN:-1}"
RUN_RAM_SWEEP="${RUN_RAM_SWEEP:-1}"
RUN_CPU_MATRIX="${RUN_CPU_MATRIX:-1}"

if [[ "$PAYLOAD_SIZES" =~ ^\[([0-9]+)\]$ ]]; then
  PAYLOAD_SIZES="${BASH_REMATCH[1]}"
fi

IFS=',' read -r -a RAM_VALUES <<< "$RAM_SWEEP_VALUES"
IFS=',' read -r -a CPU_CORE_VALUES <<< "$CPU_MATRIX_CORE_COUNTS"
IFS=',' read -r -a CPU_FRACTION_VALUES <<< "$CPU_MATRIX_CAPACITY_FRACTIONS"
RAM_RUN_COUNT="${#RAM_VALUES[@]}"
CPU_RUN_COUNT=$(( ${#CPU_CORE_VALUES[@]} * ${#CPU_FRACTION_VALUES[@]} ))
[ "$RUN_RAM_SWEEP" -eq 1 ] || RAM_RUN_COUNT=0
[ "$RUN_CPU_MATRIX" -eq 1 ] || CPU_RUN_COUNT=0

# Results tracking
declare -a RUN_RESULTS
RUN_COUNT=0
PASS_COUNT=0
FAIL_COUNT=0

echo "============================================================"
echo " Resource experiment threshold sweep - $DATE_TAG"
echo " OpenMLS: $RAM_RUN_COUNT RAM + $CPU_RUN_COUNT CPU single-profile runs"
echo " Layout: $LOGICAL_WORKERS clients, $PROFILED_SINGLETON_COUNT profiled singleton"
echo " RAM CPUs: $RAM_SWEEP_CPU_COUNT reserved CPU thread(s)"
echo " Mode : scientific threshold (1 profiled container / run)"
echo " Policy: stop-on-profiled-failure"
echo "============================================================"
echo ""

# ==================================================================
# Common OpenMLS flags for scientific production runs
# ==================================================================
COMMON_ARGS=(
  --workers "$LOGICAL_WORKERS"
  --ds-port 3001
  --relay-port 4001
  --output-dir benchmark_output
  --worker-layout-mode hybrid
  --singleton-min-count 1
  --singleton-fraction "$SINGLETON_FRACTION"
  --profiled-singleton-count "$PROFILED_SINGLETON_COUNT"
  --cpu-affinity-mode profiled-nor-background
  --cpu-affinity-sample-seconds "$AFFINITY_SAMPLE_SECONDS"
  --resource-failure-policy stop-on-profiled-failure
  --resource-monitor-interval-ms 250
  --packed-clients-per-container "$PACKED_CLIENTS_PER_CONTAINER"
  --packed-worker-internal-parallelism "$PACKED_WORKER_INTERNAL_PARALLELISM"
  --bridge-count 4
  --force-cleanup-mls-ports
  --runner-in-docker
  --ds-delivery-mode group-log
  --process-pending-fanout
  --fanout-adaptive
  --max-fanout-parallelism 128
  --min-fanout-parallelism 16
  --fanout-error-rate-threshold 0.01
  --fanout-p95-threshold-ms 8000
  --http-pool-max-idle-per-host 64
  --runner-http-connect-timeout-ms 5000
  --runner-http-request-timeout-ms "$RUNNER_HTTP_REQUEST_TIMEOUT_MS"
  --worker-http-pool-max-idle-per-host 64
  --worker-http-connect-timeout-ms 5000
  --worker-http-request-timeout-ms "$WORKER_HTTP_REQUEST_TIMEOUT_MS"
  --worker-outbound-http-permits 32
  --compose-parallel-limit 48
  --startup-batch-size 64
  --startup-batch-sleep-seconds 0.5
  --post-startup-settle-seconds "$POST_STARTUP_SETTLE_SECONDS"
  --health-timeout-seconds "$HEALTH_TIMEOUT_SECONDS"
  --health-poll-seconds 0.5
  --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT_SECONDS"
  --worker-health-poll-ms 250
  --compose-down-timeout-seconds 2
  --teardown-batch-size 64
  --teardown-batch-sleep-seconds 0.1
  --min-size "$MIN_SIZE"
  --max-size "$MAX_SIZE"
  --step-size "$STEP_SIZE"
  --plateau-order "$PLATEAU_ORDER"
  --roundtrips "$ROUNDTRIPS"
  --update-rounds "$UPDATE_ROUNDS"
  --app-rounds "$APP_ROUNDS"
  --max-update-samples-per-plateau "$MAX_UPDATE_SAMPLES"
  --max-app-samples-per-payload "$MAX_APP_SAMPLES"
  --payload-sizes "$PAYLOAD_SIZES"
)

# ==================================================================
# Validation function
# ==================================================================
validate_run() {
  local run_dir="$1"
  local run_label="$2"
  local validator="${OPENMLS_DIR}/scripts/validate_resource_experiment_outputs.py"
  if [ -x /usr/bin/python3 ] || command -v python3 &>/dev/null; then
    if [ -f "$validator" ]; then
      python3 "$validator" "$run_dir" 2>&1 || true
    fi
  fi
}

run_r_analysis() {
  local run_dir="$1"
  local rscript="${OPENMLS_DIR}/statistics/resource_experiment_analysis.R"
  local analysis_dir="${run_dir}/resource_analysis"
  if command -v Rscript &>/dev/null; then
    if [ -f "$rscript" ]; then
      if Rscript -e 'quit(status = if (requireNamespace("optparse", quietly = TRUE)) 0 else 1)' \
          >/dev/null 2>&1; then
        Rscript "$rscript" -d "$run_dir" -o "$analysis_dir" 2>&1 || true
      else
        echo "[analysis] Skipping R analysis: package 'optparse' is not installed"
      fi
    fi
  fi
}

# ==================================================================
# Run a single profile
# ==================================================================
run_profile() {
  local label="$1"
  local experiment="$2"
  local profile_idx="$3"
  local run_id="$4"
  shift 4
  local extra_args=("$@")

  local scenario_seed
  scenario_seed="$(shuf -i 1-2147483647 -n 1)"

  local -a build_args=()
  if [ "$BUILD_IMAGES_NEXT_RUN" -eq 1 ]; then
    build_args=(--build-images)
    BUILD_IMAGES_NEXT_RUN=0
  fi

  echo ""
  echo "========== [$label] run-id: $run_id =========="
  echo "  experiment=$experiment profile_index=$profile_idx"
  echo "  scenario_seed=$scenario_seed"
  echo ""

  cd "$OPENMLS_DIR"

  local -a _args=(
    "${COMMON_ARGS[@]}"
    --resource-experiment "$experiment"
    --resource-profile-index "$profile_idx"
    --scenario-seed "$scenario_seed"
    "${build_args[@]}"
    "${extra_args[@]}"
    --run-id "$run_id"
  )

  local exit_code=0
  OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  "$PYTHON_BIN" scripts/run_compose_benchmark.py "${_args[@]}" || exit_code=$?

  cd "$SCRIPT_DIR"

  local run_dir="${OPENMLS_DIR}/benchmark_output/${run_id}"

  if [ "$exit_code" -eq 0 ]; then
    echo "[$label] Runner exited OK"
  else
    echo "[$label] Runner exited with code $exit_code — collecting diagnostics"
  fi

  # Always validate and analyze
  if [ -d "$run_dir" ]; then
    validate_run "$run_dir" "$label"
    run_r_analysis "$run_dir"
  fi

  cleanup_docker

  # Record result
  local status="PASS"
  if [ "$exit_code" -ne 0 ]; then
    status="FAIL (exit=$exit_code)"
  fi

  RUN_RESULTS+=("${label}|${run_id}|${status}")
  RUN_COUNT=$((RUN_COUNT + 1))
  if [ "$exit_code" -eq 0 ]; then
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi

  echo "-------- $label done. --------"
}

# ==================================================================
# RAM sweep
# ==================================================================
# Default RAM values: 32m, 64m, 128m, 256m, 512m, 1g
# Generated by: generate_ram_sweep_profiles with ram_sweep_cpu_count=10
echo ""
echo "### RAM sweep — $RAM_RUN_COUNT profiles ###"

if [ "$RUN_RAM_SWEEP" -eq 1 ]; then
  for IDX in "${!RAM_VALUES[@]}"; do
    run_profile \
      "RAM-${RAM_VALUES[$IDX]}" \
      "ram-sweep-singleton" \
      "$IDX" \
      "openmls_ram_sweep_idx${IDX}_${DATE_TAG}" \
      --ram-sweep-values "$RAM_SWEEP_VALUES" \
      --ram-sweep-cpu-count "$RAM_SWEEP_CPU_COUNT"
  done
fi

# ==================================================================
# CPU matrix
# ==================================================================
# Default matrix: 1,2,4 cores × 0.25,0.50,0.75,1.00 fractions
echo ""
echo "### CPU matrix — $CPU_RUN_COUNT profiles ###"

if [ "$RUN_CPU_MATRIX" -eq 1 ]; then
  for ((IDX = 0; IDX < CPU_RUN_COUNT; IDX++)); do
    run_profile \
      "CPU-matrix-idx${IDX}" \
      "cpu-matrix-singleton" \
      "$IDX" \
      "openmls_cpu_matrix_idx${IDX}_${DATE_TAG}" \
      --cpu-matrix-core-counts "$CPU_MATRIX_CORE_COUNTS" \
      --cpu-matrix-capacity-fractions "$CPU_MATRIX_CAPACITY_FRACTIONS"
  done
fi

# ==================================================================
# Final summary
# ==================================================================
{
  echo ""
  echo "============================================================"
  echo " Resource experiment sweep summary - $DATE_TAG"
  echo " Total runs: $RUN_COUNT | Pass: $PASS_COUNT | Fail: $FAIL_COUNT"
  echo "============================================================"
  echo ""
  echo "label|run_id|status"
  echo "----|------|------"
  for entry in "${RUN_RESULTS[@]}"; do
    echo "$entry"
  done
} | tee "$SUMMARY_FILE"

echo ""
echo "Summary written to: $SUMMARY_FILE"
echo ""
echo "All resource experiment runs complete ($DATE_TAG)"
echo " $RAM_RUN_COUNT × RAM sweep  (single profile)"
echo " $CPU_RUN_COUNT × CPU matrix (single profile)"
echo "============================================================"

if [ "$FAIL_COUNT" -ne 0 ]; then
  exit 1
fi
