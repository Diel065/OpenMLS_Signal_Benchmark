#!/usr/bin/env bash
#
# OpenMLS Parallel Resource Sweep Benchmarks
# -------------------------------------------
# Runs 10-profile parallel resource sweeps using 10 profiled singleton
# workers simultaneously. Each profiled worker tests one resource level.
# All non-profiled workers are packed with maximum density for speed.
#
# Two sweep types:
#   SWEEP=ram:  10 app-heap budgets (32k..1g), Docker memory held high
#   SWEEP=cpu:  10 Docker CPU fractions (default: 1.00..0.01), app-heap held high
#   SWEEP=both: run both sweep types
#
# Usage:
#   SWEEP=ram   bash benchmark_scripts/run_openmls_parallel_resource_sweeps.sh
#   SWEEP=cpu   bash benchmark_scripts/run_openmls_parallel_resource_sweeps.sh
#   SWEEP=both  bash benchmark_scripts/run_openmls_parallel_resource_sweeps.sh
#
# Environment variables:
#   STRICT_CPUSET=0   (default) allow fallback on too-few-cores hosts
#   STRICT_CPUSET=1   fail if 8 distinct profiled cores unavailable
#   CPU_SWEEP_FRACTIONS=1.0,0.5,... override the default Docker-valid CPU sweep
#   PLATEAU_ORDER=ascending|staircase|randomized (default: ascending)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OPENMLS_DIR="$(cd "$SCRIPT_DIR/../OpenMLS_containerized" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

SWEEP="${SWEEP:-both}"
STRICT_CPUSET="${STRICT_CPUSET:-0}"

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
PLATEAU_ORDER="${PLATEAU_ORDER:-ascending}"

MAX_UPDATE_SAMPLES_PER_PLATEAU="${MAX_UPDATE_SAMPLES_PER_PLATEAU:-4}"
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-4}"

CPU_AFFINITY_SAMPLE="${CPU_AFFINITY_SAMPLE:-20}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"
CPU_SWEEP_FRACTIONS="${CPU_SWEEP_FRACTIONS:-1.00,0.75,0.50,0.25,0.10,0.05,0.04,0.03,0.02,0.01}"

export PATH="$HOME/.cargo/bin:$PATH"

log()   { printf '\n===== %s =====\n' "$*"; }
warn()  { printf 'WARN: %s\n' "$*" >&2; }

ONLINE_CPUS=0

check_cpu_cores() {
    ONLINE_CPUS="$(nproc 2>/dev/null || echo 0)"
    echo "[cpu] Online CPU cores on this host: $ONLINE_CPUS"
    if [ "$ONLINE_CPUS" -lt "$PROFILED_SINGLETON_COUNT" ]; then
        if [ "$STRICT_CPUSET" = "1" ]; then
            echo "ERROR: Strict cpuset mode requires >= $PROFILED_SINGLETON_COUNT online cores, have $ONLINE_CPUS"
            exit 1
        fi
        warn "Host has fewer than $PROFILED_SINGLETON_COUNT online CPU cores ($ONLINE_CPUS). Strict isolation not achievable."
    elif [ "$ONLINE_CPUS" -lt $((PROFILED_SINGLETON_COUNT + 4)) ]; then
        warn "Host has only $ONLINE_CPUS cores. $PROFILED_SINGLETON_COUNT profiled + background need > $PROFILED_SINGLETON_COUNT ideally."
    else
        echo "[cpu] $ONLINE_CPUS cores available — sufficient for $PROFILED_SINGLETON_COUNT profiled + background."
    fi
}

cleanup_docker() {
    echo "[cleanup] Stopping leftover containers..."
    docker compose -f "$OPENMLS_DIR/docker-compose.yml" down --timeout 2 2>/dev/null || true
    for f in "$OPENMLS_DIR"/docker-compose_benchmark_*.yml "$OPENMLS_DIR"/docker-compose.*.generated.yml; do
        [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
    done
    docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
    # Fix root-owned output from prior sudo runs
    local outdir="$OPENMLS_DIR/benchmark_output"
    if [ -d "$outdir" ] && [ "$(stat -c '%U' "$outdir")" = "root" ]; then
        sudo chown -R "$(whoami):$(whoami)" "$outdir" 2>/dev/null || true
    fi
}

run_parallel_sweep() {
    local sweep_type="$1"
    local sweep_label="$2"
    local run_id="parallel_${sweep_type}_${DATE_TAG}"
    local run_dir="$OPENMLS_DIR/benchmark_output/$run_id"

    log "Parallel $sweep_label sweep — $run_id"
    echo "  Workers: $WORKERS  |  Profiled: $PROFILED_SINGLETON_COUNT singleton(s)"
    echo "  Packed: $((WORKERS - PROFILED_SINGLETON_COUNT)) clients in $(( (WORKERS - PROFILED_SINGLETON_COUNT + PACKED_PER_CONTAINER - 1) / PACKED_PER_CONTAINER )) containers  |  Max group: $MAX_GROUP_SIZE"
    echo "  Strict cpuset: $STRICT_CPUSET"

    local affinity_mode="none"
    if [ "$ONLINE_CPUS" -ge "$PROFILED_SINGLETON_COUNT" ]; then
        affinity_mode="profiled-nor-background"
        echo "  CPU affinity: $affinity_mode ($ONLINE_CPUS cores available)"
    else
        echo "  CPU affinity: $affinity_mode (only $ONLINE_CPUS cores; strict isolation unavailable)"
    fi

    local scenario_seed singleton_selection_seed
    scenario_seed="$(shuf -i 1-2155583647 -n 1)"
    singleton_selection_seed="$(shuf -i 1-2147483317 -n 1)"

    local python_bin="python3"
    [ -x "$OPENMLS_DIR/.venv/bin/python" ] && python_bin="$OPENMLS_DIR/.venv/bin/python"

    local cmd=(
        "$python_bin" scripts/run_compose_benchmark.py
        --workers               "$WORKERS"
        --run-id                "$run_id"
        --scenario-seed         "$scenario_seed"
        --singleton-selection-seed "$singleton_selection_seed"
        --output-dir            benchmark_output
        --worker-layout-mode    hybrid
        --singleton-min-count   "$SINGLETON_MIN_COUNT"
        --singleton-fraction    "$SINGLETON_FRACTION"
        --packed-clients-per-container "$PACKED_PER_CONTAINER"
        --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM"
        --bridge-count          "$BRIDGE_COUNT"
        --resource-experiment   "$sweep_type"
        --profiled-singleton-count "$PROFILED_SINGLETON_COUNT"
        --resource-failure-policy remove-and-continue
        --cpu-affinity-mode     "$affinity_mode"
        --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE"
        --embedded-docker-memory 4g
        --cpu-sweep-fractions   "$CPU_SWEEP_FRACTIONS"
        --health-timeout-seconds "$HEALTH_TIMEOUT"
        --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT"
        --health-poll-seconds    0.5
        --worker-health-poll-ms  250
        --min-size              2
        --max-size              "$MAX_GROUP_SIZE"
        --step-size             "$STEP_SIZE"
        --plateau-order         "$PLATEAU_ORDER"
        --roundtrips            "$ROUNDTRIPS"
        --update-rounds         "$UPDATE_ROUNDS"
        --app-rounds            "$APP_ROUNDS"
        --max-update-samples-per-plateau "$MAX_UPDATE_SAMPLES_PER_PLATEAU"
        --max-app-samples-per-payload    "$MAX_APP_SAMPLES_PER_PAYLOAD"
        --payload-sizes         32,256,2048
        --http-pool-max-idle-per-host   "$WORKER_HTTP_POOL"
        --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL"
        --worker-outbound-http-permits     "$WORKER_OUTBOUND_PERMITS"
        --max-fanout-parallelism   "$FANOUT_PARALLELISM"
        --min-fanout-parallelism   "$FANOUT_MIN"
        --force-cleanup-mls-ports
        --no-aggregate
        --resource-output-validation
        --build-images
    )

    echo "[run] cd $OPENMLS_DIR && ${cmd[*]}"
    cd "$OPENMLS_DIR"

    local exit_code=0
    "${cmd[@]}" 2>&1 | tee "/tmp/${run_id}.log" || exit_code=$?

    cd "$SCRIPT_DIR"

    [ "$exit_code" -ne 0 ] && warn "Benchmark runner exited with code $exit_code"

    echo ""
    echo "[validate] Running output validation..."
    local validator="$OPENMLS_DIR/scripts/validate_resource_experiment_outputs.py"
    if [ -f "$validator" ]; then
        python3 "$validator" "$run_dir" 2>&1 || {
            warn "Resource experiment output validation FAILED for $run_id"
            return 1
        }
        echo "[validate] Output validation PASSED for $run_id"
    else
        warn "Validator not found: $validator"
    fi

    if [ "$exit_code" -ne 0 ]; then
        return "$exit_code"
    fi

    echo ""
    echo "[done] Run $run_id completed.  Output: $run_dir"
    return 0
}

main() {
    echo "============================================================"
    echo " OpenMLS Parallel Resource Sweeps"
    echo " Sweep: $SWEEP  |  Workers: $WORKERS  |  Strict: $STRICT_CPUSET"
    echo " Date: $DATE_TAG"
    echo "============================================================"

    check_cpu_cores
    cleanup_docker

    local overall_exit=0

    case "$SWEEP" in
        ram)  run_parallel_sweep "ram-app-heap-sweep" "RAM App-Heap"   || overall_exit=1 ;;
        cpu)  run_parallel_sweep "cpu-quota-sweep"    "CPU Quota"      || overall_exit=1 ;;
        both) run_parallel_sweep "ram-app-heap-sweep" "RAM App-Heap"   || overall_exit=1
              cleanup_docker
              run_parallel_sweep "cpu-quota-sweep"    "CPU Quota"      || overall_exit=1 ;;
        *)    echo "ERROR: SWEEP must be 'ram', 'cpu', or 'both', got '$SWEEP'"; exit 1 ;;
    esac

    cleanup_docker

    echo ""
    echo "============================================================"
    if [ "$overall_exit" -eq 0 ]; then echo " All parallel sweeps passed."; else echo " One or more parallel sweeps FAILED."; fi
    echo "============================================================"
    exit $overall_exit
}

main "$@"
