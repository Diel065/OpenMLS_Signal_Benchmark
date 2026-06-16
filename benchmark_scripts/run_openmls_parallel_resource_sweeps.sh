#!/usr/bin/env bash
#
# OpenMLS Parallel Resource Sweep Benchmarks
# -------------------------------------------
# Runs 8-profile parallel resource sweeps using 8 profiled singleton
# workers simultaneously. Each profiled worker tests one resource level.
#
# Two sweep types:
#   SWEEP=ram:  8 app-heap budgets (32k..1g), Docker memory held high
#   SWEEP=cpu:  8 Docker CPU fractions (1.00..0.0005), app-heap held high
#   SWEEP=both: run both sweep types
#
# Usage:
#   cd repo_root
#   bash benchmark_scripts/run_openmls_parallel_resource_sweeps.sh
#
# Environment variables:
#   MODE=smoke        (default) small run with group size 64
#   MODE=production   full benchmark
#   SWEEP=ram         (default) RAM/app-heap sweep
#   SWEEP=cpu         CPU quota sweep
#   SWEEP=both        both sweep types
#   STRICT_CPUSET=0   (default) allow fallback on too-few-cores hosts
#   STRICT_CPUSET=1   fail if 8 distinct profiled cores unavailable
#   MAX_GROUP_SIZE=64 (default for smoke) maximum group size
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OPENMLS_DIR="$(cd "$SCRIPT_DIR/../OpenMLS_containerized" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

MODE="${MODE:-smoke}"
SWEEP="${SWEEP:-ram}"
STRICT_CPUSET="${STRICT_CPUSET:-0}"

if [ "$MODE" = "smoke" ]; then
    MAX_GROUP_SIZE="${MAX_GROUP_SIZE:-64}"
    WORKERS=32
    SINGLETON_MIN_COUNT=8
    SINGLETON_FRACTION=0.25
    PACKED_PER_CONTAINER=4
    CPU_AFFINITY_SAMPLE=1
    HEALTH_TIMEOUT=120
    WORKER_HEALTH_TIMEOUT=180
elif [ "$MODE" = "production" ]; then
    MAX_GROUP_SIZE="${MAX_GROUP_SIZE:-256}"
    WORKERS=256
    SINGLETON_MIN_COUNT=16
    SINGLETON_FRACTION=0.125
    PACKED_PER_CONTAINER=32
    CPU_AFFINITY_SAMPLE=20
    HEALTH_TIMEOUT=240
    WORKER_HEALTH_TIMEOUT=600
else
    echo "ERROR: MODE must be 'smoke' or 'production', got '$MODE'"
    exit 1
fi

PROFILED_SINGLETON_COUNT=8

export PATH="$HOME/.cargo/bin:$PATH"

log() {
    printf '\n===== %s =====\n' "$*"
}

warn() {
    printf 'WARN: %s\n' "$*" >&2
}

check_cpu_cores() {
    local online
    online="$(nproc 2>/dev/null || echo 0)"
    echo "[cpu] Online CPU cores on this host: $online"

    if [ "$online" -lt 8 ]; then
        if [ "$STRICT_CPUSET" = "1" ]; then
            echo "ERROR: Strict cpuset mode requires >= 8 online cores, have $online"
            exit 1
        else
            warn "Host has fewer than 8 online CPU cores ($online)."
            warn "Strict CPU isolation (1 profiled worker per core) is not achievable."
            warn "Benchmarks will run but profiling isolation may be compromised."
        fi
    elif [ "$online" -lt 12 ]; then
        warn "Host has only $online online CPU cores."
        warn "8 profiled cores + DS/relay + packed workers need > 8 cores ideally."
    else
        echo "[cpu] $online cores available — sufficient for 8 profiled + background."
    fi
}

cleanup_docker() {
    echo "[cleanup] Stopping any leftover containers and compose projects..."
    docker compose -f "$OPENMLS_DIR/docker-compose.yml" down --timeout 2 2>/dev/null || true
    for f in "$OPENMLS_DIR"/docker-compose_benchmark_*.yml "$OPENMLS_DIR"/docker-compose.*.generated.yml; do
        [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
    done
    docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
}

run_parallel_sweep() {
    local sweep_type="$1"
    local sweep_label="$2"
    local run_id="parallel_${sweep_type}_${MODE}_${DATE_TAG}"
    local output_dir="$OPENMLS_DIR/benchmark_output"
    local run_dir="$output_dir/$run_id"

    log "Parallel $sweep_label sweep — $run_id"
    echo "  Mode: $MODE"
    echo "  Profiled singletons: $PROFILED_SINGLETON_COUNT"
    echo "  Max group size: $MAX_GROUP_SIZE"
    echo "  Workers: $WORKERS"
    echo "  Strict cpuset: $STRICT_CPUSET"
    echo ""

    local affinity_mode="none"
    if [ "$STRICT_CPUSET" = "1" ]; then
        affinity_mode="profiled-nor-background"
    fi

    local scenario_seed
    local singleton_selection_seed
    scenario_seed="$(shuf -i 1-2155583647 -n 1)"
    singleton_selection_seed="$(shuf -i 1-2147483317 -n 1)"

    local python_bin="python3"
    if [ -x "$OPENMLS_DIR/.venv/bin/python" ]; then
        python_bin="$OPENMLS_DIR/.venv/bin/python"
    fi

    local cmd=(
        "$python_bin" scripts/run_compose_benchmark.py
        --workers "$WORKERS"
        --run-id "$run_id"
        --scenario-seed "$scenario_seed"
        --singleton-selection-seed "$singleton_selection_seed"
        --output-dir benchmark_output
        --worker-layout-mode hybrid
        --singleton-min-count "$SINGLETON_MIN_COUNT"
        --singleton-fraction "$SINGLETON_FRACTION"
        --packed-clients-per-container "$PACKED_PER_CONTAINER"
        --packed-worker-internal-parallelism 4
        --resource-experiment "$sweep_type"
        --profiled-singleton-count "$PROFILED_SINGLETON_COUNT"
        --cpu-affinity-mode "$affinity_mode"
        --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE"
        --embedded-docker-memory 4g
        --health-timeout-seconds "$HEALTH_TIMEOUT"
        --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT"
        --health-poll-seconds 0.5
        --worker-health-poll-ms 250
        --min-size 2
        --max-size "$MAX_GROUP_SIZE"
        --step-size "$MAX_GROUP_SIZE"
        --roundtrips 1
        --update-rounds 1
        --app-rounds 1
        --max-update-samples-per-plateau 1
        --max-app-samples-per-payload 1
        --payload-sizes 32
        --no-aggregate
        --resource-output-validation
    )

    if [ "$MODE" = "production" ]; then
        cmd+=(--build-images)
    fi

    echo "[run] Starting benchmark..."
    echo "[run] cd $OPENMLS_DIR && ${cmd[*]}"

    cd "$OPENMLS_DIR"

    local exit_code=0
    "${cmd[@]}" 2>&1 | tee "$run_dir.log" || exit_code=$?

    cd "$SCRIPT_DIR"

    if [ "$exit_code" -ne 0 ]; then
        warn "Benchmark runner exited with code $exit_code"
    fi

    echo ""
    echo "[validate] Running output validation..."
    local validator="$OPENMLS_DIR/scripts/validate_resource_experiment_outputs.py"
    if [ -f "$validator" ]; then
        python3 "$validator" "$run_dir" 2>&1 || {
            warn "Resource experiment output validation FAILED for $run_id"
            echo "See $run_dir for details."
            return 1
        }
        echo "[validate] Output validation PASSED for $run_id"
    else
        warn "Validator not found: $validator"
    fi

    echo ""
    echo "[done] Run $run_id completed."
    echo "  Output: $run_dir"
    echo "  Log: $run_dir.log"
    return 0
}

main() {
    echo "============================================================"
    echo " OpenMLS Parallel Resource Sweeps"
    echo " Mode: $MODE | Sweep: $SWEEP | Strict cpuset: $STRICT_CPUSET"
    echo " Date: $DATE_TAG"
    echo "============================================================"

    check_cpu_cores

    cleanup_docker

    local overall_exit=0

    case "$SWEEP" in
        ram)
            run_parallel_sweep "ram-app-heap-sweep" "RAM App-Heap" || overall_exit=1
            ;;
        cpu)
            run_parallel_sweep "cpu-quota-sweep" "CPU Quota" || overall_exit=1
            ;;
        both)
            run_parallel_sweep "ram-app-heap-sweep" "RAM App-Heap" || overall_exit=1
            cleanup_docker
            run_parallel_sweep "cpu-quota-sweep" "CPU Quota" || overall_exit=1
            ;;
        *)
            echo "ERROR: SWEEP must be 'ram', 'cpu', or 'both', got '$SWEEP'"
            exit 1
            ;;
    esac

    cleanup_docker

    echo ""
    echo "============================================================"
    if [ "$overall_exit" -eq 0 ]; then
        echo " All parallel sweeps passed."
    else
        echo " One or more parallel sweeps FAILED."
    fi
    echo "============================================================"
    exit $overall_exit
}

main "$@"
