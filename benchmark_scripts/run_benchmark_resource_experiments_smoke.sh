#!/usr/bin/env bash
#
# Resource experiment smoke test
# ------------------------------
# Tiny worker counts and group sizes for fast testing.
# One profiled singleton per run, single profile index.
# Uses 1-second CPU affinity sampling.
#
# Usage:
#   chmod +x run_benchmark_resource_experiments_smoke.sh
#   bash run_benchmark_resource_experiments_smoke.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

export PATH="$HOME/.cargo/bin:$PATH"

python_for() {
  local stack_dir="$1"
  if [ -x "$stack_dir/.venv/bin/python" ]; then
    printf '%s\n' "$stack_dir/.venv/bin/python"
  else
    printf '%s\n' "python3"
  fi
}

cleanup_docker_smoke() {
  local dir="$SCRIPT_DIR/.."
  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=mls-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
}

PYTHON_BIN="$(python_for "$SCRIPT_DIR/../OpenMLS_containerized")"
OPENMLS_DIR="$(cd "$SCRIPT_DIR/../OpenMLS_containerized" && pwd)"

echo "============================================================"
echo " Resource experiment SMOKE test - $DATE_TAG"
echo "============================================================"
echo ""

# ── RAM smoke: profile index 0 (64m), tiny workers ──
echo "========== RAM smoke: profile index 0 =========="
cd "$OPENMLS_DIR"

set +e
OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=256 \
"$PYTHON_BIN" scripts/run_compose_benchmark.py \
  --workers 4 \
  --worker-layout-mode hybrid \
  --singleton-min-count 1 \
  --singleton-selection-strategy evenly-spaced \
  --resource-experiment ram-sweep-singleton \
  --profiled-singleton-count 1 \
  --resource-profile-index 0 \
  --ram-sweep-values 64m,128m \
  --ram-sweep-cpu-count 2 \
  --resource-failure-policy stop-on-profiled-failure \
  --cpu-affinity-mode profiled-nor-background \
  --cpu-affinity-sample-seconds 1 \
  --resource-monitor-interval-ms 500 \
  --packed-clients-per-container 2 \
  --packed-worker-internal-parallelism 2 \
  --bridge-count 1 \
  --build-images \
  --force-cleanup-mls-ports \
  --runner-in-docker \
  --ds-delivery-mode group-log \
  --compose-parallel-limit 8 \
  --startup-batch-size 8 \
  --startup-batch-sleep-seconds 0.2 \
  --post-startup-settle-seconds 3 \
  --health-timeout-seconds 60 \
  --health-poll-seconds 0.5 \
  --worker-health-timeout-seconds 120 \
  --worker-health-poll-ms 250 \
  --compose-down-timeout-seconds 2 \
  --min-size 2 \
  --max-size 4 \
  --step-size '[1,2]' \
  --roundtrips 1 \
  --update-rounds 1 \
  --app-rounds 1 \
  --payload-sizes '[16,256]' \
  --run-id "smoke_resource_ram_single_${DATE_TAG}"
ram_exit=$?
set -e

cd "$SCRIPT_DIR"
cleanup_docker_smoke

echo ""
if [ $ram_exit -eq 0 ]; then
  echo "RAM smoke PASSED"
else
  echo "RAM smoke exited with code $ram_exit"
fi

# Validate RAM smoke outputs
RAM_DIR="${OPENMLS_DIR}/benchmark_output/smoke_resource_ram_single_${DATE_TAG}"
if [ -d "$RAM_DIR" ]; then
  echo "[validate] Checking RAM smoke outputs..."
  python3 "${OPENMLS_DIR}/scripts/validate_resource_experiment_outputs.py" "$RAM_DIR" || true
fi

# ── CPU smoke: profile index 0 (1 core @ 25%), tiny workers ──
echo ""
echo "========== CPU smoke: profile index 0 =========="
cd "$OPENMLS_DIR"

set +e
OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=256 \
"$PYTHON_BIN" scripts/run_compose_benchmark.py \
  --workers 4 \
  --worker-layout-mode hybrid \
  --singleton-min-count 1 \
  --singleton-selection-strategy evenly-spaced \
  --resource-experiment cpu-matrix-singleton \
  --profiled-singleton-count 1 \
  --resource-profile-index 0 \
  --cpu-matrix-core-counts 1 \
  --cpu-matrix-capacity-fractions 0.50,1.00 \
  --resource-failure-policy stop-on-profiled-failure \
  --cpu-affinity-mode profiled-nor-background \
  --cpu-affinity-sample-seconds 1 \
  --resource-monitor-interval-ms 500 \
  --packed-clients-per-container 2 \
  --packed-worker-internal-parallelism 2 \
  --bridge-count 1 \
  --build-images \
  --force-cleanup-mls-ports \
  --runner-in-docker \
  --ds-delivery-mode group-log \
  --compose-parallel-limit 8 \
  --startup-batch-size 8 \
  --startup-batch-sleep-seconds 0.2 \
  --post-startup-settle-seconds 3 \
  --health-timeout-seconds 60 \
  --health-poll-seconds 0.5 \
  --worker-health-timeout-seconds 120 \
  --worker-health-poll-ms 250 \
  --compose-down-timeout-seconds 2 \
  --min-size 2 \
  --max-size 4 \
  --step-size '[1,2]' \
  --roundtrips 1 \
  --update-rounds 1 \
  --app-rounds 1 \
  --payload-sizes '[16,256]' \
  --run-id "smoke_resource_cpu_single_${DATE_TAG}"
cpu_exit=$?
set -e

cd "$SCRIPT_DIR"
cleanup_docker_smoke

echo ""
if [ $cpu_exit -eq 0 ]; then
  echo "CPU smoke PASSED"
else
  echo "CPU smoke exited with code $cpu_exit"
fi

# Validate CPU smoke outputs
CPU_DIR="${OPENMLS_DIR}/benchmark_output/smoke_resource_cpu_single_${DATE_TAG}"
if [ -d "$CPU_DIR" ]; then
  echo "[validate] Checking CPU smoke outputs..."
  python3 "${OPENMLS_DIR}/scripts/validate_resource_experiment_outputs.py" "$CPU_DIR" || true
fi

echo ""
echo "============================================================"
echo " Smoke tests complete"
echo " RAM smoke exit code: $ram_exit"
echo " CPU smoke exit code: $cpu_exit"
echo "============================================================"
