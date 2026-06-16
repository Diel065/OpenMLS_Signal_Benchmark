#!/usr/bin/env bash
#
# Resource experiment MULTIPLEXED STRESS runner (NOT for threshold analysis)
# ---------------------------------------------------------------------------
# WARNING: This is a multiplexed stress/integration run.
# Do not use this output as clean resource-threshold data.
#
# Runs 6 profiled singletons per invocation with different profiles.
# Useful for integration/stress testing only.
# For scientific threshold data, use run_benchmark_resource_experiments.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

export PATH="$HOME/.cargo/bin:$PATH"

echo "WARNING: This is a multiplexed stress/integration run."
echo "Do not use this output as clean resource-threshold data."
echo ""

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
  for f in "$dir"/docker-compose_benchmark_*.yml "$dir"/docker-compose.*.generated.yml; do
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
PROFILED_SINGLETON_COUNT=6
AFFINITY_SAMPLE_SECONDS=5

echo "============================================================"
echo " Resource experiment MULTIPLEXED STRESS - $DATE_TAG"
echo " OpenMLS: 6 iterations alternating RAM sweep / CPU matrix"
echo " Workers: 128, profiled singletons: $PROFILED_SINGLETON_COUNT per run"
echo "============================================================"
echo ""

run_openmls_ram_sweep() {
  local ITER="$1"
  local RUN_ID="openmls_stress_ram_${ITER}_${DATE_TAG}"
  local SCENARIO_SEED SINGLETON_SELECTION_SEED
  SCENARIO_SEED="$(shuf -i 1-2147483647 -n 1)"
  SINGLETON_SELECTION_SEED="$(shuf -i 1-2147483647 -n 1)"

  echo "========== [RAM stress $ITER] run-id: $RUN_ID =========="
  cd "$OPENMLS_DIR"

  local exit_code=0
  OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=256 \
  "$PYTHON_BIN" scripts/run_compose_benchmark.py \
    --workers 128 --ds-port 3001 --relay-port 4001 \
    --scenario-seed "$SCENARIO_SEED" --singleton-selection-seed "$SINGLETON_SELECTION_SEED" \
    --output-dir benchmark_output --worker-layout-mode hybrid \
    --singleton-min-count 12 --singleton-fraction 0.0625 \
    --singleton-selection-strategy evenly-spaced \
    --resource-experiment ram-sweep-singleton \
    --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
    --ram-sweep-values 32m,64m,128m,256m,512m,1g --ram-sweep-cpu-count 10 \
    --cpu-affinity-mode profiled-nor-background \
    --cpu-affinity-sample-seconds "$AFFINITY_SAMPLE_SECONDS" \
    --resource-monitor-interval-ms 250 --packed-clients-per-container 48 \
    --packed-worker-internal-parallelism 16 --bridge-count 4 --build-images \
    --force-cleanup-mls-ports --runner-in-docker --ds-delivery-mode group-log \
    --process-pending-fanout --fanout-adaptive --max-fanout-parallelism 128 \
    --min-fanout-parallelism 16 --fanout-error-rate-threshold 0.01 \
    --fanout-p95-threshold-ms 8000 --http-pool-max-idle-per-host 64 \
    --runner-http-connect-timeout-ms 5000 --runner-http-request-timeout-ms 120000 \
    --worker-http-pool-max-idle-per-host 64 --worker-http-connect-timeout-ms 5000 \
    --worker-http-request-timeout-ms 45000 --worker-outbound-http-permits 32 \
    --compose-parallel-limit 48 --startup-batch-size 64 \
    --startup-batch-sleep-seconds 0.5 --post-startup-settle-seconds 10 \
    --health-timeout-seconds 240 --health-poll-seconds 0.5 \
    --worker-health-timeout-seconds 600 --worker-health-poll-ms 250 \
    --compose-down-timeout-seconds 2 --teardown-batch-size 64 \
    --teardown-batch-sleep-seconds 0.1 --min-size 2 --max-size 128 \
    --step-size '[1,32]' --roundtrips 2 --update-rounds 8 --app-rounds 8 \
    --max-update-samples-per-plateau 8 --max-app-samples-per-payload 8 \
    --payload-sizes '[16,4096]' --run-id "$RUN_ID" || exit_code=$?

  cd "$SCRIPT_DIR"
  cleanup_docker
  return $exit_code
}

run_openmls_cpu_matrix() {
  local ITER="$1"
  local RUN_ID="openmls_stress_cpu_${ITER}_${DATE_TAG}"
  local SCENARIO_SEED SINGLETON_SELECTION_SEED
  SCENARIO_SEED="$(shuf -i 1-2147483647 -n 1)"
  SINGLETON_SELECTION_SEED="$(shuf -i 1-2147483647 -n 1)"

  echo "========== [CPU stress $ITER] run-id: $RUN_ID =========="
  cd "$OPENMLS_DIR"

  local exit_code=0
  OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=256 \
  "$PYTHON_BIN" scripts/run_compose_benchmark.py \
    --workers 128 --ds-port 3001 --relay-port 4001 \
    --scenario-seed "$SCENARIO_SEED" --singleton-selection-seed "$SINGLETON_SELECTION_SEED" \
    --output-dir benchmark_output --worker-layout-mode hybrid \
    --singleton-min-count 12 --singleton-fraction 0.0625 \
    --singleton-selection-strategy evenly-spaced \
    --resource-experiment cpu-matrix-singleton \
    --profiled-singleton-count "$PROFILED_SINGLETON_COUNT" \
    --cpu-matrix-core-counts 1,2,4 --cpu-matrix-capacity-fractions 0.25,0.50,0.75,1.00 \
    --cpu-affinity-mode profiled-nor-background \
    --cpu-affinity-sample-seconds "$AFFINITY_SAMPLE_SECONDS" \
    --resource-monitor-interval-ms 250 --packed-clients-per-container 48 \
    --packed-worker-internal-parallelism 16 --bridge-count 4 --build-images \
    --force-cleanup-mls-ports --runner-in-docker --ds-delivery-mode group-log \
    --process-pending-fanout --fanout-adaptive --max-fanout-parallelism 128 \
    --min-fanout-parallelism 16 --fanout-error-rate-threshold 0.01 \
    --fanout-p95-threshold-ms 8000 --http-pool-max-idle-per-host 64 \
    --runner-http-connect-timeout-ms 5000 --runner-http-request-timeout-ms 120000 \
    --worker-http-pool-max-idle-per-host 64 --worker-http-connect-timeout-ms 5000 \
    --worker-http-request-timeout-ms 45000 --worker-outbound-http-permits 32 \
    --compose-parallel-limit 48 --startup-batch-size 64 \
    --startup-batch-sleep-seconds 0.5 --post-startup-settle-seconds 10 \
    --health-timeout-seconds 240 --health-poll-seconds 0.5 \
    --worker-health-timeout-seconds 600 --worker-health-poll-ms 250 \
    --compose-down-timeout-seconds 2 --teardown-batch-size 64 \
    --teardown-batch-sleep-seconds 0.1 --min-size 2 --max-size 128 \
    --step-size '[1,32]' --roundtrips 2 --update-rounds 8 --app-rounds 8 \
    --max-update-samples-per-plateau 8 --max-app-samples-per-payload 8 \
    --payload-sizes '[16,4096]' --run-id "$RUN_ID" || exit_code=$?

  cd "$SCRIPT_DIR"
  cleanup_docker
  return $exit_code
}

cleanup_docker

for I in $(seq 1 3); do
  run_openmls_ram_sweep "$I" || true
  run_openmls_cpu_matrix "$I" || true
done

echo ""
echo "============================================================"
echo " All 6 multiplexed stress runs complete ($DATE_TAG)"
echo " REMINDER: This is stress data, not clean threshold evidence."
echo "============================================================"
