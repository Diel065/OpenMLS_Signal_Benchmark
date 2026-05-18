#!/usr/bin/env bash
#
# OpenMLS benchmark runner
# -----------------------------------------
# Runs 10 iterations of the OpenMLS benchmark.
# Each run waits for the previous to fully finish and all
# OpenMLS containers to be torn down before starting the next.
#
# All flags are written out literally so you can tweak anything
# without hunting for variables.
#
# Usage:
#   chmod +x run_benchmark_alternating.sh
#   sudo bash run_benchmark_alternating.sh
#
# Run from the repository root (parent of *_containerized/).
# Requires docker, python3 with pyyaml, and the Rust musl target.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

cleanup_docker() {
  echo "===== Cleaning up all dangling containers / networks ====="
  # Remove any leftover OpenMLS containers
  docker compose -f "$SCRIPT_DIR/OpenMLS_containerized/docker-compose.yml" down --timeout 2 2>/dev/null || true
  # Nuke any leftover generated compose files
  for f in "$SCRIPT_DIR/OpenMLS_containerized"/docker-compose_benchmark_*.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  # Force-remove any stray containers from mls-* images
  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
}

echo "============================================================"
echo " OpenMLS benchmark suite  —  $DATE_TAG"
echo " 10 × OpenMLS  =  10 runs total"
echo "============================================================"
echo ""

# ------------------------------------------------------------------
# OpenMLS benchmark command  (all flags explicit, no variables)
# ------------------------------------------------------------------
run_openmls() {
  local ITER="$1"
  local RUN_ID="openmls_run_${ITER}_${DATE_TAG}"

  # Randomize seeds for each run to ensure data variance
  local SCENARIO_SEED
  local SINGLETON_SELECTION_SEED
  SCENARIO_SEED="$(shuf -i 1-2155583647 -n 1)"
  SINGLETON_SELECTION_SEED="$(shuf -i 1-2147483317 -n 1)"

  echo ""
  echo "========== [OpenMLS iteration $ITER / 10]  run-id: $RUN_ID =========="
  echo "  scenario_seed=$SCENARIO_SEED  singleton_selection_seed=$SINGLETON_SELECTION_SEED"
  echo ""

  cd "$SCRIPT_DIR/OpenMLS_containerized"

  OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  .venv/bin/python scripts/run_compose_benchmark.py \
    --workers 1024 \
    --scenario-seed "$SCENARIO_SEED" \
    --singleton-selection-seed "$SINGLETON_SELECTION_SEED" \
    --worker-layout-mode hybrid \
    --singleton-min-count 12 \
    --singleton-fraction 0.0625 \
    --singleton-selection-strategy evenly-spaced \
    --packed-clients-per-container 48 \
    --packed-worker-internal-parallelism 16 \
    --bridge-count 4 \
    --build-images \
    --force-cleanup-mls-ports \
    --runner-in-docker \
    --ds-delivery-mode group-log \
    --process-pending-fanout \
    --fanout-adaptive \
    --max-fanout-parallelism 128 \
    --min-fanout-parallelism 16 \
    --fanout-error-rate-threshold 0.01 \
    --fanout-p95-threshold-ms 8000 \
    --http-pool-max-idle-per-host 64 \
    --runner-http-connect-timeout-ms 5000 \
    --runner-http-request-timeout-ms 120000 \
    --worker-http-pool-max-idle-per-host 64 \
    --worker-http-connect-timeout-ms 5000 \
    --worker-http-request-timeout-ms 45000 \
    --worker-outbound-http-permits 32 \
    --compose-parallel-limit 48 \
    --startup-batch-size 64 \
    --startup-batch-sleep-seconds 0.5 \
    --post-startup-settle-seconds 10 \
    --health-timeout-seconds 240 \
    --health-poll-seconds 0.5 \
    --worker-health-timeout-seconds 600 \
    --worker-health-poll-ms 250 \
    --compose-down-timeout-seconds 2 \
    --teardown-batch-size 64 \
    --teardown-batch-sleep-seconds 0.1 \
    --min-size 2 \
    --max-size 1024 \
    --step-size 64 \
    --roundtrips 1 \
    --update-rounds 2 \
    --app-rounds 2 \
    --max-update-samples-per-plateau 2 \
    --max-app-samples-per-payload 2 \
    --payload-sizes 32 \
    --devices-file devices.yaml \
    --enable-external-devices \
    --external-device luckfox-pico-plus-01 \
    --external-device raspberry-pi-01 \
    --external-coverage-lane \
    --wipe-device-run-dirs \
    --run-id "$RUN_ID"

  cd "$SCRIPT_DIR"

  echo ""
  echo "-------- OpenMLS iteration $ITER done. Cleaning up. --------"
  cleanup_docker
}


# ==================================================================
# Main loop: 10 OpenMLS iterations
# ==================================================================

# Start clean
cleanup_docker

for I in $(seq 1 10); do
  run_openmls "$I"
done

echo ""
echo "============================================================"
echo " All 10 OpenMLS runs complete ($DATE_TAG)"
echo "============================================================"
