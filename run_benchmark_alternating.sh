#!/usr/bin/env bash
#
# Alternating benchmark runner
# -----------------------------------------
# Runs 10 iterations of OpenMLS then 10 of Signal, alternating.
# Each benchmark waits for the previous to fully finish and all
# containers to be torn down before starting the next.
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
  # Remove any leftover containers from either benchmark
  docker compose -f "$SCRIPT_DIR/OpenMLS_containerized/docker-compose.yml" down --timeout 2 2>/dev/null || true
  docker compose -f "$SCRIPT_DIR/Signal_containerized/docker-compose.yml" down --timeout 2 2>/dev/null || true
  # Nuke any leftover generated compose files
  for f in "$SCRIPT_DIR/OpenMLS_containerized"/docker-compose_benchmark_*.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  for f in "$SCRIPT_DIR/Signal_containerized"/docker-compose_benchmark_*.yml; do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
  # Force-remove any stray containers from mls-* / signal-* images
  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker container ls -aq --filter "name=signal-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
}

echo "============================================================"
echo " Alternating benchmark suite  —  $DATE_TAG"
echo " 10 × OpenMLS + 10 × Signal  =  20 runs total"
echo "============================================================"
echo ""

# ------------------------------------------------------------------
# OpenMLS benchmark command  (all flags explicit, no variables)
# ------------------------------------------------------------------
run_openmls() {
  local ITER="$1"
  local RUN_ID="openmls_run_${ITER}_${DATE_TAG}"

  echo ""
  echo "========== [OpenMLS iteration $ITER / 10]  run-id: $RUN_ID =========="
  echo ""

  cd "$SCRIPT_DIR/OpenMLS_containerized"

  OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  .venv/bin/python scripts/run_compose_benchmark.py \
    --workers 512 \
    --worker-layout-mode hybrid \
    --singleton-min-count 16 \
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
    --max-size 512 \
    --step-size 16 \
    --roundtrips 1 \
    --update-rounds 4 \
    --app-rounds 4 \
    --max-update-samples-per-plateau 4 \
    --max-app-samples-per-payload 4 \
    --payload-sizes 32,64,256 \
    --devices-file devices.yaml \
    --enable-external-devices \
    --external-device luckfox-pico-plus-01 \
    --wipe-device-run-dirs \
    --run-id "$RUN_ID"

  cd "$SCRIPT_DIR"

  echo ""
  echo "-------- OpenMLS iteration $ITER done. Cleaning up. --------"
  cleanup_docker
}

# ------------------------------------------------------------------
# Signal benchmark command  (all flags explicit, no variables)
# ------------------------------------------------------------------
run_signal() {
  local ITER="$1"
  local RUN_ID="signal_run_${ITER}_${DATE_TAG}"

  echo ""
  echo "========== [Signal iteration $ITER / 10]  run-id: $RUN_ID =========="
  echo ""

  cd "$SCRIPT_DIR/Signal_containerized"

  SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  .venv/bin/python scripts/run_compose_benchmark.py \
    --workers 512 \
    --worker-layout-mode hybrid \
    --singleton-min-count 16 \
    --singleton-fraction 0.0625 \
    --singleton-selection-strategy evenly-spaced \
    --packed-clients-per-container 48 \
    --packed-worker-internal-parallelism 16 \
    --bridge-count 4 \
    --build-images \
    --force-cleanup-signal-ports \
    --runner-in-docker \
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
    --max-size 512 \
    --step-size 16 \
    --roundtrips 1 \
    --app-rounds 4 \
    --max-app-samples-per-payload 4 \
    --payload-sizes 32,64,256 \
    --devices-file devices.yaml \
    --enable-external-devices \
    --external-device luckfox-pico-plus-01 \
    --wipe-device-run-dirs \
    --run-id "$RUN_ID"

  cd "$SCRIPT_DIR"

  echo ""
  echo "-------- Signal iteration $ITER done. Cleaning up. --------"
  cleanup_docker
}

# ==================================================================
# Main loop: 10 iterations alternating OpenMLS / Signal
# ==================================================================

# Start clean
cleanup_docker

for I in $(seq 1 10); do
  run_openmls "$I"
  run_signal "$I"
done

echo ""
echo "============================================================"
echo " All 20 runs complete ($DATE_TAG)"
echo "============================================================"
