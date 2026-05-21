#!/usr/bin/env bash
#
# Alternating benchmark runner — NO external devices / NO resource constraints
# -----------------------------------------
# Runs 16 iterations, alternating OpenMLS and Signal.
# Each benchmark waits for the previous one to finish and then tears down
# containers before starting the next benchmark.
#
# Usage:
#   chmod +x run_benchmark_no_iot.sh
#   bash run_benchmark_no_iot.sh
#
# Run from the repository root (parent of *_containerized/).
# Requires Docker and the project Python environments with pyyaml and pexpect.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

# Ensure cargo is on PATH (it's in ~/.cargo/bin but sudo may not have it)
export PATH="$HOME/.cargo/bin:$PATH"

if command -v cargo &>/dev/null; then
  BUILD_EXTERNAL_FLAG="--build-external-binaries"
else
  echo "[setup] WARNING: 'cargo' not found on PATH -- --build-external-binaries will be skipped." >&2
  echo "[setup] Install Rust (rustup) to enable automatic cross-compilation of external worker binaries." >&2
  BUILD_EXTERNAL_FLAG=""
fi

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
  local f

  for f in \
    "$dir"/docker-compose_benchmark_*.yml \
    "$dir"/docker-compose.*.generated.yml
  do
    [ -f "$f" ] && docker compose -f "$f" down --timeout 2 2>/dev/null || true
  done
}

cleanup_docker() {
  echo "===== Cleaning up leftover benchmark containers / networks ====="

  if [ -f "$SCRIPT_DIR/OpenMLS_containerized/docker-compose.yml" ]; then
    docker compose -f "$SCRIPT_DIR/OpenMLS_containerized/docker-compose.yml" down --timeout 2 2>/dev/null || true
  fi
  if [ -f "$SCRIPT_DIR/Signal_containerized/docker-compose.yml" ]; then
    docker compose -f "$SCRIPT_DIR/Signal_containerized/docker-compose.yml" down --timeout 2 2>/dev/null || true
  fi

  cleanup_generated_compose "$SCRIPT_DIR/OpenMLS_containerized"
  cleanup_generated_compose "$SCRIPT_DIR/Signal_containerized"

  docker container ls -aq --filter "name=mls-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker container ls -aq --filter "name=signal-" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=mls-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
  docker network ls -q --filter "name=signal-" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
}

trap cleanup_docker EXIT

# ------------------------------------------------------------------
# Virtual environment setup
# ------------------------------------------------------------------
ensure_venv() {
  local stack_dir="$1"
  local req="$stack_dir/requirements.txt"

  if [ ! -f "$req" ]; then
    echo "[setup] WARNING: no requirements.txt at $req, skipping"
    return
  fi

  if [ ! -d "$stack_dir/.venv" ]; then
    echo "[setup] Creating .venv in $stack_dir ..."
    python3 -m venv "$stack_dir/.venv"
  fi

  echo "[setup] Updating .venv in $stack_dir ..."
  "$stack_dir/.venv/bin/pip" install -q -r "$req" 2>/dev/null || \
    "$stack_dir/.venv/bin/pip" install --break-system-packages -q -r "$req"
}

# ------------------------------------------------------------------
# Pre-flight: venvs
# ------------------------------------------------------------------
ensure_venv "$SCRIPT_DIR/OpenMLS_containerized"
ensure_venv "$SCRIPT_DIR/Signal_containerized"

echo "============================================================"
echo " Alternating benchmark suite (no IoT devices) - $DATE_TAG"
echo " OpenMLS: 16 x 2048 workers, no resource constraints, no external devices"
echo " Signal : 16 x 2048 workers, no resource constraints, no external devices"
echo "============================================================"
echo ""

# ------------------------------------------------------------------
# OpenMLS benchmark command
# ------------------------------------------------------------------
run_openmls() {
  local ITER="$1"
  local RUN_ID="openmls_run_${ITER}_${DATE_TAG}"
  local SCENARIO_SEED
  local SINGLETON_SELECTION_SEED
  local PYTHON_BIN

  SCENARIO_SEED="$(shuf -i 1-2147483647 -n 1)"
  SINGLETON_SELECTION_SEED="$(shuf -i 1-2147483647 -n 1)"
  PYTHON_BIN="$(python_for "$SCRIPT_DIR/OpenMLS_containerized")"

  echo ""
  echo "========== [OpenMLS iteration $ITER / 16] run-id: $RUN_ID =========="
  echo "  scenario_seed=$SCENARIO_SEED singleton_selection_seed=$SINGLETON_SELECTION_SEED"
  echo "  singleton_resource_envelope=unconstrained"
  echo "  external_devices=none"
  echo ""

  cd "$SCRIPT_DIR/OpenMLS_containerized"

  local -a _args=(
    --workers 2048
    --ds-port 3001
    --relay-port 4001
    --scenario-seed "$SCENARIO_SEED"
    --singleton-selection-seed "$SINGLETON_SELECTION_SEED"
    --output-dir benchmark_output
    --worker-layout-mode hybrid
    --singleton-min-count 12
    --singleton-fraction 0.0625
    --singleton-selection-strategy evenly-spaced
#    --singleton-cpus 0.25
#    --singleton-memory 256m
#    --singleton-memory-swap 256m
    --resource-monitor-interval-ms 250
    --packed-clients-per-container 48
    --packed-worker-internal-parallelism 16
    --bridge-count 4
    --build-images
#    $BUILD_EXTERNAL_FLAG
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
    --runner-http-request-timeout-ms 120000
    --worker-http-pool-max-idle-per-host 64
    --worker-http-connect-timeout-ms 5000
    --worker-http-request-timeout-ms 45000
    --worker-outbound-http-permits 32
    --compose-parallel-limit 48
    --startup-batch-size 64
    --startup-batch-sleep-seconds 0.5
    --post-startup-settle-seconds 10
    --health-timeout-seconds 240
    --health-poll-seconds 0.5
    --worker-health-timeout-seconds 600
    --worker-health-poll-ms 250
    --compose-down-timeout-seconds 2
    --teardown-batch-size 64
    --teardown-batch-sleep-seconds 0.1
    --min-size 2
    --max-size 2048
    --step-size '[1,32]'
    --roundtrips 2
    --update-rounds 8
    --app-rounds 8
    --max-update-samples-per-plateau 8
    --max-app-samples-per-payload 8
    --payload-sizes '[16,4096]'
#    --devices-file devices.yaml
#    --enable-external-devices
#    --external-device luckfox-pico-plus-01
#    --external-device raspberry-pi-01
#    --external-coverage-lane
#    --wipe-device-run-dirs
    --run-id "$RUN_ID"
  )

  OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  "$PYTHON_BIN" scripts/run_compose_benchmark.py "${_args[@]}"

  cd "$SCRIPT_DIR"

  echo ""
  echo "-------- OpenMLS iteration $ITER done. Cleaning up. --------"
  cleanup_docker
}

# ------------------------------------------------------------------
# Signal benchmark command
# ------------------------------------------------------------------
run_signal() {
  local ITER="$1"
  local RUN_ID="signal_run_${ITER}_${DATE_TAG}"
  local SINGLETON_SELECTION_SEED
  local PYTHON_BIN

  SINGLETON_SELECTION_SEED="$(shuf -i 1-2147483647 -n 1)"
  PYTHON_BIN="$(python_for "$SCRIPT_DIR/Signal_containerized")"

  echo ""
  echo "========== [Signal iteration $ITER / 16] run-id: $RUN_ID =========="
  echo "  singleton_selection_seed=$SINGLETON_SELECTION_SEED"
  echo "  singleton_resource_envelope=unconstrained"
  echo "  external_devices=none"
  echo ""

  cd "$SCRIPT_DIR/Signal_containerized"

  local -a _args=(
    --workers 2048
    --kr-port 3001
    --relay-port 4001
    --singleton-selection-seed "$SINGLETON_SELECTION_SEED"
    --output-dir benchmark_output
    --worker-layout-mode hybrid
    --singleton-min-count 16
    --singleton-fraction 0.125
    --singleton-selection-strategy evenly-spaced
#    --singleton-cpus 0.5
#    --singleton-memory 256m
#    --singleton-memory-swap 256m
#    --singleton-pids-limit 256
    --resource-monitor-interval-ms 250
    --packed-clients-per-container 16
    --packed-worker-internal-parallelism 16
    --bridge-count 4
    --build-images
#    $BUILD_EXTERNAL_FLAG
    --force-cleanup-signal-ports
    --runner-in-docker
    --fanout-adaptive
    --max-fanout-parallelism 64
    --min-fanout-parallelism 8
    --fanout-error-rate-threshold 0.02
    --fanout-p95-threshold-ms 8000
    --http-pool-max-idle-per-host 64
    --runner-http-connect-timeout-ms 5000
    --runner-http-request-timeout-ms 120000
    --worker-http-pool-max-idle-per-host 64
    --worker-http-connect-timeout-ms 5000
    --worker-http-request-timeout-ms 45000
    --worker-outbound-http-permits 32
    --compose-parallel-limit 48
    --startup-batch-size 64
    --startup-batch-sleep-seconds 0.5
    --post-startup-settle-seconds 10
    --health-timeout-seconds 240
    --health-poll-seconds 0.5
    --worker-health-timeout-seconds 600
    --worker-health-poll-ms 250
    --compose-down-timeout-seconds 2
    --teardown-batch-size 64
    --teardown-batch-sleep-seconds 0.1
    --min-size 2
    --max-size 2048
    --step-size '[1,32]'
    --roundtrips 2
    --app-rounds 8
    --max-app-samples-per-payload 8
    --payload-sizes '[16,4096]'
#    --devices-file devices.yaml
#    --enable-external-devices
#    --external-device luckfox-pico-plus-01
#    --external-device raspberry-pi-01
#    --wipe-device-run-dirs
    --run-id "$RUN_ID"
  )

  SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  "$PYTHON_BIN" scripts/run_compose_benchmark.py "${_args[@]}"

  cd "$SCRIPT_DIR"

  echo ""
  echo "-------- Signal iteration $ITER done. Cleaning up. --------"
  cleanup_docker
}

# ==================================================================
# Main loop: 16 iterations alternating OpenMLS / Signal
# ==================================================================

cleanup_docker

for I in $(seq 1 16); do
  run_openmls "$I"
  run_signal "$I"
done

echo ""
echo "============================================================"
echo " All 32 runs complete ($DATE_TAG)"
echo "============================================================"
