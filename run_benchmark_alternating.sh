#!/usr/bin/env bash
#
# Alternating benchmark runner
# -----------------------------------------
# Runs 10 iterations, alternating OpenMLS and Signal.
# Each benchmark waits for the previous one to finish and then tears down
# containers before starting the next benchmark.
#
# OpenMLS mirrors the current 1250-worker constrained run profile used by
# run_benchmark_openmls.sh and the recent OpenMLS benchmark_output runs.
# Signal uses the largest current successful Signal resource-schema profile
# found under Signal_containerized/benchmark_output, with both external devices.
#
# VM runs: start setup_external_device_benchmark_bridges.sh on the laptop first.
# The devices.yaml host_ip/worker_url_candidates values are expected to point at
# the laptop-side bridges that forward service ports to the VM.
#
# Usage:
#   chmod +x run_benchmark_alternating.sh
#   sudo bash run_benchmark_alternating.sh
#
# Run from the repository root (parent of *_containerized/).
# Requires Docker, the project Python environments with pyyaml, configured
# external-device access, and prebuilt external worker binaries.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATE_TAG="$(date +%Y%m%d_%H%M%S)"

python_for() {
  local stack_dir="$1"
  if [ -x "$stack_dir/.venv/bin/python" ]; then
    printf '%s\n' "$stack_dir/.venv/bin/python"
  else
    printf '%s\n' "python3"
  fi
}

require_enabled_devices() {
  local stack_dir="$1"
  shift
  local py
  py="$(python_for "$stack_dir")"

  "$py" - "$stack_dir/devices.yaml" "$@" <<'PY'
import sys
from pathlib import Path

try:
    import yaml
except ImportError as exc:
    raise SystemExit("PyYAML is required for device preflight checks") from exc

path = Path(sys.argv[1])
expected = sys.argv[2:]
raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
devices = {str(d.get("id")): d for d in raw.get("devices", [])}
missing = [dev for dev in expected if dev not in devices]
disabled = [dev for dev in expected if dev in devices and not bool(devices[dev].get("enabled", True))]
if missing or disabled:
    parts = []
    if missing:
        parts.append("missing: " + ", ".join(missing))
    if disabled:
        parts.append("disabled: " + ", ".join(disabled))
    print(f"Device preflight failed for {path}: " + "; ".join(parts), file=sys.stderr)
    raise SystemExit(1)
PY
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

echo "============================================================"
echo " Alternating benchmark suite - $DATE_TAG"
echo " OpenMLS: 10 x 1250 workers, Pico + Raspberry Pi, 0.25 CPU / 256m singletons"
echo " Signal : 10 x 600 workers, Pico + Raspberry Pi, 0.5 CPU / 256m / pids=256 singletons"
echo "============================================================"
echo ""
echo "VM mode reminder: keep setup_external_device_benchmark_bridges.sh running on the laptop."
echo ""

require_enabled_devices "$SCRIPT_DIR/OpenMLS_containerized" luckfox-pico-plus-01 raspberry-pi-01
require_enabled_devices "$SCRIPT_DIR/Signal_containerized" luckfox-pico-plus-01 raspberry-pi-01

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
  echo "========== [OpenMLS iteration $ITER / 10] run-id: $RUN_ID =========="
  echo "  scenario_seed=$SCENARIO_SEED singleton_selection_seed=$SINGLETON_SELECTION_SEED"
  echo "  singleton_resource_envelope=cpus=0.25,memory=256m,memory_swap=256m"
  echo "  external_devices=luckfox-pico-plus-01,raspberry-pi-01"
  echo ""

  cd "$SCRIPT_DIR/OpenMLS_containerized"

  OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  "$PYTHON_BIN" scripts/run_compose_benchmark.py \
    --workers 1250 \
    --scenario-seed "$SCENARIO_SEED" \
    --singleton-selection-seed "$SINGLETON_SELECTION_SEED" \
    --output-dir benchmark_output \
    --worker-layout-mode hybrid \
    --singleton-min-count 12 \
    --singleton-fraction 0.0625 \
    --singleton-selection-strategy evenly-spaced \
    --singleton-cpus 0.25 \
    --singleton-memory 256m \
    --singleton-memory-swap 256m \
    --resource-monitor-interval-ms 250 \
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
    --max-size 1250 \
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
  echo "========== [Signal iteration $ITER / 10] run-id: $RUN_ID =========="
  echo "  singleton_selection_seed=$SINGLETON_SELECTION_SEED"
  echo "  singleton_resource_envelope=cpus=0.5,memory=256m,memory_swap=256m,pids=256"
  echo "  external_devices=luckfox-pico-plus-01,raspberry-pi-01"
  echo ""

  cd "$SCRIPT_DIR/Signal_containerized"

  SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT=512 \
  "$PYTHON_BIN" scripts/run_compose_benchmark.py \
    --workers 600 \
    --singleton-selection-seed "$SINGLETON_SELECTION_SEED" \
    --output-dir benchmark_output \
    --worker-layout-mode hybrid \
    --singleton-min-count 16 \
    --singleton-fraction 0.125 \
    --singleton-selection-strategy evenly-spaced \
    --singleton-cpus 0.5 \
    --singleton-memory 256m \
    --singleton-memory-swap 256m \
    --singleton-pids-limit 256 \
    --resource-monitor-interval-ms 250 \
    --packed-clients-per-container 16 \
    --packed-worker-internal-parallelism 16 \
    --bridge-count 4 \
    --build-images \
    --force-cleanup-signal-ports \
    --runner-in-docker \
    --fanout-adaptive \
    --max-fanout-parallelism 64 \
    --min-fanout-parallelism 8 \
    --fanout-error-rate-threshold 0.02 \
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
    --max-size 600 \
    --step-size 299 \
    --roundtrips 1 \
    --app-rounds 1 \
    --max-app-samples-per-payload 1 \
    --payload-sizes 32 \
    --devices-file devices.yaml \
    --enable-external-devices \
    --external-device luckfox-pico-plus-01 \
    --external-device raspberry-pi-01 \
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

cleanup_docker

for I in $(seq 1 10); do
  run_openmls "$I"
  run_signal "$I"
done

echo ""
echo "============================================================"
echo " All 20 runs complete ($DATE_TAG)"
echo "============================================================"
