#!/usr/bin/env bash
# Signal constrained RAM/CPU sweeps, mirroring run_02_devshm_signal_1x.sh settings.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

printf '\n===== Signal constrained run (dev/shm): %s =====\n' "$(date -Is)"
OUTPUT_ROOT=/dev/shm \
PROTOCOL=signal \
SWEEP="${SWEEP:-both}" \
N="${N:-1}" \
SIGNAL_WORKERS="${WORKERS:-1024}" \
SIGNAL_MAX_CONVERSATION_SIZE="${MAX_SIZE:-1024}" \
SIGNAL_STEP_SIZE="${STEP_SIZE:-48}" \
STEP_SIZE_SWITCH_AT="${STEP_SIZE_SWITCH_AT:-}" \
STEP_SIZE_AFTER_SWITCH="${STEP_SIZE_AFTER_SWITCH:-}" \
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-20}" \
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-20}" \
SINGLETON_SELECTION_STRATEGY="${SINGLETON_SELECTION_STRATEGY:-evenly-spaced}" \
SIGNAL_APP_ROUNDS="${APP_ROUNDS:-3}" \
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-3}" \
PAYLOAD_SIZES="${PAYLOAD_SIZES:-512}" \
BUILD_IMAGES="${BUILD_IMAGES:-1}" \
"$SCRIPT_DIR/run_01_constrained_container_sweeps.sh"
