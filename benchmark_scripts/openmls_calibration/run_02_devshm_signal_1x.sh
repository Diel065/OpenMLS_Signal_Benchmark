#!/usr/bin/env bash
# Single Signal baseline run, writing volatile results to /dev/shm.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

printf '\n===== Signal run (dev/shm): %s =====\n' "$(date -Is)"
OUTPUT_ROOT=/dev/shm \
PROTOCOL=signal \
WORKERS="${WORKERS:-1024}" \
MAX_SIZE="${MAX_SIZE:-1024}" \
STEP_SIZE="${STEP_SIZE:-48}" \
STEP_SIZE_SWITCH_AT="${STEP_SIZE_SWITCH_AT:-}" \
STEP_SIZE_AFTER_SWITCH="${STEP_SIZE_AFTER_SWITCH:-}" \
PROFILED_SINGLETON_COUNT="${PROFILED_SINGLETON_COUNT:-20}" \
SINGLETON_MIN_COUNT="${SINGLETON_MIN_COUNT:-20}" \
SINGLETON_SELECTION_STRATEGY="${SINGLETON_SELECTION_STRATEGY:-evenly-spaced}" \
APP_ROUNDS="${APP_ROUNDS:-3}" \
MAX_APP_SAMPLES_PER_PAYLOAD="${MAX_APP_SAMPLES_PER_PAYLOAD:-3}" \
PAYLOAD_SIZES="${PAYLOAD_SIZES:-512}" \
BUILD_IMAGES="${BUILD_IMAGES:-1}" \
"$SCRIPT_DIR/run_02_tmp_exact_two_openmls_signal.sh"
