#!/usr/bin/env bash
# RAM-only constrained OpenMLS then Signal sweep for intermediate failure points.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

OUTPUT_ROOT="${OUTPUT_ROOT:-/dev/shm}" \
PROTOCOL=both \
SWEEP=ram \
N="${N:-1}" \
BUILD_IMAGES="${BUILD_IMAGES:-1}" \
WORKERS=875 \
MAX_GROUP_SIZE=875 \
STEP_SIZE=16 \
PLATEAU_ORDER=ascending \
PROFILED_SINGLETON_COUNT=4 \
SINGLETON_MIN_COUNT=4 \
SIGNAL_WORKERS=875 \
SIGNAL_MAX_CONVERSATION_SIZE=875 \
SIGNAL_STEP_SIZE=16 \
OPENMLS_RAM_SWEEP_VALUES="750k,2m,3m,4m" \
SIGNAL_RAM_SWEEP_VALUES="750k,2m,3m,4m" \
PAYLOAD_SIZES="${PAYLOAD_SIZES:-512}" \
"$SCRIPT_DIR/run_01_constrained_container_sweeps.sh"
