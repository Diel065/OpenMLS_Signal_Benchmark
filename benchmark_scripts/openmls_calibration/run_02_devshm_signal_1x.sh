#!/usr/bin/env bash
# Single Signal benchmark run, writing to /dev/shm (39 GB free, volatile VM disk).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

printf '\n===== Signal run (dev/shm): %s =====\n' "$(date -Is)"
OUTPUT_ROOT=/dev/shm PROTOCOL=signal "$SCRIPT_DIR/run_02_tmp_exact_two_openmls_signal.sh"
