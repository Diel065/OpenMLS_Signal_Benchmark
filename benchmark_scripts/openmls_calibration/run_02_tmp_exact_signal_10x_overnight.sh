#!/usr/bin/env bash
# Temporary overnight wrapper: run the run_02 Signal settings exactly two times.
# Reduced from 10 to 2 runs due to ~8 GB disk per run at 1024 workers.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATUS=0

for ((RUN=1; RUN<=2; RUN++)); do
  printf '\n===== Signal run %s/2: %s =====\n' "$RUN" "$(date -Is)"
  if ! PROTOCOL=signal "$SCRIPT_DIR/run_02_tmp_exact_two_openmls_signal.sh"; then
    STATUS=1
    printf '===== Signal run %s/2 failed: %s =====\n' "$RUN" "$(date -Is)" >&2
  fi
done

exit "$STATUS"
