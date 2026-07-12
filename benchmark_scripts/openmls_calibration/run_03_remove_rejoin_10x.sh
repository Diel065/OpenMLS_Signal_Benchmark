#!/usr/bin/env bash
# Run the remove_rejoin benchmark ten times with different seeds.
# Acquires clean RemoveCommit + ProcessWelcome scaling data at every plateau.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATUS=0

for ((RUN=1; RUN<=10; RUN++)); do
  printf '\n===== remove_rejoin run %s/10: %s =====\n' "$RUN" "$(date -Is)"
  if ! "$SCRIPT_DIR/run_03_remove_rejoin_one.sh"; then
    STATUS=1
    printf '===== remove_rejoin run %s/10 failed: %s =====\n' "$RUN" "$(date -Is)" >&2
  fi
done

exit "$STATUS"
