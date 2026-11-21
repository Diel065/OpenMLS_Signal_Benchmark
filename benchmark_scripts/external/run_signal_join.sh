#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ "${CRYPTO_PROFILE:-classical}" != "classical" ] || [ "${SIGNAL_GROUP_MODEL:-pairwise}" != "pairwise" ]; then
  echo "ERROR: external Signal join requires classical/pairwise" >&2
  exit 2
fi
export EXTERNAL_SESSION_JOIN=1
export SIGNAL_GROUP_MODEL=pairwise
export APP_ROUNDS=0 MAX_APP_SAMPLES_PER_PAYLOAD=0 MIN_EXTERNAL_SAMPLES_PER_OPERATION=0
export PLATEAU_ORDER=ascending ROUNDTRIPS=1
export BUILD_IMAGES="${BUILD_IMAGES:-0}" BUILD_EXTERNAL_BINARIES=0
export SCENARIO="${SCENARIO:-external-device-signal-measured-join}"
exec bash "$SCRIPT_DIR/run_signal.sh"
