#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ "${1:-}" == "--teardown" ]]; then
  "$DIR/setup_pi_benchmark_tunnel.sh" --teardown || true
  "$DIR/setup_luckfox_benchmark_bridge.sh" --teardown || true
  exit 0
fi

"$DIR/setup_pi_benchmark_tunnel.sh"
"$DIR/setup_luckfox_benchmark_bridge.sh"
