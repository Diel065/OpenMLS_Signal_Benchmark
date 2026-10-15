#!/usr/bin/env bash
set -euo pipefail


SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
    echo "Usage: $0 <target-triple>"
    echo ""
    echo "Available targets for external devices:"
    echo "  armv7-unknown-linux-musleabihf   # LuckFox Pico Plus (ARM Cortex-A7)"
    echo "  aarch64-unknown-linux-gnu        # Raspberry Pi 64-bit"
    echo "  aarch64-unknown-linux-musl       # Raspberry Pi 64-bit (static)"
    exit 1
fi

echo "[build] Building worker for target: $TARGET"
echo "[build] Profile: minsize (optimized for size, stripped)"

rustup target add "$TARGET" 2>/dev/null || true

cargo build \
    --profile minsize \
    --target "$TARGET" \
    --bin worker

BINARY="target/$TARGET/minsize/worker"
if [ -f "$BINARY" ]; then
    echo "[build] Success: $BINARY"
    echo "[build] Size: $(ls -lh "$BINARY" | awk '{print $5}')"
    file "$BINARY"
else
    echo "[build] FAILED: binary not found at $BINARY"
    exit 1
fi
