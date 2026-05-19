#!/usr/bin/env bash
#
# Quick OpenMLS external-device prep after laptop/device reboot.
#
# Usage:
#   ./prepare_openmls_external_devices.sh
#
# Useful overrides:
#   LUCKFOX_IFACE=enx... ./prepare_openmls_external_devices.sh
#   BUILD_EXTERNAL_WORKERS=0 ./prepare_openmls_external_devices.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OPENMLS_DIR="$SCRIPT_DIR/OpenMLS_containerized"

# Mirrors OpenMLS_containerized/devices.yaml.
LUCKFOX_SERIAL="${LUCKFOX_SERIAL:-242d5fe430c7c951}"
LUCKFOX_DEVICE_IP="${LUCKFOX_DEVICE_IP:-172.32.0.93}"
LUCKFOX_HOST_IP="${LUCKFOX_HOST_IP:-172.32.0.98}"
LUCKFOX_PREFIX="${LUCKFOX_PREFIX:-16}"
LUCKFOX_DEVICE_IFACE="${LUCKFOX_DEVICE_IFACE:-usb0}"
LUCKFOX_IFACE="${LUCKFOX_IFACE:-}"

RASPI_HOST="${RASPI_HOST:-192.168.178.33}"
RASPI_USER="${RASPI_USER:-diel}"
RASPI_PASS="${RASPI_PASS:-diel}"

BUILD_EXTERNAL_WORKERS="${BUILD_EXTERNAL_WORKERS:-missing}"

log() {
  printf '\n===== %s =====\n' "$*"
}

warn() {
  printf 'WARN: %s\n' "$*" >&2
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

detect_luckfox_iface() {
  if [[ -n "$LUCKFOX_IFACE" ]]; then
    printf '%s\n' "$LUCKFOX_IFACE"
    return 0
  fi

  local by_addr
  by_addr="$(ip -o -4 addr show | awk -v ip="$LUCKFOX_HOST_IP" '$0 ~ ip { print $2; exit }')"
  if [[ -n "$by_addr" ]]; then
    printf '%s\n' "$by_addr"
    return 0
  fi

  local net iface target
  for net in /sys/class/net/*; do
    iface="${net##*/}"
    case "$iface" in
      lo|docker*|br-*|veth*|virbr*|tailscale*|wl*|wlan*)
        continue
        ;;
    esac
    target="$(readlink -f "$net/device" 2>/dev/null || true)"
    if [[ "$target" == *"/usb"* ]]; then
      printf '%s\n' "$iface"
      return 0
    fi
  done

  return 1
}

ensure_worker_binary() {
  local target="$1"
  local binary="$OPENMLS_DIR/target/$target/minsize/worker"

  if [[ -x "$binary" && "$BUILD_EXTERNAL_WORKERS" != "1" ]]; then
    echo "worker binary present: $binary"
    return 0
  fi

  if [[ "$BUILD_EXTERNAL_WORKERS" == "0" ]]; then
    warn "worker binary missing or rebuild skipped: $binary"
    return 0
  fi

  log "Building OpenMLS worker for $target"
  (
    cd "$OPENMLS_DIR"
    rustup target add "$target"
    RUSTFLAGS='-C linker=rust-lld' cargo build --profile minsize --target "$target" --bin worker
  )
}

prepare_luckfox() {
  log "Luckfox Pico Plus over USB RNDIS"

  adb start-server >/dev/null
  adb devices -l

  if ! adb devices | awk '{print $1}' | grep -Fxq "$LUCKFOX_SERIAL"; then
    echo "Luckfox ADB serial not found: $LUCKFOX_SERIAL" >&2
    echo "Check USB power/data cable, then run: adb devices -l" >&2
    exit 1
  fi

  echo "[luckfox] device info"
  adb -s "$LUCKFOX_SERIAL" shell 'hostname; whoami; uname -m; ip addr show usb0 2>/dev/null || ip addr show'

  echo "[luckfox] ensuring device-side $LUCKFOX_DEVICE_IFACE = $LUCKFOX_DEVICE_IP/$LUCKFOX_PREFIX"
  adb -s "$LUCKFOX_SERIAL" shell "
    set -e
    ip link set $LUCKFOX_DEVICE_IFACE up
    ip addr show dev $LUCKFOX_DEVICE_IFACE | grep -q '$LUCKFOX_DEVICE_IP/' || \
      ip addr add $LUCKFOX_DEVICE_IP/$LUCKFOX_PREFIX dev $LUCKFOX_DEVICE_IFACE
    ip addr show dev $LUCKFOX_DEVICE_IFACE
  "

  local host_iface
  if ! host_iface="$(detect_luckfox_iface)"; then
    echo "Could not auto-detect the host USB RNDIS interface." >&2
    echo "Run 'ip -br addr', find the enx.../usb interface, then retry with:" >&2
    echo "  LUCKFOX_IFACE=enx... ./prepare_openmls_external_devices.sh" >&2
    exit 1
  fi

  echo "[luckfox] ensuring host-side $host_iface = $LUCKFOX_HOST_IP/$LUCKFOX_PREFIX"
  sudo ip link set "$host_iface" up
  if ! ip -o -4 addr show dev "$host_iface" | grep -q " $LUCKFOX_HOST_IP/"; then
    sudo ip addr add "$LUCKFOX_HOST_IP/$LUCKFOX_PREFIX" dev "$host_iface"
  fi
  ip -br addr show "$host_iface"

  if ping -c 1 -W 2 "$LUCKFOX_DEVICE_IP" >/dev/null 2>&1; then
    echo "[luckfox] ping ok: $LUCKFOX_DEVICE_IP"
  else
    warn "Luckfox ping failed. ADB may still work, but benchmark HTTP reachability will likely fail."
  fi

  echo "[luckfox] stopping old worker and preparing directories"
  adb -s "$LUCKFOX_SERIAL" shell '
    killall worker 2>/dev/null || pkill worker 2>/dev/null || true
    mkdir -p /opt/openmls-benchmark /results/openmls /tmp/openmls-benchmark
  '
}

prepare_raspi() {
  log "Raspberry Pi 5 over SSH/Wi-Fi"

  if ping -c 1 -W 2 "$RASPI_HOST" >/dev/null 2>&1; then
    echo "[raspi] ping ok: $RASPI_HOST"
  else
    warn "Raspberry Pi ping failed. It may be powered off, on another IP, or blocking ICMP."
  fi

  local ssh_cmd=(
    ssh
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o LogLevel=ERROR
    -o BatchMode=no
    -o ConnectTimeout=10
    "$RASPI_USER@$RASPI_HOST"
  )

  if [[ -n "$RASPI_PASS" ]] && command -v sshpass >/dev/null 2>&1; then
    ssh_cmd=(sshpass -p "$RASPI_PASS" "${ssh_cmd[@]}")
  elif [[ -n "$RASPI_PASS" ]]; then
    warn "sshpass not found; SSH may prompt for the Raspberry Pi password."
  fi

  "${ssh_cmd[@]}" '
    set -e
    hostname
    whoami
    uname -m
    killall worker 2>/dev/null || pkill worker 2>/dev/null || true
    mkdir -p /home/diel/openmls-benchmark/bin \
             /home/diel/openmls-benchmark/results/openmls \
             /home/diel/openmls-benchmark/tmp
    if test -x /home/diel/openmls-benchmark/bin/worker; then
      echo "remote worker binary present"
    else
      echo "remote worker binary missing; benchmark runner will push the local aarch64 binary if present"
    fi
  '
}

main() {
  need_cmd adb
  need_cmd ip
  need_cmd ping
  need_cmd ssh

  log "Checking local external worker binaries"
  ensure_worker_binary armv7-unknown-linux-musleabihf
  ensure_worker_binary aarch64-unknown-linux-musl

  prepare_luckfox
  prepare_raspi

  log "Ready"
  echo "External-device prep finished."
  echo "Next benchmark entrypoint: ./run_benchmark_openmls.sh"
}

main "$@"
