#!/usr/bin/env bash
set -euo pipefail

LAPTOP_TS_IP="${LAPTOP_TS_IP:-100.71.112.120}"
VM_IP="${VM_IP:-192.168.11.127}"
LUCKFOX_SERIAL="${LUCKFOX_SERIAL:-242d5fe430c7c951}"
LUCKFOX_DEVICE_IP="${LUCKFOX_DEVICE_IP:-172.32.0.93}"
LUCKFOX_HOST_IP="${LUCKFOX_HOST_IP:-172.32.0.98}"
LUCKFOX_PREFIX="${LUCKFOX_PREFIX:-16}"
LUCKFOX_DEVICE_IFACE="${LUCKFOX_DEVICE_IFACE:-usb0}"
LUCKFOX_IFACE="${LUCKFOX_IFACE:-}"
ADB_PROXY_PORT="${ADB_PROXY_PORT:-15037}"
LUCKFOX_WORKER_PROXY_PORT="${LUCKFOX_WORKER_PROXY_PORT:-18081}"
PID_DIR="${PID_DIR:-/tmp/openmls_signal_external_bridges}"
PID_FILE="$PID_DIR/luckfox.pid"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }; }

iface() {
  if [[ -n "$LUCKFOX_IFACE" ]]; then
    printf '%s\n' "$LUCKFOX_IFACE"
    return
  fi
  ip -o -4 addr show | awk -v ip="$LUCKFOX_HOST_IP" '$0 ~ ip { print $2; exit }'
}

stop() {
  if [[ -f "$PID_FILE" ]]; then
    while read -r pid; do
      [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
  fi
}

proxy() {
  local bind_ip="$1" listen_port="$2" target_host="$3" target_port="$4"
  socat "TCP-LISTEN:${listen_port},bind=${bind_ip},reuseaddr,fork" "TCP:${target_host}:${target_port}" &
  local pid=$!
  sleep 0.1
  kill -0 "$pid" 2>/dev/null || { echo "proxy failed: ${bind_ip}:${listen_port}" >&2; exit 1; }
  echo "$pid" >> "$PID_FILE"
}

if [[ "${1:-}" == "--teardown" ]]; then
  stop
  exit 0
fi

need adb
need ip
need ping
need socat

adb start-server >/dev/null
adb devices | awk -v serial="$LUCKFOX_SERIAL" 'NR > 1 && $1 == serial && $2 == "device" { found = 1 } END { exit !found }'
adb -s "$LUCKFOX_SERIAL" shell "ip link set $LUCKFOX_DEVICE_IFACE up; ip addr show dev $LUCKFOX_DEVICE_IFACE | grep -q '$LUCKFOX_DEVICE_IP/' || ip addr add $LUCKFOX_DEVICE_IP/$LUCKFOX_PREFIX dev $LUCKFOX_DEVICE_IFACE"
HOST_IFACE="$(iface)"
[[ -n "$HOST_IFACE" ]] || { echo "set LUCKFOX_IFACE" >&2; exit 1; }
sudo ip link set "$HOST_IFACE" up
ip -o -4 addr show dev "$HOST_IFACE" | grep -q " $LUCKFOX_HOST_IP/" || sudo ip addr add "$LUCKFOX_HOST_IP/$LUCKFOX_PREFIX" dev "$HOST_IFACE"
ping -c 1 -W 2 "$LUCKFOX_DEVICE_IP" >/dev/null

mkdir -p "$PID_DIR"
stop
: > "$PID_FILE"

proxy "$LAPTOP_TS_IP" "$ADB_PROXY_PORT" 127.0.0.1 5037
proxy "$LAPTOP_TS_IP" "$LUCKFOX_WORKER_PROXY_PORT" "$LUCKFOX_DEVICE_IP" 8080
proxy "$LUCKFOX_HOST_IP" 3000 "$VM_IP" 3000
proxy "$LUCKFOX_HOST_IP" 4000 "$VM_IP" 4000

echo "luckfox adb: ${LAPTOP_TS_IP}:${ADB_PROXY_PORT} -> 127.0.0.1:5037"
echo "luckfox worker: http://${LAPTOP_TS_IP}:${LUCKFOX_WORKER_PROXY_PORT} -> http://${LUCKFOX_DEVICE_IP}:8080"
echo "luckfox services: ${LUCKFOX_HOST_IP}:3000,4000 -> ${VM_IP}:3000,4000"
