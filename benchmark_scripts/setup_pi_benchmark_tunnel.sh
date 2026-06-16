#!/usr/bin/env bash
set -euo pipefail

LAPTOP_TS_IP="${LAPTOP_TS_IP:-100.71.112.120}"
VM_IP="${VM_IP:-192.168.11.127}"
PI_HOST="${PI_HOST:-192.168.178.33}"
PI_USER="${PI_USER:-diel}"
PI_SSH_PROXY_PORT="${PI_SSH_PROXY_PORT:-10022}"
PI_WORKER_PROXY_PORT="${PI_WORKER_PROXY_PORT:-18080}"
LAPTOP_LAN_IP="${LAPTOP_LAN_IP:-}"
PID_DIR="${PID_DIR:-/tmp/openmls_signal_external_bridges}"
PID_FILE="$PID_DIR/raspi.pid"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }; }

lan_ip() {
  if [[ -n "$LAPTOP_LAN_IP" ]]; then
    printf '%s\n' "$LAPTOP_LAN_IP"
  else
    ip -o -4 route get "$PI_HOST" | sed -n 's/.* src \([0-9.]*\).*/\1/p' | sed -n '1p'
  fi
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

need ip
need ping
need socat
need ssh

LAN_IP="$(lan_ip)"
[[ -n "$LAN_IP" ]] || { echo "could not determine laptop LAN IP" >&2; exit 1; }
ping -c 1 -W 2 "$PI_HOST" >/dev/null
mkdir -p "$PID_DIR"
stop
: > "$PID_FILE"

proxy "$LAPTOP_TS_IP" "$PI_SSH_PROXY_PORT" "$PI_HOST" 22
proxy "$LAPTOP_TS_IP" "$PI_WORKER_PROXY_PORT" "$PI_HOST" 8080
proxy "$LAN_IP" 3000 "$VM_IP" 3000
proxy "$LAN_IP" 4000 "$VM_IP" 4000

ssh -o BatchMode=yes -o ConnectTimeout=5 -p "$PI_SSH_PROXY_PORT" "$PI_USER@$LAPTOP_TS_IP" true >/dev/null 2>&1 || true

echo "raspi ssh: ${LAPTOP_TS_IP}:${PI_SSH_PROXY_PORT} -> ${PI_HOST}:22"
echo "raspi worker: http://${LAPTOP_TS_IP}:${PI_WORKER_PROXY_PORT} -> http://${PI_HOST}:8080"
echo "raspi services: ${LAN_IP}:3000,4000 -> ${VM_IP}:3000,4000"
