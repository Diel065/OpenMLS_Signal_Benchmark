#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "[info] re-running with sudo"
  exec sudo bash "$0" "$@"
fi

echo "[info] Applying OpenMLS/Docker benchmark scaling parameters"
echo "[warning] This will restart containerd and docker."

# -------------------------------------------------------------------
# 1) Systemd limits for Docker/containerd
# -------------------------------------------------------------------

mkdir -p /etc/systemd/system/docker.service.d
cat >/etc/systemd/system/docker.service.d/override.conf <<'EOF'
[Service]
LimitNOFILE=4194304
LimitNPROC=infinity
TasksMax=infinity
LimitCORE=0
EOF

mkdir -p /etc/systemd/system/containerd.service.d
cat >/etc/systemd/system/containerd.service.d/override.conf <<'EOF'
[Service]
LimitNOFILE=4194304
LimitNPROC=infinity
TasksMax=infinity
LimitCORE=0
EOF

# -------------------------------------------------------------------
# 2) Login/session limits
#    Useful for shells, scripts, Python, docker compose client, etc.
#    Requires logout/login to fully apply to new sessions.
# -------------------------------------------------------------------

cat >/etc/security/limits.d/99-mls-benchmark.conf <<'EOF'
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
EOF

# -------------------------------------------------------------------
# 3) Kernel/sysctl tuning for many containers, veth devices, inotify,
#    file descriptors, and many short-lived HTTP connections.
# -------------------------------------------------------------------

cat >/etc/sysctl.d/99-mls-benchmark-scale.conf <<'EOF'
# -------------------------------------------------------------------
# OpenMLS benchmark / large Docker container scale tuning
# -------------------------------------------------------------------

# File descriptor ceilings.
fs.file-max = 16777216
fs.nr_open = 16777216

# Inotify: containerd can otherwise fail with:
# "failed to create inotify fd: too many open files"
fs.inotify.max_user_instances = 1048576
fs.inotify.max_user_watches = 1048576
fs.inotify.max_queued_events = 1048576

# Process/thread headroom.
kernel.pid_max = 4194304
kernel.threads-max = 1048576

# Memory map headroom.
vm.max_map_count = 1048576

# Docker bridge / connection tracking headroom.
net.netfilter.nf_conntrack_max = 1048576

# TCP/listen backlog.
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 250000

# More room for outgoing local HTTP connections.
net.ipv4.ip_local_port_range = 10000 65000
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15

# Neighbor/ARP table headroom for many Docker bridges and veth endpoints.
net.ipv4.neigh.default.gc_thresh1 = 8192
net.ipv4.neigh.default.gc_thresh2 = 32768
net.ipv4.neigh.default.gc_thresh3 = 65536
net.ipv4.neigh.default.gc_interval = 30
net.ipv4.neigh.default.gc_stale_time = 60
EOF

# nf_conntrack may not be loaded yet on some systems.
modprobe nf_conntrack 2>/dev/null || true

echo "[info] Reloading sysctl"
sysctl --system

echo "[info] Reloading systemd and restarting Docker services"
systemctl daemon-reload
systemctl restart containerd
systemctl restart docker

echo ""
echo "[info] Verification:"
echo "------------------------------------------------------------"
systemctl show docker -p LimitNOFILE -p LimitNPROC -p TasksMax -p LimitCORE
systemctl show containerd -p LimitNOFILE -p LimitNPROC -p TasksMax -p LimitCORE

echo ""
echo "[info] Runtime process limits:"
echo "------------------------------------------------------------"
if pidof dockerd >/dev/null 2>&1; then
  echo "[dockerd]"
  grep -i "open files" "/proc/$(pidof dockerd)/limits" || true
fi

if pidof containerd >/dev/null 2>&1; then
  echo "[containerd]"
  grep -i "open files" "/proc/$(pidof containerd)/limits" || true
fi

echo ""
echo "[info] Important sysctl values:"
echo "------------------------------------------------------------"
sysctl \
  fs.file-max \
  fs.nr_open \
  fs.inotify.max_user_instances \
  fs.inotify.max_user_watches \
  fs.inotify.max_queued_events \
  kernel.pid_max \
  kernel.threads-max \
  vm.max_map_count \
  net.core.somaxconn \
  net.core.netdev_max_backlog \
  net.ipv4.ip_local_port_range \
  net.ipv4.tcp_tw_reuse \
  net.ipv4.tcp_fin_timeout \
  net.netfilter.nf_conntrack_max \
  net.netfilter.nf_conntrack_count \
  net.ipv4.neigh.default.gc_thresh1 \
  net.ipv4.neigh.default.gc_thresh2 \
  net.ipv4.neigh.default.gc_thresh3 \
  2>/dev/null || true

echo ""
echo "[done] System tuning applied."
echo "[note] For shell ulimit changes, log out and back in, then check: ulimit -n"

systemctl show docker -p LimitNOFILE -p TasksMax
systemctl show containerd -p LimitNOFILE -p TasksMax
cat /proc/$(pidof dockerd)/limits | grep -i "open files"
cat /proc/$(pidof containerd)/limits | grep -i "open files"
ulimit -n
