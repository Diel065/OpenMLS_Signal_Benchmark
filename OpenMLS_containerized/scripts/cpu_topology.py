"""
CPU topology detection and load-sampling utilities.

Detects online CPUs, CPU topology (core, socket, NUMA node), and samples
per-CPU load from /proc/stat for least-loaded CPU selection.

Uses:
  - lscpu for topology
  - /proc/stat for per-CPU load sampling
  - /sys/devices/system/cpu/ for online status
"""

import os
import subprocess
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import json


@dataclass
class CpuInfo:
    """Information about a single CPU thread."""
    cpu_id: int
    core_id: Optional[int] = None
    socket_id: Optional[int] = None
    numa_node: Optional[int] = None
    online: bool = True
    busy_fraction: float = 0.0
    is_smt_sibling: bool = False


@dataclass
class CpuTopology:
    """Complete CPU topology snapshot."""
    cpus: Dict[int, CpuInfo] = field(default_factory=dict)
    online_cpu_count: int = 0
    total_cpu_count: int = 0
    smt_enabled: bool = False
    core_to_cpus: Dict[int, List[int]] = field(default_factory=dict)


@dataclass
class CpuStat:
    """Per-CPU /proc/stat snapshot."""
    user: int = 0
    nice: int = 0
    system: int = 0
    idle: int = 0
    iowait: int = 0
    irq: int = 0
    softirq: int = 0
    steal: int = 0
    guest: int = 0
    guest_nice: int = 0

    @property
    def total(self) -> int:
        return (self.user + self.nice + self.system + self.idle +
                self.iowait + self.irq + self.softirq + self.steal +
                self.guest + self.guest_nice)

    @property
    def busy(self) -> int:
        return self.total - self.idle - self.iowait


def detect_cpu_topology() -> CpuTopology:
    """Detect CPU topology using lscpu and /sys/devices/system/cpu/.

    Falls back gracefully if lscpu is not available, detecting only
    online CPUs from /sys/devices/system/cpu/cpu*/online.
    """
    topology = CpuTopology()

    lscpu_topology: Dict[int, Dict[str, Optional[int]]] = {}
    smt_cores: Dict[int, List[int]] = {}

    try:
        result = subprocess.run(
            ["lscpu", "-J", "-e=CPU,CORE,SOCKET,NODE,ONLINE"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if "cpus" in data:
                for entry in data["cpus"]:
                    cpu_id = int(entry.get("cpu", -1))
                    if cpu_id < 0:
                        continue
                    core_id_str = entry.get("core", "")
                    socket_id_str = entry.get("socket", "")
                    node_id_str = entry.get("node", "")
                    online_str = str(entry.get("online", "yes")).strip().lower()

                    core_id = int(core_id_str) if core_id_str not in ("", "-", None) else None
                    socket_id = int(socket_id_str) if socket_id_str not in ("", "-", None) else None
                    numa_node = int(node_id_str) if node_id_str not in ("", "-", None) else None
                    online = online_str != "no"

                    lscpu_topology[cpu_id] = {
                        "core_id": core_id,
                        "socket_id": socket_id,
                        "numa_node": numa_node,
                        "online": online,
                    }

                    if core_id is not None and online:
                        if core_id not in topology.core_to_cpus:
                            topology.core_to_cpus[core_id] = []
                        topology.core_to_cpus[core_id].append(cpu_id)
                        if core_id not in smt_cores:
                            smt_cores[core_id] = []
                        smt_cores[core_id].append(cpu_id)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception):
        pass

    smt_enabled = any(len(cpus) > 1 for cpus in smt_cores.values())

    sibling_map: Dict[int, List[int]] = {}
    if smt_enabled:
        for core_id, cpu_list in smt_cores.items():
            if len(cpu_list) > 1:
                for cpu in cpu_list:
                    sibling_map[cpu] = [c for c in cpu_list if c != cpu]

    for cpu_id in sorted(lscpu_topology.keys()):
        info = lscpu_topology[cpu_id]
        topology.cpus[cpu_id] = CpuInfo(
            cpu_id=cpu_id,
            core_id=info.get("core_id"),
            socket_id=info.get("socket_id"),
            numa_node=info.get("numa_node"),
            online=info.get("online", True),
            is_smt_sibling=(cpu_id in sibling_map),
        )
        topology.total_cpu_count += 1
        if info.get("online", True):
            topology.online_cpu_count += 1

    if not topology.cpus:
        online_cpus = _detect_online_cpus_from_sys()
        for cpu_id in online_cpus:
            topology.cpus[cpu_id] = CpuInfo(cpu_id=cpu_id, online=True)
            topology.online_cpu_count += 1
            topology.total_cpu_count += 1

    topology.smt_enabled = smt_enabled

    if not smt_enabled:
        for cpu_id in topology.cpus:
            if cpu_id not in topology.core_to_cpus:
                topology.core_to_cpus[cpu_id] = [cpu_id]

    return topology


def _detect_online_cpus_from_sys() -> List[int]:
    """Detect online CPUs from /sys/devices/system/cpu/cpu*/online."""
    cpus = []
    sys_cpu_path = "/sys/devices/system/cpu"
    if not os.path.exists(sys_cpu_path):
        return list(range(os.cpu_count() or 1))

    for entry in sorted(os.listdir(sys_cpu_path)):
        if entry.startswith("cpu") and entry[3:].isdigit():
            cpu_id = int(entry[3:])
            online_path = os.path.join(sys_cpu_path, entry, "online")
            if os.path.exists(online_path):
                try:
                    with open(online_path) as f:
                        if f.read().strip() == "1":
                            cpus.append(cpu_id)
                except (IOError, OSError):
                    pass
            else:
                cpus.append(cpu_id)
    return cpus


def get_online_cpu_list(topology: CpuTopology) -> List[int]:
    """Get a sorted list of online CPU IDs from topology."""
    return sorted(cpu_id for cpu_id, info in topology.cpus.items() if info.online)


def _parse_proc_stat_cpu_line(line: str) -> Tuple[int, CpuStat]:
    """Parse a single CPU line from /proc/stat.

    Returns (cpu_id, CpuStat). The 'cpu' total line has cpu_id=-1.
    """
    parts = line.strip().split()
    if not parts:
        return -1, CpuStat()

    label = parts[0]
    if label == "cpu":
        cpu_id = -1
    elif label.startswith("cpu") and label[3:].isdigit():
        cpu_id = int(label[3:])
    else:
        return -1, CpuStat()

    values = parts[1:11]
    stat = CpuStat()
    fields = ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice"]
    for i, field in enumerate(fields):
        if i < len(values):
            setattr(stat, field, int(values[i]))

    return cpu_id, stat


def sample_cpu_load(duration_seconds: float = 20.0) -> Dict[int, float]:
    """Sample per-CPU busy fraction over the given duration.

    Reads /proc/stat twice, waiting duration_seconds between reads,
    and computes the busy fraction per CPU.

    Returns a dict mapping cpu_id -> busy_fraction (0.0 to 1.0).
    """
    def read_proc_stat() -> Dict[int, CpuStat]:
        stats: Dict[int, CpuStat] = {}
        try:
            with open("/proc/stat") as f:
                for line in f:
                    if line.startswith("cpu"):
                        cpu_id, stat = _parse_proc_stat_cpu_line(line)
                        if cpu_id >= 0:
                            stats[cpu_id] = stat
        except (IOError, OSError):
            pass
        return stats

    first = read_proc_stat()
    if not first:
        return {}

    if duration_seconds > 0:
        time.sleep(duration_seconds)

    second = read_proc_stat()
    if not second:
        return {}

    loads: Dict[int, float] = {}
    for cpu_id in first:
        if cpu_id in second:
            prev = first[cpu_id]
            curr = second[cpu_id]
            total_diff = curr.total - prev.total
            busy_diff = curr.busy - prev.busy
            if total_diff > 0:
                loads[cpu_id] = min(1.0, max(0.0, busy_diff / total_diff))
            else:
                loads[cpu_id] = 0.0

    return loads


def select_least_loaded_cpus(
    topology: CpuTopology,
    required_count: int,
    load_samples: Dict[int, float],
    exclude_cpus: Optional[List[int]] = None,
    prefer_physical_cores: bool = True,
    reserve_smt_siblings: bool = False,
) -> List[int]:
    """Select the least-loaded CPUs for profiled worker assignment.

    If required_count is 0, returns an empty list (no CPUs to assign).

    Args:
        topology: Detected CPU topology.
        required_count: Number of CPUs to select.
        load_samples: Per-CPU busy fractions (0.0 to 1.0).
        exclude_cpus: CPUs to exclude from selection.
        prefer_physical_cores: If True, prefer CPUs that are NOT SMT siblings.
        reserve_smt_siblings: If True, also reserve the SMT siblings of selected CPUs.

    Returns:
        Sorted list of selected CPU IDs.

    Raises:
        ValueError: If insufficient CPUs are available.
    """
    if required_count <= 0:
        return []

    exclude_set = set(exclude_cpus or [])
    online_cpus = get_online_cpu_list(topology)

    candidates = [c for c in online_cpus if c not in exclude_set]

    if reserve_smt_siblings and topology.smt_enabled:
        sibling_map: Dict[int, List[int]] = {}
        for core_id, cpu_list in topology.core_to_cpus.items():
            if len(cpu_list) > 1:
                for cpu in cpu_list:
                    sibling_map[cpu] = [c for c in cpu_list if c != cpu]

        new_excludes: Set[int] = set()
        chosen: Set[int] = set()

    if len(candidates) < required_count:
        raise ValueError(
            f"Insufficient online CPUs: need {required_count}, "
            f"have {len(candidates)} after exclusions"
        )

    busy = {cpu: load_samples.get(cpu, 0.0) for cpu in candidates}

    preferred = []
    secondary = []
    for cpu in candidates:
        info = topology.cpus.get(cpu)
        if prefer_physical_cores and info and info.is_smt_sibling:
            secondary.append(cpu)
        else:
            preferred.append(cpu)

    if len(preferred) < required_count:
        combined = preferred + secondary
    else:
        combined = preferred

    combined.sort(key=lambda c: (busy[c], c))

    selected = combined[:required_count]

    if reserve_smt_siblings and topology.smt_enabled:
        reserved_set = set(selected)
        for cpu in selected:
            siblings = sibling_map.get(cpu, [])
            for sib in siblings:
                if sib not in exclude_set:
                    reserved_set.add(sib)
        selected = sorted(reserved_set)

    return sorted(selected)


def get_smt_siblings(topology: CpuTopology, cpu_list: List[int]) -> List[int]:
    """Get SMT sibling CPUs for the given CPU list.

    Returns CPUs that share a physical core with any CPU in the input list,
    excluding the input CPUs themselves.
    """
    if not topology.smt_enabled:
        return []

    siblings: Set[int] = set()
    for core_id, core_cpus in topology.core_to_cpus.items():
        if len(core_cpus) <= 1:
            continue
        if any(cpu in cpu_list for cpu in core_cpus):
            for cpu in core_cpus:
                if cpu not in cpu_list:
                    siblings.add(cpu)

    return sorted(siblings)


def topology_to_dict(topology: CpuTopology) -> Dict:
    """Convert a CpuTopology to a JSON-serializable dict."""
    cpus_list = []
    for cpu_id in sorted(topology.cpus.keys()):
        info = topology.cpus[cpu_id]
        cpus_list.append({
            "cpu_id": info.cpu_id,
            "core_id": info.core_id,
            "socket_id": info.socket_id,
            "numa_node": info.numa_node,
            "online": info.online,
            "is_smt_sibling": info.is_smt_sibling,
        })

    return {
        "online_cpu_count": topology.online_cpu_count,
        "total_cpu_count": topology.total_cpu_count,
        "smt_enabled": topology.smt_enabled,
        "cpus": cpus_list,
        "core_to_cpus": {
            str(k): v for k, v in sorted(topology.core_to_cpus.items())
        },
    }
