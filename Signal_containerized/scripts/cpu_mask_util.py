from __future__ import annotations

from typing import List, Set


def cpu_list_to_docker_cpuset(cpus: List[int]) -> str:
    if not cpus:
        return ""
    values = sorted(set(cpus))
    ranges = []
    start = end = values[0]
    for cpu in values[1:]:
        if cpu == end + 1:
            end = cpu
            continue
        ranges.append(f"{start}" if start == end else f"{start}-{end}")
        start = end = cpu
    ranges.append(f"{start}" if start == end else f"{start}-{end}")
    return ",".join(ranges)


def docker_cpuset_to_cpu_list(cpuset_str: str) -> List[int]:
    cpus: Set[int] = set()
    for part in (cpuset_str or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, high = [int(v) for v in part.split("-", 1)]
            cpus.update(range(low, high + 1))
        else:
            cpus.add(int(part))
    return sorted(cpus)
