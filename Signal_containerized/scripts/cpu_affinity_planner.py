"""CPU affinity planner compatible with the OpenMLS sidecar schema."""

from __future__ import annotations

import json
import os
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class ProfiledAssignment:
    worker_id: str
    container_name: str
    logical_client_id: str
    assigned_cpus: List[int]
    assigned_mask_hex: str
    assigned_cpu_count: int
    rayon_num_threads: int
    experiment_kind: str
    resource_profile_id: str


@dataclass
class BackgroundAssignment:
    container_name: str
    container_role: str
    assigned_cpus: List[int]
    assigned_mask_hex: str


@dataclass
class AffinityPlan:
    run_id: str
    created_at: str
    hostname: str
    cpu_affinity_mode: str
    sample_seconds: float
    selection_policy: str
    online_cpus: List[int]
    online_cpu_mask_hex: str
    cpu_topology: Dict[str, Any]
    sampled_cpu_load: Dict[int, float]
    profiled_assignments: List[ProfiledAssignment]
    profiled_mask_hex: str
    reserved_mask_hex: str
    background_cpus: List[int]
    background_mask_hex: str
    smt_sibling_policy: str
    warnings: List[str] = field(default_factory=list)
    background_assignments: List[BackgroundAssignment] = field(default_factory=list)


def cpu_list_to_mask(cpu_list: List[int]) -> int:
    mask = 0
    for cpu in sorted(set(cpu_list)):
        if cpu < 0:
            raise ValueError(f"negative CPU id: {cpu}")
        mask |= 1 << cpu
    return mask


def mask_to_hex(mask: int) -> str:
    return hex(mask)


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


def get_online_cpu_list() -> List[int]:
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:
        return list(range(os.cpu_count() or 1))


def _read_proc_stat() -> Dict[int, tuple[int, int]]:
    stats: Dict[int, tuple[int, int]] = {}
    try:
        with open("/proc/stat", encoding="utf-8") as handle:
            for line in handle:
                parts = line.split()
                if not parts or not parts[0].startswith("cpu") or not parts[0][3:].isdigit():
                    continue
                cpu = int(parts[0][3:])
                nums = [int(v) for v in parts[1:11]]
                total = sum(nums)
                idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
                stats[cpu] = (total, total - idle)
    except OSError:
        pass
    return stats


def sample_cpu_load(duration_seconds: float) -> Dict[int, float]:
    first = _read_proc_stat()
    if duration_seconds > 0:
        time.sleep(duration_seconds)
    second = _read_proc_stat()
    loads: Dict[int, float] = {}
    for cpu, (total0, busy0) in first.items():
        if cpu not in second:
            continue
        total1, busy1 = second[cpu]
        total_delta = total1 - total0
        loads[cpu] = max(0.0, min(1.0, (busy1 - busy0) / total_delta)) if total_delta > 0 else 0.0
    return loads


def create_affinity_plan(
    run_id: str,
    profiled_worker_specs: List[Dict[str, str]],
    background_specs: List[Dict[str, str]],
    cpu_affinity_mode: str = "profiled-nor-background",
    sample_seconds: float = 20.0,
    reserve_smt_siblings: bool = False,
    profiled_cpu_counts: Optional[Dict[str, int]] = None,
) -> AffinityPlan:
    if cpu_affinity_mode == "none":
        return AffinityPlan(run_id, time.strftime("%Y-%m-%dT%H:%M:%S%z"), socket.gethostname(), "none", sample_seconds, "none", [], "0x0", {}, {}, [], "0x0", "0x0", [], "0x0", "no_reservation")

    online_cpus = get_online_cpu_list()
    profiled_cpu_counts = profiled_cpu_counts or {s["worker_id"]: 1 for s in profiled_worker_specs}
    needed = sum(profiled_cpu_counts.values())
    if needed > len(online_cpus):
        raise ValueError(f"Insufficient online CPUs: need {needed}, have {len(online_cpus)}")

    loads = sample_cpu_load(sample_seconds) if needed else {}
    selected = sorted(online_cpus, key=lambda cpu: (loads.get(cpu, 0.0), cpu))[:needed]

    profiled: List[ProfiledAssignment] = []
    index = 0
    for spec in profiled_worker_specs:
        count = profiled_cpu_counts.get(spec["worker_id"], 1)
        assigned = sorted(selected[index:index + count])
        index += count
        profiled.append(ProfiledAssignment(
            worker_id=spec["worker_id"],
            container_name=spec["container_name"],
            logical_client_id=spec["logical_client_id"],
            assigned_cpus=assigned,
            assigned_mask_hex=mask_to_hex(cpu_list_to_mask(assigned)),
            assigned_cpu_count=len(assigned),
            rayon_num_threads=int(spec.get("rayon_num_threads", count)),
            experiment_kind=spec.get("experiment_kind", ""),
            resource_profile_id=spec.get("resource_profile_id", ""),
        ))

    profiled_set: Set[int] = set()
    for assignment in profiled:
        profiled_set.update(assignment.assigned_cpus)
    background_cpus = [cpu for cpu in online_cpus if cpu not in profiled_set]
    bg_mask = mask_to_hex(cpu_list_to_mask(background_cpus))
    background = [BackgroundAssignment(s["container_name"], s["container_role"], background_cpus, bg_mask) for s in background_specs]

    return AffinityPlan(
        run_id=run_id,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        hostname=socket.gethostname(),
        cpu_affinity_mode=cpu_affinity_mode,
        sample_seconds=sample_seconds,
        selection_policy="least_loaded",
        online_cpus=online_cpus,
        online_cpu_mask_hex=mask_to_hex(cpu_list_to_mask(online_cpus)),
        cpu_topology={"online_cpu_count": len(online_cpus), "total_cpu_count": os.cpu_count() or len(online_cpus)},
        sampled_cpu_load=loads,
        profiled_assignments=profiled,
        profiled_mask_hex=mask_to_hex(cpu_list_to_mask(sorted(profiled_set))),
        reserved_mask_hex=mask_to_hex(cpu_list_to_mask(sorted(profiled_set))),
        background_cpus=background_cpus,
        background_mask_hex=bg_mask,
        smt_sibling_policy="reserve_siblings" if reserve_smt_siblings else "no_reservation",
        warnings=[] if background_cpus or not background_specs else ["Background mask is empty after reserving profiled CPUs"],
        background_assignments=background,
    )


def _assignment_to_dict(pa: ProfiledAssignment) -> Dict[str, Any]:
    return pa.__dict__.copy()


def _background_to_dict(ba: BackgroundAssignment) -> Dict[str, Any]:
    return ba.__dict__.copy()


def write_affinity_plan_json(plan: AffinityPlan, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "cpu_affinity_plan.json")
    data = {
        "run_id": plan.run_id,
        "created_at": plan.created_at,
        "hostname": plan.hostname,
        "cpu_affinity_mode": plan.cpu_affinity_mode,
        "sample_seconds": plan.sample_seconds,
        "selection_policy": plan.selection_policy,
        "online_cpus": plan.online_cpus,
        "online_cpu_mask_hex": plan.online_cpu_mask_hex,
        "cpu_topology": plan.cpu_topology,
        "sampled_cpu_load": {str(k): v for k, v in plan.sampled_cpu_load.items()},
        "profiled_assignments": [_assignment_to_dict(pa) for pa in plan.profiled_assignments],
        "profiled_mask_hex": plan.profiled_mask_hex,
        "reserved_mask_hex": plan.reserved_mask_hex,
        "background_cpus": plan.background_cpus,
        "background_mask_hex": plan.background_mask_hex,
        "smt_sibling_policy": plan.smt_sibling_policy,
        "warnings": plan.warnings,
        "background_assignments": [_background_to_dict(ba) for ba in plan.background_assignments],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
    return path


def get_background_cpuset(plan: AffinityPlan) -> str:
    return cpu_list_to_docker_cpuset(plan.background_cpus)


def get_profiled_cpuset(plan: AffinityPlan, worker_id: str) -> Optional[str]:
    for assignment in plan.profiled_assignments:
        if assignment.worker_id == worker_id:
            return cpu_list_to_docker_cpuset(assignment.assigned_cpus)
    return None


def validate_affinity_plan(plan: AffinityPlan) -> List[str]:
    if plan.cpu_affinity_mode == "none":
        return []
    errors: List[str] = []
    assigned: Set[int] = set()
    for assignment in plan.profiled_assignments:
        if assignment.assigned_cpu_count == 0:
            errors.append(f"Profiled worker {assignment.worker_id} has 0 assigned CPUs")
        if assignment.rayon_num_threads != assignment.assigned_cpu_count:
            errors.append(f"Profiled worker {assignment.worker_id}: rayon_num_threads does not match assigned_cpu_count")
        for cpu in assignment.assigned_cpus:
            if cpu in assigned:
                errors.append(f"CPU {cpu} assigned to multiple profiled workers")
            assigned.add(cpu)
    return errors
