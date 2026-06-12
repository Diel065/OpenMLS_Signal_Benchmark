"""
CPU affinity planner.

Implements the full CPU affinity planning pipeline:
  1. Detect online CPUs and topology
  2. Sample per-CPU load
  3. Select least-loaded CPUs for profiled workers
  4. Compute profiled/background masks
  5. Write cpu_affinity_plan.json
"""

import json
import os
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from .cpu_mask_util import (
        cpu_list_to_mask,
        cpu_list_to_docker_cpuset,
        mask_to_hex,
        mask_to_cpu_list,
        masks_overlap,
        complement_mask,
        union_masks,
        mask_popcount,
        ensure_non_overlapping,
    )
    from .cpu_topology import (
        CpuTopology,
        detect_cpu_topology,
        get_online_cpu_list,
        sample_cpu_load,
        select_least_loaded_cpus,
        get_smt_siblings,
        topology_to_dict,
    )
    from .resource_profiles import ResourceProfile
except ImportError:
    from cpu_mask_util import (
        cpu_list_to_mask,
        cpu_list_to_docker_cpuset,
        mask_to_hex,
        mask_to_cpu_list,
        masks_overlap,
        complement_mask,
        union_masks,
        mask_popcount,
        ensure_non_overlapping,
    )
    from cpu_topology import (
        CpuTopology,
        detect_cpu_topology,
        get_online_cpu_list,
        sample_cpu_load,
        select_least_loaded_cpus,
        get_smt_siblings,
        topology_to_dict,
    )
    from resource_profiles import ResourceProfile


@dataclass
class ProfiledAssignment:
    """Assignment of a profiled singleton to specific CPUs."""
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
    """Assignment of a background container to the background mask."""
    container_name: str
    container_role: str  # "packed", "infrastructure", "runner_or_helper"
    assigned_cpus: List[int]
    assigned_mask_hex: str


@dataclass
class AffinityPlan:
    """Complete CPU affinity plan for a benchmark run."""
    run_id: str
    created_at: str
    hostname: str
    cpu_affinity_mode: str  # "profiled-nor-background" or "none"
    sample_seconds: float
    selection_policy: str  # "least_loaded"
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


def create_affinity_plan(
    run_id: str,
    profiled_worker_specs: List[Dict[str, str]],
    background_specs: List[Dict[str, str]],
    cpu_affinity_mode: str = "profiled-nor-background",
    sample_seconds: float = 20.0,
    reserve_smt_siblings: bool = False,
    profiled_cpu_counts: Optional[Dict[str, int]] = None,
) -> AffinityPlan:
    """Create a complete CPU affinity plan.

    Args:
        run_id: Benchmark run ID.
        profiled_worker_specs: List of dicts with keys:
            worker_id, container_name, logical_client_id, experiment_kind,
            resource_profile_id
        background_specs: List of dicts with keys:
            container_name, container_role
        cpu_affinity_mode: Affinity mode string.
        sample_seconds: Duration in seconds for CPU load sampling.
        reserve_smt_siblings: Whether to reserve SMT siblings.
        profiled_cpu_counts: Optional dict mapping worker_id -> required CPU count.
                             If not provided, each profiled worker gets 1 CPU.

    Returns:
        An AffinityPlan object.

    Raises:
        ValueError: If insufficient CPUs are available.
    """
    if cpu_affinity_mode == "none":
        return _create_empty_affinity_plan(run_id, sample_seconds)

    topology = detect_cpu_topology()
    online_cpus = get_online_cpu_list(topology)
    online_mask = cpu_list_to_mask(online_cpus)

    if profiled_cpu_counts is None:
        profiled_cpu_counts = {s["worker_id"]: 1 for s in profiled_worker_specs}

    total_profiled_cpus = sum(profiled_cpu_counts.values())

    if total_profiled_cpus == 0:
        load_samples = {}
    else:
        load_samples = sample_cpu_load(sample_seconds)
        for cpu_id in online_cpus:
            if cpu_id not in load_samples:
                load_samples[cpu_id] = 0.0

    if total_profiled_cpus == 0 and not profiled_worker_specs:
        selected_cpus = []
    elif total_profiled_cpus == 0:
        selected_cpus = []
    else:
        selected_cpus = select_least_loaded_cpus(
            topology=topology,
            required_count=total_profiled_cpus,
            load_samples=load_samples,
            exclude_cpus=None,
            prefer_physical_cores=True,
            reserve_smt_siblings=reserve_smt_siblings,
        )

    profiled_assignments: List[ProfiledAssignment] = []
    warnings: List[str] = []
    cpu_index = 0

    for spec in profiled_worker_specs:
        worker_id = spec["worker_id"]
        count = profiled_cpu_counts.get(worker_id, 1)
        assigned = selected_cpus[cpu_index:cpu_index + count]
        cpu_index += count

        rayon_n = spec.get("rayon_num_threads", count)
        if not assigned:
            profiled_assignments.append(ProfiledAssignment(
                worker_id=worker_id,
                container_name=spec["container_name"],
                logical_client_id=spec["logical_client_id"],
                assigned_cpus=[],
                assigned_mask_hex="0x0",
                assigned_cpu_count=0,
                rayon_num_threads=rayon_n,
                experiment_kind=spec.get("experiment_kind", ""),
                resource_profile_id=spec.get("resource_profile_id", ""),
            ))
            continue

        mask = cpu_list_to_mask(assigned)
        profiled_assignments.append(ProfiledAssignment(
            worker_id=worker_id,
            container_name=spec["container_name"],
            logical_client_id=spec["logical_client_id"],
            assigned_cpus=sorted(assigned),
            assigned_mask_hex=mask_to_hex(mask),
            assigned_cpu_count=len(assigned),
            rayon_num_threads=rayon_n,
            experiment_kind=spec.get("experiment_kind", ""),
            resource_profile_id=spec.get("resource_profile_id", ""),
        ))

    assigned_cpu_set: Set[int] = set()
    for pa in profiled_assignments:
        assigned_cpu_set.update(pa.assigned_cpus)

    profiled_mask = cpu_list_to_mask(sorted(assigned_cpu_set))

    if reserve_smt_siblings and topology.smt_enabled:
        smt_siblings = get_smt_siblings(topology, sorted(assigned_cpu_set))
        reserved_set = set(assigned_cpu_set) | set(smt_siblings)
        reserved_mask = cpu_list_to_mask(sorted(reserved_set))
    else:
        reserved_mask = profiled_mask

    background_mask = complement_mask(reserved_mask, online_mask)
    background_cpus = mask_to_cpu_list(background_mask)

    if not background_cpus and background_specs:
        warnings.append(
            f"Background mask is empty after reserving profiled CPUs "
            f"(profiled={len(assigned_cpu_set)}, online={len(online_cpus)})"
        )

    background_assignments: List[BackgroundAssignment] = []
    bg_mask_hex = mask_to_hex(background_mask)
    for spec in background_specs:
        background_assignments.append(BackgroundAssignment(
            container_name=spec["container_name"],
            container_role=spec["container_role"],
            assigned_cpus=background_cpus,
            assigned_mask_hex=bg_mask_hex,
        ))

    if len(online_cpus) < total_profiled_cpus:
        warnings.append(
            f"Host has {len(online_cpus)} online CPUs but {total_profiled_cpus} "
            f"profiled CPUs are requested"
        )

    smt_policy = "reserve_siblings" if reserve_smt_siblings else "no_reservation"

    plan = AffinityPlan(
        run_id=run_id,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        hostname=socket.gethostname(),
        cpu_affinity_mode=cpu_affinity_mode,
        sample_seconds=sample_seconds,
        selection_policy="least_loaded",
        online_cpus=online_cpus,
        online_cpu_mask_hex=mask_to_hex(online_mask),
        cpu_topology=topology_to_dict(topology),
        sampled_cpu_load=load_samples,
        profiled_assignments=profiled_assignments,
        profiled_mask_hex=mask_to_hex(profiled_mask),
        reserved_mask_hex=mask_to_hex(reserved_mask),
        background_cpus=background_cpus,
        background_mask_hex=bg_mask_hex,
        smt_sibling_policy=smt_policy,
        warnings=warnings,
        background_assignments=background_assignments,
    )

    return plan


def _create_empty_affinity_plan(run_id: str, sample_seconds: float) -> AffinityPlan:
    """Create an empty affinity plan when affinity mode is 'none'."""
    return AffinityPlan(
        run_id=run_id,
        created_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        hostname=socket.gethostname(),
        cpu_affinity_mode="none",
        sample_seconds=sample_seconds,
        selection_policy="none",
        online_cpus=[],
        online_cpu_mask_hex="0x0",
        cpu_topology={},
        sampled_cpu_load={},
        profiled_assignments=[],
        profiled_mask_hex="0x0",
        reserved_mask_hex="0x0",
        background_cpus=[],
        background_mask_hex="0x0",
        smt_sibling_policy="no_reservation",
        warnings=[],
        background_assignments=[],
    )


def write_affinity_plan_json(plan: AffinityPlan, output_dir: str) -> str:
    """Write the affinity plan to cpu_affinity_plan.json.

    Returns the path to the written file.
    """
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, "cpu_affinity_plan.json")

    sampled_load_serializable = {str(k): v for k, v in plan.sampled_cpu_load.items()}

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
        "sampled_cpu_load": sampled_load_serializable,
        "profiled_assignments": [
            {
                "worker_id": pa.worker_id,
                "container_name": pa.container_name,
                "logical_client_id": pa.logical_client_id,
                "assigned_cpus": pa.assigned_cpus,
                "assigned_mask_hex": pa.assigned_mask_hex,
                "assigned_cpu_count": pa.assigned_cpu_count,
                "rayon_num_threads": pa.rayon_num_threads,
                "experiment_kind": pa.experiment_kind,
                "resource_profile_id": pa.resource_profile_id,
            }
            for pa in plan.profiled_assignments
        ],
        "profiled_mask_hex": plan.profiled_mask_hex,
        "reserved_mask_hex": plan.reserved_mask_hex,
        "background_cpus": plan.background_cpus,
        "background_mask_hex": plan.background_mask_hex,
        "smt_sibling_policy": plan.smt_sibling_policy,
        "warnings": plan.warnings,
        "background_assignments": [
            {
                "container_name": ba.container_name,
                "container_role": ba.container_role,
                "assigned_cpus": ba.assigned_cpus,
                "assigned_mask_hex": ba.assigned_mask_hex,
            }
            for ba in plan.background_assignments
        ],
    }

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    return filepath


def get_background_cpuset(plan: AffinityPlan) -> str:
    """Get the Docker cpuset string for background containers."""
    return cpu_list_to_docker_cpuset(plan.background_cpus)


def get_profiled_cpuset(plan: AffinityPlan, worker_id: str) -> Optional[str]:
    """Get the Docker cpuset string for a specific profiled worker."""
    for pa in plan.profiled_assignments:
        if pa.worker_id == worker_id:
            return cpu_list_to_docker_cpuset(pa.assigned_cpus)
    return None


def get_rayon_num_threads(plan: AffinityPlan, worker_id: str) -> Optional[int]:
    """Get the RAYON_NUM_THREADS value for a specific profiled worker."""
    for pa in plan.profiled_assignments:
        if pa.worker_id == worker_id:
            return pa.rayon_num_threads
    return None


def validate_affinity_plan(plan: AffinityPlan) -> List[str]:
    """Validate an affinity plan for correctness.

    Returns a list of error messages (empty if valid).
    """
    errors: List[str] = []

    if plan.cpu_affinity_mode == "none":
        return errors

    assigned_set: Set[int] = set()
    for pa in plan.profiled_assignments:
        for cpu in pa.assigned_cpus:
            if cpu in assigned_set:
                errors.append(
                    f"CPU {cpu} assigned to multiple profiled workers"
                )
            assigned_set.add(cpu)

    profiled_mask = cpu_list_to_mask(sorted(assigned_set))
    background_mask = complement_mask(profiled_mask, cpu_list_to_mask(plan.online_cpus))

    if masks_overlap(profiled_mask, background_mask):
        errors.append(
            "Profiled mask overlaps with background mask"
        )

    if not plan.background_cpus and plan.background_assignments:
        errors.append(
            "Background mask is empty but background containers are assigned"
        )

    for pa in plan.profiled_assignments:
        if pa.assigned_cpu_count == 0:
            errors.append(
                f"Profiled worker {pa.worker_id} has 0 assigned CPUs"
            )
        if pa.rayon_num_threads != pa.assigned_cpu_count:
            errors.append(
                f"Profiled worker {pa.worker_id}: RAYON_NUM_THREADS={pa.rayon_num_threads} "
                f"does not match assigned_cpu_count={pa.assigned_cpu_count}"
            )

    return errors
