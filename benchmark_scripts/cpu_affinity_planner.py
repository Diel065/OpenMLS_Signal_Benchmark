import json
import os
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from cpu_mask_util import cpu_list_to_mask, mask_to_cpu_list, masks_overlap, complement_mask
from cpu_topology import detect_cpu_topology, get_online_cpu_list, sample_cpu_load, select_least_loaded_cpus, get_smt_siblings, topology_to_dict


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
            assigned_mask_hex=hex(mask),
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
    bg_mask_hex = hex(background_mask)
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
        online_cpu_mask_hex=hex(online_mask),
        cpu_topology=topology_to_dict(topology),
        sampled_cpu_load=load_samples,
        profiled_assignments=profiled_assignments,
        profiled_mask_hex=hex(profiled_mask),
        reserved_mask_hex=hex(reserved_mask),
        background_cpus=background_cpus,
        background_mask_hex=bg_mask_hex,
        smt_sibling_policy=smt_policy,
        warnings=warnings,
        background_assignments=background_assignments,
    )

    return plan


def _create_empty_affinity_plan(run_id: str, sample_seconds: float) -> AffinityPlan:
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


def validate_affinity_plan(plan: AffinityPlan) -> List[str]:
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
        plan.warnings.append(
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
