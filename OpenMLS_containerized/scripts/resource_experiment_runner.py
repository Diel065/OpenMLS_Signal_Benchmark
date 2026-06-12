"""
Resource experiment orchestration.

Ties together CPU affinity planning, resource profile generation,
compose generation extensions, preflight checks, resource monitoring,
and failure tracking for resource experiment benchmark runs.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    from .cpu_mask_util import (
        cpu_list_to_mask,
        cpu_list_to_docker_cpuset,
        mask_to_hex,
        mask_to_cpu_list,
    )
    from .cpu_topology import detect_cpu_topology, get_online_cpu_list
    from .cpu_affinity_planner import (
        create_affinity_plan,
        write_affinity_plan_json,
        get_background_cpuset,
        get_profiled_cpuset,
        get_rayon_num_threads,
        validate_affinity_plan,
        AffinityPlan,
    )
    from .resource_profiles import (
        ResourceProfile,
        generate_ram_sweep_profiles,
        generate_cpu_matrix_profiles,
        profile_to_compose_dict,
    )
    from .resource_experiment_sidecars import SidecarWriter, get_expected_files
    from .resource_experiment_failures import (
        WorkerFailureInfo,
        classify_worker_failure,
        build_run_status,
        worker_failure_info_to_dict,
        FAILURE_CLASSES,
    )
except ImportError:
    from cpu_mask_util import (
        cpu_list_to_mask,
        cpu_list_to_docker_cpuset,
        mask_to_hex,
        mask_to_cpu_list,
    )
    from cpu_topology import detect_cpu_topology, get_online_cpu_list
    from cpu_affinity_planner import (
        create_affinity_plan,
        write_affinity_plan_json,
        get_background_cpuset,
        get_profiled_cpuset,
        get_rayon_num_threads,
        validate_affinity_plan,
        AffinityPlan,
    )
    from resource_profiles import (
        ResourceProfile,
        generate_ram_sweep_profiles,
        generate_cpu_matrix_profiles,
        profile_to_compose_dict,
    )
    from resource_experiment_sidecars import SidecarWriter, get_expected_files
    from resource_experiment_failures import (
        WorkerFailureInfo,
        classify_worker_failure,
        build_run_status,
        worker_failure_info_to_dict,
        FAILURE_CLASSES,
    )


class ResourceExperimentConfig:
    """Configuration for a resource experiment benchmark run."""

    def __init__(
        self,
        resource_experiment: str = "none",
        profiled_singleton_count: int = 1,
        ram_sweep_values: Optional[List[str]] = None,
        ram_sweep_cpu_count: int = 10,
        cpu_matrix_core_counts: Optional[List[int]] = None,
        cpu_matrix_capacity_fractions: Optional[List[float]] = None,
        cpu_affinity_mode: str = "none",
        cpu_affinity_sample_seconds: float = 20.0,
        reserve_smt_siblings: bool = False,
        resource_monitor_interval_ms: int = 500,
        failure_experiment: bool = False,
    ):
        self.resource_experiment = resource_experiment
        self.profiled_singleton_count = profiled_singleton_count
        self.ram_sweep_values = ram_sweep_values or ["32m", "64m", "128m", "256m", "512m", "1g"]
        self.ram_sweep_cpu_count = ram_sweep_cpu_count
        self.cpu_matrix_core_counts = cpu_matrix_core_counts or [1, 2, 4]
        self.cpu_matrix_capacity_fractions = cpu_matrix_capacity_fractions or [0.25, 0.50, 0.75, 1.00]
        self.cpu_affinity_mode = cpu_affinity_mode
        self.cpu_affinity_sample_seconds = cpu_affinity_sample_seconds
        self.reserve_smt_siblings = reserve_smt_siblings
        self.resource_monitor_interval_ms = resource_monitor_interval_ms
        self.failure_experiment = failure_experiment

        if self.resource_experiment in ("ram-sweep-singleton", "cpu-matrix-singleton"):
            if self.cpu_affinity_mode == "none":
                self.cpu_affinity_mode = "profiled-nor-background"

    @property
    def is_resource_experiment(self) -> bool:
        return self.resource_experiment in ("ram-sweep-singleton", "cpu-matrix-singleton")

    @property
    def experiment_kind(self) -> str:
        if self.resource_experiment == "ram-sweep-singleton":
            return "ram_sweep_singleton"
        elif self.resource_experiment == "cpu-matrix-singleton":
            return "cpu_matrix_singleton"
        return "none"


def generate_resource_profiles(config: ResourceExperimentConfig) -> List[ResourceProfile]:
    """Generate resource profiles based on experiment configuration."""
    if config.resource_experiment == "ram-sweep-singleton":
        return generate_ram_sweep_profiles(
            ram_values=config.ram_sweep_values,
            assigned_cpu_count=config.ram_sweep_cpu_count,
        )
    elif config.resource_experiment == "cpu-matrix-singleton":
        return generate_cpu_matrix_profiles(
            core_counts=config.cpu_matrix_core_counts,
            capacity_fractions=config.cpu_matrix_capacity_fractions,
        )
    return []


def build_affinity_plan(
    run_id: str,
    config: ResourceExperimentConfig,
    profiles: List[ResourceProfile],
    singleton_worker_ids: List[str],
    singleton_client_ids: List[str],
    background_containers: List[Dict[str, str]],
) -> AffinityPlan:
    """Build the CPU affinity plan for a resource experiment."""
    if config.cpu_affinity_mode == "none":
        return create_affinity_plan(
            run_id=run_id,
            profiled_worker_specs=[],
            background_specs=[],
            cpu_affinity_mode="none",
        )

    profiled_worker_specs = []
    profiled_cpu_counts = {}

    for i, (worker_id, client_id) in enumerate(zip(singleton_worker_ids, singleton_client_ids)):
        profile = profiles[i % len(profiles)] if profiles else None
        cpu_count = profile.assigned_cpu_count if profile else 1

        profiled_worker_specs.append({
            "worker_id": worker_id,
            "container_name": f"worker-{client_id}",
            "logical_client_id": client_id,
            "experiment_kind": config.experiment_kind,
            "resource_profile_id": profile.resource_profile_id if profile else "",
        })
        profiled_cpu_counts[worker_id] = cpu_count

    bg_specs = []
    for bc in background_containers:
        bg_specs.append({
            "container_name": bc["container_name"],
            "container_role": bc.get("container_role", "background"),
        })

    plan = create_affinity_plan(
        run_id=run_id,
        profiled_worker_specs=profiled_worker_specs,
        background_specs=bg_specs,
        cpu_affinity_mode=config.cpu_affinity_mode,
        sample_seconds=config.cpu_affinity_sample_seconds,
        reserve_smt_siblings=config.reserve_smt_siblings,
        profiled_cpu_counts=profiled_cpu_counts,
    )

    return plan


def apply_profiles_to_affinity_plan(
    plan: AffinityPlan,
    profiles: List[ResourceProfile],
) -> AffinityPlan:
    """Update profiles with cpuset information from the affinity plan."""
    for i, pa in enumerate(plan.profiled_assignments):
        if i < len(profiles):
            profile = profiles[i]
            profile.cpuset_cpus = cpu_list_to_docker_cpuset(pa.assigned_cpus)
            profile.cpuset_mask_hex = pa.assigned_mask_hex
            profile.rayon_num_threads = pa.rayon_num_threads
            profile.assigned_cpu_count = pa.assigned_cpu_count

    return plan


def get_compose_resource_config(
    worker_id: str,
    client_id: str,
    plan: AffinityPlan,
    profiles: List[ResourceProfile],
    profile_index: int,
) -> Dict[str, Any]:
    """Get the docker-compose resource configuration for a profiled singleton.

    Returns a dict with keys like 'cpuset', 'cpus', 'mem_limit',
    'memswap_limit', and environment vars.
    """
    result: Dict[str, Any] = {}
    env: Dict[str, str] = {}

    profile = profiles[profile_index % len(profiles)] if profiles else None

    cpuset = get_profiled_cpuset(plan, worker_id)
    if cpuset:
        result["cpuset"] = cpuset

    if profile:
        if profile.cpu_limit_cpus is not None:
            result["cpus"] = str(profile.cpu_limit_cpus)
        if profile.memory_limit:
            result["mem_limit"] = profile.memory_limit
        if profile.memory_swap:
            result["memswap_limit"] = profile.memory_swap
        if profile.rayon_num_threads > 0:
            env["RAYON_NUM_THREADS"] = str(profile.rayon_num_threads)

    rayon = get_rayon_num_threads(plan, worker_id)
    if rayon and "RAYON_NUM_THREADS" not in env:
        env["RAYON_NUM_THREADS"] = str(rayon)

    result["environment"] = env
    return result


def get_background_compose_config(plan: AffinityPlan) -> Dict[str, Any]:
    """Get the docker-compose resource configuration for background containers."""
    result: Dict[str, Any] = {}
    cpuset = get_background_cpuset(plan)
    if cpuset:
        result["cpuset"] = cpuset
    return result


def build_worker_resource_assignments(
    run_id: str,
    plan: AffinityPlan,
    profiles: List[ResourceProfile],
    selected_profile_index: Optional[int],
    singleton_worker_ids: List[str],
    singleton_client_ids: List[str],
    singleton_container_names: List[str],
    packed_container_names: List[str],
    infrastructure_container_names: List[str],
    container_ids: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Build worker_resource_assignments.csv rows."""
    if container_ids is None:
        container_ids = {}

    assignments = []
    background_cpuset = get_background_cpuset(plan)
    bg_mask_hex = plan.background_mask_hex if plan else ""

    selected_sp_profile_id = ""
    if selected_profile_index is not None and 0 <= selected_profile_index < len(profiles):
        selected_sp_profile_id = profiles[selected_profile_index].resource_profile_id

    for i, (worker_id, client_id, container_name) in enumerate(
        zip(singleton_worker_ids, singleton_client_ids, singleton_container_names)
    ):
        profile = profiles[i % len(profiles)] if profiles else None
        pa = plan.profiled_assignments[i] if i < len(plan.profiled_assignments) else None

        is_selected = False
        p_index = profile.resource_profile_index if profile else -1
        if selected_profile_index is not None and p_index == selected_profile_index:
            is_selected = True

        assignments.append({
            "run_id": run_id,
            "logical_client_id": client_id,
            "worker_id": worker_id,
            "physical_worker_id": worker_id,
            "container_name": container_name,
            "container_id": container_ids.get(container_name, ""),
            "container_mode": "singleton",
            "profile_enabled": True,
            "resource_profile_index": p_index,
            "resource_profile_id": profile.resource_profile_id if profile else "",
            "experiment_kind": profile.experiment_kind if profile else "",
            "selected_for_this_run": is_selected,
            "cpu_affinity_role": "profiled_singleton",
            "cpuset_cpus": cpu_list_to_docker_cpuset(pa.assigned_cpus) if pa else "",
            "cpuset_mask_hex": pa.assigned_mask_hex if pa else "",
            "cpu_limit_cpus": profile.cpu_limit_cpus if profile else "",
            "capacity_fraction": profile.capacity_fraction if profile else "",
            "assigned_cpu_count": profile.assigned_cpu_count if profile else 0,
            "memory_limit": profile.memory_limit if profile else "",
            "memory_swap": profile.memory_swap if profile else "",
            "rayon_num_threads": profile.rayon_num_threads if profile else 0,
            "background_cpuset_cpus": background_cpuset,
            "background_mask_hex": bg_mask_hex,
            "profile_label": profile.profile_label if profile else "",
        })

    for container_name in packed_container_names:
        assignments.append({
            "run_id": run_id,
            "logical_client_id": "",
            "worker_id": container_name,
            "physical_worker_id": container_name,
            "container_name": container_name,
            "container_id": container_ids.get(container_name, ""),
            "container_mode": "packed",
            "profile_enabled": False,
            "resource_profile_index": -1,
            "resource_profile_id": "",
            "experiment_kind": "",
            "selected_for_this_run": False,
            "cpu_affinity_role": "background_packed",
            "cpuset_cpus": background_cpuset,
            "cpuset_mask_hex": bg_mask_hex,
            "cpu_limit_cpus": "",
            "capacity_fraction": "",
            "assigned_cpu_count": 0,
            "memory_limit": "",
            "memory_swap": "",
            "rayon_num_threads": 0,
            "background_cpuset_cpus": background_cpuset,
            "background_mask_hex": bg_mask_hex,
            "profile_label": "",
        })

    for container_name in infrastructure_container_names:
        assignments.append({
            "run_id": run_id,
            "logical_client_id": "",
            "worker_id": container_name,
            "physical_worker_id": container_name,
            "container_name": container_name,
            "container_id": container_ids.get(container_name, ""),
            "container_mode": "infrastructure",
            "profile_enabled": False,
            "resource_profile_index": -1,
            "resource_profile_id": "",
            "experiment_kind": "",
            "selected_for_this_run": False,
            "cpu_affinity_role": "background_infrastructure",
            "cpuset_cpus": background_cpuset,
            "cpuset_mask_hex": bg_mask_hex,
            "cpu_limit_cpus": "",
            "capacity_fraction": "",
            "assigned_cpu_count": 0,
            "memory_limit": "",
            "memory_swap": "",
            "rayon_num_threads": 0,
            "background_cpuset_cpus": background_cpuset,
            "background_mask_hex": bg_mask_hex,
            "profile_label": "",
        })

    return assignments


def write_all_sidecars(
    run_id: str,
    output_dir: str,
    plan: AffinityPlan,
    profiles: List[ResourceProfile],
    worker_assignments: List[Dict[str, Any]],
    preflight_results: List[Dict[str, Any]],
    resource_summaries: List[Dict[str, Any]],
    worker_failures: List[Dict[str, Any]],
    run_status: Dict[str, Any],
) -> Dict[str, str]:
    """Write all sidecar files for a resource experiment run.

    Returns a dict mapping filename -> filepath.
    """
    writer = SidecarWriter(output_dir)
    paths = {}

    paths["cpu_affinity_plan.json"] = write_affinity_plan_json(plan, output_dir)

    paths["resource_profiles.csv"] = writer.write_resource_profiles(
        run_id, [p.to_dict() for p in profiles]
    )

    paths["worker_resource_assignments.csv"] = writer.write_worker_resource_assignments(
        run_id, worker_assignments
    )

    if preflight_results:
        paths["cpu_affinity_preflight.csv"] = writer.write_preflight_results(
            run_id, preflight_results
        )

    if resource_summaries:
        paths["resource_summary.csv"] = writer.write_resource_summary(
            run_id, resource_summaries
        )

    if worker_failures:
        paths["worker_failures.csv"] = writer.write_worker_failures(
            run_id, worker_failures
        )

    paths["run_status.csv"] = writer.write_run_status(run_id, run_status)

    return paths
