"""Resource assignments recorded with each experiment."""

from typing import Any, Dict, List, Optional

try:
    from .cpu_affinity_planner import AffinityPlan, get_background_cpuset
    from .cpu_mask_util import cpu_list_to_docker_cpuset
    from .resource_profiles import ResourceProfile, get_selected_profile
except ImportError:
    from cpu_affinity_planner import AffinityPlan, get_background_cpuset
    from cpu_mask_util import cpu_list_to_docker_cpuset
    from resource_profiles import ResourceProfile, get_selected_profile


def _profile_for_profiled_index(
    profiles: List[ResourceProfile],
    index: int,
) -> Optional[ResourceProfile]:
    """Return the resource profile for a profiled singleton index.

    Singleton experiments select exactly one profile and apply it to the one
    profiled worker. Parallel sweeps select multiple profiles; in that case the
    sidecars and affinity plan must preserve the one-profile-per-singleton
    ordering used by compose generation.
    """
    if not profiles:
        return None

    selected_profiles = [p for p in profiles if p.selected_for_this_run]
    if len(selected_profiles) > 1:
        return selected_profiles[index % len(selected_profiles)]

    selected = get_selected_profile(profiles)
    if selected is not None:
        return selected

    return profiles[index % len(profiles)]


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

    profile_by_id: Dict[str, ResourceProfile] = {}
    if profiles:
        profile_by_id = {p.resource_profile_id: p for p in profiles}

    for i, (worker_id, client_id, container_name) in enumerate(
        zip(singleton_worker_ids, singleton_client_ids, singleton_container_names)
    ):
        pa = plan.profiled_assignments[i] if i < len(plan.profiled_assignments) else None
        profile = None
        if pa is not None and pa.resource_profile_id:
            profile = profile_by_id.get(pa.resource_profile_id)
        if profile is None:
            profile = _profile_for_profiled_index(profiles, i)

        is_selected = bool(profile and profile.selected_for_this_run)
        p_index = profile.resource_profile_index if profile else -1

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
            "memory_model": profile.memory_model if profile else "",
            "docker_memory_limit": profile.docker_memory_limit if profile else "",
            "app_heap_budget": profile.app_heap_budget if profile else "",
            "app_heap_budget_bytes": profile.app_heap_budget_bytes if profile else "",
            "rayon_num_threads": profile.rayon_num_threads if profile else 0,
            "background_cpuset_cpus": background_cpuset,
            "background_mask_hex": bg_mask_hex,
            "profile_label": profile.profile_label if profile else "",
            "sweep_kind": profile.sweep_kind if profile else "",
            "app_heap_interpretation": profile.app_heap_interpretation if profile else "",
            "cpu_interpretation": profile.cpu_interpretation if profile else "",
            "cpu_period_us": profile.cpu_period_us if profile else "",
            "cpu_quota_us": profile.cpu_quota_us if profile else "",
            "group_creator": profile.group_creator if profile else False,
            "group_creator_reason": profile.group_creator_reason if profile else "",
            "strict_cpuset_satisfied": profile.strict_cpuset_satisfied if profile else False,
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
            "memory_model": "",
            "docker_memory_limit": "",
            "app_heap_budget": "",
            "app_heap_budget_bytes": "",
            "rayon_num_threads": 0,
            "background_cpuset_cpus": background_cpuset,
            "background_mask_hex": bg_mask_hex,
            "profile_label": "",
            "sweep_kind": "",
            "app_heap_interpretation": "",
            "cpu_interpretation": "",
            "cpu_period_us": "",
            "cpu_quota_us": "",
            "group_creator": False,
            "group_creator_reason": "",
            "strict_cpuset_satisfied": False,
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
            "memory_model": "",
            "docker_memory_limit": "",
            "app_heap_budget": "",
            "app_heap_budget_bytes": "",
            "rayon_num_threads": 0,
            "background_cpuset_cpus": background_cpuset,
            "background_mask_hex": bg_mask_hex,
            "profile_label": "",
            "sweep_kind": "",
            "app_heap_interpretation": "",
            "cpu_interpretation": "",
            "cpu_period_us": "",
            "cpu_quota_us": "",
            "group_creator": False,
            "group_creator_reason": "",
            "strict_cpuset_satisfied": False,
        })

    return assignments
