"""
Resource profile generation for RAM sweep, CPU matrix, and parallel sweep experiments.

Generates structured resource profiles for:
  - ram_sweep_singleton: Sweep memory limits with abundant CPU
  - cpu_matrix_singleton: Matrix of (core_count, capacity_fraction) combinations
  - embedded_budget_singleton: Docker memory x Docker CPU combinations
  - ram_sweep: Parallel 10-profile sweep of Docker memory limits
  - cpu_quota_sweep: Parallel 10-profile sweep of Docker CPU fractions

Supports explicit profile selection via index or ID for single-profile
scientific threshold experiments. Supports parallel 10-profile sweeps.

Signal uses docker-cgroup memory model (no app-heap-budget allocator).
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import uuid


DOCKER_MIN_MEMORY_BYTES = 6 * 1024 * 1024
DOCKER_MIN_CPU_QUOTA = 0.01
DOCKER_HARD_QUOTA_CPU_FLOOR = 0.01
CGROUP_V2_CPU_MAX_QUOTA_FLOOR_US = 10000
CGROUP_V2_CPU_QUOTA_HEADROOM_FACTOR = 1.0
DOCKER_CFS_MAX_PERIOD_US = 1_000_000


@dataclass
class ResourceProfile:
    """A single resource profile for a profiled singleton container."""
    resource_profile_id: str
    experiment_kind: str
    profile_label: str
    resource_profile_index: int = -1
    cpu_limit_cpus: Optional[float] = None
    capacity_fraction: Optional[float] = None
    assigned_cpu_count: int = 1
    memory_limit: Optional[str] = None
    memory_swap: Optional[str] = None
    rayon_num_threads: int = 1
    cpuset_cpus: Optional[str] = None
    cpuset_mask_hex: Optional[str] = None
    profile_notes: str = ""
    selected_for_this_run: bool = False
    cpuset_role: str = ""
    memory_model: str = "docker-cgroup"
    docker_memory_limit: Optional[str] = None
    app_heap_budget: Optional[str] = None
    app_heap_budget_bytes: Optional[int] = None
    sweep_kind: str = ""
    app_heap_interpretation: str = ""
    cpu_interpretation: str = ""
    cpu_period_us: int = 100000
    cpu_quota_us: Optional[int] = None
    group_creator: bool = False
    group_creator_reason: str = ""
    strict_cpuset_satisfied: bool = False

    def to_dict(self) -> Dict:
        docker_memory_limit = self.docker_memory_limit or self.memory_limit or ""
        return {
            "resource_profile_id": self.resource_profile_id,
            "experiment_kind": self.experiment_kind,
            "resource_profile_index": self.resource_profile_index,
            "profile_label": self.profile_label,
            "selected_for_this_run": self.selected_for_this_run,
            "cpu_limit_cpus": self.cpu_limit_cpus,
            "capacity_fraction": self.capacity_fraction,
            "assigned_cpu_count": self.assigned_cpu_count,
            "memory_limit": self.memory_limit,
            "memory_swap": self.memory_swap,
            "memory_model": self.memory_model,
            "docker_memory_limit": docker_memory_limit,
            "app_heap_budget": self.app_heap_budget or "",
            "app_heap_budget_bytes": self.app_heap_budget_bytes or "",
            "rayon_num_threads": self.rayon_num_threads,
            "cpuset_cpus": self.cpuset_cpus or "",
            "cpuset_mask_hex": self.cpuset_mask_hex or "",
            "cpuset_role": self.cpuset_role or "",
            "profile_notes": self.profile_notes,
            "sweep_kind": self.sweep_kind or "",
            "app_heap_interpretation": self.app_heap_interpretation or "",
            "cpu_interpretation": self.cpu_interpretation or "",
            "cpu_period_us": self.cpu_period_us,
            "cpu_quota_us": self.cpu_quota_us,
            "group_creator": self.group_creator,
            "group_creator_reason": self.group_creator_reason or "",
            "strict_cpuset_satisfied": self.strict_cpuset_satisfied,
        }

    def to_csv_row(self) -> Dict:
        return self.to_dict()


def generate_ram_sweep_profiles(
    ram_values: List[str],
    assigned_cpu_count: int = 10,
    run_id: str = "",
) -> List[ResourceProfile]:
    """Generate resource profiles for a RAM sweep experiment.

    Each profile varies only memory_limit; CPU is abundant.
    CPU quota is unset (cpu_limit_cpus = None).
    RAYON_NUM_THREADS = assigned_cpu_count.
    memory_swap = memory_limit for each profile.

    Args:
        ram_values: List of memory limit strings, e.g., ["32m", "64m", "128m"].
        assigned_cpu_count: Number of CPUs assigned to cpuset.
        run_id: Benchmark run ID for profile labeling.

    Returns:
        List of ResourceProfile objects, one per RAM value.
    """
    if len(ram_values) > 32:
        raise ValueError(f"RAM sweep supports at most 32 values, got {len(ram_values)}")

    profiles = []
    for idx, ram_value in enumerate(ram_values):
        if not validate_memory_string(ram_value):
            raise ValueError(f"Invalid Docker memory limit '{ram_value}'")
        memory_bytes = parse_memory_to_bytes(ram_value)
        if memory_bytes < DOCKER_MIN_MEMORY_BYTES:
            raise ValueError(
                f"Docker memory limit '{ram_value}' is below the daemon minimum "
                f"of 6 MiB ({DOCKER_MIN_MEMORY_BYTES} bytes, or 6144k)"
            )
        profile_id = f"ram_{ram_value.replace('m','m').replace('g','g')}"
        label = f"RAM={ram_value} CPUs={assigned_cpu_count}"
        profile = ResourceProfile(
            resource_profile_id=profile_id,
            experiment_kind="ram_sweep_singleton",
            resource_profile_index=idx,
            profile_label=label,
            cpu_limit_cpus=None,
            capacity_fraction=None,
            assigned_cpu_count=assigned_cpu_count,
            memory_limit=ram_value,
            memory_swap=ram_value,
            rayon_num_threads=assigned_cpu_count,
            cpuset_role="ram_sweep",
            memory_model="docker-cgroup",
            profile_notes=f"RAM sweep: {ram_value} memory, {assigned_cpu_count} CPUs",
        )
        profiles.append(profile)

    return profiles


def generate_cpu_matrix_profiles(
    core_counts: List[int],
    capacity_fractions: List[float],
    high_memory_limit: Optional[str] = None,
    run_id: str = "",
) -> List[ResourceProfile]:
    """Generate resource profiles for a CPU matrix experiment.

    Creates a full Cartesian product of core_counts x capacity_fractions.
    Docker cpus quota = assigned_cpu_count * capacity_fraction.
    RAYON_NUM_THREADS = assigned_cpu_count.
    Memory is unlimited or set to a safe high value (not an experimental variable).

    Invalid combinations (e.g., capacity > assigned_cpu_count) are skipped.

    Args:
        core_counts: List of CPU thread counts, e.g., [1, 2, 4].
        capacity_fractions: List of capacity fractions, e.g., [0.25, 0.5, 0.75, 1.0].
        high_memory_limit: Optional safe-high memory limit to prevent RAM bottleneck.
        run_id: Benchmark run ID for profile labeling.

    Returns:
        List of ResourceProfile objects.
    """
    profiles = []
    for assigned_cpus in core_counts:
        if assigned_cpus < 1:
            raise ValueError(f"CPU core counts must be >= 1, got {assigned_cpus}")
        for fraction in capacity_fractions:
            if fraction <= 0:
                raise ValueError(
                    f"CPU capacity fractions must be greater than zero, got {fraction}"
                )
            if fraction > 1:
                continue
            cpu_quota = assigned_cpus * fraction

            if cpu_quota < DOCKER_MIN_CPU_QUOTA:
                raise ValueError(
                    f"CPU profile {assigned_cpus} x {fraction} requests {cpu_quota} CPU, "
                    f"below the minimum distinct Docker CFS quota of {DOCKER_MIN_CPU_QUOTA} CPU"
                )

            idx = len(profiles)
            pct = int(fraction * 100)
            profile_id = f"cpu_{assigned_cpus}c_{pct:03d}"
            label = f"CPUs={assigned_cpus} Fraction={fraction:.2f}"

            profile = ResourceProfile(
                resource_profile_id=profile_id,
                experiment_kind="cpu_matrix_singleton",
                resource_profile_index=idx,
                profile_label=label,
                cpu_limit_cpus=cpu_quota,
                capacity_fraction=fraction,
                assigned_cpu_count=assigned_cpus,
                memory_limit=high_memory_limit,
                memory_swap=high_memory_limit,
                rayon_num_threads=assigned_cpus,
                cpuset_role="cpu_matrix",
                memory_model="docker-cgroup",
                cpu_interpretation="docker-cfs-hard-quota",
                profile_notes=f"CPU matrix: {assigned_cpus} cores @ {fraction:.0%} capacity",
            )
            profiles.append(profile)

    return profiles


def generate_embedded_budget_profiles(
    heap_budgets: List[str],
    core_counts: List[int],
    capacity_fractions: List[float],
    docker_memory_limit: str = "256m",
    run_id: str = "",
) -> List[ResourceProfile]:
    """Generate profiles for deeply embedded memory-budget experiments.

    Signal uses docker-cgroup memory model. Docker supplies CPU affinity/quota
    and a safe Linux container memory envelope. The small memory variable is
    passed to the Rust worker as a docker-cgroup memory limit.

    Signal has no app-heap-budget allocator; all memory constraints are
    applied via docker-cgroup.
    """
    if not validate_memory_string(docker_memory_limit):
        raise ValueError(f"Invalid Docker memory limit '{docker_memory_limit}'")
    docker_memory_bytes = parse_memory_to_bytes(docker_memory_limit)
    if docker_memory_bytes < DOCKER_MIN_MEMORY_BYTES:
        raise ValueError(
            f"Embedded-budget Docker memory limit '{docker_memory_limit}' is below "
            f"the daemon minimum of 6 MiB ({DOCKER_MIN_MEMORY_BYTES} bytes)"
        )

    profiles = []
    for heap_budget in heap_budgets:
        if not validate_memory_string(heap_budget):
            raise ValueError(f"Invalid docker-cgroup memory budget '{heap_budget}'")
        heap_budget_bytes = parse_memory_to_bytes(heap_budget)
        if heap_budget_bytes <= 0:
            raise ValueError(f"Invalid docker-cgroup memory budget '{heap_budget}'")
        if heap_budget_bytes >= docker_memory_bytes:
            raise ValueError(
                f"Docker-cgroup memory budget '{heap_budget}' must be below the safe "
                f"Docker memory limit '{docker_memory_limit}'"
            )

        for assigned_cpus in core_counts:
            if assigned_cpus < 1:
                raise ValueError(f"Embedded CPU core counts must be >= 1, got {assigned_cpus}")
            for fraction in capacity_fractions:
                if fraction <= 0:
                    raise ValueError(
                        f"Embedded CPU capacity fractions must be greater than zero, got {fraction}"
                    )
                if fraction > 1:
                    continue
                cpu_quota = assigned_cpus * fraction
                if cpu_quota < DOCKER_MIN_CPU_QUOTA:
                    raise ValueError(
                        f"Embedded CPU profile {assigned_cpus} x {fraction} requests "
                        f"{cpu_quota} CPU, below Docker CFS minimum {DOCKER_MIN_CPU_QUOTA}"
                    )

                idx = len(profiles)
                pct = int(round(fraction * 100))
                mem_key = heap_budget.lower()
                profile_id = f"embedded_mem_{mem_key}_cpu_{assigned_cpus}c_{pct:03d}"
                label = (
                    f"DockerMem={heap_budget} SafeDockerMem={docker_memory_limit} "
                    f"CPUs={assigned_cpus} Fraction={fraction:.2f}"
                )

                profiles.append(ResourceProfile(
                    resource_profile_id=profile_id,
                    experiment_kind="embedded_budget_singleton",
                    resource_profile_index=idx,
                    profile_label=label,
                    cpu_limit_cpus=cpu_quota,
                    capacity_fraction=fraction,
                    assigned_cpu_count=assigned_cpus,
                    memory_limit=heap_budget,
                    memory_swap=heap_budget,
                    rayon_num_threads=assigned_cpus,
                    cpuset_role="embedded_budget",
                    memory_model="docker-cgroup",
                    docker_memory_limit=docker_memory_limit,
                    cpu_interpretation="docker-cfs-hard-quota",
                    profile_notes=(
                        f"Embedded docker-cgroup memory budget: {heap_budget} synthetic memory, "
                        f"Docker memory held at safe Linux/container limit {docker_memory_limit}; "
                        f"CPU {assigned_cpus} cores @ {fraction:.0%} via Docker"
                    ),
                ))

    return profiles


def generate_parallel_ram_sweep_profiles(
    heap_budgets: Optional[List[str]] = None,
    docker_memory_limit: str = "4g",
    assigned_cpu_count: int = 1,
    run_id: str = "",
) -> List[ResourceProfile]:
    """Generate 10 parallel docker-cgroup memory profiles for RAM sweep.

    Each profile tests a single Docker memory limit. Signal uses docker-cgroup
    (no app-heap-budget allocator). CPU is non-limiting: one core per worker
    with capacity 1.0.

    The highest-valued profile (1 GiB = largest memory limit) is marked
    as the group creator.

    Args:
        heap_budgets: List of 10 memory limit strings for docker-cgroup.
        docker_memory_limit: Safe Docker memory limit for reference (unused as wall).
        assigned_cpu_count: CPU cores assigned to each profiled worker.
        run_id: Benchmark run ID for profile labeling.

    Returns:
        List of 10 ResourceProfile objects, ordered by increasing limit.
    """
    if heap_budgets is None:
        heap_budgets = ["32k", "64k", "128k", "512k", "1m", "2m", "8m", "32m", "256m", "1g"]
    if len(heap_budgets) != 10:
        raise ValueError(f"Parallel RAM sweep requires exactly 10 memory limits, got {len(heap_budgets)}")

    if not validate_memory_string(docker_memory_limit):
        raise ValueError(f"Invalid Docker memory limit '{docker_memory_limit}'")
    docker_memory_bytes = parse_memory_to_bytes(docker_memory_limit)
    if docker_memory_bytes < DOCKER_MIN_MEMORY_BYTES:
        raise ValueError(
            f"RAM sweep Docker memory limit '{docker_memory_limit}' is below "
            f"the daemon minimum of 6 MiB"
        )

    mem_bytes_list = []
    for mv in heap_budgets:
        if not validate_memory_string(mv):
            raise ValueError(f"Invalid docker-cgroup memory limit '{mv}'")
        mbytes = parse_memory_to_bytes(mv)
        if mbytes <= 0:
            raise ValueError(f"Invalid docker-cgroup memory limit '{mv}'")
        if mbytes >= docker_memory_bytes:
            raise ValueError(
                f"Docker memory limit '{mv}' must be below max control '{docker_memory_limit}'"
            )
        mem_bytes_list.append((mv, mbytes))

    max_idx = len(mem_bytes_list) - 1
    profiles = []
    for idx, (mem_limit, mem_bytes) in enumerate(mem_bytes_list):
        is_group_creator = (idx == max_idx)
        profile_id = f"ram_docker_mem_{mem_limit.lower()}"
        label = f"DockerMem={mem_limit} CPU=1c"

        profiles.append(ResourceProfile(
            resource_profile_id=profile_id,
            experiment_kind="ram_sweep",
            resource_profile_index=idx,
            profile_label=label,
            cpu_limit_cpus=1.0,
            capacity_fraction=1.0,
            assigned_cpu_count=assigned_cpu_count,
            memory_limit=mem_limit,
            memory_swap=mem_limit,
            rayon_num_threads=assigned_cpu_count,
            cpuset_role="ram_sweep",
            memory_model="docker-cgroup",
            docker_memory_limit=mem_limit,
            sweep_kind="ram_sweep",
            app_heap_interpretation="",
            cpu_interpretation="CPU is non-limiting (1 core at 100% capacity)",
            group_creator=is_group_creator,
            group_creator_reason=(
                f"highest-valued profiled worker ({mem_limit} docker-cgroup memory)"
                if is_group_creator else ""
            ),
            profile_notes=(
                f"Parallel RAM sweep: {mem_limit} docker-cgroup memory limit, "
                f"CPU abundant at {assigned_cpu_count} cores"
            ),
        ))

    return profiles


def generate_parallel_cpu_sweep_profiles(
    cpu_fractions: Optional[List[float]] = None,
    app_heap_budget: str = "64g",
    docker_memory_limit: str = "72g",
    assigned_cpu_count: int = 1,
    run_id: str = "",
) -> List[ResourceProfile]:
    """Generate 10 parallel CPU quota profiles for CPU sweep.

    Each profile tests a single Docker CPU fraction.
    Docker memory is held absurdly high (64 GiB default) so memory
    cannot plausibly be the limiting factor.

    Signal uses docker-cgroup memory model (no app-heap-budget allocator).

    The highest-valued profile (1.00 CPU fraction) is marked as the
    group creator.

    CPU period starts at 100000 us (standard Docker/CFS default) and may
    scale up to Docker's 1000000 us CFS period limit so tiny fractions still
    have distinct quotas.

    Args:
        cpu_fractions: List of 10 CPU fraction values.
        app_heap_budget: Absurdly high docker-cgroup memory limit string
            (kept for API compatibility; Signal uses docker-cgroup).
        docker_memory_limit: Docker memory envelope (above the working memory).
        assigned_cpu_count: CPU cores assigned to each profiled worker.
        run_id: Benchmark run ID for profile labeling.

    Returns:
        List of 10 ResourceProfile objects, ordered by decreasing fraction.
    """
    if cpu_fractions is None:
        cpu_fractions = [1.00, 0.75, 0.50, 0.25, 0.10, 0.05, 0.04, 0.03, 0.02, 0.01]
    if len(cpu_fractions) != 10:
        raise ValueError(f"Parallel CPU sweep requires exactly 10 fractions, got {len(cpu_fractions)}")

    for fraction in cpu_fractions:
        if fraction < DOCKER_HARD_QUOTA_CPU_FLOOR:
            raise ValueError(
                f"Requested Docker hard-quota CPU fraction {fraction} is below "
                f"the supported validated floor {DOCKER_HARD_QUOTA_CPU_FLOOR} "
                f"on this benchmark configuration. Use >={DOCKER_HARD_QUOTA_CPU_FLOOR}, "
                "or implement a separate synthetic sub-floor slowdown model."
            )

    if len(set(cpu_fractions)) != len(cpu_fractions):
        raise ValueError(
            "All CPU fractions in the hard-quota sweep must be distinct. "
            f"Got duplicates in: {cpu_fractions}"
        )

    if not validate_memory_string(app_heap_budget):
        raise ValueError(f"Invalid docker-cgroup memory limit '{app_heap_budget}'")
    mem_bytes = parse_memory_to_bytes(app_heap_budget)
    if mem_bytes <= 0:
        raise ValueError(f"Invalid docker-cgroup memory limit '{app_heap_budget}'")

    if not validate_memory_string(docker_memory_limit):
        raise ValueError(f"Invalid Docker memory limit '{docker_memory_limit}'")

    cpu_period_us = 100000

    min_quota_us = min(int(round(cpu_period_us * fraction)) for fraction in cpu_fractions)
    if min_quota_us < int(CGROUP_V2_CPU_QUOTA_HEADROOM_FACTOR * CGROUP_V2_CPU_MAX_QUOTA_FLOOR_US):
        scale = int(
            (CGROUP_V2_CPU_QUOTA_HEADROOM_FACTOR * CGROUP_V2_CPU_MAX_QUOTA_FLOOR_US + min_quota_us - 1)
            / min_quota_us
        )
        cpu_period_us = cpu_period_us * scale
        if cpu_period_us > DOCKER_CFS_MAX_PERIOD_US:
            raise ValueError(
                "CPU sweep fractions require Docker CFS cpu_period_us="
                f"{cpu_period_us}, above Docker's supported maximum "
                f"{DOCKER_CFS_MAX_PERIOD_US}. Increase the smallest CPU "
                "fraction or lower CGROUP_V2_CPU_QUOTA_HEADROOM_FACTOR."
            )
        import sys as _sys
        print(
            f"[resource_profiles] CPU period auto-scaled from 100000 us to {cpu_period_us} us "
            f"(x{scale}) to keep all quotas >= {int(CGROUP_V2_CPU_QUOTA_HEADROOM_FACTOR * CGROUP_V2_CPU_MAX_QUOTA_FLOOR_US)} us "
            f"(cgroup v2 cpu.max floor is {CGROUP_V2_CPU_MAX_QUOTA_FLOOR_US} us)",
            file=_sys.stderr,
        )

    profiles = []
    for idx, fraction in enumerate(cpu_fractions):
        if fraction <= 0 or fraction > 1.0:
            raise ValueError(f"CPU fraction must be in (0, 1.0], got {fraction}")
        cpu_quota = assigned_cpu_count * fraction
        effective_min_cpu_quota = max(DOCKER_MIN_CPU_QUOTA * 0.01, 0.00001)
        if cpu_quota < effective_min_cpu_quota:
            raise ValueError(
                f"CPU profile fraction {fraction} requests {cpu_quota} CPU, "
                f"below effective Docker CFS minimum {effective_min_cpu_quota}"
            )

        cpu_quota_us = int(round(cpu_period_us * fraction))
        is_group_creator = (idx == 0)
        profile_id = f"cpu_quota_{str(fraction).replace('.', 'p')}"
        label = f"CPUFraction={fraction:.4f} DockerMem={app_heap_budget}"

        profiles.append(ResourceProfile(
            resource_profile_id=profile_id,
            experiment_kind="cpu_quota_sweep",
            resource_profile_index=idx,
            profile_label=label,
            cpu_limit_cpus=cpu_quota,
            capacity_fraction=fraction,
            assigned_cpu_count=assigned_cpu_count,
            memory_limit=docker_memory_limit,
            memory_swap=docker_memory_limit,
            rayon_num_threads=assigned_cpu_count,
            cpuset_role="cpu_quota_sweep",
            memory_model="docker-cgroup",
            docker_memory_limit=docker_memory_limit,
            sweep_kind="cpu_quota_sweep",
            app_heap_interpretation="",
            cpu_interpretation="docker-cfs-hard-quota",
            cpu_period_us=cpu_period_us,
            cpu_quota_us=cpu_quota_us,
            group_creator=is_group_creator,
            group_creator_reason=(
                f"highest-valued profiled worker ({fraction:.4f} CPU fraction)"
                if is_group_creator else ""
            ),
            profile_notes=(
                f"Parallel CPU quota sweep: {fraction:.4f} fraction, "
                f"docker-cgroup memory held at safe non-limiting {app_heap_budget}; "
                f"Docker CPU period={cpu_period_us}us quota={cpu_quota_us}us"
            ),
        ))

    return profiles


def generate_resource_profile_id(experiment_kind: str, index: int, key: str) -> str:
    return f"{experiment_kind}_{key}_{index}"


def validate_memory_string(mem_str: str) -> bool:
    if not mem_str:
        return True

    import re
    pattern = r'^\d+[bkmg]$'
    return bool(re.match(pattern, mem_str.lower()))


def parse_memory_to_bytes(mem_str: str) -> int:
    if not mem_str:
        return -1

    import re
    match = re.match(r'^(\d+)([bkmg])$', mem_str.lower())
    if not match:
        return -1

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {
        'b': 1,
        'k': 1024,
        'm': 1024 * 1024,
        'g': 1024 * 1024 * 1024,
    }

    return value * multipliers.get(unit, 1)


def profile_to_compose_dict(profile: ResourceProfile) -> Dict:
    result: Dict = {}

    if profile.cpuset_cpus:
        result["cpuset"] = profile.cpuset_cpus

    if profile.cpu_quota_us is not None and profile.cpu_period_us != 100000:
        result["cpu_quota"] = str(int(profile.cpu_quota_us))
        result["cpu_period"] = str(int(profile.cpu_period_us))
    elif profile.cpu_limit_cpus is not None:
        result["cpus"] = str(profile.cpu_limit_cpus)

    if profile.memory_limit:
        result["mem_limit"] = profile.memory_limit

    if profile.memory_swap:
        result["memswap_limit"] = profile.memory_swap

    return result


def select_profile_by_index(
    profiles: List[ResourceProfile],
    profile_index: int,
) -> ResourceProfile:
    if profile_index < 0 or profile_index >= len(profiles):
        raise ValueError(
            f"Resource profile index {profile_index} is out of range "
            f"(valid: 0..{len(profiles) - 1}, have {len(profiles)} profiles)"
        )

    for p in profiles:
        p.selected_for_this_run = False

    profile = profiles[profile_index]
    profile.selected_for_this_run = True
    return profile


def select_profile_by_id(
    profiles: List[ResourceProfile],
    profile_id: str,
) -> ResourceProfile:
    for p in profiles:
        p.selected_for_this_run = False

    for p in profiles:
        if p.resource_profile_id == profile_id:
            p.selected_for_this_run = True
            return p

    known_ids = [p.resource_profile_id for p in profiles]
    raise ValueError(
        f"Unknown resource profile ID '{profile_id}'. "
        f"Known IDs: {', '.join(known_ids)}"
    )


def select_profile(
    profiles: List[ResourceProfile],
    profile_index: Optional[int] = None,
    profile_id: Optional[str] = None,
    profiled_singleton_count: int = 1,
) -> ResourceProfile:
    if profiled_singleton_count == 1:
        if profile_index is None and profile_id is None:
            raise ValueError(
                "Production threshold mode requires --resource-profile-index "
                "or --resource-profile-id when --profiled-singleton-count=1. "
                "Use --profiled-singleton-count > 1 for multiplexed stress mode."
            )

    if profile_index is not None and profile_id is not None:
        idx_profile = profiles[profile_index] if 0 <= profile_index < len(profiles) else None
        if idx_profile is None or idx_profile.resource_profile_id != profile_id:
            raise ValueError(
                f"Profile index {profile_index} (ID: {idx_profile.resource_profile_id if idx_profile else 'out of range'}) "
                f"does not match profile ID '{profile_id}'"
            )

    if profile_index is not None:
        return select_profile_by_index(profiles, profile_index)

    if profile_id is not None:
        return select_profile_by_id(profiles, profile_id)

    if profiled_singleton_count > 1:
        return profiles[0]

    raise ValueError("No resource profile selected")


def select_all_profiles(profiles: List[ResourceProfile]) -> List[ResourceProfile]:
    for p in profiles:
        p.selected_for_this_run = True
    return profiles


def select_profiles_for_parallel_sweep(
    profiles: List[ResourceProfile],
    profiled_singleton_count: int,
) -> List[ResourceProfile]:
    if profiled_singleton_count > len(profiles):
        assigned = []
        for i in range(profiled_singleton_count):
            p = profiles[i % len(profiles)]
            p.selected_for_this_run = True
            assigned.append(p)
        return assigned

    for p in profiles:
        p.selected_for_this_run = False

    for i in range(profiled_singleton_count):
        profiles[i].selected_for_this_run = True

    return profiles[:profiled_singleton_count]


def get_group_creator_profile(profiles: List[ResourceProfile]) -> Optional[ResourceProfile]:
    for p in profiles:
        if p.group_creator:
            return p
    return None


def get_group_creator_client_index(profiles: List[ResourceProfile]) -> Optional[int]:
    gc = get_group_creator_profile(profiles)
    if gc is not None:
        return gc.resource_profile_index
    return None


def get_selected_profile(profiles: List[ResourceProfile]) -> Optional[ResourceProfile]:
    for p in profiles:
        if p.selected_for_this_run:
            return p
    return None


def get_selected_profile_index(profiles: List[ResourceProfile]) -> Optional[int]:
    sp = get_selected_profile(profiles)
    if sp is not None:
        return sp.resource_profile_index
    return None
