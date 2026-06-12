"""
Resource profile generation for RAM sweep and CPU matrix experiments.

Generates structured resource profiles for:
  - ram_sweep_singleton: Sweep memory limits with abundant CPU
  - cpu_matrix_singleton: Matrix of (core_count, capacity_fraction) combinations

Supports explicit profile selection via index or ID for single-profile
scientific threshold experiments.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import uuid


@dataclass
class ResourceProfile:
    """A single resource profile for a profiled singleton container."""
    resource_profile_id: str
    experiment_kind: str  # "ram_sweep_singleton" or "cpu_matrix_singleton"
    profile_label: str
    resource_profile_index: int = -1
    cpu_limit_cpus: Optional[float] = None  # Docker cpus quota
    capacity_fraction: Optional[float] = None  # 0.25, 0.50, etc.
    assigned_cpu_count: int = 1
    memory_limit: Optional[str] = None  # e.g., "128m"
    memory_swap: Optional[str] = None
    rayon_num_threads: int = 1
    cpuset_cpus: Optional[str] = None  # Docker cpuset string
    cpuset_mask_hex: Optional[str] = None
    profile_notes: str = ""
    selected_for_this_run: bool = False
    cpuset_role: str = ""

    def to_dict(self) -> Dict:
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
            "rayon_num_threads": self.rayon_num_threads,
            "cpuset_cpus": self.cpuset_cpus or "",
            "cpuset_mask_hex": self.cpuset_mask_hex or "",
            "cpuset_role": self.cpuset_role or "",
            "profile_notes": self.profile_notes,
        }

    def to_csv_row(self) -> Dict:
        """Return a dict suitable for CSV writing, with empty strings for None."""
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
    if len(ram_values) > 6:
        raise ValueError(f"RAM sweep supports at most 6 values, got {len(ram_values)}")

    profiles = []
    for idx, ram_value in enumerate(ram_values):
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
        for fraction in capacity_fractions:
            cpu_quota = assigned_cpus * fraction

            if cpu_quota > assigned_cpus:
                continue

            if cpu_quota < 0.01:
                continue

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
                profile_notes=f"CPU matrix: {assigned_cpus} cores @ {fraction:.0%} capacity",
            )
            profiles.append(profile)

    return profiles


def generate_resource_profile_id(experiment_kind: str, index: int, key: str) -> str:
    """Generate a unique resource profile ID."""
    return f"{experiment_kind}_{key}_{index}"


def validate_memory_string(mem_str: str) -> bool:
    """Validate a Docker memory limit string like '128m' or '1g'.

    Returns True if valid, False otherwise.
    """
    if not mem_str:
        return True

    import re
    pattern = r'^\d+[bkmg]$'
    return bool(re.match(pattern, mem_str.lower()))


def parse_memory_to_bytes(mem_str: str) -> int:
    """Parse a Docker memory string to bytes.

    Supports: b (bytes), k (kilobytes), m (megabytes), g (gigabytes).
    Returns -1 for unparseable strings.
    """
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
    """Convert a ResourceProfile to a dict suitable for docker-compose generation.

    Returns a dict with keys that can be directly merged into a compose service definition.
    Fields that are None are omitted from the result.
    """
    result: Dict = {}

    if profile.cpuset_cpus:
        result["cpuset"] = profile.cpuset_cpus

    if profile.cpu_limit_cpus is not None:
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
    """Select a single resource profile by its zero-based index.

    Args:
        profiles: List of all available resource profiles.
        profile_index: Zero-based index into the list.

    Returns:
        The selected ResourceProfile with selected_for_this_run set to True.

    Raises:
        ValueError: If profile_index is out of range.
    """
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
    """Select a single resource profile by its resource_profile_id.

    Args:
        profiles: List of all available resource profiles.
        profile_id: The resource_profile_id to match.

    Returns:
        The selected ResourceProfile with selected_for_this_run set to True.

    Raises:
        ValueError: If no profile matches the given ID.
    """
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
    """Select a single resource profile for a threshold experiment.

    In production threshold mode (profiled_singleton_count == 1), exactly
    one profile must be selected. Fails if neither index nor ID is provided.

    Args:
        profiles: List of all available resource profiles.
        profile_index: Optional zero-based index.
        profile_id: Optional resource_profile_id string.
        profiled_singleton_count: Number of profiled singletons.

    Returns:
        The selected ResourceProfile.

    Raises:
        ValueError: If selection is ambiguous, mismatched, or missing.
    """
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


def get_selected_profile(profiles: List[ResourceProfile]) -> Optional[ResourceProfile]:
    """Get the currently selected profile from a list, if any."""
    for p in profiles:
        if p.selected_for_this_run:
            return p
    return None


def get_selected_profile_index(profiles: List[ResourceProfile]) -> Optional[int]:
    """Get the index of the selected profile, if any."""
    sp = get_selected_profile(profiles)
    if sp is not None:
        return sp.resource_profile_index
    return None
