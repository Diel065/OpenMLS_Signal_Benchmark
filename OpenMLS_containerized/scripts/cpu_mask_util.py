"""
CPU mask/cpuset utility functions.

Provides functions for converting between CPU lists, integer bitmasks,
hex mask strings, and Docker cpuset strings.

All conversions are stable, deterministic, and handle edge cases.
"""

from typing import List, Optional, Set, Tuple
import re


def cpu_list_to_mask(cpu_list: List[int]) -> int:
    """Convert a sorted list of CPU IDs to an integer bitmask.

    CPU 0 corresponds to bit 0 (LSB).

    Example:
        cpu_list_to_mask([0, 2, 4]) -> 0b10101 -> 21

    Raises ValueError for negative or duplicate CPU IDs.
    """
    mask = 0
    seen: Set[int] = set()
    for cpu in cpu_list:
        if cpu < 0:
            raise ValueError(f"Negative CPU ID: {cpu}")
        if cpu > 1023:
            raise ValueError(f"CPU ID too large for mask: {cpu}")
        if cpu in seen:
            raise ValueError(f"Duplicate CPU ID: {cpu}")
        seen.add(cpu)
        mask |= 1 << cpu
    return mask


def mask_to_cpu_list(mask: int) -> List[int]:
    """Convert an integer bitmask to a sorted list of CPU IDs.

    Example:
        mask_to_cpu_list(0b10101) -> [0, 2, 4]
    """
    cpus: List[int] = []
    bit = 0
    while mask:
        if mask & 1:
            cpus.append(bit)
        mask >>= 1
        bit += 1
    return cpus


def cpu_list_to_docker_cpuset(cpus: List[int]) -> str:
    """Convert a sorted list of CPU IDs to a Docker cpuset string.

    Docker accepts individual CPUs and ranges separated by commas.
    Consecutive CPUs are collapsed into ranges.

    Example:
        cpu_list_to_docker_cpuset([0, 1, 2, 4, 5]) -> "0-2,4-5"

    Returns empty string for empty list.
    """
    if not cpus:
        return ""

    sorted_cpus = sorted(cpus)
    ranges: List[str] = []
    start = sorted_cpus[0]
    end = sorted_cpus[0]

    for cpu in sorted_cpus[1:]:
        if cpu == end + 1:
            end = cpu
        else:
            ranges.append(f"{start}" if start == end else f"{start}-{end}")
            start = cpu
            end = cpu
    ranges.append(f"{start}" if start == end else f"{start}-{end}")
    return ",".join(ranges)


def docker_cpuset_to_cpu_list(cpuset_str: str) -> List[int]:
    """Parse a Docker cpuset string into a sorted list of CPU IDs.

    Handles individual CPUs and ranges. Whitespace is stripped.
    Returns empty list for empty or "0" (empty cpuset) string.

    Example:
        docker_cpuset_to_cpu_list("0-2,4,5") -> [0, 1, 2, 4, 5]
    """
    if not cpuset_str or cpuset_str.strip() == "":
        return []

    cpuset_str = cpuset_str.strip()
    cpus: Set[int] = set()

    for part in cpuset_str.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            try:
                low_s, high_s = part.split("-", 1)
                low = int(low_s.strip())
                high = int(high_s.strip())
                if low < 0 or high < 0:
                    raise ValueError(f"Negative CPU in range: {part}")
                if low > high:
                    raise ValueError(f"Inverted range: {part}")
                for cpu in range(low, high + 1):
                    cpus.add(cpu)
            except ValueError as e:
                raise ValueError(f"Invalid CPU range '{part}': {e}")
        else:
            try:
                cpus.add(int(part))
            except ValueError:
                raise ValueError(f"Invalid CPU ID: {part}")

    return sorted(cpus)


def mask_to_hex(mask: int) -> str:
    """Convert an integer bitmask to a hex string with '0x' prefix.

    Example:
        mask_to_hex(0b10101) -> "0x15"
    """
    return hex(mask)


def hex_to_mask(hex_str: str) -> int:
    """Convert a hex string (with or without '0x' prefix) to integer mask."""
    return int(hex_str, 16)


def masks_overlap(mask_a: int, mask_b: int) -> bool:
    """Check if two CPU masks overlap (share any CPU).

    Example:
        masks_overlap(0b0010, 0b0110) -> True (share CPU 1)
        masks_overlap(0b0010, 0b1100) -> False
    """
    return (mask_a & mask_b) != 0


def union_masks(*masks: int) -> int:
    """Compute the bitwise OR of multiple masks."""
    result = 0
    for m in masks:
        result |= m
    return result


def complement_mask(mask: int, online_mask: int) -> int:
    """Compute background mask = online_mask & ~mask."""
    return online_mask & (~mask)


def mask_popcount(mask: int) -> int:
    """Count the number of CPUs set in a mask."""
    return mask.bit_count()


def validate_cpu_list(cpus: List[int], max_cpu_id: int = 1023) -> None:
    """Validate a CPU list. Raises ValueError on invalid CPUs."""
    if not cpus:
        raise ValueError("Empty CPU list")
    for cpu in cpus:
        if not isinstance(cpu, int):
            raise ValueError(f"Non-integer CPU ID: {cpu}")
        if cpu < 0:
            raise ValueError(f"Negative CPU ID: {cpu}")
        if cpu > max_cpu_id:
            raise ValueError(f"CPU ID {cpu} exceeds max {max_cpu_id}")
    if len(set(cpus)) != len(cpus):
        raise ValueError("Duplicate CPU IDs in list")


def format_cpuset_for_display(cpus: List[int]) -> str:
    """Format a CPU list for human display (space-separated)."""
    return " ".join(str(c) for c in sorted(cpus))


def cpu_count_from_cpuset(cpuset_str: str) -> int:
    """Count the number of CPUs in a Docker cpuset string."""
    return len(docker_cpuset_to_cpu_list(cpuset_str))


def ensure_non_overlapping(assignments: List[Tuple[str, List[int]]]) -> None:
    """Verify that multiple CPU assignments do not overlap.

    Args:
        assignments: List of (label, cpu_list) tuples.

    Raises ValueError if any overlap is detected.
    """
    assigned: Set[int] = set()
    for label, cpus in assignments:
        for cpu in cpus:
            if cpu in assigned:
                overlapping_label = None
                for prev_label, prev_cpus in assignments:
                    if cpu in prev_cpus and prev_label != label:
                        overlapping_label = prev_label
                        break
                raise ValueError(
                    f"CPU {cpu} assigned to both '{label}' and '{overlapping_label}'"
                )
            assigned.add(cpu)


def sublist_to_ranges(lst: List[int]) -> str:
    """Convert a sorted list of ints to a range string like "0-3,5,7-9"."""
    return cpu_list_to_docker_cpuset(lst)
