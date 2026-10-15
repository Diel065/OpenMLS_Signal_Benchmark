from typing import List, Set


def cpu_list_to_mask(cpu_list: List[int]) -> int:
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
    cpus: List[int] = []
    bit = 0
    while mask:
        if mask & 1:
            cpus.append(bit)
        mask >>= 1
        bit += 1
    return cpus


def cpu_list_to_docker_cpuset(cpus: List[int]) -> str:
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


def masks_overlap(mask_a: int, mask_b: int) -> bool:
    return (mask_a & mask_b) != 0


def complement_mask(mask: int, online_mask: int) -> int:
    return online_mask & (~mask)


