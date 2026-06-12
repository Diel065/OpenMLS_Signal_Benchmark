"""
Preflight checks for CPU affinity validation.

Verifies that Docker applied the expected cpusets, probes /proc for actual
CPU affinity, and checks that profiled CPUs are not overlapped by background
containers. Hard-fails on any violation that would compromise the experiment.
"""

import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Set, Tuple


def run_docker_inspect_cpuset(container_name: str) -> Optional[str]:
    """Get the HostConfig.CpusetCpus for a container via docker inspect.

    Returns the cpuset string or None if the container is not found.
    """
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.HostConfig.CpusetCpus}}", container_name],
            capture_output=True, text=True, timeout=10,
        )
        cpuset = result.stdout.strip()
        if "<no value>" in cpuset or not cpuset:
            return ""
        return cpuset
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return None


def run_docker_inspect_pid(container_name: str) -> Optional[int]:
    """Get the host PID for a container."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Pid}}", container_name],
            capture_output=True, text=True, timeout=10,
        )
        pid_str = result.stdout.strip()
        if pid_str and pid_str.isdigit():
            return int(pid_str)
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return None


def read_proc_cpus_allowed_list(pid: int) -> Optional[str]:
    """Read Cpus_allowed_list from /proc/<pid>/status."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("Cpus_allowed_list:"):
                    return line.split(":", 1)[1].strip()
    except (IOError, OSError, PermissionError):
        return None
    return None


def read_proc_task_cpus_allowed_lists(pid: int) -> Dict[int, str]:
    """Read Cpus_allowed_list for all threads of a process.

    Returns dict mapping tid -> cpus_allowed_list string.
    """
    results: Dict[int, str] = {}
    task_dir = f"/proc/{pid}/task"
    if not os.path.exists(task_dir):
        return results

    for tid in os.listdir(task_dir):
        try:
            tid_int = int(tid)
            status_path = f"{task_dir}/{tid}/status"
            if os.path.exists(status_path):
                with open(status_path) as f:
                    for line in f:
                        if line.startswith("Cpus_allowed_list:"):
                            cpus_str = line.split(":", 1)[1].strip()
                            results[tid_int] = cpus_str
                            break
        except (ValueError, IOError, OSError, PermissionError):
            continue

    return results


def get_container_env_var(container_name: str, var_name: str) -> Optional[str]:
    """Get an environment variable value from a container via docker inspect."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", f"{{{{range .Config.Env}}}}{{{{.}}}}{{{{println}}}}{{{{end}}}}", container_name],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.strip().split("\n"):
            if line.startswith(f"{var_name}="):
                return line.split("=", 1)[1]
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return None


def check_host_processes_on_cpus(cpus: List[int]) -> List[Dict[str, Any]]:
    """Check for unrelated host processes running on specified CPUs.

    Uses ps to list processes and their current CPU (psr).
    Excludes kernel threads (PID 2, kthreadd children) and known system processes.

    Returns list of warnings for each observed process.
    """
    warnings: List[Dict[str, Any]] = []
    try:
        result = subprocess.run(
            ["ps", "-eLo", "pid,tid,psr,comm,args"],
            capture_output=True, text=True, timeout=10,
        )
        lines = result.stdout.strip().split("\n")[1:]
        for line in lines:
            parts = line.strip().split(None, 4)
            if len(parts) < 4:
                continue
            try:
                psr = int(parts[2])
            except ValueError:
                continue

            if psr in cpus:
                pid = parts[0]
                comm = parts[3]
                warnings.append({
                    "pid": pid,
                    "comm": comm,
                    "psr": psr,
                    "message": f"Host process PID={pid} comm={comm} observed on profiled CPU {psr}",
                })
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        pass

    return warnings


def parse_cpuset_to_set(cpuset_str: str) -> Set[int]:
    """Parse a cpuset string into a set of CPU IDs."""
    if not cpuset_str or cpuset_str.strip() == "":
        return set()

    try:
        from .cpu_mask_util import docker_cpuset_to_cpu_list
    except ImportError:
        from cpu_mask_util import docker_cpuset_to_cpu_list
    return set(docker_cpuset_to_cpu_list(cpuset_str))


def run_preflight(
    run_id: str,
    profiled_containers: List[Dict[str, Any]],
    background_containers: List[Dict[str, Any]],
    affinity_plan: Any,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Run all preflight checks and return results.

    Args:
        run_id: Benchmark run ID.
        profiled_containers: List of dicts with keys:
            container_name, container_role, expected_cpuset, expected_rayon_threads
        background_containers: List of dicts with keys:
            container_name, container_role
        affinity_plan: The AffinityPlan object.

    Returns:
        Tuple of (results_list, all_passed).
        results_list is a list of preflight check dicts.
        all_passed is True if no FAIL statuses exist.
    """
    results: List[Dict[str, Any]] = []
    has_fail = False
    profiled_cpu_set: Set[int] = set()

    for pc in profiled_containers:
        profiled_cpu_set.update(parse_cpuset_to_set(pc.get("expected_cpuset", "")))

    for pc in profiled_containers:
        container_name = pc["container_name"]
        expected_cpuset = pc.get("expected_cpuset", "")
        expected_rayon = pc.get("expected_rayon_threads")

        docker_cpuset = run_docker_inspect_cpuset(container_name)

        if not docker_cpuset and expected_cpuset:
            results.append({
                "check_name": "profiled_cpuset_missing",
                "container_name": container_name,
                "container_role": pc.get("container_role", "profiled_singleton"),
                "expected_cpuset": expected_cpuset,
                "docker_cpuset": docker_cpuset or "",
                "host_pid": "",
                "proc_cpus_allowed_list": "",
                "thread_cpus_allowed_lists": "",
                "observed_psr_cpus": "",
                "status": "FAIL",
                "message": f"Container {container_name} has no cpuset but expected '{expected_cpuset}'",
            })
            has_fail = True
        elif docker_cpuset != expected_cpuset:
            docker_set = parse_cpuset_to_set(docker_cpuset)
            expected_set = parse_cpuset_to_set(expected_cpuset)
            if docker_set != expected_set:
                results.append({
                    "check_name": "profiled_cpuset_mismatch",
                    "container_name": container_name,
                    "container_role": pc.get("container_role", "profiled_singleton"),
                    "expected_cpuset": expected_cpuset,
                    "docker_cpuset": docker_cpuset or "",
                    "host_pid": "",
                    "proc_cpus_allowed_list": "",
                    "thread_cpus_allowed_lists": "",
                    "observed_psr_cpus": "",
                    "status": "FAIL",
                    "message": f"Docker cpuset '{docker_cpuset}' differs from expected '{expected_cpuset}'",
                })
                has_fail = True
        else:
            results.append({
                "check_name": "profiled_cpuset_match",
                "container_name": container_name,
                "container_role": pc.get("container_role", "profiled_singleton"),
                "expected_cpuset": expected_cpuset,
                "docker_cpuset": docker_cpuset or "",
                "host_pid": "",
                "proc_cpus_allowed_list": "",
                "thread_cpus_allowed_lists": "",
                "observed_psr_cpus": "",
                "status": "PASS",
                "message": f"Docker cpuset matches expected '{expected_cpuset}'",
            })

        if expected_rayon is not None:
            actual_rayon = get_container_env_var(container_name, "RAYON_NUM_THREADS")
            if actual_rayon is not None and str(actual_rayon) != str(expected_rayon):
                results.append({
                    "check_name": "rayon_num_threads_mismatch",
                    "container_name": container_name,
                    "container_role": pc.get("container_role", "profiled_singleton"),
                    "expected_cpuset": expected_cpuset,
                    "docker_cpuset": docker_cpuset or "",
                    "host_pid": "",
                    "proc_cpus_allowed_list": "",
                    "thread_cpus_allowed_lists": "",
                    "observed_psr_cpus": "",
                    "status": "FAIL",
                    "message": f"RAYON_NUM_THREADS={actual_rayon} but expected {expected_rayon}",
                })
                has_fail = True

        pid = run_docker_inspect_pid(container_name)
        if pid:
            proc_cpus = read_proc_cpus_allowed_list(pid)
            if proc_cpus:
                proc_set = parse_cpuset_to_set(proc_cpus)

                if expected_cpuset:
                    expected_set_for_pid = parse_cpuset_to_set(expected_cpuset)
                    if not expected_set_for_pid.issubset(proc_set):
                        results.append({
                            "check_name": "proc_cpus_allowed_conflict",
                            "container_name": container_name,
                            "container_role": pc.get("container_role", "profiled_singleton"),
                            "expected_cpuset": expected_cpuset,
                            "docker_cpuset": docker_cpuset or "",
                            "host_pid": str(pid),
                            "proc_cpus_allowed_list": proc_cpus,
                            "thread_cpus_allowed_lists": "",
                            "observed_psr_cpus": "",
                            "status": "FAIL",
                            "message": f"/proc/{pid}/status Cpus_allowed_list={proc_cpus} conflicts with expected {expected_cpuset}",
                        })
                        has_fail = True

            thread_cpus = read_proc_task_cpus_allowed_lists(pid)
            for tid, tcpus in thread_cpus.items():
                if tcpus and expected_cpuset:
                    tset = parse_cpuset_to_set(tcpus)
                    expected_set_for_tid = parse_cpuset_to_set(expected_cpuset)
                    if not expected_set_for_tid.issubset(tset):
                        results.append({
                            "check_name": "thread_cpus_allowed_conflict",
                            "container_name": container_name,
                            "container_role": pc.get("container_role", "profiled_singleton"),
                            "expected_cpuset": expected_cpuset,
                            "docker_cpuset": docker_cpuset or "",
                            "host_pid": str(pid),
                            "proc_cpus_allowed_list": proc_cpus or "",
                            "thread_cpus_allowed_lists": f"tid={tid}:{tcpus}",
                            "observed_psr_cpus": "",
                            "status": "FAIL",
                            "message": f"Thread {tid}: Cpus_allowed_list={tcpus} conflicts with expected {expected_cpuset}",
                        })
                        has_fail = True

    for bc in background_containers:
        container_name = bc["container_name"]
        docker_cpuset = run_docker_inspect_cpuset(container_name)
        if docker_cpuset:
            docker_set = parse_cpuset_to_set(docker_cpuset)
            overlap = docker_set & profiled_cpu_set
            if overlap:
                results.append({
                    "check_name": "background_overlaps_profiled",
                    "container_name": container_name,
                    "container_role": bc.get("container_role", "background"),
                    "expected_cpuset": bc.get("expected_cpuset", ""),
                    "docker_cpuset": docker_cpuset,
                    "host_pid": "",
                    "proc_cpus_allowed_list": "",
                    "thread_cpus_allowed_lists": "",
                    "observed_psr_cpus": "",
                    "status": "FAIL",
                    "message": f"Background container {container_name} cpuset '{docker_cpuset}' overlaps profiled CPUs {sorted(overlap)}",
                })
                has_fail = True
            else:
                results.append({
                    "check_name": "background_cpuset_clean",
                    "container_name": container_name,
                    "container_role": bc.get("container_role", "background"),
                    "expected_cpuset": bc.get("expected_cpuset", ""),
                    "docker_cpuset": docker_cpuset,
                    "host_pid": "",
                    "proc_cpus_allowed_list": "",
                    "thread_cpus_allowed_lists": "",
                    "observed_psr_cpus": "",
                    "status": "PASS",
                    "message": f"Background container {container_name} cpuset has no profiled CPU overlap",
                })

    if profiled_cpu_set:
        host_warnings = check_host_processes_on_cpus(list(profiled_cpu_set))
        for hw in host_warnings:
            results.append({
                "check_name": "host_process_on_profiled_cpu",
                "container_name": "",
                "container_role": "host_process",
                "expected_cpuset": "",
                "docker_cpuset": "",
                "host_pid": hw["pid"],
                "proc_cpus_allowed_list": "",
                "thread_cpus_allowed_lists": "",
                "observed_psr_cpus": str(hw["psr"]),
                "status": "WARN",
                "message": hw["message"],
            })

    if profiled_containers and len(profiled_containers) > 1:
        assigned_sets = []
        for pc in profiled_containers:
            assigned_sets.append((
                pc["container_name"],
                parse_cpuset_to_set(pc.get("expected_cpuset", "")),
            ))
        for i in range(len(assigned_sets)):
            for j in range(i + 1, len(assigned_sets)):
                name_a, set_a = assigned_sets[i]
                name_b, set_b = assigned_sets[j]
                if set_a and set_b and set_a & set_b:
                    results.append({
                        "check_name": "profiled_overlap",
                        "container_name": f"{name_a},{name_b}",
                        "container_role": "profiled_singleton",
                        "expected_cpuset": "",
                        "docker_cpuset": "",
                        "host_pid": "",
                        "proc_cpus_allowed_list": "",
                        "thread_cpus_allowed_lists": "",
                        "observed_psr_cpus": "",
                        "status": "FAIL",
                        "message": f"Profiled containers {name_a} and {name_b} share CPUs {sorted(set_a & set_b)}",
                    })
                    has_fail = True

    return results, not has_fail
