"""
Failure classification and tracking for resource experiments.

Tracks when and where a profiled singleton failed, classifies failures,
and writes worker_failures.csv and run_status.csv.

Includes cross-referencing with events.csv to extract benchmark cursor
context (group size, operation, epoch, phase) when a worker fails.
"""

import csv
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


FAILURE_CLASSES = [
    "completed_successfully",
    "hard_ram_oom_kill",
    "hard_container_exit",
    "cpu_timeout",
    "cpu_starvation_suspected",
    "memory_pressure_no_oom",
    "worker_unreachable",
    "benchmark_protocol_failure",
    "infrastructure_failure",
    "thread_or_process_creation_failure",
    "unknown_failure",
]


@dataclass
class WorkerFailureInfo:
    """Information about a worker failure for classification."""
    worker_id: str
    physical_worker_id: str = ""
    logical_client_id: str = ""
    container_name: str = ""
    container_id: str = ""
    resource_profile_id: str = ""
    experiment_kind: str = ""
    failure_class: str = "unknown_failure"
    failure_timestamp_ns: int = 0
    last_successful_phase: str = ""
    last_successful_operation_family: str = ""
    last_successful_benchmark_operation: str = ""
    last_successful_member_count: int = 0
    last_successful_epoch: int = 0
    current_phase: str = ""
    current_operation_family: str = ""
    current_benchmark_operation: str = ""
    current_member_count: int = 0
    current_epoch: int = 0
    container_exit_code: Optional[int] = None
    container_oom_killed: bool = False
    memory_events_oom: int = 0
    memory_events_oom_kill: int = 0
    max_memory_current: int = 0
    cpu_nr_throttled_delta: int = 0
    cpu_throttled_usec_delta: int = 0
    cpu_throttled_time_fraction: float = 0.0
    diagnostic_log_path: str = ""


def classify_worker_failure(
    info: WorkerFailureInfo,
    oom_events_path: Optional[str] = None,
    resource_samples_path: Optional[str] = None,
    cpu_throttle_threshold_fraction: float = 0.5,
) -> str:
    """Classify a worker failure based on available evidence.

    Returns one of the FAILURE_CLASSES strings.

    Classification rules (in order):
    1. If container was OOM-killed (Docker state or cgroup): hard_ram_oom_kill
    2. If oom_events.jsonl records an OOM kill: hard_ram_oom_kill
    3. If container exited nonzero without OOM evidence: hard_container_exit
    4. If worker is unreachable (health check fails): worker_unreachable
    5. If infrastructure (DS/relay) failed: infrastructure_failure
    6. If benchmark protocol failure: benchmark_protocol_failure
    7. If CPU-starved (timeout, high throttling, no OOM): cpu_starvation_suspected
    8. If memory pressure without OOM: memory_pressure_no_oom
    9. Otherwise: unknown_failure
    """
    failure_class = "unknown_failure"

    oom_killed = info.container_oom_killed
    oom_events_oom_kill = info.memory_events_oom_kill > 0

    if oom_killed or oom_events_oom_kill:
        failure_class = "hard_ram_oom_kill"
    elif info.container_exit_code is not None and info.container_exit_code != 0:
        failure_class = "hard_container_exit"
    elif info.failure_class in ("worker_unreachable", "infrastructure_failure",
                                 "benchmark_protocol_failure"):
        failure_class = info.failure_class
    elif info.cpu_throttled_time_fraction > cpu_throttle_threshold_fraction:
        if info.container_exit_code is None:
            failure_class = "cpu_starvation_suspected"
        else:
            failure_class = "cpu_timeout"
    elif info.memory_events_oom > 0:
        failure_class = "memory_pressure_no_oom"
    else:
        failure_class = "unknown_failure"

    return failure_class


def classify_worker_failure_from_resource_summary(
    resource_summary: Dict[str, Any],
    oom_events_path: Optional[str] = None,
) -> Tuple[str, WorkerFailureInfo]:
    """Classify failure from a resource_summary.csv row dict.

    Returns (failure_class, WorkerFailureInfo).
    """
    info = WorkerFailureInfo(
        worker_id=str(resource_summary.get("worker_id", "")),
        physical_worker_id=str(resource_summary.get("physical_worker_id", "")),
        logical_client_id=str(resource_summary.get("logical_client_id", "")),
        container_name=str(resource_summary.get("container_name", "")),
        resource_profile_id=str(resource_summary.get("resource_profile_id", "")),
        experiment_kind=str(resource_summary.get("experiment_kind", "")),
        container_exit_code=_safe_int(resource_summary.get("last_container_exit_code")),
        container_oom_killed=_safe_bool(resource_summary.get("last_container_oom_killed")),
        memory_events_oom=_safe_int(resource_summary.get("memory_events_oom")),
        memory_events_oom_kill=_safe_int(resource_summary.get("memory_events_oom_kill")),
        max_memory_current=_safe_int(resource_summary.get("max_memory_current")),
        cpu_nr_throttled_delta=_safe_int(resource_summary.get("cpu_nr_throttled_delta")),
        cpu_throttled_usec_delta=_safe_int(resource_summary.get("cpu_throttled_usec_delta")),
        cpu_throttled_time_fraction=_safe_float(resource_summary.get("cpu_throttled_time_fraction")),
    )

    klass = classify_worker_failure(info, oom_events_path)
    info.failure_class = klass
    return klass, info


def build_run_status(
    run_id: str,
    run_mode: str,
    experiment_kind: str,
    run_success: bool,
    worker_failures: List[WorkerFailureInfo],
    resource_experiment: str = "none",
    resource_failure_policy: str = "stop-on-profiled-failure",
    resource_profile_index: int = -1,
    resource_profile_id: str = "",
    preflight_passed: bool = False,
    output_validation_passed: bool = False,
    notes: str = "",
) -> Dict[str, Any]:
    """Build a run_status.csv row dict matching the full corrected schema."""
    first_failure = None
    for wf in worker_failures:
        if wf.failure_class != "completed_successfully":
            if first_failure is None:
                first_failure = wf
            elif wf.failure_timestamp_ns < first_failure.failure_timestamp_ns:
                first_failure = wf

    completed = run_success

    if run_success:
        valid_for_threshold = True
        valid_for_performance = True
        valid_for_churn = False
        run_status = "completed"
    elif first_failure:
        failure_class = first_failure.failure_class
        is_infrastructure = failure_class in (
            "infrastructure_failure", "preflight_failure", "output_validation_failure"
        )
        valid_for_threshold = not is_infrastructure
        valid_for_performance = False
        valid_for_churn = False
        run_status = f"failed_{failure_class}"
    else:
        valid_for_threshold = False
        valid_for_performance = False
        valid_for_churn = False
        run_status = "failed"

    return {
        "run_id": run_id,
        "run_mode": run_mode,
        "resource_experiment": resource_experiment,
        "resource_failure_policy": resource_failure_policy,
        "resource_profile_index": resource_profile_index,
        "resource_profile_id": resource_profile_id,
        "experiment_kind": experiment_kind,
        "run_status": run_status,
        "completed": completed,
        "valid_for_threshold_analysis": valid_for_threshold,
        "valid_for_clean_performance_plots": valid_for_performance,
        "valid_for_churn_recovery_analysis": valid_for_churn,
        "first_failure_timestamp_ns": first_failure.failure_timestamp_ns if first_failure else 0,
        "first_failed_worker_id": first_failure.worker_id if first_failure else "",
        "first_failed_client_id": first_failure.logical_client_id if first_failure else "",
        "first_failure_class": first_failure.failure_class if first_failure else "",
        "first_failure_operation_family": first_failure.current_operation_family if first_failure else "",
        "first_failure_benchmark_operation": first_failure.current_benchmark_operation if first_failure else "",
        "first_failure_member_count": first_failure.current_member_count if first_failure else 0,
        "first_failure_epoch": first_failure.current_epoch if first_failure else 0,
        "last_successful_operation_family": first_failure.last_successful_operation_family if first_failure else "",
        "last_successful_benchmark_operation": first_failure.last_successful_benchmark_operation if first_failure else "",
        "last_successful_member_count": first_failure.last_successful_member_count if first_failure else 0,
        "last_successful_epoch": first_failure.last_successful_epoch if first_failure else 0,
        "preflight_passed": preflight_passed,
        "resource_output_validation_passed": output_validation_passed,
        "notes": notes,
    }


def worker_failure_info_to_dict(info: WorkerFailureInfo) -> Dict[str, Any]:
    """Convert a WorkerFailureInfo to a CSV-safe dict."""
    return {
        "worker_id": info.worker_id,
        "physical_worker_id": info.physical_worker_id,
        "logical_client_id": info.logical_client_id,
        "container_name": info.container_name,
        "container_id": info.container_id,
        "resource_profile_id": info.resource_profile_id,
        "experiment_kind": info.experiment_kind,
        "failure_class": info.failure_class,
        "failure_timestamp_ns": info.failure_timestamp_ns,
        "last_successful_phase": info.last_successful_phase,
        "last_successful_operation_family": info.last_successful_operation_family,
        "last_successful_benchmark_operation": info.last_successful_benchmark_operation,
        "last_successful_member_count": info.last_successful_member_count,
        "last_successful_epoch": info.last_successful_epoch,
        "current_phase": info.current_phase,
        "current_operation_family": info.current_operation_family,
        "current_benchmark_operation": info.current_benchmark_operation,
        "current_member_count": info.current_member_count,
        "current_epoch": info.current_epoch,
        "container_exit_code": info.container_exit_code,
        "container_oom_killed": info.container_oom_killed,
        "memory_events_oom": info.memory_events_oom,
        "memory_events_oom_kill": info.memory_events_oom_kill,
        "max_memory_current": info.max_memory_current,
        "cpu_nr_throttled_delta": info.cpu_nr_throttled_delta,
        "cpu_throttled_usec_delta": info.cpu_throttled_usec_delta,
        "cpu_throttled_time_fraction": info.cpu_throttled_time_fraction,
        "diagnostic_log_path": info.diagnostic_log_path,
    }


def _safe_int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(value)
    except (ValueError, TypeError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def check_oom_events_file(oom_events_path: str, container_name: str) -> bool:
    """Check if oom_events.jsonl contains an OOM kill for a specific container."""
    if not os.path.exists(oom_events_path):
        return False
    try:
        with open(oom_events_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                if event.get("container_name") == container_name:
                    if event.get("event_type") == "oom_kill":
                        return True
    except (json.JSONDecodeError, IOError):
        pass
    return False


def extract_failure_cursors_from_events_csv(
    events_csv_path: str,
) -> Dict[str, Dict[str, Any]]:
    """Extract benchmark cursor context for failed workers from events.csv.

    Reads the events.csv file (aggregated from client-*.jsonl by the Rust
    runner) and collects cursor data for every row that has a non-empty
    failed_worker_id column.

    Returns a dict mapping logical_client_id (the failed_worker_id) to
    a dict with cursor fields:
        benchmark_target_size, benchmark_active_size, benchmark_phase,
        benchmark_operation, benchmark_operation_seq, benchmark_plateau_index,
        group_epoch, member_count, operation_family, failure_class,
        failure_detail, failure_timestamp_ns (from timestamp_ns or ts_unix_ns)
    """
    if not os.path.exists(events_csv_path):
        return {}

    cursors: Dict[str, Dict[str, Any]] = {}
    try:
        with open(events_csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                failed_id = (row.get("failed_worker_id") or "").strip()
                if not failed_id:
                    continue

                ts_field = (
                    row.get("timestamp_ns")
                    or row.get("ts_unix_ns")
                    or "0"
                )
                cursor = {
                    "benchmark_target_size": _safe_int(row.get("benchmark_target_size")),
                    "benchmark_active_size": _safe_int(row.get("benchmark_active_size")),
                    "benchmark_phase": (row.get("benchmark_phase") or "").strip(),
                    "benchmark_operation": (row.get("benchmark_operation") or "").strip(),
                    "benchmark_operation_seq": _safe_int(row.get("benchmark_operation_seq")),
                    "benchmark_plateau_index": _safe_int(row.get("benchmark_plateau_index")),
                    "group_epoch": _safe_int(row.get("group_epoch")),
                    "member_count": _safe_int(row.get("member_count")),
                    "operation_family": (row.get("operation_family") or "").strip(),
                    "failure_class": (row.get("failure_class") or "").strip(),
                    "failure_detail": (row.get("failure_detail") or "").strip(),
                    "failure_timestamp_ns": _safe_int(ts_field),
                }

                if failed_id not in cursors:
                    cursors[failed_id] = cursor
                else:
                    # Prefer rows with actual failure data (non-empty failure_class
                    # or failure_detail) over empty ones, even if they are later.
                    # Within the same priority, prefer the earlier timestamp.
                    existing = cursors[failed_id]
                    
                    def _cursor_priority(c: Dict[str, Any]) -> int:
                        """Higher = more useful. 2 = has failure data, 1 = has cursor, 0 = empty."""
                        has_failure = bool(
                            (c.get("failure_class") or "").strip()
                            or (c.get("failure_detail") or "").strip()
                        )
                        has_cursor = bool(
                            c.get("benchmark_phase") or c.get("benchmark_operation")
                        )
                        if has_failure:
                            return 2
                        if has_cursor:
                            return 1
                        return 0

                    new_pri = _cursor_priority(cursor)
                    old_pri = _cursor_priority(existing)

                    if new_pri > old_pri:
                        cursors[failed_id] = cursor
                    elif new_pri == old_pri:
                        if cursor["failure_timestamp_ns"] < existing["failure_timestamp_ns"] and existing["failure_timestamp_ns"] > 0:
                            cursors[failed_id] = cursor
                        elif existing["failure_timestamp_ns"] == 0:
                            cursors[failed_id] = cursor

    except (csv.Error, IOError, OSError):
        pass

    return cursors


def enrich_worker_failures_with_cursors(
    worker_failures: List[WorkerFailureInfo],
    events_csv_path: str,
) -> List[WorkerFailureInfo]:
    """Enrich WorkerFailureInfo objects with cursor context from events.csv.

    For each WorkerFailureInfo, looks up the logical_client_id in the
    events.csv failure cursors and populates current_phase,
    current_operation_family, current_benchmark_operation,
    current_member_count, current_epoch, and failure_timestamp_ns.

    Also backfills the failure_class from events.csv if the existing
    classification is unknown_failure and events.csv has a more specific
    class.

    Returns the same list (mutated in place).
    """
    cursors = extract_failure_cursors_from_events_csv(events_csv_path)

    for wf in worker_failures:
        cid = wf.logical_client_id
        if not cid:
            continue

        cursor = cursors.get(cid)
        if cursor is None:
            continue

        if not wf.current_phase:
            wf.current_phase = cursor.get("benchmark_phase", "")
        if not wf.current_operation_family:
            wf.current_operation_family = cursor.get("operation_family", "")
        if not wf.current_benchmark_operation:
            wf.current_benchmark_operation = cursor.get("benchmark_operation", "")
        if wf.current_member_count == 0:
            wf.current_member_count = cursor.get("member_count", 0)
        if wf.current_epoch == 0:
            wf.current_epoch = cursor.get("group_epoch", 0)
        if wf.failure_timestamp_ns == 0:
            wf.failure_timestamp_ns = cursor.get("failure_timestamp_ns", 0)

        if wf.failure_class == "unknown_failure":
            ev_class = cursor.get("failure_class", "")
            if ev_class and ev_class.lower() != "none":
                wf.failure_class = _normalize_failure_class(ev_class)

    worker_failures.sort(key=lambda w: w.failure_timestamp_ns)
    return worker_failures


def _normalize_failure_class(runner_class: str) -> str:
    """Map Rust runner failure class strings to Python-side classes."""
    mapping = {
        "oom_kill": "hard_ram_oom_kill",
        "hard_upper_bound_oom_kill": "hard_ram_oom_kill",
        "container_exit": "hard_container_exit",
        "hard_upper_bound_container_exit": "hard_container_exit",
        "cpu_starvation_timeout": "cpu_timeout",
        "cpu_starvation_suspected": "cpu_starvation_suspected",
        "worker_unreachable": "worker_unreachable",
        "protocol_failure": "benchmark_protocol_failure",
        "benchmark_protocol_failure": "benchmark_protocol_failure",
        "infrastructure_failure": "infrastructure_failure",
        "resource_pressure_memory": "memory_pressure_no_oom",
    }
    return mapping.get(runner_class, runner_class)
