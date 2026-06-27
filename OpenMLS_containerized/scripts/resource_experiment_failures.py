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
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field


FAILURE_CLASSES = [
    "completed_successfully",
    "hard_ram_oom_kill",
    "hard_container_exit",
    "cpu_timeout",
    "cpu_starvation_suspected",
    "cpu_walltime_deadline_exceeded",
    "memory_pressure_no_oom",
    "app_heap_budget_exceeded",
    "app_heap_budget_allocator_abort",
    "embedded_budget_timeout",
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
    failure_detail: str = ""
    failure_evidence_source: str = ""
    failure_evidence_detail: str = ""
    failure_action: str = ""
    attribution_confidence: str = ""
    attribution_source: str = ""
    failure_timestamp_ns: int = 0
    last_successful_phase: str = ""
    last_successful_operation_family: str = ""
    last_successful_benchmark_operation: str = ""
    last_successful_member_count: int = 0
    last_successful_epoch: int = 0
    current_phase: str = ""
    current_operation_family: str = ""
    current_benchmark_operation: str = ""
    last_observed_span_name: str = ""
    last_observed_span_id: str = ""
    current_member_count: int = 0
    current_epoch: int = 0
    memory_model: str = ""
    app_heap_budget: str = ""
    app_heap_budget_bytes: int = 0
    heap_current_live_bytes: int = 0
    heap_peak_live_bytes: int = 0
    heap_operation_peak_live_bytes: int = 0
    heap_total_allocated_bytes: int = 0
    heap_allocation_count: int = 0
    heap_deallocation_count: int = 0
    heap_failed_allocation_size_bytes: int = 0
    container_exit_code: Optional[int] = None
    container_oom_killed: bool = False
    memory_events_oom: int = 0
    memory_events_oom_kill: int = 0
    max_memory_current: int = 0
    cpu_nr_throttled_delta: int = 0
    cpu_throttled_usec_delta: int = 0
    cpu_throttled_time_fraction: float = 0.0
    last_container_status: str = ""
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

    oom_killed = info.container_oom_killed or check_oom_events_file(
        oom_events_path or "",
        info.container_name or info.physical_worker_id or info.worker_id,
    )
    oom_events_oom_kill = info.memory_events_oom_kill > 0

    if info.failure_class in (
        "app_heap_budget_exceeded",
        "app_heap_budget_allocator_abort",
        "embedded_budget_timeout",
    ):
        failure_class = info.failure_class
    elif oom_killed or oom_events_oom_kill:
        failure_class = "hard_ram_oom_kill"
    elif info.container_exit_code is not None and info.container_exit_code != 0:
        failure_class = "hard_container_exit"
    elif info.failure_class in ("worker_unreachable", "infrastructure_failure",
                                 "benchmark_protocol_failure"):
        failure_class = info.failure_class
    elif (
        info.container_exit_code in (None, 0)
        and info.last_container_status in ("running", "exited", "created")
    ):
        failure_class = "completed_successfully"
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
        container_exit_code=_safe_optional_int(resource_summary.get("last_container_exit_code")),
        container_oom_killed=_safe_bool(resource_summary.get("last_container_oom_killed")),
        memory_events_oom=_safe_int(resource_summary.get("memory_events_oom")),
        memory_events_oom_kill=_safe_int(resource_summary.get("memory_events_oom_kill")),
        max_memory_current=_safe_int(resource_summary.get("max_memory_current")),
        cpu_nr_throttled_delta=_safe_int(resource_summary.get("cpu_nr_throttled_delta")),
        cpu_throttled_usec_delta=_safe_int(resource_summary.get("cpu_throttled_usec_delta")),
        cpu_throttled_time_fraction=_safe_float(resource_summary.get("cpu_throttled_time_fraction")),
        last_container_status=str(resource_summary.get("last_container_status", "")),
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
    memory_model: str = "",
    docker_memory_limit: str = "",
    app_heap_budget: str = "",
    app_heap_budget_bytes: int = 0,
    sweep_kind: str = "",
    strict_cpuset_satisfied: bool = False,
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
    is_embedded = resource_experiment == "embedded-budget-singleton" or experiment_kind == "embedded_budget_singleton"

    if run_success and first_failure:
        valid_for_threshold = True
        valid_for_performance = False
        valid_for_churn = True
        run_status = "completed_with_worker_failures"
    elif run_success:
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
        "memory_model": memory_model or (first_failure.memory_model if first_failure else ""),
        "docker_memory_limit": docker_memory_limit,
        "app_heap_budget": app_heap_budget or (first_failure.app_heap_budget if first_failure else ""),
        "app_heap_budget_bytes": app_heap_budget_bytes or (first_failure.app_heap_budget_bytes if first_failure else 0),
        "run_status": run_status,
        "completed": completed,
        "valid_for_threshold_analysis": valid_for_threshold,
        "valid_for_embedded_heap_threshold_analysis": is_embedded,
        "valid_for_docker_resource_analysis": not is_embedded,
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
        "sweep_kind": sweep_kind,
        "strict_cpuset_satisfied": strict_cpuset_satisfied,
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
        "failure_detail": info.failure_detail,
        "failure_evidence_source": info.failure_evidence_source,
        "failure_evidence_detail": info.failure_evidence_detail,
        "failure_action": info.failure_action,
        "attribution_confidence": info.attribution_confidence,
        "attribution_source": info.attribution_source,
        "failure_timestamp_ns": info.failure_timestamp_ns,
        "last_successful_phase": info.last_successful_phase,
        "last_successful_operation_family": info.last_successful_operation_family,
        "last_successful_benchmark_operation": info.last_successful_benchmark_operation,
        "last_successful_member_count": info.last_successful_member_count,
        "last_successful_epoch": info.last_successful_epoch,
        "current_phase": info.current_phase,
        "current_operation_family": info.current_operation_family,
        "current_benchmark_operation": info.current_benchmark_operation,
        "last_observed_span_name": info.last_observed_span_name,
        "last_observed_span_id": info.last_observed_span_id,
        "current_member_count": info.current_member_count,
        "current_epoch": info.current_epoch,
        "memory_model": info.memory_model,
        "app_heap_budget": info.app_heap_budget,
        "app_heap_budget_bytes": info.app_heap_budget_bytes,
        "heap_current_live_bytes": info.heap_current_live_bytes,
        "heap_peak_live_bytes": info.heap_peak_live_bytes,
        "heap_operation_peak_live_bytes": info.heap_operation_peak_live_bytes,
        "heap_total_allocated_bytes": info.heap_total_allocated_bytes,
        "heap_allocation_count": info.heap_allocation_count,
        "heap_deallocation_count": info.heap_deallocation_count,
        "heap_failed_allocation_size_bytes": info.heap_failed_allocation_size_bytes,
        "container_exit_code": info.container_exit_code,
        "container_oom_killed": info.container_oom_killed,
        "memory_events_oom": info.memory_events_oom,
        "memory_events_oom_kill": info.memory_events_oom_kill,
        "max_memory_current": info.max_memory_current,
        "cpu_nr_throttled_delta": info.cpu_nr_throttled_delta,
        "cpu_throttled_usec_delta": info.cpu_throttled_usec_delta,
        "cpu_throttled_time_fraction": info.cpu_throttled_time_fraction,
        "last_container_status": info.last_container_status,
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


def _safe_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    if isinstance(value, (int, float)):
        return bool(value)
    return False


_APP_HEAP_KV_RE = re.compile(r'([A-Za-z0-9_]+)=("([^"]*)"|[^ ]*)')


def parse_app_heap_budget_failure(detail: str) -> Dict[str, str]:
    """Parse the structured worker APP_HEAP_BUDGET_EXCEEDED error payload."""
    if "APP_HEAP_BUDGET_EXCEEDED" not in (detail or ""):
        return {}
    parsed: Dict[str, str] = {}
    for match in _APP_HEAP_KV_RE.finditer(detail):
        key = match.group(1)
        value = match.group(3) if match.group(3) is not None else match.group(2)
        parsed[key] = value
    return parsed


def worker_failures_from_terminal_output(terminal_output_path: Optional[str]) -> List[WorkerFailureInfo]:
    """Extract app-heap failures emitted before any profile event exists."""
    if not terminal_output_path or not os.path.exists(terminal_output_path):
        return []

    try:
        with open(terminal_output_path, encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError:
        return []

    failures: List[WorkerFailureInfo] = []
    seen: Set[str] = set()
    for line in lines:
        if "APP_HEAP_BUDGET_EXCEEDED" not in line:
            continue
        app_heap = parse_app_heap_budget_failure(line.strip())
        if not app_heap:
            continue
        logical_id = app_heap.get("worker_id", "").strip()
        if not logical_id or logical_id in seen:
            continue
        seen.add(logical_id)

        physical_id = logical_id if logical_id.startswith("worker-") else f"worker-{logical_id}"
        phase = app_heap.get("span_or_phase", "").strip()
        if phase == "-":
            phase = ""
        failures.append(WorkerFailureInfo(
            worker_id=physical_id,
            physical_worker_id=physical_id,
            logical_client_id=logical_id,
            container_name=physical_id,
            resource_profile_id=app_heap.get("resource_profile_id", ""),
            experiment_kind="embedded_budget_singleton",
            failure_class=app_heap.get("failure_class", "app_heap_budget_exceeded"),
            failure_detail=line.strip(),
            failure_evidence_source="runner_terminal_output",
            failure_action="stop_run",
            attribution_confidence="exact_runner_operation",
            attribution_source="app_heap_budget_failure_payload",
            current_phase=phase,
            current_operation_family=app_heap.get("operation_family", ""),
            current_benchmark_operation=app_heap.get("benchmark_operation", ""),
            current_member_count=_safe_int(app_heap.get("member_count")),
            current_epoch=_safe_int(app_heap.get("epoch")),
            memory_model=app_heap.get("memory_model", ""),
            app_heap_budget=app_heap.get("app_heap_budget", ""),
            app_heap_budget_bytes=_safe_int(app_heap.get("app_heap_budget_bytes")),
            heap_current_live_bytes=_safe_int(app_heap.get("current_live_heap_bytes")),
            heap_peak_live_bytes=_safe_int(app_heap.get("peak_live_heap_bytes")),
            heap_operation_peak_live_bytes=_safe_int(app_heap.get("operation_peak_live_heap_bytes")),
            heap_total_allocated_bytes=_safe_int(app_heap.get("total_allocated_bytes")),
            heap_allocation_count=_safe_int(app_heap.get("allocation_count")),
            heap_deallocation_count=_safe_int(app_heap.get("deallocation_count")),
            heap_failed_allocation_size_bytes=_safe_int(app_heap.get("failed_allocation_size_bytes")),
            last_observed_span_id=app_heap.get("failure_span_id", ""),
        ))
    return failures


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
                event_names = {
                    str(event.get("container_name") or ""),
                    str(event.get("physical_worker_id") or ""),
                    str(event.get("worker_id") or ""),
                }
                event_type = str(event.get("event_type") or event.get("source") or "")
                detail = str(event.get("detail") or "").lower()
                if container_name in event_names and (
                    "oom" in event_type.lower() or "oom" in detail
                ):
                    return True
    except (json.JSONDecodeError, IOError):
        pass
    return False


def _benchmark_operation_family(operation: str, phase: str = "") -> str:
    operation = (operation or "").strip().lower()
    phase = (phase or "").strip().lower()
    if operation in ("add_commit", "add_members") or phase == "membership_add":
        return "add_commit_create"
    if operation in ("remove_commit", "remove_members") or phase == "membership_remove":
        return "remove_commit_create"
    if operation in ("self_update", "update_commit") or phase == "update":
        return "update_commit_create"
    if operation in ("send_application_message", "application_message_create") or phase == "application":
        return "application_message_create"
    if operation in ("create_group", "group_create"):
        return "group_create"
    return ""


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
            rows = list(csv.DictReader(f))

        rows.sort(key=lambda row: _safe_int(row.get("ts_unix_ns") or row.get("timestamp_ns")))
        prior_by_client: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            client_id = (row.get("client_id") or "").strip()
            failed_id = (row.get("failed_worker_id") or "").strip()
            if not failed_id:
                if client_id and (row.get("op") or row.get("benchmark_operation")):
                    prior_by_client[client_id] = row
                continue

            prior = prior_by_client.get(failed_id, {})
            ts_field = row.get("timestamp_ns") or row.get("ts_unix_ns") or "0"
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
                    "span_name": (row.get("span_name") or "").strip(),
                    "span_id": (row.get("span_id") or "").strip(),
                    "failure_class": (row.get("failure_class") or "").strip(),
                    "failure_detail": (row.get("failure_detail") or "").strip(),
                    "failure_evidence_source": (row.get("failure_evidence_source") or "").strip(),
                    "failure_evidence_detail": (row.get("failure_evidence_detail") or "").strip(),
                    "failure_action": (row.get("failure_action") or "").strip(),
                    "failed_physical_worker_id": (row.get("failed_physical_worker_id") or "").strip(),
                    "memory_model": (row.get("memory_model") or "").strip(),
                    "app_heap_budget": (row.get("app_heap_budget") or "").strip(),
                    "app_heap_budget_bytes": _safe_int(row.get("app_heap_budget_bytes")),
                    "heap_current_live_bytes": _safe_int(row.get("heap_current_live_bytes")),
                    "heap_peak_live_bytes": _safe_int(row.get("heap_peak_live_bytes")),
                    "heap_operation_peak_live_bytes": _safe_int(row.get("heap_operation_peak_live_bytes")),
                    "heap_total_allocated_bytes": _safe_int(row.get("heap_total_allocated_bytes")),
                    "heap_allocation_count": _safe_int(row.get("heap_allocation_count")),
                    "heap_deallocation_count": _safe_int(row.get("heap_deallocation_count")),
                    "heap_failed_allocation_size_bytes": _safe_int(row.get("heap_failed_allocation_size_bytes")),
                    "last_successful_phase": (prior.get("benchmark_phase") or "").strip(),
                    "last_successful_operation_family": (prior.get("operation_family") or "").strip(),
                    "last_successful_benchmark_operation": (prior.get("benchmark_operation") or "").strip(),
                    "last_successful_member_count": _safe_int(
                        prior.get("member_count") or prior.get("benchmark_active_size")
                    ),
                    "last_successful_epoch": _safe_int(prior.get("group_epoch")),
                    "last_successful_span_name": (prior.get("span_name") or prior.get("op") or "").strip(),
                    "last_successful_span_id": (prior.get("span_id") or "").strip(),
                    "failure_timestamp_ns": _safe_int(ts_field),
            }

            if not cursor["operation_family"]:
                cursor["operation_family"] = _benchmark_operation_family(
                    cursor["benchmark_operation"], cursor["benchmark_phase"]
                ) or cursor["last_successful_operation_family"]

            app_heap = parse_app_heap_budget_failure(cursor["failure_detail"])
            if app_heap:
                cursor["failure_class"] = app_heap.get("failure_class", "app_heap_budget_exceeded")
                cursor["operation_family"] = app_heap.get("operation_family", cursor["operation_family"])
                cursor["benchmark_operation"] = app_heap.get("benchmark_operation", cursor["benchmark_operation"])
                cursor["benchmark_phase"] = app_heap.get("span_or_phase", cursor["benchmark_phase"])
                cursor["member_count"] = _safe_int(app_heap.get("member_count")) or cursor["member_count"]
                cursor["group_epoch"] = _safe_int(app_heap.get("epoch")) or cursor["group_epoch"]
                cursor["memory_model"] = app_heap.get("memory_model", cursor["memory_model"])
                cursor["app_heap_budget"] = app_heap.get("app_heap_budget", cursor["app_heap_budget"])
                cursor["app_heap_budget_bytes"] = _safe_int(app_heap.get("app_heap_budget_bytes")) or cursor["app_heap_budget_bytes"]
                cursor["heap_current_live_bytes"] = _safe_int(app_heap.get("current_live_heap_bytes")) or cursor["heap_current_live_bytes"]
                cursor["heap_peak_live_bytes"] = _safe_int(app_heap.get("peak_live_heap_bytes")) or cursor["heap_peak_live_bytes"]
                cursor["heap_operation_peak_live_bytes"] = _safe_int(app_heap.get("operation_peak_live_heap_bytes")) or cursor["heap_operation_peak_live_bytes"]
                cursor["heap_total_allocated_bytes"] = _safe_int(app_heap.get("total_allocated_bytes")) or cursor["heap_total_allocated_bytes"]
                cursor["heap_allocation_count"] = _safe_int(app_heap.get("allocation_count")) or cursor["heap_allocation_count"]
                cursor["heap_deallocation_count"] = _safe_int(app_heap.get("deallocation_count")) or cursor["heap_deallocation_count"]
                cursor["heap_failed_allocation_size_bytes"] = _safe_int(app_heap.get("failed_allocation_size_bytes")) or cursor["heap_failed_allocation_size_bytes"]
                cursor["span_id"] = app_heap.get("failure_span_id", cursor["span_id"])
                cursor["attribution_confidence"] = (
                    "exact_profile_span" if cursor["span_name"] else "exact_runner_operation"
                )
                cursor["attribution_source"] = "app_heap_budget_failure_payload"
            if not cursor["member_count"]:
                cursor["member_count"] = (
                    cursor["benchmark_active_size"]
                    or cursor["last_successful_member_count"]
                )
            if not cursor["group_epoch"]:
                cursor["group_epoch"] = cursor["last_successful_epoch"]

            has_exact_runner_cursor = bool(
                    (row.get("runner_event_kind") or "").strip() == "worker_failure"
                    and cursor["benchmark_operation"]
                    and cursor["failure_evidence_source"] != "post_run_synthesis"
                )
            if not app_heap:
                cursor["attribution_confidence"] = (
                        "exact_runner_operation" if has_exact_runner_cursor else "last_observed_span"
                    )
                cursor["attribution_source"] = (
                        "runner_failure_event" if has_exact_runner_cursor else "events_csv_temporal_correlation"
                    )
            if not cursor["span_name"]:
                cursor["span_name"] = cursor["last_successful_span_name"]
                cursor["span_id"] = cursor["last_successful_span_id"]

            if failed_id not in cursors:
                cursors[failed_id] = cursor
            else:
                existing = cursors[failed_id]

                def _cursor_priority(c: Dict[str, Any]) -> int:
                    """Higher means more direct failure evidence."""
                    has_failure = bool(
                        (c.get("failure_class") or "").strip()
                        or (c.get("failure_detail") or "").strip()
                    )
                    has_cursor = bool(
                        c.get("benchmark_phase") or c.get("benchmark_operation")
                    )
                    if c.get("attribution_confidence") == "exact_runner_operation":
                        return 3
                    if has_failure:
                        return 2
                    if has_cursor:
                        return 1
                    return 0

                new_pri = _cursor_priority(cursor)
                old_pri = _cursor_priority(existing)

                if new_pri > old_pri:
                    cursors[failed_id] = cursor
                elif new_pri == old_pri and (
                    existing["failure_timestamp_ns"] == 0
                    or 0 < cursor["failure_timestamp_ns"] < existing["failure_timestamp_ns"]
                ):
                    cursors[failed_id] = cursor

    except (csv.Error, IOError, OSError):
        pass

    return cursors


def worker_failures_from_events_csv(events_csv_path: str) -> List[WorkerFailureInfo]:
    """Create failure records from authoritative runner failure events."""
    failures = []
    for failed_id, cursor in extract_failure_cursors_from_events_csv(events_csv_path).items():
        failure_class = _normalize_failure_class(cursor.get("failure_class", ""))
        failures.append(WorkerFailureInfo(
            worker_id=failed_id,
            physical_worker_id=cursor.get("failed_physical_worker_id", ""),
            logical_client_id=failed_id,
            failure_class=failure_class or "unknown_failure",
            failure_detail=cursor.get("failure_detail", ""),
            failure_evidence_source=cursor.get("failure_evidence_source", ""),
            failure_evidence_detail=cursor.get("failure_evidence_detail", ""),
            failure_action=cursor.get("failure_action", ""),
            attribution_confidence=cursor.get("attribution_confidence", ""),
            attribution_source=cursor.get("attribution_source", ""),
            failure_timestamp_ns=cursor.get("failure_timestamp_ns", 0),
            last_successful_phase=cursor.get("last_successful_phase", ""),
            last_successful_operation_family=cursor.get("last_successful_operation_family", ""),
            last_successful_benchmark_operation=cursor.get("last_successful_benchmark_operation", ""),
            last_successful_member_count=cursor.get("last_successful_member_count", 0),
            last_successful_epoch=cursor.get("last_successful_epoch", 0),
            current_phase=cursor.get("benchmark_phase", ""),
            current_operation_family=cursor.get("operation_family", ""),
            current_benchmark_operation=cursor.get("benchmark_operation", ""),
            last_observed_span_name=cursor.get("span_name", ""),
            last_observed_span_id=cursor.get("span_id", ""),
            current_member_count=cursor.get("member_count", 0),
            current_epoch=cursor.get("group_epoch", 0),
            memory_model=cursor.get("memory_model", ""),
            app_heap_budget=cursor.get("app_heap_budget", ""),
            app_heap_budget_bytes=cursor.get("app_heap_budget_bytes", 0),
            heap_current_live_bytes=cursor.get("heap_current_live_bytes", 0),
            heap_peak_live_bytes=cursor.get("heap_peak_live_bytes", 0),
            heap_operation_peak_live_bytes=cursor.get("heap_operation_peak_live_bytes", 0),
            heap_total_allocated_bytes=cursor.get("heap_total_allocated_bytes", 0),
            heap_allocation_count=cursor.get("heap_allocation_count", 0),
            heap_deallocation_count=cursor.get("heap_deallocation_count", 0),
            heap_failed_allocation_size_bytes=cursor.get("heap_failed_allocation_size_bytes", 0),
        ))
    return sorted(failures, key=lambda failure: failure.failure_timestamp_ns)


def worker_failures_from_runner_events_jsonl(runner_events_path: str) -> List[WorkerFailureInfo]:
    """Create failure records from the runner failure journal.

    This is the authoritative source when --no-aggregate is used: the Rust
    runner still writes runner-events.jsonl, but events.csv is deliberately not
    produced until an external aggregation pass runs.
    """
    if not runner_events_path or not os.path.exists(runner_events_path):
        return []

    failures_by_client: Dict[str, WorkerFailureInfo] = {}
    try:
        with open(runner_events_path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                event_kind = str(
                    event.get("event_kind") or event.get("runner_event_kind") or ""
                )
                if event_kind != "worker_failure":
                    continue

                failed_id = str(event.get("failed_worker_id") or "").strip()
                if not failed_id:
                    continue

                physical_id = str(event.get("failed_physical_worker_id") or "").strip()
                if not physical_id:
                    physical_id = failed_id if failed_id.startswith("worker-") else f"worker-{failed_id}"

                failure_detail = str(event.get("failure_detail") or "")
                app_heap = parse_app_heap_budget_failure(failure_detail)
                benchmark_phase = str(event.get("benchmark_phase") or "").strip()
                benchmark_operation = str(event.get("benchmark_operation") or "").strip()
                operation_family = _benchmark_operation_family(
                    benchmark_operation, benchmark_phase
                )
                if app_heap:
                    benchmark_phase = app_heap.get("span_or_phase", benchmark_phase)
                    if benchmark_phase == "-":
                        benchmark_phase = str(event.get("benchmark_phase") or "").strip()
                    benchmark_operation = app_heap.get(
                        "benchmark_operation", benchmark_operation
                    )
                    operation_family = app_heap.get(
                        "operation_family", operation_family
                    )

                failure = WorkerFailureInfo(
                    worker_id=failed_id,
                    physical_worker_id=physical_id,
                    logical_client_id=failed_id,
                    container_name=physical_id,
                    resource_profile_id=app_heap.get("resource_profile_id", ""),
                    experiment_kind=(
                        "embedded_budget_singleton"
                        if app_heap.get("memory_model") == "app-heap-budget"
                        else ""
                    ),
                    failure_class=_normalize_failure_class(
                        str(event.get("failure_class") or app_heap.get("failure_class") or "")
                    ) or "unknown_failure",
                    failure_detail=failure_detail,
                    failure_evidence_source=str(event.get("failure_evidence_source") or ""),
                    failure_evidence_detail=str(event.get("failure_evidence_detail") or ""),
                    failure_action=str(event.get("failure_action") or ""),
                    attribution_confidence=(
                        "exact_runner_operation"
                        if benchmark_operation or app_heap
                        else "runner_failure_event"
                    ),
                    attribution_source=(
                        "app_heap_budget_failure_payload"
                        if app_heap
                        else "runner_failure_event"
                    ),
                    failure_timestamp_ns=_safe_int(event.get("ts_unix_ns")),
                    current_phase=benchmark_phase,
                    current_operation_family=operation_family,
                    current_benchmark_operation=benchmark_operation,
                    current_member_count=(
                        _safe_int(app_heap.get("member_count"))
                        or _safe_int(event.get("benchmark_active_size"))
                    ),
                    current_epoch=_safe_int(app_heap.get("epoch")),
                    memory_model=app_heap.get("memory_model", ""),
                    app_heap_budget=app_heap.get("app_heap_budget", ""),
                    app_heap_budget_bytes=_safe_int(app_heap.get("app_heap_budget_bytes")),
                    heap_current_live_bytes=_safe_int(app_heap.get("current_live_heap_bytes")),
                    heap_peak_live_bytes=_safe_int(app_heap.get("peak_live_heap_bytes")),
                    heap_operation_peak_live_bytes=_safe_int(
                        app_heap.get("operation_peak_live_heap_bytes")
                    ),
                    heap_total_allocated_bytes=_safe_int(app_heap.get("total_allocated_bytes")),
                    heap_allocation_count=_safe_int(app_heap.get("allocation_count")),
                    heap_deallocation_count=_safe_int(app_heap.get("deallocation_count")),
                    heap_failed_allocation_size_bytes=_safe_int(
                        app_heap.get("failed_allocation_size_bytes")
                    ),
                    last_observed_span_id=app_heap.get("failure_span_id", ""),
                )

                existing = failures_by_client.get(failed_id)
                if existing is None or (
                    existing.failure_timestamp_ns == 0
                    or 0 < failure.failure_timestamp_ns < existing.failure_timestamp_ns
                ):
                    failures_by_client[failed_id] = failure
    except OSError:
        return []

    return sorted(
        failures_by_client.values(), key=lambda failure: failure.failure_timestamp_ns
    )


def append_synthetic_runner_failure_event(
    run_dir: str,
    failure: WorkerFailureInfo,
    events_csv_path: str,
) -> bool:
    """Append a terminal fallback event when no live runner event survived.

    The benchmark operation is taken from the most recent observed event and is
    deliberately marked as temporal correlation, not exact runner attribution.
    """
    existing = extract_failure_cursors_from_events_csv(events_csv_path)
    if failure.logical_client_id in existing:
        return False

    active_cursor: Dict[str, Any] = {}
    cursor_journal = os.path.join(run_dir, "profiled-operation-cursors.jsonl")
    if os.path.exists(cursor_journal):
        request_states: Dict[str, Dict[str, Any]] = {}
        with open(cursor_journal, encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(event.get("logical_client_id") or "") != failure.logical_client_id:
                    continue
                request_id = str(event.get("request_id") or "")
                if not request_id:
                    continue
                request_states[request_id] = event
        active = [
            event for event in request_states.values()
            if event.get("lifecycle") == "started"
        ]
        if active:
            active_cursor = max(active, key=lambda event: _safe_int(event.get("ts_unix_ns")))

    latest: Dict[str, Any] = {}
    if os.path.exists(events_csv_path):
        with open(events_csv_path, newline="") as handle:
            candidates = [
                row for row in csv.DictReader(handle)
                if (row.get("client_id") or "").strip() == failure.logical_client_id
            ]
        if candidates:
            latest = max(
                candidates,
                key=lambda row: _safe_int(row.get("ts_unix_ns") or row.get("timestamp_ns")),
            )

    failure.failure_timestamp_ns = failure.failure_timestamp_ns or time.time_ns()
    failure.current_phase = failure.current_phase or (
        active_cursor.get("benchmark_phase") or latest.get("benchmark_phase") or ""
    )
    failure.current_operation_family = (
        failure.current_operation_family
        or _benchmark_operation_family(
            active_cursor.get("benchmark_operation") or active_cursor.get("command") or "",
            active_cursor.get("benchmark_phase") or "",
        )
        or (latest.get("operation_family") or "")
    )
    failure.current_benchmark_operation = (
        failure.current_benchmark_operation
        or active_cursor.get("benchmark_operation")
        or active_cursor.get("command")
        or latest.get("benchmark_operation")
        or latest.get("op")
        or "unknown"
    )
    failure.last_observed_span_name = failure.last_observed_span_name or (
        latest.get("span_name") or latest.get("op") or ""
    )
    failure.last_observed_span_id = failure.last_observed_span_id or str(
        latest.get("span_id") or ""
    )
    failure.current_member_count = failure.current_member_count or _safe_int(
        active_cursor.get("benchmark_active_size")
        or latest.get("benchmark_active_size")
        or latest.get("member_count")
    )
    failure.current_epoch = failure.current_epoch or _safe_int(latest.get("group_epoch"))
    preserve_exact_app_heap = failure.attribution_source == "app_heap_budget_failure_payload"
    if active_cursor:
        failure.attribution_confidence = "exact_runner_operation"
        if not preserve_exact_app_heap:
            failure.attribution_source = "runner_active_operation_journal"
        synthesized_evidence_source = "runner_active_operation_journal"
    else:
        if not preserve_exact_app_heap:
            failure.attribution_confidence = "last_observed_span"
            failure.attribution_source = "post_run_synthesis"
        synthesized_evidence_source = "post_run_synthesis"
    event_evidence_source = failure.failure_evidence_source or synthesized_evidence_source
    failure.failure_evidence_source = event_evidence_source
    failure.failure_detail = failure.failure_detail or (
        f"{failure.failure_class}: container_status={failure.last_container_status or 'unknown'} "
        f"exit_code={failure.container_exit_code} oom_killed={failure.container_oom_killed} "
        f"memory_events_oom_kill={failure.memory_events_oom_kill}"
    )

    event = {
        "profile_schema_version": 10,
        "ts_unix_ns": failure.failure_timestamp_ns,
        "event_kind": "worker_failure",
        "failed_worker_id": failure.logical_client_id or failure.worker_id,
        "failed_physical_worker_id": failure.physical_worker_id or failure.worker_id,
        "failure_class": failure.failure_class,
        "failure_detail": failure.failure_detail,
        "failure_evidence_source": event_evidence_source,
        "failure_evidence_detail": failure.failure_evidence_detail or None,
        "failure_action": failure.failure_action or "stop_run",
        "reassigned_to_worker_id": None,
        "benchmark_plateau_index": _safe_int(
            active_cursor.get("benchmark_plateau_index") or latest.get("benchmark_plateau_index")
        ),
        "benchmark_target_size": _safe_int(
            active_cursor.get("benchmark_target_size") or latest.get("benchmark_target_size")
        ),
        "benchmark_active_size": _safe_int(
            active_cursor.get("benchmark_active_size") or latest.get("benchmark_active_size")
        ),
        "benchmark_phase": failure.current_phase,
        "benchmark_operation": failure.current_benchmark_operation,
        "benchmark_operation_seq": _safe_optional_int(
            active_cursor.get("benchmark_operation_seq") or latest.get("benchmark_operation_seq")
        ),
        "benchmark_payload_size": _safe_optional_int(
            active_cursor.get("benchmark_payload_size") or latest.get("benchmark_payload_size")
        ),
        "membership_batch_requested": _safe_optional_int(latest.get("membership_batch_requested")),
        "membership_batch_effective": _safe_optional_int(latest.get("membership_batch_effective")),
        "membership_batch_group_cap": _safe_optional_int(latest.get("membership_batch_group_cap")),
        "membership_batch_transition_cap": _safe_optional_int(latest.get("membership_batch_transition_cap")),
        "membership_batch_source": latest.get("membership_batch_source") or None,
        "configured_payload_label": latest.get("configured_payload_label") or None,
    }
    journal_path = os.path.join(run_dir, "runner-events.jsonl")
    with open(journal_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return True


SYNTHETIC_FAILURE_EVENTS_HEADER = [
    "client_id",
    "failed_worker_id",
    "failed_physical_worker_id",
    "runner_event_kind",
    "failure_class",
    "failure_detail",
    "failure_evidence_source",
    "failure_evidence_detail",
    "failure_action",
    "ts_unix_ns",
    "benchmark_target_size",
    "benchmark_active_size",
    "benchmark_phase",
    "benchmark_operation",
    "benchmark_operation_seq",
    "benchmark_plateau_index",
    "group_epoch",
    "member_count",
    "operation_family",
    "span_name",
    "span_id",
    "memory_model",
    "app_heap_budget",
    "app_heap_budget_bytes",
    "heap_current_live_bytes",
    "heap_peak_live_bytes",
    "heap_operation_peak_live_bytes",
    "heap_total_allocated_bytes",
    "heap_allocation_count",
    "heap_deallocation_count",
    "heap_failed_allocation_size_bytes",
]


def write_synthetic_failure_events_csv(
    events_csv_path: str,
    failures: List[WorkerFailureInfo],
) -> bool:
    """Write a minimal events.csv for failures before profiling emitted rows."""
    if os.path.exists(events_csv_path) and os.path.getsize(events_csv_path) > 0:
        return False
    rows = [
        failure for failure in failures
        if failure.failure_class in (
            "app_heap_budget_exceeded",
            "app_heap_budget_allocator_abort",
            "embedded_budget_timeout",
        )
    ]
    if not rows:
        return False
    with open(events_csv_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SYNTHETIC_FAILURE_EVENTS_HEADER)
        writer.writeheader()
        for failure in rows:
            writer.writerow({
                "client_id": failure.logical_client_id,
                "failed_worker_id": failure.logical_client_id or failure.worker_id,
                "failed_physical_worker_id": failure.physical_worker_id,
                "runner_event_kind": "worker_failure",
                "failure_class": failure.failure_class,
                "failure_detail": failure.failure_detail,
                "failure_evidence_source": failure.failure_evidence_source,
                "failure_evidence_detail": failure.failure_evidence_detail,
                "failure_action": failure.failure_action or "stop_run",
                "ts_unix_ns": failure.failure_timestamp_ns or time.time_ns(),
                "benchmark_target_size": failure.current_member_count,
                "benchmark_active_size": failure.current_member_count,
                "benchmark_phase": failure.current_phase,
                "benchmark_operation": failure.current_benchmark_operation,
                "benchmark_operation_seq": "",
                "benchmark_plateau_index": "",
                "group_epoch": failure.current_epoch,
                "member_count": failure.current_member_count,
                "operation_family": failure.current_operation_family,
                "span_name": failure.last_observed_span_name,
                "span_id": failure.last_observed_span_id,
                "memory_model": failure.memory_model,
                "app_heap_budget": failure.app_heap_budget,
                "app_heap_budget_bytes": failure.app_heap_budget_bytes,
                "heap_current_live_bytes": failure.heap_current_live_bytes,
                "heap_peak_live_bytes": failure.heap_peak_live_bytes,
                "heap_operation_peak_live_bytes": failure.heap_operation_peak_live_bytes,
                "heap_total_allocated_bytes": failure.heap_total_allocated_bytes,
                "heap_allocation_count": failure.heap_allocation_count,
                "heap_deallocation_count": failure.heap_deallocation_count,
                "heap_failed_allocation_size_bytes": failure.heap_failed_allocation_size_bytes,
            })
    return True


def merge_resource_summary_into_failure(
    failure: WorkerFailureInfo,
    resource_summary: Dict[str, Any],
) -> WorkerFailureInfo:
    """Attach container/cgroup evidence without replacing runner attribution."""
    failure.worker_id = str(resource_summary.get("worker_id") or failure.worker_id)
    failure.physical_worker_id = str(
        resource_summary.get("physical_worker_id") or failure.physical_worker_id
    )
    failure.logical_client_id = str(
        resource_summary.get("logical_client_id") or failure.logical_client_id
    )
    failure.container_name = str(resource_summary.get("container_name") or failure.container_name)
    failure.container_id = str(resource_summary.get("container_id") or failure.container_id)
    failure.resource_profile_id = str(
        resource_summary.get("resource_profile_id") or failure.resource_profile_id
    )
    failure.experiment_kind = str(
        resource_summary.get("experiment_kind") or failure.experiment_kind
    )
    failure.container_exit_code = _safe_optional_int(
        resource_summary.get("last_container_exit_code")
    )
    failure.container_oom_killed = _safe_bool(
        resource_summary.get("last_container_oom_killed")
    )
    failure.memory_events_oom = _safe_int(resource_summary.get("memory_events_oom"))
    failure.memory_events_oom_kill = _safe_int(resource_summary.get("memory_events_oom_kill"))
    failure.max_memory_current = _safe_int(resource_summary.get("max_memory_current"))
    failure.cpu_nr_throttled_delta = _safe_int(resource_summary.get("cpu_nr_throttled_delta"))
    failure.cpu_throttled_usec_delta = _safe_int(resource_summary.get("cpu_throttled_usec_delta"))
    failure.cpu_throttled_time_fraction = _safe_float(
        resource_summary.get("cpu_throttled_time_fraction")
    )
    failure.last_container_status = str(resource_summary.get("last_container_status") or "")
    return failure


def collect_worker_failures_from_artifacts(
    events_csv_path: str,
    resource_summaries: List[Dict[str, Any]],
    oom_events_path: Optional[str] = None,
    terminal_output_path: Optional[str] = None,
) -> List[WorkerFailureInfo]:
    """Merge runner failure events with hard container/cgroup failure evidence.

    CPU throttling and non-fatal memory pressure are measurements, not failures.
    They only explain a failure when a runner event already reports one.
    """
    failures = worker_failures_from_events_csv(events_csv_path)
    existing_clients = {failure.logical_client_id for failure in failures}
    run_dir = os.path.dirname(os.path.abspath(events_csv_path)) if events_csv_path else ""
    runner_events_path = os.path.join(run_dir, "runner-events.jsonl")
    for failure in worker_failures_from_runner_events_jsonl(runner_events_path):
        if failure.logical_client_id not in existing_clients:
            failures.append(failure)
            existing_clients.add(failure.logical_client_id)
    for failure in worker_failures_from_terminal_output(terminal_output_path):
        if failure.logical_client_id not in existing_clients:
            failures.append(failure)
            existing_clients.add(failure.logical_client_id)
    by_client = {failure.logical_client_id: failure for failure in failures}

    for summary in resource_summaries:
        client_id = str(summary.get("logical_client_id") or "")
        existing = by_client.get(client_id)
        if existing is not None:
            merge_resource_summary_into_failure(existing, summary)
            hard_class, _ = classify_worker_failure_from_resource_summary(summary, oom_events_path)
            if hard_class in ("hard_ram_oom_kill", "hard_container_exit"):
                existing.failure_class = hard_class
            continue

        resource_class, resource_failure = classify_worker_failure_from_resource_summary(
            summary, oom_events_path
        )
        if resource_class not in ("hard_ram_oom_kill", "hard_container_exit"):
            continue
        resource_failure.attribution_confidence = "exact_container_evidence"
        resource_failure.attribution_source = "resource_monitor"
        if not resource_failure.failure_evidence_source:
            resource_failure.failure_evidence_source = "docker_or_cgroup"
        by_client[client_id] = resource_failure
        failures.append(resource_failure)

    failures.sort(key=lambda failure: failure.failure_timestamp_ns)
    return failures


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
        if not wf.last_observed_span_name:
            wf.last_observed_span_name = cursor.get("span_name", "")
        if not wf.last_observed_span_id:
            wf.last_observed_span_id = cursor.get("span_id", "")
        if wf.current_member_count == 0:
            wf.current_member_count = cursor.get("member_count", 0)
        if wf.current_epoch == 0:
            wf.current_epoch = cursor.get("group_epoch", 0)
        if wf.failure_timestamp_ns == 0:
            wf.failure_timestamp_ns = cursor.get("failure_timestamp_ns", 0)
        if not wf.failure_detail:
            wf.failure_detail = cursor.get("failure_detail", "")
        if not wf.failure_evidence_source:
            wf.failure_evidence_source = cursor.get("failure_evidence_source", "")
        if not wf.failure_evidence_detail:
            wf.failure_evidence_detail = cursor.get("failure_evidence_detail", "")
        if not wf.failure_action:
            wf.failure_action = cursor.get("failure_action", "")
        if not wf.attribution_confidence:
            wf.attribution_confidence = cursor.get("attribution_confidence", "")
        if not wf.attribution_source:
            wf.attribution_source = cursor.get("attribution_source", "")
        if not wf.last_successful_phase:
            wf.last_successful_phase = cursor.get("last_successful_phase", "")
        if not wf.last_successful_operation_family:
            wf.last_successful_operation_family = cursor.get(
                "last_successful_operation_family", ""
            )
        if not wf.last_successful_benchmark_operation:
            wf.last_successful_benchmark_operation = cursor.get(
                "last_successful_benchmark_operation", ""
            )
        if wf.last_successful_member_count == 0:
            wf.last_successful_member_count = cursor.get("last_successful_member_count", 0)
        if wf.last_successful_epoch == 0:
            wf.last_successful_epoch = cursor.get("last_successful_epoch", 0)

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
        "app_heap_budget_exceeded": "app_heap_budget_exceeded",
        "app_heap_budget_allocator_abort": "app_heap_budget_allocator_abort",
        "embedded_budget_timeout": "embedded_budget_timeout",
    }
    return mapping.get(runner_class, runner_class)
