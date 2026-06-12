"""
Sidecar file writers for resource experiment output files.

Writes resource_profiles.csv, worker_resource_assignments.csv,
resource_summary.csv, worker_failures.csv, run_status.csv,
and benchmark_timeline.csv.
"""

import csv
import json
import os
import time
from typing import Any, Dict, List, Optional


RESOURCE_PROFILES_HEADER = [
    "run_id",
    "resource_profile_id",
    "experiment_kind",
    "resource_profile_index",
    "profile_label",
    "selected_for_this_run",
    "cpu_limit_cpus",
    "capacity_fraction",
    "assigned_cpu_count",
    "memory_limit",
    "memory_swap",
    "rayon_num_threads",
    "cpuset_cpus",
    "cpuset_mask_hex",
    "cpuset_role",
    "profile_notes",
]


WORKER_RESOURCE_ASSIGNMENTS_HEADER = [
    "run_id",
    "logical_client_id",
    "worker_id",
    "physical_worker_id",
    "container_name",
    "container_id",
    "container_mode",
    "profile_enabled",
    "resource_profile_index",
    "resource_profile_id",
    "experiment_kind",
    "selected_for_this_run",
    "cpu_affinity_role",
    "cpuset_cpus",
    "cpuset_mask_hex",
    "cpu_limit_cpus",
    "capacity_fraction",
    "assigned_cpu_count",
    "memory_limit",
    "memory_swap",
    "rayon_num_threads",
    "background_cpuset_cpus",
    "background_mask_hex",
    "profile_label",
]


CPU_AFFINITY_PREFLIGHT_HEADER = [
    "run_id",
    "check_name",
    "container_name",
    "container_role",
    "expected_cpuset",
    "docker_cpuset",
    "host_pid",
    "proc_cpus_allowed_list",
    "thread_cpus_allowed_lists",
    "observed_psr_cpus",
    "status",
    "message",
]


RESOURCE_SUMMARY_HEADER = [
    "run_id",
    "worker_id",
    "physical_worker_id",
    "logical_client_id",
    "container_name",
    "resource_profile_id",
    "experiment_kind",
    "cpuset_cpus",
    "cpu_limit_cpus",
    "memory_limit",
    "memory_swap",
    "rayon_num_threads",
    "sample_count",
    "max_memory_current",
    "last_memory_current",
    "memory_events_oom",
    "memory_events_oom_kill",
    "cpu_usage_usec_delta",
    "cpu_nr_throttled_delta",
    "cpu_throttled_usec_delta",
    "cpu_throttled_time_fraction",
    "max_thread_count",
    "max_process_count",
    "last_container_status",
    "last_container_exit_code",
    "last_container_oom_killed",
]


WORKER_FAILURES_HEADER = [
    "run_id",
    "worker_id",
    "physical_worker_id",
    "logical_client_id",
    "container_name",
    "container_id",
    "resource_profile_id",
    "experiment_kind",
    "failure_class",
    "failure_timestamp_ns",
    "last_successful_phase",
    "last_successful_operation_family",
    "last_successful_benchmark_operation",
    "last_successful_member_count",
    "last_successful_epoch",
    "current_phase",
    "current_operation_family",
    "current_benchmark_operation",
    "current_member_count",
    "current_epoch",
    "container_exit_code",
    "container_oom_killed",
    "memory_events_oom",
    "memory_events_oom_kill",
    "max_memory_current",
    "cpu_nr_throttled_delta",
    "cpu_throttled_usec_delta",
    "cpu_throttled_time_fraction",
    "diagnostic_log_path",
]


RUN_STATUS_HEADER = [
    "run_id",
    "run_mode",
    "resource_experiment",
    "resource_failure_policy",
    "resource_profile_index",
    "resource_profile_id",
    "experiment_kind",
    "run_status",
    "completed",
    "valid_for_threshold_analysis",
    "valid_for_clean_performance_plots",
    "valid_for_churn_recovery_analysis",
    "first_failure_timestamp_ns",
    "first_failed_worker_id",
    "first_failed_client_id",
    "first_failure_class",
    "first_failure_operation_family",
    "first_failure_benchmark_operation",
    "first_failure_member_count",
    "first_failure_epoch",
    "last_successful_operation_family",
    "last_successful_benchmark_operation",
    "last_successful_member_count",
    "last_successful_epoch",
    "preflight_passed",
    "resource_output_validation_passed",
    "notes",
]


BENCHMARK_TIMELINE_HEADER = [
    "timestamp_ns",
    "run_id",
    "phase",
    "operation_family",
    "benchmark_operation",
    "commit_kind",
    "epoch",
    "member_count",
    "actor_client_id",
    "target_client_id",
    "worker_id",
    "physical_worker_id",
    "status",
    "details",
]


class SidecarWriter:
    """Writes resource experiment sidecar files incrementally."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def _ensure_dir(self):
        os.makedirs(self.output_dir, exist_ok=True)

    def write_csv(self, filename: str, header: List[str], rows: List[Dict[str, Any]]):
        """Write a CSV file with given header and rows."""
        self._ensure_dir()
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                safe_row = {k: _safe_csv_value(v) for k, v in row.items()}
                writer.writerow(safe_row)
        return filepath

    def append_csv_row(self, filename: str, header: List[str], row: Dict[str, Any]):
        """Append a single row to a CSV file. Creates file with header if needed."""
        self._ensure_dir()
        filepath = os.path.join(self.output_dir, filename)
        file_exists = os.path.exists(filepath)
        with open(filepath, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            safe_row = {k: _safe_csv_value(v) for k, v in row.items()}
            writer.writerow(safe_row)

    def write_jsonl_line(self, filename: str, data: Dict[str, Any]):
        """Append a JSON line to a JSONL file."""
        self._ensure_dir()
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "a") as f:
            f.write(json.dumps(data) + "\n")

    def write_json(self, filename: str, data: Dict[str, Any]):
        """Write a JSON file."""
        self._ensure_dir()
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

    def write_resource_profiles(self, run_id: str, profiles: List[Dict[str, Any]]):
        """Write resource_profiles.csv from a list of profile dicts."""
        rows = []
        for p in profiles:
            row = {
                "run_id": run_id,
                **p,
            }
            rows.append(row)
        return self.write_csv("resource_profiles.csv", RESOURCE_PROFILES_HEADER, rows)

    def write_worker_resource_assignments(self, run_id: str, assignments: List[Dict[str, Any]]):
        """Write worker_resource_assignments.csv."""
        rows = []
        for a in assignments:
            row = {"run_id": run_id, **a}
            rows.append(row)
        return self.write_csv("worker_resource_assignments.csv", WORKER_RESOURCE_ASSIGNMENTS_HEADER, rows)

    def write_preflight_results(self, run_id: str, results: List[Dict[str, Any]]):
        """Write cpu_affinity_preflight.csv."""
        rows = []
        for r in results:
            row = {"run_id": run_id, **r}
            rows.append(row)
        return self.write_csv("cpu_affinity_preflight.csv", CPU_AFFINITY_PREFLIGHT_HEADER, rows)

    def write_resource_summary(self, run_id: str, summaries: List[Dict[str, Any]]):
        """Write resource_summary.csv."""
        rows = []
        for s in summaries:
            row = {"run_id": run_id, **s}
            rows.append(row)
        return self.write_csv("resource_summary.csv", RESOURCE_SUMMARY_HEADER, rows)

    def write_worker_failures(self, run_id: str, failures: List[Dict[str, Any]]):
        """Write worker_failures.csv."""
        rows = []
        for f in failures:
            row = {"run_id": run_id, **f}
            rows.append(row)
        return self.write_csv("worker_failures.csv", WORKER_FAILURES_HEADER, rows)

    def write_run_status(self, run_id: str, status: Dict[str, Any]):
        """Write run_status.csv."""
        row = {"run_id": run_id, **status}
        return self.write_csv("run_status.csv", RUN_STATUS_HEADER, [row])

    def append_benchmark_timeline(self, run_id: str, event: Dict[str, Any]):
        """Append a timeline event to benchmark_timeline.csv."""
        row = {"run_id": run_id, **event}
        return self.append_csv_row("benchmark_timeline.csv", BENCHMARK_TIMELINE_HEADER, row)


def _safe_csv_value(value: Any) -> str:
    """Convert a value to a CSV-safe string."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value:.6f}"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value)
    return str(value)


VALIDATOR_SCHEMAS = {
    "resource_profiles.csv": RESOURCE_PROFILES_HEADER,
    "worker_resource_assignments.csv": WORKER_RESOURCE_ASSIGNMENTS_HEADER,
    "cpu_affinity_preflight.csv": CPU_AFFINITY_PREFLIGHT_HEADER,
    "resource_summary.csv": RESOURCE_SUMMARY_HEADER,
    "worker_failures.csv": WORKER_FAILURES_HEADER,
    "run_status.csv": RUN_STATUS_HEADER,
    "benchmark_timeline.csv": BENCHMARK_TIMELINE_HEADER,
}


def get_expected_files() -> List[str]:
    """Get the list of expected sidecar files."""
    return [
        "cpu_affinity_plan.json",
        "cpu_affinity_preflight.csv",
        "cpu_affinity_preflight_summary.json",
        "resource_profiles.csv",
        "resource_profiles.json",
        "worker_resource_assignments.csv",
        "resource_samples.jsonl",
        "resource_summary.csv",
        "worker_failures.csv",
        "run_status.csv",
        "benchmark_outcome.json",
    ]
