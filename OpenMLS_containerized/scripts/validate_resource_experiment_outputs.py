#!/usr/bin/env python3
"""
Validate resource experiment output files.

Usage:
    python3 validate_resource_experiment_outputs.py <run_directory>

Checks that all required sidecar files exist, CSV headers match expected
schemas, JSON files parse correctly, and cross-file joins are valid.

Exits with code 0 on success, non-zero on validation failure.
"""

import argparse
import csv
import json
import os
import sys
from typing import Dict, List, Optional, Set


EXPECTED_FILES = [
    "resource_profiles.csv",
    "worker_resource_assignments.csv",
    "cpu_affinity_plan.json",
    "cpu_affinity_preflight.csv",
    "cpu_affinity_preflight_summary.json",
    "resource_samples.jsonl",
    "resource_summary.csv",
    "worker_failures.csv",
    "run_status.csv",
    "benchmark_outcome.json",
]


RESOURCE_PROFILES_HEADER = [
    "run_id", "resource_profile_id", "experiment_kind",
    "resource_profile_index", "profile_label", "selected_for_this_run",
    "cpu_limit_cpus", "capacity_fraction", "assigned_cpu_count",
    "memory_limit", "memory_swap", "memory_model", "docker_memory_limit",
    "app_heap_budget", "app_heap_budget_bytes", "rayon_num_threads",
    "cpuset_cpus", "cpuset_mask_hex", "cpuset_role", "profile_notes",
    "sweep_kind", "app_heap_interpretation", "cpu_interpretation",
    "cpu_period_us", "cpu_quota_us", "group_creator", "group_creator_reason",
    "strict_cpuset_satisfied",
]

WORKER_RESOURCE_ASSIGNMENTS_HEADER = [
    "run_id", "logical_client_id", "worker_id", "physical_worker_id",
    "container_name", "container_id", "container_mode", "profile_enabled",
    "resource_profile_index", "resource_profile_id", "experiment_kind",
    "selected_for_this_run", "cpu_affinity_role", "cpuset_cpus",
    "cpuset_mask_hex", "cpu_limit_cpus", "capacity_fraction",
    "assigned_cpu_count", "memory_limit", "memory_swap", "memory_model",
    "docker_memory_limit", "app_heap_budget", "app_heap_budget_bytes",
    "rayon_num_threads",
    "background_cpuset_cpus", "background_mask_hex", "profile_label",
    "sweep_kind", "app_heap_interpretation", "cpu_interpretation",
    "cpu_period_us", "cpu_quota_us",
    "group_creator", "group_creator_reason", "strict_cpuset_satisfied",
]

PREFLIGHT_HEADER = [
    "run_id", "check_name", "container_name", "container_role",
    "expected_cpuset", "docker_cpuset", "host_pid",
    "proc_cpus_allowed_list", "thread_cpus_allowed_lists",
    "observed_psr_cpus", "status", "message",
]

RESOURCE_SUMMARY_HEADER = [
    "run_id", "worker_id", "physical_worker_id", "logical_client_id",
    "container_name", "resource_profile_id", "experiment_kind",
    "cpuset_cpus", "cpu_limit_cpus", "memory_limit", "memory_swap",
    "rayon_num_threads", "sample_count", "max_memory_current",
    "last_memory_current", "memory_events_oom", "memory_events_oom_kill",
    "cpu_usage_usec_delta", "cpu_nr_throttled_delta",
    "cpu_throttled_usec_delta", "cpu_throttled_time_fraction",
    "max_thread_count", "max_process_count", "last_container_status",
    "last_container_exit_code", "last_container_oom_killed",
]

WORKER_FAILURES_HEADER = [
    "run_id", "worker_id", "physical_worker_id", "logical_client_id",
    "container_name", "container_id", "resource_profile_id",
    "experiment_kind", "failure_class", "failure_detail",
    "failure_evidence_source", "failure_evidence_detail", "failure_action",
    "attribution_confidence", "attribution_source", "failure_timestamp_ns",
    "last_successful_phase", "last_successful_operation_family",
    "last_successful_benchmark_operation", "last_successful_member_count",
    "last_successful_epoch", "current_phase", "current_operation_family",
    "current_benchmark_operation", "last_observed_span_name",
    "last_observed_span_id", "current_member_count", "current_epoch",
    "memory_model", "app_heap_budget", "app_heap_budget_bytes",
    "heap_current_live_bytes", "heap_peak_live_bytes",
    "heap_operation_peak_live_bytes", "heap_total_allocated_bytes",
    "heap_allocation_count", "heap_deallocation_count",
    "heap_failed_allocation_size_bytes",
    "container_exit_code", "container_oom_killed", "memory_events_oom",
    "memory_events_oom_kill", "max_memory_current",
    "cpu_nr_throttled_delta", "cpu_throttled_usec_delta",
    "cpu_throttled_time_fraction", "last_container_status",
    "diagnostic_log_path",
    "deadline_ns", "wall_ns", "sweep_kind", "cpu_period_us", "cpu_quota_us",
]

RUN_STATUS_HEADER = [
    "run_id", "run_mode", "resource_experiment", "resource_failure_policy",
    "resource_profile_index", "resource_profile_id", "experiment_kind",
    "memory_model", "docker_memory_limit", "app_heap_budget",
    "app_heap_budget_bytes", "run_status", "completed",
    "valid_for_threshold_analysis", "valid_for_embedded_heap_threshold_analysis",
    "valid_for_docker_resource_analysis",
    "valid_for_clean_performance_plots", "valid_for_churn_recovery_analysis",
    "first_failure_timestamp_ns", "first_failed_worker_id",
    "first_failed_client_id", "first_failure_class",
    "first_failure_operation_family", "first_failure_benchmark_operation",
    "first_failure_member_count", "first_failure_epoch",
    "last_successful_operation_family", "last_successful_benchmark_operation",
    "last_successful_member_count", "last_successful_epoch",
    "preflight_passed", "resource_output_validation_passed",
    "sweep_kind", "strict_cpuset_satisfied", "notes",
]

FORBIDDEN_COLUMN_SUBSTRINGS = ["pids_limit"]


class ValidationError(Exception):
    pass


def _safe_int(value) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _effective_cpu_fraction(row) -> Optional[float]:
    cpu_quota_us = _safe_int(row.get("cpu_quota_us"))
    cpu_period_us = _safe_int(row.get("cpu_period_us"))
    if cpu_period_us > 0 and cpu_quota_us > 0:
        return cpu_quota_us / cpu_period_us

    capacity_fraction = _safe_float(row.get("capacity_fraction"))
    if capacity_fraction > 0:
        return capacity_fraction

    cpu_limit = _safe_float(row.get("cpu_limit_cpus"))
    assigned_count = _safe_float(row.get("assigned_cpu_count"))
    if cpu_limit > 0 and assigned_count > 0:
        return cpu_limit / assigned_count

    return None


def parse_memory_to_bytes(value: str) -> int:
    raw = (value or "").strip().lower()
    if not raw:
        return 0
    digits = ""
    unit = ""
    for ch in raw:
        if ch.isdigit() and not unit:
            digits += ch
        else:
            unit += ch
    if not digits:
        return 0
    multiplier = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024 * 1024,
        "mb": 1024 * 1024,
        "mib": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
        "gib": 1024 * 1024 * 1024,
    }.get(unit, 0)
    return int(digits) * multiplier


class Validator:
    def __init__(self, run_dir: str):
        self.run_dir = run_dir
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def error(self, msg: str):
        self.errors.append(msg)

    def warn(self, msg: str):
        self.warnings.append(msg)

    def validate(self) -> bool:
        try:
            self._check_files_exist()
            self._check_csv_headers()
            self._check_json_files()
            self._check_forbidden_columns()
            self._check_resource_profile_joins()
            self._check_preflight_status()
            self._check_run_status()
            self._check_selected_profile()
            self._check_cpuset_overlaps()
            self._check_embedded_budget_run()
            self._check_parallel_sweep_run()
            return len(self.errors) == 0
        except ValidationError:
            return False

    def _check_files_exist(self):
        required = [
            "resource_profiles.csv",
            "cpu_affinity_plan.json",
        ]
        for fname in required:
            path = os.path.join(self.run_dir, fname)
            if not os.path.exists(path):
                self.error(f"Required file missing: {fname}")

        for fname in EXPECTED_FILES:
            path = os.path.join(self.run_dir, fname)
            if not os.path.exists(path):
                self.warn(f"Expected file not found: {fname}")

        run_status_path = os.path.join(self.run_dir, "run_status.csv")
        if not os.path.exists(run_status_path):
            self.error("Required file missing: run_status.csv")

    def _check_csv_headers(self):
        file_header_map = {
            "resource_profiles.csv": RESOURCE_PROFILES_HEADER,
            "worker_resource_assignments.csv": WORKER_RESOURCE_ASSIGNMENTS_HEADER,
            "cpu_affinity_preflight.csv": PREFLIGHT_HEADER,
            "resource_summary.csv": RESOURCE_SUMMARY_HEADER,
            "worker_failures.csv": WORKER_FAILURES_HEADER,
            "run_status.csv": RUN_STATUS_HEADER,
        }

        for fname, expected_header in file_header_map.items():
            path = os.path.join(self.run_dir, fname)
            if not os.path.exists(path):
                continue
            try:
                with open(path, newline="") as f:
                    reader = csv.reader(f)
                    try:
                        actual_header = next(reader)
                    except StopIteration:
                        self.warn(f"{fname} is empty (no header row)")
                        continue

                    if actual_header != expected_header:
                        missing = set(expected_header) - set(actual_header)
                        extra = set(actual_header) - set(expected_header)
                        if missing:
                            self.error(f"{fname}: missing columns: {sorted(missing)}")
                        if extra:
                            self.warn(f"{fname}: extra columns: {sorted(extra)}")
            except Exception as e:
                self.error(f"{fname}: failed to read: {e}")

    def _check_json_files(self):
        json_files = ["cpu_affinity_plan.json"]
        for fname in json_files:
            path = os.path.join(self.run_dir, fname)
            if not os.path.exists(path):
                continue
            try:
                with open(path) as f:
                    data = json.load(f)

                required_keys = [
                    "run_id", "online_cpu_mask_hex", "profiled_mask_hex",
                    "background_mask_hex", "profiled_assignments",
                ]
                for key in required_keys:
                    if key not in data:
                        self.error(f"{fname}: missing key '{key}'")

                if "profiled_assignments" in data and "background_assignments" in data:
                    profiled_mask = int(data.get("profiled_mask_hex", "0x0"), 16)
                    background_mask = int(data.get("background_mask_hex", "0x0"), 16)
                    if (profiled_mask & background_mask) != 0:
                        self.error(f"{fname}: profiled_mask overlaps with background_mask")
                    for assignment in data.get("profiled_assignments", []):
                        assigned_cpus = assignment.get("assigned_cpus", [])
                        assigned_count = int(assignment.get("assigned_cpu_count", 0) or 0)
                        if not assigned_cpus or assigned_count < 1:
                            self.error(
                                f"{fname}: profiled worker "
                                f"{assignment.get('worker_id', '<unknown>')} has no assigned CPU"
                            )
                        if assigned_count != len(assigned_cpus):
                            self.error(
                                f"{fname}: assigned_cpu_count={assigned_count} does not match "
                                f"assigned_cpus={assigned_cpus}"
                            )
                        rayon = int(assignment.get("rayon_num_threads", 0) or 0)
                        if rayon != assigned_count:
                            self.error(
                                f"{fname}: RAYON_NUM_THREADS={rayon} does not match "
                                f"assigned_cpu_count={assigned_count}"
                            )
                    if data.get("background_assignments") and background_mask == 0:
                        self.warn(f"{fname}: background mask is empty (background containers share CPUs with profiled workers)")

            except json.JSONDecodeError as e:
                self.error(f"{fname}: invalid JSON: {e}")
            except Exception as e:
                self.error(f"{fname}: {e}")

        pf_summary_path = os.path.join(self.run_dir, "cpu_affinity_preflight_summary.json")
        if os.path.exists(pf_summary_path):
            try:
                with open(pf_summary_path) as f:
                    pf_data = json.load(f)
                if not pf_data.get("all_passed", False):
                    self.error("cpu_affinity_preflight_summary.json: all_passed is false")
            except json.JSONDecodeError as e:
                self.error(f"cpu_affinity_preflight_summary.json: invalid JSON: {e}")

    def _check_forbidden_columns(self):
        csv_files = [f for f in EXPECTED_FILES if f.endswith(".csv")]
        for fname in csv_files:
            path = os.path.join(self.run_dir, fname)
            if not os.path.exists(path):
                continue
            try:
                with open(path, newline="") as f:
                    reader = csv.reader(f)
                    try:
                        header = next(reader)
                    except StopIteration:
                        continue

                    for col in header:
                        for forbidden in FORBIDDEN_COLUMN_SUBSTRINGS:
                            if forbidden in col.lower():
                                self.error(
                                    f"{fname}: forbidden column '{col}' "
                                    f"(matches '{forbidden}')"
                                )
            except Exception as e:
                self.error(f"{fname}: {e}")

    def _check_resource_profile_joins(self):
        profiles_path = os.path.join(self.run_dir, "resource_profiles.csv")
        if not os.path.exists(profiles_path):
            return

        profile_ids = self._read_column(profiles_path, "resource_profile_id")

        assignments_path = os.path.join(self.run_dir, "worker_resource_assignments.csv")
        if os.path.exists(assignments_path):
            assignment_profile_ids = self._read_column(assignments_path, "resource_profile_id")
            for pid in assignment_profile_ids:
                if pid and pid not in profile_ids:
                    self.warn(
                        f"worker_resource_assignments.csv references unknown "
                        f"resource_profile_id '{pid}'"
                    )

    def _check_preflight_status(self):
        preflight_path = os.path.join(self.run_dir, "cpu_affinity_preflight.csv")
        if not os.path.exists(preflight_path):
            return

        statuses = self._read_column(preflight_path, "status")
        has_fail = any(s.strip() == "FAIL" for s in statuses if s.strip())
        has_benchmark_data = self._has_benchmark_output()

        if has_fail and has_benchmark_data:
            self.error(
                "cpu_affinity_preflight.csv contains FAIL status, "
                "but benchmark output exists (should have been prevented)"
            )
        elif has_fail:
            self.warn(
                "cpu_affinity_preflight.csv contains FAIL status (no benchmark output, correct)"
            )

    def _check_selected_profile(self):
        profiles_path = os.path.join(self.run_dir, "resource_profiles.csv")
        if not os.path.exists(profiles_path):
            return

        status_rows = self._read_rows(os.path.join(self.run_dir, "run_status.csv"))
        sweep_kind = status_rows[0].get("sweep_kind", "").strip() if status_rows else ""
        is_parallel_sweep = sweep_kind in ("ram_app_heap_sweep", "cpu_quota_sweep")
        expected_selected = 10 if is_parallel_sweep else 1

        selected = self._read_column_values(profiles_path, "selected_for_this_run")
        true_count = sum(1 for v in selected if v.lower() in ("true", "1"))

        if true_count == 0:
            self.warn("No profile has selected_for_this_run=true (multiplexed mode?)")
        elif true_count != expected_selected:
            self.error(
                f"{true_count} profiles have selected_for_this_run=true "
                f"(expected exactly {expected_selected})"
            )

        assignments_path = os.path.join(self.run_dir, "worker_resource_assignments.csv")
        if os.path.exists(assignments_path):
            assign_selected = self._read_column_values(assignments_path, "selected_for_this_run")
            assign_true = sum(1 for v in assign_selected if v.lower() in ("true", "1"))
            if assign_true != expected_selected:
                self.error(
                    f"{assign_true} workers marked selected_for_this_run=true "
                    f"(expected exactly {expected_selected})"
                )

    def _check_run_status(self):
        status_path = os.path.join(self.run_dir, "run_status.csv")
        if not os.path.exists(status_path):
            return

        try:
            with open(status_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if len(rows) == 0:
                    self.error("run_status.csv has no rows")
                elif len(rows) > 1:
                    self.error(f"run_status.csv has {len(rows)} rows (expected exactly 1)")

                for row in rows:
                    valid_threshold = row.get("valid_for_threshold_analysis", "").strip().lower()
                    valid_perf = row.get("valid_for_clean_performance_plots", "").strip().lower()
                    completed = row.get("completed", "").strip().lower()

                    if completed == "true" and valid_threshold == "false":
                        self.error("run_status.csv: completed=true but valid_for_threshold_analysis=false")
                    if completed == "false" and valid_perf == "true":
                        self.error("run_status.csv: completed=false but valid_for_clean_performance_plots=true")

        except Exception as e:
            self.error(f"run_status.csv: {e}")

    def _check_cpuset_overlaps(self):
        plan_path = os.path.join(self.run_dir, "cpu_affinity_plan.json")
        if not os.path.exists(plan_path):
            return

        try:
            with open(plan_path) as f:
                data = json.load(f)

            profiled = data.get("profiled_assignments", [])
            if len(profiled) > 1:
                cpusets = []
                for pa in profiled:
                    cpus = set(pa.get("assigned_cpus", []))
                    cpusets.append((pa.get("container_name", ""), cpus))

                for i in range(len(cpusets)):
                    for j in range(i + 1, len(cpusets)):
                        overlap = cpusets[i][1] & cpusets[j][1]
                        if overlap:
                            self.error(
                                f"Profiled containers {cpusets[i][0]} and {cpusets[j][0]} "
                                f"share CPUs {sorted(overlap)}"
                            )
        except Exception as e:
            self.error(f"cpu_affinity_plan.json: {e}")

    def _check_embedded_budget_run(self):
        status_rows = self._read_rows(os.path.join(self.run_dir, "run_status.csv"))
        if not status_rows:
            return
        status = status_rows[0]
        is_embedded = (
            status.get("resource_experiment") == "embedded-budget-singleton"
            or status.get("experiment_kind") == "embedded_budget_singleton"
        )
        if not is_embedded:
            return

        required_sidecars = [
            "worker_resource_assignments.csv",
            "worker_failures.csv",
            "run_status.csv",
            "benchmark_outcome.json",
            "events.csv",
        ]
        for fname in required_sidecars:
            path = os.path.join(self.run_dir, fname)
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                self.error(f"embedded-budget run missing required sidecar: {fname}")

        if status.get("memory_model") != "app-heap-budget":
            self.error("run_status.csv: embedded-budget run must use memory_model=app-heap-budget")
        if status.get("valid_for_embedded_heap_threshold_analysis", "").lower() != "true":
            self.error("run_status.csv: embedded-budget run must be valid for embedded heap threshold analysis")
        if status.get("valid_for_docker_resource_analysis", "").lower() != "false":
            self.error("run_status.csv: embedded-budget run must not be valid for Docker resource analysis")
        if not status.get("app_heap_budget") or _safe_int(status.get("app_heap_budget_bytes")) <= 0:
            self.error("run_status.csv: embedded-budget run must include app_heap_budget and bytes")

        profiles = self._read_rows(os.path.join(self.run_dir, "resource_profiles.csv"))
        selected_profiles = [
            row for row in profiles
            if (row.get("selected_for_this_run") or "").lower() in ("true", "1")
        ]
        if len(selected_profiles) != 1:
            self.error(f"embedded-budget run must select exactly one resource profile, found {len(selected_profiles)}")
            selected = selected_profiles[0] if selected_profiles else {}
        else:
            selected = selected_profiles[0]

        if selected:
            if selected.get("memory_model") != "app-heap-budget":
                self.error("resource_profiles.csv: selected embedded profile must use memory_model=app-heap-budget")
            if selected.get("experiment_kind") != "embedded_budget_singleton":
                self.error("resource_profiles.csv: selected profile must be embedded_budget_singleton")
            if selected.get("resource_profile_index") != status.get("resource_profile_index"):
                self.error("selected resource_profile_index does not match run_status.csv")
            if selected.get("resource_profile_id") != status.get("resource_profile_id"):
                self.error("selected resource_profile_id does not match run_status.csv")

            docker_memory = selected.get("docker_memory_limit") or selected.get("memory_limit")
            docker_bytes = parse_memory_to_bytes(docker_memory)
            app_bytes = _safe_int(selected.get("app_heap_budget_bytes"))
            if docker_bytes < 6 * 1024 * 1024:
                self.error("selected embedded profile Docker memory is below safe Linux/container minimum")
            if app_bytes <= 0:
                self.error("selected embedded profile missing app_heap_budget_bytes")
            if docker_bytes and app_bytes and docker_bytes <= app_bytes:
                self.error("selected embedded profile Docker memory must be above app heap budget")
            if (selected.get("memory_limit") or "") == (selected.get("app_heap_budget") or ""):
                self.error("selected embedded profile accidentally uses app heap budget as Docker memory limit")
            if not selected.get("cpu_limit_cpus"):
                self.error("selected embedded profile missing Docker CPU quota")
            if not selected.get("cpuset_cpus"):
                self.error("selected embedded profile missing cpuset_cpus")

        assignments = self._read_rows(os.path.join(self.run_dir, "worker_resource_assignments.csv"))
        selected_assignments = [
            row for row in assignments
            if (row.get("selected_for_this_run") or "").lower() in ("true", "1")
        ]
        profiled_singletons = [
            row for row in assignments
            if row.get("container_mode") == "singleton"
            and (row.get("profile_enabled") or "").lower() in ("true", "1")
        ]
        if len(selected_assignments) != 1:
            self.error(f"embedded-budget run must assign exactly one selected profiled singleton, found {len(selected_assignments)}")
        if len(profiled_singletons) != 1:
            self.error(f"embedded-budget run must have exactly one profiled singleton, found {len(profiled_singletons)}")
        if selected_assignments:
            assignment = selected_assignments[0]
            if assignment.get("memory_model") != "app-heap-budget":
                self.error("worker_resource_assignments.csv: selected assignment missing memory_model=app-heap-budget")
            if not assignment.get("app_heap_budget"):
                self.error("worker_resource_assignments.csv: selected assignment missing app_heap_budget")
            if not assignment.get("cpu_limit_cpus"):
                self.error("worker_resource_assignments.csv: selected assignment missing Docker CPU quota")
            if not assignment.get("cpuset_cpus"):
                self.error("worker_resource_assignments.csv: selected assignment missing cpuset")
            if selected and assignment.get("resource_profile_index") != selected.get("resource_profile_index"):
                self.error("worker assignment resource_profile_index does not match selected profile")
            if selected and assignment.get("cpu_limit_cpus") != selected.get("cpu_limit_cpus"):
                self.error("worker assignment CPU quota does not match selected profile")

        failures = self._read_rows(os.path.join(self.run_dir, "worker_failures.csv"))
        heap_failures = [row for row in failures if row.get("failure_class") == "app_heap_budget_exceeded"]
        hard_oom_heap = [
            row for row in failures
            if row.get("failure_class") == "hard_ram_oom_kill"
            and "APP_HEAP_BUDGET_EXCEEDED" in (row.get("failure_detail") or "")
        ]
        if hard_oom_heap:
            self.error("worker_failures.csv misclassified app heap budget failure as hard_ram_oom_kill")

        first_class = status.get("first_failure_class", "")
        if first_class == "app_heap_budget_exceeded":
            if not heap_failures:
                self.error("run_status.csv reports app_heap_budget_exceeded but worker_failures.csv has no matching row")
            if not status.get("first_failure_operation_family") or not status.get("first_failure_benchmark_operation"):
                self.error("run_status.csv heap-budget failure lacks operation attribution")
            for row in heap_failures:
                if not row.get("current_operation_family") or not row.get("current_benchmark_operation"):
                    self.error("worker_failures.csv heap-budget failure lacks operation attribution")
                if _safe_int(row.get("heap_operation_peak_live_bytes")) <= 0:
                    self.error("worker_failures.csv heap-budget failure lacks operation peak heap state")
                if not row.get("app_heap_budget") or _safe_int(row.get("app_heap_budget_bytes")) <= 0:
                    self.error("worker_failures.csv heap-budget failure lacks app heap budget")

    def _check_parallel_sweep_run(self):
        status_rows = self._read_rows(os.path.join(self.run_dir, "run_status.csv"))
        if not status_rows:
            return
        status = status_rows[0]

        sweep_kind = status.get("sweep_kind", "").strip()
        if not sweep_kind:
            return

        is_ram = sweep_kind == "ram_app_heap_sweep"
        is_cpu = sweep_kind == "cpu_quota_sweep"
        if not is_ram and not is_cpu:
            return

        assignments = self._read_rows(os.path.join(self.run_dir, "worker_resource_assignments.csv"))
        profiled_singletons = [
            row for row in assignments
            if row.get("container_mode") == "singleton"
            and (row.get("profile_enabled") or "").lower() in ("true", "1")
        ]

        if len(profiled_singletons) != 10:
            self.error(f"Parallel {sweep_kind} must have exactly 10 profiled singleton workers, found {len(profiled_singletons)}")

        packed_profiled = [
            row for row in assignments
            if row.get("container_mode") == "packed"
            and (row.get("profile_enabled") or "").lower() in ("true", "1")
        ]
        if packed_profiled:
            self.error(f"Packed containers must not be profiled: {[r.get('container_name') for r in packed_profiled]}")

        group_creator_singletons = [
            row for row in profiled_singletons
            if (row.get("group_creator") or "").lower() in ("true", "1")
        ]
        if len(group_creator_singletons) != 1:
            self.error(f"Parallel {sweep_kind} must have exactly 1 group creator singleton, found {len(group_creator_singletons)}")

        profiles = self._read_rows(os.path.join(self.run_dir, "resource_profiles.csv"))

        if is_ram:
            expected_budgets = sorted(
                [p.get("app_heap_budget", "").strip().lower() for p in profiles if p.get("app_heap_budget")]
            )
            actual_budgets = sorted(
                [r.get("app_heap_budget", "").strip().lower() for r in profiled_singletons if r.get("app_heap_budget")]
            )
            if expected_budgets and actual_budgets and actual_budgets != expected_budgets:
                self.error(f"RAM sweep profiled workers have mismatched heap budgets vs profiles: {actual_budgets} vs expected {expected_budgets}")

            group_creators = [
                r for r in profiled_singletons
                if (r.get("group_creator") or "").lower() in ("true", "1")
            ]
            if group_creators and actual_budgets:
                gc = group_creators[0]
                gc_budget = (gc.get("app_heap_budget") or "").strip().lower()
                max_budget = max(actual_budgets, key=lambda b: parse_memory_to_bytes(b))
                if gc_budget and max_budget and gc_budget != max_budget:
                    self.error(
                        f"RAM sweep group creator has heap budget '{gc_budget}', "
                        f"but must be the highest budget worker ('{max_budget}')"
                    )

            for r in profiled_singletons:
                if r.get("memory_model") != "app-heap-budget":
                    self.error(f"RAM sweep worker {r.get('container_name')} must use memory_model=app-heap-budget")

        if is_cpu:
            profiled_fractions = sorted(
                [_safe_float(r.get("capacity_fraction")) for r in profiled_singletons if r.get("capacity_fraction")],
                reverse=True,
            )
            selected_profile_fractions = sorted(
                [
                    _safe_float(r.get("capacity_fraction"))
                    for r in profiles
                    if (r.get("selected_for_this_run") or "").strip().lower() in ("true", "1", "yes")
                    and r.get("capacity_fraction")
                ],
                reverse=True,
            )
            if selected_profile_fractions and profiled_fractions != selected_profile_fractions:
                self.error(
                    f"CPU sweep profiled workers have wrong CPU fractions: "
                    f"{profiled_fractions} vs selected profiles {selected_profile_fractions}"
                )

            for fraction in profiled_fractions:
                if fraction < 0.01:
                    self.error(
                        f"CPU sweep contains requested fraction {fraction} which is below "
                        f"the Docker hard-quota floor 0.01. This profile is not valid on this "
                        f"benchmark configuration."
                    )

            distinct_effective = set()
            for r in profiled_singletons:
                effective = _effective_cpu_fraction(r)
                if effective is not None:
                    distinct_effective.add(round(effective, 8))
            if len(distinct_effective) < len(profiled_singletons):
                self.error(
                    f"CPU sweep has collapsed CPU profiles: only {len(distinct_effective)} "
                    f"distinct effective CPU fractions among {len(profiled_singletons)} profiled "
                    f"workers. Check cgroup cpu.max floor enforcement."
                )

            group_creators = [
                r for r in profiled_singletons
                if (r.get("group_creator") or "").lower() in ("true", "1")
            ]
            if group_creators:
                gc = group_creators[0]
                gc_fraction = _safe_float(gc.get("capacity_fraction"))
                if gc_fraction < 0.99:
                    self.error(
                        f"CPU sweep group creator has CPU fraction {gc_fraction}, "
                        f"but must be the 1.00 CPU worker"
                    )

            for r in profiled_singletons:
                ahb = r.get("app_heap_budget", "").strip().lower()
                ahb_bytes = _safe_int(r.get("app_heap_budget_bytes"))
                if "64" not in ahb and ahb_bytes < 64 * 1024 * 1024 * 1024:
                    self.error(f"CPU sweep worker {r.get('container_name')} must use 64 GiB app heap budget")

        online_cpus = self._get_online_cpu_count()
        profiled_cpu_assignments = []
        plan_path = os.path.join(self.run_dir, "cpu_affinity_plan.json")
        if os.path.exists(plan_path):
            try:
                with open(plan_path) as f:
                    plan = json.load(f)
                for pa in plan.get("profiled_assignments", []):
                    profiled_cpu_assignments.append(set(pa.get("assigned_cpus", [])))
            except Exception:
                pass

        if profiled_cpu_assignments:
            all_profiled_cpus = set()
            for cset in profiled_cpu_assignments:
                all_profiled_cpus |= cset
            if len(all_profiled_cpus) < 8:
                unique_cores = len(all_profiled_cpus)
                msg = (
                    f"Parallel {sweep_kind}: only {unique_cores} distinct profiled CPU cores available "
                    f"(need 8). strict CPU isolation not verifiable on this host."
                )
                if online_cpus > 0 and online_cpus < 8:
                    msg += f" Host has only {online_cpus} online CPUs."
                self.warn(msg)
            elif len(all_profiled_cpus) >= 8:
                pass

    def _get_online_cpu_count(self) -> int:
        try:
            from cpu_topology import get_online_cpu_list
            return len(get_online_cpu_list())
        except Exception:
            return 0

    def _read_rows(self, csv_path: str) -> List[Dict[str, str]]:
        if not os.path.exists(csv_path):
            return []
        try:
            with open(csv_path, newline="") as f:
                return list(csv.DictReader(f))
        except Exception:
            return []

    def _read_column(self, csv_path: str, column_name: str) -> Set[str]:
        return {v for v in self._read_column_values(csv_path, column_name) if v}

    def _read_column_values(self, csv_path: str, column_name: str) -> List[str]:
        values: List[str] = []
        try:
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = (row.get(column_name) or "").strip()
                    values.append(val)
        except Exception:
            pass
        return values

    def _has_benchmark_output(self) -> bool:
        events_path = os.path.join(self.run_dir, "events.csv")
        return os.path.exists(events_path) and os.path.getsize(events_path) > 0

    def report(self) -> str:
        lines = []
        lines.append(f"Validation of {self.run_dir}")
        lines.append(f"  Errors: {len(self.errors)}")
        for e in self.errors:
            lines.append(f"    ERROR: {e}")
        lines.append(f"  Warnings: {len(self.warnings)}")
        for w in self.warnings:
            lines.append(f"    WARN: {w}")
        return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Validate resource experiment output files"
    )
    parser.add_argument(
        "run_dir",
        help="Path to the benchmark run output directory",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as errors",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.run_dir):
        print(f"ERROR: {args.run_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    validator = Validator(args.run_dir)
    success = validator.validate()
    print(validator.report())

    if not success:
        sys.exit(1)
    if args.strict and validator.warnings:
        print(f"Strict mode: {len(validator.warnings)} warnings treated as errors")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
