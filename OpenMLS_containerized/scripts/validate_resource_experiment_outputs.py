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
    "memory_limit", "memory_swap", "rayon_num_threads",
    "cpuset_cpus", "cpuset_mask_hex", "cpuset_role", "profile_notes",
]

WORKER_RESOURCE_ASSIGNMENTS_HEADER = [
    "run_id", "logical_client_id", "worker_id", "physical_worker_id",
    "container_name", "container_id", "container_mode", "profile_enabled",
    "resource_profile_index", "resource_profile_id", "experiment_kind",
    "selected_for_this_run", "cpu_affinity_role", "cpuset_cpus",
    "cpuset_mask_hex", "cpu_limit_cpus", "capacity_fraction",
    "assigned_cpu_count", "memory_limit", "memory_swap", "rayon_num_threads",
    "background_cpuset_cpus", "background_mask_hex", "profile_label",
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
    "experiment_kind", "failure_class", "failure_timestamp_ns",
    "last_successful_phase", "last_successful_operation_family",
    "last_successful_benchmark_operation", "last_successful_member_count",
    "last_successful_epoch", "current_phase", "current_operation_family",
    "current_benchmark_operation", "current_member_count", "current_epoch",
    "container_exit_code", "container_oom_killed", "memory_events_oom",
    "memory_events_oom_kill", "max_memory_current",
    "cpu_nr_throttled_delta", "cpu_throttled_usec_delta",
    "cpu_throttled_time_fraction", "diagnostic_log_path",
]

RUN_STATUS_HEADER = [
    "run_id", "run_mode", "resource_experiment", "resource_failure_policy",
    "resource_profile_index", "resource_profile_id", "experiment_kind",
    "run_status", "completed", "valid_for_threshold_analysis",
    "valid_for_clean_performance_plots", "valid_for_churn_recovery_analysis",
    "first_failure_timestamp_ns", "first_failed_worker_id",
    "first_failed_client_id", "first_failure_class",
    "first_failure_operation_family", "first_failure_benchmark_operation",
    "first_failure_member_count", "first_failure_epoch",
    "last_successful_operation_family", "last_successful_benchmark_operation",
    "last_successful_member_count", "last_successful_epoch",
    "preflight_passed", "resource_output_validation_passed", "notes",
]

FORBIDDEN_COLUMN_SUBSTRINGS = ["_bytes", "pids_limit"]


class ValidationError(Exception):
    pass


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

        selected = self._read_column_values(profiles_path, "selected_for_this_run")
        true_count = sum(1 for v in selected if v.lower() in ("true", "1"))

        if true_count == 0:
            self.warn("No profile has selected_for_this_run=true (multiplexed mode?)")
        elif true_count > 1:
            self.error(f"{true_count} profiles have selected_for_this_run=true (expected exactly 1)")

        assignments_path = os.path.join(self.run_dir, "worker_resource_assignments.csv")
        if os.path.exists(assignments_path):
            assign_selected = self._read_column_values(assignments_path, "selected_for_this_run")
            assign_true = sum(1 for v in assign_selected if v.lower() in ("true", "1"))
            if assign_true > 1:
                self.error(f"{assign_true} workers marked selected_for_this_run=true (expected exactly 1)")

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
