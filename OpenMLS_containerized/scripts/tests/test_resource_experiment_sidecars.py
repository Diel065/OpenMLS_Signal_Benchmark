"""
Unit tests for resource_experiment_sidecars.py
"""

import csv
import json
import os
import sys
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from resource_experiment_sidecars import (
    SidecarWriter,
    RESOURCE_PROFILES_HEADER,
    WORKER_RESOURCE_ASSIGNMENTS_HEADER,
    CPU_AFFINITY_PREFLIGHT_HEADER,
    RESOURCE_SUMMARY_HEADER,
    WORKER_FAILURES_HEADER,
    RUN_STATUS_HEADER,
    BENCHMARK_TIMELINE_HEADER,
    get_expected_files,
    validate_sidecars_exist,
    VALIDATOR_SCHEMAS,
    _safe_csv_value,
)


class TestSafeCsvValue:
    def test_none(self):
        assert _safe_csv_value(None) == ""

    def test_bool(self):
        assert _safe_csv_value(True) == "true"
        assert _safe_csv_value(False) == "false"

    def test_float(self):
        assert "0.5" in _safe_csv_value(0.5)

    def test_list(self):
        result = _safe_csv_value([0, 1, 2])
        assert "0" in result
        assert "1" in result
        assert "2" in result

    def test_string(self):
        assert _safe_csv_value("hello") == "hello"


class TestSidecarWriter:
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.writer = SidecarWriter(self.tmpdir)

    def test_write_resource_profiles(self):
        profiles = [
            {
                "resource_profile_id": "rp-1",
                "experiment_kind": "ram_sweep_singleton",
                "profile_label": "test",
                "cpu_limit_cpus": "",
                "capacity_fraction": "",
                "assigned_cpu_count": 10,
                "memory_limit": "128m",
                "memory_swap": "128m",
                "rayon_num_threads": 10,
                "cpuset_cpus": "0-9",
                "cpuset_mask_hex": "0x3ff",
                "profile_notes": "test notes",
            },
        ]
        path = self.writer.write_resource_profiles("test-run", profiles)
        assert os.path.exists(path)

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["run_id"] == "test-run"
            assert rows[0]["memory_limit"] == "128m"

    def test_write_worker_resource_assignments(self):
        assignments = [
            {
                "logical_client_id": "00001",
                "worker_id": "worker-00001",
                "physical_worker_id": "worker-00001",
                "container_name": "worker-00001",
                "container_id": "abc123",
                "container_mode": "singleton",
                "profile_enabled": True,
                "resource_profile_id": "rp-1",
                "experiment_kind": "ram_sweep_singleton",
                "cpu_affinity_role": "profiled_singleton",
                "cpuset_cpus": "0",
                "cpuset_mask_hex": "0x1",
                "cpu_limit_cpus": "",
                "capacity_fraction": "",
                "assigned_cpu_count": 10,
                "memory_limit": "128m",
                "memory_swap": "128m",
                "rayon_num_threads": 10,
                "profile_label": "test",
            },
        ]
        path = self.writer.write_worker_resource_assignments("test-run", assignments)
        assert os.path.exists(path)

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["container_mode"] == "singleton"

    def test_write_preflight_results(self):
        results = [
            {
                "check_name": "profiled_cpuset_match",
                "container_name": "worker-00001",
                "container_role": "profiled_singleton",
                "expected_cpuset": "0",
                "docker_cpuset": "0",
                "host_pid": "12345",
                "proc_cpus_allowed_list": "0",
                "thread_cpus_allowed_lists": "",
                "observed_psr_cpus": "",
                "status": "PASS",
                "message": "OK",
            },
        ]
        path = self.writer.write_preflight_results("test-run", results)
        assert os.path.exists(path)

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["status"] == "PASS"

    def test_write_run_status(self):
        status = {
            "run_mode": "ram-sweep-singleton",
            "experiment_kind": "ram_sweep_singleton",
            "run_status": "completed",
            "first_failure_timestamp_ns": 0,
            "first_failed_worker_id": "",
            "first_failed_client_id": "",
            "first_failure_class": "",
            "first_failure_operation_family": "",
            "first_failure_member_count": 0,
            "valid_for_performance_plots": True,
            "valid_for_failure_analysis": False,
            "notes": "All profiles completed",
        }
        path = self.writer.write_run_status("test-run", status)
        assert os.path.exists(path)

    def test_append_benchmark_timeline(self):
        event = {
            "timestamp_ns": 1234567890000,
            "phase": "add",
            "operation_family": "add_commit_create",
            "benchmark_operation": "add_commit",
            "commit_kind": "add",
            "epoch": 1,
            "member_count": 16,
            "actor_client_id": "00001",
            "target_client_id": "",
            "worker_id": "worker-00001",
            "physical_worker_id": "worker-00001",
            "status": "success",
            "details": "",
        }
        self.writer.append_benchmark_timeline("test-run", event)
        self.writer.append_benchmark_timeline("test-run", event)

        path = os.path.join(self.tmpdir, "benchmark_timeline.csv")
        assert os.path.exists(path)
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2

    def test_write_jsonl(self):
        self.writer.write_jsonl_line("test.jsonl", {"key": "value"})
        path = os.path.join(self.tmpdir, "test.jsonl")
        assert os.path.exists(path)
        with open(path) as f:
            lines = f.readlines()
            assert len(lines) == 1
            data = json.loads(lines[0])
            assert data["key"] == "value"

    def test_no_pids_limit_column(self):
        profiles = [
            {
                "resource_profile_id": "rp-1",
                "experiment_kind": "ram_sweep_singleton",
                "profile_label": "test",
                "cpu_limit_cpus": "",
                "capacity_fraction": "",
                "assigned_cpu_count": 10,
                "memory_limit": "128m",
                "memory_swap": "128m",
                "rayon_num_threads": 10,
                "cpuset_cpus": "0-9",
                "cpuset_mask_hex": "0x3ff",
                "profile_notes": "",
            },
        ]
        path = self.writer.write_resource_profiles("test-run", profiles)
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames
            assert "pids_limit" not in header
            assert "memory_limit_bytes" not in header
            assert "memory_swap_bytes" not in header


class TestExpectedFiles:
    def test_expected_files_list(self):
        files = get_expected_files()
        assert "cpu_affinity_plan.json" in files
        assert "resource_profiles.csv" in files
        assert "worker_failures.csv" in files
        assert "run_status.csv" in files


class TestSchemaHeaders:
    def test_resource_profiles_header(self):
        assert "run_id" in RESOURCE_PROFILES_HEADER
        assert "resource_profile_id" in RESOURCE_PROFILES_HEADER

    def test_worker_assignments_header(self):
        assert "cpu_affinity_role" in WORKER_RESOURCE_ASSIGNMENTS_HEADER
        assert "container_mode" in WORKER_RESOURCE_ASSIGNMENTS_HEADER


class TestSidecarValidation:
    """Tests for sidecar existence validation."""

    def test_all_present_on_clean_dir(self, tmp_path):
        expected = get_expected_files()
        for f in expected:
            (tmp_path / f).write_text("content")
        result = validate_sidecars_exist(str(tmp_path), run_success=True)
        assert result["valid"] is True
        assert len(result["missing"]) == 0

    def test_missing_critical_on_success_is_invalid(self, tmp_path):
        for f in get_expected_files():
            if f != "events.csv":
                (tmp_path / f).write_text("content")
        result = validate_sidecars_exist(str(tmp_path), run_success=True)
        assert result["valid"] is False
        assert "events.csv" in result["missing"]

    def test_empty_files_reported(self, tmp_path):
        (tmp_path / "events.csv").write_text("")
        (tmp_path / "run_status.csv").write_text("status")
        (tmp_path / "aggregation_manifest.json").write_text("{}")
        (tmp_path / "worker_failures.csv").write_text("worker_id\n")
        result = validate_sidecars_exist(str(tmp_path), run_success=False)
        assert "events.csv" in result["empty"]
        assert result["valid"] is True

    def test_failure_mode_allows_missing_events(self, tmp_path):
        (tmp_path / "aggregation_manifest.json").write_text("{}")
        (tmp_path / "run_status.csv").write_text("failed")
        (tmp_path / "worker_failures.csv").write_text("id\n")
        result = validate_sidecars_exist(str(tmp_path), run_success=False)
        assert result["valid"] is True

    def test_all_missing_detected(self, tmp_path):
        result = validate_sidecars_exist(str(tmp_path), run_success=True)
        assert result["valid"] is False
        assert len(result["missing"]) == len(get_expected_files())
