"""
Unit tests for cpu_affinity_planner.py
"""

import json
import os
import sys
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cpu_affinity_planner import (
    AffinityPlan,
    ProfiledAssignment,
    BackgroundAssignment,
    create_affinity_plan,
    write_affinity_plan_json,
    get_background_cpuset,
    get_profiled_cpuset,
    get_rayon_num_threads,
    validate_affinity_plan,
    _create_empty_affinity_plan,
)


class TestEmptyAffinityPlan:
    def test_empty_plan_mode_none(self):
        plan = _create_empty_affinity_plan("test-run", 0)
        assert plan.cpu_affinity_mode == "none"
        assert plan.profiled_assignments == []
        assert plan.background_assignments == []
        assert plan.online_cpus == []

    def test_empty_plan_no_warnings(self):
        plan = _create_empty_affinity_plan("test-run", 0)
        assert validate_affinity_plan(plan) == []


class TestWriteAffinityPlan:
    def test_writes_valid_json(self):
        plan = _create_empty_affinity_plan("test-run", 0)
        with tempfile.TemporaryDirectory() as tmp:
            path = write_affinity_plan_json(plan, tmp)
            assert os.path.exists(path)
            with open(path) as f:
                data = json.load(f)
            assert data["run_id"] == "test-run"
            assert "created_at" in data
            assert "online_cpu_mask_hex" in data
            assert "profiled_mask_hex" in data
            assert "background_mask_hex" in data
            assert "profiled_assignments" in data
            assert "background_assignments" in data
            assert "warnings" in data


class TestGetterFunctions:
    def test_background_cpuset_empty_plan(self):
        plan = _create_empty_affinity_plan("test-run", 0)
        assert get_background_cpuset(plan) == ""

    def test_profiled_cpuset_not_found(self):
        plan = _create_empty_affinity_plan("test-run", 0)
        assert get_profiled_cpuset(plan, "nonexistent") is None

    def test_rayon_not_found(self):
        plan = _create_empty_affinity_plan("test-run", 0)
        assert get_rayon_num_threads(plan, "nonexistent") is None


class TestValidateAffinityPlan:
    def test_empty_plan_valid(self):
        plan = _create_empty_affinity_plan("test-run", 0)
        errors = validate_affinity_plan(plan)
        assert errors == []

    def test_non_empty_plan_no_errors_with_single_profiled(self):
        plan = AffinityPlan(
            run_id="test",
            created_at="2024-01-01T00:00:00",
            hostname="test-host",
            cpu_affinity_mode="profiled-nor-background",
            sample_seconds=20.0,
            selection_policy="least_loaded",
            online_cpus=[0, 1, 2, 3],
            online_cpu_mask_hex="0xf",
            cpu_topology={},
            sampled_cpu_load={},
            profiled_assignments=[
                ProfiledAssignment(
                    worker_id="worker-00001",
                    container_name="worker-00001",
                    logical_client_id="00001",
                    assigned_cpus=[0],
                    assigned_mask_hex="0x1",
                    assigned_cpu_count=1,
                    rayon_num_threads=1,
                    experiment_kind="test",
                    resource_profile_id="rp-1",
                ),
            ],
            profiled_mask_hex="0x1",
            reserved_mask_hex="0x1",
            background_cpus=[1, 2, 3],
            background_mask_hex="0xe",
            smt_sibling_policy="no_reservation",
            warnings=[],
            background_assignments=[
                BackgroundAssignment(
                    container_name="ds",
                    container_role="infrastructure",
                    assigned_cpus=[1, 2, 3],
                    assigned_mask_hex="0xe",
                ),
            ],
        )
        errors = validate_affinity_plan(plan)
        assert errors == []

    def test_overlap_detected(self):
        plan = AffinityPlan(
            run_id="test",
            created_at="",
            hostname="",
            cpu_affinity_mode="profiled-nor-background",
            sample_seconds=0,
            selection_policy="least_loaded",
            online_cpus=[0, 1, 2, 3],
            online_cpu_mask_hex="0xf",
            cpu_topology={},
            sampled_cpu_load={},
            profiled_assignments=[
                ProfiledAssignment("w1", "c1", "1", [0, 1], "0x3", 2, 2, "test", "r1"),
                ProfiledAssignment("w2", "c2", "2", [1, 2], "0x6", 2, 2, "test", "r2"),
            ],
            profiled_mask_hex="0x7",
            reserved_mask_hex="0x7",
            background_cpus=[3],
            background_mask_hex="0x8",
            smt_sibling_policy="no_reservation",
            warnings=[],
            background_assignments=[],
        )
        errors = validate_affinity_plan(plan)
        assert any("CPU 1" in e for e in errors)

    def test_rayon_mismatch(self):
        plan = AffinityPlan(
            run_id="test",
            created_at="",
            hostname="",
            cpu_affinity_mode="profiled-nor-background",
            sample_seconds=0,
            selection_policy="least_loaded",
            online_cpus=[0, 1],
            online_cpu_mask_hex="0x3",
            cpu_topology={},
            sampled_cpu_load={},
            profiled_assignments=[
                ProfiledAssignment("w1", "c1", "1", [0], "0x1", 1, 4, "test", "r1"),
            ],
            profiled_mask_hex="0x1",
            reserved_mask_hex="0x1",
            background_cpus=[1],
            background_mask_hex="0x2",
            smt_sibling_policy="no_reservation",
            warnings=[],
            background_assignments=[],
        )
        errors = validate_affinity_plan(plan)
        assert any("RAYON_NUM_THREADS" in e for e in errors)


class TestCreateAffinityPlan:
    def test_none_mode_returns_empty(self):
        plan = create_affinity_plan(
            run_id="test",
            profiled_worker_specs=[],
            background_specs=[],
            cpu_affinity_mode="none",
        )
        assert plan.cpu_affinity_mode == "none"
        assert plan.profiled_assignments == []
