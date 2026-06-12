"""
Unit tests for resource_experiment_failures.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from resource_experiment_failures import (
    WorkerFailureInfo,
    classify_worker_failure,
    classify_worker_failure_from_resource_summary,
    build_run_status,
    worker_failure_info_to_dict,
    FAILURE_CLASSES,
)


class TestFailureClasses:
    def test_all_classes_defined(self):
        assert "completed_successfully" in FAILURE_CLASSES
        assert "hard_ram_oom_kill" in FAILURE_CLASSES
        assert "hard_container_exit" in FAILURE_CLASSES
        assert "cpu_timeout" in FAILURE_CLASSES
        assert "cpu_starvation_suspected" in FAILURE_CLASSES
        assert "memory_pressure_no_oom" in FAILURE_CLASSES
        assert "worker_unreachable" in FAILURE_CLASSES
        assert "benchmark_protocol_failure" in FAILURE_CLASSES
        assert "infrastructure_failure" in FAILURE_CLASSES
        assert "unknown_failure" in FAILURE_CLASSES
        assert "thread_or_process_creation_failure" in FAILURE_CLASSES


class TestClassifyWorkerFailure:
    def test_oom_kill_by_docker(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            container_oom_killed=True,
        )
        assert classify_worker_failure(info) == "hard_ram_oom_kill"

    def test_oom_kill_by_cgroup(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            memory_events_oom_kill=1,
        )
        assert classify_worker_failure(info) == "hard_ram_oom_kill"

    def test_nonzero_exit_without_oom(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            container_exit_code=1,
            container_oom_killed=False,
            memory_events_oom_kill=0,
        )
        assert classify_worker_failure(info) == "hard_container_exit"

    def test_cpu_starvation_suspected(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            container_exit_code=None,
            cpu_throttled_time_fraction=0.8,
        )
        assert classify_worker_failure(info) == "cpu_starvation_suspected"

    def test_memory_pressure_no_oom(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            memory_events_oom=1,
            memory_events_oom_kill=0,
            container_oom_killed=False,
        )
        assert classify_worker_failure(info) == "memory_pressure_no_oom"

    def test_unknown_failure(self):
        info = WorkerFailureInfo(worker_id="w1")
        assert classify_worker_failure(info) == "unknown_failure"


class TestClassifyFromResourceSummary:
    def test_oom_classification(self):
        summary = {
            "worker_id": "w1",
            "last_container_oom_killed": "true",
            "last_container_exit_code": "137",
        }
        klass, info = classify_worker_failure_from_resource_summary(summary)
        assert klass == "hard_ram_oom_kill"

    def test_exit_classification(self):
        summary = {
            "worker_id": "w1",
            "container_oom_killed": "false",
            "last_container_exit_code": "1",
            "memory_events_oom_kill": "0",
        }
        klass, info = classify_worker_failure_from_resource_summary(summary)
        assert klass == "hard_container_exit"

    def test_missing_values(self):
        summary = {"worker_id": "w1"}
        klass, info = classify_worker_failure_from_resource_summary(summary)
        assert klass == "unknown_failure"


class TestBuildRunStatus:
    def test_successful_run(self):
        status = build_run_status(
            run_id="test-run",
            run_mode="ram-sweep-singleton",
            experiment_kind="ram_sweep_singleton",
            run_success=True,
            worker_failures=[],
        )
        assert status["run_status"] == "completed"
        assert status["valid_for_clean_performance_plots"] is True
        assert status["valid_for_threshold_analysis"] is True
        assert status["completed"] is True

    def test_failed_run(self):
        wf = WorkerFailureInfo(
            worker_id="w1",
            failure_class="hard_ram_oom_kill",
            failure_timestamp_ns=100,
            logical_client_id="00001",
            current_operation_family="add_commit_create",
            current_member_count=8,
        )
        status = build_run_status(
            run_id="test-run",
            run_mode="ram-sweep-singleton",
            experiment_kind="ram_sweep_singleton",
            run_success=False,
            worker_failures=[wf],
        )
        assert status["run_status"] == "failed_hard_ram_oom_kill"
        assert status["valid_for_clean_performance_plots"] is False
        assert status["valid_for_threshold_analysis"] is True
        assert status["completed"] is False


class TestWorkerFailureInfoToDict:
    def test_all_fields_present(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            physical_worker_id="pw1",
            logical_client_id="00001",
            container_name="c1",
            failure_class="hard_ram_oom_kill",
        )
        d = worker_failure_info_to_dict(info)
        assert d["worker_id"] == "w1"
        assert d["failure_class"] == "hard_ram_oom_kill"
