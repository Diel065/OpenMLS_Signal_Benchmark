"""
Unit tests for resource_experiment_failures.py
"""

import sys
import os
import csv
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from resource_experiment_failures import (
    WorkerFailureInfo,
    classify_worker_failure,
    classify_worker_failure_from_resource_summary,
    build_run_status,
    worker_failure_info_to_dict,
    collect_worker_failures_from_artifacts,
    append_synthetic_runner_failure_event,
    worker_failures_from_events_csv,
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

    def test_running_worker_without_pressure_completed_successfully(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            container_exit_code=0,
            last_container_status="running",
        )
        assert classify_worker_failure(info) == "completed_successfully"

    def test_running_throttled_worker_completed_successfully(self):
        info = WorkerFailureInfo(
            worker_id="w1",
            container_exit_code=0,
            last_container_status="running",
            cpu_throttled_time_fraction=0.99,
        )
        assert classify_worker_failure(info) == "completed_successfully"


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

    def test_running_summary_is_not_a_failure(self):
        summary = {
            "worker_id": "w1",
            "last_container_status": "running",
            "last_container_exit_code": "0",
            "memory_events_oom": "0",
            "memory_events_oom_kill": "0",
            "cpu_throttled_time_fraction": "0.0",
        }
        klass, info = classify_worker_failure_from_resource_summary(summary)
        assert klass == "completed_successfully"


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

    def test_completed_failure_experiment_is_not_clean_performance_data(self):
        wf = WorkerFailureInfo(
            worker_id="w1",
            failure_class="hard_ram_oom_kill",
            failure_timestamp_ns=100,
            logical_client_id="00001",
            current_benchmark_operation="add_commit_create",
        )
        status = build_run_status(
            run_id="test-run",
            run_mode="ram-sweep-singleton",
            experiment_kind="ram_sweep_singleton",
            run_success=True,
            worker_failures=[wf],
        )
        assert status["run_status"] == "completed_with_worker_failures"
        assert status["valid_for_clean_performance_plots"] is False
        assert status["valid_for_threshold_analysis"] is True
        assert status["valid_for_churn_recovery_analysis"] is True
        assert status["completed"] is True


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


def _write_events(path, rows):
    fieldnames = [
        "client_id", "worker_id", "ts_unix_ns", "op", "span_name", "span_id",
        "runner_event_kind", "failed_worker_id", "failed_physical_worker_id",
        "failure_class", "failure_detail", "failure_evidence_source",
        "failure_evidence_detail", "failure_action", "benchmark_phase",
        "benchmark_operation", "benchmark_active_size", "operation_family",
        "member_count", "group_epoch",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_runner_event_is_authoritative_for_operation_and_reason(tmp_path):
    events = tmp_path / "events.csv"
    _write_events(events, [
        {
            "client_id": "00001", "worker_id": "worker-00001", "ts_unix_ns": "100",
            "op": "self_update.path_hpke_encrypt", "span_name": "self_update.path_hpke_encrypt",
            "span_id": "42", "benchmark_phase": "update",
            "benchmark_operation": "update_commit", "operation_family": "update_commit_create",
            "member_count": "8", "group_epoch": "9",
        },
        {
            "client_id": "00001", "worker_id": "worker-00001", "ts_unix_ns": "110",
            "op": "benchmark.worker_failure", "runner_event_kind": "worker_failure",
            "failed_worker_id": "00001", "failed_physical_worker_id": "worker-00001",
            "failure_class": "cpu_starvation_timeout", "failure_detail": "request timed out",
            "failure_evidence_source": "runner_observed_request_failure",
            "failure_evidence_detail": "deadline elapsed", "failure_action": "stop_run",
            "benchmark_phase": "application",
            "benchmark_operation": "send_application_message",
            "benchmark_active_size": "8",
        },
    ])

    failures = worker_failures_from_events_csv(str(events))
    assert len(failures) == 1
    failure = failures[0]
    assert failure.failure_class == "cpu_timeout"
    assert failure.current_benchmark_operation == "send_application_message"
    assert failure.current_operation_family == "application_message_create"
    assert failure.current_member_count == 8
    assert failure.failure_detail == "request timed out"
    assert failure.failure_evidence_source == "runner_observed_request_failure"
    assert failure.attribution_confidence == "exact_runner_operation"
    assert failure.last_observed_span_name == "self_update.path_hpke_encrypt"


def test_pressure_without_failure_event_is_not_a_worker_failure(tmp_path):
    events = tmp_path / "events.csv"
    _write_events(events, [])
    summaries = [{
        "worker_id": "worker-00001",
        "logical_client_id": "00001",
        "last_container_status": "running",
        "last_container_exit_code": "0",
        "cpu_throttled_time_fraction": "0.99",
        "memory_events_oom": "0",
        "memory_events_oom_kill": "0",
    }]

    failures = collect_worker_failures_from_artifacts(str(events), summaries)
    assert failures == []


def test_synthetic_failure_uses_unmatched_profiled_operation_cursor(tmp_path):
    events = tmp_path / "events.csv"
    _write_events(events, [])
    journal = tmp_path / "profiled-operation-cursors.jsonl"
    journal.write_text(
        json.dumps({
            "ts_unix_ns": 100,
            "lifecycle": "started",
            "request_id": "req-1",
            "logical_client_id": "00001",
            "physical_worker_id": "worker-00001",
            "command": "self_update",
            "benchmark_plateau_index": 3,
            "benchmark_target_size": 16,
            "benchmark_active_size": 16,
            "benchmark_phase": "update",
            "benchmark_operation": "update_commit",
            "benchmark_operation_seq": 2,
            "benchmark_payload_size": None,
        }) + "\n",
        encoding="utf-8",
    )
    failure = WorkerFailureInfo(
        worker_id="worker-00001",
        physical_worker_id="worker-00001",
        logical_client_id="00001",
        failure_class="hard_ram_oom_kill",
        container_oom_killed=True,
    )

    assert append_synthetic_runner_failure_event(str(tmp_path), failure, str(events))
    event = json.loads((tmp_path / "runner-events.jsonl").read_text(encoding="utf-8"))
    assert event["benchmark_operation"] == "update_commit"
    assert event["benchmark_phase"] == "update"
    assert event["failure_evidence_source"] == "runner_active_operation_journal"
    assert failure.attribution_confidence == "exact_runner_operation"
    assert failure.current_member_count == 16
    assert failure.current_operation_family == "update_commit_create"
