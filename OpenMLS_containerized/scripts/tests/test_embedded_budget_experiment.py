import csv
import json
import os
import subprocess
import sys
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from generate_compose import (
    ClientLayoutEntry,
    PhysicalWorkerEntry,
    apply_affinity_to_compose,
    restrict_profile_enabled_clients_to_affinity_selection,
)
from resource_experiment_failures import (
    parse_app_heap_budget_failure,
    worker_failures_from_events_csv,
    worker_failures_from_terminal_output,
)
from resource_profiles import generate_embedded_budget_profiles, select_profile
from validate_resource_experiment_outputs import Validator


def test_embedded_profile_generation_uses_safe_docker_memory_and_app_heap_budget():
    profiles = generate_embedded_budget_profiles(
        heap_budgets=["32k", "2m"],
        core_counts=[1],
        capacity_fractions=[1.0, 0.10],
        docker_memory_limit="256m",
    )

    assert len(profiles) == 4
    assert profiles[0].experiment_kind == "embedded_budget_singleton"
    assert profiles[0].memory_model == "app-heap-budget"
    assert profiles[0].memory_limit == "256m"
    assert profiles[0].app_heap_budget == "32k"
    assert profiles[0].app_heap_budget_bytes == 32 * 1024
    assert profiles[1].cpu_limit_cpus == 0.10


def test_embedded_profile_index_selection_marks_one_profile():
    profiles = generate_embedded_budget_profiles(
        heap_budgets=["32k", "2m"],
        core_counts=[1],
        capacity_fractions=[1.0, 0.10],
        docker_memory_limit="256m",
    )

    selected = select_profile(profiles, profile_index=3, profiled_singleton_count=1)

    assert selected.resource_profile_index == 3
    assert sum(1 for profile in profiles if profile.selected_for_this_run) == 1
    assert selected.app_heap_budget == "2m"
    assert selected.cpu_limit_cpus == 0.10


def test_compose_profile_passes_app_heap_budget_env_not_tiny_docker_memory():
    profiles = generate_embedded_budget_profiles(
        heap_budgets=["32k"],
        core_counts=[1],
        capacity_fractions=[0.25],
        docker_memory_limit="256m",
    )
    profile = profiles[0]
    profile.cpuset_cpus = "2"
    profile.cpuset_mask_hex = "0x4"
    profile.selected_for_this_run = True
    plan = {
        "profiled_assignments": [{
            "worker_id": "worker-00001",
            "container_name": "worker-00001",
            "assigned_cpus": [2],
            "rayon_num_threads": 1,
            "resource_profile_id": profile.resource_profile_id,
        }],
        "background_assignments": [],
    }
    lines = []

    result = apply_affinity_to_compose(
        lines,
        "worker-00001",
        "singleton",
        plan,
        [profile.to_dict()],
        0,
        Namespace(),
    )

    assert '    cpus: "0.25"' in lines
    assert '    mem_limit: "256m"' in lines
    assert '    mem_limit: "32k"' not in lines
    assert result["memory_model"] == "app-heap-budget"
    assert result["app_heap_budget"] == "32k"
    assert result["app_heap_budget_bytes"] == 32 * 1024


def test_resource_experiment_profiles_only_affinity_selected_singleton():
    clients = [
        ClientLayoutEntry("00001", "worker-00001", "singleton", True, "", ""),
        ClientLayoutEntry("00002", "worker-00002", "singleton", True, "", ""),
        ClientLayoutEntry("00003", "worker-pack-000", "packed", False, "", ""),
    ]
    workers = [
        PhysicalWorkerEntry("worker-00001", "singleton", ["00001"], "", ["00001"]),
        PhysicalWorkerEntry("worker-00002", "singleton", ["00002"], "", ["00002"]),
        PhysicalWorkerEntry("worker-pack-000", "packed", ["00003"], "", []),
    ]
    plan = {
        "profiled_assignments": [{
            "container_name": "worker-00001",
            "logical_client_id": "00001",
        }]
    }

    restrict_profile_enabled_clients_to_affinity_selection(clients, workers, plan)

    assert [client.profile_enabled for client in clients] == [True, False, False]
    assert workers[0].profile_enabled_client_ids == ["00001"]
    assert workers[1].profile_enabled_client_ids == []
    assert workers[2].profile_enabled_client_ids == []


def test_app_heap_budget_failure_payload_classifies_with_operation():
    detail = (
        "Worker 00001 error: APP_HEAP_BUDGET_EXCEEDED "
        "failure_class=app_heap_budget_exceeded memory_model=app-heap-budget "
        "operation_family=welcome_receive benchmark_operation=welcome_receive "
        "span_or_phase=join member_count=32 epoch=31 worker_id=00001 "
        "resource_profile_id=embedded_heap_32k_cpu_1c_100 resource_profile_index=0 "
        "app_heap_budget=32k app_heap_budget_bytes=32768 "
        "current_live_heap_bytes=40960 peak_live_heap_bytes=40960 "
        "operation_peak_live_heap_bytes=40960 total_allocated_bytes=50000 "
        "allocation_count=10 deallocation_count=2 failed_allocation_size_bytes=1024"
    )
    parsed = parse_app_heap_budget_failure(detail)

    assert parsed["failure_class"] == "app_heap_budget_exceeded"
    assert parsed["operation_family"] == "welcome_receive"
    assert parsed["app_heap_budget_bytes"] == "32768"


def test_worker_failures_from_events_preserves_app_heap_attribution(tmp_path):
    events = tmp_path / "events.csv"
    detail = (
        "APP_HEAP_BUDGET_EXCEEDED failure_class=app_heap_budget_exceeded "
        "memory_model=app-heap-budget operation_family=welcome_receive "
        "benchmark_operation=welcome_receive span_or_phase=join member_count=32 "
        "epoch=31 worker_id=00001 resource_profile_id=embedded_heap_32k_cpu_1c_100 "
        "resource_profile_index=0 app_heap_budget=32k app_heap_budget_bytes=32768 "
        "current_live_heap_bytes=40960 peak_live_heap_bytes=40960 "
        "operation_peak_live_heap_bytes=40960 total_allocated_bytes=50000 "
        "allocation_count=10 deallocation_count=2 failed_allocation_size_bytes=1024"
    )
    fields = [
        "client_id", "failed_worker_id", "failed_physical_worker_id",
        "runner_event_kind", "failure_class", "failure_detail",
        "failure_evidence_source", "failure_evidence_detail", "failure_action",
        "ts_unix_ns", "benchmark_target_size", "benchmark_active_size",
        "benchmark_phase", "benchmark_operation", "benchmark_operation_seq",
        "benchmark_plateau_index", "group_epoch", "member_count",
        "operation_family", "span_name", "span_id",
    ]
    with events.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({
            "client_id": "00001",
            "failed_worker_id": "00001",
            "failed_physical_worker_id": "worker-00001",
            "runner_event_kind": "worker_failure",
            "failure_class": "app_heap_budget_exceeded",
            "failure_detail": detail,
            "failure_evidence_source": "runner_observed_request_failure",
            "failure_action": "stop_run",
            "ts_unix_ns": "10",
        })

    failures = worker_failures_from_events_csv(str(events))

    assert len(failures) == 1
    assert failures[0].failure_class == "app_heap_budget_exceeded"
    assert failures[0].current_operation_family == "welcome_receive"
    assert failures[0].current_member_count == 32
    assert failures[0].heap_operation_peak_live_bytes == 40960


def test_worker_failures_from_terminal_output_preserves_early_app_heap_failure(tmp_path):
    terminal = tmp_path / "terminal_output.txt"
    terminal.write_text(
        "Error: Worker 00001 error: APP_HEAP_BUDGET_EXCEEDED "
        "failure_class=app_heap_budget_exceeded memory_model=app-heap-budget "
        "operation_family=create_group benchmark_operation=create_group "
        "span_or_phase=- member_count= epoch= worker_id=00001 "
        "resource_profile_id=embedded_heap_1k_cpu_1c_100 resource_profile_index=0 "
        "app_heap_budget=1k app_heap_budget_bytes=1024 "
        "current_live_heap_bytes=90467 peak_live_heap_bytes=93544 "
        "operation_peak_live_heap_bytes=90467 total_allocated_bytes=177754 "
        "allocation_count=402 deallocation_count=255\n",
        encoding="utf-8",
    )

    failures = worker_failures_from_terminal_output(str(terminal))

    assert len(failures) == 1
    assert failures[0].failure_class == "app_heap_budget_exceeded"
    assert failures[0].logical_client_id == "00001"
    assert failures[0].current_operation_family == "create_group"
    assert failures[0].current_benchmark_operation == "create_group"
    assert failures[0].attribution_source == "app_heap_budget_failure_payload"
    assert failures[0].heap_operation_peak_live_bytes == 90467


def test_embedded_validator_accepts_auditable_minimal_run(tmp_path):
    run_id = "embedded-test"
    headers_rows = {
        "resource_profiles.csv": (
            [
                "run_id", "resource_profile_id", "experiment_kind",
                "resource_profile_index", "profile_label", "selected_for_this_run",
                "cpu_limit_cpus", "capacity_fraction", "assigned_cpu_count",
                "memory_limit", "memory_swap", "memory_model", "docker_memory_limit",
                "app_heap_budget", "app_heap_budget_bytes", "rayon_num_threads",
                "cpuset_cpus", "cpuset_mask_hex", "cpuset_role", "profile_notes",
                "sweep_kind", "app_heap_interpretation", "cpu_interpretation",
                "cpu_period_us", "cpu_quota_us", "group_creator", "group_creator_reason",
                "strict_cpuset_satisfied",
            ],
            [{
                "run_id": run_id, "resource_profile_id": "embedded_heap_32k_cpu_1c_100",
                "experiment_kind": "embedded_budget_singleton", "resource_profile_index": "0",
                "profile_label": "AppHeap=32k", "selected_for_this_run": "true",
                "cpu_limit_cpus": "1.000000", "capacity_fraction": "1.000000",
                "assigned_cpu_count": "1", "memory_limit": "256m", "memory_swap": "256m",
                "memory_model": "app-heap-budget", "docker_memory_limit": "256m",
                "app_heap_budget": "32k", "app_heap_budget_bytes": "32768",
                "rayon_num_threads": "1", "cpuset_cpus": "0", "cpuset_mask_hex": "0x1",
                "cpuset_role": "embedded_budget", "profile_notes": "test",
            }],
        ),
        "worker_resource_assignments.csv": (
            [
                "run_id", "logical_client_id", "worker_id", "physical_worker_id",
                "container_name", "container_id", "container_mode", "profile_enabled",
                "resource_profile_index", "resource_profile_id", "experiment_kind",
                "selected_for_this_run", "cpu_affinity_role", "cpuset_cpus",
                "cpuset_mask_hex", "cpu_limit_cpus", "capacity_fraction",
                "assigned_cpu_count", "memory_limit", "memory_swap", "memory_model",
                "docker_memory_limit", "app_heap_budget", "app_heap_budget_bytes",
                "rayon_num_threads", "background_cpuset_cpus", "background_mask_hex",
                "profile_label",
                "sweep_kind", "app_heap_interpretation", "cpu_interpretation",
                "group_creator", "group_creator_reason", "strict_cpuset_satisfied",
            ],
            [{
                "run_id": run_id, "logical_client_id": "00001", "worker_id": "worker-00001",
                "physical_worker_id": "worker-00001", "container_name": "worker-00001",
                "container_mode": "singleton", "profile_enabled": "true",
                "resource_profile_index": "0", "resource_profile_id": "embedded_heap_32k_cpu_1c_100",
                "experiment_kind": "embedded_budget_singleton", "selected_for_this_run": "true",
                "cpu_affinity_role": "profiled_singleton", "cpuset_cpus": "0",
                "cpuset_mask_hex": "0x1", "cpu_limit_cpus": "1.000000",
                "capacity_fraction": "1.000000", "assigned_cpu_count": "1",
                "memory_limit": "256m", "memory_swap": "256m",
                "memory_model": "app-heap-budget", "docker_memory_limit": "256m",
                "app_heap_budget": "32k", "app_heap_budget_bytes": "32768",
                "rayon_num_threads": "1", "background_cpuset_cpus": "1",
                "background_mask_hex": "0x2", "profile_label": "AppHeap=32k",
            }],
        ),
        "worker_failures.csv": (
            [
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
                "heap_failed_allocation_size_bytes", "container_exit_code",
                "container_oom_killed", "memory_events_oom", "memory_events_oom_kill",
                "max_memory_current", "cpu_nr_throttled_delta",
                "cpu_throttled_usec_delta", "cpu_throttled_time_fraction",
                "last_container_status", "diagnostic_log_path",
                "deadline_ns", "wall_ns", "sweep_kind", "cpu_period_us", "cpu_quota_us",
            ],
            [{
                "run_id": run_id, "worker_id": "worker-00001", "physical_worker_id": "worker-00001",
                "logical_client_id": "00001", "resource_profile_id": "embedded_heap_32k_cpu_1c_100",
                "experiment_kind": "embedded_budget_singleton",
                "failure_class": "app_heap_budget_exceeded", "failure_detail": "APP_HEAP_BUDGET_EXCEEDED",
                "failure_evidence_source": "runner_observed_request_failure",
                "failure_action": "stop_run", "attribution_confidence": "exact_runner_operation",
                "attribution_source": "app_heap_budget_failure_payload", "failure_timestamp_ns": "10",
                "current_phase": "join", "current_operation_family": "welcome_receive",
                "current_benchmark_operation": "welcome_receive", "current_member_count": "32",
                "current_epoch": "31", "memory_model": "app-heap-budget",
                "app_heap_budget": "32k", "app_heap_budget_bytes": "32768",
                "heap_operation_peak_live_bytes": "40960",
            }],
        ),
        "run_status.csv": (
            [
                "run_id", "run_mode", "resource_experiment", "resource_failure_policy",
                "resource_profile_index", "resource_profile_id", "experiment_kind",
                "memory_model", "docker_memory_limit", "app_heap_budget",
                "app_heap_budget_bytes", "run_status", "completed",
                "valid_for_threshold_analysis", "valid_for_embedded_heap_threshold_analysis",
                "valid_for_docker_resource_analysis", "valid_for_clean_performance_plots",
                "valid_for_churn_recovery_analysis", "first_failure_timestamp_ns",
                "first_failed_worker_id", "first_failed_client_id", "first_failure_class",
                "first_failure_operation_family", "first_failure_benchmark_operation",
                "first_failure_member_count", "first_failure_epoch",
                "last_successful_operation_family", "last_successful_benchmark_operation",
                "last_successful_member_count", "last_successful_epoch",
                "preflight_passed", "resource_output_validation_passed",
                "sweep_kind", "strict_cpuset_satisfied", "notes",
            ],
            [{
                "run_id": run_id, "run_mode": "embedded-budget-singleton",
                "resource_experiment": "embedded-budget-singleton",
                "resource_failure_policy": "stop-on-profiled-failure",
                "resource_profile_index": "0", "resource_profile_id": "embedded_heap_32k_cpu_1c_100",
                "experiment_kind": "embedded_budget_singleton", "memory_model": "app-heap-budget",
                "docker_memory_limit": "256m", "app_heap_budget": "32k",
                "app_heap_budget_bytes": "32768", "run_status": "failed_app_heap_budget_exceeded",
                "completed": "false", "valid_for_threshold_analysis": "true",
                "valid_for_embedded_heap_threshold_analysis": "true",
                "valid_for_docker_resource_analysis": "false",
                "valid_for_clean_performance_plots": "false",
                "valid_for_churn_recovery_analysis": "false",
                "first_failure_timestamp_ns": "10", "first_failed_worker_id": "worker-00001",
                "first_failed_client_id": "00001", "first_failure_class": "app_heap_budget_exceeded",
                "first_failure_operation_family": "welcome_receive",
                "first_failure_benchmark_operation": "welcome_receive",
                "first_failure_member_count": "32", "first_failure_epoch": "31",
                "preflight_passed": "true", "resource_output_validation_passed": "false",
            }],
        ),
    }
    for name, (fieldnames, rows) in headers_rows.items():
        with (tmp_path / name).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    (tmp_path / "cpu_affinity_plan.json").write_text(json.dumps({
        "run_id": run_id,
        "online_cpu_mask_hex": "0x3",
        "profiled_mask_hex": "0x1",
        "background_mask_hex": "0x2",
        "profiled_assignments": [{
            "worker_id": "worker-00001",
            "container_name": "worker-00001",
            "assigned_cpus": [0],
            "assigned_cpu_count": 1,
            "rayon_num_threads": 1,
        }],
        "background_assignments": [{"container_name": "ds", "assigned_cpus": [1]}],
    }))
    (tmp_path / "cpu_affinity_preflight.csv").write_text(
        "run_id,check_name,container_name,container_role,expected_cpuset,docker_cpuset,host_pid,proc_cpus_allowed_list,thread_cpus_allowed_lists,observed_psr_cpus,status,message\n"
    )
    (tmp_path / "cpu_affinity_preflight_summary.json").write_text(json.dumps({"all_passed": True}))
    (tmp_path / "resource_summary.csv").write_text(
        "run_id,worker_id,physical_worker_id,logical_client_id,container_name,resource_profile_id,experiment_kind,cpuset_cpus,cpu_limit_cpus,memory_limit,memory_swap,rayon_num_threads,sample_count,max_memory_current,last_memory_current,memory_events_oom,memory_events_oom_kill,cpu_usage_usec_delta,cpu_nr_throttled_delta,cpu_throttled_usec_delta,cpu_throttled_time_fraction,max_thread_count,max_process_count,last_container_status,last_container_exit_code,last_container_oom_killed\n"
    )
    (tmp_path / "events.csv").write_text("client_id,failed_worker_id,failure_class,failure_detail\n00001,00001,app_heap_budget_exceeded,APP_HEAP_BUDGET_EXCEEDED\n")
    (tmp_path / "benchmark_outcome.json").write_text(json.dumps({"outcome_class": "app_heap_budget_exceeded"}))

    validator = Validator(str(tmp_path))

    assert validator.validate(), validator.report()


def test_embedded_budget_script_bash_syntax():
    script = Path(__file__).resolve().parents[3] / "benchmark_scripts" / "run_benchmark_embedded_budget_experiments.sh"
    result = subprocess.run(["bash", "-n", str(script.resolve())], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
