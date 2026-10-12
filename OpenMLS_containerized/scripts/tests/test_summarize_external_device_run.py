import csv
import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from summarize_external_device_run import _coverage_operation, summarize_run


FIELDNAMES = [
    "ts_unix_ns",
    "client_id",
    "op",
    "benchmark_operation",
    "benchmark_active_size",
    "member_count",
    "alloc_bytes",
    "cpu_process_ns",
]


def write_events(run_dir, rows):
    with (run_dir / "events.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def test_complete_devices_record_last_operation(tmp_path):
    write_events(
        tmp_path,
        [
            {
                "ts_unix_ns": "10",
                "client_id": worker,
                "op": "add_commit_total_local",
                "benchmark_operation": "add_commit",
                "benchmark_active_size": "64",
                "alloc_bytes": "100",
                "cpu_process_ns": "200",
            }
            for worker in ("pico", "pi5")
        ],
    )

    report, errors = summarize_run(tmp_path, ["pico", "pi5"], {"pico"})

    assert not errors
    assert report["success"]
    assert all(device["status"] == "complete" for device in report["devices"])
    assert all(device["last_operation"] == "add_commit" for device in report["devices"])
    assert all(device["last_group_size"] == 64 for device in report["devices"])


def test_allowed_attrition_uses_authoritative_runner_event(tmp_path):
    write_events(
        tmp_path,
        [
            {
                "ts_unix_ns": "10",
                "client_id": "pi5",
                "op": "add_commit_total_local",
                "benchmark_operation": "add_commit",
                "benchmark_active_size": "64",
                "alloc_bytes": "100",
                "cpu_process_ns": "200",
            }
        ],
    )
    failure = {
        "ts_unix_ns": 20,
        "event_kind": "worker_failure",
        "failed_worker_id": "pico",
        "failure_class": "oom_kill",
        "failure_action": "evict_oom_eviction_recipient_and_retry",
        "benchmark_phase": "membership_add",
        "benchmark_operation": "add_commit",
        "benchmark_active_size": 128,
        "benchmark_target_size": 192,
    }
    (tmp_path / "runner-events.jsonl").write_text(json.dumps(failure) + "\n", encoding="utf-8")

    report, errors = summarize_run(tmp_path, ["pico", "pi5"], {"pico"})

    assert not errors
    pico = next(device for device in report["devices"] if device["worker_id"] == "pico")
    assert pico["status"] == "attrited"
    assert pico["last_operation"] == "add_commit"
    assert pico["last_group_size"] == 128


def test_required_device_cannot_disappear_without_evidence(tmp_path):
    write_events(
        tmp_path,
        [
            {
                "ts_unix_ns": "10",
                "client_id": "pico",
                "op": "add_commit_total_local",
                "benchmark_operation": "add_commit",
                "benchmark_active_size": "64",
                "alloc_bytes": "100",
                "cpu_process_ns": "200",
            }
        ],
    )

    report, errors = summarize_run(tmp_path, ["pico", "pi5"], {"pico"})

    assert errors
    assert not report["success"]
    assert "pi5" in errors[0]


def test_legacy_add_receive_without_source_retains_density_coverage() -> None:
    receive = {
        "op": "commit_receive_total_local",
        "operation_family": "commit_receive",
        "benchmark_operation": "add_commit",
        "membership_batch_source": "",
        "added_members_count": "1",
    }
    actor = {
        **receive,
        "op": "add_commit_total_local",
        "operation_family": "add_commit_create",
    }

    assert _coverage_operation(receive) == "receiveaddcommit_k1"
    assert _coverage_operation(actor) is None
    actor["membership_batch_source"] = "external_density_k1"
    assert _coverage_operation(actor) == "addcommit_k1"
