import csv
import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "validate_external_device_coverage.py"
EXTERNAL_IDS = ("pico-plus-00001", "raspi5-00001", "raspi3bp-00001")
TOTALS = (
    "signal_session_establish.total",
    "signal_application_message_create.total",
    "signal_application_message_receive.total",
)


def write_run(tmp_path, *, missing=None, missing_cpu=None, luckfox_failure=None):
    layout = {
        "clients": [
            *[
                {"client_id": participant, "profile_enabled": True}
                for participant in EXTERNAL_IDS
            ],
            {"client_id": "00001", "profile_enabled": True},
        ]
    }
    layout_path = tmp_path / "worker_layout.json"
    layout_path.write_text(json.dumps(layout), encoding="utf-8")
    (tmp_path / "scenario_plan.json").write_text(
        json.dumps({"plateau_sequence": [8, 104, 200]}), encoding="utf-8"
    )

    fields = [
        "participant_id",
        "op",
        "benchmark_target_size",
        "benchmark_active_size",
        "benchmark_payload_size",
        "cpu_process_ns",
        "alloc_bytes",
        "success",
        "ts_unix_ns",
    ]
    events_path = tmp_path / "events.csv"
    with events_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        timestamp = 0
        for participant in EXTERNAL_IDS:
            targets = [8, 104, 200]
            if participant == "pico-plus-00001" and luckfox_failure is not None:
                targets = [target for target in targets if target < luckfox_failure]
            for operation in TOTALS:
                for target in targets:
                    cell = (participant, operation, target)
                    if cell == missing:
                        continue
                    timestamp += 1
                    writer.writerow(
                        {
                            "participant_id": participant,
                            "op": operation,
                            "benchmark_target_size": target,
                            "benchmark_active_size": target,
                            "benchmark_payload_size": "512" if "application" in operation else "",
                            "cpu_process_ns": "" if cell == missing_cpu else "17",
                            "alloc_bytes": "19",
                            "success": "true",
                            "ts_unix_ns": timestamp,
                        }
                    )
            if participant == "pico-plus-00001" and luckfox_failure is not None:
                writer.writerow(
                    {
                        "participant_id": participant,
                        "op": "signal_application_message_receive.total",
                        "benchmark_target_size": luckfox_failure,
                        "benchmark_active_size": luckfox_failure,
                        "benchmark_payload_size": "512",
                        "cpu_process_ns": "",
                        "alloc_bytes": "",
                        "success": "false",
                        "ts_unix_ns": timestamp + 1,
                    }
                )
            writer.writerow(
                {
                    "participant_id": participant,
                    "op": "signal_application_message_receive.total",
                    "benchmark_target_size": "104",
                    "benchmark_active_size": "104",
                    "benchmark_payload_size": "",
                    "cpu_process_ns": "",
                    "alloc_bytes": "",
                    "success": "true",
                    "ts_unix_ns": timestamp + 1,
                }
            )

    runner_path = tmp_path / "runner-events.jsonl"
    if luckfox_failure is None:
        runner_path.write_text("", encoding="utf-8")
    else:
        runner_path.write_text(
            json.dumps(
                {
                    "failed_worker_id": "pico-plus-00001",
                    "failure_evidence_source": "worker_health",
                    "failure_action": "remove_active_actor_and_retry",
                    "benchmark_target_size": luckfox_failure,
                }
            )
            + "\n",
            encoding="utf-8",
        )
    return events_path, layout_path, runner_path


def run_validator(tmp_path, **kwargs):
    events, layout, runner = write_run(tmp_path, **kwargs)
    command = [
        sys.executable,
        str(SCRIPT),
        str(events),
        "--layout",
        str(layout),
        "--runner-events",
        str(runner),
        "--min-size",
        "8",
        "--max-size",
        "200",
        "--step-size",
        "96",
        "--plateau-sizes",
        "8,104,200",
        "--payload-sizes",
        "512",
    ]
    for participant in EXTERNAL_IDS:
        command.extend(("--external-worker-id", participant))
    if kwargs.get("luckfox_failure") is not None:
        command.append("--allow-luckfox-attrition")
    return subprocess.run(command, text=True, capture_output=True, check=False)


def test_complete_per_device_coverage_passes(tmp_path):
    result = run_validator(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_missing_per_device_plateau_fails(tmp_path):
    result = run_validator(
        tmp_path,
        missing=("raspi5-00001", "signal_application_message_receive.total", 104),
    )
    assert result.returncode == 1
    assert "missing coverage" in result.stdout


def test_missing_process_cpu_fails(tmp_path):
    result = run_validator(
        tmp_path,
        missing_cpu=("raspi3bp-00001", "signal_session_establish.total", 200),
    )
    assert result.returncode == 1
    assert "cpu_process_ns" in result.stdout


def test_authoritative_luckfox_attrition_records_boundary_and_passes(tmp_path):
    result = run_validator(tmp_path, luckfox_failure=104)
    assert result.returncode == 0, result.stdout + result.stderr
    summary = json.loads(
        (tmp_path / "external_device_coverage_summary.json").read_text(encoding="utf-8")
    )
    assert summary["luckfox"]["failure_target_size"] == 104
    assert summary["luckfox"]["last_completed_canonical_operation"][
        "benchmark_target_size"
    ] == 8
