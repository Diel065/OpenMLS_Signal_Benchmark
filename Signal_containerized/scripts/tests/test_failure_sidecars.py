from __future__ import annotations

import csv
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import run_compose_benchmark


def test_failure_sidecars_classify_oom_from_resource_summary(tmp_path: Path) -> None:
    run_id = "oom-sidecar-test"
    worker_failures = tmp_path / "worker_failures.csv"
    worker_failures.write_text("old\n", encoding="utf-8")
    worker_failures.chmod(0o444)

    with (tmp_path / "resource_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "physical_worker_id",
                "container_id",
                "resource_limit_memory_bytes",
                "last_container_status",
                "last_container_exit_code",
                "last_container_oom_killed",
                "memory_events_oom",
                "memory_events_oom_kill",
                "max_memory_current",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "physical_worker_id": "worker-00006",
                "container_id": "abc123",
                "resource_limit_memory_bytes": "8388608",
                "last_container_status": "exited",
                "last_container_exit_code": "137",
                "last_container_oom_killed": "true",
                "memory_events_oom": "0",
                "memory_events_oom_kill": "0",
                "max_memory_current": "8269824",
            }
        )

    run_compose_benchmark.write_failure_sidecars_from_artifacts(tmp_path, run_id, False)

    rows = list(csv.DictReader(worker_failures.open(newline="", encoding="utf-8")))
    assert len(rows) == 1
    assert rows[0]["worker_id"] == "00006"
    assert rows[0]["physical_worker_id"] == "worker-00006"
    assert rows[0]["container_id"] == "abc123"
    assert rows[0]["failure_class"] == "hard_ram_oom_kill"
    assert rows[0]["container_exit_code"] == "137"
    assert rows[0]["container_oom_killed"] == "true"

    status_rows = list(csv.DictReader((tmp_path / "run_status.csv").open(newline="", encoding="utf-8")))
    assert status_rows[0]["run_status"] == "failed_hard_ram_oom_kill"
    assert status_rows[0]["completed"] == "false"
