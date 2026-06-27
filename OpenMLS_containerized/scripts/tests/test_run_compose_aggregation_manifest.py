import csv
import json
import sys
from argparse import Namespace
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from run_compose_benchmark import (
    should_run_cleanup_aggregation,
    validate_artifacts,
    write_artifact_validation,
    write_integrated_aggregation_manifest,
)


def test_integrated_aggregation_manifest_describes_existing_output(tmp_path):
    events_path = tmp_path / "events.csv"
    with events_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["op"])
        writer.writeheader()
        writer.writerows([{"op": "one"}, {"op": "two"}])
    (tmp_path / "client-00001.jsonl").write_text("{}\n", encoding="utf-8")

    write_integrated_aggregation_manifest(tmp_path, "run-1")

    manifest = json.loads((tmp_path / "aggregation_manifest.json").read_text())
    assert manifest["run_id"] == "run-1"
    assert manifest["aggregation_mode"] == "runner_integrated"
    assert manifest["aggregation_status"] == "complete_success"
    assert manifest["events_written"] == 2
    assert manifest["input_files_found"] == 1


def test_validate_artifacts_allows_no_aggregate_output(tmp_path):
    (tmp_path / "worker_layout.json").write_text("{}", encoding="utf-8")
    (tmp_path / "client-00001.jsonl").write_text("{}\n", encoding="utf-8")

    validate_artifacts(tmp_path, "hybrid", require_aggregate=False)


def test_validate_artifacts_requires_events_for_integrated_aggregate(tmp_path):
    (tmp_path / "worker_layout.json").write_text("{}", encoding="utf-8")
    (tmp_path / "client-00001.jsonl").write_text("{}\n", encoding="utf-8")

    try:
        validate_artifacts(tmp_path, "hybrid", require_aggregate=True)
    except RuntimeError as exc:
        assert "Missing aggregated CSV" in str(exc)
    else:
        raise AssertionError("expected missing events.csv to fail aggregate validation")


def test_cleanup_aggregation_runs_for_interrupted_local_profile(tmp_path):
    args = Namespace(preflight_only=False, enable_external_devices=False)
    (tmp_path / "client-00001.jsonl").write_text("{}\n", encoding="utf-8")

    assert should_run_cleanup_aggregation(args, tmp_path)

    (tmp_path / "events.csv").write_text("op\none\n", encoding="utf-8")
    assert not should_run_cleanup_aggregation(args, tmp_path)


def test_artifact_validation_treats_events_as_optional_without_aggregation(tmp_path):
    for name in (
        "resource_profiles.csv",
        "worker_resource_assignments.csv",
        "cpu_affinity_plan.json",
        "scenario_plan.json",
        "run_status.csv",
    ):
        (tmp_path / name).write_text("content", encoding="utf-8")

    write_artifact_validation(
        tmp_path,
        run_success=True,
        primary_outcome="completed",
        aggregation_status="",
    )

    payload = json.loads((tmp_path / "artifact_validation.json").read_text())
    assert payload["aggregation"]["attempted"] is False
    assert payload["sidecar_validation"]["valid"] is True
    assert "events.csv" not in payload["sidecar_validation"]["missing"]
    assert "aggregation_manifest.json" not in payload["sidecar_validation"]["missing"]
