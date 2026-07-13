#!/usr/bin/env python3
"""Validate external-device metrics and persist attrition/last-operation evidence."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Iterable


def _nonzero(value: Any) -> bool:
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError):
        return False


def _integer(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _worker_id(row: dict[str, Any]) -> str:
    return str(row.get("client_id") or row.get("worker_id") or row.get("failed_worker_id") or "")


def _group_size(row: dict[str, Any]) -> int | None:
    for column in (
        "benchmark_active_size",
        "benchmark_target_size",
        "member_count_before",
        "member_count",
        "member_count_after",
    ):
        value = _integer(row.get(column))
        if value >= 0:
            return value
    return None


def _plot_group_size(row: dict[str, Any]) -> int | None:
    columns = (
        ("member_count", "member_count_after", "member_count_before")
        if row.get("op") == "welcome_receive_total_local"
        else ("member_count_before", "member_count", "member_count_after")
    )
    for column in columns:
        value = _integer(row.get(column))
        if value > 0:
            return value
    return None


def _coverage_operation(row: dict[str, Any]) -> str | None:
    op = str(row.get("op") or "")
    family = str(row.get("operation_family") or "")
    benchmark_operation = str(row.get("benchmark_operation") or "")
    source = str(row.get("membership_batch_source") or "")
    phase = str(row.get("benchmark_phase") or "")
    k = _integer(row.get("added_members_count"))
    density_add_receive_legacy = op == "commit_receive_total_local" and not source
    if benchmark_operation == "add_commit" and (
        source in {"external_density_k1", "external_density_k8"}
        or density_add_receive_legacy
    ):
        variant = "k1" if k == 1 else "k8" if k == 8 else None
        if variant and op == "add_commit_total_local" and family == "add_commit_create":
            return f"addcommit_{variant}"
        if variant and op == "commit_receive_total_local" and family == "commit_receive":
            return f"receiveaddcommit_{variant}"
    if op == "update_commit_create_total_local" and benchmark_operation == "self_update":
        return "selfupdate"
    if op == "commit_receive_total_local" and benchmark_operation == "self_update":
        return "receiveselfupdate"
    if op == "application_message_create_total_local" and benchmark_operation == "send_application_message":
        return "sendmessage"
    if op == "application_message_receive_total_local" and benchmark_operation == "send_application_message":
        return "receivemessage"
    if phase == "remove_rejoin":
        if op == "remove_commit_create_total_local" and benchmark_operation == "remove_commit":
            return "removecommit"
        if op == "commit_receive_total_local" and benchmark_operation == "remove_commit":
            return "receiveremovecommit"
        if op == "welcome_receive_total_local" and benchmark_operation == "welcome_receive":
            return "processwelcome"
    return None


def _operation(row: dict[str, Any]) -> str:
    return str(row.get("benchmark_operation") or row.get("op") or "")


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def summarize_run(
    run_dir: Path,
    expected_worker_ids: list[str],
    attrition_allowed_worker_ids: set[str],
    minimum_observations: int = 1,
) -> tuple[dict[str, Any], list[str]]:
    if minimum_observations < 1:
        raise ValueError("minimum_observations must be positive")
    events_path = run_dir / "events.csv"
    if not events_path.is_file() or events_path.stat().st_size == 0:
        raise ValueError(f"missing or empty events.csv: {events_path}")

    devices = {
        worker_id: {
            "worker_id": worker_id,
            "attrition_allowed": worker_id in attrition_allowed_worker_ids,
            "rows": 0,
            "alloc_bytes_positive_rows": 0,
            "cpu_process_ns_positive_rows": 0,
            "v11_primary_rows": 0,
            "last_event_ts_unix_ns": -1,
            "last_operation": None,
            "last_group_size": None,
            "failure": None,
        }
        for worker_id in expected_worker_ids
    }

    primary_total_ops = {
        "add_commit_total_local",
        "remove_commit_create_total_local",
        "update_commit_create_total_local",
        "application_message_create_total_local",
        "commit_receive_total_local",
        "application_message_receive_total_local",
        "welcome_receive_total_local",
    }

    valid_coverage_counts: dict[tuple[str, str, int, int], int] = {}
    remove_rejoin_mode = False
    with events_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            worker_id = _worker_id(row)
            if worker_id not in devices:
                continue
            device = devices[worker_id]
            device["rows"] += 1
            device["alloc_bytes_positive_rows"] += int(_nonzero(row.get("alloc_bytes")))
            device["cpu_process_ns_positive_rows"] += int(_nonzero(row.get("cpu_process_ns")))
            device["v11_primary_rows"] += int(str(row.get("op") or "") in primary_total_ops)
            remove_rejoin_mode = remove_rejoin_mode or row.get("benchmark_phase") == "remove_rejoin"
            coverage_operation = _coverage_operation(row)
            target = _integer(row.get("benchmark_target_size"))
            plot_size = _plot_group_size(row)
            if (
                coverage_operation
                and target > 0
                and plot_size is not None
                and _nonzero(row.get("cpu_process_ns"))
                and _nonzero(row.get("alloc_bytes"))
            ):
                key = (worker_id, coverage_operation, target, plot_size)
                valid_coverage_counts[key] = valid_coverage_counts.get(key, 0) + 1
            timestamp = _integer(row.get("ts_unix_ns"))
            if timestamp >= device["last_event_ts_unix_ns"]:
                device["last_event_ts_unix_ns"] = timestamp
                device["last_operation"] = _operation(row) or None
                device["last_group_size"] = _group_size(row)

    for event in _read_jsonl(run_dir / "runner-events.jsonl"):
        worker_id = _worker_id(event)
        if worker_id not in devices or event.get("event_kind") != "worker_failure":
            continue
        timestamp = _integer(event.get("ts_unix_ns"))
        current = devices[worker_id].get("failure")
        if current is None or timestamp >= _integer(current.get("ts_unix_ns")):
            failure = {
                "ts_unix_ns": timestamp,
                "failure_class": event.get("failure_class"),
                "failure_action": event.get("failure_action"),
                "failure_evidence_source": event.get("failure_evidence_source"),
                "benchmark_phase": event.get("benchmark_phase"),
                "benchmark_operation": event.get("benchmark_operation"),
                "benchmark_active_size": event.get("benchmark_active_size"),
                "benchmark_target_size": event.get("benchmark_target_size"),
            }
            devices[worker_id]["failure"] = failure
            if timestamp >= devices[worker_id]["last_event_ts_unix_ns"]:
                devices[worker_id]["last_event_ts_unix_ns"] = timestamp
                devices[worker_id]["last_operation"] = _operation(event) or None
                devices[worker_id]["last_group_size"] = _group_size(event)

    errors: list[str] = []
    for worker_id, device in devices.items():
        metrics_complete = (
            device["rows"] > 0
            and device["alloc_bytes_positive_rows"] > 0
            and device["cpu_process_ns_positive_rows"] > 0
            and device["v11_primary_rows"] > 0
        )
        attrited = device["failure"] is not None
        device["metrics_complete"] = metrics_complete
        device["attrited"] = attrited
        device["status"] = "attrited" if attrited else ("complete" if metrics_complete else "missing")

        if not metrics_complete:
            if worker_id in attrition_allowed_worker_ids and attrited:
                continue
            errors.append(
                f"{worker_id}: incomplete external metrics without allowed attrition evidence "
                f"(rows={device['rows']}, alloc={device['alloc_bytes_positive_rows']}, "
                f"cpu_process={device['cpu_process_ns_positive_rows']}, "
                f"v11_primary={device['v11_primary_rows']})"
            )

    scenario_plan_path = run_dir / "scenario_plan.json"
    expected_targets: list[int] = []
    if scenario_plan_path.is_file():
        scenario_plan = json.loads(scenario_plan_path.read_text(encoding="utf-8"))
        expected_targets = sorted(
            {_integer(value) for value in scenario_plan.get("plateau_sequence", []) if _integer(value) > 0}
        )
    if minimum_observations > 1 and not expected_targets:
        errors.append("scenario_plan.json has no plateau_sequence for density validation")

    regular_operations = (
        "addcommit_k1",
        "addcommit_k8",
        "receiveaddcommit_k1",
        "receiveaddcommit_k8",
        "selfupdate",
        "receiveselfupdate",
        "sendmessage",
        "receivemessage",
    )
    remove_operations = ("removecommit", "receiveremovecommit", "processwelcome")
    required_operations = remove_operations if remove_rejoin_mode else regular_operations
    coverage_cells = []
    for worker_id in expected_worker_ids:
        failure = devices[worker_id].get("failure")
        failure_target = _integer((failure or {}).get("benchmark_target_size"))
        required_targets = [
            target
            for target in expected_targets
            if not (failure_target > 0 and worker_id in attrition_allowed_worker_ids and target >= failure_target)
        ]
        for operation in required_operations:
            for target in required_targets:
                if operation.endswith("_k8") and target < len(expected_worker_ids) + 8:
                    continue
                k = 1 if operation.endswith("_k1") else 8 if operation.endswith("_k8") else 0
                expected_plot_size = target - k if k else target
                count = valid_coverage_counts.get(
                    (worker_id, operation, target, expected_plot_size),
                    0,
                )
                passes = count >= minimum_observations
                coverage_cells.append(
                    {
                        "worker_id": worker_id,
                        "operation": operation,
                        "benchmark_target_size": target,
                        "plot_group_size": expected_plot_size,
                        "valid_observations": count,
                        "minimum_required": minimum_observations,
                        "passes": passes,
                    }
                )
                if not passes:
                    errors.append(
                        f"{worker_id}/{operation}/target={target}/group={expected_plot_size}: "
                        f"{count} valid observations, require {minimum_observations}"
                    )

    report = {
        "schema_version": 1,
        "run_dir": str(run_dir.resolve()),
        "success": not errors,
        "expected_worker_ids": expected_worker_ids,
        "attrition_allowed_worker_ids": sorted(attrition_allowed_worker_ids),
        "mode": "remove_rejoin" if remove_rejoin_mode else "regular",
        "expected_plateau_targets": expected_targets,
        "minimum_observations_per_cell": minimum_observations,
        "coverage_cells": coverage_cells,
        "devices": list(devices.values()),
        "errors": errors,
    }
    return report, errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--expected-worker", action="append", default=[], required=True)
    parser.add_argument("--attrition-allowed-worker", action="append", default=[])
    parser.add_argument("--minimum-observations", type=int, default=1)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report, errors = summarize_run(
        args.run_dir,
        args.expected_worker,
        set(args.attrition_allowed_worker),
        args.minimum_observations,
    )
    output = args.output or args.run_dir / "external_device_coverage.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    for device in report["devices"]:
        print(
            f"  [external] {device['worker_id']}: status={device['status']} "
            f"rows={device['rows']} alloc_positive={device['alloc_bytes_positive_rows']} "
            f"cpu_process_positive={device['cpu_process_ns_positive_rows']} "
            f"v11_primary={device['v11_primary_rows']} "
            f"last_operation={device['last_operation']} last_group_size={device['last_group_size']}"
        )
    print(f"  [external] report={output}")
    for error in errors:
        print(f"ERROR: {error}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
