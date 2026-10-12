#!/usr/bin/env python3
"""Strict coverage gate for Signal external-device benchmark runs."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any


CANONICAL_TOTALS = (
    "signal_session_establish.total",
    "signal_application_message_create.total",
    "signal_application_message_receive.total",
)
APPLICATION_TOTALS = set(CANONICAL_TOTALS[1:])


def positive(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0


def plateau_sequence(
    min_size: int,
    max_size: int,
    step_size: int,
    switch_at: int | None,
    step_after_switch: int | None,
) -> list[int]:
    if min_size < 2 or max_size < min_size or step_size < 1:
        raise ValueError("invalid min/max/step size")
    if (switch_at is None) != (step_after_switch is None):
        raise ValueError("switch_at and step_after_switch must be set together")

    result = [min_size]
    current = min_size
    while current < max_size:
        if switch_at is not None and step_after_switch is not None:
            if current < switch_at:
                current = min(current + step_size, switch_at, max_size)
            else:
                current = min(current + step_after_switch, max_size)
        else:
            current = min(current + step_size, max_size)
        if result[-1] != current:
            result.append(current)
    return result


def parse_explicit_plateaus(value: str) -> list[int]:
    try:
        sizes = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError as error:
        raise ValueError("plateau_sizes must be comma-separated integers") from error
    if not sizes or any(size < 1 for size in sizes):
        raise ValueError("plateau_sizes must contain positive integers")
    if any(left >= right for left, right in zip(sizes, sizes[1:])):
        raise ValueError("plateau_sizes must be strictly increasing")
    return sizes


def expected_plateau_targets(args: argparse.Namespace) -> list[int]:
    explicit = parse_explicit_plateaus(args.plateau_sizes) if args.plateau_sizes else None
    scenario_plan = args.events.parent / "scenario_plan.json"
    if scenario_plan.is_file():
        plan = json.loads(scenario_plan.read_text(encoding="utf-8"))
        planned = [int(value) for value in plan.get("plateau_sequence", [])]
        if not planned:
            raise ValueError("scenario_plan.json has no plateau_sequence")
        if explicit is not None and explicit != planned:
            raise ValueError(
                f"--plateau-sizes {explicit} does not match scenario plan {planned}"
            )
        return planned
    if explicit is not None:
        return explicit
    return plateau_sequence(
        args.min_size,
        args.max_size,
        args.step_size,
        args.switch_at,
        args.step_after_switch,
    )


def parse_failure_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid runner event JSON at {path}:{line_number}: {error}"
                ) from error
    return events


def failure_boundary(
    events: list[dict[str, Any]],
    external_ids: set[str],
    luckfox_id: str,
    allow_luckfox_attrition: bool,
) -> tuple[int | None, list[str]]:
    errors = []
    luckfox_targets = []
    for event in events:
        failed_id = str(event.get("failed_worker_id") or "")
        if failed_id not in external_ids:
            continue
        if failed_id != luckfox_id or not allow_luckfox_attrition:
            errors.append(f"unexpected external-device failure: {failed_id}")
            continue
        evidence = str(event.get("failure_evidence_source") or "")
        action = str(event.get("failure_action") or "")
        if not evidence or not any(token in action for token in ("remove", "drop")):
            errors.append(
                "Luckfox attrition lacks authoritative evidence or a remove/drop action"
            )
            continue
        try:
            luckfox_targets.append(int(event["benchmark_target_size"]))
        except (KeyError, TypeError, ValueError):
            errors.append("Luckfox attrition is missing benchmark_target_size")
    return (min(luckfox_targets) if luckfox_targets else None), errors


def validate(args: argparse.Namespace) -> tuple[dict[str, Any], list[str]]:
    if args.minimum_observations < 1:
        raise ValueError("minimum_observations must be positive")
    expected_targets = expected_plateau_targets(args)
    expected_payloads = {
        int(value.strip())
        for value in args.payload_sizes.replace("[", "").replace("]", "").split(",")
        if value.strip()
    }
    if not expected_payloads:
        raise ValueError("at least one payload size is required")

    layout = json.loads(args.layout.read_text(encoding="utf-8"))
    external_ids = set(args.external_worker_id)
    profiled = [client for client in layout.get("clients", []) if client.get("profile_enabled")]
    profiled_ids = {str(client.get("client_id") or "") for client in profiled}
    docker_profiled = [
        client for client in profiled if str(client.get("client_id") or "") not in external_ids
    ]

    errors = []
    missing_layout = sorted(external_ids - profiled_ids)
    if missing_layout:
        errors.append(f"external devices not profiled in layout: {missing_layout}")
    if len(docker_profiled) != args.expected_profiled_docker:
        errors.append(
            f"expected {args.expected_profiled_docker} profiled Docker clients, "
            f"observed {len(docker_profiled)}"
        )

    failure_events = parse_failure_events(args.runner_events)
    luckfox_failure_target, failure_errors = failure_boundary(
        failure_events,
        external_ids,
        args.luckfox_id,
        args.allow_luckfox_attrition,
    )
    errors.extend(failure_errors)

    observed: dict[tuple[str, str], set[tuple[int, int | None]]] = defaultdict(set)
    valid_cell_counts: dict[tuple[str, str, int, int | None], int] = defaultdict(int)
    row_counts: dict[tuple[str, str], int] = defaultdict(int)
    metric_failures: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"cpu_process_ns": 0, "alloc_bytes": 0, "success": 0}
    )
    last_observed: dict[str, Any] | None = None

    with args.events.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            participant = row.get("participant_id") or row.get("client_id") or ""
            operation = row.get("op") or ""
            target_raw = row.get("benchmark_target_size") or ""
            if participant not in external_ids or operation not in CANONICAL_TOTALS or not target_raw:
                continue
            try:
                target = int(target_raw)
            except ValueError:
                errors.append(
                    f"invalid benchmark_target_size for {participant}/{operation}: {target_raw!r}"
                )
                continue
            if (
                participant == args.luckfox_id
                and luckfox_failure_target is not None
                and target >= luckfox_failure_target
            ):
                # The failure plateau can contain a partial command row.  It is
                # outside the required coverage domain once authoritative
                # attrition evidence records the Luckfox removal boundary.
                continue
            payload: int | None = None
            if operation in APPLICATION_TOTALS:
                payload_raw = row.get("benchmark_payload_size") or ""
                if not payload_raw:
                    # Non-sampled fanout receives can still write a canonical
                    # total on a profiled worker, but they are not measurement
                    # rows and deliberately carry no payload metadata.
                    continue
                try:
                    payload = int(payload_raw)
                except ValueError:
                    errors.append(
                        f"invalid payload for eligible {participant}/{operation} "
                        f"target {target}: {payload_raw!r}"
                    )
                    continue

            key = (participant, operation)
            observed[key].add((target, payload))
            row_counts[key] += 1
            cpu_valid = positive(row.get("cpu_process_ns"))
            alloc_valid = positive(row.get("alloc_bytes"))
            success_valid = str(row.get("success") or "").lower() in ("true", "1")
            if not cpu_valid:
                metric_failures[key]["cpu_process_ns"] += 1
            if not alloc_valid:
                metric_failures[key]["alloc_bytes"] += 1
            if not success_valid:
                metric_failures[key]["success"] += 1
            if cpu_valid and alloc_valid and success_valid:
                valid_cell_counts[(participant, operation, target, payload)] += 1

            if participant == args.luckfox_id:
                try:
                    timestamp = int(row.get("ts_unix_ns") or 0)
                except ValueError:
                    timestamp = 0
                candidate = {
                    "operation": operation,
                    "benchmark_target_size": target,
                    "benchmark_active_size": int(row.get("benchmark_active_size") or target),
                    "timestamp_ns": timestamp,
                }
                if last_observed is None or (
                    candidate["timestamp_ns"], candidate["benchmark_target_size"]
                ) > (
                    last_observed["timestamp_ns"],
                    last_observed["benchmark_target_size"],
                ):
                    last_observed = candidate

    coverage = []
    for participant in sorted(external_ids):
        required_targets = expected_targets
        attrited = participant == args.luckfox_id and luckfox_failure_target is not None
        if attrited:
            required_targets = [target for target in expected_targets if target < luckfox_failure_target]
        for operation in CANONICAL_TOTALS:
            if operation in APPLICATION_TOTALS:
                required = {
                    (target, payload)
                    for target in required_targets
                    for payload in expected_payloads
                }
            else:
                required = {(target, None) for target in required_targets}
            missing = sorted(required - observed[(participant, operation)])
            underfilled = [
                (target, payload, valid_cell_counts[(participant, operation, target, payload)])
                for target, payload in sorted(required)
                if valid_cell_counts[(participant, operation, target, payload)]
                < args.minimum_observations
            ]
            failures = metric_failures[(participant, operation)]
            if missing:
                errors.append(f"missing coverage for {participant}/{operation}: {missing}")
            if underfilled:
                errors.append(
                    f"coverage below {args.minimum_observations} valid observations for "
                    f"{participant}/{operation}: {underfilled}"
                )
            if any(failures.values()):
                errors.append(
                    f"incomplete eligible metrics for {participant}/{operation}: {failures}"
                )
            coverage.append(
                {
                    "participant_id": participant,
                    "operation": operation,
                    "eligible_rows": row_counts[(participant, operation)],
                    "required_cells": len(required),
                    "observed_cells": len(observed[(participant, operation)]),
                    "missing_cells": [list(cell) for cell in missing],
                    "underfilled_cells": [list(cell) for cell in underfilled],
                    "minimum_observations_per_cell": args.minimum_observations,
                    "metric_failures": failures,
                    "attrited": attrited,
                }
            )

    summary = {
        "status": "pass" if not errors else "fail",
        "events": str(args.events),
        "expected_plateau_targets": expected_targets,
        "expected_payload_sizes": sorted(expected_payloads),
        "minimum_observations_per_cell": args.minimum_observations,
        "external_worker_ids": sorted(external_ids),
        "luckfox": {
            "participant_id": args.luckfox_id,
            "failure_target_size": luckfox_failure_target,
            "last_completed_canonical_operation": last_observed,
        },
        "coverage": coverage,
        "errors": errors,
    }
    return summary, errors


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("events", type=Path)
    parser.add_argument("--layout", type=Path, required=True)
    parser.add_argument("--runner-events", type=Path, required=True)
    parser.add_argument("--external-worker-id", action="append", required=True)
    parser.add_argument("--luckfox-id", default="pico-plus-00001")
    parser.add_argument("--allow-luckfox-attrition", action="store_true")
    parser.add_argument("--min-size", type=int, required=True)
    parser.add_argument("--max-size", type=int, required=True)
    parser.add_argument("--step-size", type=int, required=True)
    parser.add_argument("--switch-at", type=int)
    parser.add_argument("--step-after-switch", type=int)
    parser.add_argument("--plateau-sizes")
    parser.add_argument("--payload-sizes", required=True)
    parser.add_argument("--expected-profiled-docker", type=int, default=1)
    parser.add_argument("--minimum-observations", type=int, default=1)
    parser.add_argument("--summary", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary_path = args.summary or args.events.parent / "external_device_coverage_summary.json"
    try:
        summary, errors = validate(args)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}")
        return 2
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(f"Signal external coverage: {summary['status'].upper()}")
    print(f"  expected plateau targets: {summary['expected_plateau_targets']}")
    for cell in summary["coverage"]:
        print(
            f"  {cell['participant_id']} {cell['operation']}: "
            f"rows={cell['eligible_rows']} required={cell['required_cells']} "
            f"observed={cell['observed_cells']} missing={len(cell['missing_cells'])} "
            f"underfilled={len(cell['underfilled_cells'])} "
            f"attrited={cell['attrited']}"
        )
    if errors:
        for error in errors:
            print(f"  ERROR: {error}")
    print(f"  summary: {summary_path}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
