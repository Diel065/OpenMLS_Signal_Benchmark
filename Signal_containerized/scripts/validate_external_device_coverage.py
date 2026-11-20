#!/usr/bin/env -S uv run --frozen python
"""Strict coverage gate for Signal external-device benchmark runs."""

import argparse
import csv
import json
import math
import re
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


def validate_external_join(args: argparse.Namespace) -> tuple[dict[str, Any], list[str]]:
    run_dir = args.events.parent
    plan = json.loads((run_dir / "scenario_plan.json").read_text())
    if (plan.get("external_session_join") is not True or plan.get("group_model") != "pairwise"
            or plan.get("plateau_order") != "ascending" or plan.get("spread_profiled_joiners")):
        raise ValueError("expected an ascending external-session-join pairwise plan without spread joiners")
    targets = expected_plateau_targets(args)
    layout = json.loads(args.layout.read_text())
    ids = set(args.external_worker_id)
    clients = {c["client_id"]: c for c in layout["clients"] if c.get("profile_enabled")}
    errors = []
    if set(clients) != ids or any(clients[i].get("execution_backend") != "real_device" for i in ids & set(clients)):
        errors.append("join profiling must cover exactly the selected external devices")
    boundary, failures = failure_boundary(parse_failure_events(args.runner_events), ids,
                                          args.luckfox_id, False)
    errors.extend(failures)
    cap = plan.get("luckfox_join_max_size")
    if cap is not None:
        reason = run_dir / "luckfox_join_cap_reason.txt"
        if cap not in targets or not reason.is_file() or not reason.read_text().strip():
            errors.append("Luckfox cap requires a planned plateau and recorded failure reason")
    windows = defaultdict(list)
    joins = defaultdict(list)
    with args.events.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row.get("participant_id"), row.get("request_id"))
            if row.get("benchmark_phase") == "application":
                errors.append("application phase found in join-only run")
            if row.get("measurement_plane") == "wrapper_total" and key[1]:
                start = int(row["ts_unix_ns"])
                windows[key].append((start, start + int(row["wall_ns"])))
            if row.get("op") == "session_establish_pair_wrapper" and row.get("benchmark_operation") == "establish_session_external_join":
                target = int(row["benchmark_target_size"])
                if key[0] not in ids or target not in targets:
                    errors.append(f"unexpected join device or plateau: {key[0]}/N={target}")
                if row.get("measurement_plane") != "wrapper_total" or row.get("benchmark_phase") != "enrollment":
                    errors.append(f"join row has the wrong measurement plane or phase: {key}")
                joins[(key[0], target)].append({
                    k: row.get(k, "") for k in (
                        "request_id", "peer_id", "success", "new_session_established", "cpu_process_ns",
                        "alloc_bytes", "wall_ns", "benchmark_active_size", "workflow_pair_index",
                        "workflow_pair_count", "benchmark_workflow_id",
                    )
                })
    one_time = set()
    timing_errors = 0
    with args.events.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            key = (row.get("participant_id"), row.get("request_id"))
            if not key[1]:
                continue
            timestamp = int(row["ts_unix_ns"])
            if not any(start - 1_000_000 <= timestamp <= end + 1_000_000 for start, end in windows[key]):
                timing_errors += 1
            if row.get("benchmark_operation") == "establish_session_external_join":
                present = row.get("classical_one_time_prekey_present", "").lower()
                if present == "true":
                    one_time.add(key)
                elif present == "false":
                    errors.append(f"join used a bundle without a classical one-time prekey: {key}")
    if timing_errors:
        errors.append(f"{timing_errors} request-tagged events fall outside their wrapper window (1 ms allowance)")
    maintenance = defaultdict(list)
    for line in (run_dir / "terminal_output.txt").read_text().splitlines():
        marker = "[external-join-prekey] "
        if marker not in line:
            continue
        record = json.loads(line.split(marker, 1)[1])
        message = record["maintenance"]
        fields = {k: int(v) for k, v in re.findall(r"\b(available|threshold|before|after|refill)=(\d+)", message)}
        if "available" in fields and "threshold" in fields:
            valid = fields["available"] > fields["threshold"]
        else:
            valid = all(k in fields for k in ("before", "after", "refill")) and fields["refill"] > 0 and fields["after"] > fields["before"]
        if not valid:
            errors.append(f"unverified prekey maintenance reply: {record}")
        maintenance[(record["actor"], record["target"])].append(record)
    coverage = []
    for device in sorted(ids):
        memory_path = run_dir / "device_memory" / f"{clients.get(device, {}).get('physical_worker_id', device)}.status.txt"
        memory = memory_path.read_text() if memory_path.exists() else ""
        peaks = re.findall(r"^VmHWM:\s+(\d+)\s+kB", memory, re.MULTILINE)
        peak = int(peaks[0]) if len(peaks) == 1 else None
        if not peak:
            errors.append(f"missing unique worker peak RSS for {device}")
        for target in targets:
            rows = joins[(device, target)]
            attrited = device == args.luckfox_id and (
                (boundary is not None and target >= boundary) or (cap is not None and target > cap))
            expected = target - 1
            cell_errors = []
            if attrited and rows:
                cell_errors.append("rows above the explicit Luckfox cap must not be silently used")
            if not attrited:
                if cap is not None and target > cap and any(r["peer_id"] == args.luckfox_id for r in rows):
                    cell_errors.append("capped Luckfox remains a join peer")
                if len(rows) != expected or len({r["peer_id"] for r in rows}) != expected or any(r["peer_id"] == device for r in rows):
                    cell_errors.append(f"expected exactly {expected} distinct-peer join rows, got {len(rows)}")
                if ({int(r["workflow_pair_index"]) for r in rows} != set(range(expected))
                        or len({r["benchmark_workflow_id"] for r in rows}) != 1
                        or any(not r["benchmark_workflow_id"] or not r["request_id"] for r in rows)
                        or len({r["request_id"] for r in rows}) != expected):
                    cell_errors.append("incomplete or mixed join workflow")
                for row in rows:
                    if (row["success"].lower() != "true" or row["new_session_established"].lower() != "true"
                            or int(row["benchmark_active_size"]) != target or int(row["workflow_pair_count"]) != expected
                            or not all(positive(row[k]) for k in ("cpu_process_ns", "alloc_bytes", "wall_ns"))
                            or (device, row["request_id"]) not in one_time):
                        cell_errors.append(f"invalid fresh-session metrics or one-time bundle: {row['request_id']}")
                calls = maintenance[(device, target)]
                if len(calls) != expected or {r["peer"] for r in calls} != {r["peer_id"] for r in rows}:
                    cell_errors.append("missing per-peer UpdateOneTimePrekeys evidence")
            errors.extend(f"{device}/N={target}: {error}" for error in cell_errors)
            coverage.append({
                "participant_id": device, "operation": "establish_session_external_join", "target": target,
                "eligible_rows": len(rows), "expected_rows": expected, "attrited": attrited,
                "cpu_ms": sum(int(r["cpu_process_ns"]) for r in rows) / 1e6,
                "alloc_mib": sum(int(r["alloc_bytes"]) for r in rows) / (1 << 20),
                "wall_ms": sum(int(r["wall_ns"]) for r in rows) / 1e6,
                "peak_rss_kib": peak, "errors": cell_errors,
            })
    return {"status": "fail" if errors else "pass", "expected_plateau_targets": targets,
            "external_session_join": True, "context_window_errors": timing_errors,
            "luckfox_failure_target": boundary, "luckfox_join_max_size": cap,
            "coverage": coverage, "errors": errors}, errors


def validate(args: argparse.Namespace) -> tuple[dict[str, Any], list[str]]:
    if args.external_session_join:
        return validate_external_join(args)
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
                continue
            payload: int | None = None
            if operation in APPLICATION_TOTALS:
                payload_raw = row.get("benchmark_payload_size") or ""
                if not payload_raw:
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
    parser.add_argument("--external-session-join", action="store_true")
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
        if args.external_session_join:
            print(f"  {cell['participant_id']} N={cell['target']}: rows={cell['eligible_rows']}/{cell['expected_rows']} "
                  f"CPU={cell['cpu_ms']:.3f} ms alloc={cell['alloc_mib']:.3f} MiB peak_RSS={cell['peak_rss_kib']} KiB "
                  f"attrited={cell['attrited']}")
            continue
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
