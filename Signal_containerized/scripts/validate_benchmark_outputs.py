#!/usr/bin/env python3
"""Validate Signal benchmark publication outputs."""

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set


MIN_PROFILE_SCHEMA_VERSION = 3

SIGNAL_CORE_OPERATIONS = {
    "signal_session_establish",
    "signal_application_message_create",
    "signal_application_message_receive",
    "signal_prekey_bundle_create",
    "signal_prekey_maintenance",
}

SIGNAL_PROTOCOL_SPANS = {
    "signal_session_establish.total",
    "signal_session_establish.process_prekey_bundle",
    "signal_session_establish.prekey_bundle_fetch_repository_io",
    "signal_application_message_create.total",
    "signal_application_message_create.ratchet_encrypt_payload",
    "signal_application_message_create.relay_publish_message_io",
    "signal_application_message_receive.total",
    "signal_application_message_receive.message_decrypt",
    "signal_application_message_receive.relay_fetch_pending_message_io",
    "signal_prekey_bundle_create.total",
    "signal_prekey_bundle_create.signed_prekey_generate",
    "signal_prekey_bundle_create.bundle_assemble",
    "signal_prekey_maintenance.total",
    "signal_prekey_maintenance.refill_publish_repository_io",
}

WRAPPER_SPANS = {
    "benchmark_wrapper",
    "benchmark_wrapper.total",
    "signal_process_pending",
}

REQUIRED_COLUMNS = {
    "run_id",
    "op",
    "span_name",
    "span_layer",
    "measurement_class",
    "protocol_stack",
    "implementation",
    "wall_ns",
    "cpu_thread_ns",
    "alloc_bytes",
    "alloc_count",
    "app_heap_budget",
    "app_heap_budget_bytes",
    "app_heap_current_live_bytes",
    "app_heap_peak_live_bytes",
    "success",
    "participant_id",
    "peer_id",
}


def present(value: Optional[str]) -> bool:
    return value is not None and value.strip() != ""


def integer(row: Dict[str, str], field_name: str) -> Optional[int]:
    value = row.get(field_name)
    if not present(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def truthy(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


@dataclass
class ValidationResult:
    run_path: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    csv_row_count: int = 0
    run_id: Optional[str] = None
    schema_version: Optional[int] = None
    protocol_core_row_count: int = 0
    wrapper_row_count: int = 0
    observed_spans: Set[str] = field(default_factory=set)
    observed_operations: Set[str] = field(default_factory=set)

    @property
    def success(self) -> bool:
        return not self.errors

    def add_error(self, message: str) -> None:
        self.errors.append(message)

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_path": self.run_path,
            "success": self.success,
            "errors": self.errors,
            "warnings": self.warnings,
            "csv_row_count": self.csv_row_count,
            "run_id": self.run_id,
            "schema_version": self.schema_version,
            "protocol_core_row_count": self.protocol_core_row_count,
            "wrapper_row_count": self.wrapper_row_count,
            "observed_spans": sorted(self.observed_spans),
            "observed_operations": sorted(self.observed_operations),
        }


def validate_numeric_nonnegative(
    result: ValidationResult,
    row: Dict[str, str],
    row_number: int,
    field_name: str,
    required: bool = True,
) -> Optional[int]:
    value = row.get(field_name)
    if not present(value):
        if required:
            result.add_error(f"row {row_number} ({row.get('op')}): missing {field_name}")
        return None
    parsed = integer(row, field_name)
    if parsed is None:
        result.add_error(f"row {row_number} ({row.get('op')}): invalid integer {field_name}={value!r}")
    elif parsed < 0:
        result.add_error(f"row {row_number} ({row.get('op')}): negative {field_name}={parsed}")
    return parsed


def validate_run(
    run_path: str,
) -> ValidationResult:
    result = ValidationResult(run_path)
    events_path = os.path.join(run_path, "events.csv")
    if not os.path.exists(events_path):
        result.add_error("missing events.csv")
        return result

    with open(events_path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        headers = set(reader.fieldnames or [])
        missing_columns = sorted(REQUIRED_COLUMNS - headers)
        if missing_columns:
            result.add_error(f"missing required CSV columns: {missing_columns}")
            return result
        rows = list(reader)
    result.csv_row_count = len(rows)
    if not rows:
        result.add_error("events.csv contains no rows")
        return result

    profile_rows = [row for row in rows if present(row.get("op"))]
    if not profile_rows:
        result.add_error("events.csv contains only header or empty rows with no 'op' field")
        return result

    run_ids = {row["run_id"] for row in profile_rows if present(row.get("run_id"))}
    if len(run_ids) != 1:
        result.add_error(f"expected one nonblank run_id, found {sorted(run_ids)}")
    else:
        result.run_id = next(iter(run_ids))

    schema_versions: Set[int] = set()
    for row_number, row in enumerate(profile_rows, start=2):
        schema = integer(row, "profile_schema_version")
        if schema is None:
            result.add_error(f"row {row_number} ({row.get('op')}): invalid profile_schema_version")
        else:
            schema_versions.add(schema)
            if schema < MIN_PROFILE_SCHEMA_VERSION:
                result.add_error(
                    f"row {row_number} ({row.get('op')}): stale schema {schema}; "
                    f"requires >= {MIN_PROFILE_SCHEMA_VERSION}"
                )

        validate_numeric_nonnegative(result, row, row_number, "wall_ns")
        validate_numeric_nonnegative(result, row, row_number, "cpu_thread_ns", required=False)
        validate_numeric_nonnegative(result, row, row_number, "alloc_bytes", required=False)
        validate_numeric_nonnegative(result, row, row_number, "alloc_count", required=False)

        span_name = row.get("span_name", "")
        if span_name:
            result.observed_spans.add(span_name)
        op = row.get("op", "")
        if op:
            result.observed_operations.add(op)

        measurement_class = row.get("measurement_class", "")
        span_layer = row.get("span_layer", "")
        if measurement_class == "protocol" or span_layer == "protocol_core":
            result.protocol_core_row_count += 1
        if span_layer == "benchmark_wrapper" or op in WRAPPER_SPANS:
            result.wrapper_row_count += 1

        success_val = row.get("success", "")
        if not present(success_val):
            result.add_warning(f"row {row_number} ({row.get('op')}): missing success field")
        elif truthy(success_val) is False:
            result.add_warning(
                f"row {row_number} ({row.get('op')}): operation failed (success=false)"
            )

        if not present(row.get("protocol_stack")):
            result.add_error(f"row {row_number} ({row.get('op')}): missing protocol_stack")
        if not present(row.get("implementation")):
            result.add_error(f"row {row_number} ({row.get('op')}): missing implementation")

    if len(schema_versions) != 1:
        result.add_error(f"expected one profile schema version, found {sorted(schema_versions)}")
    elif schema_versions:
        result.schema_version = next(iter(schema_versions))

    if result.protocol_core_row_count == 0:
        result.add_error(
            "no protocol_core rows found: events.csv contains only wrapper rows. "
            "Signal protocol subspans are missing."
        )

    if result.wrapper_row_count == result.csv_row_count and result.csv_row_count > 0:
        result.add_error("all rows are benchmark_wrapper only; no protocol subspans present")

    observed_op_families = {
        row.get("op", "").split(".")[0]
        for row in profile_rows
        if present(row.get("op"))
    }

    required_core_ops = {
        "signal_session_establish",
        "signal_application_message_create",
        "signal_application_message_receive",
    }
    missing_core = required_core_ops - observed_op_families
    if missing_core:
        result.add_error(
            f"missing required Signal core operation families: {sorted(missing_core)}"
        )

    prekey_ops = {"signal_prekey_bundle_create", "signal_prekey_maintenance"}
    missing_prekey = prekey_ops - observed_op_families
    if missing_prekey:
        result.add_warning(
            f"missing Signal prekey operation families: {sorted(missing_prekey)} "
            f"(may be acceptable if prekeys are pre-provisioned)"
        )

    seen_spans = set()
    for row in profile_rows:
        span_name = row.get("span_name", "")
        op = row.get("op", "")
        if not span_name and op:
            result.add_warning(f"row with op={op} has no span_name")
        seen_spans.add(span_name or op)

    if seen_spans & WRAPPER_SPANS and not (seen_spans - WRAPPER_SPANS):
        result.add_error("only wrapper spans found; no Signal protocol subspans present")

    return result


def discover_runs(path: str) -> List[str]:
    if os.path.exists(os.path.join(path, "events.csv")):
        return [path]
    runs: List[str] = []
    for root, _dirs, files in os.walk(path):
        if "events.csv" in files:
            runs.append(root)
    return sorted(runs)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="benchmark run directory or benchmark_output root")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print(f"error: path does not exist: {args.path}", file=sys.stderr)
        return 1

    run_dirs = discover_runs(args.path)
    if not run_dirs:
        print(f"error: no directories with events.csv below {args.path}", file=sys.stderr)
        return 1

    results = [validate_run(run) for run in run_dirs]
    if args.json:
        print(json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True))
    else:
        for result in results:
            status = "PASS" if result.success else "FAIL"
            print(f"RUN: {result.run_path} [{status}]")
            print(
                f"  schema={result.schema_version} csv_rows={result.csv_row_count} "
                f"protocol_core_rows={result.protocol_core_row_count} "
                f"wrapper_rows={result.wrapper_row_count}"
            )
            print(f"  observed_spans: {sorted(result.observed_spans)}")
            for error in result.errors:
                print(f"  ERROR: {error}")
            for warning in result.warnings:
                print(f"  WARNING: {warning}")
    return 0 if all(result.success for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
