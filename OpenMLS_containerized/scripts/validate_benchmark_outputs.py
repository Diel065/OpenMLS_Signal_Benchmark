#!/usr/bin/env python3
"""Validate OpenMLS publication outputs, with strict AddCommit contracts."""

import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set


MIN_PROFILE_SCHEMA_VERSION = 10
CANONICAL_TOTAL = "add_commit_total_local"
ADD_FAMILY = "add_commit_create"
ADD_OPERATION = "add_commit"
REQUIRED_ADD_SPANS = {
    "commit_create_protocol_add",
    "commit_add.path_hpke_encrypt",
    "commit_add.path_secret_derive",
    "commit_add.group_info.serialize_plaintext",
    "commit_add.group_info.aead_encrypt",
    "commit_add.welcome_group_secrets_encrypt",
    "commit_add.welcome.new",
}
GROUP_INFO_SPANS = {
    "commit_add.group_info.serialize_plaintext",
    "commit_add.group_info.aead_encrypt",
    "commit_add.welcome_group_secrets_encrypt",
    "commit_add.welcome.new",
    "welcome_create_protocol",
}
PROCESS_L1D_SPANS = {CANONICAL_TOTAL, "commit_add.path_hpke_encrypt"}
PROCESS_ALLOCATION_SPANS = {CANONICAL_TOTAL, "commit_add.path_hpke_encrypt"}
NONZERO_ALLOCATION_SPANS = {
    CANONICAL_TOTAL,
    "commit_add.path_hpke_encrypt",
    "commit_add.path_secret_derive",
    "commit_add.group_info.serialize_plaintext",
    "commit_add.group_info.aead_encrypt",
    "commit_add.welcome_group_secrets_encrypt",
}
VALID_BATCH_SOURCES = {
    "balanced_seeded_regular",
    "balanced_seeded_external",
    "external_density_k1",
    "external_density_k8",
    "remove_rejoin",
}

REQUIRED_COLUMNS = {
    "profile_schema_version",
    "run_id",
    "op",
    "client_id",
    "worker_id",
    "device_kind",
    "global_span_id",
    "parent_global_span_id",
    "wall_ns",
    "cpu_thread_ns",
    "cpu_process_ns",
    "alloc_bytes",
    "alloc_count",
    "alloc_measurement_scope",
    "l1d_cache_accesses",
    "l1d_cache_misses",
    "l1d_measurement_scope",
    "l1d_cache_status",
    "l1d_measured_thread_count",
    "l1d_discovered_thread_count",
    "l1d_multiplexed_thread_count",
    "operation_family",
    "benchmark_operation",
    "member_count",
    "member_count_before",
    "member_count_after",
    "added_members_count",
    "membership_batch_requested",
    "membership_batch_effective",
    "membership_batch_group_cap",
    "membership_batch_transition_cap",
    "membership_batch_source",
    "group_info_plaintext_bytes",
    "group_info_ciphertext_bytes",
    "encrypted_group_info_bytes",
    "ratchet_tree_included",
    "ratchet_tree_bytes",
    "ratchet_tree_delivery_mode",
    "welcome_recipient_count",
    "filtered_direct_path_len",
    "sum_copath_resolution_sizes",
    "hpke_encrypt_count",
}


def present(value: Optional[str]) -> bool:
    return value is not None and value.strip() != ""


def integer(row: Dict[str, str], field_name: str) -> Optional[int]:
    value = row.get(field_name)
    if not present(value):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
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
    jsonl_row_count: int = 0
    run_id: Optional[str] = None
    schema_version: Optional[int] = None
    add_total_count: int = 0
    add_k_counts: Dict[int, int] = field(default_factory=dict)
    external_add_counts: Dict[str, int] = field(default_factory=dict)

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
            "jsonl_row_count": self.jsonl_row_count,
            "run_id": self.run_id,
            "schema_version": self.schema_version,
            "add_total_count": self.add_total_count,
            "add_k_counts": self.add_k_counts,
            "external_add_counts": self.external_add_counts,
        }


def load_layout(run_path: str) -> Dict[str, Any]:
    path = os.path.join(run_path, "worker_layout.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def expected_external_clients(layout: Dict[str, Any]) -> Dict[str, str]:
    expected: Dict[str, str] = {}
    for client in layout.get("clients", []):
        if client.get("profile_enabled") and str(client.get("device_kind", "")).strip():
            expected[str(client["client_id"])] = str(client["device_kind"])
    return expected


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
    allow_missing_jsonl: bool = False,
    required_k_values: Sequence[int] = (),
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
    run_ids = {row["run_id"] for row in profile_rows if present(row.get("run_id"))}
    if len(run_ids) != 1:
        result.add_error(f"expected one nonblank run_id, found {sorted(run_ids)}")
    else:
        result.run_id = next(iter(run_ids))

    schema_versions: Set[int] = set()
    global_ids: Set[str] = set()
    duplicate_global_ids: Set[str] = set()
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
        validate_numeric_nonnegative(result, row, row_number, "cpu_process_ns")
        validate_numeric_nonnegative(result, row, row_number, "alloc_bytes")
        validate_numeric_nonnegative(result, row, row_number, "alloc_count")

        global_id = row.get("global_span_id", "")
        if not global_id:
            result.add_error(f"row {row_number} ({row.get('op')}): missing global_span_id")
        elif global_id in global_ids:
            duplicate_global_ids.add(global_id)
        else:
            global_ids.add(global_id)

        l1_status = row.get("l1d_cache_status", "")
        if not l1_status:
            result.add_error(f"row {row_number} ({row.get('op')}): missing l1d_cache_status")
        elif l1_status == "disabled":
            pass
        elif l1_status.startswith("available_"):
            accesses = validate_numeric_nonnegative(
                result, row, row_number, "l1d_cache_accesses"
            )
            misses = validate_numeric_nonnegative(result, row, row_number, "l1d_cache_misses")
            if accesses is not None and misses is not None and misses > accesses:
                result.add_error(
                    f"row {row_number} ({row.get('op')}): L1D misses exceed accesses"
                )
            if not present(row.get("l1d_measurement_scope")):
                result.add_error(f"row {row_number} ({row.get('op')}): missing L1D scope")
        elif any(kind in row.get("device_kind", "").lower() for kind in ["luckfox", "local_process"]):
            # Luckfox and local processes are exempt from L1D requirements if missing
            pass

        if row.get("op") in PROCESS_L1D_SPANS and l1_status != "disabled":
            if any(kind in row.get("device_kind", "").lower() for kind in ["luckfox", "local_process"]):
                pass
            else:
                if row.get("l1d_measurement_scope") != "process_threads_at_span_start":
                    result.add_error(
                        f"row {row_number} ({row.get('op')}): process-wide L1D scope required"
                    )
                if not l1_status.startswith("available_all_process_threads"):
                    result.add_error(
                        f"row {row_number} ({row.get('op')}): complete process-thread L1D "
                        f"coverage required, got {l1_status!r}"
                    )
        if row.get("op") in PROCESS_ALLOCATION_SPANS and row.get(
            "alloc_measurement_scope"
        ) != "process_all_threads":
            result.add_error(
                f"row {row_number} ({row.get('op')}): process-wide allocation scope required"
            )
        if row.get("op") in NONZERO_ALLOCATION_SPANS:
            alloc_bytes = integer(row, "alloc_bytes")
            alloc_count = integer(row, "alloc_count")
            if alloc_bytes is None or alloc_count is None or alloc_bytes <= 0 or alloc_count <= 0:
                result.add_error(
                    f"row {row_number} ({row.get('op')}): expected nonzero allocation metrics, "
                    f"got bytes={alloc_bytes}, count={alloc_count}"
                )

    if duplicate_global_ids:
        result.add_error(f"duplicate global_span_id values: {sorted(duplicate_global_ids)[:5]}")
    if len(schema_versions) != 1:
        result.add_error(f"expected one profile schema version, found {sorted(schema_versions)}")
    elif schema_versions:
        result.schema_version = next(iter(schema_versions))

    add_rows = [
        row
        for row in profile_rows
        if row.get("operation_family") == ADD_FAMILY or row.get("op") == CANONICAL_TOTAL
    ]
    if not add_rows:
        result.add_error("no AddCommit creation rows found")
        return result

    for row_number, row in enumerate(add_rows, start=1):
        if row.get("operation_family") != ADD_FAMILY:
            result.add_error(f"AddCommit row {row_number} ({row.get('op')}): wrong operation_family")
        if row.get("benchmark_operation") != ADD_OPERATION:
            result.add_error(f"AddCommit row {row_number} ({row.get('op')}): wrong benchmark_operation")
        before = integer(row, "member_count_before")
        member_count = integer(row, "member_count")
        after = integer(row, "member_count_after")
        added = integer(row, "added_members_count")
        if None in (before, member_count, after, added):
            result.add_error(f"AddCommit row {row_number} ({row.get('op')}): incomplete N/k metadata")
        elif member_count != before or after != before + added:  # type: ignore[operator]
            result.add_error(
                f"AddCommit row {row_number} ({row.get('op')}): invalid N/k invariant "
                f"member_count={member_count}, before={before}, after={after}, k={added}"
            )

    totals = [row for row in add_rows if row.get("op") == CANONICAL_TOTAL]
    commit_parents = [row for row in add_rows if row.get("op") == "commit_create_protocol_add"]
    result.add_total_count = len(totals)
    if not totals:
        result.add_error(f"missing canonical {CANONICAL_TOTAL} rows")
    if len(totals) != len(commit_parents):
        result.add_error(
            f"canonical total count {len(totals)} does not match AddCommit parent count "
            f"{len(commit_parents)}"
        )

    total_ids = {row.get("global_span_id") for row in totals}
    children_by_total = Counter(row.get("parent_global_span_id") for row in commit_parents)
    for total_id in total_ids:
        if children_by_total[total_id] != 1:
            result.add_error(
                f"canonical total {total_id!r} has {children_by_total[total_id]} direct "
                "commit_create_protocol_add children; expected exactly one"
            )

    for span in sorted(REQUIRED_ADD_SPANS):
        count = sum(row.get("op") == span for row in add_rows)
        if count != len(totals):
            result.add_error(
                f"span {span!r} has {count} rows for {len(totals)} AddCommits"
            )

    plotted_l1d_ops = REQUIRED_ADD_SPANS | {CANONICAL_TOTAL}
    for row_number, row in enumerate(
        [row for row in add_rows if row.get("op") in plotted_l1d_ops], start=1
    ):
        l1_status = row.get("l1d_cache_status", "")
        if l1_status == "disabled":
            continue
        if not l1_status.startswith("available_"):
            if any(kind in row.get("device_kind", "").lower() for kind in ["luckfox", "local_process"]):
                result.add_warning(
                    f"AddCommit L1D row {row_number} ({row.get('op')}): L1D missing for {row.get('device_kind')}, got {row.get('l1d_cache_status')!r}"
                )
            else:
                result.add_error(
                    f"AddCommit L1D row {row_number} ({row.get('op')}): complete measurement "
                    f"required, got {row.get('l1d_cache_status')!r}"
                )

    for critical_op in (CANONICAL_TOTAL, "commit_add.path_hpke_encrypt"):
        critical_rows = [row for row in add_rows if row.get("op") == critical_op]
        if len(critical_rows) >= 3 and all(
            integer(row, "wall_ns")
            == integer(row, "cpu_thread_ns")
            == integer(row, "cpu_process_ns")
            for row in critical_rows
        ):
            result.add_error(
                f"{critical_op} has identical wall/thread/process CPU values in every row; "
                "this indicates metric aliasing"
            )

    for row_number, row in enumerate(totals, start=1):
        k = integer(row, "added_members_count")
        if k is not None:
            result.add_k_counts[k] = result.add_k_counts.get(k, 0) + 1
        if present(row.get("benchmark_plateau_index")):
            requested = integer(row, "membership_batch_requested")
            effective = integer(row, "membership_batch_effective")
            group_cap = integer(row, "membership_batch_group_cap")
            transition_cap = integer(row, "membership_batch_transition_cap")
            source = row.get("membership_batch_source")
            if None in (requested, effective, group_cap, transition_cap) or not source:
                result.add_error(f"total row {row_number}: incomplete membership batch metadata")
            else:
                if effective != k:
                    result.add_error(
                        f"total row {row_number}: effective batch {effective} != AddCommit k {k}"
                    )
                if not (1 <= requested <= group_cap and 1 <= effective <= transition_cap):
                    result.add_error(f"total row {row_number}: invalid membership batch caps")
                if source not in VALID_BATCH_SOURCES:
                    result.add_error(f"total row {row_number}: invalid batch source {source!r}")

    for required_k in required_k_values:
        if result.add_k_counts.get(required_k, 0) == 0:
            result.add_error(f"required AddCommit k={required_k} has no observations")
    batching_was_feasible = any(
        (integer(row, "membership_batch_transition_cap") or 0) > 1 for row in totals
    )
    if batching_was_feasible and not any(k > 1 for k in result.add_k_counts):
        result.add_error(
            "run had AddCommit transitions permitting k > 1 but contains no such observation"
        )

    for row_number, row in enumerate(
        [row for row in add_rows if row.get("op") in GROUP_INFO_SPANS], start=1
    ):
        plaintext = validate_numeric_nonnegative(
            result, row, row_number, "group_info_plaintext_bytes"
        )
        tree_bytes = validate_numeric_nonnegative(result, row, row_number, "ratchet_tree_bytes")
        if plaintext == 0 or tree_bytes == 0:
            result.add_error(f"GroupInfo row {row_number} ({row.get('op')}): zero artifact size")
        if truthy(row.get("ratchet_tree_included")) is not True:
            result.add_error(f"GroupInfo row {row_number} ({row.get('op')}): tree not included")
        if row.get("ratchet_tree_delivery_mode") != "welcome_extension":
            result.add_error(
                f"GroupInfo row {row_number} ({row.get('op')}): invalid tree delivery mode"
            )
        if row.get("op") == "commit_add.group_info.aead_encrypt":
            ciphertext = validate_numeric_nonnegative(
                result, row, row_number, "group_info_ciphertext_bytes"
            )
            encrypted = validate_numeric_nonnegative(
                result, row, row_number, "encrypted_group_info_bytes"
            )
            if ciphertext != encrypted or (
                ciphertext is not None and plaintext is not None and ciphertext <= plaintext
            ):
                result.add_error(
                    f"GroupInfo AEAD row {row_number}: invalid plaintext/ciphertext relationship"
                )

    for row_number, row in enumerate(
        [row for row in add_rows if row.get("op") == "commit_add.welcome_group_secrets_encrypt"],
        start=1,
    ):
        k = integer(row, "added_members_count")
        recipients = integer(row, "welcome_recipient_count")
        hpke_count = integer(row, "hpke_encrypt_count")
        if k != recipients or k != hpke_count:
            result.add_error(
                f"Welcome HPKE row {row_number}: k={k}, recipients={recipients}, hpke={hpke_count}"
            )

    for row_number, row in enumerate(
        [row for row in add_rows if row.get("op") == "commit_add.path_hpke_encrypt"], start=1
    ):
        c_value = integer(row, "sum_copath_resolution_sizes")
        hpke_count = integer(row, "hpke_encrypt_count")
        if c_value is None or c_value < 0 or hpke_count != c_value:
            result.add_error(
                f"UpdatePath HPKE row {row_number}: invalid C/hpke metadata C={c_value}, hpke={hpke_count}"
            )

    for row_number, row in enumerate(
        [row for row in add_rows if row.get("op") == "commit_add.path_secret_derive"], start=1
    ):
        f_value = integer(row, "filtered_direct_path_len")
        if f_value is None or f_value <= 0:
            result.add_error(f"path derivation row {row_number}: invalid filtered path length")

    layout = load_layout(run_path)
    external_clients = expected_external_clients(layout)
    spans_by_worker: Dict[str, Set[str]] = defaultdict(set)
    for row in add_rows:
        spans_by_worker[row.get("client_id", "")].add(row.get("op", ""))
    for client_id, device_kind in external_clients.items():
        worker_totals = [
            row for row in totals if row.get("client_id") == client_id
        ]
        result.external_add_counts[f"{device_kind}:{client_id}"] = len(worker_totals)
        if not worker_totals:
            result.add_error(
                f"external device {device_kind}:{client_id} has no canonical AddCommit observations"
            )
            continue
        missing_spans = REQUIRED_ADD_SPANS - spans_by_worker[client_id]
        if missing_spans:
            result.add_error(
                f"external device {device_kind}:{client_id} missing AddCommit spans: "
                f"{sorted(missing_spans)}"
            )

    profile_jsonl = sorted(
        filename
        for filename in os.listdir(run_path)
        if filename.startswith("client-") and filename.endswith(".jsonl")
    )
    if not profile_jsonl and not allow_missing_jsonl:
        result.add_error("no client-*.jsonl files found")
    for filename in profile_jsonl:
        path = os.path.join(run_path, filename)
        with open(path, "r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                result.jsonl_row_count += 1
                try:
                    json.loads(line)
                except json.JSONDecodeError as error:
                    result.add_error(f"malformed JSONL {filename}:{line_number}: {error}")
    if profile_jsonl and result.csv_row_count != result.jsonl_row_count:
        result.add_error(
            f"row count mismatch: events.csv={result.csv_row_count}, client JSONL={result.jsonl_row_count}"
        )

    return result


def discover_runs(path: str) -> List[str]:
    if os.path.exists(os.path.join(path, "events.csv")):
        return [path]
    runs: List[str] = []
    for root, _dirs, files in os.walk(path):
        if "events.csv" in files:
            runs.append(root)
    return sorted(runs)


def parse_required_k(value: str) -> List[int]:
    if not value.strip():
        return []
    parsed = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if any(item < 1 for item in parsed):
        raise argparse.ArgumentTypeError("k values must be positive integers")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="benchmark run directory or benchmark_output root")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--allow-missing-jsonl", action="store_true")
    parser.add_argument(
        "--require-k-values",
        default="",
        help="comma-separated AddCommit k values that must occur in every run",
    )
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print(f"error: path does not exist: {args.path}", file=sys.stderr)
        return 1
    try:
        required_k_values = parse_required_k(args.require_k_values)
    except (ValueError, argparse.ArgumentTypeError) as error:
        print(f"error: invalid --require-k-values: {error}", file=sys.stderr)
        return 2

    run_dirs = discover_runs(args.path)
    if not run_dirs:
        print(f"error: no directories with events.csv below {args.path}", file=sys.stderr)
        return 1

    results = [
        validate_run(run, args.allow_missing_jsonl, required_k_values) for run in run_dirs
    ]
    if args.json:
        print(json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True))
    else:
        for result in results:
            status = "PASS" if result.success else "FAIL"
            print(f"RUN: {result.run_path} [{status}]")
            print(
                f"  schema={result.schema_version} csv_rows={result.csv_row_count} "
                f"jsonl_rows={result.jsonl_row_count} add_totals={result.add_total_count}"
            )
            print(f"  AddCommit k counts: {dict(sorted(result.add_k_counts.items()))}")
            if result.external_add_counts:
                print(f"  External AddCommit counts: {result.external_add_counts}")
            for error in result.errors:
                print(f"  ERROR: {error}")
            for warning in result.warnings:
                print(f"  WARNING: {warning}")
    return 0 if all(result.success for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
