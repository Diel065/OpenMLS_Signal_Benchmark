import csv
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from validate_benchmark_outputs import (  # noqa: E402
    CANONICAL_TOTAL,
    REQUIRED_ADD_SPANS,
    REQUIRED_COLUMNS,
    validate_run,
)


def valid_add_rows(run_id="test-run", k=2):
    ops = [CANONICAL_TOTAL, *sorted(REQUIRED_ADD_SPANS)]
    rows = []
    total_id = "client-1:1"
    for index, op in enumerate(ops, start=1):
        row = {column: "" for column in REQUIRED_COLUMNS}
        row.update(
            {
                "profile_schema_version": "10",
                "run_id": run_id,
                "op": op,
                "client_id": "client-1",
                "worker_id": "client-1",
                "device_kind": "",
                "global_span_id": f"client-1:{index}",
                "parent_global_span_id": "",
                "wall_ns": "1000",
                "cpu_thread_ns": "900",
                "cpu_process_ns": "950",
                "alloc_bytes": "128",
                "alloc_count": "2",
                "alloc_measurement_scope": "current_thread",
                "l1d_measurement_scope": "current_thread",
                "l1d_cache_status": "available_current_thread",
                "l1d_cache_accesses": "100",
                "l1d_cache_misses": "10",
                "l1d_multiplexed_thread_count": "0",
                "operation_family": "add_commit_create",
                "benchmark_operation": "add_commit",
                "member_count": "8",
                "member_count_before": "8",
                "member_count_after": str(8 + k),
                "added_members_count": str(k),
            }
        )
        if op == CANONICAL_TOTAL:
            row.update(
                {
                    "alloc_measurement_scope": "process_all_threads",
                    "l1d_measurement_scope": "process_threads_at_span_start",
                    "l1d_cache_status": "available_all_process_threads",
                    "l1d_cache_accesses": "1000",
                    "l1d_cache_misses": "100",
                    "l1d_measured_thread_count": "4",
                    "l1d_discovered_thread_count": "4",
                    "benchmark_plateau_index": "1",
                    "benchmark_target_size": str(8 + k),
                    "membership_batch_requested": str(k),
                    "membership_batch_effective": str(k),
                    "membership_batch_group_cap": "8",
                    "membership_batch_transition_cap": "8",
                    "membership_batch_source": "balanced_seeded_regular",
                }
            )
        if op == "commit_create_protocol_add":
            row["parent_global_span_id"] = total_id
        if op == "commit_add.path_hpke_encrypt":
            row.update(
                {
                    "alloc_measurement_scope": "process_all_threads",
                    "l1d_measurement_scope": "process_threads_at_span_start",
                    "l1d_cache_status": "available_all_process_threads",
                    "l1d_cache_accesses": "500",
                    "l1d_cache_misses": "50",
                    "l1d_measured_thread_count": "4",
                    "l1d_discovered_thread_count": "4",
                    "sum_copath_resolution_sizes": "3",
                    "hpke_encrypt_count": "3",
                }
            )
        if op == "commit_add.path_secret_derive":
            row["filtered_direct_path_len"] = "4"
        if op in {
            "commit_add.group_info.serialize_plaintext",
            "commit_add.group_info.aead_encrypt",
            "commit_add.welcome_group_secrets_encrypt",
            "commit_add.welcome.new",
        }:
            row.update(
                {
                    "group_info_plaintext_bytes": "4096",
                    "ratchet_tree_included": "true",
                    "ratchet_tree_bytes": "3500",
                    "ratchet_tree_delivery_mode": "welcome_extension",
                }
            )
        if op == "commit_add.group_info.aead_encrypt":
            row["group_info_ciphertext_bytes"] = "4112"
            row["encrypted_group_info_bytes"] = "4112"
        if op == "commit_add.welcome_group_secrets_encrypt":
            row["welcome_recipient_count"] = str(k)
            row["hpke_encrypt_count"] = str(k)
        rows.append(row)
    return rows


def write_run(root, rows, *, layout=None, malformed_jsonl=False):
    run_dir = Path(root) / "run"
    run_dir.mkdir(parents=True)
    fieldnames = sorted(REQUIRED_COLUMNS | {key for row in rows for key in row})
    with (run_dir / "events.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    with (run_dir / "client-client-1.jsonl").open("w", encoding="utf-8") as handle:
        for index, row in enumerate(rows):
            if malformed_jsonl and index == len(rows) - 1:
                handle.write("{not-json}\n")
            else:
                handle.write(json.dumps(row) + "\n")
    if layout is not None:
        (run_dir / "worker_layout.json").write_text(
            json.dumps(layout), encoding="utf-8"
        )
    return run_dir


class TestValidator(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="openmls-validator-")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_valid_addcommit_contract(self):
        result = validate_run(str(write_run(self.temp_dir, valid_add_rows())), required_k_values=[2])
        self.assertTrue(result.success, result.errors)
        self.assertEqual(result.add_total_count, 1)
        self.assertEqual(result.add_k_counts, {2: 1})

    def test_rejects_stale_schema(self):
        rows = valid_add_rows()
        rows[0]["profile_schema_version"] = "9"
        result = validate_run(str(write_run(self.temp_dir, rows)))
        self.assertFalse(result.success)
        self.assertTrue(any("stale schema" in error for error in result.errors))

    def test_rejects_invalid_member_count_semantics(self):
        rows = valid_add_rows()
        rows[2]["member_count"] = "10"
        result = validate_run(str(write_run(self.temp_dir, rows)))
        self.assertFalse(result.success)
        self.assertTrue(any("invalid N/k invariant" in error for error in result.errors))

    def test_rejects_missing_required_k(self):
        result = validate_run(
            str(write_run(self.temp_dir, valid_add_rows(k=1))), required_k_values=[1, 2]
        )
        self.assertFalse(result.success)
        self.assertTrue(any("k=2" in error for error in result.errors))

    def test_accepts_only_k_one_when_every_transition_cap_is_one(self):
        rows = valid_add_rows(k=1)
        total = next(row for row in rows if row["op"] == CANONICAL_TOTAL)
        total["membership_batch_transition_cap"] = "1"
        result = validate_run(str(write_run(self.temp_dir, rows)))
        self.assertTrue(result.success, result.errors)

    def test_rejects_only_k_one_when_larger_batches_were_feasible(self):
        result = validate_run(str(write_run(self.temp_dir, valid_add_rows(k=1))))
        self.assertFalse(result.success)
        self.assertTrue(
            any("transitions permitting k > 1" in error for error in result.errors)
        )

    def test_rejects_partial_process_l1d_coverage(self):
        rows = valid_add_rows()
        total = next(row for row in rows if row["op"] == CANONICAL_TOTAL)
        total.update(
            {
                "l1d_cache_status": "available_partial_process_threads",
                "l1d_cache_accesses": "100",
                "l1d_cache_misses": "10",
                "l1d_measured_thread_count": "2",
                "l1d_discovered_thread_count": "3",
            }
        )
        result = validate_run(str(write_run(self.temp_dir, rows)))
        self.assertFalse(result.success)
        self.assertTrue(any("complete process-thread" in error for error in result.errors))

    def test_rejects_missing_external_device_addcommit(self):
        layout = {
            "clients": [
                {
                    "client_id": "luckfox-1",
                    "profile_enabled": True,
                    "device_kind": "luckfox-pico-plus",
                }
            ]
        }
        result = validate_run(str(write_run(self.temp_dir, valid_add_rows(), layout=layout)))
        self.assertFalse(result.success)
        self.assertTrue(any("has no canonical AddCommit" in error for error in result.errors))

    def test_rejects_malformed_jsonl(self):
        result = validate_run(
            str(write_run(self.temp_dir, valid_add_rows(), malformed_jsonl=True))
        )
        self.assertFalse(result.success)
        self.assertTrue(any("malformed JSONL" in error for error in result.errors))

    def test_accepts_luckfox_missing_l1d(self):
        rows = valid_add_rows()
        for row in rows:
            row.update({
                "device_kind": "luckfox_pico_plus",
                "l1d_cache_status": "unsupported_or_permission_denied",
                "l1d_cache_accesses": "",
                "l1d_cache_misses": "",
                "l1d_measurement_scope": "",
                "l1d_measured_thread_count": "",
                "l1d_discovered_thread_count": "",
            })
        result = validate_run(str(write_run(self.temp_dir, rows)))
        self.assertTrue(result.success, result.errors)

    def test_accepts_explicitly_disabled_l1d(self):
        rows = valid_add_rows()
        for row in rows:
            row.update({
                "l1d_cache_status": "disabled",
                "l1d_cache_accesses": "",
                "l1d_cache_misses": "",
                "l1d_measurement_scope": "",
                "l1d_measured_thread_count": "",
                "l1d_discovered_thread_count": "",
                "l1d_multiplexed_thread_count": "",
            })
        result = validate_run(str(write_run(self.temp_dir, rows)))
        self.assertTrue(result.success, result.errors)


if __name__ == "__main__":
    unittest.main()
