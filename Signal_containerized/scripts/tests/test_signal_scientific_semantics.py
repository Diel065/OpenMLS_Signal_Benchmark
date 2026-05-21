#!/usr/bin/env python3
"""
Static semantic checks for Signal scientific profiling.

These tests are intentionally source-level. They guard the benchmark contract
that protocol rows are libsignal-internal, wrapper rows are not protocol rows,
and PQXDH/ratchet metadata remains explicit in the schema.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ROOT = REPO.parent


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def struct_body(source: str, name: str) -> str:
    match = re.search(rf"pub(?:\(crate\))? struct {re.escape(name)}(?:<'a>)? \{{(.*?)^\}}", source, re.S | re.M)
    if not match:
        raise AssertionError(f"missing struct {name}")
    return match.group(1)


def assert_contains_all(text: str, required: list[str], label: str) -> None:
    missing = [item for item in required if item not in text]
    assert not missing, f"{label} missing: {missing}"


def test_csv_schema_contains_scientific_columns() -> None:
    metrics = read("Signal_containerized/src/signal_metrics.rs")
    csv = struct_body(metrics, "SignalCsvRow")
    required = [
        "profile_schema_version",
        "span_layer",
        "protocol_stack",
        "implementation",
        "measurement_class",
        "event_family",
        "event_subtype",
        "success",
        "error_class",
        "participant_id",
        "participant_device_id",
        "peer_id",
        "peer_device_id",
        "pair_id",
        "role",
        "direction",
        "phase",
        "wall_ns",
        "cpu_thread_ns",
        "cpu_envelope_utilization",
        "cpu_throttled_time_ratio",
        "alloc_bytes",
        "alloc_count",
        "ram_rss_delta_bytes",
        "ram_rss_utilization",
        "artifact_size_bytes",
        "prekey_stock_before",
        "prekey_stock_after",
        "prekey_refill_count",
        "prekey_refill_trigger",
        "plaintext_bytes",
        "ciphertext_bytes",
        "handshake_protocol",
        "handshake_side",
        "classical_one_time_prekey_present",
        "classical_one_time_prekey_id",
        "signed_prekey_id",
        "pq_prekey_id",
        "pq_prekey_type",
        "ciphertext_message_type",
        "message_counter",
        "previous_counter",
        "sender_ratchet_key_fingerprint",
        "receiver_chain_matched",
        "dh_ratchet_performed",
        "root_chain_updated",
        "send_chain_index_before",
        "send_chain_index_after",
        "receive_chain_index_before",
        "receive_chain_index_after",
        "skipped_message_keys_used",
        "skipped_message_keys_stored",
        "spqr_step_performed",
        "ratchet_progression_kind",
        "ratchet_progression_value",
        "run_id",
        "scenario",
        "scenario_seed",
        "node_name",
        "pod_name",
    ]
    assert_contains_all(csv, required, "SignalCsvRow")
    assert "profile_schema_version: 3" in read("Signal_containerized/libsignal-main/rust/protocol/src/profiling.rs")
    assert "profile_schema_version: 3" in read("Signal_containerized/src/bin/worker.rs")


def test_event_taxonomy_separates_wrapper_and_protocol_rows() -> None:
    worker = read("Signal_containerized/src/bin/worker.rs")
    profiling = read("Signal_containerized/libsignal-main/rust/protocol/src/profiling.rs")
    assert 'span_layer: "benchmark_wrapper"' in worker
    assert 'measurement_class: "wrapper"' in worker
    assert 'span_layer: "libsignal_main"' in profiling
    assert 'measurement_class_for_op' in profiling
    assert 'op.ends_with("_protocol")' in profiling
    assert 'op.contains("_ratchet_")' in profiling
    assert 'op.contains("_aead_")' in profiling


def test_required_protocol_event_names_are_inside_libsignal() -> None:
    libsignal_sources = "\n".join(
        read(path)
        for path in [
            "Signal_containerized/libsignal-main/rust/protocol/src/session.rs",
            "Signal_containerized/libsignal-main/rust/protocol/src/profiling.rs",
            "Signal_containerized/libsignal-main/rust/protocol/src/session_management.rs",
            "Signal_containerized/libsignal-main/rust/protocol/src/triple_ratchet.rs",
            "Signal_containerized/libsignal-main/rust/protocol/src/double_ratchet.rs",
        ]
    )
    required_events = [
        "pqxdh_initiator_process_bundle_protocol",
        "pqxdh_responder_receive_prekey_message_protocol",
        "signal_update_opks_generate_protocol",
        "signal_message_encrypt_protocol",
        "signal_message_decrypt_protocol",
        "signal_ratchet_send_chain_advance",
        "signal_ratchet_receive_chain_advance",
        "signal_ratchet_dh_step",
        "signal_ratchet_spqr_send",
        "signal_ratchet_spqr_recv",
        "signal_message_aead_encrypt",
        "signal_message_aead_decrypt",
    ]
    assert_contains_all(libsignal_sources, required_events, "libsignal protocol spans")


def test_protocol_rows_have_required_identity_and_metrics_fields() -> None:
    profiling = read("Signal_containerized/libsignal-main/rust/protocol/src/profiling.rs")
    context = struct_body(profiling, "ProfileContext")
    event = struct_body(profiling, "ProfileEvent")
    required_context = [
        "participant_id",
        "participant_device_id",
        "peer_id",
        "peer_device_id",
        "pair_id",
        "role",
        "direction",
        "phase",
        "conversation_size",
    ]
    assert_contains_all(context, required_context, "ProfileContext")
    required_event = required_context + [
        "success",
        "error_class",
        "wall_ns",
        "cpu_thread_ns",
        "alloc_bytes",
        "alloc_count",
    ]
    assert_contains_all(event, required_event, "ProfileEvent")


def test_pqxdh_prekey_paths_are_distinguishable() -> None:
    participant = read("Signal_containerized/src/signal_participant.rs")
    key_repo = read("Signal_containerized/src/key_repository.rs")
    session = read("Signal_containerized/libsignal-main/rust/protocol/src/session.rs")
    session_management = read("Signal_containerized/libsignal-main/rust/protocol/src/session_management.rs")
    assert "generate_replenishment_prekey_bundles" in participant
    assert "generate_kyber_prekey_record" in participant
    assert "last_resort_pq_prekey_record" in participant
    assert 'pq_prekey_type: "one_time".to_string()' in key_repo
    assert 'pq_prekey_type: "last_resort".to_string()' in key_repo
    assert '"one_time"' in session and '"last_resort"' in session
    assert 'pq_prekey_type: pq_prekey_id.map' in session_management
    assert "classical_one_time_prekey_present" in key_repo
    assert "pq_prekey_signature_present" in key_repo


def test_initial_and_ordinary_message_paths_are_separate() -> None:
    runner = read("Signal_containerized/src/staircase_runner.rs")
    session_management = read("Signal_containerized/libsignal-main/rust/protocol/src/session_management.rs")
    assert "handshake.initial_message_encrypt" in runner
    assert "handshake.initial_message_decrypt" in runner
    assert 'ciphertext_message_type: Some("PreKeySignalMessage")' in session_management
    assert 'ciphertext_message_type: Some("SignalMessage")' in session_management
    assert "pqxdh_responder_receive_prekey_message_protocol" in session_management
    assert "signal_message_decrypt_protocol" in session_management


def test_opk_stock_is_recipient_owned_and_dynamic() -> None:
    participant = read("Signal_containerized/src/signal_participant.rs")
    worker_api = read("Signal_containerized/src/worker_api.rs")
    runner = read("Signal_containerized/src/staircase_runner.rs")
    key_repo = read("Signal_containerized/src/key_repository.rs")
    assert "DEFAULT_ONE_TIME_PREKEY_COUNT" not in participant
    assert "SIGNAL_INITIAL_ONE_TIME_PREKEY_COUNT" in participant
    assert "generate_replenishment_prekey_bundles" in participant
    assert "UpdateOneTimePrekeys" in worker_api
    assert "published_prekey_stock" in worker_api
    assert "prekey.maintenance_after_handshake" in runner
    assert "prekey_stock" in key_repo


def test_synthetic_ratchet_counts_are_not_emitted() -> None:
    worker_api = read("Signal_containerized/src/worker_api.rs")
    assert "ratchet_step_count = Some" not in worker_api
    libsignal_ratchet = "\n".join(
        read(path)
        for path in [
            "Signal_containerized/libsignal-main/rust/protocol/src/triple_ratchet.rs",
            "Signal_containerized/libsignal-main/rust/protocol/src/double_ratchet.rs",
        ]
    )
    required = [
        "send_chain_index_before",
        "send_chain_index_after",
        "receive_chain_index_before",
        "receive_chain_index_after",
        "dh_ratchet_performed: Some(true)",
        "root_chain_updated: Some(true)",
        "ratchet_progression_value",
    ]
    assert_contains_all(libsignal_ratchet, required, "ratchet metadata")


def test_pairwise_fanout_is_not_labeled_sender_keys() -> None:
    worker_api = read("Signal_containerized/src/worker_api.rs")
    runner = read("Signal_containerized/src/staircase_runner.rs")
    assert "pairwise message encrypted" in worker_api
    assert "pairwise message received" in worker_api
    assert "SenderKey" not in runner


def main() -> int:
    tests = [
        test_csv_schema_contains_scientific_columns,
        test_event_taxonomy_separates_wrapper_and_protocol_rows,
        test_required_protocol_event_names_are_inside_libsignal,
        test_protocol_rows_have_required_identity_and_metrics_fields,
        test_pqxdh_prekey_paths_are_distinguishable,
        test_initial_and_ordinary_message_paths_are_separate,
        test_opk_stock_is_recipient_owned_and_dynamic,
        test_synthetic_ratchet_counts_are_not_emitted,
        test_pairwise_fanout_is_not_labeled_sender_keys,
    ]
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            print(f"PASS: {test.__name__}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL: {test.__name__}: {exc}")
            failed += 1
        except Exception as exc:
            print(f"ERROR: {test.__name__}: {type(exc).__name__}: {exc}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
