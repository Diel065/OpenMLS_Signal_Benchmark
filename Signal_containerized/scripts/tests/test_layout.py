#!/usr/bin/env python3
"""
Tests for the hybrid layout calculation in generate_compose.py.

These tests verify the layout formula matches the specification in MANIFEST.md.
"""
from __future__ import annotations

import math
import sys
import json
import tempfile
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import generate_compose
import run_compose_benchmark
from generate_compose import (
    compute_hybrid_layout,
    select_singleton_ids,
    worker_id,
    build_hybrid_layout,
    build_legacy_layout,
)


class FakeArgs:
    def __init__(
        self,
        workers: int,
        singleton_min_count: int = 16,
        singleton_fraction: float = 0.125,
        packed_clients_per_container: int = 16,
        singleton_selection_seed: int = 1,
        singleton_selection_strategy: str = "stratified-random",
    ):
        self.workers = workers
        self.singleton_min_count = singleton_min_count
        self.singleton_fraction = singleton_fraction
        self.packed_clients_per_container = packed_clients_per_container
        self.packed_worker_internal_parallelism = 4
        self.singleton_selection_seed = singleton_selection_seed
        self.singleton_selection_strategy = singleton_selection_strategy
        self.worker_layout_mode = "hybrid"
        self.base_worker_port = 8081
        self.kr_port = 3000
        self.relay_port = 4000
        self.bridge_count = 1
        self.publish_workers = False
        self.project_name = "signal-layout-test"
        self.include_runner = False
        self.runner_in_docker = False
        self.include_netcheck = False
        self.run_id = "test-run"
        self.scenario = "test-scenario"
        self.scenario_seed = 1
        self.output_dir = "benchmark_output"
        self.singleton_cpus = None
        self.singleton_cpus_float = None
        self.singleton_memory = None
        self.singleton_memory_bytes = None
        self.singleton_memory_swap = None
        self.singleton_memory_swap_bytes = None
        self.singleton_memory_swap_defaulted = False
        self.singleton_pids_limit = None
        self.singleton_app_heap_budget = None
        self.singleton_app_heap_budget_bytes = None
        self.resource_experiment = "none"
        self.profiled_singleton_count = 1
        self.disable_container_profiling = False
        self.cpu_affinity_mode = "none"
        self.strict_cpuset = False


def test_layout_16_workers():
    result = compute_hybrid_layout(16, 16, 0.125, 16)
    assert result["singleton_count"] == 16, f"Expected 16 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 0, f"Expected 0 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 0, f"Expected 0 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 16, f"Expected 16 physical, got {result['physical_worker_count']}"
    print("PASS: 16 workers → 16 singleton, 0 packed, 16 physical")


def test_layout_64_workers():
    result = compute_hybrid_layout(64, 16, 0.125, 16)
    assert result["singleton_count"] == 16, f"Expected 16 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 48, f"Expected 48 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 3, f"Expected 3 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 19, f"Expected 19 physical, got {result['physical_worker_count']}"
    print("PASS: 64 workers → 16 singleton, 48 packed, 3 containers, 19 physical")


def test_layout_128_workers():
    result = compute_hybrid_layout(128, 16, 0.125, 16)
    assert result["singleton_count"] == 16, f"Expected 16 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 112, f"Expected 112 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 7, f"Expected 7 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 23, f"Expected 23 physical, got {result['physical_worker_count']}"
    print("PASS: 128 workers → 16 singleton, 112 packed, 7 containers, 23 physical")


def test_layout_256_workers():
    result = compute_hybrid_layout(256, 16, 0.125, 16)
    assert result["singleton_count"] == 32, f"Expected 32 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 224, f"Expected 224 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 14, f"Expected 14 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 46, f"Expected 46 physical, got {result['physical_worker_count']}"
    print("PASS: 256 workers → 32 singleton, 224 packed, 14 containers, 46 physical")


def test_layout_512_workers():
    result = compute_hybrid_layout(512, 16, 0.125, 16)
    assert result["singleton_count"] == 64, f"Expected 64 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 448, f"Expected 448 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 28, f"Expected 28 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 92, f"Expected 92 physical, got {result['physical_worker_count']}"
    print("PASS: 512 workers → 64 singleton, 448 packed, 28 containers, 92 physical")


def test_layout_1024_workers():
    result = compute_hybrid_layout(1024, 16, 0.125, 16)
    assert result["singleton_count"] == 128, f"Expected 128 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 896, f"Expected 896 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 56, f"Expected 56 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 184, f"Expected 184 physical, got {result['physical_worker_count']}"
    print("PASS: 1024 workers → 128 singleton, 896 packed, 56 containers, 184 physical")


def test_singleton_selection_includes_00001():
    for total in [16, 32, 64, 128, 256, 512, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]
        ids = select_singleton_ids(total, s_count, seed=1, strategy="stratified-random")
        assert "00001" in ids, f"00001 must always be singleton for total={total}"

        ids_evenly = select_singleton_ids(total, s_count, seed=1, strategy="evenly-spaced")
        assert "00001" in ids_evenly, f"00001 must always be singleton (evenly-spaced) for total={total}"
    print("PASS: singleton selection always includes 00001")


def test_singleton_selection_deterministic():
    for total in [64, 256, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]

        ids_a = select_singleton_ids(total, s_count, seed=42, strategy="stratified-random")
        ids_b = select_singleton_ids(total, s_count, seed=42, strategy="stratified-random")
        assert ids_a == ids_b, f"Selection must be deterministic for seed=42, total={total}"
    print("PASS: singleton selection is deterministic")


def test_singleton_selection_count_matches():
    for total in [1, 8, 16, 32, 64, 128, 256, 512, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]
        ids = select_singleton_ids(total, s_count, seed=1, strategy="stratified-random")
        assert len(ids) == s_count, f"Expected {s_count} singleton IDs, got {len(ids)} for total={total}"
    print("PASS: singleton selection count matches layout")


def test_all_clients_covered():
    for total in [32, 64, 128, 256, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]
        singleton_ids = set(select_singleton_ids(total, s_count, seed=1, strategy="stratified-random"))

        all_ids = set(worker_id(i) for i in range(1, total + 1))
        packed_ids = all_ids - singleton_ids

        assert len(singleton_ids) + len(packed_ids) == total, f"Not all clients covered for total={total}"
        assert len(singleton_ids & packed_ids) == 0, f"Overlap between singleton and packed for total={total}"
    print("PASS: all clients are covered (singleton or packed)")


def test_hybrid_layout_build():
    for total in [32, 64, 128, 1024]:
        args = FakeArgs(total)
        layout = compute_hybrid_layout(total, args.singleton_min_count, args.singleton_fraction, args.packed_clients_per_container)
        singleton_ids = select_singleton_ids(total, layout["singleton_count"], args.singleton_selection_seed, args.singleton_selection_strategy)

        all_ids = [worker_id(i) for i in range(1, total + 1)]
        singleton_set = set(singleton_ids)
        packed_ids = [cid for cid in all_ids if cid not in singleton_set]

        clients, physical_workers = build_hybrid_layout(args, singleton_ids, packed_ids, layout)

        assert len(clients) == total, f"Expected {total} client entries, got {len(clients)}"
        assert len(physical_workers) == layout["physical_worker_count"], (
            f"Expected {layout['physical_worker_count']} physical workers, got {len(physical_workers)}"
        )

        singleton_clients = [c for c in clients if c.container_mode == "singleton"]
        packed_clients = [c for c in clients if c.container_mode == "packed"]
        assert len(singleton_clients) == layout["singleton_count"]
        assert len(packed_clients) == layout["packed_client_count"]

        all_profile_enabled = [c for c in clients if c.profile_enabled]
        assert len(all_profile_enabled) == layout["singleton_count"], "Only singletons should have profile_enabled=true"
    print("PASS: hybrid layout build produces correct structure")


def test_legacy_layout_build():
    for total in [1, 8, 16]:
        args = FakeArgs(total)
        args.worker_layout_mode = "one-container-per-client"
        clients, physical_workers = build_legacy_layout(args)

        assert len(clients) == total
        assert len(physical_workers) == total

        for c in clients:
            assert c.container_mode == "singleton"
            assert c.profile_enabled is True
    print("PASS: legacy layout build produces correct structure")


def test_disable_container_profiling_keeps_all_docker_clients_unprofiled():
    args = FakeArgs(32)
    args.disable_container_profiling = True
    layout = compute_hybrid_layout(
        args.workers,
        args.singleton_min_count,
        args.singleton_fraction,
        args.packed_clients_per_container,
    )
    singleton_ids = select_singleton_ids(
        args.workers,
        layout["singleton_count"],
        args.singleton_selection_seed,
        args.singleton_selection_strategy,
    )
    singleton_set = set(singleton_ids)
    packed_ids = [
        worker_id(index)
        for index in range(1, args.workers + 1)
        if worker_id(index) not in singleton_set
    ]

    clients, physical_workers = build_hybrid_layout(
        args, singleton_ids, packed_ids, layout
    )
    assert all(not client.profile_enabled for client in clients)
    assert all(not worker.profile_enabled_client_ids for worker in physical_workers)
    assert "--profile-path-template" not in generate_compose.generate_compose_text(
        args, physical_workers
    )
    worker_layout = generate_compose.generate_worker_layout_json(
        args, clients, physical_workers
    )
    assert worker_layout["profile_policy"] == "external_devices_only"

    args.worker_layout_mode = "one-container-per-client"
    clients, physical_workers = build_legacy_layout(args)
    assert all(not client.profile_enabled for client in clients)
    assert all(not worker.profile_enabled_client_ids for worker in physical_workers)


def test_empty_singleton_resource_envelope_is_disabled():
    args = FakeArgs(32)
    envelope = run_compose_benchmark.singleton_resource_envelope(args)
    assert envelope["enabled"] is False

    args.cpu_affinity_mode = "profiled-nor-background"
    envelope = run_compose_benchmark.singleton_resource_envelope(args)
    assert envelope["enabled"] is True


def test_profile_enabled_ids_are_explicit_for_every_worker():
    args = FakeArgs(32)
    layout = compute_hybrid_layout(
        args.workers,
        args.singleton_min_count,
        args.singleton_fraction,
        args.packed_clients_per_container,
    )
    singleton_ids = select_singleton_ids(
        args.workers,
        layout["singleton_count"],
        args.singleton_selection_seed,
        args.singleton_selection_strategy,
    )
    singleton_set = set(singleton_ids)
    packed_ids = [
        worker_id(index)
        for index in range(1, args.workers + 1)
        if worker_id(index) not in singleton_set
    ]
    _, physical_workers = build_hybrid_layout(args, singleton_ids, packed_ids, layout)
    compose = generate_compose.generate_compose_text(args, physical_workers)
    profile_flag = '      - "--profile-enabled-participant-ids"'
    empty_profile_flag = f'{profile_flag}\n      - ""'
    packed_workers = [
        physical_worker
        for physical_worker in physical_workers
        if physical_worker.container_mode == "packed"
    ]
    expected_empty_profiles = sum(
        not physical_worker.profile_enabled_client_ids
        for physical_worker in physical_workers
    )

    assert packed_workers
    assert all(not physical_worker.profile_enabled_client_ids for physical_worker in packed_workers)
    assert compose.count(profile_flag) == len(physical_workers)
    assert compose.count(empty_profile_flag) == expected_empty_profiles
    print("PASS: every worker receives an explicit profile-enabled participant list")




def test_resource_limits_apply_to_singletons_only():
    args = FakeArgs(64)
    args.singleton_cpus = "0.25"
    args.singleton_cpus_float = 0.25
    args.singleton_memory = "128m"
    args.singleton_memory_bytes = 134217728
    args.singleton_memory_swap = "128m"
    args.singleton_memory_swap_bytes = 134217728
    args.singleton_memory_swap_defaulted = True
    args.singleton_pids_limit = 128

    layout = compute_hybrid_layout(
        args.workers,
        args.singleton_min_count,
        args.singleton_fraction,
        args.packed_clients_per_container,
    )
    singleton_ids = select_singleton_ids(
        args.workers,
        layout["singleton_count"],
        args.singleton_selection_seed,
        args.singleton_selection_strategy,
    )
    all_ids = [worker_id(i) for i in range(1, args.workers + 1)]
    packed_ids = [cid for cid in all_ids if cid not in set(singleton_ids)]

    clients, physical_workers = build_hybrid_layout(args, singleton_ids, packed_ids, layout)
    layout_json = generate_compose.generate_worker_layout_json(args, clients, physical_workers)
    compose_text = generate_compose.generate_compose_text(args, physical_workers)

    singleton_workers = [pw for pw in layout_json["physical_workers"] if pw["container_mode"] == "singleton"]
    packed_workers = [pw for pw in layout_json["physical_workers"] if pw["container_mode"] == "packed"]

    assert all(pw["resource_limit_cpus"] == 0.25 for pw in singleton_workers)
    assert all(pw["resource_limit_memory_bytes"] == 134217728 for pw in singleton_workers)
    assert all(pw["resource_limit_pids"] == 128 for pw in singleton_workers)
    assert all(pw["resource_limit_cpus"] is None for pw in packed_workers)
    assert layout_json["singleton_resource_envelope"]["enabled"] is True
    assert 'cpus: "0.25"' in compose_text
    assert 'mem_limit: "128m"' in compose_text
    assert 'pids_limit: 128' in compose_text
    print("PASS: resource limits apply to singleton services and layout metadata only")


def test_signal_resource_sweeps_build_affinity_inputs_from_profiles():
    parsed = run_compose_benchmark.build_parser().parse_args(["--workers", "2", "--strict-cpuset"])
    run_compose_benchmark.apply_strict_cpuset_alias(parsed)
    assert parsed.cpu_affinity_mode == "profiled-nor-background"

    args = FakeArgs(16, singleton_min_count=2, singleton_fraction=0.0001, packed_clients_per_container=4)
    args.resource_experiment = "cpu-quota-sweep"
    args.profiled_singleton_count = 2
    args.runner_in_docker = True
    args.strict_cpuset = True

    run_compose_benchmark.apply_strict_cpuset_alias(args)
    assert args.cpu_affinity_mode == "profiled-nor-background"

    profiles = [
        {
            "resource_profile_id": "cpu_quota_1p0",
            "experiment_kind": "cpu_quota_sweep",
            "assigned_cpu_count": 1,
            "rayon_num_threads": 1,
        },
        {
            "resource_profile_id": "cpu_quota_0p5",
            "experiment_kind": "cpu_quota_sweep",
            "assigned_cpu_count": 2,
            "rayon_num_threads": 2,
        },
    ]
    specs, cpu_counts, bg_specs, layout_info = run_compose_benchmark.affinity_inputs_for_run(args, profiles)

    assert [spec["resource_profile_id"] for spec in specs] == ["cpu_quota_1p0", "cpu_quota_0p5"]
    assert sum(cpu_counts.values()) == 3
    assert {spec["container_name"] for spec in bg_specs} >= {"kr", "relay", "runner", "worker-pack-000"}
    assert layout_info["singleton_count"] == 2
    print("PASS: Signal resource sweeps build profiled affinity inputs from resource profiles")


def main() -> int:
    tests = [
        test_layout_16_workers,
        test_layout_64_workers,
        test_layout_128_workers,
        test_layout_256_workers,
        test_layout_512_workers,
        test_layout_1024_workers,
        test_singleton_selection_includes_00001,
        test_singleton_selection_deterministic,
        test_singleton_selection_count_matches,
        test_all_clients_covered,
        test_hybrid_layout_build,
        test_legacy_layout_build,
        test_disable_container_profiling_keeps_all_docker_clients_unprofiled,
        test_empty_singleton_resource_envelope_is_disabled,
        test_profile_enabled_ids_are_explicit_for_every_worker,
        test_resource_limits_apply_to_singletons_only,
        test_signal_resource_sweeps_build_affinity_inputs_from_profiles,
    ]

    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"FAIL: {test_fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR: {test_fn.__name__}: {e}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
