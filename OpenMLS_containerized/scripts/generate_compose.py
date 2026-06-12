#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import random
import re
from pathlib import Path


MEMORY_RE = re.compile(r"^(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>[bkmgt]?b?)?$", re.IGNORECASE)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Generate a docker-compose file plus worker lists for many MLS workers."
        )
    )

    p.add_argument("--workers", type=int, required=True, help="Number of logical worker clients")
    p.add_argument("--run-id", default="compose-generated-001", help="Default run id baked into the compose env")
    p.add_argument("--scenario", default="http-staircase-compose", help="Default scenario label baked into the compose env")
    p.add_argument("--scenario-seed", type=int, default=1, help="Scenario randomization seed baked into the profile env")
    p.add_argument("--output-dir", default="benchmark_output", help="Host results directory")
    p.add_argument("--compose-out", default="docker-compose.generated.yml", help="Generated compose file path")
    p.add_argument("--workers-out", default="workers.txt", help="Generated internal worker list path")
    p.add_argument("--workers-host-out", default="workers.host.txt", help="Generated host worker list path")
    p.add_argument("--project-name", default="mls-benchmark", help="Compose project name")
    p.add_argument("--base-worker-port", type=int, default=8081, help="First published host port for workers")
    p.add_argument("--ds-port", type=int, default=3000, help="Published DS port")
    p.add_argument("--relay-port", type=int, default=4000, help="Published relay port")

    p.add_argument(
        "--bridge-count",
        type=int,
        default=1,
        help=(
            "Number of Docker bridge networks to distribute workers across. "
            "Workers are assigned as evenly as possible across these bridges."
        ),
    )

    p.add_argument(
        "--publish-workers",
        action="store_true",
        help="Publish every worker on a host port. Disable this for large runs.",
    )

    p.add_argument(
        "--include-runner",
        action="store_true",
        help="Include a runner service that can run inside the Docker network.",
    )

    p.add_argument(
        "--include-netcheck",
        action="store_true",
        help=(
            "Include a debug netcheck service attached to all benchmark networks. "
            "Useful for in-network DNS/HTTP diagnostics."
        ),
    )

    # Hybrid layout flags
    p.add_argument(
        "--worker-layout-mode",
        choices=["one-container-per-client", "hybrid"],
        default="one-container-per-client",
        help="Worker layout mode: one-container-per-client (legacy) or hybrid",
    )
    p.add_argument(
        "--singleton-min-count",
        type=int,
        default=16,
        help="Minimum number of singleton measured clients in hybrid mode",
    )
    p.add_argument(
        "--singleton-fraction",
        type=float,
        default=0.125,
        help="Fraction of logical workers to use as singletons in hybrid mode",
    )
    p.add_argument(
        "--packed-clients-per-container",
        type=int,
        default=16,
        help="Number of packed virtual clients per packed container",
    )
    p.add_argument(
        "--singleton-selection-seed",
        type=int,
        default=1,
        help="Seed for deterministic singleton selection",
    )
    p.add_argument(
        "--singleton-selection-strategy",
        choices=["stratified-random", "evenly-spaced"],
        default="stratified-random",
        help="Strategy for selecting singleton client IDs",
    )
    p.add_argument(
        "--worker-layout-out",
        default="worker_layout.json",
        help="Output path for the worker layout JSON artifact",
    )
    p.add_argument(
        "--packed-worker-internal-parallelism",
        type=int,
        default=4,
        help="Internal parallelism for packed worker containers",
    )
    p.add_argument(
        "--singleton-cpus",
        default=None,
        help=(
            "Docker CPU envelope for every singleton worker service, e.g. 0.25. "
            "Unset means singleton containers are unconstrained."
        ),
    )
    p.add_argument(
        "--singleton-memory",
        default=None,
        help=(
            "Docker memory envelope for every singleton worker service, e.g. 128m. "
            "Unset means no memory limit."
        ),
    )
    p.add_argument(
        "--singleton-memory-swap",
        default=None,
        help=(
            "Docker memory+swap envelope for singleton worker services. "
            "If singleton memory is set and this is omitted, it defaults to the memory value."
        ),
    )
    p.add_argument(
        "--singleton-pids-limit",
        type=int,
        default=None,
        help="Optional Docker pids_limit for every singleton worker service.",
    )

    p.add_argument(
        "--failure-experiment",
        action="store_true",
        help=(
            "Failure-experiment mode: assign a reproducible randomized grid of "
            "CPU/RAM caps to profiled singleton workers (packed workers remain "
            "unconstrained). The grid is a Cartesian product of CPU caps (0.125, "
            "0.25, 0.5, 1.0, 2.0) and RAM caps (64m, 96m, 128m, 192m, 256m, "
            "384m, 512m, 768m, 1g), swap=RAM. Overrides --singleton-cpus and "
            "--singleton-memory. Only applies in hybrid layout mode."
        ),
    )

    # ── Resource experiment flags ──────────────────────────────────────
    p.add_argument(
        "--resource-experiment",
        choices=["none", "ram-sweep-singleton", "cpu-matrix-singleton"],
        default="none",
        help="Resource experiment mode for simulated IoT container profiling",
    )
    p.add_argument(
        "--affinity-plan-file",
        default=None,
        help="Path to a pre-computed cpu_affinity_plan.json file for this run",
    )
    p.add_argument(
        "--resource-profiles-file",
        default=None,
        help="Path to a pre-computed resource_profiles.csv or JSON file for this run",
    )
    p.add_argument(
        "--profiled-singleton-count",
        type=int,
        default=1,
        help="Number of profiled singleton workers in resource experiment mode",
    )
    p.add_argument(
        "--ram-sweep-values",
        default="32m,64m,128m,256m,512m,1g",
        help="Comma-separated list of Docker memory limits for RAM sweep",
    )
    p.add_argument(
        "--ram-sweep-cpu-count",
        type=int,
        default=10,
        help="Number of CPUs assigned to each singleton in RAM sweep",
    )
    p.add_argument(
        "--cpu-matrix-core-counts",
        default="1,2,4",
        help="Comma-separated list of CPU core counts for CPU matrix",
    )
    p.add_argument(
        "--cpu-matrix-capacity-fractions",
        default="0.25,0.50,0.75,1.00",
        help="Comma-separated list of capacity fractions for CPU matrix",
    )
    p.add_argument(
        "--resource-experiment-output-dir",
        default=None,
        help="Directory to write resource experiment sidecar files",
    )

    return p


def worker_id(i: int) -> str:
    return f"{i:05d}"


def service_name(i: int) -> str:
    return f"worker-{worker_id(i)}"


def bridge_name(i: int) -> str:
    return f"bench-net-{i:03d}"


def bridges_from_args(args: argparse.Namespace) -> list[str]:
    return [bridge_name(i) for i in range(args.bridge_count)]


def worker_bridge_index(args: argparse.Namespace, worker_index: int) -> int:
    return ((worker_index - 1) * args.bridge_count) // args.workers


def worker_bridge_name(args: argparse.Namespace, worker_index: int) -> str:
    return bridge_name(worker_bridge_index(args, worker_index))


def validate_args(args: argparse.Namespace) -> None:
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")
    if args.bridge_count < 1:
        raise SystemExit("--bridge-count must be at least 1")
    if args.bridge_count > args.workers:
        raise SystemExit("--bridge-count must not exceed --workers")
    if not (1 <= args.base_worker_port <= 65535):
        raise SystemExit("--base-worker-port must be between 1 and 65535")
    if not (1 <= args.ds_port <= 65535):
        raise SystemExit("--ds-port must be between 1 and 65535")
    if not (1 <= args.relay_port <= 65535):
        raise SystemExit("--relay-port must be between 1 and 65535")

    resource_experiment = getattr(args, "resource_experiment", "none")
    if resource_experiment != "none":
        if args.worker_layout_mode != "hybrid":
            raise SystemExit(
                "--resource-experiment requires --worker-layout-mode hybrid"
            )
        if getattr(args, "failure_experiment", False):
            raise SystemExit(
                "--resource-experiment and --failure-experiment are mutually exclusive"
            )
        if (
            args.singleton_cpus is not None
            or args.singleton_memory is not None
        ):
            raise SystemExit(
                "--resource-experiment overrides manual resource flags"
            )

    if getattr(args, "failure_experiment", False):
        if args.worker_layout_mode != "hybrid":
            raise SystemExit(
                "--failure-experiment requires --worker-layout-mode hybrid"
            )
        if (
            args.singleton_cpus is not None
            or args.singleton_memory is not None
            or args.singleton_memory_swap is not None
        ):
            raise SystemExit(
                "--failure-experiment overrides --singleton-cpus, "
                "--singleton-memory, and --singleton-memory-swap. "
                "Do not set those flags together with --failure-experiment."
            )

    if args.publish_workers:
        last_port = args.base_worker_port + args.workers - 1
        if last_port > 65535:
            raise SystemExit(
                f"Worker host ports would exceed 65535: last port would be {last_port}"
            )

    if args.worker_layout_mode == "hybrid":
        if args.singleton_min_count < 1:
            raise SystemExit("--singleton-min-count must be at least 1")
        if not (0 < args.singleton_fraction <= 1):
            raise SystemExit("--singleton-fraction must be between 0 and 1")
        if args.packed_clients_per_container < 1:
            raise SystemExit("--packed-clients-per-container must be at least 1")
        if args.packed_worker_internal_parallelism < 1:
            raise SystemExit("--packed-worker-internal-parallelism must be at least 1")

    normalize_resource_args(args)


def parse_memory_bytes(value: str | None) -> int | None:
    if value is None:
        return None

    raw = value.strip()
    if not raw:
        raise SystemExit("memory limits must not be empty")

    match = MEMORY_RE.match(raw)
    if not match:
        raise SystemExit(
            f"Unsupported memory limit '{value}'. Use a Docker-style value such as 64m, 128m, or 1g."
        )

    number = float(match.group("value"))
    unit = (match.group("unit") or "b").lower()
    multipliers = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "m": 1024 ** 2,
        "mb": 1024 ** 2,
        "g": 1024 ** 3,
        "gb": 1024 ** 3,
        "t": 1024 ** 4,
        "tb": 1024 ** 4,
    }
    if unit not in multipliers:
        raise SystemExit(
            f"Unsupported memory unit in '{value}'. Use b, k, m, g, or t."
        )

    bytes_value = int(number * multipliers[unit])
    if bytes_value <= 0:
        raise SystemExit("memory limits must be greater than zero")
    return bytes_value


def normalize_resource_args(args: argparse.Namespace) -> None:
    if args.singleton_cpus is not None:
        raw = str(args.singleton_cpus).strip()
        if not raw:
            raise SystemExit("--singleton-cpus must not be empty")
        try:
            cpus = float(raw)
        except ValueError as exc:
            raise SystemExit("--singleton-cpus must be a positive number") from exc
        if not math.isfinite(cpus) or cpus <= 0:
            raise SystemExit("--singleton-cpus must be a positive number")
        args.singleton_cpus = raw
        args.singleton_cpus_float = cpus
    else:
        args.singleton_cpus_float = None

    if args.singleton_memory is not None:
        args.singleton_memory = str(args.singleton_memory).strip()
        args.singleton_memory_bytes = parse_memory_bytes(args.singleton_memory)
    else:
        args.singleton_memory_bytes = None

    args.singleton_memory_swap_defaulted = False
    if args.singleton_memory_swap is not None:
        if args.singleton_memory is None:
            raise SystemExit("--singleton-memory-swap may only be set with --singleton-memory")
        args.singleton_memory_swap = str(args.singleton_memory_swap).strip()
        args.singleton_memory_swap_bytes = parse_memory_bytes(args.singleton_memory_swap)
    elif args.singleton_memory is not None:
        args.singleton_memory_swap = args.singleton_memory
        args.singleton_memory_swap_bytes = args.singleton_memory_bytes
        args.singleton_memory_swap_defaulted = True
    else:
        args.singleton_memory_swap_bytes = None

    if args.singleton_pids_limit is not None and args.singleton_pids_limit < 1:
        raise SystemExit("--singleton-pids-limit must be >= 1")


def resource_profile_for(args: argparse.Namespace) -> str:
    parts = []
    singleton_cpus = getattr(args, "singleton_cpus", None)
    singleton_memory = getattr(args, "singleton_memory", None)
    singleton_memory_swap = getattr(args, "singleton_memory_swap", None)
    singleton_pids_limit = getattr(args, "singleton_pids_limit", None)
    if singleton_cpus is not None:
        parts.append(f"cpus-{singleton_cpus}")
    if singleton_memory is not None:
        parts.append(f"memory-{singleton_memory}")
    if singleton_memory_swap is not None:
        parts.append(f"swap-{singleton_memory_swap}")
    if singleton_pids_limit is not None:
        parts.append(f"pids-{singleton_pids_limit}")
    if not parts:
        return ""
    return "singleton-resource-envelope_" + "_".join(parts)


def singleton_resource_limits(args: argparse.Namespace) -> dict:
    profile = resource_profile_for(args)
    return {
        "resource_limit_cpus": getattr(args, "singleton_cpus_float", None),
        "resource_limit_memory": getattr(args, "singleton_memory", None),
        "resource_limit_memory_bytes": getattr(args, "singleton_memory_bytes", None),
        "resource_limit_memory_swap": getattr(args, "singleton_memory_swap", None),
        "resource_limit_memory_swap_bytes": getattr(args, "singleton_memory_swap_bytes", None),
        "resource_limit_pids": getattr(args, "singleton_pids_limit", None),
        "resource_profile": profile,
    }


def empty_resource_limits() -> dict:
    return {
        "resource_limit_cpus": None,
        "resource_limit_memory": None,
        "resource_limit_memory_bytes": None,
        "resource_limit_memory_swap": None,
        "resource_limit_memory_swap_bytes": None,
        "resource_limit_pids": None,
        "resource_profile": "",
    }


# ── Failure-experiment grid ────────────────────────────────────────────

FAILURE_EXPERIMENT_CPU_CAPS = [0.125, 0.25, 0.5, 1.0, 2.0]
FAILURE_EXPERIMENT_RAM_CAPS = [
    "64m", "96m", "128m", "192m", "256m", "384m", "512m", "768m", "1g",
]
FAILURE_EXPERIMENT_GRID_SEED_DOMAIN = 0x4641494C_47524944


def build_failure_experiment_grid(
    seed: int,
) -> list[tuple[float, str, str]]:
    rng = random.Random(seed ^ FAILURE_EXPERIMENT_GRID_SEED_DOMAIN)
    grid: list[tuple[float, str, str]] = []
    for cpus in FAILURE_EXPERIMENT_CPU_CAPS:
        for ram in FAILURE_EXPERIMENT_RAM_CAPS:
            grid.append((cpus, ram, ram))
    rng.shuffle(grid)
    return grid


def failure_experiment_resource_limits(
    cpus: float,
    memory: str,
    memory_swap: str,
) -> dict:
    profile = (
        f"failure-experiment-resource-envelope"
        f"_cpus-{cpus}"
        f"_memory-{memory}"
    )
    return {
        "resource_limit_cpus": cpus,
        "resource_limit_memory": memory,
        "resource_limit_memory_bytes": None,
        "resource_limit_memory_swap": memory_swap,
        "resource_limit_memory_swap_bytes": None,
        "resource_limit_pids": None,
        "resource_profile": profile,
        "failure_experiment": True,
    }


# ── Hybrid layout calculation ──────────────────────────────────────────

def compute_hybrid_layout(
    total_logical_workers: int,
    singleton_min_count: int,
    singleton_fraction: float,
    packed_clients_per_container: int,
) -> dict:
    singleton_count = min(
        total_logical_workers,
        max(singleton_min_count, math.ceil(total_logical_workers * singleton_fraction)),
    )
    packed_client_count = total_logical_workers - singleton_count
    packed_container_count = math.ceil(packed_client_count / packed_clients_per_container) if packed_client_count > 0 else 0
    physical_worker_count = singleton_count + packed_container_count

    return {
        "singleton_count": singleton_count,
        "packed_client_count": packed_client_count,
        "packed_container_count": packed_container_count,
        "physical_worker_count": physical_worker_count,
    }


# ── Singleton selection ────────────────────────────────────────────────

def select_singleton_ids(
    total_logical_workers: int,
    singleton_count: int,
    seed: int,
    strategy: str,
) -> list[str]:
    if singleton_count >= total_logical_workers:
        return [worker_id(i) for i in range(1, total_logical_workers + 1)]

    singletons = {worker_id(1)}

    if singleton_count == 1:
        return sorted(singletons)

    remaining_slots = singleton_count - 1
    candidate_ids = list(range(2, total_logical_workers + 1))

    if strategy == "evenly-spaced":
        strata_size = len(candidate_ids) / remaining_slots
        for i in range(remaining_slots):
            idx = int(i * strata_size + strata_size / 2)
            idx = min(idx, len(candidate_ids) - 1)
            singletons.add(worker_id(candidate_ids[idx]))
    else:
        rng = random.Random(seed)
        strata_size = len(candidate_ids) / remaining_slots
        for i in range(remaining_slots):
            start = int(i * strata_size)
            end = int((i + 1) * strata_size) if i < remaining_slots - 1 else len(candidate_ids)
            chosen = rng.choice(candidate_ids[start:end])
            singletons.add(worker_id(chosen))

    return sorted(singletons)


# ── Layout builder ─────────────────────────────────────────────────────

class ClientLayoutEntry:
    def __init__(
        self,
        client_id: str,
        physical_worker_id: str,
        container_mode: str,
        profile_enabled: bool,
        command_url: str,
        health_url: str,
    ):
        self.client_id = client_id
        self.physical_worker_id = physical_worker_id
        self.container_mode = container_mode
        self.profile_enabled = profile_enabled
        self.command_url = command_url
        self.health_url = health_url

    def to_dict(self) -> dict:
        return {
            "client_id": self.client_id,
            "physical_worker_id": self.physical_worker_id,
            "container_mode": self.container_mode,
            "profile_enabled": self.profile_enabled,
            "command_url": self.command_url,
            "health_url": self.health_url,
            "execution_backend": "docker_container",
            "device_kind": "scratch_container",
            "transport": "docker_bridge",
            "access_backend": "docker",
            "arch": "x86_64",
            "rust_target": "x86_64-unknown-linux-musl",
        }


class PhysicalWorkerEntry:
    def __init__(
        self,
        physical_worker_id: str,
        container_mode: str,
        client_ids: list[str],
        base_url: str,
        profile_enabled_client_ids: list[str],
        resource_limits: dict | None = None,
    ):
        self.physical_worker_id = physical_worker_id
        self.container_mode = container_mode
        self.client_ids = client_ids
        self.base_url = base_url
        self.profile_enabled_client_ids = profile_enabled_client_ids
        self.resource_limits = resource_limits or empty_resource_limits()

    def to_dict(self) -> dict:
        data = {
            "physical_worker_id": self.physical_worker_id,
            "container_mode": self.container_mode,
            "client_ids": self.client_ids,
            "base_url": self.base_url,
            "profile_enabled_client_ids": self.profile_enabled_client_ids,
            "execution_backend": "docker_container",
            "device_kind": "scratch_container",
            "transport": "docker_bridge",
            "access_backend": "docker",
            "arch": "x86_64",
            "rust_target": "x86_64-unknown-linux-musl",
        }
        data.update(self.resource_limits)
        return data


def build_hybrid_layout(
    args: argparse.Namespace,
    singleton_ids: list[str],
    packed_client_ids: list[str],
    layout_info: dict,
) -> tuple[list[ClientLayoutEntry], list[PhysicalWorkerEntry]]:
    singleton_set = set(singleton_ids)
    clients: list[ClientLayoutEntry] = []
    physical_workers: list[PhysicalWorkerEntry] = []
    configured_singleton_limits = singleton_resource_limits(args)

    failure_grid = None
    failure_grid_rng = None
    if getattr(args, "failure_experiment", False):
        failure_grid = build_failure_experiment_grid(args.singleton_selection_seed)
        failure_grid_rng = random.Random(args.singleton_selection_seed ^ 0x4641494C_524E47)

    singleton_counter = 0
    for idx, cid in enumerate(singleton_ids):
        sid = f"worker-{cid}"
        base_url = f"http://{sid}:8080"
        clients.append(ClientLayoutEntry(
            client_id=cid,
            physical_worker_id=sid,
            container_mode="singleton",
            profile_enabled=True,
            command_url=f"{base_url}/client/{cid}",
            health_url=f"{base_url}/client/{cid}/health",
        ))

        if failure_grid and failure_grid_rng:
            grid_idx = idx % len(failure_grid)
            cpus_val, mem_val, swap_val = failure_grid[grid_idx]
            worker_limits = failure_experiment_resource_limits(cpus_val, mem_val, swap_val)
        else:
            worker_limits = configured_singleton_limits

        physical_workers.append(PhysicalWorkerEntry(
            physical_worker_id=sid,
            container_mode="singleton",
            client_ids=[cid],
            base_url=base_url,
            profile_enabled_client_ids=[cid],
            resource_limits=worker_limits,
        ))
        singleton_counter += 1

    pack_idx = 0
    clients_per_pack = args.packed_clients_per_container
    for start in range(0, len(packed_client_ids), clients_per_pack):
        pack_clients = packed_client_ids[start:start + clients_per_pack]
        pack_id = f"worker-pack-{pack_idx:03d}"
        base_url = f"http://{pack_id}:8080"

        for cid in pack_clients:
            clients.append(ClientLayoutEntry(
                client_id=cid,
                physical_worker_id=pack_id,
                container_mode="packed",
                profile_enabled=False,
                command_url=f"{base_url}/client/{cid}",
                health_url=f"{base_url}/client/{cid}/health",
            ))

        physical_workers.append(PhysicalWorkerEntry(
            physical_worker_id=pack_id,
            container_mode="packed",
            client_ids=pack_clients,
            base_url=base_url,
            profile_enabled_client_ids=[],
            resource_limits=empty_resource_limits(),
        ))
        pack_idx += 1

    return clients, physical_workers


def build_legacy_layout(
    args: argparse.Namespace,
) -> tuple[list[ClientLayoutEntry], list[PhysicalWorkerEntry]]:
    clients: list[ClientLayoutEntry] = []
    physical_workers: list[PhysicalWorkerEntry] = []
    configured_singleton_limits = singleton_resource_limits(args)

    for i in range(1, args.workers + 1):
        cid = worker_id(i)
        sid = f"worker-{cid}"
        base_url = f"http://{sid}:8080"
        clients.append(ClientLayoutEntry(
            client_id=cid,
            physical_worker_id=sid,
            container_mode="singleton",
            profile_enabled=True,
            command_url=f"{base_url}/client/{cid}",
            health_url=f"{base_url}/client/{cid}/health",
        ))
        physical_workers.append(PhysicalWorkerEntry(
            physical_worker_id=sid,
            container_mode="singleton",
            client_ids=[cid],
            base_url=base_url,
            profile_enabled_client_ids=[cid],
            resource_limits=configured_singleton_limits,
        ))

    return clients, physical_workers


def generate_worker_layout_json(
    args: argparse.Namespace,
    clients: list[ClientLayoutEntry],
    physical_workers: list[PhysicalWorkerEntry],
) -> dict:
    singleton_limits = singleton_resource_limits(args)
    envelope_enabled = any(
        singleton_limits[key] is not None
        for key in (
            "resource_limit_cpus",
            "resource_limit_memory",
            "resource_limit_memory_swap",
            "resource_limit_pids",
        )
    ) or getattr(args, "failure_experiment", False)

    failure_experiment_grid = None
    if getattr(args, "failure_experiment", False):
        failure_experiment_grid = {
            "mode": "failure_experiment",
            "seed": args.singleton_selection_seed,
            "cpu_caps": FAILURE_EXPERIMENT_CPU_CAPS,
            "ram_caps": FAILURE_EXPERIMENT_RAM_CAPS,
            "grid_cells": len(FAILURE_EXPERIMENT_CPU_CAPS) * len(FAILURE_EXPERIMENT_RAM_CAPS),
            "swap_equals_ram": True,
            "interpretation": (
                "Each profiled singleton receives one cell from the cartesian grid. "
                "Packed workers are unconstrained. Swap is locked to RAM so memory "
                "failures are easier to interpret."
            ),
        }

    return {
        "version": 2,
        "logical_worker_count": args.workers,
        "physical_worker_count": len(physical_workers),
        "layout_mode": args.worker_layout_mode,
        "singleton_min_count": args.singleton_min_count,
        "singleton_fraction": args.singleton_fraction,
        "packed_clients_per_container": args.packed_clients_per_container,
        "singleton_selection_seed": args.singleton_selection_seed,
        "profile_policy": "singletons_only" if args.worker_layout_mode == "hybrid" else "all",
        "failure_experiment": failure_experiment_grid,
        "singleton_resource_envelope": {
            "enabled": envelope_enabled,
            "applies_to": "all_containerized_singletons",
            "scientific_interpretation": (
                "host-specific reproducible resource envelope; not exact hardware emulation"
            ),
            "cpus": singleton_limits["resource_limit_cpus"],
            "memory": singleton_limits["resource_limit_memory"],
            "memory_swap": singleton_limits["resource_limit_memory_swap"],
            "memory_swap_defaulted_to_memory": getattr(
                args, "singleton_memory_swap_defaulted", False
            ),
            "pids_limit": singleton_limits["resource_limit_pids"],
            "resource_profile": singleton_limits["resource_profile"],
        },
        "clients": [c.to_dict() for c in clients],
        "physical_workers": [pw.to_dict() for pw in physical_workers],
    }


# ── Compose generation ─────────────────────────────────────────────────

def append_netcheck_service(
        lines: list[str],
        *,
        args: argparse.Namespace,
        bridges: list[str],
) -> None:
    monitor_script = f"""\
set +e

RUN_ID="{args.run_id}"
OUT="/results/$RUN_ID/netcheck.log"
WORKERS="/results/$RUN_ID/workers.txt"
TARGETS="/results/$RUN_ID/netcheck_targets.txt"

INTERVAL_SECONDS=5
FULL_SCAN_EVERY_SECONDS=60

mkdir -p "/results/$RUN_ID"
: > "$OUT"
exec >> "$OUT" 2>&1

ts() {{ date -u +"%Y-%m-%dT%H:%M:%SZ"; }}
log() {{ echo "[$(ts)] $*"; }}

check_url() {{
  label="$1"
  url="$2"

  if curl -fsS --connect-timeout 1 --max-time 3 "$url" >/dev/null; then
    log "OK $label $url"
  else
    rc=$?
    log "FAIL $label $url curl_exit=$rc"
  fi
}}

snapshot() {{
  log "======================================================================"
  log "NETCHECK MONITOR START"
  log "======================================================================"

  log "hostname=$(hostname)"

  log "--- /etc/resolv.conf ---"
  cat /etc/resolv.conf || true

  log "--- ip addr ---"
  ip addr || true

  log "--- ip route ---"
  ip route || true

  log "--- ss -tulpen ---"
  ss -tulpen || true

  log "--- workers.txt ---"
  if [ -f "$WORKERS" ]; then
    wc -l "$WORKERS" || true
    head -n 20 "$WORKERS" || true
    tail -n 20 "$WORKERS" || true
  else
    log "MISSING workers file: $WORKERS"
  fi
}}

worker_scan() {{
  mode="$1"

  if [ ! -f "$WORKERS" ]; then
    log "WORKER_SCAN mode=$mode missing_workers_file=$WORKERS"
    return 0
  fi

  total=0
  checked=0
  failed=0

  while IFS="=" read -r id url; do
    [ -z "$id" ] && continue

    total=$((total + 1))
    do_check=0

    if [ "$mode" = "full" ]; then
      do_check=1
    else
      if [ "$total" -le 10 ]; then
        do_check=1
      fi

      if [ $(((total + iteration) % 250)) -eq 0 ]; then
        do_check=1
      fi
    fi

    if [ "$do_check" -eq 1 ]; then
      checked=$((checked + 1))

      if ! curl -fsS --connect-timeout 1 --max-time 3 "$url/health" >/dev/null; then
        rc=$?
        host=$(printf "%s" "$url" | cut -d/ -f3 | cut -d: -f1)
        dns=$(getent hosts "$host" 2>/dev/null || true)

        log "WORKER_FAIL mode=$mode id=$id url=$url host=$host curl_exit=$rc dns=$dns"

        failed=$((failed + 1))
      fi
    fi
  done < "$WORKERS"

  healthy=$((checked - failed))

  log "WORKER_SCAN mode=$mode total_workers=$total checked=$checked healthy_checked=$healthy failed_checked=$failed"
}}

target_scan() {{
  if [ ! -f "$TARGETS" ] || [ ! -f "$WORKERS" ]; then
    return 0
  fi

  while read -r target_id; do
    [ -z "$target_id" ] && continue
    line=$(grep -E "^$target_id=" "$WORKERS" 2>/dev/null | head -n 1 || true)
    if [ -z "$line" ]; then
      log "TARGET_FAIL id=$target_id missing_from_workers_file=$WORKERS"
      continue
    fi

    id=$(printf "%s" "$line" | cut -d= -f1)
    url=$(printf "%s" "$line" | cut -d= -f2-)

    if curl -fsS --connect-timeout 1 --max-time 3 "$url/health" >/dev/null; then
      log "TARGET_OK id=$id url=$url"
    else
      rc=$?
      host=$(printf "%s" "$url" | cut -d/ -f3 | cut -d: -f1)
      dns=$(getent hosts "$host" 2>/dev/null || true)
      log "TARGET_FAIL id=$id url=$url host=$host curl_exit=$rc dns=$dns"
    fi
  done < "$TARGETS"
}}

trap 'log "NETCHECK MONITOR RECEIVED STOP SIGNAL"; exit 0' TERM INT

snapshot

last_full=0
iteration=0

while true; do
  iteration=$((iteration + 1))
  now=$(date +%s)

  log "HEARTBEAT iteration=$iteration"

  check_url "ds" "http://ds:{args.ds_port}/health"
  check_url "relay" "http://relay:{args.relay_port}/health"

  if [ "$last_full" -eq 0 ] || [ $((now - last_full)) -ge "$FULL_SCAN_EVERY_SECONDS" ]; then
    worker_scan full
    last_full="$now"
  else
    worker_scan sample
  fi

  target_scan

  sleep "$INTERVAL_SECONDS" &
  wait $!
done
"""

    monitor_script = monitor_script.replace("$", "$$")

    lines.append("")
    lines.append("  netcheck:")
    lines.append("    image: nicolaka/netshoot:latest")
    lines.append("    depends_on:")
    lines.append("      - ds")
    lines.append("      - relay")
    lines.append("    volumes:")
    lines.append(f"      - ./{args.output_dir}:/results")
    lines.append("    command:")
    lines.append("      - sh")
    lines.append("      - -lc")
    lines.append("      - |")
    for raw_line in monitor_script.strip("\n").splitlines():
        lines.append(f"        {raw_line}")
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")


def append_bounded_logging(lines: list[str], indent: str = "    ") -> None:
    lines.append(f"{indent}logging:")
    lines.append(f'{indent}  driver: "local"')
    lines.append(f"{indent}  options:")
    lines.append(f'{indent}    max-size: "10m"')
    lines.append(f'{indent}    max-file: "3"')


def physical_worker_bridge_index(
    args: argparse.Namespace,
    physical_idx: int,
    total_physical: int,
) -> int:
    if total_physical <= 1:
        return 0
    return (physical_idx * args.bridge_count) // total_physical


def generate_compose_text(
    args: argparse.Namespace,
    physical_workers: list[PhysicalWorkerEntry],
    affinity_plan: dict | None = None,
    resource_profiles: list | None = None,
    resource_experiment: str = "none",
) -> str:
    lines: list[str] = []
    bridges = bridges_from_args(args)

    lines.append(f"name: {args.project_name}")
    lines.append("")
    lines.append("x-worker-common: &worker-common")
    lines.append("  image: mls-worker")
    lines.append("  cap_add:")
    lines.append("    - PERFMON")
    lines.append("  security_opt:")
    lines.append("    - seccomp=unconfined")
    lines.append("  environment:")
    lines.append(f"    OPENMLS_PROFILE_RUN_ID: {args.run_id}")
    lines.append(f"    OPENMLS_PROFILE_SCENARIO: {args.scenario}")
    lines.append(f"    OPENMLS_PROFILE_SCENARIO_SEED: {args.scenario_seed}")
    lines.append("    MLS_DEBUG_LOGS: ${MLS_DEBUG_LOGS:-}")
    lines.append("    OPENMLS_WORKER_DEBUG_IDS: ${OPENMLS_WORKER_DEBUG_IDS:-}")
    lines.append("    OPENMLS_WORKER_COMMAND_QUEUE_CAPACITY: ${OPENMLS_WORKER_COMMAND_QUEUE_CAPACITY:-}")
    lines.append("    OPENMLS_WORKER_IDEMPOTENCY_CACHE_SIZE: ${OPENMLS_WORKER_IDEMPOTENCY_CACHE_SIZE:-}")
    lines.append("    OPENMLS_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS: ${OPENMLS_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS:-}")
    lines.append("    OPENMLS_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST: ${OPENMLS_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST:-32}")
    lines.append("    OPENMLS_WORKER_HTTP_CONNECT_TIMEOUT_MS: ${OPENMLS_WORKER_HTTP_CONNECT_TIMEOUT_MS:-5000}")
    lines.append("    OPENMLS_WORKER_HTTP_REQUEST_TIMEOUT_MS: ${OPENMLS_WORKER_HTTP_REQUEST_TIMEOUT_MS:-30000}")
    lines.append("    OPENMLS_WORKER_OUTBOUND_HTTP_PERMITS: ${OPENMLS_WORKER_OUTBOUND_HTTP_PERMITS:-32}")
    lines.append("  depends_on:")
    lines.append("    - ds")
    lines.append("    - relay")
    lines.append("  volumes:")
    lines.append(f"    - ./{args.output_dir}:/results")
    append_bounded_logging(lines, indent="  ")
    lines.append("")
    lines.append("services:")

    bg_cpuset = None
    if affinity_plan and resource_experiment != "none":
        bg_cpus = affinity_plan.get("background_cpus", [])
        if bg_cpus:
            bg_cpuset = ",".join(str(c) for c in sorted(bg_cpus))

    lines.append("  ds:")
    lines.append("    image: mls-ds")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.ds_port}"]')
    lines.append("    environment:")
    lines.append("      OPENMLS_DS_DELIVERY_MODE: ${OPENMLS_DS_DELIVERY_MODE:-group-log}")
    lines.append("      OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT: ${OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT:-128}")
    lines.append("    ports:")
    lines.append(f'      - "{args.ds_port}:{args.ds_port}"')
    if bg_cpuset:
        lines.append(f'    cpuset: "{bg_cpuset}"')
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")
    append_bounded_logging(lines)
    lines.append("")

    lines.append("  relay:")
    lines.append("    image: mls-relay")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.relay_port}"]')
    lines.append("    environment:")
    lines.append("      OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT: ${OPENMLS_SERVICE_METRICS_WARN_IN_FLIGHT:-128}")
    lines.append("    ports:")
    lines.append(f'      - "{args.relay_port}:{args.relay_port}"')
    if bg_cpuset:
        lines.append(f'    cpuset: "{bg_cpuset}"')
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")
    append_bounded_logging(lines)

    total_physical = len(physical_workers)
    next_host_port = args.base_worker_port
    singleton_idx = 0

    for pw_idx, pw in enumerate(physical_workers):
        lines.append("")
        lines.append(f"  {pw.physical_worker_id}:")
        lines.append("    <<: *worker-common")
        affinity_result = {}
        if pw.container_mode == "singleton":
            if resource_experiment != "none" and affinity_plan:
                affinity_result = apply_affinity_to_compose(
                    lines, pw.physical_worker_id, pw.container_mode,
                    affinity_plan, resource_profiles, singleton_idx, args,
                ) or {}
                singleton_idx += 1
            else:
                limits = pw.resource_limits
                if limits.get("resource_limit_cpus") is not None:
                    lines.append(f'    cpus: "{limits["resource_limit_cpus"]}"')
                if limits.get("resource_limit_memory") is not None:
                    lines.append(f'    mem_limit: "{limits["resource_limit_memory"]}"')
                if limits.get("resource_limit_memory_swap") is not None:
                    lines.append(f'    memswap_limit: "{limits["resource_limit_memory_swap"]}"')
                if limits.get("resource_limit_pids") is not None:
                    lines.append(f'    pids_limit: {limits["resource_limit_pids"]}')
        else:
            if bg_cpuset:
                lines.append(f'    cpuset: "{bg_cpuset}"')
        lines.append("    command:")
        lines.append('      - "--name"')
        lines.append(f'      - "{pw.physical_worker_id}"')

        client_ids_csv = ",".join(pw.client_ids)
        lines.append('      - "--clients"')
        lines.append(f'      - "{client_ids_csv}"')

        if pw.container_mode == "singleton" and pw.profile_enabled_client_ids:
            profile_csv = ",".join(pw.profile_enabled_client_ids)
            lines.append('      - "--profile-enabled-client-ids"')
            lines.append(f'      - "{profile_csv}"')
            lines.append('      - "--profile-path-template"')
            lines.append(f'      - "/results/{args.run_id}/client-{{client_id}}.jsonl"')

        lines.append('      - "--ds-url"')
        lines.append(f'      - "http://ds:{args.ds_port}"')
        lines.append('      - "--relay-url"')
        lines.append(f'      - "http://relay:{args.relay_port}"')
        lines.append('      - "--listen-addr"')
        lines.append('      - "0.0.0.0:8080"')

        lines.append("    environment:")
        lines.append(f"      OPENMLS_PROFILE_RUN_ID: {args.run_id}")
        lines.append(f"      OPENMLS_PROFILE_SCENARIO: {args.scenario}")
        lines.append(f"      OPENMLS_PROFILE_SCENARIO_SEED: {args.scenario_seed}")

        if affinity_result.get("rayon_num_threads"):
            lines.append(f'      RAYON_NUM_THREADS: "{affinity_result["rayon_num_threads"]}"')

        if pw.container_mode == "singleton" and pw.profile_enabled_client_ids:
            profile_csv = ",".join(pw.profile_enabled_client_ids)
            lines.append(f'      OPENMLS_PROFILE_ENABLED: "true"')
            lines.append(f'      OPENMLS_PROFILE_CLIENT_IDS: "{profile_csv}"')
            lines.append(f'      OPENMLS_PROFILE_PATH_TEMPLATE: "/results/{args.run_id}/client-{{client_id}}.jsonl"')
            first_cid = pw.profile_enabled_client_ids[0]
            lines.append(f'      OPENMLS_PROFILE_PATH: "/results/{args.run_id}/client-{first_cid}.jsonl"')
        else:
            lines.append('      OPENMLS_PROFILE_ENABLED: "false"')
            lines.append('      OPENMLS_PROFILE_CLIENT_IDS: ""')
            lines.append('      OPENMLS_PROFILE_PATH_TEMPLATE: ""')

        if pw.container_mode == "packed":
            lines.append(f'      OPENMLS_PACKED_WORKER_INTERNAL_PARALLELISM: "{args.packed_worker_internal_parallelism}"')

        lines.append("      MLS_DEBUG_LOGS: ${MLS_DEBUG_LOGS:-}")
        lines.append("      OPENMLS_WORKER_DEBUG_IDS: ${OPENMLS_WORKER_DEBUG_IDS:-}")
        lines.append("      OPENMLS_WORKER_COMMAND_QUEUE_CAPACITY: ${OPENMLS_WORKER_COMMAND_QUEUE_CAPACITY:-}")
        lines.append("      OPENMLS_WORKER_IDEMPOTENCY_CACHE_SIZE: ${OPENMLS_WORKER_IDEMPOTENCY_CACHE_SIZE:-}")
        lines.append("      OPENMLS_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS: ${OPENMLS_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS:-}")
        lines.append("      OPENMLS_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST: ${OPENMLS_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST:-32}")
        lines.append("      OPENMLS_WORKER_HTTP_CONNECT_TIMEOUT_MS: ${OPENMLS_WORKER_HTTP_CONNECT_TIMEOUT_MS:-5000}")
        lines.append("      OPENMLS_WORKER_HTTP_REQUEST_TIMEOUT_MS: ${OPENMLS_WORKER_HTTP_REQUEST_TIMEOUT_MS:-30000}")
        lines.append("      OPENMLS_WORKER_OUTBOUND_HTTP_PERMITS: ${OPENMLS_WORKER_OUTBOUND_HTTP_PERMITS:-32}")

        if args.publish_workers:
            lines.append("    ports:")
            lines.append(f'      - "{next_host_port}:8080"')
            next_host_port += 1

        bridge_idx = physical_worker_bridge_index(args, pw_idx, total_physical)
        lines.append("    networks:")
        lines.append(f"      - {bridge_name(bridge_idx)}")

    if args.include_runner:
        lines.append("")
        lines.append("  runner:")
        lines.append("    image: mls-runner")
        lines.append("    profiles:")
        lines.append("      - runner")
        lines.append("    depends_on:")
        lines.append("      - ds")
        lines.append("      - relay")
        lines.append("    environment:")
        lines.append("      OPENMLS_RUNNER_HTTP_CONNECT_TIMEOUT_MS: ${OPENMLS_RUNNER_HTTP_CONNECT_TIMEOUT_MS:-2000}")
        lines.append("      OPENMLS_RUNNER_HTTP_REQUEST_TIMEOUT_MS: ${OPENMLS_RUNNER_HTTP_REQUEST_TIMEOUT_MS:-60000}")
        lines.append("    ulimits:")
        lines.append("      nofile:")
        lines.append("        soft: 1048576")
        lines.append("        hard: 1048576")
        lines.append("    volumes:")
        lines.append(f"      - ./{args.output_dir}:/results")
        lines.append("    networks:")
        for bridge in bridges:
            lines.append(f"      - {bridge}")
        append_bounded_logging(lines)

    if args.include_netcheck:
        append_netcheck_service(lines, args=args, bridges=bridges)
        append_bounded_logging(lines)

    lines.append("")
    lines.append("networks:")
    for bridge in bridges:
        lines.append(f"  {bridge}:")
        lines.append("    driver: bridge")

    return "\n".join(lines) + "\n"


def generate_workers_internal(args: argparse.Namespace, clients: list[ClientLayoutEntry]) -> str:
    lines = []
    for c in clients:
        svc = c.physical_worker_id
        lines.append(f"{c.client_id}=http://{svc}:8080")
    return "\n".join(lines) + "\n"


def generate_workers_host(
    args: argparse.Namespace,
    physical_workers: list[PhysicalWorkerEntry],
) -> str:
    lines = []
    next_port = args.base_worker_port
    port_map: dict[str, int] = {}
    for pw in physical_workers:
        port_map[pw.physical_worker_id] = next_port
        next_port += 1

    for pw in physical_workers:
        port = port_map[pw.physical_worker_id]
        for cid in pw.client_ids:
            lines.append(f"{cid}=http://127.0.0.1:{port}")
    return "\n".join(lines) + "\n"


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def load_affinity_plan(args: argparse.Namespace) -> dict | None:
    """Load a pre-computed affinity plan JSON file."""
    plan_file = getattr(args, "affinity_plan_file", None)
    if not plan_file:
        return None
    try:
        with open(plan_file) as f:
            return json.load(f)
    except Exception as e:
        print(f"[warn] Could not load affinity plan from {plan_file}: {e}", flush=True)
        return None


def load_resource_profiles(args: argparse.Namespace) -> list | None:
    """Load pre-computed resource profiles from a JSON file."""
    profiles_file = getattr(args, "resource_profiles_file", None)
    if not profiles_file:
        return None
    try:
        with open(profiles_file) as f:
            return json.load(f)
    except Exception as e:
        print(f"[warn] Could not load resource profiles from {profiles_file}: {e}", flush=True)
        return None


def apply_affinity_to_compose(
    lines: list[str],
    physical_worker_id: str,
    container_mode: str,
    affinity_plan: dict | None,
    resource_profiles: list | None,
    profile_idx: int,
    args: argparse.Namespace,
) -> dict | None:
    """Apply cpuset and resource limits to compose service lines.
    Returns a dict with 'rayon_num_threads' and 'cpuset' info for later environment emission.
    """
    result = {}
    if affinity_plan is None:
        return result

    if container_mode == "singleton":
        found_in_plan = False
        for pa in affinity_plan.get("profiled_assignments", []):
            if pa.get("container_name") == physical_worker_id:
                cpus = pa.get("assigned_cpus", [])
                if cpus:
                    cpuset_str = ",".join(str(c) for c in sorted(cpus))
                    lines.append(f'    cpuset: "{cpuset_str}"')
                    result["cpuset"] = cpuset_str
                rayon = pa.get("rayon_num_threads", 0)
                if rayon > 0:
                    result["rayon_num_threads"] = rayon
                found_in_plan = True
                break

        if resource_profiles and profile_idx < len(resource_profiles):
            rp = resource_profiles[profile_idx]
            if rp.get("cpu_limit_cpus") is not None:
                lines.append(f'    cpus: "{rp["cpu_limit_cpus"]}"')
            if rp.get("memory_limit"):
                lines.append(f'    mem_limit: "{rp["memory_limit"]}"')
            if rp.get("memory_swap"):
                lines.append(f'    memswap_limit: "{rp["memory_swap"]}"')
            if not found_in_plan and rp.get("rayon_num_threads", 0) > 0:
                result["rayon_num_threads"] = rp["rayon_num_threads"]

    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)

    compose_out = Path(args.compose_out)
    workers_out = Path(args.workers_out)
    workers_host_out = Path(args.workers_host_out)
    layout_out = Path(args.worker_layout_out)

    resource_experiment = getattr(args, "resource_experiment", "none")
    affinity_plan = load_affinity_plan(args) if resource_experiment != "none" else None
    resource_profiles = load_resource_profiles(args) if resource_experiment != "none" else None

    if resource_experiment != "none" and affinity_plan:
        print(f"[affinity] Loaded affinity plan with {len(affinity_plan.get('profiled_assignments',[]))} profiled assignments", flush=True)

    if args.worker_layout_mode == "hybrid":
        layout_info = compute_hybrid_layout(
            args.workers,
            args.singleton_min_count,
            args.singleton_fraction,
            args.packed_clients_per_container,
        )

        singleton_ids = select_singleton_ids(
            args.workers,
            layout_info["singleton_count"],
            args.singleton_selection_seed,
            args.singleton_selection_strategy,
        )

        all_ids = [worker_id(i) for i in range(1, args.workers + 1)]
        singleton_set = set(singleton_ids)
        packed_client_ids = [cid for cid in all_ids if cid not in singleton_set]

        clients, physical_workers = build_hybrid_layout(
            args, singleton_ids, packed_client_ids, layout_info,
        )

        print(f"[layout] logical_workers={args.workers} "
              f"singleton_measured_clients={layout_info['singleton_count']} "
              f"packed_virtual_clients={layout_info['packed_client_count']} "
              f"packed_containers={layout_info['packed_container_count']} "
              f"physical_worker_containers={layout_info['physical_worker_count']} "
              f"packed_clients_per_container={args.packed_clients_per_container} "
              f"profile_policy=singletons_only")
    else:
        clients, physical_workers = build_legacy_layout(args)
        layout_info = {
            "singleton_count": args.workers,
            "packed_client_count": 0,
            "packed_container_count": 0,
            "physical_worker_count": args.workers,
        }

    layout_json = generate_worker_layout_json(args, clients, physical_workers)
    write_text(layout_out, json.dumps(layout_json, indent=2) + "\n")

    compose_text = generate_compose_text(
        args, physical_workers,
        affinity_plan=affinity_plan,
        resource_profiles=resource_profiles,
        resource_experiment=resource_experiment,
    )
    write_text(compose_out, compose_text)
    write_text(workers_out, generate_workers_internal(args, clients))
    write_text(workers_host_out, generate_workers_host(args, physical_workers))

    print(f"Wrote {compose_out}")
    print(f"Wrote {workers_out}")
    print(f"Wrote {workers_host_out}")
    print(f"Wrote {layout_out}")
    print("")
    print("What you generated:")
    print(f"- Compose file with {len(physical_workers)} physical worker services "
          f"({args.workers} logical clients)")
    print(f"- Layout mode: {args.worker_layout_mode}")
    if args.worker_layout_mode == "hybrid":
        print(f"- Singleton measured clients: {layout_info['singleton_count']}")
        print(f"- Packed virtual clients: {layout_info['packed_client_count']}")
        print(f"- Packed containers: {layout_info['packed_container_count']}")
    profile = resource_profile_for(args)
    if profile:
        print(
            "- Singleton resource envelope: "
            f"{profile} (host-specific proxy, not hardware emulation)"
        )
    if getattr(args, "failure_experiment", False):
        grid_cells = len(FAILURE_EXPERIMENT_CPU_CAPS) * len(FAILURE_EXPERIMENT_RAM_CAPS)
        print(
            f"- Failure-experiment grid: {len(FAILURE_EXPERIMENT_CPU_CAPS)} CPU caps "
            f"x {len(FAILURE_EXPERIMENT_RAM_CAPS)} RAM caps = {grid_cells} cells, "
            f"seed={args.singleton_selection_seed}"
        )
    print(f"- Workers distributed across {args.bridge_count} Docker bridge network(s)")
    print("- workers.txt for an in-network runner")
    print("- workers.host.txt for the host-runner workflow")

    if args.publish_workers:
        print("- worker ports are published on the host")
    else:
        print("- worker ports are internal-only")

    if args.include_runner:
        print("- runner service included")

    if args.include_netcheck:
        print("- netcheck monitor service included")


if __name__ == "__main__":
    main()
