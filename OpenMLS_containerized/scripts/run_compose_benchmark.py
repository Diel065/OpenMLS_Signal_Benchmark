#!/usr/bin/env python3
from __future__ import annotations

import os
import argparse
import copy
import csv
import datetime as dt
import glob
import hashlib
import json
import math
import platform
import re
import shutil
import shlex
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
MEMORY_RE = re.compile(r"^(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>[bkmgt]?b?)?$", re.IGNORECASE)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="One-command local containerized MLS benchmark runner."
    )

    p.add_argument("--workers", type=int, required=True, help="Number of logical worker clients")
    p.add_argument("--run-id", default=None, help="Optional explicit run id")
    p.add_argument("--scenario", default="http-staircase-compose", help="Scenario label")
    p.add_argument("--scenario-seed", type=int, default=1, help="Scenario randomization seed")
    p.add_argument("--output-dir", default="benchmark_output", help="Base output directory")

    p.add_argument("--min-size", type=int, default=2)
    p.add_argument("--max-size", type=int, default=None)
    p.add_argument("--step-size", default="1", help="Integer step size or uniform [min,max] range")
    p.add_argument(
        "--plateau-order",
        choices=("staircase", "ascending", "randomized"),
        default="staircase",
        help="Order target group-size plateaus as up/down staircase, ascending-only, or randomized",
    )
    p.add_argument("--roundtrips", type=int, default=1)

    p.add_argument("--update-rounds", type=int, default=2)
    p.add_argument("--max-update-samples-per-plateau", type=int, default=16)

    p.add_argument("--app-rounds", type=int, default=2)
    p.add_argument("--max-app-samples-per-payload", type=int, default=16)

    p.add_argument(
        "--payload-sizes",
        default="32,256,1024",
        help="Comma-separated payload sizes or uniform per-message [min,max] range",
    )

    p.add_argument("--base-worker-port", type=int, default=8081)
    p.add_argument("--ds-port", type=int, default=3000)
    p.add_argument("--relay-port", type=int, default=4000)

    p.add_argument(
        "--bridge-count",
        type=int,
        default=1,
        help=(
            "Number of Docker bridge networks to distribute workers across. "
            "Passed through to scripts/generate_compose.py."
        ),
    )

    p.add_argument("--health-timeout-seconds", type=int, default=90)
    p.add_argument("--health-poll-seconds", type=float, default=0.5)

    p.add_argument(
        "--post-startup-settle-seconds",
        type=float,
        default=0.0,
        help=(
            "Sleep this many seconds after all containers are started, "
            "before starting health checks / runner. Useful for large Docker stacks."
        ),
    )

    p.add_argument(
        "--worker-health-timeout-seconds",
        type=int,
        default=300,
        help=(
            "How long the in-network benchmark runner should wait for all workers "
            "to become healthy before starting MLS logic."
        ),
    )

    p.add_argument(
        "--worker-health-poll-ms",
        type=int,
        default=250,
        help="Polling interval in milliseconds for in-network worker health checks.",
    )
    p.add_argument(
        "--max-fanout-parallelism",
        type=int,
        default=0,
        help=(
            "Maximum bounded parallelism for runner-to-worker fan-out. "
            "0 lets the Rust runner choose its conservative Docker-network default."
        ),
    )
    p.add_argument(
        "--fanout-adaptive",
        action="store_true",
        help=(
            "Enable adaptive runner fan-out throttling. Starts at 32 and reduces "
            "parallelism on latency/error spikes."
        ),
    )
    p.add_argument(
        "--no-fanout-adaptive",
        action="store_true",
        help="Disable adaptive fan-out even for large Docker worker counts.",
    )
    p.add_argument(
        "--min-fanout-parallelism",
        type=int,
        default=0,
        help="Minimum adaptive runner fan-out parallelism. 0 uses the Rust runner default.",
    )
    p.add_argument(
        "--fanout-error-rate-threshold",
        type=float,
        default=0.0,
        help="Adaptive fan-out error-rate threshold. 0 uses the Rust runner default.",
    )
    p.add_argument(
        "--fanout-p95-threshold-ms",
        type=int,
        default=0,
        help="Adaptive fan-out p95 latency threshold in milliseconds. 0 uses the Rust runner default.",
    )
    p.add_argument(
        "--http-pool-max-idle-per-host",
        type=int,
        default=0,
        help="Maximum idle pooled HTTP connections per host for the Rust runner.",
    )
    p.add_argument(
        "--runner-http-connect-timeout-ms",
        type=int,
        default=2000,
        help="OPENMLS_RUNNER_HTTP_CONNECT_TIMEOUT_MS for runner->worker clients.",
    )
    p.add_argument(
        "--runner-http-request-timeout-ms",
        type=int,
        default=60000,
        help="OPENMLS_RUNNER_HTTP_REQUEST_TIMEOUT_MS for runner->worker clients.",
    )
    p.add_argument(
        "--process-pending-fanout",
        action="store_true",
        help="Use worker ProcessPending for receive-commit fan-out.",
    )
    p.add_argument(
        "--ds-delivery-mode",
        choices=("per-recipient", "group-log"),
        default="group-log",
        help="DS commit delivery mode exposed as OPENMLS_DS_DELIVERY_MODE.",
    )
    p.add_argument(
        "--worker-http-pool-max-idle-per-host",
        type=int,
        default=32,
        help="OPENMLS_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST for worker->DS/relay clients.",
    )
    p.add_argument(
        "--worker-http-connect-timeout-ms",
        type=int,
        default=5000,
        help="OPENMLS_WORKER_HTTP_CONNECT_TIMEOUT_MS for worker->DS/relay clients.",
    )
    p.add_argument(
        "--worker-http-request-timeout-ms",
        type=int,
        default=30000,
        help="OPENMLS_WORKER_HTTP_REQUEST_TIMEOUT_MS for worker->DS/relay clients.",
    )
    p.add_argument(
        "--worker-outbound-http-permits",
        type=int,
        default=32,
        help="OPENMLS_WORKER_OUTBOUND_HTTP_PERMITS per worker process.",
    )
    p.add_argument(
        "--host-health-parallelism",
        type=int,
        default=64,
        help="Maximum parallel host-side worker health probes when not using --runner-in-docker.",
    )

    p.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Only check DS/relay/worker reachability from inside the Docker network, "
            "then exit without running the MLS benchmark. No events.csv is expected."
        ),
    )

    p.add_argument(
        "--startup-batch-size",
        type=int,
        default=0,
        help=(
            "Start worker containers in batches of this size. "
            "0 means use one normal docker compose up for all services."
        ),
    )

    p.add_argument(
        "--startup-batch-sleep-seconds",
        type=float,
        default=0.25,
        help="Sleep this many seconds between worker startup batches.",
    )

    p.add_argument(
        "--compose-parallel-limit",
        type=int,
        default=None,
        help=(
            "Set COMPOSE_PARALLEL_LIMIT for docker compose operations. "
            "Useful for avoiding Docker daemon overload with many containers."
        ),
    )

    p.add_argument(
        "--compose-down-timeout-seconds",
        type=int,
        default=1,
        help=(
            "Shutdown timeout for docker compose down. "
            "Use a small value for benchmark containers to avoid long teardown waits."
        ),
    )

    p.add_argument(
        "--teardown-batch-size",
        type=int,
        default=0,
        help=(
            "Stop/remove worker containers in batches of this size before final compose down. "
            "0 means use normal docker compose down for the whole stack."
        ),
    )

    p.add_argument(
        "--teardown-batch-sleep-seconds",
        type=float,
        default=0.25,
        help="Sleep this many seconds between teardown batches.",
    )

    p.add_argument(
        "--runner-in-docker",
        action="store_true",
        help=(
            "Run benchmark_runner_http_staircase inside the Docker network. "
            "This allows workers to avoid publishing host ports."
        ),
    )
    p.add_argument(
        "--include-netcheck",
        action="store_true",
        help=(
            "Include the continuous diagnostic netcheck service in the generated "
            "Compose stack. Default: disabled."
        ),
    )

    p.add_argument(
        "--build-images",
        action="store_true",
        help="Build Docker images before running the benchmark",
    )
    p.add_argument(
        "--build-external-binaries",
        action="store_true",
        help="Cross-compile worker binaries for external devices via cargo --profile minsize",
    )
    p.add_argument(
        "--keep-stack-up",
        action="store_true",
        help="Do not run docker compose down at the end",
    )
    p.add_argument(
        "--keep-stack-up-on-failure",
        action="store_true",
        help="Do not run docker compose down when startup or runner execution fails",
    )
    p.add_argument(
        "--keep-generated-files",
        action="store_true",
        help="Keep temporary generated compose/worker files at repo root",
    )
    p.add_argument(
        "--force-cleanup-mls-ports",
        action="store_true",
        help="Before starting, forcibly remove existing Docker containers with names beginning with 'mls-'",
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
        "--profile-only-singletons",
        action="store_true",
        default=True,
        help="Only profile singleton measured clients (default: true)",
    )
    p.add_argument(
        "--disable-container-profiling",
        action="store_true",
        help="Disable profiling for Docker container workers; external devices may still be profiled.",
    )
    p.add_argument(
        "--external-coverage-lane",
        action="store_true",
        help=(
            "Guarantee active external devices are sampled as application, update, "
            "add/welcome, and remove actors while keeping packed clients unprofiled. "
            "This is enabled automatically when external devices are requested."
        ),
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
            "Docker CPU envelope for all containerized singleton workers, e.g. 0.25. "
            "Unset means no singleton CPU limit."
        ),
    )
    p.add_argument(
        "--singleton-memory",
        default=None,
        help=(
            "Docker memory envelope for all containerized singleton workers, e.g. 128m. "
            "Unset means no singleton memory limit."
        ),
    )
    p.add_argument(
        "--singleton-memory-swap",
        default=None,
        help=(
            "Docker memory+swap envelope for singleton workers. "
            "If --singleton-memory is set and this is omitted, it defaults to the memory value."
        ),
    )
    p.add_argument(
        "--singleton-pids-limit",
        type=int,
        default=None,
        help="Optional Docker pids_limit for all containerized singleton workers.",
    )
    p.add_argument(
        "--resource-monitor-interval-ms",
        type=int,
        default=500,
        help="Resource-monitor sample interval for constrained singleton containers.",
    )
    p.add_argument(
        "--no-resource-monitor",
        action="store_true",
        help="Disable resource_samples.jsonl/resource_summary.csv collection.",
    )
    p.add_argument(
        "--failure-experiment",
        action="store_true",
        dest="failure_experiment",
        help=(
            "Enable failure-experiment mode: assigns a reproducible randomized grid "
            "of CPU/RAM caps to each profiled singleton worker (packed workers "
            "remain unconstrained). Overrides --singleton-cpus and --singleton-memory."
        ),
    )

    # ── Resource experiment flags ───────────────────────────────────
    p.add_argument(
        "--resource-experiment",
        choices=[
            "none",
            "ram-sweep-singleton",
            "cpu-matrix-singleton",
            "embedded-budget-singleton",
            "ram-app-heap-sweep",
            "cpu-quota-sweep",
        ],
        default="none",
        help="Resource experiment mode for simulated IoT container profiling",
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
        "--embedded-heap-budgets",
        default="32k,64k,128k,256k,512k,1m,2m",
        help="Comma-separated synthetic application heap budgets for embedded-budget mode",
    )
    p.add_argument(
        "--embedded-cpu-fractions",
        default="1.00,0.50,0.25,0.10,0.05",
        help="Comma-separated Docker CPU fractions for embedded-budget mode",
    )
    p.add_argument(
        "--embedded-cpu-cores",
        default="1",
        help="Comma-separated Docker CPU core counts for embedded-budget mode",
    )
    p.add_argument(
        "--embedded-docker-memory",
        default="256m",
        help="Safe Docker memory limit used while Rust enforces app heap budget",
    )
    p.add_argument(
        "--cpu-sweep-fractions",
        default="1.00,0.75,0.50,0.25,0.10,0.05,0.04,0.03,0.02,0.01",
        help=(
            "Comma-separated 10 Docker CPU fractions for cpu-quota-sweep. "
            "Defaults stay within Docker CFS cpu_period/cpu_quota limits. "
            "All fractions must be >= 0.01 (Docker hard-quota floor)."
        ),
    )
    p.add_argument(
        "--cpu-affinity-mode",
        choices=["none", "profiled-nor-background"],
        default="none",
        help="CPU affinity planning mode",
    )
    p.add_argument(
        "--cpu-affinity-sample-seconds",
        type=float,
        default=20.0,
        help="Duration in seconds for CPU load sampling (0 or 1 for smoke tests)",
    )
    p.add_argument(
        "--reserve-smt-siblings",
        action="store_true",
        help="Reserve SMT/hyperthread siblings of profiled CPUs",
    )
    p.add_argument(
        "--affinity-plan-file",
        default=None,
        help="Path to a pre-computed cpu_affinity_plan.json",
    )
    p.add_argument(
        "--resource-profiles-file",
        default=None,
        help="Path to a pre-computed resource_profiles.json file",
    )
    p.add_argument(
        "--resource-experiment-output-dir",
        default=None,
        help="Directory to write resource experiment sidecar files",
    )
    p.add_argument(
        "--resource-profile-index",
        type=int,
        default=None,
        help="Select a single resource profile by zero-based index for threshold experiments",
    )
    p.add_argument(
        "--resource-profile-id",
        default=None,
        help="Select a single resource profile by resource_profile_id for threshold experiments",
    )
    p.add_argument(
        "--resource-failure-policy",
        choices=["stop-on-profiled-failure", "remove-and-continue"],
        default="stop-on-profiled-failure",
        help="Action when the profiled singleton fails (default: stop-on-profiled-failure)",
    )
    p.add_argument(
        "--resource-output-validation",
        action="store_true",
        dest="resource_output_validation",
        default=True,
        help="Validate resource experiment outputs after run (default: enabled)",
    )
    p.add_argument(
        "--no-resource-output-validation",
        action="store_false",
        dest="resource_output_validation",
        help="Skip resource experiment output validation after run.",
    )

    # External device flags
    p.add_argument(
        "--devices-file",
        default=None,
        help="Path to YAML config file for external real devices",
    )
    p.add_argument(
        "--enable-external-devices",
        action="store_true",
        help="Enable external device orchestration (requires --devices-file)",
    )
    p.add_argument(
        "--external-device",
        action="append",
        default=[],
        dest="external_device_ids",
        help="Specific external device ID(s) to enable (repeatable). Default: all enabled devices.",
    )
    p.add_argument(
        "--no-aggregate",
        action="store_true",
        help="Pass --no-aggregate to the Rust benchmark runner (for post-run aggregation with external devices)",
    )
    p.add_argument(
        "--wipe-run-dir",
        action="store_true",
        help="Wipe local benchmark_output/<run_id> before starting",
    )
    p.add_argument(
        "--wipe-device-run-dirs",
        action="store_true",
        help="Wipe remote device run directories before starting",
    )
    p.add_argument(
        "--no-device-stop-after-run",
        action="store_true",
        help="Do not stop external device workers after the benchmark run",
    )

    return p


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def timestamped_run_id(worker_count: int) -> str:
    now = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"compose-{worker_count}w-{now}"


def sanitize_project_name(run_id: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", run_id).strip("-_").lower()
    if not cleaned:
        cleaned = "mls-benchmark"
    return f"mls-{cleaned}"[:63]


def validate_run_id(run_id: str) -> None:
    if not run_id:
        raise ValueError("Run ID must not be empty")
    if run_id in ("/", ".", ".."):
        raise ValueError(f"Run ID must not be '{run_id}'")
    if "/" in run_id:
        raise ValueError("Run ID must not contain '/'")
    if not RUN_ID_RE.match(run_id):
        raise ValueError(
            f"Run ID must only contain [A-Za-z0-9._-], got '{run_id}'"
        )


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
        raise SystemExit(f"Unsupported memory unit in '{value}'. Use b, k, m, g, or t.")

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


def singleton_resource_envelope(args: argparse.Namespace) -> dict:
    is_failure_experiment = getattr(args, "failure_experiment", False)
    enabled = is_failure_experiment or any(
        value is not None
        for value in (
            args.singleton_cpus_float,
            args.singleton_memory,
            args.singleton_memory_swap,
            args.singleton_pids_limit,
        )
    )
    profile_parts = []
    if is_failure_experiment:
        profile_parts.append("failure-experiment-multi-envelope")
    if args.singleton_cpus is not None:
        profile_parts.append(f"cpus-{args.singleton_cpus}")
    if args.singleton_memory is not None:
        profile_parts.append(f"memory-{args.singleton_memory}")
    if args.singleton_memory_swap is not None:
        profile_parts.append(f"swap-{args.singleton_memory_swap}")
    if args.singleton_pids_limit is not None:
        profile_parts.append(f"pids-{args.singleton_pids_limit}")
    resource_profile = (
        "singleton-resource-envelope_" + "_".join(profile_parts)
        if profile_parts
        else ""
    )
    return {
        "enabled": enabled,
        "failure_experiment": is_failure_experiment,
        "cpus": args.singleton_cpus_float,
        "memory": args.singleton_memory,
        "memory_bytes": args.singleton_memory_bytes,
        "memory_swap": args.singleton_memory_swap,
        "memory_swap_bytes": args.singleton_memory_swap_bytes,
        "memory_swap_defaulted_to_memory": args.singleton_memory_swap_defaulted,
        "pids_limit": args.singleton_pids_limit,
        "applies_to": "all_containerized_singletons",
        "resource_profile": resource_profile,
        "scientific_interpretation": (
            "host-specific reproducible resource envelope; not exact hardware emulation"
        ),
    }


def output_root_for(root: Path, output_dir_name: str) -> Path:
    output_root = Path(output_dir_name)
    if not output_root.is_absolute():
        output_root = root / output_root
    return output_root


def safe_wipe_run_dir(run_dir: Path, output_root: Path, run_id: str) -> None:
    validate_run_id(run_id)
    resolved_output_root = output_root.resolve()
    resolved_run_dir = run_dir.resolve(strict=False)
    if resolved_run_dir.parent != resolved_output_root or resolved_run_dir.name != run_id:
        raise RuntimeError(f"Refusing to wipe unsafe run directory: {run_dir}")
    if run_dir.exists():
        print(f"[cleanup] wiping local run directory: {run_dir}", flush=True)
        shutil.rmtree(run_dir)


def insert_after_leader(existing: list, additions: list) -> list:
    if not additions:
        return existing
    if not existing:
        return list(additions)
    return existing[:1] + list(additions) + existing[1:]


def run_cmd(
        cmd: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        check: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd), env=env, check=check)


def tee_subprocess_output(
        cmd: list[str],
        *,
        cwd: Path,
        output_path: Path,
        env: dict[str, str] | None = None,
) -> int:
    with output_path.open("w", encoding="utf-8") as out_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="")
            out_file.write(line)

        return proc.wait()


def wait_for_health(url: str, timeout_seconds: int, poll_seconds: float) -> None:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = resp.read().decode("utf-8", errors="replace").strip()
                if 200 <= resp.status < 300 and body == "ok":
                    return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass

        time.sleep(poll_seconds)

    raise RuntimeError(f"Timed out waiting for health endpoint: {url}")


def wait_for_workers_health_parallel(
        worker_lines: list[str],
        timeout_seconds: int,
        poll_seconds: float,
        max_parallelism: int,
) -> None:
    if not worker_lines:
        return

    parsed_workers: list[tuple[str, str]] = []
    for line in worker_lines:
        worker_id, worker_url = line.split("=", 1)
        parsed_workers.append((worker_id, worker_url))

    parallelism = max(1, min(max_parallelism, len(parsed_workers)))
    print(
        f"[health] waiting for {len(parsed_workers)} workers with host parallelism={parallelism}",
        flush=True,
    )

    def probe(worker: tuple[str, str]) -> str:
        worker_id, worker_url = worker
        wait_for_health(f"{worker_url}/health", timeout_seconds, poll_seconds)
        return worker_id

    completed = 0
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = [executor.submit(probe, worker) for worker in parsed_workers]
        for future in as_completed(futures):
            worker_id = future.result()
            completed += 1
            if completed <= 10 or completed == len(parsed_workers) or completed % 100 == 0:
                print(
                    f"[health] worker {worker_id} ok ({completed}/{len(parsed_workers)})",
                    flush=True,
                )


def read_worker_lines(path: Path) -> list[str]:
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return lines


def build_host_runner_layout(layout_data: dict, worker_lines: list[str]) -> dict:
    """Return a layout whose command endpoints are reachable from the host."""
    worker_urls: dict[str, str] = {}
    for line in worker_lines:
        worker_id, worker_url = line.split("=", 1)
        worker_urls[worker_id] = worker_url.rstrip("/")

    host_layout = copy.deepcopy(layout_data)
    physical_host_urls: dict[str, str] = {}
    for client in host_layout.get("clients", []):
        client_id = str(client.get("client_id", ""))
        base_url = worker_urls.get(client_id)
        if not base_url:
            continue

        if client.get("execution_backend") == "docker_container":
            command_url = f"{base_url}/client/{client_id}"
        else:
            command_url = base_url

        client["command_url"] = command_url
        client["health_url"] = f"{command_url}/health"
        physical_worker_id = str(client.get("physical_worker_id", ""))
        if physical_worker_id:
            physical_host_urls.setdefault(physical_worker_id, base_url)

    for worker in host_layout.get("physical_workers", []):
        physical_worker_id = str(worker.get("physical_worker_id", ""))
        if physical_worker_id in physical_host_urls:
            worker["base_url"] = physical_host_urls[physical_worker_id]

    return host_layout


def validate_artifacts(run_dir: Path, layout_mode: str, require_aggregate: bool = True) -> None:
    csv_path = run_dir / "events.csv"
    layout_path = run_dir / "worker_layout.json"

    if layout_mode == "hybrid":
        if not layout_path.exists():
            raise RuntimeError(f"Missing worker_layout.json in hybrid mode: {layout_path}")

    if require_aggregate:
        if not csv_path.exists():
            raise RuntimeError(f"Missing aggregated CSV: {csv_path}")
        if csv_path.stat().st_size == 0:
            raise RuntimeError(f"Aggregated CSV is empty: {csv_path}")

    jsonl_files = sorted(run_dir.glob("client-*.jsonl"))
    if not jsonl_files:
        raise RuntimeError(f"No per-worker JSONL files found in {run_dir}")

    non_empty_jsonl = [p for p in jsonl_files if p.stat().st_size > 0]
    if not non_empty_jsonl:
        raise RuntimeError(f"All per-worker JSONL files are empty in {run_dir}")


def write_integrated_aggregation_manifest(run_dir: Path, run_id: str) -> None:
    manifest_path = run_dir / "aggregation_manifest.json"
    if manifest_path.exists():
        return

    events_path = run_dir / "events.csv"
    with events_path.open("r", newline="", encoding="utf-8") as handle:
        events_written = sum(1 for _ in csv.reader(handle)) - 1
    events_written = max(0, events_written)

    jsonl_files = sorted(run_dir.glob("client-*.jsonl"))
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest = {
        "run_id": run_id,
        "started_at": timestamp,
        "finished_at": timestamp,
        "aggregation_mode": "runner_integrated",
        "aggregation_status": "complete_success",
        "events_written": events_written,
        "input_files_expected": len(jsonl_files),
        "input_files_found": len(jsonl_files),
        "input_files_missing": [],
        "malformed_records": [],
        "truncated_records": [],
        "runner_events_included": any(
            (run_dir / filename).exists()
            for filename in ("runner-events.jsonl", "runner_events.jsonl")
        ),
        "output_path": str(events_path),
        "error_message": None,
        "temporary_file_retained": None,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def run_strict_output_validation(root: Path, run_dir: Path) -> None:
    validator = root / "scripts" / "validate_benchmark_outputs.py"
    result = subprocess.run(
        [sys.executable, str(validator), str(run_dir)],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            "Strict OpenMLS publication validation failed after the run.\n" + details
        )


def copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        shutil.copy2(src, dst)


def port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False


def required_host_ports(args: argparse.Namespace) -> list[int]:
    ports = [args.ds_port, args.relay_port]

    if not args.runner_in_docker:
        if args.worker_layout_mode == "hybrid":
            import math
            singleton_count = min(
                args.workers,
                max(args.singleton_min_count, math.ceil(args.workers * args.singleton_fraction)),
            )
            packed_client_count = args.workers - singleton_count
            packed_container_count = math.ceil(packed_client_count / args.packed_clients_per_container) if packed_client_count > 0 else 0
            physical_count = singleton_count + packed_container_count
        else:
            physical_count = args.workers
        ports.extend(args.base_worker_port + i for i in range(physical_count))

    return ports


def check_required_ports(args: argparse.Namespace) -> None:
    busy = [p for p in required_host_ports(args) if not port_is_free(p)]
    if not busy:
        return

    busy_text = ", ".join(str(p) for p in busy)
    raise RuntimeError(
        "One or more required host ports are already in use: "
        f"{busy_text}\n"
        "Stop the previous benchmark stack, or choose different ports.\n"
        "You can also rerun with --force-cleanup-mls-ports to remove old mls-* Docker containers."
    )


def docker_cleanup_mls_containers(root: Path) -> None:
    result = subprocess.run(
        ["docker", "ps", "-aq", "--filter", "name=mls-"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if container_ids:
        print(f"[cleanup] removing {len(container_ids)} old mls-* containers")

        for batch in chunks(container_ids, 64):
            subprocess.run(
                ["docker", "rm", "-f", *batch],
                cwd=str(root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )

    network_result = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    network_names = [
        line.strip()
        for line in network_result.stdout.splitlines()
        if line.strip().startswith("mls-")
    ]

    if network_names:
        print(f"[cleanup] removing {len(network_names)} old mls-* networks")

        for batch in chunks(network_names, 32):
            subprocess.run(
                ["docker", "network", "rm", *batch],
                cwd=str(root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )


def write_compose_logs(root: Path, compose_file: Path, dest: Path, append: bool = False) -> None:
    mode = "a" if append else "w"
    with dest.open(mode, encoding="utf-8") as f:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "logs", "--no-color"],
            cwd=str(root),
            stdout=f,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )


def run_capture(
        root: Path,
        dest: Path,
        cmd: list[str],
        *,
        env: dict[str, str] | None = None,
) -> None:
    with dest.open("w", encoding="utf-8") as f:
        f.write("$ " + " ".join(cmd) + "\n\n")
        subprocess.run(
            cmd,
            cwd=str(root),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )


def write_url_capture(dest: Path, url: str, timeout_seconds: float = 3.0) -> None:
    with dest.open("w", encoding="utf-8") as f:
        f.write(f"$ GET {url}\n\n")
        try:
            with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
                f.write(response.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, TimeoutError, OSError) as err:
            f.write(f"ERROR: {err}\n")


def extract_failed_worker_ids(terminal_output_path: Path) -> list[str]:
    if not terminal_output_path.exists():
        return []

    text = terminal_output_path.read_text(encoding="utf-8", errors="replace")
    ids: list[str] = []
    seen: set[str] = set()

    def add(worker_id: str) -> None:
        if worker_id not in seen:
            seen.add(worker_id)
            ids.append(worker_id)

    tail = text[-250_000:]
    priority_patterns = [
        r"failures=\[([^\]]+)\]",
        r"runner\.worker_command failed: worker=([0-9]{5})",
        r"Worker ([0-9]{5}) error:",
        r"client=([0-9]{5}) url=http://(?:ds|relay)",
    ]

    for pattern in priority_patterns:
        for match in re.finditer(pattern, tail, flags=re.DOTALL):
            if match.lastindex == 1 and "[" not in match.group(1) and ":" not in match.group(1):
                add(match.group(1))
                continue

            for worker_id in re.findall(r"\b([0-9]{5})\b", match.group(0)):
                add(worker_id)

    fallback_patterns = [
        r"worker=([0-9]{5})",
        r"Worker ([0-9]{5})",
        r"worker-([0-9]{5})",
        r"client=([0-9]{5})",
    ]
    for pattern in fallback_patterns:
        for match in re.finditer(pattern, text):
            worker_id = match.group(1)
            add(worker_id)

    return ids[:50]


def compose_service_logs(
        root: Path,
        compose_file: Path,
        dest: Path,
        services: list[str],
        env: dict[str, str] | None,
) -> None:
    with dest.open("w", encoding="utf-8") as f:
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "logs",
                "--no-color",
                *services,
            ],
            cwd=str(root),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )


def compose_container_id(
        root: Path,
        compose_file: Path,
        service: str,
        env: dict[str, str] | None,
) -> str | None:
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "ps", "-q", "--all", service],
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )

    container_id = result.stdout.strip()
    return container_id or None


def project_network_names(root: Path, project_name: str) -> list[str]:
    result = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )

    prefixes = (f"{project_name}_", f"{project_name}-")
    return [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip().startswith(prefixes)
    ]


def write_netcheck_targets(run_dir: Path, worker_ids: list[str]) -> None:
    if not worker_ids:
        return

    target_path = run_dir / "netcheck_targets.txt"
    target_path.write_text("\n".join(worker_ids) + "\n", encoding="utf-8")


def read_int_file(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def warn_if_neighbor_cache_tight(layout_path: Path, service_count: int = 3) -> None:
    try:
        layout = json.loads(layout_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    physical_workers = int(layout.get("physical_worker_count") or 0)
    gc_thresh3 = read_int_file(Path("/proc/sys/net/ipv4/neigh/default/gc_thresh3"))
    if physical_workers <= 0 or not gc_thresh3:
        return

    estimated_neighbors = physical_workers + service_count
    if estimated_neighbors >= int(gc_thresh3 * 0.8):
        print(
            "[preflight-warning] physical containers plus services are close to "
            f"neighbor-cache gc_thresh3: estimated={estimated_neighbors} "
            f"gc_thresh3={gc_thresh3}. Hybrid runs usually stay below this, but "
            "singleton or high-physical-container runs may need net.ipv4.neigh.* "
            "and nf_conntrack tuning.",
            flush=True,
        )


def capture_failure_evidence(
    *,
    root: Path,
    compose_file: Path,
    run_dir: Path,
    compose_env: dict[str, str] | None,
    project_name: str,
    profiled_container_names: list[str] | None = None,
) -> dict | None:
    """Capture Docker inspect and cgroup state for profiled containers before cleanup.

    Must be called BEFORE docker compose down.
    Writes failure_evidence.json and returns the evidence dict.
    """
    import csv as csv_mod
    evidence_entries: list[dict] = []
    container_names = list(profiled_container_names or [])

    if not container_names:
        compose_ps = run_capture_to_str(
            root,
            ["docker", "compose", "-f", str(compose_file), "ps", "-q"],
            env=compose_env,
        )
        if compose_ps:
            container_names = [line.strip() for line in compose_ps.splitlines() if line.strip()]

    for name in container_names:
        try:
            entry = _capture_single_container(root, compose_file, name, compose_env)
            if entry:
                evidence_entries.append(entry)
        except Exception as exc:
            evidence_entries.append({
                "container_name": name,
                "capture_error": f"{type(exc).__name__}: {exc}",
            })

    if not evidence_entries:
        return None

    payload = {
        "captured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "container_count": len(evidence_entries),
        "containers": evidence_entries,
    }
    (run_dir / "failure_evidence.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"[evidence] captured docker/cgroup state for {len(evidence_entries)} container(s)", flush=True)
    return payload


def _capture_single_container(
    root: Path,
    compose_file: Path,
    container_name: str,
    compose_env: dict[str, str] | None,
) -> dict | None:
    """Capture docker inspect and cgroup state for a single container."""
    container_id = compose_container_id(root, compose_file, container_name, compose_env)
    if not container_id:
        return None

    entry: dict = {
        "container_name": container_name,
        "container_id": container_id,
    }

    pid = 0
    try:
        inspect_raw = subprocess.run(
            ["docker", "inspect", container_id],
            cwd=str(root), capture_output=True, text=True, timeout=10,
            env=compose_env,
        )
        if inspect_raw.returncode == 0 and inspect_raw.stdout.strip():
            inspect_data = json.loads(inspect_raw.stdout)
            if inspect_data and isinstance(inspect_data, list):
                insp = inspect_data[0]
                state = insp.get("State", {})
                hc = insp.get("HostConfig", {})
                entry["docker_state"] = {
                    "status": state.get("Status", ""),
                    "running": state.get("Running"),
                    "exit_code": state.get("ExitCode"),
                    "oom_killed": state.get("OOMKilled"),
                    "started_at": state.get("StartedAt", ""),
                    "finished_at": state.get("FinishedAt", ""),
                    "error": state.get("Error", ""),
                }
                entry["docker_config"] = {
                    "memory": hc.get("Memory", 0),
                    "memory_swap": hc.get("MemorySwap", 0),
                    "nano_cpus": hc.get("NanoCpus", 0),
                    "cpu_quota": hc.get("CpuQuota", 0),
                    "cpu_period": hc.get("CpuPeriod", 0),
                    "cpuset_cpus": hc.get("CpusetCpus", ""),
                }
                pid = state.get("Pid", 0)
    except Exception:
        pid = 0

    if pid and pid > 0:
        try:
            cgroup_paths = cgroup_paths_for_pid(pid)
            entry["cgroup"] = {}
            for controller, cgroup_file, key in [
                ("memory", "memory.current", "memory_current"),
                ("memory", "memory.max", "memory_max"),
                ("memory", "memory.events", "memory_events"),
                ("memory", "memory.peak", "memory_peak"),
                ("cpu", "cpu.stat", "cpu_stat"),
                ("cpu", "cpu.max", "cpu_max"),
                ("cpu", "cpuset.cpus.effective", "cpuset_cpus_effective"),
                ("pids", "pids.current", "pids_current"),
                ("pids", "pids.events", "pids_events"),
            ]:
                try:
                    base = cgroup_paths.get(controller) or cgroup_paths.get("unified")
                    filepath = base / cgroup_file if base else None
                    if filepath and filepath.exists():
                        raw = filepath.read_text().strip()
                        entry["cgroup"][key] = raw
                except Exception:
                    pass
        except Exception as exc:
            entry["cgroup_capture_error"] = str(exc)

    try:
        log_result = subprocess.run(
            ["docker", "logs", "--tail", "50", container_id],
            cwd=str(root), capture_output=True, text=True, timeout=10,
            env=compose_env,
        )
        if log_result.returncode == 0:
            entry["log_tail"] = log_result.stdout[-4000:] if len(log_result.stdout) > 4000 else log_result.stdout
    except Exception:
        pass

    return entry


def run_capture_to_str(root: Path, cmd: list[str], *, env: dict[str, str] | None = None) -> str | None:
    """Run a command and return stdout as string, or None on failure."""
    try:
        result = subprocess.run(
            cmd, cwd=str(root), capture_output=True, text=True, timeout=15,
            env=env,
        )
        if result.returncode == 0:
            return result.stdout
    except Exception:
        pass
    return None


def collect_failure_diagnostics(
        *,
        root: Path,
        compose_file: Path,
        run_dir: Path,
        args: argparse.Namespace,
        compose_env: dict[str, str] | None,
        project_name: str,
        terminal_output_path: Path,
) -> None:
    diag_dir = run_dir / "failure_diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    failed_worker_ids = extract_failed_worker_ids(terminal_output_path)
    write_netcheck_targets(run_dir, failed_worker_ids)

    print(
        f"[diagnostics] collecting failure evidence in {diag_dir} "
        f"failed_workers={failed_worker_ids or '-'}",
        flush=True,
    )

    run_capture(
        root,
        diag_dir / "docker-compose-ps.txt",
        ["docker", "compose", "-f", str(compose_file), "ps"],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-ip-neigh.txt",
        [
            "sh",
            "-lc",
            "ip neigh show 2>&1; printf '\\n## state counts\\n'; "
            "ip neigh show 2>/dev/null | awk '{count[$NF]++} END {for (state in count) print state, count[state]}' || true",
        ],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-conntrack.txt",
        [
            "sh",
            "-lc",
            "printf 'nf_conntrack_count='; cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || true; "
            "printf '\\nnf_conntrack_max='; cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null || true; printf '\\n'",
        ],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-sockets.txt",
        ["sh", "-lc", "ss -s 2>&1 || true"],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-cpu-io.txt",
        ["sh", "-lc", "top -b -n1 | head -40 2>&1; printf '\\n## vmstat\\n'; vmstat 1 3 2>&1 || true"],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-neighbor-sysctls.txt",
        [
            "sh",
            "-lc",
            "for f in /proc/sys/net/ipv4/neigh/default/gc_thresh1 "
            "/proc/sys/net/ipv4/neigh/default/gc_thresh2 "
            "/proc/sys/net/ipv4/neigh/default/gc_thresh3; do "
            "printf '%s=' \"$f\"; cat \"$f\" 2>/dev/null || true; done",
        ],
        env=compose_env,
    )
    write_url_capture(diag_dir / "ds-metrics.json", f"http://127.0.0.1:{args.ds_port}/metrics")
    write_url_capture(
        diag_dir / "relay-metrics.json",
        f"http://127.0.0.1:{args.relay_port}/metrics",
    )

    services = ["runner", "ds", "relay"]
    if args.include_netcheck:
        services.append("netcheck")
    services.extend(f"worker-{worker_id}" for worker_id in failed_worker_ids)
    services.extend(f"worker-pack-{i:03d}" for i in range(100))
    compose_service_logs(
        root,
        compose_file,
        diag_dir / "focused-compose-logs.txt",
        services,
        compose_env,
    )

    for worker_id in failed_worker_ids:
        service = f"worker-{worker_id}"
        container_id = compose_container_id(root, compose_file, service, compose_env)
        if container_id:
            run_capture(
                root,
                diag_dir / f"{service}-inspect.json",
                ["docker", "inspect", container_id],
                env=compose_env,
            )
            run_capture(
                root,
                diag_dir / f"{service}-docker-logs.txt",
                ["docker", "logs", "--timestamps", container_id],
                env=compose_env,
            )

    networks = project_network_names(root, project_name)
    for network in networks:
        run_capture(
            root,
            diag_dir / f"network-{network}-inspect.json",
            ["docker", "network", "inspect", network],
            env=compose_env,
        )

    if failed_worker_ids:
        probe_lines = []
        for worker_id in failed_worker_ids[:10]:
            probe_lines.append(f'echo "## worker-{worker_id}"')
            probe_lines.append(
                f'curl -fsS --connect-timeout 2 --max-time 5 '
                f'http://worker-{worker_id}:8080/health || true'
            )
            probe_lines.append("")
        probe_script = "\n".join(probe_lines)

        if args.include_netcheck:
            run_capture(
                root,
                diag_dir / "runner-network-health-probes.txt",
                [
                    "docker",
                    "compose",
                    "-f",
                    str(compose_file),
                    "run",
                    "--rm",
                    "--no-deps",
                    "netcheck",
                    "sh",
                    "-lc",
                    probe_script,
                ],
                env=compose_env,
            )

        for worker_id in failed_worker_ids[:10]:
            worker_index = int(worker_id)
            bridge_index = ((worker_index - 1) * args.bridge_count) // args.workers
            bridge_suffix = f"bench-net-{bridge_index:03d}"
            matching_networks = [name for name in networks if name.endswith(bridge_suffix)]
            for network in matching_networks[:1]:
                run_capture(
                    root,
                    diag_dir / f"failed-worker-{worker_id}-own-network-health.txt",
                    [
                        "docker",
                        "run",
                        "--rm",
                        "--network",
                        network,
                        "nicolaka/netshoot:latest",
                        "sh",
                        "-lc",
                        (
                            f"curl -fsS --connect-timeout 2 --max-time 5 "
                            f"http://worker-{worker_id}:8080/health || true"
                        ),
                    ],
                    env=compose_env,
                )

        for network in networks:
            run_capture(
                root,
                diag_dir / f"diagnostic-container-health-probe-{network}.txt",
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    network,
                    "nicolaka/netshoot:latest",
                    "sh",
                    "-lc",
                    probe_script,
                ],
                env=compose_env,
            )

    if args.include_netcheck:
        netcheck_log = run_dir / "netcheck.log"
        if netcheck_log.exists():
            shutil.copy2(netcheck_log, diag_dir / "netcheck.log")


def physical_worker_services(
    args: argparse.Namespace,
    compose_file: Path,
    root: Path,
) -> list[str]:
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "config", "--services"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []

    services = [s.strip() for s in result.stdout.splitlines() if s.strip()]
    return [s for s in services if s.startswith("worker-") and s not in ("worker-00000",)]


class ResourceLimitVerificationError(RuntimeError):
    pass


def write_run_outcome(
    run_dir: Path,
    outcome_class: str,
    evidence: dict | None = None,
) -> None:
    payload = {
        "outcome_class": outcome_class,
        "written_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "evidence": evidence or {},
    }
    (run_dir / "benchmark_outcome.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def write_artifact_validation(
    run_dir: Path,
    run_success: bool,
    primary_outcome: str = "",
    failure_class: str = "",
    aggregation_status: str = "",
) -> None:
    """Write artifact_validation.json with sidecar presence and consistency checks.

    Args:
        run_dir: The run output directory.
        run_success: Whether the run succeeded (affects strictness of checks).
        primary_outcome: The primary run outcome (e.g., "completed", "failed_workload").
        failure_class: Failure classification if run failed.
        aggregation_status: Aggregation status (e.g., "complete_success", "partial_success").
    """
    try:
        from resource_experiment_sidecars import validate_sidecars_exist, get_expected_files
    except ImportError:
        return

    sidecar_result = validate_sidecars_exist(str(run_dir), run_success=run_success)
    if run_success and aggregation_status == "":
        missing = [
            name
            for name in sidecar_result["missing"]
            if name not in ("events.csv", "aggregation_manifest.json")
        ]
        sidecar_result = {
            **sidecar_result,
            "valid": (
                sidecar_result["valid"]
                or not any(
                    name
                    in {
                        "resource_profiles.csv",
                        "worker_resource_assignments.csv",
                        "cpu_affinity_plan.json",
                        "scenario_plan.json",
                        "run_status.csv",
                    }
                    for name in missing
                )
            ),
            "missing": missing,
        }
    manifest_exists = (run_dir / "aggregation_manifest.json").exists()

    payload = {
        "checked_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "primary_outcome": primary_outcome,
        "failure_class": failure_class,
        "run_success": run_success,
        "sidecar_validation": {
            "valid": sidecar_result["valid"],
            "present": sidecar_result["present"],
            "missing": sidecar_result["missing"],
            "empty": sidecar_result["empty"],
        },
        "aggregation": {
            "attempted": aggregation_status != "",
            "status": aggregation_status,
            "manifest_exists": manifest_exists,
        },
        "clean_analysis_eligible": (
            run_success
            and sidecar_result["valid"]
            and not sidecar_result["missing"]
            and aggregation_status in ("complete_success", "")
        ),
    }
    (run_dir / "artifact_validation.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def docker_inspect_one(
    root: Path,
    container_id: str,
    env: dict[str, str] | None,
) -> dict | None:
    result = subprocess.run(
        ["docker", "inspect", container_id],
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if not parsed:
        return None
    return parsed[0]


def containerized_singletons_from_layout(layout: dict) -> list[dict]:
    singletons: list[dict] = []
    for entry in layout.get("physical_workers", []):
        if entry.get("container_mode") != "singleton":
            continue
        if entry.get("execution_backend") == "real_device":
            continue
        singletons.append(entry)
    return singletons


def has_configured_resource_limit(entry: dict) -> bool:
    return any(
        entry.get(key) is not None
        for key in (
            "resource_limit_cpus",
            "resource_limit_memory",
            "resource_limit_memory_bytes",
            "resource_limit_memory_swap",
            "resource_limit_memory_swap_bytes",
            "resource_limit_pids",
        )
    )


def expected_singleton_limit_targets(layout: dict, *, resource_experiment: bool = False) -> list[dict]:
    targets: list[dict] = []
    for entry in containerized_singletons_from_layout(layout):
        if resource_experiment and not entry.get("profiled_singleton", False):
            continue
        if has_configured_resource_limit(entry):
            targets.append(entry)
    return targets


def observed_cpu_limit(host_config: dict) -> float | None:
    nano_cpus = int(host_config.get("NanoCpus") or 0)
    if nano_cpus > 0:
        return nano_cpus / 1_000_000_000.0

    quota = int(host_config.get("CpuQuota") or 0)
    period = int(host_config.get("CpuPeriod") or 0)
    if quota > 0 and period > 0:
        return quota / period

    return None


def verify_resource_limits(
    *,
    root: Path,
    compose_file: Path,
    run_dir: Path,
    layout_path: Path,
    args: argparse.Namespace,
    compose_env: dict[str, str] | None,
) -> list[dict]:
    envelope = singleton_resource_envelope(args)
    artifact_path = run_dir / "resource_limits_verified.json"
    is_resource_experiment = getattr(args, "resource_experiment", "none") != "none"

    if not envelope["enabled"] and not is_resource_experiment:
        artifact_path.write_text(
            json.dumps(
                {
                    "enabled": False,
                    "aggregate_status": "disabled",
                    "singleton_resource_envelope": envelope,
                    "targets": [],
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return []

    layout = json.loads(layout_path.read_text(encoding="utf-8"))
    containerized_singletons = containerized_singletons_from_layout(layout)
    targets = expected_singleton_limit_targets(
        layout,
        resource_experiment=is_resource_experiment,
    )
    records = []
    failures = []

    if not targets:
        failures.append("resource envelope was enabled but no constrained singleton workers were found in worker_layout.json")
    if is_resource_experiment and len(targets) != args.profiled_singleton_count:
        failures.append(
            f"resource experiment expected {args.profiled_singleton_count} profiled singleton(s) "
            f"but worker_layout.json contains {len(targets)} constrained profiled singleton(s)"
        )
    required_singletons = (
        [entry for entry in containerized_singletons if entry.get("profiled_singleton", False)]
        if is_resource_experiment
        else containerized_singletons
    )
    missing_limit_metadata = [
        entry.get("physical_worker_id", "<unknown>")
        for entry in required_singletons
        if not has_configured_resource_limit(entry)
    ]
    if missing_limit_metadata:
        failures.append(
            "resource envelope was enabled but these containerized singletons have no limit metadata: "
            + ", ".join(missing_limit_metadata[:20])
        )

    for target in targets:
        service = target["physical_worker_id"]
        container_id = compose_container_id(root, compose_file, service, compose_env)
        expected_memory_bytes = target.get("resource_limit_memory_bytes")
        if expected_memory_bytes is None and target.get("resource_limit_memory"):
            try:
                expected_memory_bytes = parse_memory_bytes(target["resource_limit_memory"])
            except SystemExit:
                pass
        expected_swap_bytes = target.get("resource_limit_memory_swap_bytes")
        if expected_swap_bytes is None and target.get("resource_limit_memory_swap"):
            try:
                expected_swap_bytes = parse_memory_bytes(target["resource_limit_memory_swap"])
            except SystemExit:
                pass
        expected = {
            "cpus": target.get("resource_limit_cpus"),
            "memory_bytes": expected_memory_bytes,
            "memory_swap_bytes": expected_swap_bytes,
            "pids_limit": target.get("resource_limit_pids"),
            "resource_profile": target.get("resource_profile", ""),
        }
        observed: dict = {"container_id": container_id}
        checks: dict[str, bool] = {}
        messages: list[str] = []

        if not container_id:
            messages.append("container id not found via docker compose ps")
            record = {
                "physical_worker_id": service,
                "worker_id": service,
                "logical_client_id": (target.get("client_ids") or [""])[0],
                "container_name": service,
                "resource_profile_id": target.get("resource_profile_id", ""),
                "experiment_kind": target.get("resource_experiment_type", ""),
                "cpuset_cpus": target.get("cpuset_cpus", ""),
                "memory_limit": target.get("resource_limit_memory", ""),
                "memory_swap": target.get("resource_limit_memory_swap", ""),
                "rayon_num_threads": target.get("rayon_num_threads", 0),
                "expected": expected,
                "observed": observed,
                "checks": checks,
                "status": "fail",
                "messages": messages,
            }
            records.append(record)
            failures.append(f"{service}: container id not found")
            continue

        inspect = docker_inspect_one(root, container_id, compose_env)
        if not inspect:
            messages.append("docker inspect did not return container metadata")
            record = {
                "physical_worker_id": service,
                "worker_id": service,
                "logical_client_id": (target.get("client_ids") or [""])[0],
                "container_name": service,
                "resource_profile_id": target.get("resource_profile_id", ""),
                "experiment_kind": target.get("resource_experiment_type", ""),
                "cpuset_cpus": target.get("cpuset_cpus", ""),
                "memory_limit": target.get("resource_limit_memory", ""),
                "memory_swap": target.get("resource_limit_memory_swap", ""),
                "rayon_num_threads": target.get("rayon_num_threads", 0),
                "expected": expected,
                "observed": observed,
                "checks": checks,
                "status": "fail",
                "messages": messages,
            }
            records.append(record)
            failures.append(f"{service}: docker inspect unavailable")
            continue

        host_config = inspect.get("HostConfig") or {}
        observed.update({
            "memory": host_config.get("Memory"),
            "memory_swap": host_config.get("MemorySwap"),
            "nano_cpus": host_config.get("NanoCpus"),
            "cpu_quota": host_config.get("CpuQuota"),
            "cpu_period": host_config.get("CpuPeriod"),
            "effective_cpus": observed_cpu_limit(host_config),
            "pids_limit": host_config.get("PidsLimit"),
        })

        if expected["memory_bytes"] is not None:
            checks["memory"] = int(host_config.get("Memory") or 0) == int(expected["memory_bytes"])
            if not checks["memory"]:
                messages.append(
                    f"memory expected {expected['memory_bytes']} observed {host_config.get('Memory')}"
                )

        if expected["memory_swap_bytes"] is not None:
            checks["memory_swap"] = int(host_config.get("MemorySwap") or 0) == int(expected["memory_swap_bytes"])
            if not checks["memory_swap"]:
                messages.append(
                    f"memory_swap expected {expected['memory_swap_bytes']} observed {host_config.get('MemorySwap')}"
                )

        cgroup_cpu_max = None
        cgroup_cpu_max_quota = None
        cgroup_cpu_max_period = None
        pid = int((inspect.get("State") or {}).get("Pid") or 0)
        if pid > 0:
            cgroups = cgroup_paths_for_pid(pid)
            cpu_path = cgroups.get("cpu") or cgroups.get("unified")
            if cpu_path:
                cpu_max_raw = (cpu_path / "cpu.max").read_text(encoding="utf-8").strip()
                cgroup_cpu_max = cpu_max_raw
                parts = cpu_max_raw.split()
                if len(parts) == 2:
                    cgroup_cpu_max_quota = parts[0]
                    cgroup_cpu_max_period = parts[1]
        observed["cgroup_cpu_max"] = cgroup_cpu_max
        observed["cgroup_cpu_max_quota"] = cgroup_cpu_max_quota
        observed["cgroup_cpu_max_period"] = cgroup_cpu_max_period

        if expected["cpus"] is not None:
            observed_cpus = observed["effective_cpus"]
            tolerance = max(0.001, float(expected["cpus"]) * 0.005)
            checks["cpus"] = (
                observed_cpus is not None
                and abs(float(observed_cpus) - float(expected["cpus"])) <= tolerance
            )
            if not checks["cpus"]:
                messages.append(
                    f"cpus expected {expected['cpus']} observed {observed_cpus}"
                )
            if cgroup_cpu_max_quota is not None and cgroup_cpu_max_period is not None:
                try:
                    cgroup_effective = float(cgroup_cpu_max_quota) / float(cgroup_cpu_max_period)
                    checks["cgroup_cpu_max_matches_docker"] = (
                        observed_cpus is not None
                        and abs(cgroup_effective - float(observed_cpus)) <= max(0.001, float(observed_cpus) * 0.02)
                    )
                    if not checks["cgroup_cpu_max_matches_docker"]:
                        messages.append(
                            f"cgroup cpu.max ({cgroup_cpu_max}) effective={cgroup_effective:.6f} "
                            f"differs from Docker effective_cpus={observed_cpus}"
                        )
                except (ValueError, ZeroDivisionError):
                    pass

        if expected["pids_limit"] is not None:
            checks["pids_limit"] = int(host_config.get("PidsLimit") or 0) == int(expected["pids_limit"])
            if not checks["pids_limit"]:
                messages.append(
                    f"pids_limit expected {expected['pids_limit']} observed {host_config.get('PidsLimit')}"
                )

        status = "pass" if checks and all(checks.values()) else "fail"
        if not checks:
            status = "fail"
            messages.append("no configured checks were evaluated")
        if status != "pass":
            failures.append(f"{service}: " + "; ".join(messages))

        records.append({
            "physical_worker_id": service,
            "worker_id": service,
            "logical_client_id": (target.get("client_ids") or [""])[0],
            "container_name": service,
            "resource_profile_id": target.get("resource_profile_id", ""),
            "experiment_kind": target.get("resource_experiment_type", ""),
            "cpuset_cpus": target.get("cpuset_cpus", ""),
            "memory_limit": target.get("resource_limit_memory", ""),
            "memory_swap": target.get("resource_limit_memory_swap", ""),
            "rayon_num_threads": target.get("rayon_num_threads", 0),
            "container_id": container_id,
            "expected": expected,
            "observed": observed,
            "checks": checks,
            "status": status,
            "messages": messages,
        })

    aggregate_status = "pass" if not failures else "fail"
    artifact = {
        "enabled": True,
        "aggregate_status": aggregate_status,
        "singleton_resource_envelope": envelope,
        "targets": records,
        "failure_count": len(failures),
        "failures": failures,
    }
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if failures:
        write_run_outcome(
            run_dir,
            "invalid_resource_envelope",
            {
                "resource_limits_verified": str(artifact_path),
                "failures": failures,
            },
        )
        raise ResourceLimitVerificationError(
            "Docker did not apply the configured singleton resource envelope: "
            + "; ".join(failures[:5])
        )

    print(
        f"[resources] verified Docker resource limits for {len(records)} singleton container(s)",
        flush=True,
    )
    return records


def read_key_value_file(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            parts = raw.split()
            if len(parts) == 2:
                try:
                    values[parts[0]] = int(parts[1])
                except ValueError:
                    continue
    except OSError:
        pass
    return values


def read_cgroup_scalar(path: Path) -> int | str | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if raw == "max":
        return raw
    try:
        return int(raw)
    except ValueError:
        return None


def cgroup_paths_for_pid(pid: int) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    try:
        lines = Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").splitlines()
    except OSError:
        return paths

    for line in lines:
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        _, controllers, rel_path = parts
        rel_path = rel_path.lstrip("/")
        if not controllers:
            unified = Path("/sys/fs/cgroup") / rel_path
            paths.setdefault("unified", unified)
            paths.setdefault("cpu", unified)
            paths.setdefault("memory", unified)
            paths.setdefault("pids", unified)
            continue

        for controller in controllers.split(","):
            controller_path = Path("/sys/fs/cgroup") / controller / rel_path
            paths.setdefault(controller, controller_path)
            if controller.startswith("cpu"):
                paths.setdefault("cpu", controller_path)
            if controller == "memory":
                paths.setdefault("memory", controller_path)
            if controller == "pids":
                paths.setdefault("pids", controller_path)

    return paths


class ResourceMonitor:
    def __init__(
        self,
        *,
        root: Path,
        run_dir: Path,
        run_id: str,
        targets: list[dict],
        interval_ms: int,
        compose_env: dict[str, str] | None,
    ):
        self.root = root
        self.run_dir = run_dir
        self.run_id = run_id
        self.interval_seconds = interval_ms / 1000.0
        self.compose_env = compose_env
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.samples_path = run_dir / "resource_samples.jsonl"
        self.summary_path = run_dir / "resource_summary.csv"
        self.raw_summary_path = run_dir / "resource_monitor_summary.csv"
        self.oom_events_path = run_dir / "oom_events.jsonl"
        self.warning_path = run_dir / "resource_monitor_warnings.txt"
        self.warnings: list[str] = []
        self.targets = [self._build_target(record) for record in targets]
        self.summary: dict[str, dict] = {
            target["physical_worker_id"]: {
                "worker_id": target["worker_id"],
                "physical_worker_id": target["physical_worker_id"],
                "logical_client_id": target["logical_client_id"],
                "container_name": target["container_name"],
                "container_id": target["container_id"],
                "experiment_kind": target["experiment_kind"],
                "cpuset_cpus": target["cpuset_cpus"],
                "resource_limit_cpus": target["resource_limit_cpus"],
                "resource_limit_memory_bytes": target["resource_limit_memory_bytes"],
                "memory_limit": target["memory_limit"],
                "memory_swap": target["memory_swap"],
                "rayon_num_threads": target["rayon_num_threads"],
                "resource_profile_id": target.get("resource_profile_id", ""),
                "samples": 0,
                "timestamp_first_unix_ns": None,
                "timestamp_last_unix_ns": None,
                "max_memory_current": None,
                "last_memory_current": None,
                "memory_events_low": None,
                "memory_events_high": None,
                "memory_events_max": None,
                "memory_events_oom": None,
                "memory_events_oom_kill": None,
                "cpu_usage_usec_first": None,
                "cpu_usage_usec_last": None,
                "cpu_user_usec_first": None,
                "cpu_user_usec_last": None,
                "cpu_system_usec_first": None,
                "cpu_system_usec_last": None,
                "cpu_nr_periods_first": None,
                "cpu_nr_periods_last": None,
                "cpu_nr_throttled_first": None,
                "cpu_nr_throttled_last": None,
                "cpu_throttled_usec_first": None,
                "cpu_throttled_usec_last": None,
                "pids_current_max": None,
                "last_container_status": "",
                "last_container_exit_code": None,
                "last_container_oom_killed": None,
            }
            for target in self.targets
        }

    def _build_target(self, record: dict) -> dict:
        expected = record.get("expected") or {}
        container_id = record.get("container_id")
        inspect = docker_inspect_one(self.root, container_id, self.compose_env) if container_id else None
        state = (inspect or {}).get("State") or {}
        pid = int(state.get("Pid") or 0)
        cgroups = cgroup_paths_for_pid(pid) if pid > 0 else {}
        return {
            "worker_id": record.get("worker_id", record.get("physical_worker_id", "")),
            "physical_worker_id": record.get("physical_worker_id", ""),
            "logical_client_id": record.get("logical_client_id", ""),
            "container_name": record.get("container_name", record.get("physical_worker_id", "")),
            "container_id": container_id,
            "experiment_kind": record.get("experiment_kind", ""),
            "cpuset_cpus": record.get("cpuset_cpus", ""),
            "resource_limit_cpus": expected.get("cpus"),
            "resource_limit_memory_bytes": expected.get("memory_bytes"),
            "memory_limit": record.get("memory_limit", ""),
            "memory_swap": record.get("memory_swap", ""),
            "rayon_num_threads": record.get("rayon_num_threads", 0),
            "resource_profile": expected.get("resource_profile", ""),
            "resource_profile_id": record.get("resource_profile_id", ""),
            "pid": pid,
            "cgroups": cgroups,
            "state": {
                "status": state.get("Status", ""),
                "running": state.get("Running"),
                "exit_code": state.get("ExitCode"),
                "oom_killed": state.get("OOMKilled"),
            },
            "last_state_refresh": 0.0,
        }

    def start(self) -> None:
        if not self.targets:
            return
        self.thread = threading.Thread(target=self._run, name="resource-monitor", daemon=True)
        self.thread.start()
        print(
            f"[resources] monitoring {len(self.targets)} constrained singleton container(s) "
            f"every {self.interval_seconds:.3f}s",
            flush=True,
        )

    def stop(self) -> Path | None:
        if not self.thread:
            return None
        self.stop_event.set()
        self.thread.join(timeout=max(5.0, self.interval_seconds * 4))
        if self.thread.is_alive():
            self.warnings.append("resource monitor thread did not stop before timeout")
        self._write_summary()
        if self.warnings:
            self.warning_path.write_text("\n".join(self.warnings) + "\n", encoding="utf-8")
        return self.summary_path

    def _run(self) -> None:
        self.samples_path.parent.mkdir(parents=True, exist_ok=True)
        with self.samples_path.open("a", encoding="utf-8") as out:
            while not self.stop_event.is_set():
                started = time.time()
                timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
                for target in self.targets:
                    try:
                        sample = self._sample_target(target, timestamp)
                        out.write(json.dumps(sample, sort_keys=True) + "\n")
                        self._update_summary(sample)
                    except Exception as exc:
                        message = (
                            f"{timestamp} {target.get('physical_worker_id', '<unknown>')}: "
                            f"{type(exc).__name__}: {exc}"
                        )
                        self.warnings.append(message)
                out.flush()
                elapsed = time.time() - started
                remaining = max(0.0, self.interval_seconds - elapsed)
                self.stop_event.wait(remaining)

    def _refresh_state(self, target: dict, now: float) -> None:
        if now - float(target.get("last_state_refresh") or 0.0) < max(1.0, self.interval_seconds * 4):
            return
        container_id = target.get("container_id")
        inspect = docker_inspect_one(self.root, container_id, self.compose_env) if container_id else None
        state = (inspect or {}).get("State") or {}
        pid = int(state.get("Pid") or 0)
        target["pid"] = pid
        if pid > 0 and not target.get("cgroups"):
            target["cgroups"] = cgroup_paths_for_pid(pid)
        target["state"] = {
            "status": state.get("Status", ""),
            "running": state.get("Running"),
            "exit_code": state.get("ExitCode"),
            "oom_killed": state.get("OOMKilled"),
        }
        target["last_state_refresh"] = now

    def _sample_target(self, target: dict, timestamp: str) -> dict:
        now = time.time()
        self._refresh_state(target, now)
        cgroups = target.get("cgroups") or {}
        memory_path = cgroups.get("memory") or cgroups.get("unified")
        cpu_path = cgroups.get("cpu") or cgroups.get("unified")
        pids_path = cgroups.get("pids") or cgroups.get("unified")

        memory_events = read_key_value_file(memory_path / "memory.events") if memory_path else {}
        cpu_stat = read_key_value_file(cpu_path / "cpu.stat") if cpu_path else {}
        state = target.get("state") or {}

        sample = {
            "timestamp": timestamp,
            "timestamp_unix_ns": time.time_ns(),
            "run_id": self.run_id,
            "worker_id": target.get("worker_id"),
            "physical_worker_id": target.get("physical_worker_id"),
            "logical_client_id": target.get("logical_client_id"),
            "container_name": target.get("container_name"),
            "container_id": target.get("container_id"),
            "experiment_kind": target.get("experiment_kind", ""),
            "cpuset_cpus": target.get("cpuset_cpus", ""),
            "resource_limit_cpus": target.get("resource_limit_cpus"),
            "resource_limit_memory_bytes": target.get("resource_limit_memory_bytes"),
            "resource_profile": target.get("resource_profile", ""),
            "resource_profile_id": target.get("resource_profile_id", ""),
            "memory.current": read_cgroup_scalar(memory_path / "memory.current") if memory_path else None,
            "memory.max": read_cgroup_scalar(memory_path / "memory.max") if memory_path else None,
            "memory.events.low": memory_events.get("low"),
            "memory.events.high": memory_events.get("high"),
            "memory.events.max": memory_events.get("max"),
            "memory.events.oom": memory_events.get("oom"),
            "memory.events.oom_kill": memory_events.get("oom_kill"),
            "cpu.stat.usage_usec": cpu_stat.get("usage_usec"),
            "cpu.stat.user_usec": cpu_stat.get("user_usec"),
            "cpu.stat.system_usec": cpu_stat.get("system_usec"),
            "cpu.stat.nr_periods": cpu_stat.get("nr_periods"),
            "cpu.stat.nr_throttled": cpu_stat.get("nr_throttled"),
            "cpu.stat.throttled_usec": cpu_stat.get("throttled_usec"),
            "cpu.max": (cpu_path / "cpu.max").read_text(encoding="utf-8").strip() if cpu_path else None,
            "pids.current": read_cgroup_scalar(pids_path / "pids.current") if pids_path else None,
            "container.status": state.get("status"),
            "container.running": state.get("running"),
            "container.exit_code": state.get("exit_code"),
            "container.oom_killed": state.get("oom_killed"),
        }
        return sample

    def _update_summary(self, sample: dict) -> None:
        physical_id = sample.get("physical_worker_id")
        if physical_id not in self.summary:
            return
        row = self.summary[physical_id]
        row["samples"] += 1

        ts_ns = sample.get("timestamp_unix_ns")
        if ts_ns is not None:
            if row["timestamp_first_unix_ns"] is None:
                row["timestamp_first_unix_ns"] = ts_ns
            row["timestamp_last_unix_ns"] = ts_ns

        sample_pid = sample.get("resource_profile_id")
        if sample_pid and not row.get("resource_profile_id"):
            row["resource_profile_id"] = sample_pid

        memory_current = sample.get("memory.current")
        if isinstance(memory_current, int):
            row["last_memory_current"] = memory_current
            current_max = row["max_memory_current"]
            row["max_memory_current"] = (
                memory_current
                if current_max is None
                else max(int(current_max), memory_current)
            )

        previous_oom_kill = int_or_zero(row.get("memory_events_oom_kill"))
        for key in ("low", "high", "max", "oom", "oom_kill"):
            sample_key = f"memory.events.{key}"
            value = sample.get(sample_key)
            if value is not None:
                row[f"memory_events_{key}"] = value

        current_oom_kill = int_or_zero(sample.get("memory.events.oom_kill"))
        if current_oom_kill > previous_oom_kill:
            self._append_oom_event(
                sample,
                "docker_cgroup",
                f"memory.events.oom_kill increased from {previous_oom_kill} to {current_oom_kill}",
            )

        for summary_key, sample_key in (
            ("cpu_usage_usec", "cpu.stat.usage_usec"),
            ("cpu_user_usec", "cpu.stat.user_usec"),
            ("cpu_system_usec", "cpu.stat.system_usec"),
            ("cpu_nr_periods", "cpu.stat.nr_periods"),
            ("cpu_nr_throttled", "cpu.stat.nr_throttled"),
            ("cpu_throttled_usec", "cpu.stat.throttled_usec"),
        ):
            value = sample.get(sample_key)
            if value is None:
                continue
            first_key = f"{summary_key}_first"
            last_key = f"{summary_key}_last"
            if row[first_key] is None:
                row[first_key] = value
            row[last_key] = value

        pids_current = sample.get("pids.current")
        if isinstance(pids_current, int):
            current_max = row["pids_current_max"]
            row["pids_current_max"] = (
                pids_current
                if current_max is None
                else max(int(current_max), pids_current)
            )

        row["last_container_status"] = sample.get("container.status") or ""
        row["last_container_exit_code"] = sample.get("container.exit_code")
        previous_container_oom = bool(row.get("last_container_oom_killed"))
        row["last_container_oom_killed"] = sample.get("container.oom_killed")
        if sample.get("container.oom_killed") is True and not previous_container_oom:
            self._append_oom_event(sample, "docker_state", "container State.OOMKilled became true")

    def _append_oom_event(self, sample: dict, source: str, detail: str) -> None:
        event = {
            "schema_version": 1,
            "ts_unix_ns": sample.get("timestamp_unix_ns") or time.time_ns(),
            "source": source,
            "worker_id": None,
            "physical_worker_id": sample.get("physical_worker_id"),
            "detail": detail,
            "container_id": sample.get("container_id"),
        }
        with self.oom_events_path.open("a", encoding="utf-8") as out:
            out.write(json.dumps(event, sort_keys=True) + "\n")

    def _write_summary(self) -> None:
        fieldnames = [
            "worker_id",
            "physical_worker_id",
            "logical_client_id",
            "container_name",
            "container_id",
            "resource_profile_id",
            "experiment_kind",
            "cpuset_cpus",
            "resource_limit_cpus",
            "resource_limit_memory_bytes",
            "memory_limit",
            "memory_swap",
            "rayon_num_threads",
            "samples",
            "elapsed_wall_ns",
            "max_memory_current",
            "last_memory_current",
            "memory_events_low",
            "memory_events_high",
            "memory_events_max",
            "memory_events_oom",
            "memory_events_oom_kill",
            "cpu_usage_usec_delta",
            "cpu_user_usec_delta",
            "cpu_system_usec_delta",
            "cpu_nr_periods_delta",
            "cpu_nr_throttled_delta",
            "cpu_throttled_usec_delta",
            "throttled_period_fraction",
            "cpu_usage_fraction",
            "throttled_time_rate",
            "pids_current_max",
            "last_container_status",
            "last_container_exit_code",
            "last_container_oom_killed",
        ]
        canonical_rows = []
        with self.raw_summary_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for raw in self.summary.values():
                row = dict(raw)
                elapsed_wall_ns = delta(
                    row.pop("timestamp_first_unix_ns"),
                    row.pop("timestamp_last_unix_ns"),
                )
                row["elapsed_wall_ns"] = elapsed_wall_ns
                row["cpu_usage_usec_delta"] = delta(
                    row.pop("cpu_usage_usec_first"),
                    row.pop("cpu_usage_usec_last"),
                )
                row["cpu_user_usec_delta"] = delta(
                    row.pop("cpu_user_usec_first"),
                    row.pop("cpu_user_usec_last"),
                )
                row["cpu_system_usec_delta"] = delta(
                    row.pop("cpu_system_usec_first"),
                    row.pop("cpu_system_usec_last"),
                )
                nr_periods_delta = delta(
                    row.pop("cpu_nr_periods_first"),
                    row.pop("cpu_nr_periods_last"),
                )
                row["cpu_nr_periods_delta"] = nr_periods_delta
                nr_throttled_delta = delta(
                    row.pop("cpu_nr_throttled_first"),
                    row.pop("cpu_nr_throttled_last"),
                )
                row["cpu_nr_throttled_delta"] = nr_throttled_delta
                throttled_usec_delta = delta(
                    row.pop("cpu_throttled_usec_first"),
                    row.pop("cpu_throttled_usec_last"),
                )
                row["cpu_throttled_usec_delta"] = throttled_usec_delta

                if (nr_periods_delta is not None
                        and isinstance(nr_periods_delta, (int, float))
                        and nr_periods_delta > 0):
                    row["throttled_period_fraction"] = round(
                        float(nr_throttled_delta or 0) / float(nr_periods_delta), 6)
                else:
                    row["throttled_period_fraction"] = ""

                elapsed_us = elapsed_wall_ns / 1000.0 if isinstance(elapsed_wall_ns, (int, float)) else 0
                if elapsed_us > 0:
                    delta_usage = row["cpu_usage_usec_delta"]
                    if isinstance(delta_usage, (int, float)):
                        row["cpu_usage_fraction"] = round(float(delta_usage) / elapsed_us, 6)
                    else:
                        row["cpu_usage_fraction"] = ""
                    delta_throttled = row["cpu_throttled_usec_delta"]
                    if isinstance(delta_throttled, (int, float)):
                        row["throttled_time_rate"] = round(float(delta_throttled) / elapsed_us, 6)
                    else:
                        row["throttled_time_rate"] = ""
                else:
                    row["cpu_usage_fraction"] = ""
                    row["throttled_time_rate"] = ""

                writer.writerow({key: row.get(key) for key in fieldnames})

                canonical_rows.append({
                    "worker_id": row.get("worker_id", ""),
                    "physical_worker_id": row.get("physical_worker_id", ""),
                    "logical_client_id": row.get("logical_client_id", ""),
                    "container_name": row.get("container_name", ""),
                    "resource_profile_id": row.get("resource_profile_id", ""),
                    "experiment_kind": row.get("experiment_kind", ""),
                    "cpuset_cpus": row.get("cpuset_cpus", ""),
                    "cpu_limit_cpus": row.get("resource_limit_cpus"),
                    "memory_limit": row.get("memory_limit", ""),
                    "memory_swap": row.get("memory_swap", ""),
                    "rayon_num_threads": row.get("rayon_num_threads", 0),
                    "sample_count": row.get("samples", 0),
                    "max_memory_current": row.get("max_memory_current"),
                    "last_memory_current": row.get("last_memory_current"),
                    "memory_events_oom": row.get("memory_events_oom"),
                    "memory_events_oom_kill": row.get("memory_events_oom_kill"),
                    "cpu_usage_usec_delta": row.get("cpu_usage_usec_delta"),
                    "cpu_nr_throttled_delta": row.get("cpu_nr_throttled_delta"),
                    "cpu_throttled_usec_delta": row.get("cpu_throttled_usec_delta"),
                    "cpu_throttled_time_fraction": row.get("throttled_period_fraction"),
                    "max_thread_count": row.get("pids_current_max"),
                    "max_process_count": "",
                    "last_container_status": row.get("last_container_status", ""),
                    "last_container_exit_code": row.get("last_container_exit_code"),
                    "last_container_oom_killed": row.get("last_container_oom_killed"),
                })

        from resource_experiment_sidecars import SidecarWriter
        SidecarWriter(str(self.run_dir)).write_resource_summary(
            self.run_id,
            canonical_rows,
        )


def external_worker_oom_dmesg_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw in text.splitlines():
        lowered = raw.lower()
        has_oom_signal = (
            "out of memory" in lowered
            or "oom-kill" in lowered
            or "oom_reaper" in lowered
            or "killed process" in lowered
        )
        if has_oom_signal and "worker" in lowered:
            lines.append(raw.strip())
    return lines


class ExternalOomMonitor:
    def __init__(self, *, run_dir: Path, targets: list[tuple[object, str, str]], interval_ms: int):
        self.targets = targets
        self.interval_seconds = max(interval_ms / 1000.0, 0.25)
        self.oom_events_path = run_dir / "oom_events.jsonl"
        self.warning_path = run_dir / "external_oom_monitor_warnings.txt"
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.seen: dict[str, set[str]] = {physical_id: set() for _, physical_id, _ in targets}
        self.warnings: list[str] = []

    def start(self) -> None:
        if not self.targets:
            return
        for backend, physical_id, _ in self.targets:
            self.seen[physical_id].update(self._read_lines(backend, physical_id))
        self.thread = threading.Thread(target=self._run, name="external-oom-monitor", daemon=True)
        self.thread.start()
        print(
            f"[resources] monitoring {len(self.targets)} external device dmesg stream(s) for worker OOM kills",
            flush=True,
        )

    def stop(self) -> None:
        if not self.thread:
            return
        self.stop_event.set()
        self.thread.join(timeout=max(5.0, self.interval_seconds * 4))
        if self.thread.is_alive():
            self.warnings.append("external OOM monitor thread did not stop before timeout")
        if self.warnings:
            self.warning_path.write_text("\n".join(self.warnings) + "\n", encoding="utf-8")

    def _read_lines(self, backend: object, physical_id: str) -> list[str]:
        try:
            result = backend.shell("dmesg", check=False)
        except Exception as exc:
            self.warnings.append(
                f"{dt.datetime.now(dt.timezone.utc).isoformat()} {physical_id}: {type(exc).__name__}: {exc}"
            )
            return []
        if result.returncode != 0:
            text = (result.stderr or result.stdout or "").strip()
            if text:
                self.warnings.append(
                    f"{dt.datetime.now(dt.timezone.utc).isoformat()} {physical_id}: dmesg failed: {text}"
                )
            return []
        return external_worker_oom_dmesg_lines(result.stdout or "")

    def _run(self) -> None:
        while not self.stop_event.is_set():
            for backend, physical_id, worker_id in self.targets:
                for line in self._read_lines(backend, physical_id):
                    if line in self.seen[physical_id]:
                        continue
                    self.seen[physical_id].add(line)
                    event = {
                        "schema_version": 1,
                        "ts_unix_ns": time.time_ns(),
                        "source": "external_dmesg",
                        "worker_id": worker_id,
                        "physical_worker_id": physical_id,
                        "detail": line,
                    }
                    with self.oom_events_path.open("a", encoding="utf-8") as out:
                        out.write(json.dumps(event, sort_keys=True) + "\n")
            self.stop_event.wait(self.interval_seconds)


def external_oom_monitor_targets(args: argparse.Namespace, root: Path) -> list[tuple[object, str, str]]:
    if not args.enable_external_devices or not args.devices_file:
        return []
    from external_devices import create_backend, load_devices_config

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / devices_file
    configs = [config for config in load_devices_config(devices_file) if config.enabled]
    if args.external_device_ids:
        configs = [config for config in configs if config.id in args.external_device_ids]
    return [
        (create_backend(config), config.id, config.worker.get("id", config.id))
        for config in configs
    ]


def delta(first: int | None, last: int | None) -> int | None:
    if first is None or last is None:
        return None
    return max(0, int(last) - int(first))


def classify_failure_from_resource_summary(run_dir: Path) -> tuple[str, dict]:
    summary_path = run_dir / "resource_summary.csv"
    evidence: dict = {"resource_summary": str(summary_path) if summary_path.exists() else None}
    oom_events_path = run_dir / "oom_events.jsonl"
    if oom_events_path.exists():
        oom_events = []
        for line in oom_events_path.read_text(encoding="utf-8").splitlines():
            try:
                oom_events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        if oom_events:
            evidence["oom_events"] = str(oom_events_path)
            evidence["oom_workers"] = sorted({
                str(event.get("worker_id") or event.get("physical_worker_id"))
                for event in oom_events
                if event.get("worker_id") or event.get("physical_worker_id")
            })
            return "hard_upper_bound_oom_kill", evidence
    if not summary_path.exists():
        return "infrastructure_failure", evidence

    try:
        rows = list(csv.DictReader(summary_path.open("r", encoding="utf-8")))
    except OSError as exc:
        evidence["summary_error"] = str(exc)
        return "infrastructure_failure", evidence

    oom_rows = [
        row for row in rows
        if str(row.get("last_container_oom_killed", "")).lower() == "true"
        or int_or_zero(row.get("memory_events_oom_kill")) > 0
    ]
    if oom_rows:
        evidence["oom_workers"] = [row.get("physical_worker_id") for row in oom_rows]
        return "hard_upper_bound_oom_kill", evidence

    exited_rows = [
        row for row in rows
        if row.get("last_container_status") == "exited"
        or int_or_zero(row.get("last_container_exit_code")) not in (0, None)
    ]
    if exited_rows:
        evidence["exited_workers"] = [row.get("physical_worker_id") for row in exited_rows]
        return "hard_upper_bound_container_exit", evidence

    throttled_rows = [
        row for row in rows
        if int_or_zero(row.get("cpu_nr_throttled_delta")) > 0
        or int_or_zero(row.get("cpu_throttled_usec_delta")) > 0
    ]
    if throttled_rows:
        evidence["cpu_throttled_workers"] = [row.get("physical_worker_id") for row in throttled_rows[:20]]

    memory_pressure_rows = [
        row for row in rows
        if int_or_zero(row.get("memory_events_high")) > 0
        or int_or_zero(row.get("memory_events_max")) > 0
        or int_or_zero(row.get("memory_events_oom")) > 0
    ]
    if memory_pressure_rows:
        evidence["memory_pressure_workers"] = [row.get("physical_worker_id") for row in memory_pressure_rows[:20]]
        return "resource_pressure_memory", evidence

    return "infrastructure_failure", evidence


def int_or_zero(value) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def chunks(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def build_compose_env(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    if args.compose_parallel_limit is not None:
        env["COMPOSE_PARALLEL_LIMIT"] = str(args.compose_parallel_limit)

    env["OPENMLS_DS_DELIVERY_MODE"] = args.ds_delivery_mode
    env["OPENMLS_RUNNER_HTTP_CONNECT_TIMEOUT_MS"] = str(args.runner_http_connect_timeout_ms)
    env["OPENMLS_RUNNER_HTTP_REQUEST_TIMEOUT_MS"] = str(args.runner_http_request_timeout_ms)
    env["OPENMLS_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST"] = str(args.worker_http_pool_max_idle_per_host)
    env["OPENMLS_WORKER_HTTP_CONNECT_TIMEOUT_MS"] = str(args.worker_http_connect_timeout_ms)
    env["OPENMLS_WORKER_HTTP_REQUEST_TIMEOUT_MS"] = str(args.worker_http_request_timeout_ms)
    env["OPENMLS_WORKER_OUTBOUND_HTTP_PERMITS"] = str(args.worker_outbound_http_permits)
    return env


def compose_down(
        *,
        root: Path,
        compose_file: Path,
        args: argparse.Namespace,
        env: dict[str, str] | None,
) -> None:
    timeout = str(args.compose_down_timeout_seconds)

    if args.teardown_batch_size <= 0:
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "down",
                "--timeout",
                timeout,
            ],
            cwd=str(root),
            env=env,
            check=False,
        )
        return

    print("[compose] stopping/removing runner service if present")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "stop",
            "-t",
            timeout,
            "runner",
        ],
        cwd=str(root),
        env=env,
        check=False,
    )
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "rm",
            "-f",
            "runner",
        ],
        cwd=str(root),
        env=env,
        check=False,
    )

    workers = physical_worker_services(args, compose_file, root)
    workers.reverse()

    for batch in chunks(workers, args.teardown_batch_size):
        print(
            f"[compose] stopping/removing workers {batch[-1]} .. {batch[0]} "
            f"({len(batch)} workers)"
        )

        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "stop",
                "-t",
                timeout,
                *batch,
            ],
            cwd=str(root),
            env=env,
            check=False,
        )

        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "rm",
                "-f",
                *batch,
            ],
            cwd=str(root),
            env=env,
            check=False,
        )

        if args.teardown_batch_sleep_seconds > 0:
            time.sleep(args.teardown_batch_sleep_seconds)

    print("[compose] final down for ds/relay/network")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "down",
            "--timeout",
            timeout,
        ],
        cwd=str(root),
        env=env,
        check=False,
    )


def launch_external_devices(
    args: argparse.Namespace,
    root: Path,
    ds_port: int,
    relay_port: int,
    run_id: str,
) -> tuple[list[dict], list[dict], list[str]]:
    """Start external devices and return (layout_clients, layout_workers, worker_lines)."""
    from external_devices import (
        create_backend,
        load_devices_config,
        validate_run_id as py_validate_run_id,
        WorkerLaunch,
        build_external_device_layout_entry,
        build_worker_url_candidates,
        wait_for_first_healthy_url,
    )

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / devices_file
    if not devices_file.exists():
        raise RuntimeError(f"Devices file not found: {devices_file}")

    py_validate_run_id(run_id)

    configs = load_devices_config(devices_file)
    enabled = [c for c in configs if c.enabled]
    if args.external_device_ids:
        enabled = [c for c in enabled if c.id in args.external_device_ids]

    if not enabled:
        print("[device] no enabled external devices found")
        return [], [], []

    layout_clients: list[dict] = []
    layout_workers: list[dict] = []
    worker_lines: list[str] = []

    for dev_config in enabled:
        dev_id = dev_config.id
        print(f"[device] launching external device: {dev_id}", flush=True)

        backend = create_backend(dev_config)

        # Check reachable
        print(f"[device] {dev_id}: checking reachability...", flush=True)
        backend.check_reachable()

        expected_arch = dev_config.target.get("arch")
        if expected_arch:
            arch_output = backend.shell("uname -m", check=False).stdout
            arch_lines = [line.strip() for line in arch_output.splitlines() if line.strip()]
            arch = arch_lines[-1] if arch_lines else ""
            if arch != expected_arch:
                raise RuntimeError(
                    f"Device {dev_id} architecture mismatch: "
                    f"expected {expected_arch}, got {arch or '<unknown>'}"
                )
            print(f"[device] {dev_id}: architecture ok ({arch})", flush=True)

        print(f"[device] {dev_id}: checking clock...", flush=True)
        backend.ensure_clock_synchronized()

        print(f"[device] {dev_id}: checking perf-event access...", flush=True)
        backend.ensure_perf_event_access()

        transport = dev_config.transport
        device_ip = transport.get("device_ip", "172.32.0.93")
        host_ip = transport.get("host_ip", "172.32.0.98")
        worker_port = transport.get("worker_port", 8080)
        listen_addr = f"0.0.0.0:{worker_port}"

        # Stop existing worker
        print(f"[device] {dev_id}: stopping existing worker...", flush=True)
        backend.stop_worker()

        # Wipe remote directories
        if args.wipe_device_run_dirs:
            print(f"[device] {dev_id}: wiping remote directories...", flush=True)
            backend.wipe_for_run(run_id)

        # Push worker binary
        configured_binary = Path(dev_config.target.get(
            "binary",
            "target/armv7-unknown-linux-musleabihf/minsize/worker",
        ))
        local_binary = configured_binary if configured_binary.is_absolute() else root / configured_binary
        remote_binary = dev_config.worker.get("remote_binary", "/worker")
        if local_binary.exists():
            print(f"[device] {dev_id}: pushing worker binary...", flush=True)
            backend.install_worker(local_binary, remote_binary)
        else:
            print(f"[device] {dev_id}: worker binary not found at {local_binary}; checking remote {remote_binary}", flush=True)
            remote_check = backend.shell(
                f"test -x {shlex.quote(remote_binary)}",
                check=False,
            )
            if remote_check.returncode != 0:
                raise RuntimeError(
                    f"External worker binary is missing locally and remotely for {dev_id}. "
                    f"Expected local binary at {local_binary} or executable remote binary at {remote_binary}."
                )
            print(f"[device] {dev_id}: using existing remote worker binary {remote_binary}", flush=True)

        # Build DS/relay URLs for device
        device_ds_url = f"http://{host_ip}:{ds_port}"
        device_relay_url = f"http://{host_ip}:{relay_port}"

        worker_id = dev_config.worker.get("id", dev_id)
        remote_results_root = dev_config.worker.get("remote_results_root", "/results/openmls")
        remote_tmp = dev_config.worker.get("remote_tmp", "/tmp/openmls-benchmark")
        profile_template = f"{remote_results_root}/{run_id}/client-{{client_id}}.jsonl"

        launch = WorkerLaunch(
            worker_id=worker_id,
            binary_path=remote_binary,
            ds_url=device_ds_url,
            relay_url=device_relay_url,
            listen_addr=listen_addr,
            run_id=run_id,
            scenario=args.scenario,
            scenario_seed=args.scenario_seed,
            profile_path_template=profile_template,
            remote_results_root=remote_results_root,
            remote_tmp=remote_tmp,
            node_name=dev_id,
        )

        print(f"[device] {dev_id}: starting worker (DS={device_ds_url}, Relay={device_relay_url})...", flush=True)
        backend.start_worker(launch)

        # Health check
        worker_urls = build_worker_url_candidates(dev_config, device_ip, worker_port)
        print(f"[device] {dev_id}: waiting for health on {', '.join(worker_urls)}...", flush=True)
        try:
            worker_url = wait_for_first_healthy_url(worker_urls, timeout_s=60)
        except Exception as e:
            log = backend.shell(
                f"cat {shlex.quote(remote_tmp)}/worker.log 2>/dev/null || true",
                check=False,
            ).stdout.strip()
            ps = backend.shell("ps | grep '[w]orker' || true", check=False).stdout.strip()
            backend.stop_worker()
            details = [
                f"External worker {dev_id} did not become healthy.",
                f"Tried URLs: {', '.join(worker_urls)}",
                f"Remote binary: {remote_binary}",
            ]
            if ps:
                details.append(f"Worker process:\n{ps}")
            if log:
                details.append(f"Worker log:\n{log}")
            raise RuntimeError("\n".join(details)) from e

        # Build layout entries
        client_entry, phys_entry, worker_line = build_external_device_layout_entry(
            dev_config,
            transport_ip=device_ip,
            host_ip=host_ip,
            worker_port=worker_port,
            ds_port=ds_port,
            relay_port=relay_port,
            run_id=run_id,
            worker_url=worker_url,
        )

        layout_clients.append(client_entry)
        layout_workers.append(phys_entry)
        worker_lines.append(worker_line)

        print(f"[device] {dev_id}: worker {worker_id} ready at {worker_url}", flush=True)

    return layout_clients, layout_workers, worker_lines


def stop_external_device_workers(args: argparse.Namespace, root: Path) -> None:
    if not args.enable_external_devices or not args.devices_file or args.no_device_stop_after_run:
        return

    from external_devices import create_backend, load_devices_config

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / args.devices_file
    if not devices_file.exists():
        return

    configs = load_devices_config(devices_file)
    enabled = [c for c in configs if c.enabled]
    if args.external_device_ids:
        enabled = [c for c in enabled if c.id in args.external_device_ids]

    for dev_config in enabled:
        try:
            backend = create_backend(dev_config)
            print(f"[device] {dev_config.id}: stopping worker...", flush=True)
            backend.stop_worker()
        except Exception as e:
            print(f"[device] {dev_config.id}: stop failed (non-fatal): {e}", flush=True)


def pull_external_device_profiles(
    args: argparse.Namespace,
    root: Path,
    run_id: str,
    run_dir: Path,
) -> None:
    """Pull profile files from external devices into the local run directory."""
    from external_devices import create_backend, load_devices_config, validate_run_id as py_validate_run_id

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / devices_file
    if not devices_file.exists():
        return

    py_validate_run_id(run_id)

    configs = load_devices_config(devices_file)
    enabled = [c for c in configs if c.enabled]
    if args.external_device_ids:
        enabled = [c for c in enabled if c.id in args.external_device_ids]

    for dev_config in enabled:
        dev_id = dev_config.id
        backend = create_backend(dev_config)

        remote_results_root = dev_config.worker.get("remote_results_root", "/results/openmls")
        remote_path = f"{remote_results_root}/{run_id}"

        device_local_dir = run_dir / "external" / dev_id
        device_local_dir.mkdir(parents=True, exist_ok=True)

        print(f"[device] {dev_id}: pulling profiles from {remote_path} to {device_local_dir}", flush=True)
        try:
            backend.pull(remote_path, device_local_dir)
        except Exception as e:
            print(f"[device] {dev_id}: pull failed (non-fatal): {e}", flush=True)

        # Also try pulling individual JSONL files to the top-level run dir.
        # Retry with backoff in case the device worker is still flushing data.
        worker_id = dev_config.worker.get("id", dev_id)
        remote_jsonl = f"{remote_path}/client-{worker_id}.jsonl"
        local_jsonl = run_dir / f"client-{worker_id}.jsonl"

        pulled = False
        for attempt in range(1, 4):
            try:
                local_runner_profile = local_jsonl.read_bytes() if local_jsonl.exists() else b""
                backend.pull(remote_jsonl, local_jsonl)
                if local_runner_profile:
                    pulled_profile = local_jsonl.read_bytes()
                    separator = b"" if not pulled_profile or pulled_profile.endswith(b"\n") else b"\n"
                    local_jsonl.write_bytes(pulled_profile + separator + local_runner_profile)
                size = local_jsonl.stat().st_size if local_jsonl.exists() else 0
                print(f"[device] {dev_id}: pulled profile ({size} bytes) to {local_jsonl}", flush=True)
                pulled = True
                break
            except Exception as e:
                if attempt < 3:
                    wait = attempt * 2.0
                    print(f"[device] {dev_id}: pull attempt {attempt} failed, retrying in {wait}s: {e}", flush=True)
                    time.sleep(wait)
                else:
                    print(f"[device] {dev_id}: pulling individual JSONL failed after 3 attempts (non-fatal): {e}", flush=True)

        if not pulled:
            # Check what files exist on the device
            print(f"[device] {dev_id}: checking remote directory contents for debugging...", flush=True)
            try:
                ls_result = backend.shell(f"ls -la {shlex.quote(remote_path)}/ 2>/dev/null || echo 'NO_DIR'", check=False)
                print(f"[device] {dev_id}: remote {remote_path}/ contents:\n{ls_result.stdout.strip()}", flush=True)
            except Exception as ls_err:
                print(f"[device] {dev_id}: could not list remote dir: {ls_err}", flush=True)


def run_standalone_aggregation(
    run_dir: Path,
    layout_file: Optional[Path],
    workers_file: Optional[Path],
    *,
    allow_partial: bool = False,
    manifest_path: Optional[Path] = None,
) -> int:
    """Run the Rust aggregate_profiles binary.

    Args:
        run_dir: The run directory to aggregate.
        layout_file: Optional path to worker_layout.json.
        workers_file: Optional path to workers file.
        allow_partial: If True, pass --allow-partial flag for failure-tolerant aggregation.
        manifest_path: Optional path for aggregation manifest (defaults to run dir).
    """
    root = repo_root()
    aggregate_binary = os.environ.get("OPENMLS_AGGREGATE_PROFILES_BINARY")
    if aggregate_binary:
        cmd = [aggregate_binary]
    else:
        source_paths = [
            root / "Cargo.lock",
            root / "Cargo.toml",
            root / "src" / "bin" / "aggregate_profiles.rs",
            root / "src" / "lib.rs",
            root / "src" / "staircase_runner.rs",
        ]
        source_mtime = max(
            (p.stat().st_mtime for p in source_paths if p.exists()),
            default=0.0,
        )
        candidate_binaries = [
            root / "target" / "release" / "aggregate_profiles",
            root / "target" / "debug" / "aggregate_profiles",
        ]
        fresh_binary = next(
            (
                p
                for p in candidate_binaries
                if p.exists() and p.stat().st_mtime >= source_mtime
            ),
            None,
        )
        if fresh_binary:
            cmd = [str(fresh_binary)]
        else:
            cargo = os.environ.get("CARGO") or shutil.which("cargo")
            if not cargo:
                print(
                    "[aggregate] cargo not found on PATH and no fresh aggregate_profiles "
                    "binary is available. If this was launched with sudo, run the benchmark "
                    "without sudo, preserve PATH/CARGO, or set OPENMLS_AGGREGATE_PROFILES_BINARY.",
                    flush=True,
                )
                return 127
            cmd = [cargo, "run", "--bin", "aggregate_profiles", "--"]

    cmd += ["--run-dir", str(run_dir)]
    if layout_file:
        cmd += ["--layout-file", str(layout_file)]
    if workers_file:
        cmd += ["--workers-file", str(workers_file)]
    if allow_partial:
        cmd += ["--allow-partial"]
        manifest = manifest_path or (run_dir / "aggregation_manifest.json")
        cmd += ["--manifest", str(manifest)]

    print(f"[aggregate] running standalone aggregation{' (partial mode)' if allow_partial else ''}", flush=True)
    try:
        result = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)
    except FileNotFoundError as exc:
        print(f"[aggregate] failed to start aggregation command {cmd[0]!r}: {exc}", flush=True)
        return 127
    if result.returncode != 0:
        print(f"[aggregate] aggregation failed (exit={result.returncode}): {result.stderr}", flush=True)
    else:
        print(f"[aggregate] aggregation complete: {result.stdout.strip()}", flush=True)
    return result.returncode


def standalone_aggregation_inputs(run_dir: Path) -> tuple[Optional[Path], Optional[Path]]:
    layout_file = run_dir / "worker_layout.json"
    workers_file = run_dir / "workers.combined.txt"
    if not workers_file.exists():
        workers_file = run_dir / "workers.txt"
    return (
        layout_file if layout_file.exists() else None,
        workers_file if workers_file.exists() else None,
    )


def command_stdout(cmd: list[str], cwd: Path | None = None) -> str:
    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception as exc:
        return f"<unavailable: {exc}>"
    return (result.stdout or result.stderr).strip()


def sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def collect_external_device_build_metadata(root: Path, args: argparse.Namespace) -> list[dict]:
    if not args.enable_external_devices or not args.devices_file:
        return []

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / devices_file

    try:
        from external_devices import load_devices_config

        configs = load_devices_config(devices_file)
    except Exception as exc:
        return [{"metadata_error": f"{type(exc).__name__}: {exc}"}]

    enabled = [c for c in configs if c.enabled]
    if args.external_device_ids:
        enabled = [c for c in enabled if c.id in args.external_device_ids]

    devices = []
    for config in enabled:
        configured_binary = Path(config.target.get("binary", ""))
        local_binary = (
            configured_binary
            if configured_binary.is_absolute()
            else root / configured_binary
        )
        devices.append({
            "id": config.id,
            "worker_id": config.worker.get("id", config.id),
            "kind": config.kind,
            "device_kind": config.metadata.get("device_kind", config.kind),
            "transport": config.metadata.get("transport", config.transport.get("type", "")),
            "access_backend": config.metadata.get("access_backend", config.connection.get("type", "")),
            "rust_target": config.target.get("rust_target", ""),
            "binary_path": str(local_binary),
            "binary_sha256": sha256_file(local_binary),
            "remote_binary": config.worker.get("remote_binary", ""),
            "remote_results_root": config.worker.get("remote_results_root", ""),
        })
    return devices


def read_text_file(path: str) -> str | None:
    try:
        text = Path(path).read_text(encoding="utf-8").strip()
        return text or None
    except Exception:
        return None


def collect_cpu_metadata() -> dict:
    governors = sorted({
        value
        for path in glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")
        if (value := read_text_file(path))
    })
    freqs = [
        int(value)
        for path in glob.glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq")
        if (value := read_text_file(path)) and value.isdigit()
    ]
    return {
        "model_name": next(
            (
                line.split(":", 1)[1].strip()
                for line in (read_text_file("/proc/cpuinfo") or "").splitlines()
                if line.lower().startswith("model name")
            ),
            None,
        ),
        "scaling_governors": governors,
        "scaling_cur_freq_khz_min": min(freqs) if freqs else None,
        "scaling_cur_freq_khz_max": max(freqs) if freqs else None,
    }


def write_benchmark_metadata(run_dir: Path, root: Path, args: argparse.Namespace, run_id: str, scenario: str) -> None:
    external_binary = root / "target/armv7-unknown-linux-musleabihf/minsize/worker"
    metadata = {
        "profile_schema_version": 5,
        "run_id": run_id,
        "scenario": scenario,
        "scenario_seed": args.scenario_seed,
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "benchmark_profile": {
            "workers": args.workers,
            "min_size": args.min_size,
            "max_size": args.max_size if args.max_size is not None else args.workers,
            "step_size": args.step_size,
            "plateau_order": args.plateau_order,
            "roundtrips": args.roundtrips,
            "update_rounds": args.update_rounds,
            "max_update_samples_per_plateau": args.max_update_samples_per_plateau,
            "app_rounds": args.app_rounds,
            "max_app_samples_per_payload": args.max_app_samples_per_payload,
            "payload_sizes": args.payload_sizes,
            "worker_layout_mode": args.worker_layout_mode,
            "singleton_resource_envelope": singleton_resource_envelope(args),
            "process_pending_fanout": args.process_pending_fanout,
            "profile_only_singletons": args.profile_only_singletons,
            "disable_container_profiling": args.disable_container_profiling,
            "external_coverage_lane": args.external_coverage_lane,
            "randomized_payload_order": True,
            "randomized_external_actor_position": True,
        },
        "host": {
            "platform": platform.platform(),
            "uname": command_stdout(["uname", "-a"]),
            "cpu": collect_cpu_metadata(),
        },
        "git": {
            "benchmark_commit": command_stdout(["git", "rev-parse", "HEAD"], cwd=root),
            "benchmark_dirty_short": command_stdout(["git", "status", "--short"], cwd=root),
            "openmls_commit": command_stdout(["git", "-C", str(root / "openmls-main"), "rev-parse", "HEAD"]),
            "openmls_dirty_short": command_stdout(["git", "-C", str(root / "openmls-main"), "status", "--short"]),
        },
        "docker": {
            "version": command_stdout(["docker", "version", "--format", "{{json .}}"]),
            "info": command_stdout(["docker", "info", "--format", "{{json .}}"]),
        },
        "external_device_build": {
            "enabled": args.enable_external_devices,
            "device_ids": args.external_device_ids,
            "default_binary_path": str(external_binary),
            "default_binary_sha256": sha256_file(external_binary),
            "profile": "minsize",
            "rust_target": "armv7-unknown-linux-musleabihf",
            "devices": collect_external_device_build_metadata(root, args),
        },
    }
    (run_dir / "benchmark_run_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> int:
    args = build_parser().parse_args()
    if args.enable_external_devices:
        # External runs are publication inputs, so device coverage is mandatory rather
        # than an opt-in sampling mode.
        args.external_coverage_lane = True
    root = repo_root()

    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

    if args.bridge_count < 1:
        raise SystemExit("--bridge-count must be at least 1")

    if args.bridge_count > args.workers:
        raise SystemExit("--bridge-count must not exceed --workers")

    if args.startup_batch_size < 0:
        raise SystemExit("--startup-batch-size must be >= 0")

    if args.startup_batch_sleep_seconds < 0:
        raise SystemExit("--startup-batch-sleep-seconds must be >= 0")

    if args.compose_parallel_limit is not None and args.compose_parallel_limit < 1:
        raise SystemExit("--compose-parallel-limit must be >= 1")

    if args.compose_down_timeout_seconds < 0:
        raise SystemExit("--compose-down-timeout-seconds must be >= 0")

    if args.teardown_batch_size < 0:
        raise SystemExit("--teardown-batch-size must be >= 0")

    if args.teardown_batch_sleep_seconds < 0:
        raise SystemExit("--teardown-batch-sleep-seconds must be >= 0")

    if args.post_startup_settle_seconds < 0:
        raise SystemExit("--post-startup-settle-seconds must be >= 0")

    if args.worker_health_timeout_seconds < 1:
        raise SystemExit("--worker-health-timeout-seconds must be >= 1")

    if args.worker_health_poll_ms < 1:
        raise SystemExit("--worker-health-poll-ms must be >= 1")

    if args.max_fanout_parallelism < 0:
        raise SystemExit("--max-fanout-parallelism must be >= 0")

    if args.min_fanout_parallelism < 0:
        raise SystemExit("--min-fanout-parallelism must be >= 0")

    if args.fanout_adaptive and args.no_fanout_adaptive:
        raise SystemExit("--fanout-adaptive and --no-fanout-adaptive cannot both be set")

    if args.fanout_error_rate_threshold < 0:
        raise SystemExit("--fanout-error-rate-threshold must be >= 0")

    if args.fanout_p95_threshold_ms < 0:
        raise SystemExit("--fanout-p95-threshold-ms must be >= 0")

    if args.http_pool_max_idle_per_host < 0:
        raise SystemExit("--http-pool-max-idle-per-host must be >= 0")

    if args.worker_http_pool_max_idle_per_host < 1:
        raise SystemExit("--worker-http-pool-max-idle-per-host must be >= 1")

    if args.worker_http_connect_timeout_ms < 1:
        raise SystemExit("--worker-http-connect-timeout-ms must be >= 1")

    if args.worker_http_request_timeout_ms < 1:
        raise SystemExit("--worker-http-request-timeout-ms must be >= 1")

    if args.worker_outbound_http_permits < 1:
        raise SystemExit("--worker-outbound-http-permits must be >= 1")

    if args.host_health_parallelism < 1:
        raise SystemExit("--host-health-parallelism must be >= 1")

    if args.singleton_min_count < 1:
        raise SystemExit("--singleton-min-count must be >= 1")

    if not (0 < args.singleton_fraction <= 1):
        raise SystemExit("--singleton-fraction must be between 0 and 1")

    if args.packed_clients_per_container < 1:
        raise SystemExit("--packed-clients-per-container must be >= 1")

    if args.packed_worker_internal_parallelism < 1:
        raise SystemExit("--packed-worker-internal-parallelism must be >= 1")

    if args.resource_monitor_interval_ms < 1:
        raise SystemExit("--resource-monitor-interval-ms must be >= 1")

    if args.failure_experiment:
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

    normalize_resource_args(args)

    if args.enable_external_devices and not args.devices_file:
        raise SystemExit("--enable-external-devices requires --devices-file")

    run_id = args.run_id or timestamped_run_id(args.workers)
    try:
        validate_run_id(run_id)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    scenario = args.scenario
    output_dir_name = args.output_dir
    output_root = output_root_for(root, output_dir_name)
    run_dir = output_root / run_id
    if args.wipe_run_dir:
        safe_wipe_run_dir(run_dir, output_root, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    write_benchmark_metadata(run_dir, root, args, run_id, scenario)

    project_name = sanitize_project_name(run_id)

    compose_tmp = root / f"docker-compose.{run_id}.generated.yml"
    workers_internal_tmp = root / f"workers.{run_id}.txt"
    workers_host_tmp = root / f"workers.{run_id}.host.txt"
    layout_tmp = root / f"worker_layout.{run_id}.json"

    terminal_output_path = run_dir / "terminal_output.txt"
    compose_logs_path = run_dir / "compose_services.log"

    generator = root / "scripts" / "generate_compose.py"
    if not generator.exists():
        raise SystemExit(f"Missing generator script: {generator}")

    compose_up = False
    failure_seen = False
    external_device_stop_required = False
    resource_monitor: ResourceMonitor | None = None
    resource_monitor_stopped = False
    external_oom_monitor: ExternalOomMonitor | None = None
    external_oom_monitor_stopped = False
    resource_experiment = getattr(args, "resource_experiment", "none")
    resource_experiment_state = None
    preflight_passed = False

    compose_env = build_compose_env(args)

    try:
        if args.force_cleanup_mls_ports:
            docker_cleanup_mls_containers(root)

        check_required_ports(args)

        if args.build_images:
            run_cmd(
                ["docker", "build", "--target", "ds-runtime", "-t", "mls-ds", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "relay-runtime", "-t", "mls-relay", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "worker-runtime", "-t", "mls-worker", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "runner-runtime", "-t", "mls-runner", "."],
                cwd=root,
            )

        if args.build_external_binaries:
            devices_file = Path(args.devices_file) if args.devices_file else root / "devices.yaml"
            if not devices_file.is_absolute():
                devices_file = root / devices_file
            if not devices_file.exists():
                raise RuntimeError(f"Devices file not found: {devices_file}")
            from external_devices import load_devices_config
            configs = load_devices_config(devices_file)
            enabled = [c for c in configs if c.enabled]
            if args.external_device_ids:
                enabled = [c for c in enabled if c.id in args.external_device_ids]
            built = []
            for config in enabled:
                rust_target = config.target.get("rust_target")
                if not rust_target:
                    print(f"[build] WARNING: device '{config.id}' has no rust_target, skipping")
                    continue
                print(f"[build] Cross-compiling worker for device '{config.id}' ({rust_target}) ...")
                build_env = {
                    **os.environ,
                    "RUSTFLAGS": "-C linker=rust-lld",
                    "CC_aarch64_unknown_linux_musl": "aarch64-linux-gnu-gcc",
                }
                run_cmd(
                    ["cargo", "build", "--profile", "minsize", "--target", rust_target, "--bin", "worker"],
                    cwd=root,
                    env=build_env,
                )
                built.append(config.id)
            if built:
                print(f"[build] Built external worker binaries for: {', '.join(built)}")
            else:
                print("[build] No external device binaries were built")

        generator_cmd = [
            sys.executable,
            str(generator),
            "--workers",
            str(args.workers),
            "--run-id",
            run_id,
            "--scenario",
            scenario,
            "--scenario-seed",
            str(args.scenario_seed),
            "--output-dir",
            output_dir_name,
            "--compose-out",
            str(compose_tmp),
            "--workers-out",
            str(workers_internal_tmp),
            "--workers-host-out",
            str(workers_host_tmp),
            "--project-name",
            project_name,
            "--base-worker-port",
            str(args.base_worker_port),
            "--ds-port",
            str(args.ds_port),
            "--relay-port",
            str(args.relay_port),
            "--bridge-count",
            str(args.bridge_count),
            "--worker-layout-mode",
            args.worker_layout_mode,
            "--singleton-min-count",
            str(args.singleton_min_count),
            "--singleton-fraction",
            str(args.singleton_fraction),
            "--packed-clients-per-container",
            str(args.packed_clients_per_container),
            "--singleton-selection-seed",
            str(args.singleton_selection_seed),
            "--singleton-selection-strategy",
            args.singleton_selection_strategy,
            "--worker-layout-out",
            str(layout_tmp),
            "--packed-worker-internal-parallelism",
            str(args.packed_worker_internal_parallelism),
        ]
        if args.disable_container_profiling:
            generator_cmd.append("--disable-container-profiling")

        if args.singleton_cpus is not None:
            generator_cmd += ["--singleton-cpus", args.singleton_cpus]
        if args.singleton_memory is not None:
            generator_cmd += ["--singleton-memory", args.singleton_memory]
        if args.singleton_memory_swap is not None:
            generator_cmd += ["--singleton-memory-swap", args.singleton_memory_swap]
        if args.singleton_pids_limit is not None:
            generator_cmd += ["--singleton-pids-limit", str(args.singleton_pids_limit)]

        if args.runner_in_docker:
            generator_cmd.append("--include-runner")
        else:
            generator_cmd.append("--publish-workers")

        if args.include_netcheck:
            generator_cmd.append("--include-netcheck")
        if args.failure_experiment:
            generator_cmd.append("--failure-experiment")

        # ── Affinity planning for unconstrained container baselines ───
        if (
            resource_experiment == "none"
            and args.cpu_affinity_mode != "none"
            and args.profiled_singleton_count > 0
            and not args.disable_container_profiling
        ):
            from generate_compose import select_singleton_ids, compute_hybrid_layout
            from cpu_affinity_planner import (
                create_affinity_plan,
                validate_affinity_plan,
                write_affinity_plan_json,
            )

            layout_info = compute_hybrid_layout(
                args.workers, args.singleton_min_count,
                args.singleton_fraction, args.packed_clients_per_container,
            )
            singleton_ids = select_singleton_ids(
                args.workers, layout_info["singleton_count"],
                args.singleton_selection_seed, args.singleton_selection_strategy,
            )
            count = min(len(singleton_ids), args.profiled_singleton_count)
            profiled_singleton_ids = singleton_ids[:count]
            profiled_worker_ids = [f"worker-{cid}" for cid in profiled_singleton_ids]
            profiled_specs = [
                {
                    "worker_id": wid,
                    "container_name": f"worker-{cid}",
                    "logical_client_id": cid,
                    "experiment_kind": "unconstrained_container_baseline",
                    "resource_profile_id": "",
                    "rayon_num_threads": 1,
                }
                for wid, cid in zip(profiled_worker_ids, profiled_singleton_ids)
            ]
            profiled_cpu_counts = {wid: 1 for wid in profiled_worker_ids}
            bg_specs = [
                {"container_name": "ds", "container_role": "infrastructure"},
                {"container_name": "relay", "container_role": "infrastructure"},
            ]
            if args.runner_in_docker:
                bg_specs.append({"container_name": "runner", "container_role": "runner_or_helper"})
            profiled_id_set = set(profiled_singleton_ids)
            for cid in singleton_ids:
                if cid not in profiled_id_set:
                    bg_specs.append({
                        "container_name": f"worker-{cid}",
                        "container_role": "unprofiled_singleton",
                    })
            for p in range(layout_info["packed_container_count"]):
                bg_specs.append({
                    "container_name": f"worker-pack-{p:03d}",
                    "container_role": "packed",
                })

            print(f"[affinity] Computing baseline CPU affinity plan "
                  f"({len(profiled_specs)} profiled workers, "
                  f"{sum(profiled_cpu_counts.values())} total CPUs)...", flush=True)
            plan = create_affinity_plan(
                run_id=run_id,
                profiled_worker_specs=profiled_specs,
                background_specs=bg_specs,
                cpu_affinity_mode=args.cpu_affinity_mode,
                sample_seconds=args.cpu_affinity_sample_seconds,
                reserve_smt_siblings=args.reserve_smt_siblings,
                profiled_cpu_counts=profiled_cpu_counts,
            )
            affinity_errors = validate_affinity_plan(plan)
            if affinity_errors:
                raise RuntimeError(
                    "Invalid CPU affinity plan: " + "; ".join(affinity_errors)
                )
            for w in plan.warnings:
                print(f"[affinity] WARNING: {w}", flush=True)
            for pa in plan.profiled_assignments:
                print(f"[affinity]   {pa.container_name}: cpus={pa.assigned_cpus} "
                      f"rayon={pa.rayon_num_threads}")
            print(f"[affinity] Background cpuset: {len(plan.background_cpus)} CPUs", flush=True)
            os.makedirs(str(run_dir), exist_ok=True)
            generator_cmd += ["--affinity-plan-file", write_affinity_plan_json(plan, str(run_dir))]

        # ── Affinity planning for resource experiments ───────────────
        if resource_experiment != "none":
            from generate_compose import worker_id, select_singleton_ids, compute_hybrid_layout
            from cpu_mask_util import cpu_list_to_docker_cpuset
            from cpu_affinity_planner import (
                create_affinity_plan,
                validate_affinity_plan,
                write_affinity_plan_json,
            )
            from resource_profiles import (
                generate_ram_sweep_profiles,
                generate_cpu_matrix_profiles,
                generate_embedded_budget_profiles,
                generate_parallel_ram_sweep_profiles,
                generate_parallel_cpu_sweep_profiles,
                select_profile,
                select_all_profiles,
                get_selected_profile,
                get_selected_profile_index,
                get_group_creator_profile,
                get_group_creator_client_index,
            )
            from resource_experiment_sidecars import SidecarWriter

            layout_info = compute_hybrid_layout(
                args.workers, args.singleton_min_count,
                args.singleton_fraction, args.packed_clients_per_container,
            )
            singleton_ids = select_singleton_ids(
                args.workers, layout_info["singleton_count"],
                args.singleton_selection_seed, args.singleton_selection_strategy,
            )

            count = min(len(singleton_ids), args.profiled_singleton_count)
            profiled_singleton_ids = singleton_ids[:count]
            profiled_worker_ids = [f"worker-{cid}" for cid in profiled_singleton_ids]

            ekind = resource_experiment.replace("-", "_")
            if resource_experiment == "ram-sweep-singleton":
                ram_vals = [v.strip() for v in args.ram_sweep_values.split(",") if v.strip()]
                profiles = generate_ram_sweep_profiles(
                    ram_values=ram_vals,
                    assigned_cpu_count=args.ram_sweep_cpu_count,
                    run_id=run_id,
                )
            elif resource_experiment == "cpu-matrix-singleton":
                core_vals = [int(v.strip()) for v in args.cpu_matrix_core_counts.split(",") if v.strip()]
                frac_vals = [float(v.strip()) for v in args.cpu_matrix_capacity_fractions.split(",") if v.strip()]
                profiles = generate_cpu_matrix_profiles(
                    core_counts=core_vals,
                    capacity_fractions=frac_vals,
                    run_id=run_id,
                )
            elif resource_experiment == "ram-app-heap-sweep":
                profiles = generate_parallel_ram_sweep_profiles(
                    docker_memory_limit=getattr(args, "embedded_docker_memory", "4g"),
                    assigned_cpu_count=1,
                    run_id=run_id,
                )
            elif resource_experiment == "cpu-quota-sweep":
                cpu_sweep_fractions = [
                    float(v.strip())
                    for v in args.cpu_sweep_fractions.split(",")
                    if v.strip()
                ]
                profiles = generate_parallel_cpu_sweep_profiles(
                    cpu_fractions=cpu_sweep_fractions,
                    assigned_cpu_count=1,
                    run_id=run_id,
                )
            else:
                heap_vals = [v.strip() for v in args.embedded_heap_budgets.split(",") if v.strip()]
                core_vals = [int(v.strip()) for v in args.embedded_cpu_cores.split(",") if v.strip()]
                frac_vals = [float(v.strip()) for v in args.embedded_cpu_fractions.split(",") if v.strip()]
                profiles = generate_embedded_budget_profiles(
                    heap_budgets=heap_vals,
                    core_counts=core_vals,
                    capacity_fractions=frac_vals,
                    docker_memory_limit=args.embedded_docker_memory,
                    run_id=run_id,
                )

            # ── Select single profile for threshold mode ──
            profile_index = getattr(args, "resource_profile_index", None)
            profile_id = getattr(args, "resource_profile_id", None)

            is_parallel_sweep = resource_experiment in ("ram-app-heap-sweep", "cpu-quota-sweep")

            if is_parallel_sweep:
                profiles = select_all_profiles(profiles)
                selected_profile = select_profile(
                    profiles,
                    profile_index=profile_index,
                    profile_id=profile_id,
                    profiled_singleton_count=args.profiled_singleton_count,
                )
            else:
                selected_profile = select_profile(
                    profiles,
                    profile_index=profile_index,
                    profile_id=profile_id,
                    profiled_singleton_count=args.profiled_singleton_count,
                )
            actual_profile_index = get_selected_profile_index(profiles)
            selected_profile_id_str = (
                selected_profile.resource_profile_id
                if args.profiled_singleton_count == 1
                else ""
            )

            failure_policy = getattr(args, "resource_failure_policy", "stop-on-profiled-failure")

            if args.profiled_singleton_count == 1:
                print(f"[resource] Selected profile index={actual_profile_index} "
                      f"id={selected_profile_id_str} "
                      f"policy={failure_policy}", flush=True)

            profiled_specs = []
            profiled_cpu_counts = {}
            assigned_profiles = []

            if is_parallel_sweep:
                gc_profile = get_group_creator_profile(profiles)
                if gc_profile is not None and len(profiles) == args.profiled_singleton_count:
                    print(f"[parallel-sweep] Group creator: {gc_profile.resource_profile_id} "
                          f"({gc_profile.group_creator_reason})", flush=True)
                from resource_profiles import get_group_creator_client_index
                gc_idx = get_group_creator_client_index(profiles)
                if gc_idx is not None and gc_idx >= 0 and gc_idx < len(profiles):
                    reordered_indices = [gc_idx] + [i for i in range(len(profiles)) if i != gc_idx]
                else:
                    reordered_indices = list(range(len(profiles)))

                for i, (wid, cid) in enumerate(zip(profiled_worker_ids, profiled_singleton_ids)):
                    profile_idx = reordered_indices[i % len(reordered_indices)]
                    profile = profiles[profile_idx] if profiles else None
                    assigned_profiles.append(profile)
                    cpu_n = profile.assigned_cpu_count if profile else 1
                    rayon_n = cpu_n
                    profiled_specs.append({
                        "worker_id": wid,
                        "container_name": f"worker-{cid}",
                        "logical_client_id": cid,
                        "experiment_kind": ekind,
                        "resource_profile_id": profile.resource_profile_id if profile else "",
                        "rayon_num_threads": rayon_n,
                        "group_creator": getattr(profile, "group_creator", False) if profile else False,
                    })
                    profiled_cpu_counts[wid] = cpu_n
            else:
                for i, (wid, cid) in enumerate(zip(profiled_worker_ids, profiled_singleton_ids)):
                    profile = (
                        selected_profile
                        if args.profiled_singleton_count == 1
                        else profiles[i % len(profiles)]
                    ) if profiles else None
                    assigned_profiles.append(profile)
                    cpu_n = profile.assigned_cpu_count if profile else 1
                    rayon_n = cpu_n
                    profiled_specs.append({
                        "worker_id": wid,
                        "container_name": f"worker-{cid}",
                        "logical_client_id": cid,
                        "experiment_kind": ekind,
                        "resource_profile_id": profile.resource_profile_id if profile else "",
                        "rayon_num_threads": rayon_n,
                    })
                    profiled_cpu_counts[wid] = cpu_n

            bg_specs = [
                {"container_name": "ds", "container_role": "infrastructure"},
                {"container_name": "relay", "container_role": "infrastructure"},
            ]
            if args.runner_in_docker:
                bg_specs.append({"container_name": "runner", "container_role": "runner_or_helper"})

            profiled_id_set = set(profiled_singleton_ids)
            for cid in singleton_ids:
                if cid not in profiled_id_set:
                    bg_specs.append({
                        "container_name": f"worker-{cid}",
                        "container_role": "unprofiled_singleton",
                    })

            packed_count = layout_info["packed_container_count"]
            for p in range(packed_count):
                bg_specs.append({
                    "container_name": f"worker-pack-{p:03d}",
                    "container_role": "packed",
                })

            affinity_mode = args.cpu_affinity_mode
            if affinity_mode == "none" and resource_experiment not in ("ram-app-heap-sweep", "cpu-quota-sweep"):
                affinity_mode = "profiled-nor-background"

            print(f"[affinity] Computing CPU affinity plan "
                  f"({len(profiled_specs)} profiled workers, "
                  f"{sum(profiled_cpu_counts.values())} total CPUs)...", flush=True)

            plan = create_affinity_plan(
                run_id=run_id,
                profiled_worker_specs=profiled_specs,
                background_specs=bg_specs,
                cpu_affinity_mode=affinity_mode,
                sample_seconds=args.cpu_affinity_sample_seconds,
                reserve_smt_siblings=args.reserve_smt_siblings,
                profiled_cpu_counts=profiled_cpu_counts,
            )

            affinity_errors = validate_affinity_plan(plan)
            if affinity_errors:
                raise RuntimeError(
                    "Invalid CPU affinity plan: " + "; ".join(affinity_errors)
                )

            for w in plan.warnings:
                print(f"[affinity] WARNING: {w}", flush=True)

            for i, pa in enumerate(plan.profiled_assignments):
                profile = assigned_profiles[i] if i < len(assigned_profiles) else None
                if profile:
                    profile.cpuset_cpus = cpu_list_to_docker_cpuset(pa.assigned_cpus)
                    profile.cpuset_mask_hex = pa.assigned_mask_hex
                print(f"[affinity]   {pa.container_name}: cpus={pa.assigned_cpus} "
                      f"rayon={pa.rayon_num_threads} profile={pa.resource_profile_id}")

            bg_cpus = plan.background_cpus
            print(f"[affinity] Background cpuset: {len(bg_cpus)} CPUs", flush=True)

            os.makedirs(str(run_dir), exist_ok=True)

            plan_path = write_affinity_plan_json(plan, str(run_dir))
            profiles_path = str(run_dir / "resource_profiles.json")
            profiles_for_assignment = (
                [profile for profile in assigned_profiles if profile is not None]
                if is_parallel_sweep and assigned_profiles
                else profiles
            )
            profiles_data = [p.to_dict() for p in profiles_for_assignment]
            with open(profiles_path, "w") as f:
                json.dump(profiles_data, f, indent=2)

            sw = SidecarWriter(str(run_dir))
            sw.write_resource_profiles(run_id, [p.to_dict() for p in profiles_for_assignment])

            generator_cmd += [
                "--resource-experiment", resource_experiment,
                "--profiled-singleton-count", str(args.profiled_singleton_count),
                "--ram-sweep-values", args.ram_sweep_values,
                "--ram-sweep-cpu-count", str(args.ram_sweep_cpu_count),
                "--cpu-matrix-core-counts", args.cpu_matrix_core_counts,
                "--cpu-matrix-capacity-fractions", args.cpu_matrix_capacity_fractions,
                "--embedded-heap-budgets", args.embedded_heap_budgets,
                "--embedded-cpu-fractions", args.embedded_cpu_fractions,
                "--embedded-cpu-cores", args.embedded_cpu_cores,
                "--embedded-docker-memory", args.embedded_docker_memory,
                "--affinity-plan-file", plan_path,
                "--resource-profiles-file", profiles_path,
            ]

            # Store for later lifecycle phases
            resource_experiment_state = {
                "plan": plan,
                "profiles": profiles_for_assignment,
                "selected_profile": selected_profile,
                "selected_profile_index": actual_profile_index,
                "selected_profile_id": selected_profile_id_str,
                "memory_model": getattr(selected_profile, "memory_model", ""),
                "docker_memory_limit": getattr(selected_profile, "docker_memory_limit", None) or getattr(selected_profile, "memory_limit", ""),
                "app_heap_budget": getattr(selected_profile, "app_heap_budget", "") or "",
                "app_heap_budget_bytes": getattr(selected_profile, "app_heap_budget_bytes", 0) or 0,
                "sweep_kind": getattr(selected_profile, "sweep_kind", ""),
                "strict_cpuset_satisfied": getattr(selected_profile, "strict_cpuset_satisfied", False),
                "failure_policy": failure_policy,
                "profiled_singleton_ids": profiled_singleton_ids,
                "profiled_worker_ids": profiled_worker_ids,
                "profiled_container_names": [f"worker-{cid}" for cid in profiled_singleton_ids],
                "bg_specs": bg_specs,
                "packed_count": packed_count,
                "layout_info": layout_info,
                "experiment_kind": ekind,
                "resource_experiment": resource_experiment,
            }

        run_cmd(generator_cmd, cwd=root)

        copy_if_exists(compose_tmp, run_dir / "docker-compose.generated.yml")
        copy_if_exists(workers_internal_tmp, run_dir / "workers.txt")
        copy_if_exists(workers_host_tmp, run_dir / "workers.host.txt")
        copy_if_exists(layout_tmp, run_dir / "worker_layout.json")
        warn_if_neighbor_cache_tight(layout_tmp)

        if resource_experiment_state is not None:
            from resource_experiment_runner import build_worker_resource_assignments

            state = resource_experiment_state
            packed_names = [
                f"worker-pack-{index:03d}" for index in range(state["packed_count"])
            ]
            infrastructure_names = ["ds", "relay"]
            if args.runner_in_docker:
                infrastructure_names.append("runner")
            assignments = build_worker_resource_assignments(
                run_id=run_id,
                plan=state["plan"],
                profiles=state["profiles"],
                selected_profile_index=state["selected_profile_index"],
                singleton_worker_ids=state["profiled_worker_ids"],
                singleton_client_ids=state["profiled_singleton_ids"],
                singleton_container_names=state["profiled_container_names"],
                packed_container_names=packed_names,
                infrastructure_container_names=infrastructure_names,
            )
            SidecarWriter(str(run_dir)).write_worker_resource_assignments(
                run_id, assignments
            )

        try:
            if args.startup_batch_size > 0:
                print("[compose] starting ds and relay")
                run_cmd(
                    ["docker", "compose", "-f", str(compose_tmp), "up", "-d", "ds", "relay"],
                    cwd=root,
                    env=compose_env,
                )
                compose_up = True

                if args.startup_batch_sleep_seconds > 0:
                    time.sleep(args.startup_batch_sleep_seconds)

                worker_services_result = subprocess.run(
                    ["docker", "compose", "-f", str(compose_tmp), "config", "--services"],
                    cwd=str(root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                all_services = [
                    s.strip()
                    for s in worker_services_result.stdout.splitlines()
                    if s.strip().startswith("worker-")
                ]

                for start in range(0, len(all_services), args.startup_batch_size):
                    batch = all_services[start:start + args.startup_batch_size]
                    print(
                        f"[compose] starting workers {batch[0]} .. {batch[-1]} "
                        f"({start + len(batch)}/{len(all_services)})"
                    )

                    run_cmd(
                        ["docker", "compose", "-f", str(compose_tmp), "up", "-d", *batch],
                        cwd=root,
                        env=compose_env,
                    )

                    if args.startup_batch_sleep_seconds > 0:
                        time.sleep(args.startup_batch_sleep_seconds)
            else:
                run_cmd(
                    ["docker", "compose", "-f", str(compose_tmp), "up", "-d"],
                    cwd=root,
                    env=compose_env,
                )
                compose_up = True

        except subprocess.CalledProcessError as e:
            failure_seen = True
            compose_up = True
            write_compose_logs(root, compose_tmp, compose_logs_path, append=False)
            collect_failure_diagnostics(
                root=root,
                compose_file=compose_tmp,
                run_dir=run_dir,
                args=args,
                compose_env=compose_env,
                project_name=project_name,
                terminal_output_path=terminal_output_path,
            )

            raise RuntimeError(
                "docker compose up failed.\n"
                f"See compose logs in: {compose_logs_path}\n"
                f"Original error: {e}"
            ) from e

        if args.post_startup_settle_seconds > 0:
            print(
                f"[compose] settling for {args.post_startup_settle_seconds:.1f}s "
                "before health checks",
                flush=True,
            )
            time.sleep(args.post_startup_settle_seconds)

        if args.cpu_affinity_mode == "none" and resource_experiment in ("ram-app-heap-sweep", "cpu-quota-sweep"):
            resource_targets = []
            print("[resources] CPU affinity disabled in parallel sweep mode; "
                  "skipping container resource limit verification", flush=True)
        else:
            resource_targets = verify_resource_limits(
                root=root,
                compose_file=compose_tmp,
                run_dir=run_dir,
                layout_path=run_dir / "worker_layout.json",
                args=args,
                compose_env=compose_env,
            )
        if resource_targets and not args.no_resource_monitor:
            resource_monitor = ResourceMonitor(
                root=root,
                run_dir=run_dir,
                run_id=run_id,
                targets=resource_targets,
                interval_ms=args.resource_monitor_interval_ms,
                compose_env=compose_env,
            )
            resource_monitor.start()
        elif resource_targets:
            print("[resources] resource monitor disabled by --no-resource-monitor", flush=True)

        print(f"[health] waiting for ds on http://127.0.0.1:{args.ds_port}/health", flush=True)
        wait_for_health(
            f"http://127.0.0.1:{args.ds_port}/health",
            args.health_timeout_seconds,
            args.health_poll_seconds,
        )
        print("[health] ds ok", flush=True)

        print(f"[health] waiting for relay on http://127.0.0.1:{args.relay_port}/health", flush=True)
        wait_for_health(
            f"http://127.0.0.1:{args.relay_port}/health",
            args.health_timeout_seconds,
            args.health_poll_seconds,
        )
        print("[health] relay ok", flush=True)

        if args.include_netcheck:
            print("[netcheck] starting continuous network monitor", flush=True)
            run_cmd(
                ["docker", "compose", "-f", str(compose_tmp), "up", "-d", "netcheck"],
                cwd=root,
                env=compose_env,
            )
            print(f"[netcheck] writing continuous log to {run_dir / 'netcheck.log'}", flush=True)

        if args.runner_in_docker:
            print("[health] skipping host worker health checks; runner will check workers inside Docker network")
        else:
            wait_for_workers_health_parallel(
                read_worker_lines(workers_host_tmp),
                args.health_timeout_seconds,
                args.health_poll_seconds,
                args.host_health_parallelism,
            )

        # ---- External device orchestration ---------------------------------
        external_layout_clients: list[dict] = []
        external_layout_workers: list[dict] = []
        external_worker_lines: list[str] = []

        if args.enable_external_devices and args.devices_file:
            external_device_stop_required = not args.no_device_stop_after_run
            ext_clients, ext_workers, ext_lines = launch_external_devices(
                args, root, args.ds_port, args.relay_port, run_id,
            )
            external_layout_clients = ext_clients
            external_layout_workers = ext_workers
            external_worker_lines = ext_lines
            external_oom_monitor = ExternalOomMonitor(
                run_dir=run_dir,
                targets=external_oom_monitor_targets(args, root),
                interval_ms=args.resource_monitor_interval_ms,
            )
            external_oom_monitor.start()

            # Merge external devices into worker layout
            layout_path = run_dir / "worker_layout.json"
            if layout_path.exists():
                layout_data = json.loads(layout_path.read_text(encoding="utf-8"))
                layout_data["clients"] = insert_after_leader(
                    layout_data.get("clients", []),
                    external_layout_clients,
                )
                layout_data["physical_workers"].extend(external_layout_workers)
                layout_data["logical_worker_count"] = len(layout_data["clients"])
                layout_data["physical_worker_count"] = len(layout_data["physical_workers"])
                layout_path.write_text(json.dumps(layout_data, indent=2), encoding="utf-8")
                # Also update the temp layout file for the runner
                if layout_tmp.exists():
                    layout_tmp.write_text(json.dumps(layout_data, indent=2), encoding="utf-8")

            # Merge external devices into workers files
            combined_workers = run_dir / "workers.combined.txt"
            combined_host_workers = run_dir / "workers.combined.host.txt"

            # Start with Docker internal workers
            internal_lines = read_worker_lines(workers_internal_tmp) if workers_internal_tmp.exists() else []
            internal_lines = insert_after_leader(internal_lines, external_worker_lines)
            combined_workers.write_text("\n".join(internal_lines) + "\n", encoding="utf-8")

            # Host workers
            host_lines = read_worker_lines(workers_host_tmp) if workers_host_tmp.exists() else []
            host_lines = insert_after_leader(host_lines, external_worker_lines)
            combined_host_workers.write_text("\n".join(host_lines) + "\n", encoding="utf-8")

            print(f"[device] merged {len(external_layout_clients)} external device(s) into layout/workers files", flush=True)

        # ---- Decide which layout / workers files to use ----
        effective_layout_tmp = layout_tmp
        effective_workers_host_tmp = workers_host_tmp
        effective_workers_internal_tmp = workers_internal_tmp

        if args.enable_external_devices and external_worker_lines:
            combined_host = run_dir / "workers.combined.host.txt"
            combined_internal = run_dir / "workers.combined.txt"
            if combined_host.exists():
                effective_workers_host_tmp = combined_host
            if combined_internal.exists():
                effective_workers_internal_tmp = combined_internal

        if not args.runner_in_docker and layout_tmp.exists():
            host_layout_path = run_dir / "worker_layout.host.json"
            host_layout_data = build_host_runner_layout(
                json.loads(layout_tmp.read_text(encoding="utf-8")),
                read_worker_lines(effective_workers_host_tmp),
            )
            host_layout_path.write_text(
                json.dumps(host_layout_data, indent=2),
                encoding="utf-8",
            )
            effective_layout_tmp = host_layout_path

        layout_path_for_runner = f"/results/{run_id}/worker_layout.json" if args.runner_in_docker else str(effective_layout_tmp)
        workers_file_for_docker_runner = (
            f"/results/{run_id}/workers.combined.txt"
            if args.enable_external_devices and external_worker_lines
            else f"/results/{run_id}/workers.txt"
        )

        use_no_aggregate = args.no_aggregate or args.enable_external_devices

        # ── Resource experiment preflight ──────────────────────────
        resource_experiment_integration_obj = None
        preflight_passed = True
        if resource_experiment != "none" and resource_experiment_state is not None:
            state = resource_experiment_state  # noqa: F821

            skip_preflight = (args.cpu_affinity_mode == "none"
                              and resource_experiment in ("ram-app-heap-sweep", "cpu-quota-sweep"))

            if skip_preflight:
                print("[preflight] Skipping CPU affinity preflight in parallel sweep mode "
                      "with cpu-affinity-mode=none", flush=True)
                preflight_results = []
                all_preflight_passed = True

            if not skip_preflight:
                from preflight_checks import run_preflight as re_run_preflight
                from resource_experiment_failures import build_run_status as re_build_run_status_pf

                # Docker compose names containers as <project>-<service>-<replica>
                profiled_names = [f"{project_name}-{name}-1" for name in state["profiled_container_names"]]
                bg_names_raw = [s["container_name"] for s in state["bg_specs"]]
                bg_names_raw = [name for name in bg_names_raw if name != "runner"]
                bg_names = [f"{project_name}-{name}-1" for name in bg_names_raw]

                profiled_containers_preflight = []
                for idx, name in enumerate(profiled_names):
                    short_name = state["profiled_container_names"][idx]
                    cpuset = ""
                    rayon = 0
                    for pa in state["plan"].profiled_assignments:
                        if pa.container_name == short_name:
                            cpuset = cpu_list_to_docker_cpuset(pa.assigned_cpus)
                            rayon = pa.rayon_num_threads
                            break
                    profiled_containers_preflight.append({
                        "container_name": name,
                        "container_role": "profiled_singleton",
                        "expected_cpuset": cpuset,
                        "expected_rayon_threads": rayon,
                    })

                bg_containers_preflight = []
                bg_cpuset_str = cpu_list_to_docker_cpuset(state["plan"].background_cpus)
                for name in bg_names:
                    bg_containers_preflight.append({
                        "container_name": name,
                        "container_role": "background",
                        "expected_cpuset": bg_cpuset_str,
                    })

                print("[preflight] Running CPU affinity preflight checks...", flush=True)
                preflight_results, all_preflight_passed = re_run_preflight(
                    run_id=run_id,
                    profiled_containers=profiled_containers_preflight,
                    background_containers=bg_containers_preflight,
                    affinity_plan=state["plan"],
                )

                sw_pf = SidecarWriter(str(run_dir))
                sw_pf.write_preflight_results(run_id, preflight_results)

                pf_summary = {
                    "run_id": run_id,
                    "all_passed": all_preflight_passed,
                    "fail_count": sum(1 for r in preflight_results if r.get("status") == "FAIL"),
                    "warn_count": sum(1 for r in preflight_results if r.get("status") == "WARN"),
                    "checked_container_count": len(preflight_results),
                    "checked_thread_count": 0,
                    "profiled_mask_hex": state["plan"].profiled_mask_hex,
                    "reserved_mask_hex": state["plan"].reserved_mask_hex,
                    "background_mask_hex": state["plan"].background_mask_hex,
                }
                pf_summary_path = run_dir / "cpu_affinity_preflight_summary.json"
                with open(pf_summary_path, "w") as f:
                    json.dump(pf_summary, f, indent=2)

                if not all_preflight_passed:
                    preflight_passed = False
                    fail_rows = [r for r in preflight_results if r.get("status") == "FAIL"]
                    fail_msgs = "\n".join(
                        f"  {r.get('check_name')}: {r.get('message')}" for r in fail_rows
                    )
                    print(f"[preflight] FAILED: {len(fail_rows)} check(s) failed:\n{fail_msgs}", flush=True)

                    sw_pf.write_run_status(run_id, re_build_run_status_pf(
                        run_id=run_id,
                        run_mode=state["resource_experiment"],
                        experiment_kind=state["experiment_kind"],
                        run_success=False,
                        worker_failures=[],
                        resource_experiment=state["resource_experiment"],
                        resource_failure_policy=state["failure_policy"],
                        resource_profile_index=state["selected_profile_index"],
                        resource_profile_id=state["selected_profile_id"],
                        preflight_passed=False,
                        output_validation_passed=False,
                        memory_model=state.get("memory_model", ""),
                        docker_memory_limit=state.get("docker_memory_limit", ""),
                        app_heap_budget=state.get("app_heap_budget", ""),
                        app_heap_budget_bytes=state.get("app_heap_budget_bytes", 0),
                        sweep_kind=state.get("sweep_kind", ""),
                        strict_cpuset_satisfied=state.get("strict_cpuset_satisfied", False),
                        notes=f"Affinity preflight failed: {fail_msgs}",
                    ))

                    raise RuntimeError(
                        f"CPU affinity preflight failed with {len(fail_rows)} FAIL(s). "
                        f"Workload blocked. See cpu_affinity_preflight.csv for details."
                    )
                else:
                    print(f"[preflight] All {len(preflight_results)} checks PASSED "
                          f"({pf_summary['warn_count']} warnings)", flush=True)

        # ── Benchmark command construction ──────────────────────────

        if args.runner_in_docker:
            benchmark_cmd = [
                "docker",
                "compose",
                "-f",
                str(compose_tmp),
                "run",
                "--rm",
                "runner",
                "--ds-url",
                f"http://ds:{args.ds_port}",
                "--workers-file",
                workers_file_for_docker_runner,
                "--worker-layout",
                layout_path_for_runner,
                "--min-size",
                str(args.min_size),
                "--max-size",
                str(args.max_size if args.max_size is not None else args.workers),
                "--step-size",
                str(args.step_size),
                "--plateau-order",
                args.plateau_order,
                "--roundtrips",
                str(args.roundtrips),
                "--update-rounds",
                str(args.update_rounds),
                "--max-update-samples-per-plateau",
                str(args.max_update_samples_per_plateau),
                "--app-rounds",
                str(args.app_rounds),
                "--max-app-samples-per-payload",
                str(args.max_app_samples_per_payload),
                "--payload-sizes",
                args.payload_sizes,
                "--worker-health-timeout-seconds",
                str(args.worker_health_timeout_seconds),
                "--worker-health-poll-ms",
                str(args.worker_health_poll_ms),
                "--max-fanout-parallelism",
                str(args.max_fanout_parallelism),
                "--min-fanout-parallelism",
                str(args.min_fanout_parallelism),
                *(["--fanout-adaptive"] if args.fanout_adaptive else []),
                *(["--no-fanout-adaptive"] if args.no_fanout_adaptive else []),
                "--fanout-error-rate-threshold",
                str(args.fanout_error_rate_threshold),
                "--fanout-p95-threshold-ms",
                str(args.fanout_p95_threshold_ms),
                "--http-pool-max-idle-per-host",
                str(args.http_pool_max_idle_per_host),
                *(["--process-pending-fanout"] if args.process_pending_fanout else []),
                *(["--preflight-only"] if args.preflight_only else []),
                *(["--profile-only-singletons"] if args.profile_only_singletons else []),
                *(["--external-coverage-lane"] if args.external_coverage_lane else []),
                *(["--no-aggregate"] if use_no_aggregate else []),
                *(["--failure-experiment"] if args.failure_experiment else []),
                "--profiled-failure-policy",
                args.resource_failure_policy,
                "--run-id",
                run_id,
                "--scenario",
                scenario,
                "--scenario-seed",
                str(args.scenario_seed),
                "--output-dir",
                "/results",
            ]
        else:
            benchmark_cmd = [
                "cargo",
                "run",
                "--bin",
                "benchmark_runner_http_staircase",
                "--",
                "--ds-url",
                f"http://127.0.0.1:{args.ds_port}",
                "--workers-file",
                str(effective_workers_host_tmp),
                "--worker-layout",
                layout_path_for_runner,
                "--min-size",
                str(args.min_size),
                "--max-size",
                str(args.max_size if args.max_size is not None else args.workers),
                "--step-size",
                str(args.step_size),
                "--plateau-order",
                args.plateau_order,
                "--roundtrips",
                str(args.roundtrips),
                "--update-rounds",
                str(args.update_rounds),
                "--max-update-samples-per-plateau",
                str(args.max_update_samples_per_plateau),
                "--app-rounds",
                str(args.app_rounds),
                "--max-app-samples-per-payload",
                str(args.max_app_samples_per_payload),
                "--payload-sizes",
                args.payload_sizes,
                "--worker-health-timeout-seconds",
                str(args.worker_health_timeout_seconds),
                "--worker-health-poll-ms",
                str(args.worker_health_poll_ms),
                "--max-fanout-parallelism",
                str(args.max_fanout_parallelism),
                "--min-fanout-parallelism",
                str(args.min_fanout_parallelism),
                *(["--fanout-adaptive"] if args.fanout_adaptive else []),
                *(["--no-fanout-adaptive"] if args.no_fanout_adaptive else []),
                "--fanout-error-rate-threshold",
                str(args.fanout_error_rate_threshold),
                "--fanout-p95-threshold-ms",
                str(args.fanout_p95_threshold_ms),
                "--http-pool-max-idle-per-host",
                str(args.http_pool_max_idle_per_host),
                *(["--process-pending-fanout"] if args.process_pending_fanout else []),
                *(["--preflight-only"] if args.preflight_only else []),
                *(["--profile-only-singletons"] if args.profile_only_singletons else []),
                *(["--external-coverage-lane"] if args.external_coverage_lane else []),
                *(["--no-aggregate"] if use_no_aggregate else []),
                *(["--failure-experiment"] if args.failure_experiment else []),
                "--profiled-failure-policy",
                args.resource_failure_policy,
                "--run-id",
                run_id,
                "--scenario",
                scenario,
                "--scenario-seed",
                str(args.scenario_seed),
                "--output-dir",
                output_dir_name,
            ]

        print("[runner] starting benchmark runner", flush=True)
        print("[runner] " + " ".join(benchmark_cmd), flush=True)

        exit_code = tee_subprocess_output(
            benchmark_cmd,
            cwd=root,
            output_path=terminal_output_path,
            env=compose_env,
        )

        if resource_monitor and not resource_monitor_stopped:
            resource_monitor.stop()
            resource_monitor_stopped = True
        if external_oom_monitor and not external_oom_monitor_stopped:
            external_oom_monitor.stop()
            external_oom_monitor_stopped = True

        if exit_code != 0:
            failure_seen = True
            outcome_class, evidence = classify_failure_from_resource_summary(run_dir)
            evidence["runner_exit_code"] = exit_code

            # ── Run partial aggregation on failure ──────────────────
            try:
                layout_file = run_dir / "worker_layout.json"
                workers_file = run_dir / "workers.combined.txt"
                agg_exit = run_standalone_aggregation(
                    run_dir,
                    layout_file if layout_file.exists() else None,
                    workers_file if workers_file.exists() else None,
                    allow_partial=True,
                    manifest_path=run_dir / "aggregation_manifest.json",
                )
                if agg_exit != 0:
                    print(f"[aggregate] partial aggregation exit={agg_exit}, continuing", flush=True)
            except Exception as agg_exc:
                print(f"[aggregate] partial aggregation exception: {agg_exc}", flush=True)

            # ── Capture container evidence before cleanup ──────────
            if resource_experiment != "none":
                try:
                    profiled_cnames = resource_experiment_state.get("profiled_container_names", []) if "resource_experiment_state" in dir() else []
                    capture_failure_evidence(
                        root=root,
                        compose_file=compose_tmp,
                        run_dir=run_dir,
                        compose_env=compose_env,
                        project_name=project_name,
                        profiled_container_names=profiled_cnames + ["ds", "relay", "runner"],
                    )
                except Exception:
                    pass

            # ── Write resource experiment failure sidecars ─────────
            # Aggregate first so runner-events.jsonl is authoritative for both
            # the operation cursor and the failure reason.
            if resource_experiment != "none":
                try:
                    from resource_experiment_failures import (
                        append_synthetic_runner_failure_event,
                        build_run_status as re_build_run_status,
                        collect_worker_failures_from_artifacts,
                        write_synthetic_failure_events_csv,
                        worker_failure_info_to_dict,
                    )
                    from resource_experiment_sidecars import SidecarWriter as RESidecarWriter

                    sw_re = RESidecarWriter(str(run_dir))
                    state = resource_experiment_state  # noqa: F821
                    summary_path = run_dir / "resource_summary.csv"
                    resource_summaries = []
                    if summary_path.exists():
                        import csv as csv_mod
                        with open(summary_path, newline="") as handle:
                            resource_summaries = list(csv_mod.DictReader(handle))

                    events_path = run_dir / "events.csv"
                    worker_failures = collect_worker_failures_from_artifacts(
                        str(events_path),
                        resource_summaries,
                        str(run_dir / "oom_events.jsonl"),
                        str(terminal_output_path),
                    )
                    app_heap_failures = [
                        wf for wf in worker_failures
                        if wf.failure_class in (
                            "app_heap_budget_exceeded",
                            "app_heap_budget_allocator_abort",
                            "embedded_budget_timeout",
                        )
                    ]
                    if app_heap_failures:
                        outcome_class = app_heap_failures[0].failure_class
                        evidence["app_heap_budget_failure"] = worker_failure_info_to_dict(
                            app_heap_failures[0]
                        )

                    synthesized = False
                    for worker_failure in worker_failures:
                        if worker_failure.failure_timestamp_ns == 0:
                            worker_failure.failure_timestamp_ns = int(time.time_ns())
                        worker_failure.diagnostic_log_path = str(run_dir / "compose_services.log")
                        if worker_failure.attribution_source in (
                            "resource_monitor",
                            "app_heap_budget_failure_payload",
                        ):
                            synthesized = append_synthetic_runner_failure_event(
                                str(run_dir), worker_failure, str(events_path)
                            ) or synthesized

                    if synthesized:
                        agg_exit = run_standalone_aggregation(
                            run_dir,
                            layout_file if layout_file.exists() else None,
                            workers_file if workers_file.exists() else None,
                            allow_partial=True,
                            manifest_path=run_dir / "aggregation_manifest.json",
                        )
                        if agg_exit == 0:
                            worker_failures = collect_worker_failures_from_artifacts(
                                str(events_path),
                                resource_summaries,
                                str(run_dir / "oom_events.jsonl"),
                                str(terminal_output_path),
                            )
                            for worker_failure in worker_failures:
                                worker_failure.diagnostic_log_path = str(
                                    run_dir / "compose_services.log"
                                )

                    write_synthetic_failure_events_csv(str(events_path), worker_failures)

                    sw_re.write_worker_failures(
                        run_id,
                        [worker_failure_info_to_dict(wf) for wf in worker_failures],
                    )
                    sw_re.write_run_status(
                        run_id,
                        re_build_run_status(
                            run_id=run_id,
                            run_mode=state["resource_experiment"],
                            experiment_kind=state["experiment_kind"],
                            run_success=False,
                            worker_failures=worker_failures,
                            resource_experiment=state["resource_experiment"],
                            resource_failure_policy=state["failure_policy"],
                            resource_profile_index=state["selected_profile_index"],
                            resource_profile_id=state["selected_profile_id"],
                            preflight_passed=preflight_passed,
                            output_validation_passed=False,
                            memory_model=state.get("memory_model", ""),
                            docker_memory_limit=state.get("docker_memory_limit", ""),
                            app_heap_budget=state.get("app_heap_budget", ""),
                            app_heap_budget_bytes=state.get("app_heap_budget_bytes", 0),
                            sweep_kind=state.get("sweep_kind", ""),
                            strict_cpuset_satisfied=state.get("strict_cpuset_satisfied", False),
                            notes=f"Runner exit code {exit_code}",
                        ),
                    )
                except Exception as re_exc:
                    print(f"[resource] Failed to write failure sidecars: {re_exc}", flush=True)

            write_artifact_validation(
                run_dir,
                run_success=False,
                primary_outcome="failed_workload",
                failure_class=outcome_class,
                aggregation_status="partial_success",
            )

            write_run_outcome(run_dir, outcome_class, evidence)
            collect_failure_diagnostics(
                root=root,
                compose_file=compose_tmp,
                run_dir=run_dir,
                args=args,
                compose_env=compose_env,
                project_name=project_name,
                terminal_output_path=terminal_output_path,
            )
            raise RuntimeError(f"Benchmark runner exited with code {exit_code}")

        # ---- Post-run: pull external device profiles -----------------------
        if args.enable_external_devices and args.devices_file:
            settle = 3.0
            print(f"[device] settling {settle}s for external device workers to finish writing profiles...", flush=True)
            time.sleep(settle)
            print("[device] pulling external device profiles...", flush=True)
            pull_external_device_profiles(args, root, run_id, run_dir)

        if use_no_aggregate and not args.preflight_only:
            layout_file, workers_file = standalone_aggregation_inputs(run_dir)
            agg_exit = run_standalone_aggregation(run_dir, layout_file, workers_file)
            if agg_exit != 0:
                print(f"[aggregate] standalone aggregation had non-zero exit, but continuing", flush=True)

        if not args.preflight_only:
            validate_artifacts(
                run_dir,
                args.worker_layout_mode,
                require_aggregate=not use_no_aggregate,
            )
            if use_no_aggregate:
                print(
                    "[aggregate] --no-aggregate used; skipping integrated aggregation manifest",
                    flush=True,
                )
            else:
                write_integrated_aggregation_manifest(run_dir, run_id)

        # Stop external device workers
        if external_device_stop_required:
            stop_external_device_workers(args, root)
            external_device_stop_required = False

        write_compose_logs(root, compose_tmp, compose_logs_path, append=False)

        # ── Resource experiment sidecar generation ─────────────────
        # Must run BEFORE strict validation so sidecars exist even
        # if publication validation flags non-critical issues.
        output_validation_passed = True
        if resource_experiment != "none":
            try:
                from resource_experiment_runner import build_worker_resource_assignments
                from resource_experiment_failures import (
                    build_run_status as re_build_run_status,
                    collect_worker_failures_from_artifacts,
                    worker_failure_info_to_dict,
                )
                from resource_experiment_sidecars import SidecarWriter as RESidecarWriter

                sw_re = RESidecarWriter(str(run_dir))
                state = resource_experiment_state  # noqa: F821

                packed_names = [f"worker-pack-{p:03d}" for p in range(state["packed_count"])]
                infra_names = ["ds", "relay"]
                if args.runner_in_docker:
                    infra_names.append("runner")

                assignments = build_worker_resource_assignments(
                    run_id=run_id,
                    plan=state["plan"],
                    profiles=state["profiles"],
                    selected_profile_index=state["selected_profile_index"],
                    singleton_worker_ids=state["profiled_worker_ids"],
                    singleton_client_ids=state["profiled_singleton_ids"],
                    singleton_container_names=state["profiled_container_names"],
                    packed_container_names=packed_names,
                    infrastructure_container_names=infra_names,
                )
                sw_re.write_worker_resource_assignments(run_id, assignments)

                summary_path = run_dir / "resource_summary.csv"
                resource_summaries = []
                if summary_path.exists():
                    import csv as csv_mod
                    with open(summary_path, newline="") as handle:
                        resource_summaries = list(csv_mod.DictReader(handle))

                events_path = run_dir / "events.csv"
                worker_failures = collect_worker_failures_from_artifacts(
                    str(events_path),
                    resource_summaries,
                    str(run_dir / "oom_events.jsonl"),
                    str(terminal_output_path),
                )
                for worker_failure in worker_failures:
                    worker_failure.diagnostic_log_path = str(run_dir / "compose_services.log")

                failure_dicts = [worker_failure_info_to_dict(wf) for wf in worker_failures]
                if failure_dicts:
                    sw_re.write_worker_failures(run_id, failure_dicts)
                else:
                    sw_re.write_worker_failures(run_id, [])

                run_status = re_build_run_status(
                    run_id=run_id,
                    run_mode=state["resource_experiment"],
                    experiment_kind=state["experiment_kind"],
                    run_success=True,
                    worker_failures=worker_failures,
                    resource_experiment=state["resource_experiment"],
                    resource_failure_policy=state["failure_policy"],
                    resource_profile_index=state["selected_profile_index"],
                    resource_profile_id=state["selected_profile_id"],
                    preflight_passed=preflight_passed,
                    output_validation_passed=True,
                    memory_model=state.get("memory_model", ""),
                    docker_memory_limit=state.get("docker_memory_limit", ""),
                    app_heap_budget=state.get("app_heap_budget", ""),
                    app_heap_budget_bytes=state.get("app_heap_budget_bytes", 0),
                    sweep_kind=state.get("sweep_kind", ""),
                    strict_cpuset_satisfied=state.get("strict_cpuset_satisfied", False),
                    notes="",
                )
                sw_re.write_run_status(run_id, run_status)
            except Exception as re_exc:
                print(f"[resource] Failed to write sidecars: {re_exc}", flush=True)
                output_validation_passed = False

        # ── Strict output validation (publication checks) ──────────
        if not args.preflight_only and not use_no_aggregate:
            try:
                run_strict_output_validation(root, run_dir)
            except Exception as val_exc:
                print(f"[validate] Publication validation FAILED: {val_exc}", flush=True)
                output_validation_passed = False
                if resource_experiment != "none":
                    try:
                        from resource_experiment_failures import build_run_status as re_build_run_status_pv
                        from resource_experiment_sidecars import SidecarWriter as RESidecarWriter_pv
                        state2 = resource_experiment_state  # noqa: F821
                        RESidecarWriter_pv(str(run_dir)).write_run_status(
                            run_id,
                            re_build_run_status_pv(
                                run_id=run_id,
                                run_mode=state2["resource_experiment"],
                                experiment_kind=state2["experiment_kind"],
                                run_success=False,
                                worker_failures=[],
                                resource_experiment=state2["resource_experiment"],
                                resource_failure_policy=state2["failure_policy"],
                                resource_profile_index=state2["selected_profile_index"],
                                resource_profile_id=state2["selected_profile_id"],
                                preflight_passed=preflight_passed,
                                output_validation_passed=False,
                                memory_model=state2.get("memory_model", ""),
                                docker_memory_limit=state2.get("docker_memory_limit", ""),
                                app_heap_budget=state2.get("app_heap_budget", ""),
                                app_heap_budget_bytes=state2.get("app_heap_budget_bytes", 0),
                                sweep_kind=state2.get("sweep_kind", ""),
                                strict_cpuset_satisfied=state2.get("strict_cpuset_satisfied", False),
                                notes=f"Publication validation: {val_exc}",
                            ),
                        )
                    except Exception:
                        pass
                write_run_outcome(
                    run_dir,
                    "output_validation_failure",
                    {"publication_validator_error": str(val_exc)},
                )
                raise RuntimeError("Publication output validation failed") from val_exc
        elif not args.preflight_only:
            print(
                "[validate] skipping publication CSV validation because --no-aggregate was used",
                flush=True,
            )
        else:
            print("[preflight] skipping artifact validation because --preflight-only was used")

        write_run_outcome(
            run_dir,
            "success",
            {
                "resource_summary": str(run_dir / "resource_summary.csv")
                if (run_dir / "resource_summary.csv").exists()
                else None,
            },
        )

        # Run resource experiment output validation
        if resource_experiment != "none" and getattr(args, "resource_output_validation", True):
            validator_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "validate_resource_experiment_outputs.py",
            )
            if os.path.exists(validator_path):
                result = subprocess.run(
                    [sys.executable, validator_path, str(run_dir)],
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode != 0:
                    validation_detail = (result.stdout + "\n" + result.stderr).strip()
                    print(f"[resource] Output validation FAILED: {validation_detail}", flush=True)
                    from resource_experiment_failures import build_run_status as re_build_run_status_ov
                    from resource_experiment_sidecars import SidecarWriter as RESidecarWriter_ov
                    state_ov = resource_experiment_state  # noqa: F821
                    RESidecarWriter_ov(str(run_dir)).write_run_status(
                        run_id,
                        re_build_run_status_ov(
                            run_id=run_id,
                            run_mode=state_ov["resource_experiment"],
                            experiment_kind=state_ov["experiment_kind"],
                            run_success=False,
                            worker_failures=[],
                            resource_experiment=state_ov["resource_experiment"],
                            resource_failure_policy=state_ov["failure_policy"],
                            resource_profile_index=state_ov["selected_profile_index"],
                            resource_profile_id=state_ov["selected_profile_id"],
                            preflight_passed=preflight_passed,
                            output_validation_passed=False,
                            memory_model=state_ov.get("memory_model", ""),
                            docker_memory_limit=state_ov.get("docker_memory_limit", ""),
                            app_heap_budget=state_ov.get("app_heap_budget", ""),
                            app_heap_budget_bytes=state_ov.get("app_heap_budget_bytes", 0),
                            sweep_kind=state_ov.get("sweep_kind", ""),
                            strict_cpuset_satisfied=state_ov.get("strict_cpuset_satisfied", False),
                            notes="Resource output validation failed",
                        ),
                    )
                    write_run_outcome(
                        run_dir,
                        "output_validation_failure",
                        {"validator_output": validation_detail},
                    )
                    raise RuntimeError("Resource experiment output validation failed")
                else:
                    print(f"[resource] Output validation PASSED", flush=True)

            # ── Write artifact validation on success ─────────────────
            write_artifact_validation(
                run_dir,
                run_success=True,
                primary_outcome="completed",
                aggregation_status="complete_success" if not use_no_aggregate else "",
            )

        print("")
        print(f"Run complete: {run_id}")
        print(f"Results: {run_dir}")
        return 0

    except Exception as e:
        failure_seen = True
        if resource_monitor and not resource_monitor_stopped:
            try:
                resource_monitor.stop()
                resource_monitor_stopped = True
            except Exception as monitor_exc:
                print(f"[resources] monitor stop failed during error handling: {monitor_exc}", flush=True)
        if external_oom_monitor and not external_oom_monitor_stopped:
            try:
                external_oom_monitor.stop()
                external_oom_monitor_stopped = True
            except Exception as monitor_exc:
                print(f"[resources] OOM monitor stop failed during error handling: {monitor_exc}", flush=True)

        fatal_outcome_class = "infrastructure_failure"
        fatal_outcome_evidence = {"error": str(e)}
        if isinstance(e, ResourceLimitVerificationError):
            fatal_outcome_class = "invalid_resource_envelope"
        elif (run_dir / "resource_summary.csv").exists():
            classified_outcome, classified_evidence = classify_failure_from_resource_summary(run_dir)
            if classified_outcome != "infrastructure_failure":
                fatal_outcome_class = classified_outcome
            fatal_outcome_evidence.update(classified_evidence)

        existing_outcome_path = run_dir / "benchmark_outcome.json"
        if existing_outcome_path.exists():
            try:
                existing_outcome = json.loads(existing_outcome_path.read_text(encoding="utf-8"))
                fatal_outcome_class = existing_outcome.get("outcome_class", fatal_outcome_class)
            except (OSError, json.JSONDecodeError):
                pass

        if not existing_outcome_path.exists():
            write_run_outcome(run_dir, fatal_outcome_class, fatal_outcome_evidence)

        try:
            layout_file_fatal = run_dir / "worker_layout.json"
            workers_file_fatal = run_dir / "workers.combined.txt"
            run_standalone_aggregation(
                run_dir,
                layout_file_fatal if layout_file_fatal.exists() else None,
                workers_file_fatal if workers_file_fatal.exists() else None,
                allow_partial=True,
                manifest_path=run_dir / "aggregation_manifest.json",
            )
        except Exception as agg_exc:
            print(f"[aggregate] fatal-path partial aggregation failed: {agg_exc}", flush=True)

        # ── Write resource experiment sidecars even on fatal error ─
        if resource_experiment != "none" and resource_experiment_state is not None:
            try:
                from resource_experiment_failures import (
                    append_synthetic_runner_failure_event,
                    build_run_status as re_build_run_status_fatal,
                    collect_worker_failures_from_artifacts,
                    write_synthetic_failure_events_csv,
                    worker_failure_info_to_dict,
                )
                from resource_experiment_sidecars import SidecarWriter as RESidecarWriter_fatal
                sw_re_fatal = RESidecarWriter_fatal(str(run_dir))
                state_fatal = resource_experiment_state  # noqa: F821

                summary_path = run_dir / "resource_summary.csv"
                resource_summaries_fatal = []
                if summary_path.exists():
                    import csv as csv_mod
                    with open(summary_path, newline="") as handle:
                        resource_summaries_fatal = list(csv_mod.DictReader(handle))
                events_path = run_dir / "events.csv"
                worker_failures_fatal = collect_worker_failures_from_artifacts(
                    str(events_path),
                    resource_summaries_fatal,
                    str(run_dir / "oom_events.jsonl"),
                    str(terminal_output_path),
                )
                for worker_failure in worker_failures_fatal:
                    if worker_failure.failure_timestamp_ns == 0:
                        worker_failure.failure_timestamp_ns = int(time.time_ns())
                    worker_failure.diagnostic_log_path = str(run_dir / "compose_services.log")
                synthesized_fatal = any(
                    append_synthetic_runner_failure_event(
                        str(run_dir), worker_failure, str(events_path)
                    )
                    for worker_failure in worker_failures_fatal
                    if worker_failure.attribution_source in (
                        "resource_monitor",
                        "app_heap_budget_failure_payload",
                    )
                )
                if synthesized_fatal:
                    run_standalone_aggregation(
                        run_dir,
                        layout_file_fatal if layout_file_fatal.exists() else None,
                        workers_file_fatal if workers_file_fatal.exists() else None,
                        allow_partial=True,
                        manifest_path=run_dir / "aggregation_manifest.json",
                    )
                    worker_failures_fatal = collect_worker_failures_from_artifacts(
                        str(events_path),
                        resource_summaries_fatal,
                        str(run_dir / "oom_events.jsonl"),
                        str(terminal_output_path),
                    )
                    for worker_failure in worker_failures_fatal:
                        worker_failure.diagnostic_log_path = str(
                            run_dir / "compose_services.log"
                        )
                write_synthetic_failure_events_csv(str(events_path), worker_failures_fatal)

                sw_re_fatal.write_worker_failures(
                    run_id,
                    [worker_failure_info_to_dict(wf) for wf in worker_failures_fatal],
                )

                rs_fatal = re_build_run_status_fatal(
                    run_id=run_id,
                    run_mode=state_fatal["resource_experiment"],
                    experiment_kind=state_fatal["experiment_kind"],
                    run_success=False,
                    worker_failures=worker_failures_fatal,
                    resource_experiment=state_fatal["resource_experiment"],
                    resource_failure_policy=state_fatal["failure_policy"],
                    resource_profile_index=state_fatal["selected_profile_index"],
                    resource_profile_id=state_fatal["selected_profile_id"],
                    preflight_passed=preflight_passed,
                    output_validation_passed=False,
                    memory_model=state_fatal.get("memory_model", ""),
                    docker_memory_limit=state_fatal.get("docker_memory_limit", ""),
                    app_heap_budget=state_fatal.get("app_heap_budget", ""),
                    app_heap_budget_bytes=state_fatal.get("app_heap_budget_bytes", 0),
                    sweep_kind=state_fatal.get("sweep_kind", ""),
                    strict_cpuset_satisfied=state_fatal.get("strict_cpuset_satisfied", False),
                    notes=f"Fatal error: {type(e).__name__}: {e}",
                )
                sw_re_fatal.write_run_status(run_id, rs_fatal)
            except Exception as re_exc_fatal:
                print(f"[resource] Failed to write fatal-error sidecars: {re_exc_fatal}", flush=True)

        # ── Capture container evidence on fatal error ────────────
        try:
            profiled_cnames_fatal = []
            if resource_experiment != "none":
                try:
                    profiled_cnames_fatal = resource_experiment_state.get("profiled_container_names", [])
                except Exception:
                    pass
            capture_failure_evidence(
                root=root,
                compose_file=compose_tmp,
                run_dir=run_dir,
                compose_env=compose_env,
                project_name=project_name,
                profiled_container_names=profiled_cnames_fatal + ["ds", "relay", "runner"],
            )
        except Exception:
            pass

        # ── Write artifact validation on fatal error ────────────
        if resource_experiment != "none":
            try:
                write_artifact_validation(
                    run_dir,
                    run_success=False,
                    primary_outcome=fatal_outcome_class,
                    failure_class=fatal_outcome_class,
                    aggregation_status="",
                )
            except Exception:
                pass

        print(
            f"[error] benchmark orchestration failed before cleanup: "
            f"{type(e).__name__}: {e}",
            file=sys.stderr,
            flush=True,
        )
        raise

    finally:
        if resource_monitor and not resource_monitor_stopped:
            try:
                resource_monitor.stop()
                resource_monitor_stopped = True
            except Exception as exc:
                print(f"[resources] monitor stop failed (non-fatal): {exc}", flush=True)

        if external_oom_monitor and not external_oom_monitor_stopped:
            try:
                external_oom_monitor.stop()
                external_oom_monitor_stopped = True
            except Exception as exc:
                print(f"[resources] external OOM monitor stop failed (non-fatal): {exc}", flush=True)

        if external_device_stop_required:
            stop_external_device_workers(args, root)

        if compose_up:
            try:
                write_compose_logs(root, compose_tmp, compose_logs_path, append=True)
            except Exception:
                pass

        keep_stack = args.keep_stack_up or (failure_seen and args.keep_stack_up_on_failure)
        if not keep_stack:
            compose_down(
                root=root,
                compose_file=compose_tmp,
                args=args,
                env=compose_env,
            )
        elif compose_up:
            print("[compose] keeping stack up", flush=True)

        if not args.keep_generated_files:
            for path in (compose_tmp, workers_internal_tmp, workers_host_tmp, layout_tmp):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
