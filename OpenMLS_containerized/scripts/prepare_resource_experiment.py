#!/usr/bin/env python3
"""
Pre-compute CPU affinity plan and resource profiles for a resource experiment run.

Usage:
    python3 prepare_resource_experiment.py \
        --run-id openmls_run_1_20260612 \
        --resource-experiment ram-sweep-singleton \
        --profiled-singleton-count 6 \
        --ram-sweep-values 32m,64m,128m,256m,512m,1g \
        --ram-sweep-cpu-count 10 \
        --singleton-worker-ids worker-00001,worker-00017,... \
        --singleton-client-ids 00001,00017,... \
        --output-dir /path/to/run_dir \
        [--cpu-affinity-sample-seconds 20] \
        [--reserve-smt-siblings]

Prints the paths to the generated affinity plan and resource profiles files.
"""

import argparse
import json
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from cpu_mask_util import cpu_list_to_mask, cpu_list_to_docker_cpuset, mask_to_hex
from cpu_topology import detect_cpu_topology, get_online_cpu_list
from cpu_affinity_planner import (
    create_affinity_plan,
    write_affinity_plan_json,
    validate_affinity_plan,
)
from resource_profiles import (
    generate_ram_sweep_profiles,
    generate_cpu_matrix_profiles,
)
from resource_experiment_sidecars import SidecarWriter


def main():
    parser = argparse.ArgumentParser(description="Pre-compute resource experiment affinity plan")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--resource-experiment", required=True,
                        choices=["ram-sweep-singleton", "cpu-matrix-singleton"])
    parser.add_argument("--profiled-singleton-count", type=int, required=True)
    parser.add_argument("--ram-sweep-values", default="32m,64m,128m,256m,512m,1g")
    parser.add_argument("--ram-sweep-cpu-count", type=int, default=10)
    parser.add_argument("--cpu-matrix-core-counts", default="1,2,4")
    parser.add_argument("--cpu-matrix-capacity-fractions", default="0.25,0.50,0.75,1.00")
    parser.add_argument("--singleton-worker-ids", required=True,
                        help="Comma-separated list of singleton worker IDs")
    parser.add_argument("--singleton-client-ids", required=True,
                        help="Comma-separated list of singleton client IDs")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--cpu-affinity-sample-seconds", type=float, default=20.0)
    parser.add_argument("--reserve-smt-siblings", action="store_true")
    parser.add_argument("--background-containers", default="ds,relay",
                        help="Comma-separated list of background container names")
    parser.add_argument("--packed-containers", default="",
                        help="Comma-separated list of packed worker container names")
    args = parser.parse_args()

    worker_ids = [w.strip() for w in args.singleton_worker_ids.split(",") if w.strip()]
    client_ids = [c.strip() for c in args.singleton_client_ids.split(",") if c.strip()]

    if len(worker_ids) != len(client_ids):
        print(f"ERROR: worker IDs count ({len(worker_ids)}) != client IDs count ({len(client_ids)})",
              file=sys.stderr)
        sys.exit(1)

    count = min(len(worker_ids), args.profiled_singleton_count)
    worker_ids = worker_ids[:count]
    client_ids = client_ids[:count]

    experiment_kind = args.resource_experiment.replace("-", "_")

    if args.resource_experiment == "ram-sweep-singleton":
        ram_values = [v.strip() for v in args.ram_sweep_values.split(",") if v.strip()]
        profiles = generate_ram_sweep_profiles(
            ram_values=ram_values,
            assigned_cpu_count=args.ram_sweep_cpu_count,
            run_id=args.run_id,
        )
    else:
        core_counts = [int(v.strip()) for v in args.cpu_matrix_core_counts.split(",") if v.strip()]
        fractions = [float(v.strip()) for v in args.cpu_matrix_capacity_fractions.split(",") if v.strip()]
        profiles = generate_cpu_matrix_profiles(
            core_counts=core_counts,
            capacity_fractions=fractions,
            run_id=args.run_id,
        )

    bg_names = [n.strip() for n in args.background_containers.split(",") if n.strip()]
    packed_names = [n.strip() for n in args.packed_containers.split(",") if n.strip()]

    background_specs = []
    for name in bg_names:
        background_specs.append({"container_name": name, "container_role": "infrastructure"})
    for name in packed_names:
        background_specs.append({"container_name": name, "container_role": "packed"})

    profiled_worker_specs = []
    profiled_cpu_counts = {}
    for i, (wid, cid) in enumerate(zip(worker_ids, client_ids)):
        profile = profiles[i % len(profiles)] if profiles else None
        cpu_count = profile.assigned_cpu_count if profile else 1
        profiled_worker_specs.append({
            "worker_id": wid,
            "container_name": f"worker-{cid}",
            "logical_client_id": cid,
            "experiment_kind": experiment_kind,
            "resource_profile_id": profile.resource_profile_id if profile else "",
        })
        profiled_cpu_counts[wid] = cpu_count

    print(f"[prepare] Building affinity plan for {count} profiled workers "
          f"({sum(profiled_cpu_counts.values())} total CPUs requested)...", flush=True)
    print(f"[prepare] Sampling CPU load for {args.cpu_affinity_sample_seconds}s...", flush=True)

    plan = create_affinity_plan(
        run_id=args.run_id,
        profiled_worker_specs=profiled_worker_specs,
        background_specs=background_specs,
        cpu_affinity_mode="profiled-nor-background",
        sample_seconds=args.cpu_affinity_sample_seconds,
        reserve_smt_siblings=args.reserve_smt_siblings,
        profiled_cpu_counts=profiled_cpu_counts,
    )

    errors = validate_affinity_plan(plan)
    if errors:
        for e in errors:
            print(f"[prepare] WARNING: {e}", file=sys.stderr)

    if plan.warnings:
        for w in plan.warnings:
            print(f"[prepare] WARNING: {w}", file=sys.stderr)

    os.makedirs(args.output_dir, exist_ok=True)

    for i, pa in enumerate(plan.profiled_assignments):
        if i < len(profiles):
            profiles[i].cpuset_cpus = cpu_list_to_docker_cpuset(pa.assigned_cpus)
            profiles[i].cpuset_mask_hex = pa.assigned_mask_hex

    plan_path = write_affinity_plan_json(plan, args.output_dir)

    profiles_path = os.path.join(args.output_dir, "resource_profiles.json")
    profiles_data = [p.to_dict() for p in profiles]
    with open(profiles_path, "w") as f:
        json.dump(profiles_data, f, indent=2)

    writer = SidecarWriter(args.output_dir)
    writer.write_resource_profiles(args.run_id, [p.to_dict() for p in profiles])

    print(f"[prepare] Affinity plan written to: {plan_path}")
    print(f"[prepare] Resource profiles written to: {profiles_path}")

    for pa in plan.profiled_assignments:
        print(f"[prepare]   {pa.container_name}: cpus={pa.assigned_cpus} "
              f"rayon={pa.rayon_num_threads} profile={pa.resource_profile_id}")

    print(f"[prepare] Background cpuset: {len(plan.background_cpus)} CPUs "
          f"({', '.join(str(c) for c in sorted(plan.background_cpus)[:8])}{'...' if len(plan.background_cpus) > 8 else ''})")

    print(f"PLAN_FILE={plan_path}")
    print(f"PROFILES_FILE={profiles_path}")


if __name__ == "__main__":
    main()
