import json
import os
import sys
import csv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from validate_resource_experiment_outputs import (
    RESOURCE_PROFILES_HEADER,
    RUN_STATUS_HEADER,
    WORKER_RESOURCE_ASSIGNMENTS_HEADER,
    Validator,
)


def test_validator_rejects_zero_cpu_profiled_assignment(tmp_path):
    (tmp_path / "cpu_affinity_plan.json").write_text(json.dumps({
        "run_id": "run",
        "online_cpu_mask_hex": "0xff",
        "profiled_mask_hex": "0x0",
        "background_mask_hex": "0xff",
        "profiled_assignments": [{
            "worker_id": "worker-00001",
            "assigned_cpus": [],
            "assigned_cpu_count": 0,
            "rayon_num_threads": 1,
        }],
        "background_assignments": [{"container_name": "ds", "assigned_cpus": [0]}],
    }))

    validator = Validator(str(tmp_path))
    validator._check_json_files()

    assert any("has no assigned CPU" in error for error in validator.errors)


def test_parallel_cpu_sweep_validator_accepts_new_default_fractions(tmp_path):
    fractions = [1.0, 0.75, 0.50, 0.25, 0.10, 0.05, 0.02, 0.01]

    with (tmp_path / "run_status.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RUN_STATUS_HEADER)
        writer.writeheader()
        row = {field: "" for field in RUN_STATUS_HEADER}
        row.update({"run_id": "run", "sweep_kind": "cpu_quota_sweep"})
        writer.writerow(row)

    with (tmp_path / "resource_profiles.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESOURCE_PROFILES_HEADER)
        writer.writeheader()
        for idx, fraction in enumerate(fractions):
            row = {field: "" for field in RESOURCE_PROFILES_HEADER}
            row.update(
                {
                    "run_id": "run",
                    "resource_profile_index": str(idx),
                    "resource_profile_id": f"cpu_{idx}",
                    "selected_for_this_run": "true",
                    "capacity_fraction": str(fraction),
                    "sweep_kind": "cpu_quota_sweep",
                }
            )
            writer.writerow(row)

    with (tmp_path / "worker_resource_assignments.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=WORKER_RESOURCE_ASSIGNMENTS_HEADER)
        writer.writeheader()
        for idx, fraction in enumerate(fractions):
            row = {field: "" for field in WORKER_RESOURCE_ASSIGNMENTS_HEADER}
            row.update(
                {
                    "run_id": "run",
                    "logical_client_id": f"{idx + 1:05d}",
                    "worker_id": f"{idx + 1:05d}",
                    "container_name": f"worker-{idx + 1:05d}",
                    "container_mode": "singleton",
                    "profile_enabled": "true",
                    "resource_profile_index": str(idx),
                    "resource_profile_id": f"cpu_{idx}",
                    "selected_for_this_run": "true",
                    "capacity_fraction": str(fraction),
                    "app_heap_budget": "64g",
                    "app_heap_budget_bytes": str(64 * 1024 * 1024 * 1024),
                    "group_creator": "true" if idx == 0 else "false",
                    "sweep_kind": "cpu_quota_sweep",
                    "cpu_quota_us": str(int(1000000 * fraction)),
                    "cpu_period_us": "1000000",
                }
            )
            writer.writerow(row)

    validator = Validator(str(tmp_path))
    validator._check_parallel_sweep_run()

    assert validator.errors == []


def test_parallel_cpu_sweep_validator_rejects_sub_floor_fraction(tmp_path):
    fractions = [1.0, 0.75, 0.50, 0.25, 0.10, 0.05, 0.02, 0.005]

    with (tmp_path / "run_status.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RUN_STATUS_HEADER)
        writer.writeheader()
        row = {field: "" for field in RUN_STATUS_HEADER}
        row.update({"run_id": "run", "sweep_kind": "cpu_quota_sweep"})
        writer.writerow(row)

    with (tmp_path / "resource_profiles.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESOURCE_PROFILES_HEADER)
        writer.writeheader()
        for idx, fraction in enumerate(fractions):
            row = {field: "" for field in RESOURCE_PROFILES_HEADER}
            row.update(
                {
                    "run_id": "run",
                    "resource_profile_index": str(idx),
                    "resource_profile_id": f"cpu_{idx}",
                    "selected_for_this_run": "true",
                    "capacity_fraction": str(fraction),
                    "sweep_kind": "cpu_quota_sweep",
                }
            )
            writer.writerow(row)

    with (tmp_path / "worker_resource_assignments.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=WORKER_RESOURCE_ASSIGNMENTS_HEADER)
        writer.writeheader()
        for idx, fraction in enumerate(fractions):
            row = {field: "" for field in WORKER_RESOURCE_ASSIGNMENTS_HEADER}
            row.update(
                {
                    "run_id": "run",
                    "logical_client_id": f"{idx + 1:05d}",
                    "worker_id": f"{idx + 1:05d}",
                    "container_name": f"worker-{idx + 1:05d}",
                    "container_mode": "singleton",
                    "profile_enabled": "true",
                    "resource_profile_index": str(idx),
                    "resource_profile_id": f"cpu_{idx}",
                    "selected_for_this_run": "true",
                    "capacity_fraction": str(fraction),
                    "app_heap_budget": "64g",
                    "app_heap_budget_bytes": str(64 * 1024 * 1024 * 1024),
                    "group_creator": "true" if idx == 0 else "false",
                    "sweep_kind": "cpu_quota_sweep",
                }
            )
            writer.writerow(row)

    validator = Validator(str(tmp_path))
    validator._check_parallel_sweep_run()

    assert any("below the Docker hard-quota floor" in e for e in validator.errors)


def test_parallel_cpu_sweep_validator_rejects_collapsed_profiles(tmp_path):
    fractions = [1.0, 0.75, 0.50, 0.25, 0.10, 0.05, 0.02, 0.01]

    with (tmp_path / "run_status.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RUN_STATUS_HEADER)
        writer.writeheader()
        row = {field: "" for field in RUN_STATUS_HEADER}
        row.update({"run_id": "run", "sweep_kind": "cpu_quota_sweep"})
        writer.writerow(row)

    with (tmp_path / "resource_profiles.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESOURCE_PROFILES_HEADER)
        writer.writeheader()
        for idx, fraction in enumerate(fractions):
            row = {field: "" for field in RESOURCE_PROFILES_HEADER}
            row.update(
                {
                    "run_id": "run",
                    "resource_profile_index": str(idx),
                    "resource_profile_id": f"cpu_{idx}",
                    "selected_for_this_run": "true",
                    "capacity_fraction": str(fraction),
                    "sweep_kind": "cpu_quota_sweep",
                }
            )
            writer.writerow(row)

    with (tmp_path / "worker_resource_assignments.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=WORKER_RESOURCE_ASSIGNMENTS_HEADER)
        writer.writeheader()
        for idx in range(8):
            row = {field: "" for field in WORKER_RESOURCE_ASSIGNMENTS_HEADER}
            row.update(
                {
                    "run_id": "run",
                    "logical_client_id": f"{idx + 1:05d}",
                    "worker_id": f"{idx + 1:05d}",
                    "container_name": f"worker-{idx + 1:05d}",
                    "container_mode": "singleton",
                    "profile_enabled": "true",
                    "resource_profile_index": str(idx),
                    "resource_profile_id": f"cpu_{idx}",
                    "selected_for_this_run": "true",
                    "capacity_fraction": "0.10",
                    "app_heap_budget": "64g",
                    "app_heap_budget_bytes": str(64 * 1024 * 1024 * 1024),
                    "group_creator": "true" if idx == 0 else "false",
                    "sweep_kind": "cpu_quota_sweep",
                    "cpu_quota_us": "100000",
                    "cpu_period_us": "1000000",
                }
            )
            writer.writerow(row)

    validator = Validator(str(tmp_path))
    validator._check_parallel_sweep_run()

    assert any("collapsed CPU profiles" in e for e in validator.errors)
