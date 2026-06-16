import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from resource_experiment_sidecars import RESOURCE_SUMMARY_HEADER
from run_compose_benchmark import ResourceMonitor


def test_resource_monitor_writes_canonical_summary(tmp_path):
    monitor = ResourceMonitor(
        root=tmp_path,
        run_dir=tmp_path,
        run_id="test-run",
        targets=[{
            "worker_id": "worker-00001",
            "physical_worker_id": "worker-00001",
            "logical_client_id": "00001",
            "container_name": "worker-00001",
            "container_id": None,
            "resource_profile_id": "cpu_1c_001",
            "experiment_kind": "cpu_matrix_singleton",
            "cpuset_cpus": "1",
            "memory_limit": "",
            "memory_swap": "",
            "rayon_num_threads": 1,
            "expected": {
                "cpus": 0.01,
                "memory_bytes": None,
                "resource_profile": "cpu_1c_001",
            },
        }],
        interval_ms=250,
        compose_env=None,
    )
    row = monitor.summary["worker-00001"]
    row.update({
        "samples": 2,
        "timestamp_first_unix_ns": 1_000_000_000,
        "timestamp_last_unix_ns": 2_000_000_000,
        "cpu_usage_usec_first": 0,
        "cpu_usage_usec_last": 10_000,
        "cpu_user_usec_first": 0,
        "cpu_user_usec_last": 8_000,
        "cpu_system_usec_first": 0,
        "cpu_system_usec_last": 2_000,
        "cpu_nr_periods_first": 0,
        "cpu_nr_periods_last": 10,
        "cpu_nr_throttled_first": 0,
        "cpu_nr_throttled_last": 9,
        "cpu_throttled_usec_first": 0,
        "cpu_throttled_usec_last": 900_000,
        "last_container_status": "running",
        "last_container_exit_code": 0,
        "last_container_oom_killed": False,
    })

    monitor._write_summary()

    with (tmp_path / "resource_summary.csv").open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert reader.fieldnames == RESOURCE_SUMMARY_HEADER
    assert rows[0]["resource_profile_id"] == "cpu_1c_001"
    assert rows[0]["cpu_throttled_time_fraction"] == "0.900000"
    assert "resource_limit_memory_bytes" not in reader.fieldnames
    assert (tmp_path / "resource_monitor_summary.csv").exists()
