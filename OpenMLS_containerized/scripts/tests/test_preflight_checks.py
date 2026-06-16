import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from preflight_checks import run_preflight


def _profiled(expected_cpuset="1"):
    return [{
        "container_name": "worker-1",
        "container_role": "profiled_singleton",
        "expected_cpuset": expected_cpuset,
        "expected_rayon_threads": 1,
    }]


def test_empty_profiled_cpuset_is_a_hard_failure():
    with patch("preflight_checks.run_docker_inspect_cpuset", return_value=""), \
            patch("preflight_checks.get_container_env_var", return_value="1"), \
            patch("preflight_checks.run_docker_inspect_pid", return_value=None), \
            patch("preflight_checks.check_host_processes_on_cpus", return_value=[]):
        results, passed = run_preflight("run", _profiled(""), [], object())

    assert passed is False
    assert any(row["check_name"] == "profiled_cpuset_plan_empty" for row in results)


def test_proc_cpuset_must_match_exactly():
    with patch("preflight_checks.run_docker_inspect_cpuset", return_value="1"), \
            patch("preflight_checks.get_container_env_var", return_value="1"), \
            patch("preflight_checks.run_docker_inspect_pid", return_value=123), \
            patch("preflight_checks.read_proc_cpus_allowed_list", return_value="1-2"), \
            patch("preflight_checks.read_proc_task_cpus_allowed_lists", return_value={}), \
            patch("preflight_checks.check_host_processes_on_cpus", return_value=[]):
        results, passed = run_preflight("run", _profiled("1"), [], object())

    assert passed is False
    assert any(row["check_name"] == "proc_cpus_allowed_conflict" for row in results)


def test_missing_background_cpuset_is_a_hard_failure():
    inspect_values = {"worker-1": "1", "background": ""}
    with patch(
        "preflight_checks.run_docker_inspect_cpuset",
        side_effect=lambda name: inspect_values[name],
    ), patch("preflight_checks.get_container_env_var", return_value="1"), \
            patch("preflight_checks.run_docker_inspect_pid", return_value=None), \
            patch("preflight_checks.check_host_processes_on_cpus", return_value=[]):
        results, passed = run_preflight(
            "run",
            _profiled("1"),
            [{"container_name": "background", "expected_cpuset": "0,2-3"}],
            object(),
        )

    assert passed is False
    assert any(row["check_name"] == "background_cpuset_missing" for row in results)
