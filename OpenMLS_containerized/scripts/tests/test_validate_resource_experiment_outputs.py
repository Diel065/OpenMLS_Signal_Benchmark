import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from validate_resource_experiment_outputs import Validator


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
