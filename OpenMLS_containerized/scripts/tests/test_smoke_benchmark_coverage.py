import unittest
import os
import json
import csv
import subprocess
import sys
import tempfile
import re
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from validate_benchmark_outputs import validate_run

REPO_ROOT = SCRIPTS_DIR.parent
BENCHMARK_BIN = "benchmark_runner_http_staircase_local"

def check_commit_receive_in_jsonl(run_dir):
    jsonl_files = list(Path(run_dir).glob("client-*.jsonl"))
    count = 0
    for jf in jsonl_files:
        with open(jf) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("op") == "commit_receive_protocol":
                        count += 1
                except json.JSONDecodeError:
                    pass
    return count

def check_commit_receive_in_csv(run_dir):
    events_csv = Path(run_dir) / "events.csv"
    if not events_csv.exists():
        return 0
    count = 0
    with open(events_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("op") == "commit_receive_protocol":
                count += 1
    return count

def check_commit_receive_metadata(run_dir):
    events_csv = Path(run_dir) / "events.csv"
    if not events_csv.exists():
        return {}
    required_meta = [
        "commit_kind",
        "commit_create_op",
        "commit_size_bytes",
        "committer_leaf_index",
        "commit_receive_sampling_policy",
        "commit_id",
        "group_epoch",
        "tree_size",
        "member_count",
        "ciphersuite",
    ]
    present_meta = {col: 0 for col in required_meta}
    with open(events_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("op") == "commit_receive_protocol":
                for col in required_meta:
                    if row.get(col):
                        present_meta[col] += 1
    return present_meta

@unittest.skipUnless(
    os.environ.get("OPENMLS_RUN_SMOKE_BENCHMARK_TESTS") == "1",
    "Skipping smoke benchmark test (set OPENMLS_RUN_SMOKE_BENCHMARK_TESTS=1 to run)"
)
class TestSmokeBenchmarkCoverage(unittest.TestCase):

    def test_smoke_benchmark_emits_commit_receive_protocol(self):
        with tempfile.TemporaryDirectory(prefix="smoke_coverage_") as tmp_dir:
            run_id = "smoke-test-coverage"

            cmd = [
                "cargo", "run",
                f"--manifest-path={REPO_ROOT / 'Cargo.toml'}",
                f"--bin={BENCHMARK_BIN}",
                "--",
                "--spawn-local-workers", "16",
                "--min-size", "2",
                "--max-size", "16",
                "--step-size", "7",
                "--roundtrips", "1",
                "--update-rounds", "0",
                "--max-update-samples-per-plateau", "0",
                "--app-rounds", "0",
                "--payload-sizes", "32",
                "--run-id", run_id,
                "--scenario", "smoke-coverage-test",
                "--scenario-seed", "1",
                "--output-dir", tmp_dir,
                "--max-commit-receive-samples-per-plateau", "8",
                "--commit-receive-sampling-seed", "42",
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
                cwd=str(REPO_ROOT),
            )

            run_dir = Path(tmp_dir) / run_id

            # Print output for debugging
            print("STDOUT:", result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
            print("STDERR:", result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr)
            print("Return code:", result.returncode)

            self.assertEqual(result.returncode, 0,
                             f"Benchmark runner failed with code {result.returncode}")

            # Check run_dir exists
            self.assertTrue(run_dir.exists(), f"Run directory {run_dir} not found")

            # Check JSONL files exist
            jsonl_files = list(run_dir.glob("client-*.jsonl"))
            self.assertGreater(len(jsonl_files), 0,
                               f"No client-*.jsonl files in {run_dir}")

            # Check events.csv exists
            events_csv = run_dir / "events.csv"
            self.assertTrue(events_csv.exists(), f"events.csv not found in {run_dir}")

            # Check commit_receive_protocol in JSONL
            jsonl_count = check_commit_receive_in_jsonl(str(run_dir))
            self.assertGreater(jsonl_count, 0,
                               f"commit_receive_protocol not found in JSONL (searched {len(jsonl_files)} files)")

            # Check commit_receive_protocol in CSV
            csv_count = check_commit_receive_in_csv(str(run_dir))
            self.assertGreater(csv_count, 0,
                               f"commit_receive_protocol not found in CSV")

            # Check commit_receive metadata fields are populated
            meta = check_commit_receive_metadata(str(run_dir))
            for col, count in meta.items():
                self.assertGreater(
                    count, 0,
                    f"commit_receive_protocol metadata field '{col}' has no populated values"
                )

            # Run validator on the output
            validation = validate_run(
                str(run_dir), allow_missing_jsonl=False, required_k_values=[1]
            )
            self.assertTrue(validation.success, validation.errors)
            self.assertTrue(
                any(k > 1 for k in validation.add_k_counts),
                f"Expected at least one multi-member AddCommit, got {validation.add_k_counts}",
            )

if __name__ == "__main__":
    unittest.main()
