import csv
import json
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from run_compose_benchmark import write_integrated_aggregation_manifest


def test_integrated_aggregation_manifest_describes_existing_output(tmp_path):
    events_path = tmp_path / "events.csv"
    with events_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["op"])
        writer.writeheader()
        writer.writerows([{"op": "one"}, {"op": "two"}])
    (tmp_path / "client-00001.jsonl").write_text("{}\n", encoding="utf-8")

    write_integrated_aggregation_manifest(tmp_path, "run-1")

    manifest = json.loads((tmp_path / "aggregation_manifest.json").read_text())
    assert manifest["run_id"] == "run-1"
    assert manifest["aggregation_mode"] == "runner_integrated"
    assert manifest["aggregation_status"] == "complete_success"
    assert manifest["events_written"] == 2
    assert manifest["input_files_found"] == 1
