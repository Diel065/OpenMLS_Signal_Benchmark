#!/usr/bin/env python3
"""Small native-process regression smoke; no Docker or external hardware required."""
import argparse
from collections import Counter, defaultdict
import csv
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request

from validate_benchmark_outputs import validate_run


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run_case(bin_dir, output, name, model=None, worker=None, packed=False, external=False, heap_failure=False, staircase=False):
    run_dir = output / name
    run_dir.mkdir()
    processes, logs = [], []

    def start(binary, args, env=None):
        log = (run_dir / f"process-{len(processes)}.log").open("w")
        logs.append(log)
        proc = subprocess.Popen([str(binary), *args], stdout=log, stderr=log, env=env)
        processes.append(proc)

    def healthy(url):
        for _ in range(200):
            try:
                with urllib.request.urlopen(url, timeout=1) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            if any(p.poll() is not None for p in processes):
                raise AssertionError(f"process exited; see {run_dir}")
            time.sleep(0.05)
        raise AssertionError(f"health timeout: {url}")

    try:
        kr, relay = free_port(), free_port()
        start(bin_dir / "key_repository", ["--listen-addr", f"127.0.0.1:{kr}"])
        start(bin_dir / "message_relay", ["--listen-addr", f"127.0.0.1:{relay}"])
        clients, physical = [], []
        groups = [["00001"], ["00002"], ["00003", "00004"]] if packed else [[f"{i:05}"] for i in range(1, 5)]
        for ids in groups:
            port = free_port()
            base = f"http://127.0.0.1:{port}"
            is_packed = len(ids) > 1
            mode = "packed" if is_packed else "singleton"
            profiled = [] if is_packed else ids
            env = dict(os.environ, SIGNAL_PROFILE_RUN_ID=name, SIGNAL_PROFILE_SCENARIO="group-model-smoke")
            if heap_failure and ids == ["00001"]:
                env.update(SIGNAL_MEMORY_MODEL="app-heap-budget", SIGNAL_APP_HEAP_BUDGET_BYTES="1")
            if profiled:
                env["SIGNAL_PROFILE_PATH"] = str(run_dir / f"participant-{ids[0]}.jsonl")
                if external:
                    env["SIGNAL_PROFILE_EXECUTION_BACKEND"] = "real_device"
            start(worker or bin_dir / "worker", ["--name", ids[0], "--participants", ",".join(ids),
                "--listen-addr", f"127.0.0.1:{port}", "--kr-url", f"http://127.0.0.1:{kr}",
                "--relay-url", f"http://127.0.0.1:{relay}",
                "--profile-enabled-participant-ids", ",".join(profiled),
                "--profile-path-template", str(run_dir / "participant-{participant_id}.jsonl")], env)
            healthy(base + "/health")
            physical.append(dict(physical_worker_id=ids[0], container_mode=mode, client_ids=ids,
                base_url=base, profile_enabled_client_ids=profiled))
            for participant in ids:
                clients.append(dict(client_id=participant, physical_worker_id=ids[0], container_mode=mode,
                    profile_enabled=participant in profiled, command_url=f"{base}/participant/{participant}",
                    health_url=f"{base}/participant/{participant}/health"))
        layout = dict(version=1, logical_worker_count=4, physical_worker_count=len(groups),
            layout_mode="hybrid" if packed else "one-container-per-client", profile_policy="singletons",
            clients=clients, physical_workers=physical)
        layout_path = run_dir / "worker_layout.json"
        layout_path.write_text(json.dumps(layout))
        args = [str(bin_dir / "benchmark_runner_http_staircase"), "--kr-url", f"http://127.0.0.1:{kr}",
            "--relay-url", f"http://127.0.0.1:{relay}", "--worker-layout", str(layout_path),
            "--run-id", name, "--output-dir", str(output), "--min-size", "2", "--max-size", "4",
            "--app-rounds", "1", "--payload-sizes", "32", "--max-app-samples-per-payload", "2",
            "--roundtrips", "2" if model == "sender-keys" or staircase else "1",
            "--plateau-order", "staircase" if model == "sender-keys" or staircase else "ascending"]
        if model:
            args += ["--group-model", model]
        with (run_dir / "runner.log").open("w") as log:
            subprocess.run(args, stdout=log, stderr=log, timeout=120, check=True)
        rows = list(csv.DictReader((run_dir / "events.csv").open()))
        result = validate_run(str(run_dir))
        assert not result.errors, result.errors
        expected_model = "sender_keys" if model == "sender-keys" else "pairwise"
        assert {r["group_model"] for r in rows} == {expected_model}
        sends = defaultdict(list)
        for row in rows:
            send_op = "signal_application_message_create.total" if model == "sender-keys" else "pairwise_fanout_send_wrapper"
            if row["op"] == send_op and row["benchmark_phase"] == "application":
                sends[(row["participant_id"], row["benchmark_workflow_id"])].append(row)
        assert sends
        for workflow in sends.values():
            expected = 1 if model == "sender-keys" else int(workflow[0]["benchmark_active_size"]) - 1
            assert len(workflow) == expected
            assert all(int(r["workflow_pair_count"]) == expected for r in workflow)
        if model != "sender-keys":
            assert sum(r["op"] == "signal_application_message_create.total" for r in rows) == sum(
                r["op"] == "pairwise_fanout_send_wrapper" for r in rows)
        if model == "sender-keys":
            assert any(r["op"] == "signal_sender_key_removal_refresh.total" for r in rows)
            onboarding = [r for r in rows if r["op"] == "signal_sender_key_onboarding.total"]
            assert onboarding and all(r["role"] == "joiner" for r in onboarding)
            for total in [r for r in rows if r["event_family"] == "sender_keys" and r["measurement_plane"] == "protocol_total"]:
                children = [r for r in rows if r["parent_global_span_id"] == total["global_span_id"]
                    and r["participant_id"] == total["participant_id"] and r["event_family"] == "sender_keys"]
                assert children
                if total["op"] == "signal_application_message_create.total":
                    assert sum(r["op"] == "signal_application_message_create.sender_key_encrypt_payload" for r in children) == 1
                if total["op"] == "signal_application_message_receive.total":
                    assert sum(r["op"] == "signal_application_message_receive.sender_key_decrypt_payload" for r in children) == 1
                for metric in ("alloc_bytes", "alloc_count", "cpu_process_ns", "cpu_thread_ns"):
                    assert int(total[metric]) == sum(int(r[metric]) for r in children), (metric, total)
            assert all(r["span_inclusive"] == "true" for r in onboarding)
        if heap_failure:
            failures = [json.loads(line) for line in (run_dir / "runner-events.jsonl").read_text().splitlines()]
            assert any(r["failure_class"] == "app_heap_budget_exceeded" for r in failures)
            assert any(r["op"] == "signal_sender_key_onboarding.total" and r["participant_id"] != "00001" for r in rows)
        if packed:
            assert not any((run_dir / f"participant-{i:05}.jsonl").exists() for i in (3, 4))
        signature = Counter((r["op"], r["participant_id"], r["phase"], r["benchmark_target_size"], r["benchmark_phase"])
            for r in rows if r["measurement_plane"] == "wrapper_total")
        print(f"PASS {name}: {len(sends)} logical sends, model={expected_model}")
        return signature
    finally:
        for proc in processes:
            proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log in logs:
            log.close()


def check_onboarding_heap_attribution(bin_dir, output):
    port = free_port()
    path = output / "onboarding-heap.jsonl"
    env = dict(os.environ, SIGNAL_MEMORY_MODEL="app-heap-budget", SIGNAL_APP_HEAP_BUDGET_BYTES="1",
        SIGNAL_PROFILE_PATH=str(path))
    with (output / "onboarding-heap.log").open("w") as log:
        proc = subprocess.Popen([str(bin_dir / "worker"), "--name", "heap-test",
            "--listen-addr", f"127.0.0.1:{port}", "--kr-url", "http://127.0.0.1:9",
            "--relay-url", "http://127.0.0.1:9"], env=env, stdout=log, stderr=log)
        try:
            for _ in range(200):
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                        break
                except OSError:
                    time.sleep(0.05)
            command = dict(cmd="sender_key_onboard", participants=[], generation=1, conversation_size=1)
            request = urllib.request.Request(f"http://127.0.0.1:{port}/command",
                data=json.dumps(command).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=10) as response:
                outcome = json.load(response)
            assert outcome["status"] == "error", outcome
            assert "APP_HEAP_BUDGET_EXCEEDED" in outcome["message"]
            assert "operation_family=SignalSenderKeyOnboarding" in outcome["message"]
            assert "member_count=1" in outcome["message"]
            print("PASS Sender Keys onboarding heap attribution")
        finally:
            proc.terminate()
            proc.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--baseline-worker", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = (args.output_dir or Path(tempfile.mkdtemp(prefix="signal-group-models-"))).resolve()
    output.mkdir(parents=True, exist_ok=True)
    default = run_case(args.bin_dir.resolve(), output, "default")
    explicit = run_case(args.bin_dir.resolve(), output, "pairwise", "pairwise")
    assert default == explicit, "default and explicit pairwise command structures differ"
    if args.baseline_worker:
        baseline = run_case(args.bin_dir.resolve(), output, "baseline", worker=args.baseline_worker.resolve())
        assert default == baseline, "pairwise command structure differs from baseline binary"
        stateful = run_case(args.bin_dir.resolve(), output, "pairwise-stateful", "pairwise", staircase=True)
        old_stateful = run_case(args.bin_dir.resolve(), output, "baseline-stateful", worker=args.baseline_worker.resolve(), staircase=True)
        def populations(signature):
            result = Counter()
            for (op, _, phase, target, benchmark_phase), count in signature.items():
                result[(op, phase, target, benchmark_phase)] += count
            return result
        assert populations(stateful) == populations(old_stateful), "pairwise state progression differs from baseline"
    run_case(args.bin_dir.resolve(), output, "sender-keys", "sender-keys")
    run_case(args.bin_dir.resolve(), output, "sender-keys-packed", "sender-keys", packed=True)
    run_case(args.bin_dir.resolve(), output, "sender-keys-external-interface", "sender-keys", external=True)
    run_case(args.bin_dir.resolve(), output, "sender-keys-heap-failure", "sender-keys", heap_failure=True)
    check_onboarding_heap_attribution(args.bin_dir.resolve(), output)
    print(f"Smoke artifacts: {output}")


if __name__ == "__main__":
    main()
