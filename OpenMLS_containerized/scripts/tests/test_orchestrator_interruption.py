from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
ORCHESTRATORS = (
    REPO_ROOT / "OpenMLS_containerized" / "scripts" / "run_compose_benchmark.py",
    REPO_ROOT / "Signal_containerized" / "scripts" / "run_compose_benchmark.py",
)


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS, ids=("openmls", "signal"))
@pytest.mark.parametrize("termination_signal", (signal.SIGINT, signal.SIGTERM))
def test_tee_forwards_termination_and_reaps_child(
    tmp_path: Path,
    orchestrator: Path,
    termination_signal: signal.Signals,
) -> None:
    driver = tmp_path / "driver.py"
    output = tmp_path / "terminal_output.txt"
    child_pid_file = tmp_path / "child.pid"
    driver.write_text(
        textwrap.dedent(
            """
            import importlib.util
            import pathlib
            import sys

            module_path = pathlib.Path(sys.argv[1])
            sys.path.insert(0, str(module_path.parent))
            spec = importlib.util.spec_from_file_location("orchestrator_under_test", module_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            child_code = '''
            import os
            import pathlib
            import signal
            import sys
            import time

            pathlib.Path(sys.argv[1]).write_text(str(os.getpid()), encoding="utf-8")
            print("READY", flush=True)

            def stop(_signum, _frame):
                raise SystemExit(0)

            signal.signal(signal.SIGINT, stop)
            signal.signal(signal.SIGTERM, stop)
            while True:
                time.sleep(0.1)
            '''
            result = module.tee_subprocess_output(
                [sys.executable, "-c", child_code, sys.argv[3]],
                cwd=module_path.parent,
                output_path=pathlib.Path(sys.argv[2]),
            )
            raise SystemExit(result)
            """
        ),
        encoding="utf-8",
    )

    process = subprocess.Popen(
        [sys.executable, str(driver), str(orchestrator), str(output), str(child_pid_file)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if output.exists() and "READY" in output.read_text(encoding="utf-8"):
            break
        if process.poll() is not None:
            pytest.fail(f"driver exited before child was ready: {process.stdout.read()}")
        time.sleep(0.05)
    else:
        process.kill()
        pytest.fail("timed out waiting for orchestrated child")

    process.send_signal(termination_signal)
    stdout, _ = process.communicate(timeout=45)
    assert process.returncode == 128 + termination_signal, stdout

    child_pid = int(child_pid_file.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and Path(f"/proc/{child_pid}").exists():
        time.sleep(0.05)
    assert not Path(f"/proc/{child_pid}").exists(), "benchmark child survived orchestrator termination"
