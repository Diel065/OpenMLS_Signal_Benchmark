from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class DeviceConfig:
    id: str
    enabled: bool
    kind: str

    connection: dict
    transport: dict
    target: dict
    worker: dict
    metadata: dict = field(default_factory=dict)


@dataclass
class WorkerLaunch:
    worker_id: str
    binary_path: str
    ds_url: str
    relay_url: str
    listen_addr: str
    run_id: str
    scenario: str
    scenario_seed: int
    profile_path_template: str
    remote_results_root: str
    remote_tmp: str
    node_name: str = ""


# ---------------------------------------------------------------------------
# Run ID validation (mirrors the Rust validate_run_id)
# ---------------------------------------------------------------------------

RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


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


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------


def load_devices_config(path: Path) -> list[DeviceConfig]:
    try:
        import yaml
    except ImportError:
        raise RuntimeError(
            "PyYAML is required to parse device config. "
            "Install it with: pip install pyyaml"
        )

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not raw or "devices" not in raw:
        raise ValueError(f"No 'devices' key found in {path}")

    devices: list[DeviceConfig] = []
    for entry in raw["devices"]:
        devices.append(DeviceConfig(
            id=entry["id"],
            enabled=bool(entry.get("enabled", True)),
            kind=entry.get("kind", ""),
            connection=entry.get("connection", {}),
            transport=entry.get("transport", {}),
            target=entry.get("target", {}),
            worker=entry.get("worker", {}),
            metadata=entry.get("metadata", {}),
        ))
    return devices


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _dedupe_strings(values) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def build_worker_url_candidates(
    config: DeviceConfig,
    transport_ip: str,
    worker_port: int,
) -> list[str]:
    transport = config.transport
    urls: list[str] = [f"http://{transport_ip}:{worker_port}"]
    urls.extend(
        f"http://{ip}:{worker_port}"
        for ip in _as_list(transport.get("device_ip_candidates"))
    )
    urls.extend(_as_list(transport.get("worker_urls")))
    urls.extend(_as_list(transport.get("worker_url_candidates")))
    return _dedupe_strings(urls)


def wait_for_first_healthy_url(urls: list[str], timeout_s: float = 30.0) -> str:
    deadline = time.time() + timeout_s
    last_error = ""
    candidates = _dedupe_strings(urls)
    if not candidates:
        raise RuntimeError("No worker URL candidates configured")

    while time.time() < deadline:
        for url in candidates:
            try:
                with urllib.request.urlopen(f"{url.rstrip('/')}/health", timeout=2) as resp:
                    body = resp.read().decode("utf-8", errors="replace").strip()
                    if 200 <= resp.status < 300 and body == "ok":
                        return url.rstrip("/")
                    last_error = f"{url}: HTTP {resp.status} body={body!r}"
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_error = f"{url}: {exc}"
        time.sleep(0.5)

    raise RuntimeError(
        "Timed out waiting for external worker health. "
        f"Tried: {', '.join(candidates)}. Last error: {last_error}"
    )


# ---------------------------------------------------------------------------
# Abstract backend
# ---------------------------------------------------------------------------


class DeviceBackend:
    def check_reachable(self) -> None:
        raise NotImplementedError

    def shell(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        raise NotImplementedError

    def push(self, local: Path, remote: str) -> None:
        raise NotImplementedError

    def pull(self, remote: str, local: Path) -> None:
        raise NotImplementedError

    def wipe_for_run(self, run_id: str) -> None:
        validate_run_id(run_id)
        self._do_wipe(run_id)

    def _do_wipe(self, safe_run_id: str) -> None:
        raise NotImplementedError

    def install_worker(self, local_binary: Path, remote_binary: str) -> None:
        raise NotImplementedError

    def start_worker(self, launch: WorkerLaunch) -> None:
        raise NotImplementedError

    def stop_worker(self) -> None:
        raise NotImplementedError

    def wait_health(self, url: str, timeout_s: float = 30.0) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    body = resp.read().decode("utf-8", errors="replace").strip()
                    if 200 <= resp.status < 300 and body == "ok":
                        return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(0.5)
        raise RuntimeError(f"Timed out waiting for health endpoint: {url}")

    def read_epoch_seconds(self) -> int:
        result = self.shell("date -u +%s", check=True)
        value = result.stdout.strip().splitlines()[-1].strip()
        try:
            return int(value)
        except ValueError as exc:
            raise RuntimeError(f"Could not parse device UTC epoch from: {value!r}") from exc

    def set_epoch_seconds(self, epoch_seconds: int) -> None:
        self.shell(f"date -u -s @{int(epoch_seconds)}", check=True)

    def ensure_clock_synchronized(self, max_skew_seconds: int = 300) -> None:
        host_epoch = int(time.time())
        before = self.read_epoch_seconds()
        skew = abs(host_epoch - before)
        if skew <= max_skew_seconds:
            print(f"[device] clock skew ok ({skew}s)", flush=True)
            return

        print(
            f"[device] clock skew {skew}s exceeds {max_skew_seconds}s; syncing device clock",
            flush=True,
        )
        self.set_epoch_seconds(host_epoch)
        after = self.read_epoch_seconds()
        remaining_skew = abs(int(time.time()) - after)
        if remaining_skew > max_skew_seconds:
            raise RuntimeError(
                "Device clock remains out of sync after setting UTC time "
                f"(skew={remaining_skew}s, max={max_skew_seconds}s)."
            )
        print(f"[device] clock synchronized (skew {remaining_skew}s)", flush=True)


# ---------------------------------------------------------------------------
# ADB backend
# ---------------------------------------------------------------------------


class AdbDeviceBackend(DeviceBackend):
    def __init__(self, config: DeviceConfig, serial: str | None = None):
        self.config = config
        self.serial = serial or config.connection.get("serial", "")
        self.server_socket = ""
        self._selected = False
        self._adb_base = ["adb"]
        if self.serial:
            self._adb_base += ["-s", self.serial]

    def _server_socket_candidates(self) -> list[str]:
        values = [""]
        values.extend(_as_list(self.config.connection.get("server_socket")))
        values.extend(_as_list(self.config.connection.get("server_sockets")))
        values.extend(_as_list(self.config.connection.get("server_socket_candidates")))
        return ["" if str(value).strip().lower() == "local" else str(value).strip()
                for value in _dedupe_strings(values)]

    def _adb(self, args: list[str], check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
        cmd = self._adb_base + args
        env = os.environ.copy()
        if self.server_socket:
            env["ADB_SERVER_SOCKET"] = self.server_socket
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
            env=env,
        )

    def check_reachable(self) -> None:
        last_output = ""
        for server_socket in self._server_socket_candidates():
            self.server_socket = server_socket
            label = server_socket or "local"
            result = self._adb(["devices", "-l"], check=False)
            last_output = (result.stdout or "") + (result.stderr or "")
            if self.serial:
                found = self.serial in result.stdout
            else:
                found = any("\tdevice" in line for line in result.stdout.splitlines())
            if not found:
                continue
            info = self._adb(["shell", "hostname; whoami; uname -a; uname -m"])
            self._selected = True
            print(f"[adb] selected server: {label}")
            print(f"[adb] device info:\n{info.stdout}")
            return

        target = self.serial or "<any device>"
        raise RuntimeError(
            f"ADB device '{target}' not reachable via configured server candidates. "
            f"Last adb output: {last_output.strip()}"
        )

    def _ensure_selected(self) -> None:
        if not self._selected:
            self.check_reachable()

    def shell(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        self._ensure_selected()
        return self._adb(["shell", command], check=check, timeout=120)

    def push(self, local: Path, remote: str) -> None:
        self._ensure_selected()
        self._adb(["push", str(local), remote])

    def pull(self, remote: str, local: Path) -> None:
        self._ensure_selected()
        local.parent.mkdir(parents=True, exist_ok=True)
        self._adb(["pull", remote, str(local)])

    def _do_wipe(self, safe_run_id: str) -> None:
        self._ensure_selected()
        remote_results_root = self.config.worker.get("remote_results_root", "/results/openmls")
        remote_tmp = self.config.worker.get("remote_tmp", "/tmp/openmls-benchmark")
        cmds = (
            f"set -e; "
            f"killall worker 2>/dev/null || pkill worker 2>/dev/null || true; "
            f"rm -rf {shlex.quote(remote_results_root)}/{shlex.quote(safe_run_id)}; "
            f"mkdir -p {shlex.quote(remote_results_root)}/{shlex.quote(safe_run_id)}; "
            f"rm -rf {shlex.quote(remote_tmp)}; "
            f"mkdir -p {shlex.quote(remote_tmp)}"
        )
        self._adb(["shell", cmds])

    def install_worker(self, local_binary: Path, remote_binary: str) -> None:
        self.push(local_binary, remote_binary)
        self._adb(["shell", f"chmod +x {shlex.quote(remote_binary)}"])

    def start_worker(self, launch: WorkerLaunch) -> None:
        self._ensure_selected()
        profile_flag = ""
        env_args = ""
        if launch.profile_path_template:
            profile_path = launch.profile_path_template.replace("{client_id}", launch.worker_id)
            env = {
                "OPENMLS_PROFILE_ENABLED": "true",
                "OPENMLS_PROFILE_CLIENT_IDS": launch.worker_id,
                "OPENMLS_PROFILE_PATH": profile_path,
                "OPENMLS_PROFILE_PATH_TEMPLATE": launch.profile_path_template,
                "OPENMLS_PROFILE_RUN_ID": launch.run_id,
                "OPENMLS_PROFILE_SCENARIO": launch.scenario,
                "OPENMLS_PROFILE_SCENARIO_SEED": str(launch.scenario_seed),
            }
            if launch.node_name:
                env["OPENMLS_PROFILE_NODE"] = launch.node_name
            env_args = " ".join(
                f"{key}={shlex.quote(value)}" for key, value in env.items()
            )
            profile_flag = (
                f" --profile-path-template {shlex.quote(launch.profile_path_template)}"
                f" --profile-enabled-client-ids {shlex.quote(launch.worker_id)}"
            )

        worker_cmd = (
            f"{shlex.quote(launch.binary_path)}"
            f" --name {shlex.quote(launch.worker_id)}"
            f" --ds-url {shlex.quote(launch.ds_url)}"
            f" --relay-url {shlex.quote(launch.relay_url)}"
            f" --listen-addr {shlex.quote(launch.listen_addr)}"
            f"{profile_flag}"
        )
        if env_args:
            worker_cmd = f"env {env_args} {worker_cmd}"

        launcher = (
            f"exec {worker_cmd} "
            f"> {shlex.quote(launch.remote_tmp)}/worker.log 2>&1"
        )

        cmd = (
            f"mkdir -p {shlex.quote(launch.remote_results_root)}/{shlex.quote(launch.run_id)} "
            f"{shlex.quote(launch.remote_tmp)}; "
            f"rm -f {shlex.quote(launch.remote_tmp)}/worker.pid; "
            f"start-stop-daemon -S -b -m "
            f"-p {shlex.quote(launch.remote_tmp)}/worker.pid "
            f"-x /bin/sh -- -c {shlex.quote(launcher)}"
        )
        self._adb(["shell", cmd])

    def stop_worker(self) -> None:
        self._ensure_selected()
        remote_tmp = self.config.worker.get("remote_tmp", "/tmp/openmls-benchmark")
        cmd = (
            f"start-stop-daemon -K -p {shlex.quote(remote_tmp)}/worker.pid 2>/dev/null || "
            f"killall worker 2>/dev/null || pkill worker 2>/dev/null || true"
        )
        self._adb(["shell", cmd], check=False)

# ---------------------------------------------------------------------------
# SSH backend (stub for future Raspberry Pi support)
# ---------------------------------------------------------------------------


class SshDeviceBackend(DeviceBackend):
    def __init__(self, config: DeviceConfig):
        self.config = config
        self.user = config.connection.get("user", "root")
        self.identity_file = config.connection.get("identity_file", "")
        self.password = str(config.connection.get("password", ""))
        password_env = config.connection.get("password_env", "")
        if not self.password and password_env:
            self.password = os.environ.get(password_env, "")
        self.host = config.connection.get("host", "")
        self.port = int(config.connection.get("port", 22))
        self._selected = False

    def _parse_candidate(self, value) -> tuple[str, int]:
        if isinstance(value, dict):
            host = str(value.get("host", self.host)).strip()
            port = int(value.get("port", self.port))
            return host, port
        text = str(value).strip()
        if text.count(":") == 1:
            host, port = text.rsplit(":", 1)
            if port.isdigit():
                return host, int(port)
        return text, self.port

    def _candidate_hosts(self) -> list[tuple[str, int]]:
        values = [
            {"host": self.config.connection.get("host", ""), "port": self.config.connection.get("port", 22)}
        ]
        values.extend(_as_list(self.config.connection.get("host_candidates")))
        values.extend(_as_list(self.config.connection.get("connection_candidates")))
        values.extend(_as_list(self.config.connection.get("ssh_candidates")))
        seen: set[tuple[str, int]] = set()
        result: list[tuple[str, int]] = []
        for value in values:
            host, port = self._parse_candidate(value)
            if not host:
                continue
            key = (host, port)
            if key not in seen:
                seen.add(key)
                result.append(key)
        return result

    def _ssh_base(self) -> list[str]:
        cmd = ["ssh"]
        if self.identity_file:
            cmd += ["-i", str(Path(self.identity_file).expanduser())]
        cmd += ["-p", str(self.port),
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "LogLevel=ERROR",
                "-o", "BatchMode=no",
                "-o", "ConnectTimeout=10",
                f"{self.user}@{self.host}"]
        return cmd

    def _run_password_command(
        self,
        cmd: list[str],
        *,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess:
        try:
            import pexpect
        except ImportError as exc:
            raise RuntimeError(
                "Password-based SSH requires the Python 'pexpect' package. "
                "Install pexpect, configure connection.identity_file, or use an SSH agent."
            ) from exc

        child = pexpect.spawn(cmd[0], cmd[1:], encoding="utf-8", timeout=timeout)
        output_parts: list[str] = []
        password_prompts = 0

        try:
            while True:
                idx = child.expect([
                    r"(?i)are you sure you want to continue connecting",
                    r"(?i)(?:password|passphrase).*:",
                    pexpect.EOF,
                    pexpect.TIMEOUT,
                ])
                before = child.before or ""

                if idx == 0:
                    child.sendline("yes")
                elif idx == 1:
                    password_prompts += 1
                    if password_prompts > 4:
                        child.close(force=True)
                        output = "".join(output_parts)
                        result = subprocess.CompletedProcess(cmd, 255, output, output)
                        if check:
                            raise subprocess.CalledProcessError(
                                result.returncode,
                                cmd,
                                output=result.stdout,
                                stderr=result.stderr,
                            )
                        return result
                    child.sendline(self.password)
                elif idx == 2:
                    output_parts.append(before)
                    break
                else:
                    child.close(force=True)
                    output_parts.append(before)
                    output = "".join(output_parts)
                    if check:
                        raise subprocess.TimeoutExpired(cmd, timeout, output=output)
                    return subprocess.CompletedProcess(cmd, 124, output, output)
        finally:
            if child.isalive():
                child.close(force=True)
            else:
                child.close()

        output = "".join(output_parts)
        if child.exitstatus is not None:
            returncode = int(child.exitstatus)
        elif child.signalstatus is not None:
            returncode = 128 + int(child.signalstatus)
        else:
            returncode = 1

        result = subprocess.CompletedProcess(
            cmd,
            returncode,
            output,
            output if returncode != 0 else "",
        )
        if check and returncode != 0:
            raise subprocess.CalledProcessError(
                returncode,
                cmd,
                output=result.stdout,
                stderr=result.stderr,
            )
        return result

    def _run_command(
        self,
        cmd: list[str],
        *,
        check: bool = True,
        timeout: int = 60,
    ) -> subprocess.CompletedProcess:
        if self.password:
            return self._run_password_command(cmd, check=check, timeout=timeout)
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
        )

    def _ssh(self, command: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
        cmd = self._ssh_base() + [command]
        return self._run_command(cmd, check=check, timeout=timeout)

    def check_reachable(self) -> None:
        errors: list[str] = []
        for host, port in self._candidate_hosts():
            self.host = host
            self.port = port
            result = self._ssh("hostname; whoami; uname -a; uname -m", check=False)
            if result.returncode == 0:
                self._selected = True
                print(f"[ssh] selected target: {self.user}@{self.host}:{self.port}")
                print(f"[ssh] device info:\n{result.stdout}")
                return
            errors.append(f"{self.user}@{host}:{port}: {(result.stderr or result.stdout).strip()}")
        raise RuntimeError(
            "SSH device not reachable via configured candidates:\n" + "\n".join(errors)
        )

    def _ensure_selected(self) -> None:
        if not self._selected:
            self.check_reachable()

    def shell(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        self._ensure_selected()
        return self._ssh(command, check=check, timeout=120)

    def _scp_cmd(self, src: str, dst: str, *, recursive: bool = False) -> list[str]:
        cmd = ["scp"]
        if recursive:
            cmd.append("-r")
        if self.identity_file:
            cmd += ["-i", str(Path(self.identity_file).expanduser())]
        cmd += ["-P", str(self.port),
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "LogLevel=ERROR",
                "-o", "BatchMode=no",
                "-o", "ConnectTimeout=10"]
        cmd += [src, dst]
        return cmd

    def push(self, local: Path, remote: str) -> None:
        self._ensure_selected()
        remote_scp = f"{self.user}@{self.host}:{remote}"
        self._run_command(self._scp_cmd(str(local), remote_scp), check=True, timeout=120)

    def pull(self, remote: str, local: Path) -> None:
        self._ensure_selected()
        local.parent.mkdir(parents=True, exist_ok=True)
        remote_scp = f"{self.user}@{self.host}:{remote}"
        self._run_command(
            self._scp_cmd(remote_scp, str(local), recursive=True),
            check=True,
            timeout=120,
        )

    def _do_wipe(self, safe_run_id: str) -> None:
        self._ensure_selected()
        remote_results_root = self.config.worker.get("remote_results_root", "/results/openmls")
        remote_tmp = self.config.worker.get("remote_tmp", "/tmp/openmls-benchmark")
        cmds = (
            f"set -e; "
            f"killall worker 2>/dev/null || pkill worker 2>/dev/null || true; "
            f"rm -rf {shlex.quote(remote_results_root)}/{shlex.quote(safe_run_id)}; "
            f"mkdir -p {shlex.quote(remote_results_root)}/{shlex.quote(safe_run_id)}; "
            f"rm -rf {shlex.quote(remote_tmp)}; "
            f"mkdir -p {shlex.quote(remote_tmp)}"
        )
        self._ssh(cmds)

    def install_worker(self, local_binary: Path, remote_binary: str) -> None:
        self._ensure_selected()
        remote_parent = str(PurePosixPath(remote_binary).parent)
        self._ssh(f"mkdir -p {shlex.quote(remote_parent)}")
        self.push(local_binary, remote_binary)
        self._ssh(f"chmod +x {shlex.quote(remote_binary)}")

    def set_epoch_seconds(self, epoch_seconds: int) -> None:
        self._ensure_selected()
        self.shell(f"sudo -S date -u -s @{int(epoch_seconds)}", check=True)

    def start_worker(self, launch: WorkerLaunch) -> None:
        self._ensure_selected()
        profile_flag = ""
        env_prefix = ""
        if launch.profile_path_template:
            profile_path = launch.profile_path_template.replace("{client_id}", launch.worker_id)
            env = {
                "OPENMLS_PROFILE_ENABLED": "true",
                "OPENMLS_PROFILE_CLIENT_IDS": launch.worker_id,
                "OPENMLS_PROFILE_PATH": profile_path,
                "OPENMLS_PROFILE_PATH_TEMPLATE": launch.profile_path_template,
                "OPENMLS_PROFILE_RUN_ID": launch.run_id,
                "OPENMLS_PROFILE_SCENARIO": launch.scenario,
                "OPENMLS_PROFILE_SCENARIO_SEED": str(launch.scenario_seed),
            }
            if launch.node_name:
                env["OPENMLS_PROFILE_NODE"] = launch.node_name
            env_prefix = " ".join(
                f"{key}={shlex.quote(value)}" for key, value in env.items()
            ) + " "
            profile_flag = (
                f" --profile-path-template {shlex.quote(launch.profile_path_template)}"
                f" --profile-enabled-client-ids {shlex.quote(launch.worker_id)}"
            )

        cmd = (
            f"mkdir -p {shlex.quote(launch.remote_results_root)}/{shlex.quote(launch.run_id)} "
            f"{shlex.quote(launch.remote_tmp)}; "
            f"rm -f {shlex.quote(launch.remote_tmp)}/worker.pid; "
            f"{env_prefix}nohup {shlex.quote(launch.binary_path)}"
            f" --name {shlex.quote(launch.worker_id)}"
            f" --ds-url {shlex.quote(launch.ds_url)}"
            f" --relay-url {shlex.quote(launch.relay_url)}"
            f" --listen-addr {shlex.quote(launch.listen_addr)}"
            f"{profile_flag}"
            f" > {shlex.quote(launch.remote_tmp)}/worker.log 2>&1 &"
            f" echo $! > {shlex.quote(launch.remote_tmp)}/worker.pid"
        )
        self._ssh(cmd)

    def stop_worker(self) -> None:
        self._ensure_selected()
        self._ssh("killall worker 2>/dev/null || pkill worker 2>/dev/null || true", check=False)

# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_backend(config: DeviceConfig) -> DeviceBackend:
    conn_type = config.connection.get("type", "adb")

    if conn_type == "adb":
        return AdbDeviceBackend(config)
    elif conn_type == "ssh":
        return SshDeviceBackend(config)
    else:
        raise ValueError(f"Unsupported connection type: {conn_type}")


# ---------------------------------------------------------------------------
# Build worker layout entries for external devices
# ---------------------------------------------------------------------------


def build_external_device_layout_entry(
    config: DeviceConfig,
    transport_ip: str,
    host_ip: str,
    worker_port: int,
    ds_port: int,
    relay_port: int,
    run_id: str,
    worker_url: str | None = None,
) -> tuple[dict, dict, str]:
    """
    Returns (layout_client_entry, layout_physical_worker_entry, worker_file_line).

    The `worker_file_line` is in ID=URL format for the workers file.
    """
    worker_id = config.worker.get("id", config.id)
    device_ip = config.transport.get("device_ip", transport_ip)
    url = (worker_url or f"http://{device_ip}:{worker_port}").rstrip("/")

    worker_file_line = f"{worker_id}={url}"

    meta = config.metadata

    layout_client = {
        "client_id": worker_id,
        "physical_worker_id": config.id,
        "container_mode": "singleton",
        "profile_enabled": True,
        "command_url": url,
        "health_url": f"{url}/health",
        "execution_backend": meta.get("execution_backend", "real_device"),
        "device_kind": meta.get("device_kind", config.kind),
        "transport": meta.get("transport", config.transport.get("type", "usb_rndis")),
        "access_backend": meta.get("access_backend", config.connection.get("type", "adb")),
        "arch": config.target.get("arch", "armv7l"),
        "rust_target": config.target.get("rust_target", ""),
        "resource_limit_cpus": None,
        "resource_limit_memory": None,
        "resource_limit_memory_bytes": None,
        "resource_limit_memory_swap": None,
        "resource_limit_memory_swap_bytes": None,
        "resource_limit_pids": None,
        "resource_profile": "",
    }

    layout_physical_worker = {
        "physical_worker_id": config.id,
        "container_mode": "singleton",
        "client_ids": [worker_id],
        "base_url": url,
        "profile_enabled_client_ids": [worker_id],
        "execution_backend": meta.get("execution_backend", "real_device"),
        "device_kind": meta.get("device_kind", config.kind),
        "transport": meta.get("transport", config.transport.get("type", "usb_rndis")),
        "access_backend": meta.get("access_backend", config.connection.get("type", "adb")),
        "arch": config.target.get("arch", "armv7l"),
        "rust_target": config.target.get("rust_target", ""),
        "resource_limit_cpus": None,
        "resource_limit_memory": None,
        "resource_limit_memory_bytes": None,
        "resource_limit_memory_swap": None,
        "resource_limit_memory_swap_bytes": None,
        "resource_limit_pids": None,
        "resource_profile": "",
    }

    return layout_client, layout_physical_worker, worker_file_line
