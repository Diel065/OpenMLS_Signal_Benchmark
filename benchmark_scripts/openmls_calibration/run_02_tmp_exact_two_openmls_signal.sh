#!/usr/bin/env bash
# Temporary execution wrapper for one OpenMLS run and one Signal run.
# It intentionally does not modify or source run_02_unconstrained_container_baseline.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OPENMLS_DIR="$REPO_ROOT/OpenMLS_containerized"
SIGNAL_DIR="$REPO_ROOT/Signal_containerized"

OUTPUT_ROOT="${OUTPUT_ROOT:-/tmp}"
if [ "$OUTPUT_ROOT" != "/tmp" ]; then
  echo "ERROR: this temporary script is pinned to output root /tmp; got OUTPUT_ROOT=$OUTPUT_ROOT" >&2
  exit 2
fi

PROTOCOL="${PROTOCOL:-both}"
case "$PROTOCOL" in
  both|openmls|signal) ;;
  *)
    echo "ERROR: PROTOCOL must be one of: both, openmls, signal; got $PROTOCOL" >&2
    exit 2
    ;;
esac

WORKERS="${WORKERS:-1024}"
MAX_SIZE="${MAX_SIZE:-1024}"
PROFILED_SINGLETON_COUNT=10
SINGLETON_MIN_COUNT=10
SINGLETON_FRACTION="0.000000001"
PACKED_PER_CONTAINER=64
PACKED_INTERNAL_PARALLELISM=16
BRIDGE_COUNT=2
STEP_SIZE="16"
APP_ROUNDS=4
UPDATE_ROUNDS=4
ROUNDTRIPS=1
MAX_APP_SAMPLES_PER_PAYLOAD=4
MAX_UPDATE_SAMPLES_PER_PLATEAU=4
PAYLOAD_SIZES="32,512,2048"
CPU_AFFINITY_SAMPLE="${CPU_AFFINITY_SAMPLE:-20}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
WORKER_HEALTH_TIMEOUT="${WORKER_HEALTH_TIMEOUT:-600}"
WORKER_HTTP_POOL="${WORKER_HTTP_POOL:-64}"
WORKER_OUTBOUND_PERMITS="${WORKER_OUTBOUND_PERMITS:-32}"
FANOUT_PARALLELISM="${FANOUT_PARALLELISM:-128}"
FANOUT_MIN="${FANOUT_MIN:-16}"
NOFILE_LIMIT="${NOFILE_LIMIT:-1048576}"
MIN_TMP_FREE_GB="${MIN_TMP_FREE_GB:-10}"
BUILD_IMAGES="${BUILD_IMAGES:-1}"

DATE_TAG="$(date +%Y%m%d_%H%M%S)"
RUN_TOKEN="tmp02_${DATE_TAG}_pid$$"
OPENMLS_RUN_ID="${RUN_TOKEN}_openmls"
SIGNAL_RUN_ID="${RUN_TOKEN}_signal"
OPENMLS_RUN_DIR="$OUTPUT_ROOT/$OPENMLS_RUN_ID"
SIGNAL_RUN_DIR="$OUTPUT_ROOT/$SIGNAL_RUN_ID"
REPORT="$OUTPUT_ROOT/${RUN_TOKEN}_report.txt"
COMMAND_LOG="$OUTPUT_ROOT/${RUN_TOKEN}_commands.sh"

OPENMLS_STATUS="not_run"
SIGNAL_STATUS="not_run"
OPENMLS_VERIFY_STATUS="not_run"
SIGNAL_VERIFY_STATUS="not_run"

log() {
  printf '%s\n' "$*" | tee -a "$REPORT"
}

quote_command() {
  local arg
  for arg in "$@"; do
    printf '%q ' "$arg"
  done
  printf '\n'
}

python_bin_for() {
  local dir="$1"
  if [ -x "$dir/.venv/bin/python" ]; then
    printf '%s\n' "$dir/.venv/bin/python"
  else
    printf '%s\n' python3
  fi
}

relpath_from() {
  python3 - "$1" "$2" <<'PY'
import os, sys
print(os.path.relpath(sys.argv[1], sys.argv[2]))
PY
}

random_seed() {
  python3 - <<'PY'
import random
print(random.randint(1, 2147483647))
PY
}

cleanup_stack() {
  local run_dir="$1"
  if [ -f "$run_dir/docker-compose.generated.yml" ]; then
    docker compose -f "$run_dir/docker-compose.generated.yml" down --timeout 2 >/dev/null 2>&1 || true
  fi
}

cleanup_all() {
  cleanup_stack "$OPENMLS_RUN_DIR"
  cleanup_stack "$SIGNAL_RUN_DIR"
}
trap cleanup_all EXIT

preflight() {
  : > "$REPORT"
  : > "$COMMAND_LOG"

  log "temporary two-run benchmark wrapper"
  log "repo=$REPO_ROOT"
  log "git_commit=$(git -C "$REPO_ROOT" rev-parse HEAD)"
  log "report=$REPORT"
  log "command_log=$COMMAND_LOG"
  log ""

  if [ -d "$OPENMLS_RUN_DIR" ] || [ -d "$SIGNAL_RUN_DIR" ]; then
    log "ERROR: fresh output directories already exist; refusing to risk stale data"
    log "openmls_run_dir=$OPENMLS_RUN_DIR"
    log "signal_run_dir=$SIGNAL_RUN_DIR"
    exit 2
  fi

  local free_kb min_kb cpus
  free_kb="$(df -Pk "$OUTPUT_ROOT" | awk 'NR==2 {print $4}')"
  min_kb=$((MIN_TMP_FREE_GB * 1024 * 1024))
  if [ "$free_kb" -lt "$min_kb" ]; then
    log "ERROR: not enough free space in $OUTPUT_ROOT: have ${free_kb} KiB, require ${min_kb} KiB (${MIN_TMP_FREE_GB} GiB)"
    exit 2
  fi
  log "tmp_free_kib=$free_kb"

  cpus="$(nproc)"
  if [ "$cpus" -lt "$PROFILED_SINGLETON_COUNT" ]; then
    log "ERROR: need at least $PROFILED_SINGLETON_COUNT usable logical CPUs for profiled containers; nproc=$cpus"
    exit 2
  fi
  log "usable_logical_cpus=$cpus"

  docker info >/dev/null
  docker compose version >/dev/null
  python3 --version >/dev/null
  log "docker_ready=yes"
  log "docker_compose_ready=yes"
  log "python_ready=yes"
  if command -v cargo >/dev/null 2>&1; then
    log "cargo_ready=yes"
  else
  log "cargo_ready=no (ok when using --runner-in-docker with built images)"
  fi
  log "build_images=$BUILD_IMAGES"
  log "protocol=$PROTOCOL"

  log ""
  log "configuration_common: workers=$WORKERS max_size=$MAX_SIZE step_size=$STEP_SIZE app_rounds=$APP_ROUNDS update_rounds=$UPDATE_ROUNDS profiled_containers=$PROFILED_SINGLETON_COUNT output_root=$OUTPUT_ROOT"
  log "payload_sizes=$PAYLOAD_SIZES"
  log "payload_limitation=existing runners enumerate fixed payload sizes; exact per-message random discrete choice from {32,512,2048} is not supported without a Rust runner patch"
  log "signal_update_limitation=Signal runner accepts --update-rounds but current pairwise Signal path has no MLS-style self-update phase"
  log "output_separation=openmls:$OPENMLS_RUN_DIR signal:$SIGNAL_RUN_DIR"
  log ""
}

run_openmls() {
  local py output_arg scenario_seed singleton_seed
  py="$(python_bin_for "$OPENMLS_DIR")"
  output_arg="$(relpath_from "$OUTPUT_ROOT" "$OPENMLS_DIR")"
  scenario_seed="$(random_seed)"
  singleton_seed="$(random_seed)"
  local image_args=()
  if [ "$BUILD_IMAGES" = "1" ]; then
    image_args=(--build-images)
  fi
  local cmd=(
    "$py" scripts/run_compose_benchmark.py
    --workers "$WORKERS"
    --run-id "$OPENMLS_RUN_ID"
    --scenario tmp-two-run-unconstrained-container-baseline
    --scenario-seed "$scenario_seed"
    --singleton-selection-seed "$singleton_seed"
    --output-dir "$output_arg"
    --worker-layout-mode hybrid
    --singleton-min-count "$SINGLETON_MIN_COUNT"
    --singleton-fraction "$SINGLETON_FRACTION"
    --packed-clients-per-container "$PACKED_PER_CONTAINER"
    --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM"
    --bridge-count "$BRIDGE_COUNT"
    --profiled-singleton-count "$PROFILED_SINGLETON_COUNT"
    --cpu-affinity-mode profiled-nor-background
    --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE"
    --health-timeout-seconds "$HEALTH_TIMEOUT"
    --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT"
    --health-poll-seconds 0.5
    --worker-health-poll-ms 250
    --min-size 2
    --max-size "$MAX_SIZE"
    --step-size "$STEP_SIZE"
    --plateau-order staircase
    --roundtrips "$ROUNDTRIPS"
    --update-rounds "$UPDATE_ROUNDS"
    --app-rounds "$APP_ROUNDS"
    --max-update-samples-per-plateau "$MAX_UPDATE_SAMPLES_PER_PLATEAU"
    --max-app-samples-per-payload "$MAX_APP_SAMPLES_PER_PAYLOAD"
    --payload-sizes "$PAYLOAD_SIZES"
    --http-pool-max-idle-per-host "$WORKER_HTTP_POOL"
    --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL"
    --worker-outbound-http-permits "$WORKER_OUTBOUND_PERMITS"
    --max-fanout-parallelism "$FANOUT_PARALLELISM"
    --min-fanout-parallelism "$FANOUT_MIN"
    --force-cleanup-mls-ports
    --runner-in-docker
    --keep-stack-up
    --keep-stack-up-on-failure
    "${image_args[@]}"
  )

  log "===== OpenMLS benchmark execution 1/2 ====="
  log "openmls_command=$(quote_command "${cmd[@]}")"
  quote_command "cd" "$OPENMLS_DIR" "&&" "${cmd[@]}" >> "$COMMAND_LOG"
  set +e
  (cd "$OPENMLS_DIR" || exit; ulimit -n "$NOFILE_LIMIT" 2>/dev/null || true; "${cmd[@]}")
  local rc=$?
  set -e
  OPENMLS_STATUS="exit_$rc"
  log "openmls_exit_code=$rc"
}

run_signal() {
  local py output_arg scenario_seed singleton_seed
  py="$(python_bin_for "$SIGNAL_DIR")"
  output_arg="$(relpath_from "$OUTPUT_ROOT" "$SIGNAL_DIR")"
  scenario_seed="$(random_seed)"
  singleton_seed="$(random_seed)"
  local image_args=()
  if [ "$BUILD_IMAGES" = "1" ]; then
    image_args=(--build-images)
  fi
  local cmd=(
    "$py" scripts/run_compose_benchmark.py
    --workers "$WORKERS"
    --run-id "$SIGNAL_RUN_ID"
    --scenario tmp-two-run-unconstrained-container-baseline
    --scenario-seed "$scenario_seed"
    --singleton-selection-seed "$singleton_seed"
    --output-dir "$output_arg"
    --worker-layout-mode hybrid
    --singleton-min-count "$SINGLETON_MIN_COUNT"
    --singleton-fraction "$SINGLETON_FRACTION"
    --packed-clients-per-container "$PACKED_PER_CONTAINER"
    --packed-worker-internal-parallelism "$PACKED_INTERNAL_PARALLELISM"
    --bridge-count "$BRIDGE_COUNT"
    --profiled-singleton-count "$PROFILED_SINGLETON_COUNT"
    --cpu-affinity-mode profiled-nor-background
    --cpu-affinity-sample-seconds "$CPU_AFFINITY_SAMPLE"
    --health-timeout-seconds "$HEALTH_TIMEOUT"
    --worker-health-timeout-seconds "$WORKER_HEALTH_TIMEOUT"
    --health-poll-seconds 0.5
    --worker-health-poll-ms 250
    --min-size 2
    --max-size "$MAX_SIZE"
    --step-size "$STEP_SIZE"
    --plateau-order staircase
    --roundtrips "$ROUNDTRIPS"
    --update-rounds "$UPDATE_ROUNDS"
    --app-rounds "$APP_ROUNDS"
    --max-update-samples-per-plateau "$MAX_UPDATE_SAMPLES_PER_PLATEAU"
    --max-app-samples-per-payload "$MAX_APP_SAMPLES_PER_PAYLOAD"
    --payload-sizes "$PAYLOAD_SIZES"
    --http-pool-max-idle-per-host "$WORKER_HTTP_POOL"
    --worker-http-pool-max-idle-per-host "$WORKER_HTTP_POOL"
    --worker-outbound-http-permits "$WORKER_OUTBOUND_PERMITS"
    --max-fanout-parallelism "$FANOUT_PARALLELISM"
    --min-fanout-parallelism "$FANOUT_MIN"
    --force-cleanup-signal-ports
    --runner-in-docker
    --keep-stack-up
    --keep-stack-up-on-failure
    "${image_args[@]}"
  )

  log "===== Signal benchmark execution 2/2 ====="
  log "signal_command=$(quote_command "${cmd[@]}")"
  quote_command "cd" "$SIGNAL_DIR" "&&" "${cmd[@]}" >> "$COMMAND_LOG"
  set +e
  (cd "$SIGNAL_DIR" || exit; ulimit -n "$NOFILE_LIMIT" 2>/dev/null || true; "${cmd[@]}")
  local rc=$?
  set -e
  SIGNAL_STATUS="exit_$rc"
  log "signal_exit_code=$rc"
}

verify_run() {
  local protocol="$1" run_dir="$2" run_id="$3" profile_glob="$4"
  PROTOCOL="$protocol" RUN_DIR="$run_dir" RUN_ID="$run_id" PROFILE_GLOB="$profile_glob" REPORT="$REPORT" python3 <<'PY'
import csv, json, math, os, re, subprocess, sys
from pathlib import Path

protocol = os.environ["PROTOCOL"]
run_dir = Path(os.environ["RUN_DIR"])
run_id = os.environ["RUN_ID"]
profile_glob = os.environ["PROFILE_GLOB"]
report = Path(os.environ["REPORT"])

def parse_cpuset(value):
    cpus = set()
    for part in (value or "").split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            a, b = part.split('-', 1)
            cpus.update(range(int(a), int(b) + 1))
        else:
            cpus.add(int(part))
    return cpus

def proc_allowed(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("Cpus_allowed_list:"):
                return line.split(':', 1)[1].strip()
    except OSError:
        return ""
    return ""

def compose_ps_id(compose_file, service):
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "ps", "-q", service],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""

def docker_inspect(container_id):
    if not container_id:
        return {}
    result = subprocess.run(
        ["docker", "inspect", container_id],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return {}
    try:
        return json.loads(result.stdout)[0]
    except Exception:
        return {}

summary = {
    "protocol": protocol,
    "run_id": run_id,
    "run_dir": str(run_dir),
    "exists": run_dir.exists(),
    "events_csv_exists": False,
    "events_csv_non_empty": False,
    "events_row_count": 0,
    "artifact_count": 0,
    "artifacts": [],
    "empty_files": [],
    "profile_file_count": 0,
    "profile_nonempty_count": 0,
    "profile_exactly_10": False,
    "layout_profile_enabled_count": None,
    "affinity_plan_profiled_count": None,
    "affinity_distinct_single_cpu_assignments": False,
    "actual_cpuset_pass": False,
    "actual_cpuset_checks": [],
    "log_issue_count": 0,
    "log_issue_samples": [],
    "signal_subspans_valid": None,
    "signal_subspan_details": {},
}

if run_dir.exists():
    for p in sorted(run_dir.iterdir()):
        kind = "dir" if p.is_dir() else "file"
        size = None if p.is_dir() else p.stat().st_size
        summary["artifacts"].append({"name": p.name, "type": kind, "size": size})
        if p.is_file() and p.stat().st_size == 0:
            summary["empty_files"].append(p.name)
    summary["artifact_count"] = len(summary["artifacts"])

events_path = run_dir / "events.csv"
rows = []
if events_path.exists():
    summary["events_csv_exists"] = True
    summary["events_csv_non_empty"] = events_path.stat().st_size > 0
    try:
        with events_path.open(newline='', encoding='utf-8') as handle:
            rows = list(csv.DictReader(handle))
        summary["events_row_count"] = len(rows)
    except Exception as exc:
        summary["events_read_error"] = str(exc)

profile_files = sorted(run_dir.glob(profile_glob)) if run_dir.exists() else []
summary["profile_file_count"] = len(profile_files)
summary["profile_nonempty_count"] = sum(1 for p in profile_files if p.stat().st_size > 0)
summary["profile_exactly_10"] = summary["profile_nonempty_count"] == 10

layout_path = run_dir / "worker_layout.json"
if layout_path.exists():
    try:
        layout = json.loads(layout_path.read_text(encoding='utf-8'))
        clients = layout.get("clients", [])
        summary["layout_profile_enabled_count"] = sum(1 for c in clients if c.get("profile_enabled"))
    except Exception as exc:
        summary["layout_error"] = str(exc)

plan_path = run_dir / "cpu_affinity_plan.json"
compose_file = run_dir / "docker-compose.generated.yml"
if plan_path.exists():
    try:
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
        assignments = plan.get("profiled_assignments", [])
        summary["affinity_plan_profiled_count"] = len(assignments)
        assigned = [a.get("assigned_cpus", []) for a in assignments]
        flat = [cpu for cpus in assigned for cpu in cpus]
        summary["affinity_distinct_single_cpu_assignments"] = (
            len(assignments) == 10 and all(len(cpus) == 1 for cpus in assigned) and len(flat) == len(set(flat))
        )
        checks = []
        if compose_file.exists():
            for a in assignments:
                service = a.get("container_name", "")
                expected = ",".join(str(c) for c in a.get("assigned_cpus", []))
                cid = compose_ps_id(compose_file, service)
                inspect = docker_inspect(cid)
                host_config = inspect.get("HostConfig") or {}
                state = inspect.get("State") or {}
                pid = int(state.get("Pid") or 0)
                docker_cpuset = host_config.get("CpusetCpus") or ""
                proc_cpuset = proc_allowed(pid) if pid > 0 else ""
                ok = bool(cid) and state.get("Running") and parse_cpuset(expected) == parse_cpuset(docker_cpuset) == parse_cpuset(proc_cpuset)
                checks.append({
                    "service": service,
                    "container_id": cid,
                    "expected_cpuset": expected,
                    "docker_cpuset": docker_cpuset,
                    "proc_cpus_allowed_list": proc_cpuset,
                    "running": bool(state.get("Running")),
                    "status": "pass" if ok else "fail",
                })
        summary["actual_cpuset_checks"] = checks
        summary["actual_cpuset_pass"] = bool(checks) and all(c["status"] == "pass" for c in checks)
    except Exception as exc:
        summary["affinity_error"] = str(exc)

issue_re = re.compile(r"\b(error|panic|exception|missing[_ -]?span|nan|partial|failed)\b", re.I)
for log_name in ("terminal_output.txt", "compose_services.log"):
    p = run_dir / log_name
    if not p.exists() or not p.is_file():
        continue
    try:
        for line in p.read_text(encoding='utf-8', errors='replace').splitlines():
            if issue_re.search(line) and "fanout-error-rate-threshold" not in line:
                summary["log_issue_count"] += 1
                if len(summary["log_issue_samples"]) < 20:
                    summary["log_issue_samples"].append(f"{log_name}: {line[:300]}")
    except OSError:
        pass

if protocol == "signal":
    required = [
        "signal_session_establish.total",
        "signal_session_establish.process_prekey_bundle",
        "signal_application_message_create.total",
        "signal_application_message_create.ratchet_encrypt_payload",
        "signal_application_message_receive.total",
        "signal_application_message_receive.message_decrypt",
    ]
    names = set()
    numeric_values = []
    run_id_matches = 0
    protocol_rows = 0
    protocol_core_rows = 0
    for row in rows:
        names.update(v for v in (row.get("span_name"), row.get("op"), row.get("event_subtype")) if v)
        if row.get("run_id") == run_id:
            run_id_matches += 1
        stack = (row.get("protocol_stack") or row.get("implementation") or "").lower()
        if "signal" in stack:
            protocol_rows += 1
        if (row.get("span_layer") or "") == "protocol_core":
            protocol_core_rows += 1
        for col in ("ts_unix_ns", "wall_ns", "cpu_thread_ns"):
            val = row.get(col)
            if val not in (None, ""):
                try:
                    f = float(val)
                    if not math.isnan(f):
                        numeric_values.append((col, f))
                except ValueError:
                    pass
    missing = [name for name in required if name not in names]
    nonzero = any(v > 0 for _, v in numeric_values)
    summary["signal_subspan_details"] = {
        "required": required,
        "missing": missing,
        "recorded_name_count": len(names),
        "run_id_matching_rows": run_id_matches,
        "signal_protocol_rows": protocol_rows,
        "protocol_core_rows": protocol_core_rows,
        "numeric_value_count": len(numeric_values),
        "numeric_values_nonzero": nonzero,
    }
    summary["signal_subspans_valid"] = (
        bool(rows)
        and not missing
        and run_id_matches > 0
        and protocol_rows > 0
        and protocol_core_rows > 0
        and len(numeric_values) > 0
        and nonzero
    )

verification_path = run_dir / f"tmp_two_run_verification_{protocol}.json"
if run_dir.exists():
    verification_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')

ok = (
    summary["events_csv_exists"]
    and summary["events_csv_non_empty"]
    and summary["profile_exactly_10"]
    and summary["affinity_distinct_single_cpu_assignments"]
    and summary["actual_cpuset_pass"]
)
if protocol == "signal":
    ok = ok and bool(summary["signal_subspans_valid"])

with report.open('a', encoding='utf-8') as handle:
    handle.write(f"\n===== {protocol} output inspection =====\n")
    handle.write(f"run_dir={run_dir}\n")
    handle.write(f"events_csv_exists={summary['events_csv_exists']} non_empty={summary['events_csv_non_empty']} rows={summary['events_row_count']}\n")
    handle.write(f"artifact_count={summary['artifact_count']}\n")
    handle.write("artifacts=" + ", ".join(f"{a['name']}:{a['type']}:{a['size']}" for a in summary['artifacts']) + "\n")
    handle.write(f"empty_files={summary['empty_files']}\n")
    handle.write(f"profile_files={summary['profile_file_count']} nonempty={summary['profile_nonempty_count']} exactly_10={summary['profile_exactly_10']}\n")
    handle.write(f"layout_profile_enabled_count={summary['layout_profile_enabled_count']}\n")
    handle.write(f"affinity_plan_profiled_count={summary['affinity_plan_profiled_count']} distinct_single_cpu={summary['affinity_distinct_single_cpu_assignments']} actual_cpuset_pass={summary['actual_cpuset_pass']}\n")
    handle.write(f"log_issue_count={summary['log_issue_count']}\n")
    for sample in summary['log_issue_samples']:
        handle.write(f"log_issue_sample={sample}\n")
    if protocol == "signal":
        handle.write(f"signal_subspans_valid={summary['signal_subspans_valid']} details={json.dumps(summary['signal_subspan_details'], sort_keys=True)}\n")
    handle.write(f"verification_json={verification_path}\n")
    handle.write(f"verification_status={'pass' if ok else 'fail'}\n")

sys.exit(0 if ok else 1)
PY
}

preflight

if [ "$PROTOCOL" != "signal" ]; then
  run_openmls
  set +e
  verify_run openmls "$OPENMLS_RUN_DIR" "$OPENMLS_RUN_ID" 'client-*.jsonl'
  OPENMLS_VERIFY_RC=$?
  set -e
  OPENMLS_VERIFY_STATUS="exit_$OPENMLS_VERIFY_RC"
  cleanup_stack "$OPENMLS_RUN_DIR"
else
  OPENMLS_STATUS="skipped"
  OPENMLS_VERIFY_STATUS="skipped"
fi

if [ "$PROTOCOL" != "openmls" ]; then
  run_signal
  set +e
  verify_run signal "$SIGNAL_RUN_DIR" "$SIGNAL_RUN_ID" 'participant-*.jsonl'
  SIGNAL_VERIFY_RC=$?
  set -e
  SIGNAL_VERIFY_STATUS="exit_$SIGNAL_VERIFY_RC"
  cleanup_stack "$SIGNAL_RUN_DIR"
else
  SIGNAL_STATUS="skipped"
  SIGNAL_VERIFY_STATUS="skipped"
fi

log ""
log "===== final summary ====="
log "git_commit=$(git -C "$REPO_ROOT" rev-parse HEAD)"
log "code_config_changes_made_by_this_script=none"
log "openmls_status=$OPENMLS_STATUS verify=$OPENMLS_VERIFY_STATUS path=$OPENMLS_RUN_DIR"
log "signal_status=$SIGNAL_STATUS verify=$SIGNAL_VERIFY_STATUS path=$SIGNAL_RUN_DIR"
log "report=$REPORT"
log "command_log=$COMMAND_LOG"

FAILED=0
if [ "$PROTOCOL" != "signal" ] && { [ "$OPENMLS_STATUS" != "exit_0" ] || [ "$OPENMLS_VERIFY_STATUS" != "exit_0" ]; }; then
  FAILED=1
fi
if [ "$PROTOCOL" != "openmls" ] && { [ "$SIGNAL_STATUS" != "exit_0" ] || [ "$SIGNAL_VERIFY_STATUS" != "exit_0" ]; }; then
  FAILED=1
fi
if [ "$FAILED" != "0" ]; then
  exit 1
fi
