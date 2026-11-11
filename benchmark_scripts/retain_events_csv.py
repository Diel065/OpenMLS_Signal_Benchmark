"""OUTPUT_RETENTION=events-csv: keep events.csv as events.csv.xz and delete the profile traces.

The benchmark scripts call this after all their own checks of a run. For each run directory:
1. the run outcome must be "success";
2. the profile lines of the participant-*/client-*.jsonl traces must equal the events.csv
   rows (runner failure events, runner_event_kind set, are left out on both sides);
3. events.csv is compressed with xz -9e, and the archive must decompress to the same SHA-256;
4. storage_retention.json records every deleted file; then the traces, the raw copies pulled
   from external devices (external/) that are, byte for byte, a prefix of their trace, and the
   uncompressed events.csv are deleted. Any other raw copy stays (for example one that ends in
   an event without its newline, which the pull left out of the trace).
A run that fails step 1 or 2 keeps all files; a run that has storage_retention.json is never
touched again. All other files always stay. Mode "full" (the scripts' default) changes nothing.
"""

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

MODES = ("full", "events-csv")
MANIFEST = "storage_retention.json"
TRACE_PATTERNS = ("participant-*.jsonl", "client-*.jsonl")
XZ_COMPRESS = ["xz", "-9e", "-T0", "--memlimit-compress=50%", "-c"]


def trace_files(run_dir: Path) -> list[Path]:
    return sorted(p for pattern in TRACE_PATTERNS for p in run_dir.glob(pattern) if p.is_file())


def device_trace_copies(run_dir: Path) -> list[Path]:
    external = run_dir / "external"
    if not external.is_dir():
        return []
    return sorted(p for pattern in TRACE_PATTERNS for p in external.rglob(pattern) if p.is_file())


def is_runner_event(line: bytes) -> bool:
    return b'"runner_event_kind":"' in line or b'"runner_event_kind": "' in line


def count_profile_lines(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for line in handle if line.strip() and not is_runner_event(line))


def is_byte_prefix(raw_copy: Path, trace: Path) -> bool:
    if not trace.is_file():
        return False
    with raw_copy.open("rb") as raw, trace.open("rb") as full:
        for chunk in iter(lambda: raw.read(1 << 20), b""):
            if full.read(len(chunk)) != chunk:
                return False
    return True


def count_profile_rows(events_csv: Path) -> int:
    csv.field_size_limit(sys.maxsize)
    with events_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
        kind = header.index("runner_event_kind") if "runner_event_kind" in header else None
        return sum(
            1 for row in reader
            if row and (kind is None or kind >= len(row) or not row[kind])
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compress_verified(events_csv: Path, archive: Path, expected_sha256: str) -> None:
    tmp = archive.with_name(archive.name + ".tmp")
    try:
        with events_csv.open("rb") as source, tmp.open("wb") as target:
            subprocess.run(XZ_COMPRESS, stdin=source, stdout=target, check=True)
        subprocess.run(["xz", "-t", str(tmp)], check=True)
        digest = hashlib.sha256()
        with subprocess.Popen(["xz", "-dc", str(tmp)], stdout=subprocess.PIPE) as proc:
            for chunk in iter(lambda: proc.stdout.read(1 << 20), b""):
                digest.update(chunk)
        if proc.returncode != 0 or digest.hexdigest() != expected_sha256:
            raise RuntimeError(f"{tmp} does not decompress to events.csv (sha256 differs)")
        tmp.replace(archive)
        fsync(archive)
    finally:
        tmp.unlink(missing_ok=True)


def fsync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def file_entry(run_dir: Path, path: Path, lines: int | None = None) -> dict:
    entry = {"path": str(path.relative_to(run_dir)), "bytes": path.stat().st_size}
    if lines is not None:
        entry["lines"] = lines
    return entry


def write_manifest(run_dir: Path, manifest: dict) -> None:
    manifest = {"schema_version": 1, "mode": "events-csv", "written_unix": int(time.time()), **manifest}
    (run_dir / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    fsync(run_dir / MANIFEST)
    fsync(run_dir)


def keep_full(run_dir: Path, reason: str) -> None:
    write_manifest(run_dir, {"status": "kept_full", "reason": reason})
    print(f"WARNING: [retention] {run_dir}: all files kept: {reason}", file=sys.stderr, flush=True)


def retain(run_dir: Path) -> None:
    if (run_dir / MANIFEST).exists():
        print(f"[retention] {run_dir}: has {MANIFEST} already; nothing done", flush=True)
        return

    try:
        outcome = json.loads((run_dir / "benchmark_outcome.json").read_text(encoding="utf-8"))
        outcome_class = outcome.get("outcome_class")
    except (OSError, ValueError) as error:
        outcome_class = f"unreadable ({error})"
    if outcome_class != "success":
        keep_full(run_dir, f"outcome_class is {outcome_class}")
        return

    events_csv = run_dir / "events.csv"
    traces = trace_files(run_dir)
    if not events_csv.is_file() or events_csv.stat().st_size == 0:
        keep_full(run_dir, "events.csv is missing or empty")
        return
    if not traces:
        keep_full(run_dir, "no participant-*/client-*.jsonl traces")
        return

    trace_lines = {path: count_profile_lines(path) for path in traces}
    csv_rows = count_profile_rows(events_csv)
    if sum(trace_lines.values()) != csv_rows:
        keep_full(
            run_dir,
            f"trace profile lines ({sum(trace_lines.values())}) != events.csv profile rows ({csv_rows})",
        )
        return

    csv_sha256 = sha256_file(events_csv)
    archive = run_dir / "events.csv.xz"
    compress_verified(events_csv, archive, csv_sha256)

    deleted = [file_entry(run_dir, path, lines) for path, lines in trace_lines.items()]
    kept_copies = []
    for copy in device_trace_copies(run_dir):
        if is_byte_prefix(copy, run_dir / copy.name):
            deleted.append(file_entry(run_dir, copy))
        else:
            kept_copies.append(str(copy.relative_to(run_dir)))
    deleted.append(file_entry(run_dir, events_csv, csv_rows))
    write_manifest(run_dir, {
        "status": "retained",
        "events_csv": {"profile_rows": csv_rows, "sha256": csv_sha256},
        "events_csv_xz": {"bytes": archive.stat().st_size, "sha256": sha256_file(archive)},
        "deleted": deleted,
        "kept_device_copies_not_a_prefix_of_their_trace": kept_copies,
    })
    for entry in deleted:
        (run_dir / entry["path"]).unlink()
    freed = sum(entry["bytes"] for entry in deleted) - archive.stat().st_size
    print(f"[retention] {run_dir}: kept events.csv.xz, deleted {len(deleted)} files, "
          f"freed {freed / 1e6:.0f} MB", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", required=True, help="OUTPUT_RETENTION value: full or events-csv")
    parser.add_argument("run_dirs", nargs="*", type=Path)
    args = parser.parse_args()
    if args.mode not in MODES:
        print(f"ERROR: OUTPUT_RETENTION must be one of {', '.join(MODES)}, not {args.mode!r}",
              file=sys.stderr)
        return 2
    if args.mode == "full":
        return 0
    try:
        subprocess.run(XZ_COMPRESS, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                       stderr=subprocess.PIPE, check=True)
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"ERROR: OUTPUT_RETENTION=events-csv needs a working `{' '.join(XZ_COMPRESS)}`: {error}",
              file=sys.stderr)
        return 2
    for run_dir in args.run_dirs:
        if not run_dir.is_dir():
            print(f"ERROR: [retention] not a run directory: {run_dir}", file=sys.stderr)
            return 1
        retain(run_dir.resolve())
    return 0


if __name__ == "__main__":
    sys.exit(main())
