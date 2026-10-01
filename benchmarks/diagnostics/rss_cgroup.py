"""Private, non-admission cgroup memory-attribution diagnostic (#1473).

Samples one delegated cgroup-v2 subtree (a systemd user slice or scope) while
a host ladder rung executes inside it, separating anonymous working set from
page cache. Every sample records the subtree's hierarchical memory.stat, the
kernel-authoritative `memory.peak`, summed per-process VmHWM across all
descendant processes, and the same facts per child cgroup, so each BenchExec
tool scope's peak is captured before systemd destroys the inactive unit.

Settles what the ladder's rung-level peak RSS figure actually measures. Raw
samples remain local; the summary records the scope unit name, digests, and
byte counters only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time

STAT_KEYS = ("anon", "file", "sock", "slab", "kernel", "percpu", "pagetables")
STATUS_KEYS = ("VmHWM", "VmRSS", "RssAnon", "RssFile")
EMPTY_POLLS = 5


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _read_scope_procs(procs: Path, found: list[tuple[Path, int]]) -> None:
    try:
        for line in procs.read_text().split():
            found.append((procs.parent, int(line)))
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return


def scope_processes(root: Path) -> list[tuple[Path, int]]:
    found: list[tuple[Path, int]] = []
    for procs in root.rglob("cgroup.procs"):
        _read_scope_procs(procs, found)
    return found


def _read_proc_totals(pid: int, totals: dict[str, int]) -> None:
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return
    for line in status.splitlines():
        key, _, value = line.partition(":")
        if key in totals:
            totals[key] += int(value.split()[0]) * 1024


def sample_processes(processes) -> dict[str, int]:
    totals = dict.fromkeys(STATUS_KEYS, 0)
    for _, pid in processes:
        _read_proc_totals(pid, totals)
    return totals


def _read_memory_file(root: Path, values: dict[str, int], name: str) -> None:
    try:
        values[name] = int((root / name).read_text().strip())
    except (FileNotFoundError, OSError):
        values[name] = None


def read_memory_files(root: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    for name in ("memory.current", "memory.peak", "memory.swap.peak"):
        _read_memory_file(root, values, name)
    return values


def read_memory_stat(root: Path) -> dict[str, int]:
    values = {}
    try:
        text = (root / "memory.stat").read_text()
    except (FileNotFoundError, OSError):
        return values
    for line in text.splitlines():
        key, _, value = line.partition(" ")
        if key in STAT_KEYS:
            values[key] = int(value)
    return values


def _read_child_memory(path: Path, facts: dict[str, int], name: str) -> None:
    try:
        facts[name] = int((path / name).read_text().strip())
    except (OSError, ValueError):
        return


def child_cgroups(root: Path) -> dict[str, dict[str, int]]:
    """Per-child-cgroup memory facts; each tool scope's peak dies with its unit."""
    children = {}
    try:
        entries = list(root.iterdir())
    except OSError:
        return children
    for path in entries:
        if not path.is_dir() or not (path / "memory.stat").is_file():
            continue
        facts: dict[str, int] = {}
        for name in ("memory.current", "memory.peak"):
            _read_child_memory(path, facts, name)
        stat = read_memory_stat(path)
        for key in ("anon", "file"):
            if key in stat:
                facts[key] = stat[key]
        if facts:
            children[path.name] = facts
    return children


def read_events(root: Path) -> dict[str, int]:
    values = {}
    try:
        for line in (root / "memory.events").read_text().splitlines():
            key, _, value = line.partition(" ")
            values[key] = int(value)
    except (FileNotFoundError, OSError):
        pass
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cgroup", type=Path, required=True)
    parser.add_argument("--interval-ms", type=int, default=200)
    parser.add_argument("--wait-timeout-seconds", type=int, default=900)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.cgroup
    if not root.is_dir():
        raise SystemExit(f"cgroup not found: {root}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    count = 0
    maxima: dict[str, int] = {}
    empty_run = 0
    saw_process = False
    with args.output.open("w") as samples:
        while True:
            processes = scope_processes(root)
            totals = sample_processes(processes)
            moment = {
                "elapsed_seconds": time.monotonic() - started,
                **read_memory_files(root),
                **read_memory_stat(root),
                **{f"sum_{key.lower()}": value for key, value in totals.items()},
                "processes": len(processes),
                "children": child_cgroups(root),
            }
            samples.write(json.dumps(moment) + "\n")
            samples.flush()
            for key, value in moment.items():
                if isinstance(value, int):
                    maxima[key] = max(maxima.get(key, 0), value)
            count += 1
            if moment["processes"] > 0:
                saw_process = True
                empty_run = 0
            elif saw_process:
                empty_run += 1
                if empty_run >= EMPTY_POLLS:
                    break
            if time.monotonic() - started > args.wait_timeout_seconds:
                break
            time.sleep(args.interval_ms / 1000)
    child_peaks = {}
    for line in args.output.read_text().splitlines():
        moment = json.loads(line)
        for name, facts in moment.get("children", {}).items():
            previous = child_peaks.setdefault(name, {})
            for key, value in facts.items():
                previous[key] = max(previous.get(key, 0), value)
    summary = {
        "claim": "diagnostic_only_not_admission",
        "purpose": "#1473 rung peak RSS attribution",
        "scope_unit": root.name,
        "interval_ms": args.interval_ms,
        "samples": count,
        "saw_process": saw_process,
        "peak": maxima,
        "child_peaks": child_peaks,
        "memory_peak_bytes": maxima.get("memory.peak"),
        "memory_swap_peak_bytes": maxima.get("memory.swap.peak"),
        "events": read_events(root),
    }
    (args.output.parent / "cgroup-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
