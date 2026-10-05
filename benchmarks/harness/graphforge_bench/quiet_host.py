"""Require a sustained quiet window on the shared bench host before a timed launch.

A rung's wall time and I/O rates are only comparable when nothing else competes
for the host. Concurrent builds on OVHC-AGENCY have inflated phase timings by
11-23% while every counter still looked correct, so the launch waits for one
window in which no compiler, benchmark or GraphForge process runs and aggregate
CPU stays low. Process names come from `/proc/<pid>/comm`, never from command
lines, so the check cannot match its own invocation.
"""

from __future__ import annotations

from collections.abc import Callable
import math
import os
from pathlib import Path
import time
from typing import Any

WINDOW_SECONDS = 60
SAMPLE_SECONDS = 5
MAXIMUM_MEAN_BUSY_CORES = 0.5
MAXIMUM_PEAK_BUSY_CORES = 2.0
# `comm` is truncated to 15 bytes: the benchmark certify and generator
# executables both appear as `graphforge-benc`.
BUSY_PROCESS_NAMES = frozenset(
    {
        "bazel",
        "bazelisk",
        "benchexec",
        "cargo",
        "cargo-nextest",
        "gf",
        "gf.real",
        "graphforge-benc",
        "maturin",
        "runexec",
        "rustc",
    }
)


class HostNotQuietError(RuntimeError):
    """No quiet window opened before the wait expired."""

    def __init__(self, busy_processes: list[str], busy_cores: float):
        super().__init__("host_not_quiet")
        self.busy_processes = busy_processes
        self.busy_cores = busy_cores


def busy_processes(proc: Path = Path("/proc")) -> list[str]:
    """Names of running processes that compete with a timed measurement."""
    own = os.getpid()
    names = set()
    for entry in proc.iterdir():
        if not entry.name.isdigit() or int(entry.name) == own:
            continue
        try:
            name = (entry / "comm").read_text(encoding="utf-8").strip()
        except OSError:
            continue  # The process exited between listing and reading.
        if name in BUSY_PROCESS_NAMES:
            names.add(name)
    return sorted(names)


def cpu_jiffies(proc: Path = Path("/proc")) -> tuple[int, int]:
    """Aggregate (total, idle) CPU jiffies; idle includes I/O wait."""
    fields = (proc / "stat").read_text(encoding="utf-8").splitlines()[0].split()
    if fields[0] != "cpu" or len(fields) < 9:
        raise ValueError("/proc/stat has no aggregate cpu line")
    values = [int(value) for value in fields[1:9]]
    return sum(values), values[3] + values[4]


def wait_for_quiet_host(
    wait_seconds: int,
    *,
    proc: Path = Path("/proc"),
    cores: int | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Return the first fully quiet window, waiting at most `wait_seconds` to start one.

    A window restarts at the first busy sample. When the wait expires before a
    window starts quiet and stays quiet, raise `HostNotQuietError` with the last
    observation rather than launching on a contended host.
    """
    cpu_count = cores or os.cpu_count() or 1
    started = clock()
    samples_per_window = WINDOW_SECONDS // SAMPLE_SECONDS
    while True:
        names = busy_processes(proc)
        busy: list[float] = []
        previous = cpu_jiffies(proc)
        while not names and len(busy) < samples_per_window:
            sleep(SAMPLE_SECONDS)
            current = cpu_jiffies(proc)
            total = current[0] - previous[0]
            idle = current[1] - previous[1]
            busy.append(cpu_count * (total - idle) / total if total > 0 else 0.0)
            previous = current
            names = busy_processes(proc)
            if busy[-1] > MAXIMUM_PEAK_BUSY_CORES:
                break
        mean = math.fsum(busy) / len(busy) if busy else 0.0
        if (
            not names
            and len(busy) == samples_per_window
            and mean <= MAXIMUM_MEAN_BUSY_CORES
            and max(busy) <= MAXIMUM_PEAK_BUSY_CORES
        ):
            return {
                "window_seconds": WINDOW_SECONDS,
                "waited_seconds": max(0, math.floor(clock() - started) - WINDOW_SECONDS),
                "mean_busy_cores": round(mean, 3),
                "peak_busy_cores": round(max(busy), 3),
            }
        if clock() - started >= wait_seconds:
            raise HostNotQuietError(names, round(max(busy, default=0.0), 3))
        if not busy:
            sleep(SAMPLE_SECONDS)
