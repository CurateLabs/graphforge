"""Swap accounting of the cgroup BenchExec gives one phase (#1914).

`/proc/vmstat` counts every process on the host, so on a shared host another
process's swap-out failed an unrelated phase. BenchExec runs each phase in its own
cgroup (`benchmark_*`, a sibling of its own `benchexec_process_*` cgroup), and a
cgroup counts only the pages charged to its own processes: `memory.stat`'s
`pswpout` is the pages of the phase's tree that went to swap, and
`memory.swap.peak` is the most swap it held.

BenchExec removes the run cgroup when the run ends, and its counters go with it, so
`PhaseSwapSampler` reads them while the run is alive and keeps the largest value
seen. Both are monotonic for the life of the cgroup, so a sample taken any time
after a page-out still shows it.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

SCHEMA = "graphforge-phase-cgroup-swap/1"
EVIDENCE_NAME = "phase-cgroup-swap.json"
CGROUP_ROOT = Path("/sys/fs/cgroup")
# The names BenchExec gives its own process cgroup and each run's cgroup
# (`benchexec.cgroupsv2`).
PROCESS_PREFIX = "benchexec_process_"
RUN_PREFIX = "benchmark_"


@dataclass(frozen=True)
class RunSwap:
    """What one run cgroup has swapped, as of the last sample that found it."""

    pswpout_pages: int
    swap_peak_bytes: int
    swap_max: str | None

    @property
    def swapped(self) -> bool:
        return self.pswpout_pages > 0 or self.swap_peak_bytes > 0


def read_run_swap(cgroup: Path) -> RunSwap | None:
    """The swap counters of one cgroup directory, or None when it cannot be read.

    `pswpout` in `memory.stat` is required: without it the cgroup says nothing about
    swap. `memory.swap.peak` (Linux 6.5+) adds the most swap held; its absence leaves
    that at 0 rather than hiding the `pswpout` reading.
    """
    try:
        stat_text = (cgroup / "memory.stat").read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError):
        return None
    pswpout: int | None = None
    for line in stat_text.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] == "pswpout" and fields[1].isdigit():
            pswpout = int(fields[1])
    if pswpout is None:
        return None
    peak = 0
    try:
        text = (cgroup / "memory.swap.peak").read_text(encoding="ascii").strip()
        peak = int(text) if text.isdigit() else 0
    except (OSError, UnicodeDecodeError):
        pass
    try:
        swap_max: str | None = (cgroup / "memory.swap.max").read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        swap_max = None
    return RunSwap(pswpout, peak, swap_max)


def process_cgroup(
    pid: int, *, proc_root: Path = Path("/proc"), cgroup_root: Path = CGROUP_ROOT
) -> Path | None:
    """The cgroup directory of a process, from its unified (`0::`) hierarchy entry."""
    try:
        text = (proc_root / str(pid) / "cgroup").read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    for line in text.splitlines():
        if line.startswith("0::/"):
            return cgroup_root / line[4:]
    return None


class PhaseSwapSampler:
    """Track the swap counters of the run cgroups next to BenchExec's own process.

    BenchExec moves itself into `<scope>/benchexec_process_*` shortly after it
    starts; until it has, its process sits in the launcher's cgroup, which holds
    unrelated processes and is never read. The run cgroups are the `benchmark_*`
    children of that scope.
    """

    def __init__(self, *, proc_root: Path = Path("/proc"), cgroup_root: Path = CGROUP_ROOT) -> None:
        self._proc_root = proc_root
        self._cgroup_root = cgroup_root
        self._scope: Path | None = None
        self._runs: dict[str, RunSwap] = {}
        self.samples = 0

    def sample(self, pid: int) -> None:
        """Fold the current counters of every live run cgroup into the evidence."""
        if self._scope is None:
            own = process_cgroup(pid, proc_root=self._proc_root, cgroup_root=self._cgroup_root)
            if own is None or not own.name.startswith(PROCESS_PREFIX):
                return
            self._scope = own.parent
        try:
            runs = [path for path in self._scope.iterdir() if path.name.startswith(RUN_PREFIX)]
        except OSError:
            return
        for run in runs:
            reading = read_run_swap(run)
            if reading is None:
                continue
            self.samples += 1
            seen = self._runs.get(run.name)
            if seen is not None:
                reading = RunSwap(
                    max(reading.pswpout_pages, seen.pswpout_pages),
                    max(reading.swap_peak_bytes, seen.swap_peak_bytes),
                    reading.swap_max,
                )
            self._runs[run.name] = reading

    @property
    def observed(self) -> bool:
        return bool(self._runs)

    @property
    def swapped(self) -> bool:
        return any(run.swapped for run in self._runs.values())

    def evidence(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "samples": self.samples,
            "runs": {
                name: {
                    "pswpout_pages": run.pswpout_pages,
                    "swap_peak_bytes": run.swap_peak_bytes,
                    "swap_max": run.swap_max,
                }
                for name, run in sorted(self._runs.items())
            },
        }

    def write(self, path: Path) -> None:
        path.write_text(json.dumps(self.evidence(), sort_keys=True) + "\n", encoding="utf-8")


def load_evidence(path: Path) -> dict[str, Any] | None:
    """The evidence a phase's BenchExec run recorded, or None when it recorded none."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(document, dict) or document.get("schema") != SCHEMA:
        return None
    runs = document.get("runs")
    if not isinstance(runs, dict) or not all(
        isinstance(run, dict)
        and isinstance(run.get("pswpout_pages"), int)
        and isinstance(run.get("swap_peak_bytes"), int)
        for run in runs.values()
    ):
        return None
    return document


def verdict(evidence: dict[str, Any] | None) -> bool | None:
    """True when the phase's own pages were swapped, False when not, None if unobserved."""
    if evidence is None or not evidence["runs"]:
        return None
    return any(
        run["pswpout_pages"] > 0 or run["swap_peak_bytes"] > 0 for run in evidence["runs"].values()
    )


def detail(evidence: dict[str, Any]) -> str:
    """What the phase's cgroup swapped, for a `phase_swapped` rung's failure detail."""
    parts = [
        f"{name}: pswpout {run['pswpout_pages']} pages, swap peak {run['swap_peak_bytes']} bytes"
        for name, run in sorted(evidence["runs"].items())
    ]
    return f"the phase's own cgroup paged out ({'; '.join(parts)}); see {EVIDENCE_NAME}"
