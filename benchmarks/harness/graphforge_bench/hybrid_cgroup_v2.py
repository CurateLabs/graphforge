"""Hybrid cgroup accommodations for Fly Machines and similar hosts.

Fly exposes cgroup v2 at ``/sys/fs/cgroup/unified`` while legacy v1 hierarchies
remain mounted. BenchExec treats that as hybrid mode, executes with v1, and
does not emit PSI pressure metrics even though the unified hierarchy exposes
them. These helpers detect that layout and derive pressure totals from the
unified PSI files around a BenchExec invocation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TypeVar

T = TypeVar("T")

PRESSURE_KEYS = (
    "pressure-cpu-some",
    "pressure-io-some",
    "pressure-memory-some",
)
_PSI_SUBSYSTEMS = {
    "pressure-cpu-some": "cpu",
    "pressure-io-some": "io",
    "pressure-memory-some": "memory",
}


def cgroup_v1_mountpoints(*, mounts_path: Path = Path("/proc/mounts")) -> tuple[Path, ...]:
    """Return mountpoints for legacy cgroup v1 hierarchies."""

    mountpoints: list[Path] = []
    for line in mounts_path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[2] == "cgroup":
            mountpoints.append(Path(parts[1]))
    return tuple(mountpoints)


def has_cgroup_v2_mount(*, mounts_path: Path = Path("/proc/mounts")) -> bool:
    for line in mounts_path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[2] == "cgroup2":
            return True
    return False


def is_hybrid_cgroup_layout(
    *,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    mounts_path: Path = Path("/proc/mounts"),
) -> bool:
    """Return whether the host exposes both v1 mounts and a unified v2 root."""

    unified = cgroup_root / "unified"
    return (
        has_cgroup_v2_mount(mounts_path=mounts_path)
        and bool(cgroup_v1_mountpoints(mounts_path=mounts_path))
        and (unified / "cgroup.controllers").is_file()
    )


def benchexec_cgroup_version(*, mounts_path: Path = Path("/proc/mounts")) -> int | None:
    """Mirror BenchExec's cgroup version selection from ``/proc/mounts``."""

    version: int | None = None
    for line in mounts_path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        if parts[2] == "cgroup":
            return 1
        if parts[2] == "cgroup2" and version != 1:
            version = 2
    return version


def unified_v2_root(*, cgroup_root: Path = Path("/sys/fs/cgroup")) -> Path:
    return cgroup_root / "unified"


def read_psi_total_seconds(pressure_file: Path) -> float:
    """Return the ``some`` line ``total`` stall seconds from one PSI file."""

    try:
        contents = pressure_file.read_text(encoding="utf-8")
    except OSError:
        return 0.0
    for line in contents.splitlines():
        if not line.startswith("some "):
            continue
        for token in line.split():
            if token.startswith("total="):
                return int(token.split("=", 1)[1]) / 1_000_000_000
    return 0.0


def read_unified_pressure_totals(*, unified_root: Path) -> dict[str, float]:
    return {
        key: read_psi_total_seconds(unified_root / f"{subsystem}.pressure")
        for key, subsystem in _PSI_SUBSYSTEMS.items()
    }


def hybrid_pressure_deltas(
    before: Mapping[str, float], after: Mapping[str, float]
) -> dict[str, float]:
    return {
        key: max(0.0, after.get(key, 0.0) - before.get(key, 0.0))
        for key in PRESSURE_KEYS
    }


@contextmanager
def measure_hybrid_pressure(
    *,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> Iterator[Callable[[], dict[str, float]]]:
    """Capture unified PSI totals around a BenchExec invocation on hybrid hosts."""

    unified_root = unified_v2_root(cgroup_root=cgroup_root)
    before = read_unified_pressure_totals(unified_root=unified_root)
    deltas: dict[str, float] = {key: 0.0 for key in PRESSURE_KEYS}

    def _result() -> dict[str, float]:
        return dict(deltas)

    try:
        yield _result
    finally:
        after = read_unified_pressure_totals(unified_root=unified_root)
        deltas.update(hybrid_pressure_deltas(before, after))


def supplement_missing_pressure(
    measurements: dict[str, object],
    *,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> dict[str, object]:
    """Fill hybrid PSI metrics when BenchExec v1 omitted optional pressure keys."""

    if not is_hybrid_cgroup_layout(cgroup_root=cgroup_root):
        return measurements
    if benchexec_cgroup_version() != 1:
        return measurements
    missing = [key for key in PRESSURE_KEYS if key not in measurements]
    if not missing:
        return measurements
    totals = read_unified_pressure_totals(unified_root=unified_v2_root(cgroup_root=cgroup_root))
    for key in missing:
        measurements[key] = totals.get(key, 0.0)
    return measurements
