"""Typed native-Linux admission for ReFrame and BenchExec.

This module deliberately checks the host before importing BenchExec.  BenchExec
is Linux-only, so importing it on macOS would otherwise fail with an untyped
dynamic-loader error instead of a useful qualification result.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import math
from pathlib import Path
import platform
import subprocess
import sys

SCHEMA = "graphforge-local-admission-evidence/1"
REQUIRED_METRICS = (
    "walltime",
    "cputime",
    "memory",
    "blkio-read",
    "blkio-write",
    "pressure-cpu-some",
    "pressure-io-some",
    "pressure-memory-some",
)


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str]], CommandResult]


def _run(command: Sequence[str]) -> CommandResult:
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def _facts(
    system: str,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    proc_root: Path = Path("/proc"),
) -> dict[str, object]:
    linux = system == "Linux"
    return {
        "operating_system": system.lower(),
        "cgroups_version": 2 if linux and (cgroup_root / "cgroup.controllers").is_file() else None,
        "mount_namespace": linux and (proc_root / "self/ns/mnt").exists(),
        "pid_namespace": linux and (proc_root / "self/ns/pid").exists(),
        "user_namespace": linux and (proc_root / "self/ns/user").exists(),
    }


def _evidence(
    result: str,
    cause: str | None,
    facts: Mapping[str, object],
    measurements: Mapping[str, object] | None = None,
) -> dict[str, object]:
    document: dict[str, object] = {
        "schema": SCHEMA,
        "result": result,
        "cause": cause,
        "facts": dict(facts),
    }
    if measurements is not None:
        document["measurements"] = dict(measurements)
    return document


def _metrics_are_finite_numbers(measurements: Mapping[str, object]) -> bool:
    for name in REQUIRED_METRICS:
        value = measurements[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        if not math.isfinite(value) or value < 0:
            return False
    return True


def qualify_local_host(
    *,
    system: str | None = None,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    proc_root: Path = Path("/proc"),
    runner: CommandRunner = _run,
) -> dict[str, object]:
    """Run the complete no-provider admission and return sanitized evidence."""

    detected_system = system or platform.system()
    facts = _facts(detected_system, cgroup_root, proc_root)
    if detected_system != "Linux":
        return _evidence("disqualified", "unsupported_operating_system", facts)
    if facts["cgroups_version"] != 2:
        return _evidence("disqualified", "cgroups_v2_unavailable", facts)
    if not all(facts[name] for name in ("mount_namespace", "pid_namespace", "user_namespace")):
        return _evidence("disqualified", "namespace_controls_unavailable", facts)

    check = runner([sys.executable, "-m", "benchexec.check_cgroups", "--wait", "0", "--no-thread"])
    if check.returncode != 0:
        return _evidence("disqualified", "benchexec_cgroups_unavailable", facts)

    fixture = runner([sys.executable, "-m", "graphforge_bench.local_admission_fixture"])
    if fixture.returncode != 0:
        return _evidence("failed", "benchexec_fixture_failed", facts)
    try:
        measurements = json.loads(fixture.stdout)
    except json.JSONDecodeError:
        return _evidence("failed", "malformed_benchexec_evidence", facts)

    if not isinstance(measurements, dict):
        return _evidence("failed", "malformed_benchexec_evidence", facts)

    missing = [name for name in REQUIRED_METRICS if name not in measurements]
    if missing:
        return _evidence("failed", "mandatory_metric_missing", facts)
    if not _metrics_are_finite_numbers(measurements):
        return _evidence("failed", "malformed_benchexec_evidence", facts)
    if measurements.get("terminationreason") != "walltime":
        return _evidence("failed", "process_tree_limit_not_enforced", facts, measurements)
    if measurements.get("descendant_stopped") is not True:
        return _evidence("failed", "descendant_survived_termination", facts, measurements)
    return _evidence("passed", None, facts, measurements)


def main() -> None:
    document = qualify_local_host()
    print(json.dumps(document, sort_keys=True))
    if document["result"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
