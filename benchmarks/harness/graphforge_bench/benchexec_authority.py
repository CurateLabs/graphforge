"""Fail-closed normalization of BenchExec's Linux process-tree evidence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
import math
from typing import Any

from graphforge_bench.local_admission import REQUIRED_METRICS

SCHEMA = "graphforge-benchexec-run/1"
LOCAL_ADMISSION_SCHEMA = "graphforge-local-admission-evidence/1"


class EvidenceError(ValueError):
    """BenchExec or phase evidence is absent, malformed, or contradictory."""


def _finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


class Outcome(StrEnum):
    PASSED = "passed"
    TIMEOUT = "timeout"
    OOM = "oom"
    EXIT = "exit"
    SIGNAL = "signal"
    HARNESS = "harness"
    CORRECTNESS = "correctness"


@dataclass(frozen=True)
class Limits:
    wall_seconds: float
    cpu_seconds: float
    memory_bytes: int
    cores: tuple[int, ...]

    def validate(self) -> None:
        if (
            not _finite_number(self.wall_seconds)
            or self.wall_seconds <= 0
            or not _finite_number(self.cpu_seconds)
            or self.cpu_seconds <= 0
            or self.memory_bytes <= 0
            or not self.cores
            or any(core < 0 for core in self.cores)
            or len(set(self.cores)) != len(self.cores)
        ):
            raise EvidenceError("invalid BenchExec limits")


def require_local_admission(evidence: Mapping[str, Any]) -> Mapping[str, Any]:
    """Consume #986's native fixture proof before accepting run evidence."""
    if (
        evidence.get("schema") != LOCAL_ADMISSION_SCHEMA
        or evidence.get("result") != "passed"
        or evidence.get("cause") is not None
    ):
        raise EvidenceError("native BenchExec admission did not pass")
    measurements = evidence.get("measurements")
    if not isinstance(measurements, Mapping):
        raise EvidenceError("native BenchExec child-tree proof is missing")
    for key in REQUIRED_METRICS:
        value = measurements.get(key)
        if not _finite_number(value) or value < 0:
            raise EvidenceError(f"native BenchExec admission metric is invalid: {key}")
    if measurements.get("terminationreason") != "walltime":
        raise EvidenceError("native BenchExec termination proof is missing")
    if measurements.get("descendant_stopped") is not True:
        raise EvidenceError("native BenchExec child-tree proof is missing")
    return measurements


def adapt_run_result(raw: Mapping[str, Any], *, correctness: bool) -> dict[str, Any]:
    """Translate BenchExec RunExecutor keys without weakening mandatory evidence."""
    exitcode = raw.get("exitcode")
    value = getattr(exitcode, "value", None)
    signal = getattr(exitcode, "signal", None)
    return {
        "wall_seconds": raw.get("walltime"),
        "cpu_seconds": raw.get("cputime"),
        "peak_rss_bytes": raw.get("memory"),
        "read_bytes": raw.get("blkio-read"),
        "write_bytes": raw.get("blkio-write"),
        "pressure_cpu_seconds": raw.get("pressure-cpu-some"),
        "pressure_io_seconds": raw.get("pressure-io-some"),
        "pressure_memory_seconds": raw.get("pressure-memory-some"),
        "termination_reason": raw.get("terminationreason"),
        "exit_code": value,
        "signal": signal,
        "correctness": correctness,
    }


def _number(values: Mapping[str, Any], key: str) -> float:
    value = values.get(key)
    if not _finite_number(value) or value < 0:
        raise EvidenceError(f"missing or invalid BenchExec field: {key}")
    return float(value)


def _integer(values: Mapping[str, Any], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EvidenceError(f"missing or invalid BenchExec field: {key}")
    return value


def _outcome(result: Mapping[str, Any]) -> tuple[Outcome, int | None, int | None]:
    termination = result.get("termination_reason")
    if termination in {"cputime", "walltime"}:
        return Outcome.TIMEOUT, None, None
    if termination == "memory":
        return Outcome.OOM, None, None
    if termination not in (None, ""):
        return Outcome.HARNESS, None, None
    signal = result.get("signal")
    if signal is not None:
        if not isinstance(signal, int) or signal <= 0:
            raise EvidenceError("invalid BenchExec signal")
        return Outcome.SIGNAL, None, signal
    exit_code = result.get("exit_code")
    if not isinstance(exit_code, int):
        raise EvidenceError("missing BenchExec exit_code")
    if exit_code != 0:
        return Outcome.EXIT, exit_code, None
    if result.get("correctness") is False:
        return Outcome.CORRECTNESS, 0, None
    if result.get("correctness") is not True:
        raise EvidenceError("missing correctness verdict")
    return Outcome.PASSED, 0, None


def normalize_run(
    *,
    benchexec: Mapping[str, Any],
    graphforge: Mapping[str, Any],
    limits: Limits,
) -> dict[str, Any]:
    """Preserve both sources while making BenchExec authoritative for resources."""
    limits.validate()
    outcome, exit_code, signal = _outcome(benchexec)
    authority = {
        "wall_seconds": _number(benchexec, "wall_seconds"),
        "cpu_seconds": _number(benchexec, "cpu_seconds"),
        "peak_rss_bytes": _integer(benchexec, "peak_rss_bytes"),
        "read_bytes": _integer(benchexec, "read_bytes"),
        "write_bytes": _integer(benchexec, "write_bytes"),
        "pressure_cpu_seconds": _number(benchexec, "pressure_cpu_seconds"),
        "pressure_io_seconds": _number(benchexec, "pressure_io_seconds"),
        "pressure_memory_seconds": _number(benchexec, "pressure_memory_seconds"),
    }
    gf_status = graphforge.get("status")
    phases = graphforge.get("phases")
    if gf_status not in ("passed", "failed") or not isinstance(phases, list) or not phases:
        raise EvidenceError("GraphForge run telemetry is malformed")
    duration_ms = 0
    for phase in phases:
        if not isinstance(phase, dict) or not isinstance(phase.get("phase"), str):
            raise EvidenceError("GraphForge phase telemetry is malformed")
        value = phase.get("duration_ms")
        if not isinstance(value, int) or value < 0:
            raise EvidenceError("GraphForge phase duration_ms is malformed")
        duration_ms += value
    disagreements: list[str] = []
    if (gf_status == "passed") != (outcome == Outcome.PASSED):
        disagreements.append("status")
    if abs(duration_ms / 1000 - authority["wall_seconds"]) > max(
        1.0, authority["wall_seconds"] * 0.1
    ):
        disagreements.append("wall_time")
    return {
        "schema": SCHEMA,
        "outcome": outcome,
        "exit_code": exit_code,
        "signal": signal,
        "authority": authority,
        "limits": {
            "wall_seconds": limits.wall_seconds,
            "cpu_seconds": limits.cpu_seconds,
            "memory_bytes": limits.memory_bytes,
            "cores": list(limits.cores),
        },
        "graphforge": dict(graphforge),
        "disagreements": disagreements,
    }
