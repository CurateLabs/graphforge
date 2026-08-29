"""Fail-closed normalization of BenchExec's Linux process-tree evidence."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping


SCHEMA = "graphforge-benchexec-phase/1"


class EvidenceError(ValueError):
    """BenchExec or phase evidence is absent, malformed, or contradictory."""


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
            self.wall_seconds <= 0
            or self.cpu_seconds <= 0
            or self.memory_bytes <= 0
            or not self.cores
            or any(core < 0 for core in self.cores)
            or len(set(self.cores)) != len(self.cores)
        ):
            raise EvidenceError("invalid BenchExec limits")


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
        "termination_reason": raw.get("terminationreason"),
        "exit_code": value,
        "signal": signal,
        "correctness": correctness,
    }


def _number(values: Mapping[str, Any], key: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise EvidenceError(f"missing or invalid BenchExec field: {key}")
    return float(value)


def _integer(values: Mapping[str, Any], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EvidenceError(f"missing or invalid BenchExec field: {key}")
    return value


def _outcome(result: Mapping[str, Any]) -> tuple[Outcome, int | None, int | None]:
    termination = result.get("termination_reason")
    if termination == "cputime" or termination == "walltime":
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


def normalize_phase(
    *,
    phase: str,
    benchexec: Mapping[str, Any],
    graphforge: Mapping[str, Any],
    limits: Limits,
) -> dict[str, Any]:
    """Preserve both sources while making BenchExec authoritative for resources."""
    limits.validate()
    if not phase or any(character not in "abcdefghijklmnopqrstuvwxyz_" for character in phase):
        raise EvidenceError("invalid phase")
    outcome, exit_code, signal = _outcome(benchexec)
    authority = {
        "wall_seconds": _number(benchexec, "wall_seconds"),
        "cpu_seconds": _number(benchexec, "cpu_seconds"),
        "peak_rss_bytes": _integer(benchexec, "peak_rss_bytes"),
        "read_bytes": _integer(benchexec, "read_bytes"),
        "write_bytes": _integer(benchexec, "write_bytes"),
    }
    gf_phase = graphforge.get("phase")
    gf_status = graphforge.get("status")
    if gf_phase != phase or gf_status not in ("passed", "failed"):
        raise EvidenceError("GraphForge phase telemetry is malformed")
    disagreements: list[str] = []
    if (gf_status == "passed") != (outcome == Outcome.PASSED):
        disagreements.append("status")
    gf_duration_ms = graphforge.get("duration_ms")
    if not isinstance(gf_duration_ms, int) or gf_duration_ms < 0:
        raise EvidenceError("GraphForge duration_ms is malformed")
    if abs(gf_duration_ms / 1000 - authority["wall_seconds"]) > max(
        1.0, authority["wall_seconds"] * 0.1
    ):
        disagreements.append("wall_time")
    return {
        "schema": SCHEMA,
        "phase": phase,
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
