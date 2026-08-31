"""Scale orchestration parity matrix for legacy vs benchmark harness evidence.

Compares normalized lifecycle evidence without rerunning provider-scale ladders.
Issue #959 uses tiny/local shadow fixtures first; completed #900 ladder bundles
can be ingested read-only in a follow-up.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.progressive_qualification import PHASES

MATRIX_SCHEMA = "graphforge-scale-orchestration-parity-matrix/1"
ACCEPTED_SCHEMA = "graphforge-scale-orchestration-accepted-differences/1"
CERT_SCHEMA = "graphforge-public-certification/1"


class ParityError(ValueError):
    """Parity input is missing, malformed, or contradictory."""


class Outcome(StrEnum):
    MATCH = "match"
    ACCEPTED_DIFFERENCE = "accepted_difference"
    UNEXPLAINED_GAP = "unexplained_gap"


@dataclass(frozen=True)
class NormalizedPhase:
    phase: str
    status: str
    duration_ms: int | None
    peak_rss_bytes: int | None


@dataclass(frozen=True)
class NormalizedEvidence:
    profile_id: str
    status: str
    phases: tuple[NormalizedPhase, ...]
    source: str


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_schema(name: str) -> Draft202012Validator:
    document = json.loads((workspace_root() / "schemas" / name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(document)
    return Draft202012Validator(document)


def _validate(validator: Draft202012Validator, document: Mapping[str, Any], label: str) -> None:
    error = next(validator.iter_errors(document), None)
    if error is not None:
        raise ParityError(f"{label}: {error.message}")


def load_accepted_differences(root: Path | None = None) -> Mapping[str, Any]:
    path = (root or workspace_root()) / "fixtures" / "parity" / "accepted-differences.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema") != ACCEPTED_SCHEMA:
        raise ParityError(f"unsupported accepted-differences schema: {document.get('schema')}")
    return document


def normalize_legacy_evidence(document: Mapping[str, Any]) -> NormalizedEvidence:
    if "profile" not in document or "phases" not in document:
        raise ParityError("legacy evidence requires profile and phases")
    phases: list[NormalizedPhase] = []
    overall = "passed"
    for entry in document["phases"]:
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            raise ParityError("legacy phase name is required")
        ok = bool(entry.get("ok"))
        if not ok:
            overall = "failed"
        duration_ms = None
        duration_secs = entry.get("duration_secs")
        if isinstance(duration_secs, (int, float)) and duration_secs >= 0:
            duration_ms = round(float(duration_secs) * 1_000)
        peak_rss_bytes = None
        max_rss_kib = entry.get("max_rss_kib")
        if isinstance(max_rss_kib, int) and max_rss_kib >= 0:
            peak_rss_bytes = max_rss_kib * 1_024
        phases.append(
            NormalizedPhase(
                phase=name,
                status="passed" if ok else "failed",
                duration_ms=duration_ms,
                peak_rss_bytes=peak_rss_bytes,
            )
        )
        if not ok:
            break
    return NormalizedEvidence(
        profile_id=str(document["profile"]),
        status=overall,
        phases=tuple(phases),
        source="legacy_profile_phases",
    )


def normalize_new_evidence(document: Mapping[str, Any]) -> NormalizedEvidence:
    if document.get("schema") != CERT_SCHEMA:
        raise ParityError(f"unsupported certification schema: {document.get('schema')}")
    phases: list[NormalizedPhase] = []
    for entry in document.get("phases", ()):
        phase = entry.get("phase")
        status = entry.get("status")
        if not isinstance(phase, str) or status not in {"passed", "failed"}:
            raise ParityError("new certification phase requires phase and status")
        duration_ms = entry.get("duration_ms")
        if not isinstance(duration_ms, int) or duration_ms < 0:
            raise ParityError(f"invalid duration_ms for phase {phase}")
        peak_rss_bytes = entry.get("peak_rss_bytes")
        if peak_rss_bytes is not None and (
            not isinstance(peak_rss_bytes, int) or peak_rss_bytes < 0
        ):
            raise ParityError(f"invalid peak_rss_bytes for phase {phase}")
        phases.append(
            NormalizedPhase(
                phase=phase,
                status=status,
                duration_ms=duration_ms,
                peak_rss_bytes=peak_rss_bytes,
            )
        )
        if status == "failed":
            break
    status = document.get("status")
    if status not in {"passed", "failed"}:
        raise ParityError("new certification status must be passed or failed")
    profile_id = document.get("profile_id")
    if not isinstance(profile_id, str) or not profile_id:
        raise ParityError("new certification profile_id is required")
    return NormalizedEvidence(
        profile_id=profile_id,
        status=status,
        phases=tuple(phases),
        source=CERT_SCHEMA,
    )


def _phase_mapping(accepted: Mapping[str, Any]) -> dict[str, str | None]:
    mapping: dict[str, str | None] = {}
    for row in accepted.get("phase_mapping", ()):
        legacy = row.get("legacy")
        new = row.get("new")
        if isinstance(legacy, str):
            mapping[legacy] = new if isinstance(new, str) else None
    return mapping


def _accepted_ids(accepted: Mapping[str, Any]) -> dict[str, set[str]]:
    by_dimension: dict[str, set[str]] = {}
    for row in accepted.get("differences", ()):
        diff_id = row.get("id")
        if not isinstance(diff_id, str):
            continue
        for dimension in row.get("dimensions", ()):
            if isinstance(dimension, str):
                by_dimension.setdefault(dimension, set()).add(diff_id)
    return by_dimension


def _worst(outcomes: Sequence[Outcome]) -> Outcome:
    if any(outcome == Outcome.UNEXPLAINED_GAP for outcome in outcomes):
        return Outcome.UNEXPLAINED_GAP
    if any(outcome == Outcome.ACCEPTED_DIFFERENCE for outcome in outcomes):
        return Outcome.ACCEPTED_DIFFERENCE
    return Outcome.MATCH


def compare_evidence(
    legacy: NormalizedEvidence,
    new: NormalizedEvidence,
    *,
    accepted: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    registry = accepted or load_accepted_differences()
    mapping = _phase_mapping(registry)
    accepted_by_dimension = _accepted_ids(registry)

    dimensions: list[dict[str, Any]] = []
    phase_rows: list[dict[str, Any]] = []

    if legacy.status == new.status:
        dimensions.append(
            {
                "name": "overall_status",
                "outcome": Outcome.MATCH.value,
                "accepted_difference_id": None,
                "detail": f"both {legacy.status}",
            }
        )
    else:
        dimensions.append(
            {
                "name": "overall_status",
                "outcome": Outcome.UNEXPLAINED_GAP.value,
                "accepted_difference_id": None,
                "detail": f"legacy={legacy.status} new={new.status}",
            }
        )

    legacy_phases = {phase.phase: phase for phase in legacy.phases}
    new_phases = {phase.phase: phase for phase in new.phases}
    expected_new = set(PHASES)

    for legacy_name, legacy_phase in legacy_phases.items():
        if legacy_name in mapping:
            new_name = mapping[legacy_name]
        elif legacy_name in expected_new:
            new_name = legacy_name
        else:
            new_name = None

        if new_name is None:
            diff_ids = accepted_by_dimension.get("phase_coverage", set())
            outcome = Outcome.ACCEPTED_DIFFERENCE if diff_ids else Outcome.UNEXPLAINED_GAP
            phase_rows.append(
                {
                    "legacy_phase": legacy_name,
                    "new_phase": None,
                    "outcome": outcome.value,
                    "accepted_difference_id": next(iter(diff_ids), None),
                    "detail": "legacy-only phase",
                }
            )
            continue
        new_phase = new_phases.get(new_name)
        if new_phase is None:
            phase_rows.append(
                {
                    "legacy_phase": legacy_name,
                    "new_phase": new_name,
                    "outcome": Outcome.UNEXPLAINED_GAP.value,
                    "accepted_difference_id": None,
                    "detail": "mapped new phase missing",
                }
            )
            continue
        if legacy_phase.status != new_phase.status:
            phase_rows.append(
                {
                    "legacy_phase": legacy_name,
                    "new_phase": new_name,
                    "outcome": Outcome.UNEXPLAINED_GAP.value,
                    "accepted_difference_id": None,
                    "detail": f"status legacy={legacy_phase.status} new={new_phase.status}",
                }
            )
            continue
        phase_rows.append(
            {
                "legacy_phase": legacy_name,
                "new_phase": new_name,
                "outcome": Outcome.MATCH.value,
                "accepted_difference_id": None,
                "detail": "status aligned",
            }
        )

    extra_new = sorted(set(new_phases) - expected_new)
    if extra_new:
        dimensions.append(
            {
                "name": "phase_coverage",
                "outcome": Outcome.UNEXPLAINED_GAP.value,
                "accepted_difference_id": None,
                "detail": f"unexpected new phases: {', '.join(extra_new)}",
            }
        )
    else:
        dimensions.append(
            {
                "name": "phase_coverage",
                "outcome": Outcome.MATCH.value,
                "accepted_difference_id": None,
                "detail": "new evidence uses the ten-phase public contract",
            }
        )

    for dimension_name, diff_ids in accepted_by_dimension.items():
        if dimension_name == "phase_coverage" and any(
            row["outcome"] == Outcome.ACCEPTED_DIFFERENCE.value for row in phase_rows
        ):
            dimensions.append(
                {
                    "name": dimension_name,
                    "outcome": Outcome.ACCEPTED_DIFFERENCE.value,
                    "accepted_difference_id": next(iter(diff_ids), None),
                    "detail": "documented phase-model delta",
                }
            )
        elif dimension_name not in {row["name"] for row in dimensions}:
            dimensions.append(
                {
                    "name": dimension_name,
                    "outcome": Outcome.ACCEPTED_DIFFERENCE.value,
                    "accepted_difference_id": next(iter(diff_ids), None),
                    "detail": "declared accepted difference; not asserted on tiny shadow fixtures",
                }
            )

    overall = _worst([Outcome(row["outcome"]) for row in dimensions + phase_rows])
    matrix = {
        "schema": MATRIX_SCHEMA,
        "legacy_source": legacy.source,
        "new_source": new.source,
        "overall": overall.value,
        "dimensions": dimensions,
        "phase_rows": phase_rows,
    }
    _validate(_load_schema("scale-orchestration-parity-matrix.json"), matrix, "parity matrix")
    return matrix


def assert_no_unexplained_gaps(matrix: Mapping[str, Any]) -> None:
    if matrix.get("overall") == Outcome.UNEXPLAINED_GAP.value:
        gaps = [
            row
            for row in matrix.get("dimensions", ()) + matrix.get("phase_rows", ())
            if row.get("outcome") == Outcome.UNEXPLAINED_GAP.value
        ]
        raise ParityError(f"unexplained parity gaps: {gaps!r}")


def compare_fixture_pair(
    legacy_path: Path,
    new_path: Path,
    *,
    accepted: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    legacy_doc = json.loads(legacy_path.read_text(encoding="utf-8"))
    new_doc = json.loads(new_path.read_text(encoding="utf-8"))
    return compare_evidence(
        normalize_legacy_evidence(legacy_doc),
        normalize_new_evidence(new_doc),
        accepted=accepted,
    )
