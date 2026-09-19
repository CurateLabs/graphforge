"""Fail-closed GDC in-process measurement boundary checks (#959).

Live GDC runners emit engineering evidence only. In-process wall times and
resource counters are diagnostic; they must never be relabeled as BenchExec
process-tree authority or audited certification.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class GdcMeasurementBoundaryError(ValueError):
    """GDC evidence violates the measurement boundary contract."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def _execution_authority_blocks(
    evidence: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    blocks: list[Mapping[str, Any]] = []
    top_level = evidence.get("execution_authority")
    if isinstance(top_level, Mapping):
        blocks.append(top_level)
    identities = evidence.get("identities")
    if isinstance(identities, Mapping):
        nested = identities.get("execution_authority")
        if isinstance(nested, Mapping):
            blocks.append(nested)
    return tuple(blocks)


def assert_live_diagnostic_boundary(
    evidence: Mapping[str, Any],
    *,
    label: str = "evidence",
) -> None:
    """Reject live GDC evidence that claims certification or gate authority."""
    if evidence.get("certification") is not False:
        raise GdcMeasurementBoundaryError(
            "certification_claim",
            f"{label} must set certification=false for engineering GDC lanes",
        )
    if "benchexec" in evidence:
        raise GdcMeasurementBoundaryError(
            "misattributed_authority",
            f"{label} must not embed BenchExec authority on in-process GDC runners",
        )
    resources = evidence.get("resources")
    if isinstance(resources, Mapping) and "correctness_authority" in resources:
        if resources.get("correctness_authority") is not False:
            raise GdcMeasurementBoundaryError(
                "resource_gate_authority",
                f"{label} resources.correctness_authority must be false",
            )
    for block in _execution_authority_blocks(evidence):
        if block.get("caller_supplied_result"):
            raise GdcMeasurementBoundaryError(
                "caller_supplied_result",
                f"{label} must not accept caller-supplied results",
            )
