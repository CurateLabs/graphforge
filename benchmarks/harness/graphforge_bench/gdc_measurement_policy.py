"""Fail-closed GDC in-process measurement boundary checks (#959).

Live GDC runners emit engineering evidence only. In-process wall times and
resource counters are diagnostic; they must never be relabeled as BenchExec
process-tree authority or audited certification.

Per-operation query latency has exactly one authority (#1877): the driver
clock of ``graphforge-benchmark-gdc-scorecard query``, declared in
``docs/development/benchmarking.md``. ``assert_query_latency_authority``
refuses latency from any other source.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import math
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.gdc_contracts import workspace_root

QUERY_EVIDENCE_SCHEMA = "graphforge-gdc-query-evidence/1"
QUERY_LATENCY_CLOCK = "graphforge-gdc-query-clock/1"
QUERY_DRIVER = "graphforge-benchmark-gdc-scorecard query"


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


def nearest_rank(values: Sequence[int], percent: int) -> int:
    """Nearest-rank percentile: ``sorted(values)[ceil(percent * n / 100) - 1]``."""
    if not values or not 1 <= percent <= 100:
        raise ValueError("nearest_rank needs samples and a percent in 1..100")
    ordered = sorted(values)
    return ordered[math.ceil(percent * len(ordered) / 100) - 1]


def _query_evidence_validator() -> Draft202012Validator:
    path = workspace_root() / "schemas" / "gdc-query-evidence.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(document)
    return Draft202012Validator(document)


def _check_variant_latency(variant: Mapping[str, Any], label: str) -> list[tuple[Any, ...]]:
    """Check one variant; return its failed samples as failure-record keys."""
    query_id = variant.get("query_id")
    name = f"{label} variant {query_id!r}"
    warmup = variant.get("warmup")
    if (
        not isinstance(warmup, Mapping)
        or warmup.get("excluded") is not True
        or set(warmup) != {"binding_id", "excluded", "completed"}
    ):
        raise GdcMeasurementBoundaryError(
            "warmup_latency_included",
            f"{name}: the warm-up pass is excluded and carries no latency",
        )
    samples = variant.get("samples")
    if not isinstance(samples, list) or not samples:
        raise GdcMeasurementBoundaryError(
            "latency_not_from_samples", f"{name}: a variant records its samples"
        )
    latencies: list[int] = []
    failed: list[tuple[Any, ...]] = []
    for sample in samples:
        if not isinstance(sample, Mapping):
            raise GdcMeasurementBoundaryError("invalid_document", f"{name}: sample not an object")
        if sample.get("status") == "failed":
            if "latency_ns" in sample:
                raise GdcMeasurementBoundaryError(
                    "failed_sample_latency", f"{name}: a failed sample carries no latency"
                )
            failed.append(
                (query_id, sample.get("binding_id"), sample.get("cause"), sample.get("error_code"))
            )
            continue
        value = sample.get("latency_ns")
        if sample.get("status") != "measured" or not (
            isinstance(value, int) and not isinstance(value, bool)
        ):
            raise GdcMeasurementBoundaryError(
                "latency_not_from_samples",
                f"{name}: a measured sample carries integer latency_ns",
            )
        latencies.append(value)
    derived = (
        {
            "count": len(latencies),
            "p50_ns": nearest_rank(latencies, 50),
            "p95_ns": nearest_rank(latencies, 95),
        }
        if latencies
        else None
    )
    summary = variant.get("summary")
    if (dict(summary) if isinstance(summary, Mapping) else summary) != derived:
        raise GdcMeasurementBoundaryError(
            "latency_not_from_samples",
            f"{name}: summary {summary} is not the nearest-rank summary {derived} "
            "of its measured driver-clock samples",
        )
    if variant.get("status") != ("failed" if failed else "measured"):
        raise GdcMeasurementBoundaryError(
            "failure_record_mismatch", f"{name}: status disagrees with its samples"
        )
    return failed


def assert_query_latency_authority(
    evidence: Mapping[str, Any],
    *,
    label: str = "query evidence",
) -> None:
    """Accept per-operation latency only from the declared GDC query driver clock.

    The evidence must come from the driver, over a reconciled project, with
    every percentile derived from its own measured samples and the warm-up
    excluded. Failed samples carry no latency and are listed in `failures`;
    a run with any failure has `status: failed`. Anything else, including a
    BenchExec or phase-timing number in a latency field, is refused with a
    typed cause.
    """
    assert_live_diagnostic_boundary(evidence, label=label)
    if evidence.get("schema") != QUERY_EVIDENCE_SCHEMA:
        raise GdcMeasurementBoundaryError(
            "invalid_document", f"{label} schema must be {QUERY_EVIDENCE_SCHEMA}"
        )
    clock = evidence.get("latency_clock")
    driver = evidence.get("driver")
    if (
        not isinstance(clock, Mapping)
        or clock.get("id") != QUERY_LATENCY_CLOCK
        or clock.get("producer") != QUERY_DRIVER
        or not isinstance(driver, Mapping)
        or driver.get("name") != QUERY_DRIVER
    ):
        raise GdcMeasurementBoundaryError(
            "foreign_latency_clock",
            f"{label} latency must come from {QUERY_LATENCY_CLOCK} in {QUERY_DRIVER}",
        )
    reconciliation = evidence.get("reconciliation")
    if not isinstance(reconciliation, Mapping) or reconciliation.get("status") != "reconciled":
        raise GdcMeasurementBoundaryError(
            "unreconciled_counts", f"{label} latency requires a reconciled project"
        )
    variants = evidence.get("variants")
    if not isinstance(variants, list) or not variants:
        raise GdcMeasurementBoundaryError("invalid_document", f"{label} has no variants")
    failed: list[tuple[Any, ...]] = []
    for variant in variants:
        if not isinstance(variant, Mapping):
            raise GdcMeasurementBoundaryError(
                "invalid_document", f"{label} variant is not an object"
            )
        failed.extend(_check_variant_latency(variant, label))
    records = evidence.get("failures")
    listed = [
        (r.get("query_id"), r.get("binding_id"), r.get("cause"), r.get("error_code"))
        if isinstance(r, Mapping)
        else None
        for r in (records if isinstance(records, list) else [None])
    ]
    if listed != failed or evidence.get("status") != ("failed" if failed else "passed"):
        raise GdcMeasurementBoundaryError(
            "failure_record_mismatch",
            f"{label} status and failures must list exactly its failed samples",
        )
    error = next(_query_evidence_validator().iter_errors(dict(evidence)), None)
    if error is not None:
        location = "/".join(str(part) for part in error.absolute_path)
        raise GdcMeasurementBoundaryError(
            "invalid_document", f"{label} at /{location}: {error.message}"
        )
