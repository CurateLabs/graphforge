"""#959 parity gate status — tracks acceptance criteria without fabricating evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from graphforge_bench.ladder_bundle_ingest import validate_ladder_bundle
from graphforge_bench.native_ladder_bundle import NativeBundleError, validate_native_bundle
from graphforge_bench.progressive_provider_attempt import CANONICAL_RUNGS
from graphforge_bench.scale_parity import (
    compare_fixture_pair,
    compare_ladder_bundle,
    coverage_map,
    load_accepted_differences,
    validate_historical_legacy_cert,
    workspace_root,
)

GATE_SCHEMA = "graphforge-scale-orchestration-parity-gate-status/1"


def _criterion(
    name: str,
    *,
    met: bool,
    blocked_by: str | None = None,
    evidence: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "met": met,
        "blocked_by": blocked_by,
        "evidence": evidence,
    }


def ladder_bundle_root(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "fixtures" / "parity" / "ladder-bundle"


def _legacy_orchestration_present(root: Path | None = None) -> bool:
    """Return True while legacy Makefile targets or workflows remain in-tree."""
    repo = (root or workspace_root()).parent
    makefile = repo / "Makefile"
    if not makefile.is_file():
        return True
    text = makefile.read_text(encoding="utf-8")
    legacy_targets = (
        "bench-g500-ladder:",
        "bench-g500-scale20:",
        "g500-ladder-qualification:",
    )
    if any(target in text for target in legacy_targets):
        return True
    legacy_workflow = repo / ".github" / "workflows" / "g500-certification.yml"
    if legacy_workflow.is_file():
        return True
    registry = repo / "config" / "gate-registry.json"
    if registry.is_file():
        gate_doc = json.loads(registry.read_text(encoding="utf-8"))
        workflow_ids = {row["id"] for row in gate_doc.get("workflows", [])}
        if "g500-certification" in workflow_ids:
            return True
    return False


def _read_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _canonical_prefix(rung_files: list[Path]) -> tuple[bool, list[int]]:
    """Return whether files and embedded scales form an authoritative prefix."""
    if not rung_files or len(rung_files) > len(CANONICAL_RUNGS):
        return False, []
    expected = [f"s{scale}-rung.json" for scale in CANONICAL_RUNGS[: len(rung_files)]]
    if [path.name for path in rung_files] != expected:
        return False, []
    scales: list[int] = []
    for path in rung_files:
        document = _read_object(path)
        scale = document.get("scale") if document is not None else None
        if type(scale) is not int:
            return False, scales
        scales.append(scale)
    return scales == list(CANONICAL_RUNGS[: len(scales)]), scales


def _full_ladder_bundle_complete(bundle: Path, *, canonical_prefix: bool) -> tuple[bool, str]:
    """Recognize completed native engineering evidence without a cloud ceremony."""
    if not canonical_prefix:
        return False, "rung files are not a canonical prefix"
    try:
        receipt = validate_native_bundle(bundle)
    except (NativeBundleError, OSError) as error:
        return False, str(error)
    return receipt["complete"], f"scales={receipt['scales']}; empty_work_root={receipt['empty']}"


def parity_gate_status(root: Path | None = None) -> dict[str, Any]:
    """Report #959 acceptance-criteria readiness from checked-in fixtures only."""
    base = root or workspace_root()
    fixtures = base / "fixtures" / "parity"
    bundle = ladder_bundle_root(base)
    rung_files = sorted(bundle.glob("*-rung.json")) if bundle.is_dir() else []

    tiny_matrix = compare_fixture_pair(
        fixtures / "legacy" / "tiny-pass.json",
        fixtures / "new" / "tiny-pass.json",
    )
    tiny_ok = tiny_matrix["overall"] in {"match", "accepted_difference"}

    historical_path = fixtures / "legacy" / "cert-s20-minimal.json"
    historical_ok = False
    if historical_path.is_file():
        try:
            validate_historical_legacy_cert(historical_path, expected_sha="a" * 40)
            historical_ok = True
        except Exception:
            historical_ok = False

    ladder_ok = False
    ladder_detail = "no ingested #900 rung bundles"
    canonical_prefix, rung_scales = _canonical_prefix(rung_files)
    if rung_files:
        try:
            validate_ladder_bundle(bundle)
            matrices = compare_ladder_bundle(bundle)
            ladder_ok = bool(matrices) and all(
                matrix["overall"] in {"match", "accepted_difference"} for matrix in matrices
            )
            ladder_detail = (
                f"{len(rung_files)} rung file(s), {len(matrices)} comparison(s); "
                f"canonical_prefix={canonical_prefix}; scales={rung_scales}"
            )
        except Exception as error:
            ladder_detail = f"{len(rung_files)} rung file(s); comparison failed: {error}"

    accepted = load_accepted_differences()
    migration_fixtures = [
        path.name for path in (fixtures / "legacy").glob("*.json") if path.name != "tiny-pass.json"
    ]

    prefix_comparison_met = bool(rung_files) and ladder_ok and canonical_prefix
    prefix_parity_ready = tiny_ok and prefix_comparison_met
    legacy_retired = not _legacy_orchestration_present(base)
    structural_retirement_ready = legacy_retired and historical_ok and bool(migration_fixtures)
    full_ladder_complete, full_ladder_detail = _full_ladder_bundle_complete(
        bundle, canonical_prefix=canonical_prefix
    )
    full_ladder_evidence_complete = prefix_parity_ready and full_ladder_complete

    harness_authoritative_met = full_ladder_evidence_complete

    criteria = [
        _criterion(
            "parity_matrix_no_unexplained_gaps",
            met=prefix_parity_ready,
            blocked_by="#900 ladder bundles" if tiny_ok and not rung_files else None,
            evidence=f"tiny overall={tiny_matrix['overall']}; ladder {ladder_detail}",
        ),
        _criterion(
            "harness_authoritative_after_ladder_comparison",
            met=harness_authoritative_met,
            blocked_by="#900" if not full_ladder_evidence_complete else None,
            evidence=ladder_detail,
        ),
        _criterion(
            "legacy_orchestration_retired_with_coverage",
            met=legacy_retired,
            blocked_by="legacy Makefile targets and workflows remain"
            if not legacy_retired
            else None,
            evidence=(
                f"coverage entries={len(coverage_map())}; legacy_present={not legacy_retired}"
            ),
        ),
        _criterion(
            "historical_evidence_readable",
            met=historical_ok,
            evidence=str(historical_path.relative_to(base)) if historical_ok else None,
        ),
        _criterion(
            "migration_fixtures_preserved",
            met=bool(migration_fixtures),
            evidence=", ".join(sorted(migration_fixtures)) or None,
        ),
        _criterion(
            "no_duplicate_s18_s26_ladder_for_parity",
            met=True,
            evidence="compare_ladder_bundle ingests #900 output read-only only",
        ),
        _criterion(
            "full_ladder_evidence_complete",
            met=full_ladder_evidence_complete,
            blocked_by="complete native S18-S26 evidence and work-root inventory"
            if not full_ladder_evidence_complete
            else None,
            evidence=full_ladder_detail,
        ),
    ]

    return {
        "schema": GATE_SCHEMA,
        "structural_retirement_ready": structural_retirement_ready,
        "prefix_parity_ready": prefix_parity_ready,
        "full_ladder_evidence_complete": full_ladder_evidence_complete,
        "accepted_differences_schema": accepted.get("schema"),
        "criteria": criteria,
    }


def assert_tiny_parity_ready(root: Path | None = None) -> None:
    """Fail closed when tiny/local shadow parity is not green."""
    base = root or workspace_root()
    tiny_only = compare_fixture_pair(
        base / "fixtures" / "parity" / "legacy" / "tiny-pass.json",
        base / "fixtures" / "parity" / "new" / "tiny-pass.json",
    )
    if tiny_only["overall"] == "unexplained_gap":
        raise ValueError(f"unexplained tiny parity gaps: {tiny_only}")
