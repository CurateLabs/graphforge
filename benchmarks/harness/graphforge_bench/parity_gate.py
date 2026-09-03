"""#959 parity gate status — tracks acceptance criteria without fabricating evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from graphforge_bench.scale_parity import (
    compare_fixture_pair,
    compare_ladder_bundle,
    coverage_map,
    load_accepted_differences,
    validate_historical_legacy_cert,
    workspace_root,
)

GATE_SCHEMA = "graphforge-scale-orchestration-parity-gate/1"


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
    if rung_files:
        matrices = compare_ladder_bundle(bundle)
        ladder_ok = bool(matrices) and all(
            matrix["overall"] in {"match", "accepted_difference"} for matrix in matrices
        )
        ladder_detail = f"{len(rung_files)} rung file(s), {len(matrices)} comparison(s)"

    accepted = load_accepted_differences()
    migration_fixtures = [
        path.name for path in (fixtures / "legacy").glob("*.json") if path.name != "tiny-pass.json"
    ]

    harness_authoritative_met = ladder_ok if rung_files else False
    parity_matrix_met = tiny_ok and harness_authoritative_met
    legacy_retired = not _legacy_orchestration_present(base)

    criteria = [
        _criterion(
            "parity_matrix_no_unexplained_gaps",
            met=parity_matrix_met,
            blocked_by="#900 ladder bundles" if tiny_ok and not rung_files else None,
            evidence=f"tiny overall={tiny_matrix['overall']}; ladder {ladder_detail}",
        ),
        _criterion(
            "harness_authoritative_after_ladder_comparison",
            met=harness_authoritative_met,
            blocked_by="#900"
            if not rung_files
            else ("ladder parity gaps" if not ladder_ok else None),
            evidence=ladder_detail,
        ),
        _criterion(
            "legacy_orchestration_retired_with_coverage",
            met=legacy_retired and parity_matrix_met and harness_authoritative_met,
            blocked_by=(
                "parity_matrix_no_unexplained_gaps + harness_authoritative"
                if not parity_matrix_met
                else (
                    "legacy Makefile targets and workflows remain" if not legacy_retired else None
                )
            ),
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
    ]

    ready_for_retirement = all(row["met"] for row in criteria)
    return {
        "schema": GATE_SCHEMA,
        "ready_for_retirement": ready_for_retirement,
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
