"""#959 parity gate status — tracks acceptance criteria without fabricating evidence."""

from __future__ import annotations

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

    criteria = [
        _criterion(
            "parity_matrix_no_unexplained_gaps",
            met=tiny_ok and (ladder_ok if rung_files else False),
            blocked_by="#900 ladder bundles" if tiny_ok and not rung_files else None,
            evidence=f"tiny overall={tiny_matrix['overall']}; ladder {ladder_detail}",
        ),
        _criterion(
            "harness_authoritative_after_ladder_comparison",
            met=False,
            blocked_by="#900",
            evidence=ladder_detail,
        ),
        _criterion(
            "legacy_orchestration_retired_with_coverage",
            met=False,
            blocked_by="parity_matrix_no_unexplained_gaps + harness_authoritative",
            evidence=f"coverage entries={len(coverage_map())}",
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
