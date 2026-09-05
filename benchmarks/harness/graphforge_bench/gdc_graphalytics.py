"""GDC Graphalytics suite adapter (workload semantics).

Shares identity/acquisition contracts from ``gdc_contracts`` without embedding
those contracts' workload-free rules into algorithm mapping. Rust owns mapping,
tolerance, and reference validation via ``graphforge-benchmark-gdc-graphalytics``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from graphforge_bench.gdc_contracts import (
    GdcContractError,
    load_pinned_identity,
    load_suite_declaration,
    validate_suite_acquisition,
    workspace_root,
)

ALGORITHMS = ("bfs", "pr", "wcc", "cdlp", "lcc", "sssp")
LADDER_SCHEMA = "graphforge-gdc-graphalytics-ladder/1"
JOB_SCHEMA = "graphforge-gdc-graphalytics-job/1"
EVIDENCE_SCHEMA = "graphforge-gdc-graphalytics-evidence/1"


class GraphalyticsSuiteError(ValueError):
    """Graphalytics suite mapping or validation failed."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def ladder_path(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "profiles" / "gdc" / "graphalytics-ladder.json"


def load_ladder(root: Path | None = None) -> dict[str, Any]:
    path = ladder_path(root)
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema") != LADDER_SCHEMA:
        raise GraphalyticsSuiteError("invalid_document", "unexpected ladder schema")
    if document.get("suite_id") != "graphalytics":
        raise GraphalyticsSuiteError("invalid_document", "ladder suite_id must be graphalytics")
    datasets = document.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise GraphalyticsSuiteError("invalid_document", "ladder datasets missing")
    ordered = sorted(datasets, key=lambda item: item["order"])
    if ordered[0]["id"] != "ga-tiny":
        raise GraphalyticsSuiteError(
            "invalid_document",
            "ordered ladder must begin with bounded fixture ga-tiny",
        )
    return document


def ordered_dataset_ids(root: Path | None = None) -> list[str]:
    ladder = load_ladder(root)
    return [item["id"] for item in sorted(ladder["datasets"], key=lambda item: item["order"])]


def runner_binary(root: Path | None = None) -> Path:
    base = root or workspace_root()
    override = os.environ.get("GRAPHFORGE_GDC_GRAPHALYTICS_BIN")
    if override:
        return Path(override)
    target = base / "target"
    for profile in ("debug", "release"):
        candidate = target / profile / "graphforge-benchmark-gdc-graphalytics"
        if candidate.is_file():
            return candidate
    raise GraphalyticsSuiteError(
        "missing_runner",
        "graphforge-benchmark-gdc-graphalytics binary not built; "
        "run cargo build -p graphforge-benchmark-gdc-graphalytics",
    )


def _run_runner(args: list[str], root: Path | None = None) -> subprocess.CompletedProcess[str]:
    binary = runner_binary(root)
    return subprocess.run(
        [str(binary), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def list_algorithm_rules(root: Path | None = None) -> dict[str, dict[str, str]]:
    completed = _run_runner(["list-algorithms"], root)
    if completed.returncode != 0:
        raise GraphalyticsSuiteError("invalid_document", completed.stderr.strip())
    rules: dict[str, dict[str, str]] = {}
    for line in completed.stdout.splitlines():
        algorithm, _, rest = line.partition(" ")
        if not rest.startswith("validation="):
            raise GraphalyticsSuiteError("invalid_document", f"bad algorithm line: {line}")
        validation_part, _, determinism_part = rest.partition(" ")
        validation = validation_part.removeprefix("validation=")
        if not determinism_part.startswith("determinism="):
            raise GraphalyticsSuiteError("invalid_document", f"bad algorithm line: {line}")
        determinism = determinism_part.removeprefix("determinism=")
        rules[algorithm] = {"validation": validation, "determinism": determinism}
    if set(rules) != set(ALGORITHMS):
        raise GraphalyticsSuiteError(
            "invalid_document",
            f"runner must declare all six algorithms, got {sorted(rules)}",
        )
    return rules


def run_tiny_suite(
    *,
    fixture_name: str = "compatible",
    root: Path | None = None,
    evidence_path: Path | None = None,
) -> dict[str, Any]:
    """Replay committed outputs through the Rust validator (not live proof)."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / "graphalytics-tiny" / fixture_name
    provenance = _validated_provenance(base, fixture)
    with tempfile.TemporaryDirectory(prefix="gdc-graphalytics-") as tmp:
        tmp_path = Path(tmp)
        identities_path = tmp_path / "identities.json"
        identities_path.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        out_evidence = evidence_path or (tmp_path / "evidence.json")
        completed = _run_runner(
            [
                "run-suite",
                str(fixture / "jobs"),
                str(fixture / "references"),
                str(fixture / "system-outputs"),
                str(identities_path),
                str(out_evidence),
            ],
            base,
        )
        if not out_evidence.is_file():
            raise GraphalyticsSuiteError(
                "invalid_document",
                f"runner failed to emit evidence: {completed.stderr.strip()}",
            )
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
        _validate_evidence_envelope(evidence, "static_replay")
        if completed.returncode != 0 and fixture_name == "compatible":
            raise GraphalyticsSuiteError(
                "reference_mismatch",
                f"compatible fixture must pass: {completed.stderr.strip()}",
            )
        return evidence


def run_tiny_live_suite(
    *,
    fixture_name: str = "compatible",
    root: Path | None = None,
    evidence_path: Path | None = None,
) -> dict[str, Any]:
    """Execute ga-tiny through the real in-memory public GraphForge API."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / "graphalytics-tiny" / fixture_name
    provenance = _validated_provenance(base, fixture)
    with tempfile.TemporaryDirectory(prefix="gdc-graphalytics-live-") as tmp:
        tmp_path = Path(tmp)
        identities_path = tmp_path / "identities.json"
        identities_path.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        out_evidence = evidence_path or (tmp_path / "evidence.json")
        completed = _run_runner(
            [
                "run-live",
                str(fixture / "ga-tiny.edges"),
                str(fixture / "jobs"),
                str(fixture / "references"),
                str(identities_path),
                str(out_evidence),
            ],
            base,
        )
        if not out_evidence.is_file():
            raise GraphalyticsSuiteError(
                "invalid_document",
                f"live runner failed to emit evidence: {completed.stderr.strip()}",
            )
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
        _validate_evidence_envelope(evidence, "live_public_api")
        if completed.returncode != 0 and fixture_name == "compatible":
            raise GraphalyticsSuiteError(
                "reference_mismatch",
                f"compatible live fixture must pass: {completed.stderr.strip()}",
            )
        return evidence


def suite_declaration(root: Path | None = None) -> dict[str, Any]:
    return load_suite_declaration((root or workspace_root()) / "suites" / "gdc-graphalytics.json")


def identity_path(profile: str, root: Path | None = None) -> Path:
    suite = suite_declaration(root)
    profiles = suite.get("identity_profiles") or {}
    if profile not in profiles:
        raise GraphalyticsSuiteError(
            "identity_drift",
            f"unknown Graphalytics identity profile: {profile}",
        )
    return (root or workspace_root()) / profiles[profile]


def _validated_provenance(base: Path, fixture: Path) -> dict[str, Any]:
    suite = suite_declaration(base)
    acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
    contract = validate_suite_acquisition(suite, acquisition, fixture, base)
    return {
        **contract["identities"],
        "datasets": contract["datasets"],
        "references": contract["references"],
    }


def _validate_evidence_envelope(evidence: dict[str, Any], execution_mode: str) -> None:
    if evidence.get("schema") != EVIDENCE_SCHEMA:
        raise GraphalyticsSuiteError(
            "invalid_document",
            "unexpected graphalytics evidence schema",
        )
    if evidence.get("execution_mode") != execution_mode:
        raise GraphalyticsSuiteError(
            "invalid_document",
            f"expected {execution_mode} evidence, got {evidence.get('execution_mode')}",
        )
    outcomes = evidence.get("algorithms")
    if (
        not isinstance(outcomes, list)
        or len(outcomes) != len(ALGORITHMS)
        or any(
            not isinstance(item, dict) or item.get("workload_key") not in ALGORITHMS
            for item in outcomes
        )
        or sorted(item.get("workload_key", "") for item in outcomes) != sorted(ALGORITHMS)
    ):
        raise GraphalyticsSuiteError(
            "invalid_document", "evidence requires each of the six algorithms exactly once"
        )
    if evidence.get("certification") is not False:
        raise GraphalyticsSuiteError(
            "invalid_document",
            "engineering Graphalytics evidence must remain certification=false",
        )


def assert_identity_profiles_are_separated(root: Path | None = None) -> None:
    """Historical wiki-Talk markers must not claim ga-tiny or v1.0.5 authority."""
    base = root or workspace_root()
    static_pin = load_pinned_identity(identity_path("static", base))
    live_pin = load_pinned_identity(identity_path("live", base))
    static_ids = {item["id"] for item in static_pin["datasets"]}
    live_ids = {item["id"] for item in live_pin["datasets"]}
    if "ga-tiny" in static_ids:
        raise GraphalyticsSuiteError(
            "identity_drift",
            "static historical pin must not claim ga-tiny",
        )
    if "wiki-Talk" in live_ids:
        raise GraphalyticsSuiteError(
            "identity_drift",
            "live ga-tiny pin must not claim wiki-Talk",
        )
    if live_pin["spec"]["release"] != "v1.0.5":
        raise GraphalyticsSuiteError(
            "identity_drift",
            "live pin must keep the Graphalytics v1.0.5 spec identity",
        )
    if static_pin["spec"]["release"] == live_pin["spec"]["release"]:
        raise GraphalyticsSuiteError(
            "identity_drift",
            "static historical pin must not reuse the live spec release",
        )


def assert_separate_from_graph500(root: Path | None = None) -> None:
    """Profiles/validation/evidence for Graphalytics must not share Graph500 paths."""
    base = root or workspace_root()
    suite = json.loads((base / "suites" / "gdc-graphalytics.json").read_text(encoding="utf-8"))
    if suite.get("family") != "gdc" or suite.get("suite_id") != "graphalytics":
        raise GraphalyticsSuiteError(
            "invalid_document",
            "suite must remain a GDC Graphalytics suite",
        )
    if "graph500" in json.dumps(suite):
        raise GraphalyticsSuiteError(
            "invalid_document",
            "Graphalytics suite must not embed Graph500",
        )
    ladder = load_ladder(base)
    if any("graph500" in item["id"].lower() and item["order"] == 1 for item in ladder["datasets"]):
        raise GraphalyticsSuiteError(
            "invalid_document",
            "bounded Graphalytics ladder must not start on Graph500",
        )


def map_job_file(path: Path, root: Path | None = None) -> dict[str, Any]:
    completed = _run_runner(["map-job", str(path)], root)
    if completed.returncode == 3:
        raise GraphalyticsSuiteError("semantic_incompatibility", completed.stderr.strip())
    if completed.returncode != 0:
        raise GraphalyticsSuiteError("invalid_document", completed.stderr.strip())
    return json.loads(completed.stdout)


# Re-export contract error for callers that mix provenance + suite failures.
__all__ = [
    "ALGORITHMS",
    "EVIDENCE_SCHEMA",
    "JOB_SCHEMA",
    "LADDER_SCHEMA",
    "GdcContractError",
    "GraphalyticsSuiteError",
    "assert_identity_profiles_are_separated",
    "assert_separate_from_graph500",
    "identity_path",
    "list_algorithm_rules",
    "load_ladder",
    "map_job_file",
    "ordered_dataset_ids",
    "run_tiny_live_suite",
    "run_tiny_suite",
    "suite_declaration",
]
