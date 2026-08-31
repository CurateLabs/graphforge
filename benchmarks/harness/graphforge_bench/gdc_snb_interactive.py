"""GDC SNB Interactive suite adapter (workload semantics).

Shares identity/acquisition contracts from ``gdc_contracts`` without embedding
those contracts' workload-free rules into operation mapping. Rust owns mapping,
phase separation, completeness policy, and reference validation via
``graphforge-benchmark-gdc-snb-interactive``.
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
    validate_acquisition,
    workspace_root,
)

OPERATIONS = tuple(
    [f"ic{i}" for i in range(1, 15)]
    + [f"is{i}" for i in range(1, 8)]
    + [f"iu{i}" for i in range(1, 9)]
)
SUPPORTED_OPERATIONS = ("is1", "is3", "is4")
PHASES = ("load", "warmup", "execution", "validation")
LADDER_SCHEMA = "graphforge-gdc-snb-interactive-ladder/1"
JOB_SCHEMA = "graphforge-gdc-snb-interactive-job/1"
EVIDENCE_SCHEMA = "graphforge-gdc-snb-interactive-evidence/1"
TINY_DATASET = "snb-sf0.003"


class SnbInteractiveSuiteError(ValueError):
    """SNB Interactive suite mapping or validation failed."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def ladder_path(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "profiles" / "gdc" / "snb-interactive-ladder.json"


def load_ladder(root: Path | None = None) -> dict[str, Any]:
    path = ladder_path(root)
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema") != LADDER_SCHEMA:
        raise SnbInteractiveSuiteError("invalid_document", "unexpected ladder schema")
    if document.get("suite_id") != "snb-interactive":
        raise SnbInteractiveSuiteError(
            "invalid_document",
            "ladder suite_id must be snb-interactive",
        )
    datasets = document.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise SnbInteractiveSuiteError("invalid_document", "ladder datasets missing")
    ordered = sorted(datasets, key=lambda item: item["order"])
    if ordered[0]["id"] != TINY_DATASET:
        raise SnbInteractiveSuiteError(
            "invalid_document",
            f"ordered ladder must begin with bounded fixture {TINY_DATASET}",
        )
    return document


def ordered_dataset_ids(root: Path | None = None) -> list[str]:
    ladder = load_ladder(root)
    return [item["id"] for item in sorted(ladder["datasets"], key=lambda item: item["order"])]


def runner_binary(root: Path | None = None) -> Path:
    base = root or workspace_root()
    override = os.environ.get("GRAPHFORGE_GDC_SNB_INTERACTIVE_BIN")
    if override:
        return Path(override)
    target = base / "target"
    for profile in ("debug", "release"):
        candidate = target / profile / "graphforge-benchmark-gdc-snb-interactive"
        if candidate.is_file():
            return candidate
    raise SnbInteractiveSuiteError(
        "missing_runner",
        "graphforge-benchmark-gdc-snb-interactive binary not built; "
        "run cargo build -p graphforge-benchmark-gdc-snb-interactive",
    )


def _run_runner(args: list[str], root: Path | None = None) -> subprocess.CompletedProcess[str]:
    binary = runner_binary(root)
    return subprocess.run(
        [str(binary), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def list_operation_rules(root: Path | None = None) -> dict[str, dict[str, str]]:
    completed = _run_runner(["list-operations"], root)
    if completed.returncode != 0:
        raise SnbInteractiveSuiteError("invalid_document", completed.stderr.strip())
    rules: dict[str, dict[str, str]] = {}
    for line in completed.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            raise SnbInteractiveSuiteError("invalid_document", f"bad operation line: {line}")
        operation = parts[0]
        fields = dict(part.split("=", 1) for part in parts[1:] if "=" in part)
        if not {"class", "validation", "support"}.issubset(fields):
            raise SnbInteractiveSuiteError("invalid_document", f"bad operation line: {line}")
        rules[operation] = fields
    if set(rules) != set(OPERATIONS):
        raise SnbInteractiveSuiteError(
            "invalid_document",
            f"runner must declare all Interactive operations, got {sorted(rules)}",
        )
    return rules


def run_tiny_suite(
    *,
    fixture_name: str = "compatible",
    root: Path | None = None,
    evidence_path: Path | None = None,
) -> dict[str, Any]:
    """Run the bounded snb-sf0.003 Interactive suite through the Rust runner."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / "snb-interactive-tiny" / fixture_name
    pin = load_pinned_identity(base / "profiles" / "gdc" / "snb-interactive-identity.json")
    acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
    contract_evidence = validate_acquisition(pin, acquisition, fixture)
    identities = contract_evidence["identities"]
    with tempfile.TemporaryDirectory(prefix="gdc-snb-interactive-") as tmp:
        tmp_path = Path(tmp)
        identities_path = tmp_path / "identities.json"
        identities_path.write_text(json.dumps(identities, indent=2) + "\n", encoding="utf-8")
        out_evidence = evidence_path or (tmp_path / "evidence.json")
        completed = _run_runner(
            [
                "run-suite",
                str(fixture / "jobs"),
                str(fixture / "references"),
                str(fixture / "system-outputs"),
                str(fixture / "phases.json"),
                str(identities_path),
                str(out_evidence),
            ],
            base,
        )
        if not out_evidence.is_file():
            raise SnbInteractiveSuiteError(
                "invalid_document",
                f"runner failed to emit evidence: {completed.stderr.strip()}",
            )
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
        if evidence.get("schema") != EVIDENCE_SCHEMA:
            raise SnbInteractiveSuiteError(
                "invalid_document",
                "unexpected snb-interactive evidence schema",
            )
        if evidence.get("audited_gdc_certification") is not False:
            raise SnbInteractiveSuiteError(
                "certification_masquerade",
                "engineering evidence must set audited_gdc_certification=false",
            )
        if evidence.get("run_class") != "engineering":
            raise SnbInteractiveSuiteError(
                "certification_masquerade",
                "engineering evidence must declare run_class=engineering",
            )
        if completed.returncode != 0 and fixture_name == "compatible":
            raise SnbInteractiveSuiteError(
                "reference_mismatch",
                f"compatible fixture must pass: {completed.stderr.strip()}",
            )
        return evidence


def assert_not_audited_certification(evidence: dict[str, Any]) -> None:
    if evidence.get("audited_gdc_certification") is not False:
        raise SnbInteractiveSuiteError(
            "certification_masquerade",
            "results must never masquerade as audited GDC certification",
        )
    if evidence.get("run_class") != "engineering":
        raise SnbInteractiveSuiteError(
            "certification_masquerade",
            "results must declare run_class=engineering",
        )


def map_job_file(path: Path, root: Path | None = None) -> dict[str, Any]:
    completed = _run_runner(["map-job", str(path)], root)
    if completed.returncode == 3:
        raise SnbInteractiveSuiteError("semantic_incompatibility", completed.stderr.strip())
    if completed.returncode != 0:
        raise SnbInteractiveSuiteError("invalid_document", completed.stderr.strip())
    return json.loads(completed.stdout)


__all__ = [
    "EVIDENCE_SCHEMA",
    "JOB_SCHEMA",
    "LADDER_SCHEMA",
    "OPERATIONS",
    "PHASES",
    "SUPPORTED_OPERATIONS",
    "TINY_DATASET",
    "GdcContractError",
    "SnbInteractiveSuiteError",
    "assert_not_audited_certification",
    "list_operation_rules",
    "load_ladder",
    "map_job_file",
    "ordered_dataset_ids",
    "run_tiny_suite",
]
