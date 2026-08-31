"""GDC SNB Interactive suite adapter (workload semantics).

Shares identity/acquisition contracts from ``gdc_contracts`` without embedding
those contracts' workload-free rules into operation mapping. Rust owns mapping,
validation modes, phase separation, and reference validation via
``graphforge-benchmark-gdc-snb-interactive``.

Results here are engineering evidence only. They never masquerade as an audited
GDC certification (the runner stamps ``certification: false``).
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

COMPLEX_READS = tuple(f"IC{index}" for index in range(1, 15))
SHORT_READS = tuple(f"IS{index}" for index in range(1, 8))
UPDATES = tuple(f"IU{index}" for index in range(1, 9))
OPERATIONS = COMPLEX_READS + SHORT_READS + UPDATES

JOB_SCHEMA = "graphforge-gdc-snb-interactive-job/1"
EVIDENCE_SCHEMA = "graphforge-gdc-snb-interactive-evidence/1"

UPDATE_CAUSE = "interactive_update_stream_not_exposed"
IC14_CAUSE = "weighted_interaction_path_enumeration_not_exposed"


class SnbInteractiveSuiteError(ValueError):
    """SNB Interactive suite mapping or validation failed."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def identity_path(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "profiles" / "gdc" / "snb-interactive-identity.json"


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
        operation, _, rest = line.partition(" ")
        fields: dict[str, str] = {}
        for token in rest.split():
            key, _, value = token.partition("=")
            fields[key] = value
        if "category" not in fields or "validation" not in fields or "mapping" not in fields:
            raise SnbInteractiveSuiteError("invalid_document", f"bad operation line: {line}")
        rules[operation] = fields
    if set(rules) != set(OPERATIONS):
        raise SnbInteractiveSuiteError(
            "invalid_document",
            f"runner must declare all {len(OPERATIONS)} operations, got {sorted(rules)}",
        )
    return rules


def run_tiny_suite(
    *,
    fixture_name: str = "compatible",
    root: Path | None = None,
    evidence_path: Path | None = None,
) -> dict[str, Any]:
    """Run the bounded snb-sf0.003 SNB Interactive suite through the Rust runner."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / "snb-interactive-tiny" / fixture_name
    pin = load_pinned_identity(identity_path(base))
    acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
    # Provenance evidence from shared contracts (checksummed assets only).
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
        if evidence.get("certification") is not False:
            raise SnbInteractiveSuiteError(
                "invalid_document",
                "evidence must never claim GDC certification",
            )
        if completed.returncode != 0 and fixture_name == "compatible":
            raise SnbInteractiveSuiteError(
                "reference_mismatch",
                f"compatible fixture must pass: {completed.stderr.strip()}",
            )
        return evidence


def map_operation_file(path: Path, root: Path | None = None) -> dict[str, Any]:
    completed = _run_runner(["map-operation", str(path)], root)
    if completed.returncode == 3:
        raise SnbInteractiveSuiteError("semantic_incompatibility", completed.stderr.strip())
    if completed.returncode != 0:
        raise SnbInteractiveSuiteError("invalid_document", completed.stderr.strip())
    return json.loads(completed.stdout)


def assert_separate_from_other_suites(root: Path | None = None) -> None:
    """SNB Interactive profiles/validation/evidence stay distinct from siblings."""
    base = root or workspace_root()
    suite = json.loads((base / "suites" / "gdc-snb-interactive.json").read_text(encoding="utf-8"))
    if suite.get("family") != "gdc" or suite.get("suite_id") != "snb-interactive":
        raise SnbInteractiveSuiteError(
            "invalid_document",
            "suite must remain a GDC SNB Interactive suite",
        )
    rendered = json.dumps(suite)
    for foreign in ("graph500", "graphalytics", "snb-bi", "finbench", "spb"):
        if foreign in rendered:
            raise SnbInteractiveSuiteError(
                "invalid_document",
                f"SNB Interactive suite must not embed {foreign}",
            )
    if suite.get("runner") != "gdc-snb-interactive":
        raise SnbInteractiveSuiteError(
            "invalid_document",
            "SNB Interactive suite must use the gdc-snb-interactive runner",
        )


__all__ = [
    "COMPLEX_READS",
    "EVIDENCE_SCHEMA",
    "IC14_CAUSE",
    "JOB_SCHEMA",
    "OPERATIONS",
    "SHORT_READS",
    "UPDATES",
    "UPDATE_CAUSE",
    "GdcContractError",
    "SnbInteractiveSuiteError",
    "assert_separate_from_other_suites",
    "identity_path",
    "list_operation_rules",
    "map_operation_file",
    "run_tiny_suite",
]
