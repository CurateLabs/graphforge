"""GDC SNB BI suite adapter (workload semantics).

Shares identity/acquisition contracts from ``gdc_contracts`` without embedding
those contracts' workload-free rules into operation mapping. Rust owns mapping,
validation modes, phase separation, per-phase resource recording, and reference
validation via ``graphforge-benchmark-gdc-snb-bi``.

Resource evidence (load/query/spill/rss/io) is kept in a distinct ``resources``
section, separate from the per-operation correctness ``operations``. Results
here are engineering evidence only. They never masquerade as an audited GDC
certification (the runner stamps ``certification: false``).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any

from graphforge_bench.gdc_contracts import (
    GdcContractError,
    load_pinned_identity,
    validate_acquisition,
    workspace_root,
)

ANALYTICAL_READS = tuple(f"BI{index}" for index in range(1, 21))
BATCH_INSERTS = tuple(f"INS{index}" for index in range(1, 9))
BATCH_DELETES = tuple(f"DEL{index}" for index in range(1, 9))
OPERATIONS = ANALYTICAL_READS + BATCH_INSERTS + BATCH_DELETES

JOB_SCHEMA = "graphforge-gdc-snb-bi-job/1"
EVIDENCE_SCHEMA = "graphforge-gdc-snb-bi-evidence/1"
RESOURCE_SCHEMA = "graphforge-gdc-snb-bi-resources/1"
LIVE_EVIDENCE_SCHEMA = "graphforge-gdc-snb-bi-live-evidence/2"
LIVE_OPERATION = "BI2"
LIVE_FIXTURE = "snb-bi-live"

BATCH_UPDATE_CAUSE = "bi_batch_update_stream_not_exposed"
WEIGHTED_PATH_CAUSE = "weighted_shortest_path_not_exposed"
WEIGHTED_PATH_READS = ("BI15", "BI19", "BI20")

BOUNDED_TINY_DATASET = "snb-bi-sf0.003"


class SnbBiSuiteError(ValueError):
    """SNB BI suite mapping or validation failed."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def identity_path(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "profiles" / "gdc" / "snb-bi-identity.json"


def runner_binary(root: Path | None = None) -> Path:
    base = root or workspace_root()
    override = os.environ.get("GRAPHFORGE_GDC_SNB_BI_BIN")
    if override:
        return Path(override)
    target = base / "target"
    for profile in ("debug", "release"):
        candidate = target / profile / "graphforge-benchmark-gdc-snb-bi"
        if candidate.is_file():
            return candidate
    raise SnbBiSuiteError(
        "missing_runner",
        "graphforge-benchmark-gdc-snb-bi binary not built; "
        "run cargo build -p graphforge-benchmark-gdc-snb-bi",
    )


def _run_runner(args: list[str], root: Path | None = None) -> subprocess.CompletedProcess[str]:
    binary = runner_binary(root)
    return subprocess.run(
        [str(binary), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _raise_live_error(completed: subprocess.CompletedProcess[str]) -> None:
    message = completed.stderr.strip()
    if "reference_mismatch" in message:
        raise SnbBiSuiteError("reference_mismatch", message)
    if "parameter" in message:
        raise SnbBiSuiteError("parameter_identity_mismatch", message)
    if "identity" in message:
        raise SnbBiSuiteError("identity_drift", message)
    if "checksum" in message:
        raise SnbBiSuiteError("checksum_mismatch", message)
    raise SnbBiSuiteError("invalid_document", message)


def validate_live_fixture(
    fixture: Path,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Ask the trusted Rust runner to validate the complete closed context."""
    base = root or workspace_root()
    completed = _run_runner(["validate-live-context", str(fixture)], base)
    if completed.returncode != 0:
        _raise_live_error(completed)
    return json.loads((fixture / "identity.json").read_text(encoding="utf-8"))


def run_live_bi2(
    *,
    root: Path | None = None,
    parameters_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the trusted Rust-owned in-memory API execution and evidence path."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / LIVE_FIXTURE
    with tempfile.TemporaryDirectory(prefix="gdc-snb-bi-live-") as tmp:
        temp = Path(tmp)
        execution_fixture = fixture
        if parameters_override:
            execution_fixture = temp / "fixture"
            shutil.copytree(fixture, execution_fixture)
            parameter_path = execution_fixture / "parameters.json"
            parameters = json.loads(parameter_path.read_text(encoding="utf-8"))
            for name, value in parameters_override.items():
                parameters["bindings"][name]["value"] = value
            parameter_path.write_text(json.dumps(parameters, indent=2) + "\n", encoding="utf-8")
        evidence_path = temp / "evidence.json"
        completed = _run_runner(
            ["run-live", str(execution_fixture), str(evidence_path)],
            base,
        )
        if completed.returncode != 0:
            _raise_live_error(completed)
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    if evidence.get("schema") != LIVE_EVIDENCE_SCHEMA:
        raise SnbBiSuiteError("invalid_document", "unexpected live evidence schema")
    if evidence.get("certification") is not False:
        raise SnbBiSuiteError("invalid_document", "live evidence must set certification=false")
    return evidence


def list_operation_rules(root: Path | None = None) -> dict[str, dict[str, str]]:
    completed = _run_runner(["list-operations"], root)
    if completed.returncode != 0:
        raise SnbBiSuiteError("invalid_document", completed.stderr.strip())
    rules: dict[str, dict[str, str]] = {}
    for line in completed.stdout.splitlines():
        operation, _, rest = line.partition(" ")
        fields: dict[str, str] = {}
        for token in rest.split():
            key, _, value = token.partition("=")
            fields[key] = value
        if "category" not in fields or "validation" not in fields or "mapping" not in fields:
            raise SnbBiSuiteError("invalid_document", f"bad operation line: {line}")
        rules[operation] = fields
    if set(rules) != set(OPERATIONS):
        raise SnbBiSuiteError(
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
    """Replay the legacy synthetic static contract fixture through Rust."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / "snb-bi-tiny" / fixture_name
    pin = load_pinned_identity(identity_path(base))
    acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
    # Provenance evidence from shared contracts (checksummed assets only).
    contract_evidence = validate_acquisition(pin, acquisition, fixture)
    identities = contract_evidence["identities"]
    with tempfile.TemporaryDirectory(prefix="gdc-snb-bi-") as tmp:
        tmp_path = Path(tmp)
        identities_path = tmp_path / "identities.json"
        identities_path.write_text(json.dumps(identities, indent=2) + "\n", encoding="utf-8")
        out_evidence = evidence_path or (tmp_path / "evidence.json")
        completed = _run_runner(
            [
                "run-static-suite",
                str(fixture / "jobs"),
                str(fixture / "references"),
                str(fixture / "system-outputs"),
                str(fixture / "resources.json"),
                str(identities_path),
                str(out_evidence),
            ],
            base,
        )
        if not out_evidence.is_file():
            raise SnbBiSuiteError(
                "invalid_document",
                f"runner failed to emit evidence: {completed.stderr.strip()}",
            )
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
        if evidence.get("schema") != EVIDENCE_SCHEMA:
            raise SnbBiSuiteError(
                "invalid_document",
                "unexpected snb-bi evidence schema",
            )
        if evidence.get("certification") is not False:
            raise SnbBiSuiteError(
                "invalid_document",
                "evidence must never claim GDC certification",
            )
        if "resources" not in evidence or "operations" not in evidence:
            raise SnbBiSuiteError(
                "invalid_document",
                "evidence must record resources separately from correctness",
            )
        if completed.returncode != 0 and fixture_name == "compatible":
            raise SnbBiSuiteError(
                "reference_mismatch",
                f"compatible fixture must pass: {completed.stderr.strip()}",
            )
        return evidence


def map_operation_file(path: Path, root: Path | None = None) -> dict[str, Any]:
    completed = _run_runner(["map-operation", str(path)], root)
    if completed.returncode == 3:
        raise SnbBiSuiteError("semantic_incompatibility", completed.stderr.strip())
    if completed.returncode != 0:
        raise SnbBiSuiteError("invalid_document", completed.stderr.strip())
    return json.loads(completed.stdout)


def assert_large_scale_factors_are_opt_in(root: Path | None = None) -> None:
    """Only the synthetic tiny fixture is replayed by default; scale runs are opt-in.

    ``snb-bi-sf0.003`` is a historical synthetic fixture identifier, not an
    official scale-factor claim. Real generated scale factors are external and
    opt-in.
    """
    base = root or workspace_root()
    suite = json.loads((base / "suites" / "gdc-snb-bi.json").read_text(encoding="utf-8"))
    datasets = suite.get("datasets", [])
    if datasets != [BOUNDED_TINY_DATASET]:
        raise SnbBiSuiteError(
            "invalid_document",
            "default SNB BI suite must run only the bounded tiny fixture; "
            "larger scale factors are opt-in / external",
        )
    pin = load_pinned_identity(identity_path(base))
    pinned_ids = [dataset["id"] for dataset in pin.get("datasets", [])]
    if pinned_ids != [BOUNDED_TINY_DATASET]:
        raise SnbBiSuiteError(
            "invalid_document",
            "pinned identity must bound the committed fixture to the tiny scale factor",
        )


def assert_separate_from_other_suites(root: Path | None = None) -> None:
    """SNB BI profiles/validation/evidence stay distinct from siblings."""
    base = root or workspace_root()
    suite = json.loads((base / "suites" / "gdc-snb-bi.json").read_text(encoding="utf-8"))
    if suite.get("family") != "gdc" or suite.get("suite_id") != "snb-bi":
        raise SnbBiSuiteError(
            "invalid_document",
            "suite must remain a GDC SNB BI suite",
        )
    rendered = json.dumps(suite)
    for foreign in ("graph500", "graphalytics", "snb-interactive", "finbench", "spb"):
        if foreign in rendered:
            raise SnbBiSuiteError(
                "invalid_document",
                f"SNB BI suite must not embed {foreign}",
            )
    if suite.get("runner") != "gdc-snb-bi":
        raise SnbBiSuiteError(
            "invalid_document",
            "SNB BI suite must use the gdc-snb-bi runner",
        )


__all__ = [
    "ANALYTICAL_READS",
    "BATCH_DELETES",
    "BATCH_INSERTS",
    "BATCH_UPDATE_CAUSE",
    "BOUNDED_TINY_DATASET",
    "EVIDENCE_SCHEMA",
    "JOB_SCHEMA",
    "LIVE_EVIDENCE_SCHEMA",
    "LIVE_FIXTURE",
    "LIVE_OPERATION",
    "OPERATIONS",
    "RESOURCE_SCHEMA",
    "WEIGHTED_PATH_CAUSE",
    "WEIGHTED_PATH_READS",
    "GdcContractError",
    "SnbBiSuiteError",
    "assert_large_scale_factors_are_opt_in",
    "assert_separate_from_other_suites",
    "identity_path",
    "list_operation_rules",
    "map_operation_file",
    "run_live_bi2",
    "run_tiny_suite",
    "validate_live_fixture",
]
