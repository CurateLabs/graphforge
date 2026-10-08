"""GDC FinBench Transaction suite adapter (workload semantics).

Shares identity/acquisition contracts from ``gdc_contracts`` without embedding
those contracts' workload-free rules into operation mapping. Rust owns mapping,
validation modes, phase separation, reference validation, and the three-lane
failure model (correctness, resource, harness) via
``graphforge-benchmark-gdc-finbench-transaction``.

Results here are engineering evidence only. They never masquerade as an audited
GDC certification (the runner stamps ``certification: false``).
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
from graphforge_bench.gdc_measurement_policy import (
    GdcMeasurementBoundaryError,
    assert_live_diagnostic_boundary,
)

COMPLEX_READS = tuple(f"TCR{index}" for index in range(1, 13))
SIMPLE_READS = tuple(f"TSR{index}" for index in range(1, 7))
WRITES = tuple(f"TW{index}" for index in range(1, 20))
READ_WRITES = tuple(f"TRW{index}" for index in range(1, 4))
OPERATIONS = COMPLEX_READS + SIMPLE_READS + WRITES + READ_WRITES

JOB_SCHEMA = "graphforge-gdc-finbench-transaction-job/1"
EVIDENCE_SCHEMA = "graphforge-gdc-finbench-transaction-evidence/1"
LIVE_EXECUTION_MODE = "live_graphforge"
LIVE_DATASET_ID = "finbench-engineering-live-tcr10-v1"
LIVE_FIXTURE = "finbench-transaction-live"

WRITE_CAUSE = "finbench_transaction_write_semantics_not_exposed"
TRUNCATION_ORDER_CAUSE = "truncation_order_not_supported"

# Every read maps to exact public Cypher, truncation included; only writes and
# read-write transactions fail closed.
COMPATIBLE_READS = COMPLEX_READS + SIMPLE_READS

# Reads whose specification takes truncationLimit / truncationOrder.
TRUNCATED_READS = ("TCR1", "TCR2", "TCR5", "TCR6", "TCR7", "TCR8", "TCR9", "TCR11", "TCR12")
DEFAULT_TRUNCATION_LIMIT = 500
TRUNCATION_ORDER = "TIMESTAMP_DESCENDING"

QUERY_CATALOG_SCHEMA = "graphforge-gdc-finbench-query-catalog/1"
QUERY_EVIDENCE_SCHEMA = "graphforge-gdc-finbench-query-evidence/1"
QUERY_DATASET_ID = "finbench-engineering-queries-v1"
QUERY_FIXTURE = "finbench-transaction-queries"


class FinBenchTransactionSuiteError(ValueError):
    """FinBench Transaction suite mapping or validation failed."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def _enforce_measurement_boundary(evidence: dict[str, Any], *, label: str) -> None:
    try:
        assert_live_diagnostic_boundary(evidence, label=label)
    except GdcMeasurementBoundaryError as error:
        raise FinBenchTransactionSuiteError(error.cause, str(error)) from error


def identity_path(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "profiles" / "gdc" / "finbench-transaction-identity.json"


def runner_binary(root: Path | None = None) -> Path:
    base = root or workspace_root()
    override = os.environ.get("GRAPHFORGE_GDC_FINBENCH_TRANSACTION_BIN")
    if override:
        return Path(override)
    target = base / "target"
    for profile in ("debug", "release"):
        candidate = target / profile / "graphforge-benchmark-gdc-finbench-transaction"
        if candidate.is_file():
            return candidate
    raise FinBenchTransactionSuiteError(
        "missing_runner",
        "graphforge-benchmark-gdc-finbench-transaction binary not built; "
        "run cargo build -p graphforge-benchmark-gdc-finbench-transaction",
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
        raise FinBenchTransactionSuiteError("invalid_document", completed.stderr.strip())
    rules: dict[str, dict[str, str]] = {}
    for line in completed.stdout.splitlines():
        operation, _, rest = line.partition(" ")
        fields: dict[str, str] = {}
        for token in rest.split():
            key, _, value = token.partition("=")
            fields[key] = value
        if "category" not in fields or "validation" not in fields or "mapping" not in fields:
            raise FinBenchTransactionSuiteError("invalid_document", f"bad operation line: {line}")
        rules[operation] = fields
    if set(rules) != set(OPERATIONS):
        raise FinBenchTransactionSuiteError(
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
    """Run the bounded finbench-engineering-tiny-v1 Transaction suite through the Rust runner."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / "finbench-transaction-tiny" / fixture_name
    pin = load_pinned_identity(identity_path(base))
    acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
    # Provenance evidence from shared contracts (checksummed assets only).
    contract_evidence = validate_acquisition(pin, acquisition, fixture)
    identities = contract_evidence["identities"]
    with tempfile.TemporaryDirectory(prefix="gdc-finbench-transaction-") as tmp:
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
            raise FinBenchTransactionSuiteError(
                "invalid_document",
                f"runner failed to emit evidence: {completed.stderr.strip()}",
            )
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
        if evidence.get("schema") != EVIDENCE_SCHEMA:
            raise FinBenchTransactionSuiteError(
                "invalid_document",
                "unexpected finbench-transaction evidence schema",
            )
        _enforce_measurement_boundary(evidence, label="finbench-transaction evidence")
        if completed.returncode != 0 and fixture_name == "compatible":
            raise FinBenchTransactionSuiteError(
                "reference_mismatch",
                f"compatible fixture must pass: {completed.stderr.strip()}",
            )
        return evidence


def _raise_live_error(completed: subprocess.CompletedProcess[str]) -> None:
    message = completed.stderr.strip()
    if "reference_mismatch" in message:
        raise FinBenchTransactionSuiteError("correctness_failed", message)
    if "parameter" in message:
        raise FinBenchTransactionSuiteError("parameter_identity_mismatch", message)
    if "identity" in message:
        raise FinBenchTransactionSuiteError("identity_drift", message)
    if "checksum" in message:
        raise FinBenchTransactionSuiteError("checksum_mismatch", message)
    if "static" in message:
        raise FinBenchTransactionSuiteError("static_output_rejected", message)
    raise FinBenchTransactionSuiteError("invalid_document", message or "live runner failed")


def validate_live_fixture(
    fixture: Path,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Ask the trusted Rust runner to validate the complete closed live context."""
    completed = _run_runner(["validate-live-context", str(fixture)], root)
    if completed.returncode != 0:
        _raise_live_error(completed)
    return json.loads((fixture / "identity.json").read_text(encoding="utf-8"))


def run_live_suite(
    *,
    root: Path | None = None,
    evidence_path: Path | None = None,
    params_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Orchestrate trusted Rust-owned in-memory TCR10; never accept static output."""
    base = root or workspace_root()
    fixture = base / "fixtures" / "gdc" / LIVE_FIXTURE
    with tempfile.TemporaryDirectory(prefix="gdc-finbench-live-") as tmp:
        execution_fixture = fixture
        if params_override:
            execution_fixture = Path(tmp) / "fixture"
            shutil.copytree(fixture, execution_fixture)
            parameter_path = execution_fixture / "parameters.json"
            parameters = json.loads(parameter_path.read_text(encoding="utf-8"))
            for name, value in params_override.items():
                if name not in parameters["bindings"]:
                    raise FinBenchTransactionSuiteError(
                        "harness_error", f"unknown live parameter {name}"
                    )
                parameters["bindings"][name]["value"] = value
            parameter_path.write_text(json.dumps(parameters, indent=2) + "\n", encoding="utf-8")
        out_evidence = evidence_path or (Path(tmp) / "evidence.json")
        completed = _run_runner(["run-live", str(execution_fixture), str(out_evidence)], base)
        if not out_evidence.is_file():
            _raise_live_error(completed)
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
        if completed.returncode != 0:
            raise FinBenchTransactionSuiteError(
                evidence.get("status", "harness_error"),
                completed.stderr.strip() or "live execution failed",
            )
        if evidence.get("execution_mode") != LIVE_EXECUTION_MODE:
            raise FinBenchTransactionSuiteError(
                "static_output_rejected",
                "live lane did not prove live_graphforge execution",
            )
        _enforce_measurement_boundary(evidence, label="live finbench-transaction evidence")
        return evidence


def list_query_catalog(root: Path | None = None) -> dict[str, Any]:
    """The runner's read query definitions (Cypher, parameters, truncation) as data."""
    completed = _run_runner(["list-queries"], root)
    if completed.returncode != 0:
        raise FinBenchTransactionSuiteError("invalid_document", completed.stderr.strip())
    catalog = json.loads(completed.stdout)
    if catalog.get("schema") != QUERY_CATALOG_SCHEMA:
        raise FinBenchTransactionSuiteError("invalid_document", "unexpected query catalog schema")
    return catalog


def query_fixture_path(root: Path | None = None) -> Path:
    return (root or workspace_root()) / "fixtures" / "gdc" / QUERY_FIXTURE


def run_query_fixture(
    *,
    root: Path | None = None,
    fixture: Path | None = None,
    evidence_path: Path | None = None,
) -> dict[str, Any]:
    """Run every read live over the query fixture and validate against its expected rows."""
    source = fixture or query_fixture_path(root)
    with tempfile.TemporaryDirectory(prefix="gdc-finbench-queries-") as tmp:
        out_evidence = evidence_path or (Path(tmp) / "evidence.json")
        completed = _run_runner(["run-queries", str(source), str(out_evidence)], root)
        if not out_evidence.is_file():
            raise FinBenchTransactionSuiteError(
                "harness_error", completed.stderr.strip() or "query runner wrote no evidence"
            )
        evidence = json.loads(out_evidence.read_text(encoding="utf-8"))
    if evidence.get("schema") != QUERY_EVIDENCE_SCHEMA:
        raise FinBenchTransactionSuiteError("invalid_document", "unexpected query evidence schema")
    _enforce_measurement_boundary(evidence, label="finbench-transaction query evidence")
    if completed.returncode != 0:
        raise FinBenchTransactionSuiteError(
            evidence.get("status", "harness_error"),
            json.dumps([o for o in evidence.get("outcomes", []) if o.get("status") != "passed"]),
        )
    return evidence


def map_operation_file(path: Path, root: Path | None = None) -> dict[str, Any]:
    completed = _run_runner(["map-operation", str(path)], root)
    if completed.returncode == 3:
        raise FinBenchTransactionSuiteError("semantic_incompatibility", completed.stderr.strip())
    if completed.returncode != 0:
        raise FinBenchTransactionSuiteError("invalid_document", completed.stderr.strip())
    return json.loads(completed.stdout)


def assert_separate_from_other_suites(root: Path | None = None) -> None:
    """FinBench Transaction profiles/validation/evidence stay distinct from siblings."""
    base = root or workspace_root()
    suite = json.loads(
        (base / "suites" / "gdc-finbench-transaction.json").read_text(encoding="utf-8")
    )
    if suite.get("family") != "gdc" or suite.get("suite_id") != "finbench-transaction":
        raise FinBenchTransactionSuiteError(
            "invalid_document",
            "suite must remain a GDC FinBench Transaction suite",
        )
    rendered = json.dumps(suite)
    for foreign in ("graph500", "graphalytics", "snb-interactive", "snb-bi", "spb"):
        if foreign in rendered:
            raise FinBenchTransactionSuiteError(
                "invalid_document",
                f"FinBench Transaction suite must not embed {foreign}",
            )
    if suite.get("runner") != "gdc-finbench-transaction":
        raise FinBenchTransactionSuiteError(
            "invalid_document",
            "FinBench Transaction suite must use the gdc-finbench-transaction runner",
        )


__all__ = [
    "COMPATIBLE_READS",
    "COMPLEX_READS",
    "DEFAULT_TRUNCATION_LIMIT",
    "EVIDENCE_SCHEMA",
    "JOB_SCHEMA",
    "LIVE_DATASET_ID",
    "LIVE_EXECUTION_MODE",
    "LIVE_FIXTURE",
    "OPERATIONS",
    "QUERY_CATALOG_SCHEMA",
    "QUERY_DATASET_ID",
    "QUERY_EVIDENCE_SCHEMA",
    "QUERY_FIXTURE",
    "READ_WRITES",
    "SIMPLE_READS",
    "TRUNCATED_READS",
    "TRUNCATION_ORDER",
    "TRUNCATION_ORDER_CAUSE",
    "WRITES",
    "WRITE_CAUSE",
    "FinBenchTransactionSuiteError",
    "GdcContractError",
    "assert_separate_from_other_suites",
    "identity_path",
    "list_operation_rules",
    "list_query_catalog",
    "map_operation_file",
    "query_fixture_path",
    "run_live_suite",
    "run_query_fixture",
    "run_tiny_suite",
    "validate_live_fixture",
]
