"""Shared GDC identity, acquisition, and evidence contracts.

Suite adapters share these contracts without sharing workload semantics.
Bulk datasets are never committed; only checksummed provenance is validated.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

PIN_SCHEMA = "graphforge-gdc-pinned-identity/1"
ACQUISITION_SCHEMA = "graphforge-gdc-acquisition/1"
EVIDENCE_SCHEMA = "graphforge-gdc-suite-evidence/1"
SUITE_SCHEMA = "graphforge-benchmark-suite/1"

EXECUTABLE_SUITES = (
    "graphalytics",
    "snb-interactive",
    "snb-bi",
    "finbench-transaction",
)
INVENTORY_SUITES = ("spb",)
ALL_SUITES = EXECUTABLE_SUITES + INVENTORY_SUITES


class GdcContractError(ValueError):
    """Pinned identity or acquisition evidence is incomplete or contradictory."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_schema(name: str) -> Draft202012Validator:
    document = json.loads((workspace_root() / "schemas" / name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(document)
    return Draft202012Validator(document)


def _validate(validator: Draft202012Validator, document: Mapping[str, Any], label: str) -> None:
    error = next(validator.iter_errors(document), None)
    if error is not None:
        raise GdcContractError("invalid_document", f"{label}: {error.message}")


def _tool_key(identity: Mapping[str, Any] | None) -> tuple[Any, ...]:
    if identity is None:
        return ("null",)
    return (
        identity.get("name"),
        identity.get("source"),
        identity.get("release"),
        identity.get("commit"),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def load_pinned_identity(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise GdcContractError("invalid_document", "pinned identity must be an object")
    _validate(_load_schema("gdc-pinned-identity.json"), document, "pinned identity")
    if document["schema"] != PIN_SCHEMA:
        raise GdcContractError("invalid_document", "unexpected pinned identity schema")
    return document


def load_acquisition(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise GdcContractError("invalid_document", "acquisition must be an object")
    _validate(_load_schema("gdc-acquisition.json"), document, "acquisition")
    if document["schema"] != ACQUISITION_SCHEMA:
        raise GdcContractError("invalid_document", "unexpected acquisition schema")
    return document


def load_suite_declaration(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise GdcContractError("invalid_document", "suite declaration must be an object")
    _validate(_load_schema("gdc-suite-declaration.json"), document, "suite declaration")
    if document["schema"] != SUITE_SCHEMA:
        raise GdcContractError("invalid_document", "unexpected suite schema")
    return document


def _reject_incomplete_pin(pin: Mapping[str, Any]) -> None:
    required = ("suite_id", "disposition", "spec", "generator", "driver", "datasets", "references")
    missing = [name for name in required if name not in pin]
    if missing:
        raise GdcContractError(
            "incomplete_provenance",
            f"pinned identity missing fields: {', '.join(missing)}",
        )
    for tool_name in ("spec", "generator", "driver"):
        tool = pin[tool_name]
        if tool is None:
            continue
        if not isinstance(tool, Mapping):
            raise GdcContractError("incomplete_provenance", f"{tool_name} identity is incomplete")
        if not tool.get("name") or not tool.get("source"):
            raise GdcContractError("incomplete_provenance", f"{tool_name} identity is incomplete")
        if tool.get("release") is None and tool.get("commit") is None:
            raise GdcContractError(
                "incomplete_provenance",
                f"{tool_name} requires release or commit",
            )
    if pin["disposition"] == "executable":
        if not pin["datasets"]:
            raise GdcContractError("incomplete_provenance", "executable suite requires datasets")
        for dataset in pin["datasets"]:
            for field in ("id", "checksum_sha256", "license", "acquisition", "source"):
                if not dataset.get(field):
                    raise GdcContractError(
                        "incomplete_provenance",
                        f"dataset provenance incomplete: {dataset.get('id', '<unknown>')}",
                    )


def _evidence(
    *,
    suite_id: str,
    disposition: str,
    status: str,
    cause: str | None,
    identities: Mapping[str, Any],
    datasets: list[dict[str, Any]],
    references: list[dict[str, Any]],
) -> dict[str, Any]:
    document = {
        "schema": EVIDENCE_SCHEMA,
        "suite_id": suite_id,
        "disposition": disposition,
        "status": status,
        "cause": cause,
        "identities": {
            "spec": dict(identities["spec"]),
            "generator": None if identities["generator"] is None else dict(identities["generator"]),
            "driver": None if identities["driver"] is None else dict(identities["driver"]),
        },
        "datasets": datasets,
        "references": references,
    }
    _validate(_load_schema("gdc-suite-evidence.json"), document, "suite evidence")
    return document


def validate_acquisition(
    pin: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    asset_root: Path,
) -> dict[str, Any]:
    """Validate acquired inputs against a pinned identity without workload semantics."""
    _reject_incomplete_pin(pin)
    _validate(_load_schema("gdc-pinned-identity.json"), pin, "pinned identity")
    _validate(_load_schema("gdc-acquisition.json"), acquisition, "acquisition")

    if acquisition.get("suite_id") != pin["suite_id"]:
        raise GdcContractError("identity_drift", "acquisition suite_id drifted from pin")

    for label, pinned, recorded in (
        ("spec", pin["spec"], acquisition.get("recorded_spec")),
        ("generator", pin["generator"], acquisition.get("recorded_generator")),
        ("driver", pin["driver"], acquisition.get("recorded_driver")),
    ):
        if _tool_key(pinned) != _tool_key(
            recorded if isinstance(recorded, Mapping) or recorded is None else None
        ):
            raise GdcContractError("identity_drift", f"{label} identity drifted from pin")

    if pin["disposition"] == "inventory_only":
        if acquisition.get("assets") or acquisition.get("references"):
            raise GdcContractError(
                "invalid_document",
                "inventory-only suites must not acquire executable assets",
            )
        return _evidence(
            suite_id=pin["suite_id"],
            disposition=pin["disposition"],
            status="passed",
            cause=None,
            identities={
                "spec": pin["spec"],
                "generator": None,
                "driver": None,
            },
            datasets=[],
            references=[],
        )

    pinned_datasets = {item["id"]: item for item in pin["datasets"]}
    acquired_assets = {item["id"]: item for item in acquisition.get("assets", [])}
    missing = sorted(set(pinned_datasets) - set(acquired_assets))
    if missing:
        raise GdcContractError(
            "missing_assets",
            f"missing acquired datasets: {', '.join(missing)}",
        )
    unexpected = sorted(set(acquired_assets) - set(pinned_datasets))
    if unexpected:
        raise GdcContractError(
            "incomplete_provenance",
            f"acquired datasets not pinned: {', '.join(unexpected)}",
        )

    datasets_out: list[dict[str, Any]] = []
    for dataset_id, pinned in pinned_datasets.items():
        asset = acquired_assets[dataset_id]
        path = asset_root / asset["path"]
        if not path.is_file():
            raise GdcContractError("missing_assets", f"dataset file missing: {asset['path']}")
        digest = _sha256_file(path)
        if digest != pinned["checksum_sha256"] or digest != asset["checksum_sha256"]:
            raise GdcContractError(
                "checksum_mismatch",
                f"dataset checksum mismatch: {dataset_id}",
            )
        if asset.get("license") != pinned["license"]:
            raise GdcContractError(
                "incomplete_provenance",
                f"dataset license missing or drifted: {dataset_id}",
            )
        datasets_out.append(
            {
                "id": dataset_id,
                "checksum_sha256": digest,
                "license": pinned["license"],
            }
        )

    pinned_refs = {(item["dataset_id"], item["workload_key"]): item for item in pin["references"]}
    acquired_refs = {
        (item["dataset_id"], item["workload_key"]): item
        for item in acquisition.get("references", [])
    }
    missing_refs = sorted(set(pinned_refs) - set(acquired_refs))
    if missing_refs:
        rendered = ", ".join(f"{dataset}:{key}" for dataset, key in missing_refs)
        raise GdcContractError("reference_mismatch", f"missing references: {rendered}")
    unexpected_refs = sorted(set(acquired_refs) - set(pinned_refs))
    if unexpected_refs:
        rendered = ", ".join(f"{dataset}:{key}" for dataset, key in unexpected_refs)
        raise GdcContractError("reference_mismatch", f"unexpected references: {rendered}")

    references_out: list[dict[str, Any]] = []
    for key, pinned in pinned_refs.items():
        asset = acquired_refs[key]
        path = asset_root / asset["path"]
        if not path.is_file():
            raise GdcContractError("missing_assets", f"reference file missing: {asset['path']}")
        digest = _sha256_file(path)
        if digest != pinned["checksum_sha256"] or digest != asset["checksum_sha256"]:
            raise GdcContractError(
                "reference_mismatch",
                f"reference checksum mismatch: {key[0]}:{key[1]}",
            )
        references_out.append(
            {
                "dataset_id": key[0],
                "workload_key": key[1],
                "checksum_sha256": digest,
            }
        )

    return _evidence(
        suite_id=pin["suite_id"],
        disposition=pin["disposition"],
        status="passed",
        cause=None,
        identities={
            "spec": pin["spec"],
            "generator": pin["generator"],
            "driver": pin["driver"],
        },
        datasets=datasets_out,
        references=references_out,
    )


def list_gdc_suites(root: Path | None = None) -> tuple[dict[str, Any], ...]:
    base = root or workspace_root()
    by_id: dict[str, dict[str, Any]] = {}
    for path in (base / "suites").glob("gdc-*.json"):
        suite = load_suite_declaration(path)
        suite_id = suite["suite_id"]
        if suite_id in by_id:
            raise GdcContractError(
                "incomplete_provenance",
                f"duplicate GDC suite declaration: {suite_id}",
            )
        by_id[suite_id] = suite
    if set(by_id) != set(ALL_SUITES):
        raise GdcContractError(
            "incomplete_provenance",
            "GDC suite index must declare each suite independently exactly once",
        )
    return tuple(by_id[suite_id] for suite_id in ALL_SUITES)
