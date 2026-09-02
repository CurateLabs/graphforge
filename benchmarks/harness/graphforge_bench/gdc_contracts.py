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
STATUS_SCHEMA = "graphforge-gdc-suite-status/1"

# SPB is RDF/SPARQL. GraphForge's current product surface is property-graph +
# Cypher / analyst verbs, so the harness inventories SPB without approximating.
SPB_INVENTORY_REASON = "rdf_sparql_outside_property_graph_cypher_surface"
SPB_ACTIVATION_CRITERIA = (
    "product_exposes_supported_rdf_or_sparql_binding",
    "official_spb_spec_and_driver_pins_recorded",
    "reference_validation_path_exists_without_cypher_approximation",
)


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
    return ("identity", json.dumps(dict(identity), sort_keys=True, separators=(",", ":")))


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
        provenance_kind = tool.get("provenance_kind")
        if provenance_kind is not None:
            if pin.get("suite_id") != "snb-bi":
                raise GdcContractError(
                    "incomplete_provenance",
                    "content-addressed tool identity is restricted to the SNB BI synthetic fixture",
                )
            if provenance_kind not in {
                "content_addressed_synthetic",
                "repository_source",
            }:
                raise GdcContractError(
                    "incomplete_provenance",
                    f"{tool_name} provenance kind is unsupported",
                )
            if not tool.get("content_sha256") or not tool.get("content_description"):
                raise GdcContractError(
                    "incomplete_provenance",
                    f"{tool_name} content identity is incomplete",
                )
        elif tool.get("release") is None and tool.get("commit") is None:
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


def suite_identity_profiles(suite: Mapping[str, Any]) -> dict[str, str]:
    """Return declared identity-profile paths, or an empty map for single-pin suites."""
    profiles = suite.get("identity_profiles")
    if profiles is None:
        return {}
    if not isinstance(profiles, Mapping) or not profiles:
        raise GdcContractError("invalid_document", "identity_profiles must be a non-empty object")
    resolved: dict[str, str] = {}
    for key, value in profiles.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise GdcContractError("invalid_document", "identity_profiles entries must be strings")
        resolved[key] = value
    paths = list(resolved.values())
    if len(paths) != len(set(paths)):
        raise GdcContractError(
            "incomplete_provenance",
            "identity_profiles must use distinct pin paths",
        )
    if suite["pinned_identity"] not in paths:
        raise GdcContractError(
            "identity_drift",
            "suite pinned_identity is not one of the declared identity_profiles",
        )
    return resolved


def resolve_pinned_identity(
    suite: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    root: Path | None = None,
) -> dict[str, Any]:
    """Select the pin the suite and acquisition jointly name."""
    base = root or workspace_root()
    profiles = suite_identity_profiles(suite)
    selected = acquisition.get("identity_profile")
    if profiles:
        if not isinstance(selected, str) or not selected:
            raise GdcContractError(
                "incomplete_provenance",
                "acquisition must select a suite identity_profile",
            )
        if selected not in profiles:
            raise GdcContractError(
                "identity_drift",
                f"acquisition identity_profile {selected!r} is not declared by the suite",
            )
        pin_path = profiles[selected]
    else:
        if selected is not None:
            raise GdcContractError(
                "identity_drift",
                "acquisition selected an identity_profile the suite does not declare",
            )
        pin_path = suite["pinned_identity"]
    pin = load_pinned_identity(base / pin_path)
    if pin["suite_id"] != suite["suite_id"]:
        raise GdcContractError(
            "identity_drift",
            "resolved pin suite_id drifted from suite declaration",
        )
    if pin["disposition"] != suite["disposition"]:
        raise GdcContractError(
            "identity_drift",
            "resolved pin disposition drifted from suite declaration",
        )
    return pin


def validate_suite_acquisition(
    suite: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    asset_root: Path,
    root: Path | None = None,
) -> dict[str, Any]:
    """Resolve the suite/acquisition identity profile, then validate assets."""
    pin = resolve_pinned_identity(suite, acquisition, root)
    return validate_acquisition(pin, acquisition, asset_root)


def _assert_pin_matches_suite(suite: Mapping[str, Any], pin: Mapping[str, Any]) -> None:
    if pin["suite_id"] != suite["suite_id"] or pin["disposition"] != suite["disposition"]:
        raise GdcContractError(
            "identity_drift",
            f"pinned identity drifted from suite declaration: {suite['suite_id']}",
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
        profiles = suite_identity_profiles(suite)
        pin_paths = {suite["pinned_identity"], *profiles.values()}
        for pin_path in pin_paths:
            _assert_pin_matches_suite(suite, load_pinned_identity(base / pin_path))
        by_id[suite_id] = suite
    if set(by_id) != set(ALL_SUITES):
        raise GdcContractError(
            "incomplete_provenance",
            "GDC suite index must declare each suite independently exactly once",
        )
    return tuple(by_id[suite_id] for suite_id in ALL_SUITES)


def suite_status(suite_id: str, root: Path | None = None) -> dict[str, Any]:
    """Report a suite disposition without inventing an incompatible runner."""
    if suite_id not in ALL_SUITES:
        raise GdcContractError("invalid_document", f"unknown GDC suite: {suite_id}")
    suites = {suite["suite_id"]: suite for suite in list_gdc_suites(root)}
    suite = suites[suite_id]
    pin = load_pinned_identity((root or workspace_root()) / suite["pinned_identity"])
    if pin["disposition"] != suite["disposition"]:
        raise GdcContractError(
            "identity_drift",
            f"suite disposition drifted from pinned identity: {suite_id}",
        )
    if suite["disposition"] == "inventory_only":
        if suite_id != "spb":
            raise GdcContractError(
                "invalid_document",
                f"unexpected inventory-only suite: {suite_id}",
            )
        document = {
            "schema": STATUS_SCHEMA,
            "suite_id": suite_id,
            "disposition": "inventory_only",
            "executable": False,
            "reason": SPB_INVENTORY_REASON,
            "activation_criteria": list(SPB_ACTIVATION_CRITERIA),
            "pinned_identity": suite["pinned_identity"],
        }
    else:
        document = {
            "schema": STATUS_SCHEMA,
            "suite_id": suite_id,
            "disposition": "executable",
            "executable": True,
            "reason": None,
            "activation_criteria": [],
            "pinned_identity": suite["pinned_identity"],
        }
    _validate(_load_schema("gdc-suite-status.json"), document, "suite status")
    return document


def assert_no_executable_spb_profile(root: Path | None = None) -> None:
    """Fail closed if an executable SPB profile is advertised."""
    base = root or workspace_root()
    status = suite_status("spb", base)
    if status["executable"] or status["disposition"] != "inventory_only":
        raise GdcContractError("invalid_document", "SPB must remain inventory-only")
    for path in (base / "profiles").rglob("*spb*.json"):
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise GdcContractError("invalid_document", f"invalid SPB profile: {path.name}")
        if document.get("schema") != PIN_SCHEMA:
            raise GdcContractError(
                "invalid_document",
                f"executable SPB profile advertised: {path.relative_to(base)}",
            )
        if document.get("disposition") != "inventory_only":
            raise GdcContractError(
                "invalid_document",
                f"executable SPB profile advertised: {path.relative_to(base)}",
            )
        if document.get("generator") is not None or document.get("driver") is not None:
            raise GdcContractError(
                "invalid_document",
                f"executable SPB tooling advertised: {path.relative_to(base)}",
            )
        if document.get("datasets") or document.get("references"):
            raise GdcContractError(
                "invalid_document",
                f"executable SPB assets advertised: {path.relative_to(base)}",
            )
    for path in (base / "suites").glob("*spb*.json"):
        suite = load_suite_declaration(path)
        if suite["disposition"] != "inventory_only" or suite.get("datasets"):
            raise GdcContractError(
                "invalid_document",
                f"executable SPB suite advertised: {path.name}",
            )
