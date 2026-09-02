"""Adapt two complete progressive rungs into closed #951 storage qualification."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import hashlib
from itertools import pairwise
import json
from pathlib import Path
import re
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from graphforge_bench.progressive_run import publish_json_no_clobber

BENCHMARK_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_ROOT = BENCHMARK_ROOT.parent
RUNG_SCHEMA = BENCHMARK_ROOT / "schemas/progressive-qualification-rung-evidence.json"
QUALIFICATION_SCHEMA = (
    REPOSITORY_ROOT / "docs/development/evidence/g500-ladder-qualification.schema.json"
)
PROVIDER_RESULT_SCHEMA = BENCHMARK_ROOT / "schemas/progressive-provider-run-result.json"
PROVIDER_PLAN_SCHEMA = BENCHMARK_ROOT / "schemas/progressive-provider-run-plan.json"
ASSEMBLY_CONTRACT = "graphforge-progressive-rung-assembly/2"
QUALIFICATION_CONTRACT = "graphforge-g500-ladder-qualification/3"
S26_EDGES = 1 << 30
S26_NODES = 1 << 26
SOURCE_CATEGORIES = (
    ("canonical_node_topology", "topology_nodes", "storage_owned_snapshot"),
    ("canonical_edge_topology", "topology_edges", "storage_owned_snapshot"),
    ("properties", "properties", "storage_owned_snapshot"),
    ("uuid_surrogate_indexes", "uuid_and_surrogates", "storage_owned_snapshot"),
    ("adjacency_csr", "adjacency", "storage_owned_snapshot"),
    ("catalog_manifests", "catalog_and_manifests", "storage_owned_snapshot"),
)
STORAGE_CATEGORIES = (
    "topology_nodes",
    "topology_edges",
    "properties",
    "uuid_and_surrogates",
    "adjacency",
    "catalog_and_manifests",
    "construction_staging",
    "portable_package",
    "clean_imported_project",
    "other",
)
STORAGE_FIELDS = (
    "logical_references",
    "logical_bytes",
    "physical_objects",
    "physical_logical_bytes",
    "allocated_bytes",
)
APPLICATION_IO_PHASES = (
    "append_merge",
    "seal_authentication",
    "shape_consume_reauthentication",
    "encode_write_postwrite_authentication",
    "publication_preauthentication",
    "cas_install_read_write",
    "hydration_verification",
    "fsync_synchronization",
    "recovery_reauthentication",
)
APPLICATION_IO_FIELDS = (
    "read_bytes",
    "write_bytes",
    "read_calls",
    "write_calls",
    "object_count",
    "block_count",
    "fsync_calls",
)
ADJACENT_SOURCES = ((20, 22), (22, 24))
COMMIT = re.compile(r"^[0-9a-f]{40}$")
HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
IMAGE_DIGEST = re.compile(r"^registry\.fly\.io/[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$")
FORBIDDEN_KEY = re.compile(
    r"(?:secret|credential|token|password|host_path|absolute_path|machine[_-]?id|"
    r"volume[_-]?id|provider_resource_id)",
    re.I,
)
ABSOLUTE_PATH = re.compile(r"(?:^|[\s=:])(?:/|[A-Za-z]:[\\/])")
RAW_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)


class StorageQualificationError(ValueError):
    """Input evidence or a derived qualification is incomplete or contradictory."""


def reject_unsanitized(value: Any, trail: str = "$") -> None:
    """Reject path, identity, and credential material recursively."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str) or FORBIDDEN_KEY.search(key):
                raise StorageQualificationError(f"sensitive evidence key at {trail}.{key}")
            reject_unsanitized(child, f"{trail}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_unsanitized(child, f"{trail}[{index}]")
    elif isinstance(value, str):
        if ABSOLUTE_PATH.search(value):
            raise StorageQualificationError(f"absolute host path at {trail}")
        if RAW_UUID.fullmatch(value):
            raise StorageQualificationError(f"raw UUID at {trail}")


def _schema(path: Path, value: Mapping[str, Any], label: str) -> None:
    try:
        contract = json.loads(path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(contract)
        Draft202012Validator(contract).validate(value)
    except (OSError, json.JSONDecodeError, SchemaError) as error:
        raise StorageQualificationError(f"committed {label} schema is invalid: {error}") from error
    except ValidationError as error:
        location = ".".join(str(part) for part in error.absolute_path) or "$"
        raise StorageQualificationError(
            f"{label} schema violation at {location}: {error.message}"
        ) from error


def _integer(value: Any, label: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < (1 if positive else 0):
        raise StorageQualificationError(f"{label} must be an exact nonnegative integer")
    return value


def _validate_snapshot(snapshot: Mapping[str, Any], label: str) -> None:
    categories = snapshot["categories"]
    if set(categories) != set(STORAGE_CATEGORIES):
        raise StorageQualificationError(f"{label} categories must be complete and unique")
    sums = dict.fromkeys(STORAGE_FIELDS, 0)
    for name in STORAGE_CATEGORIES:
        row = categories[name]
        if set(row) != set(STORAGE_FIELDS):
            raise StorageQualificationError(f"{label} category is malformed: {name}")
        for field in STORAGE_FIELDS:
            sums[field] += _integer(row[field], f"{label}.{name}.{field}")
        if row["physical_objects"] > row["logical_references"]:
            raise StorageQualificationError(
                f"{label} physical identities exceed logical references"
            )
    expected = {
        "logical_references": sums["logical_references"],
        "logical_bytes": sums["logical_bytes"],
        "retained_logical_eof_bytes": sums["physical_logical_bytes"],
        "allocated_physical_bytes": sums["allocated_bytes"],
        "physical_objects": sums["physical_objects"],
    }
    if any(snapshot[name] != value for name, value in expected.items()):
        raise StorageQualificationError(f"{label} category totals do not reconcile")
    other = categories["other"]
    if any(other[field] != 0 for field in STORAGE_FIELDS):
        raise StorageQualificationError(f"{label} contains unclassified storage")


def _validate_application_io(application_io: Mapping[str, Any]) -> None:
    phases = application_io["phases"]
    totals = application_io["totals"]
    if set(phases) != set(APPLICATION_IO_PHASES):
        raise StorageQualificationError("application I/O phases must be complete and unique")
    sums = dict.fromkeys(APPLICATION_IO_FIELDS, 0)
    for name in APPLICATION_IO_PHASES:
        phase = phases[name]
        if set(phase) != set(APPLICATION_IO_FIELDS):
            raise StorageQualificationError(f"application I/O phase is malformed: {name}")
        for field in APPLICATION_IO_FIELDS:
            sums[field] += _integer(phase[field], f"application_io.{name}.{field}")
        if (phase["read_bytes"] == 0) != (phase["read_calls"] == 0):
            raise StorageQualificationError("application I/O read bytes and calls disagree")
        if (phase["write_bytes"] == 0) != (phase["write_calls"] == 0):
            raise StorageQualificationError("application I/O write bytes and calls disagree")
    if totals != sums:
        raise StorageQualificationError("application I/O totals do not reconcile")


def validate_source_rung(value: Mapping[str, Any]) -> None:
    """Validate one current, complete controller-produced rung."""
    reject_unsanitized(value)
    _schema(RUNG_SCHEMA, value, "progressive rung")
    if value.get("assembly_contract") != ASSEMBLY_CONTRACT:
        raise StorageQualificationError("historical rung assembly is not adapter authority")
    if value.get("status") != "passed" or value.get("correctness") is not True:
        raise StorageQualificationError("storage qualification requires passed correct rungs")
    storage = value["storage_attribution"]
    _validate_snapshot(storage["source"], "source")
    _validate_snapshot(storage["imported"], "imported")
    construction = storage["construction"]
    _validate_application_io(construction["application_io"])
    staging = construction["staging"]
    for field in STORAGE_FIELDS:
        _integer(staging[field], f"construction.staging.{field}")
    staging_peak = _integer(
        construction["staging_transient_peak_allocated_bytes"],
        "construction.staging_transient_peak_allocated_bytes",
    )
    total_peak = _integer(
        construction["transient_peak_allocated_bytes"],
        "construction.transient_peak_allocated_bytes",
    )
    if (
        staging["physical_objects"] > staging["logical_references"]
        or staging_peak < staging["allocated_bytes"]
        or total_peak < staging_peak
    ):
        raise StorageQualificationError("construction staging authority contradicts its peak")
    counts = storage["counts"]
    scale = _integer(value["scale"], "scale", positive=True)
    if (
        scale not in {20, 22, 24}
        or value.get("source") != "canonical_ladder"
        or value.get("profile_id") != f"graph500-s{scale}-provider"
    ):
        raise StorageQualificationError(
            "storage qualification requires the canonical provider source/profile"
        )
    nodes = 1 << scale
    edges = 16 * nodes
    if (
        counts["source_nodes"] != nodes
        or counts["source_edges"] != edges
        or counts["imported_nodes"] != nodes
        or counts["imported_edges"] != edges
        or value["live_edges"] != edges
    ):
        raise StorageQualificationError("authoritative counts contradict rung scale")


def _artifact(
    category: str,
    totals: Mapping[str, Any],
    source: str,
    *,
    transient_peak: int | None = None,
) -> dict[str, Any]:
    allocated = totals["allocated_bytes"]
    return {
        "category": category,
        "logical_bytes": totals["logical_bytes"],
        "allocated_bytes": allocated,
        "current_retained_bytes": allocated,
        "transient_peak_allocated_bytes": (allocated if transient_peak is None else transient_peak),
        "logical_references": totals["logical_references"],
        "physical_objects": totals["physical_objects"],
        "source": source,
    }


def adapt_rung(value: Mapping[str, Any]) -> dict[str, Any]:
    """Convert one validated progressive rung without rounding any observation."""
    validate_source_rung(value)
    storage = value["storage_attribution"]
    source = storage["source"]
    imported = storage["imported"]
    construction = storage["construction"]
    portable = storage["portable_package"]
    lifecycle = storage["lifecycle"]
    counts = storage["counts"]
    artifacts = [
        _artifact(name, source["categories"][category], owner)
        for name, category, owner in SOURCE_CATEGORIES
    ]
    artifacts.append(
        _artifact(
            "construction_staging_spill",
            construction["staging"],
            "construction_receipts",
            transient_peak=construction["staging_transient_peak_allocated_bytes"],
        )
    )
    artifacts.append(
        {
            "category": "portable_package",
            "logical_bytes": portable["allocation_logical_bytes"],
            "allocated_bytes": portable["allocation_allocated_bytes"],
            "current_retained_bytes": portable["allocation_allocated_bytes"],
            "transient_peak_allocated_bytes": portable["allocation_allocated_bytes"],
            # Each writer-owned package object is one retained package reference.
            "logical_references": portable["allocation_physical_objects"],
            "physical_objects": portable["allocation_physical_objects"],
            "source": "exact_descriptor",
        }
    )
    artifacts.append(
        {
            "category": "clean_imported_project",
            "logical_bytes": imported["logical_bytes"],
            "allocated_bytes": imported["allocated_physical_bytes"],
            "current_retained_bytes": imported["allocated_physical_bytes"],
            "transient_peak_allocated_bytes": imported["allocated_physical_bytes"],
            "logical_references": imported["logical_references"],
            "physical_objects": imported["physical_objects"],
            "source": "clean_import_snapshot",
        }
    )
    phases = []
    for name in APPLICATION_IO_PHASES:
        counters = construction["application_io"]["phases"][name]
        phases.append(
            {
                "phase": name,
                "applicable": any(counters[field] != 0 for field in APPLICATION_IO_FIELDS),
                **counters,
            }
        )
    totals = {
        "logical_bytes": sum(item["logical_bytes"] for item in artifacts),
        "allocated_bytes": sum(item["allocated_bytes"] for item in artifacts),
        "current_retained_bytes": lifecycle["retained_storage_bytes"],
        "transient_peak_allocated_bytes": lifecycle["transient_peak_storage_bytes"],
        **{
            f"phase_{field}": sum(phase[field] for phase in phases)
            for field in APPLICATION_IO_FIELDS
        },
    }
    by_category = {item["category"]: item for item in artifacts}
    live_nodes = counts["source_nodes"]
    live_edges = counts["source_edges"]
    return {
        "id": f"S{value['scale']}",
        "scale": value["scale"],
        "live_nodes": live_nodes,
        "live_edges": live_edges,
        "source_project_current_allocated_bytes": lifecycle[
            "source_project_current_allocated_bytes"
        ],
        "workspace_current_allocated_bytes": lifecycle["retained_storage_bytes"],
        "artifacts": artifacts,
        "phases": phases,
        "totals": totals,
        "ratios": {
            "canonical_node_bytes_per_live_node": {
                "numerator_bytes": by_category["canonical_node_topology"]["logical_bytes"],
                "denominator_count": live_nodes,
            },
            "canonical_edge_bytes_per_live_edge": {
                "numerator_bytes": by_category["canonical_edge_topology"]["logical_bytes"],
                "denominator_count": live_edges,
            },
            "authoritative_project_bytes_per_live_edge": {
                "numerator_bytes": lifecycle["source_project_current_allocated_bytes"],
                "denominator_count": live_edges,
            },
            "full_lifecycle_peak_bytes_per_live_edge": {
                "numerator_bytes": lifecycle["transient_peak_storage_bytes"],
                "denominator_count": live_edges,
            },
        },
    }


def ceil_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise StorageQualificationError("projection denominator must be positive")
    return (numerator + denominator - 1) // denominator


def _read_digest(path: Path) -> tuple[Mapping[str, Any], str]:
    try:
        encoded = path.read_bytes()
        value = json.loads(encoded)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StorageQualificationError(f"invalid evidence document: {path.name}") from error
    if not isinstance(value, Mapping):
        raise StorageQualificationError(f"evidence root must be an object: {path.name}")
    return value, hashlib.sha256(encoded).hexdigest()


def _read_anchored_result(path: Path, expected_sha256: str) -> Mapping[str, Any]:
    if HEX_DIGEST.fullmatch(expected_sha256) is None:
        raise StorageQualificationError("provider result anchor must be a SHA-256 digest")
    try:
        encoded = path.read_bytes()
    except OSError as error:
        raise StorageQualificationError("anchored provider result is unavailable") from error
    if hashlib.sha256(encoded).hexdigest() != expected_sha256:
        raise StorageQualificationError("provider result does not match its external anchor")
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StorageQualificationError("anchored provider result is malformed") from error
    if not isinstance(value, Mapping):
        raise StorageQualificationError("anchored provider result must be an object")
    return value


def _bound_provider_rung(
    path: Path,
    *,
    provider_result_sha256: str,
    expected_commit: str,
    expected_image_digest: str,
) -> Mapping[str, Any]:
    rung, rung_digest = _read_digest(path)
    validate_source_rung(rung)
    scale = int(rung["scale"])
    base = path.parent
    result = _read_anchored_result(base / f"s{scale}-result.json", provider_result_sha256)
    plan, plan_digest = _read_digest(base / f"s{scale}-plan.json")
    _schema(PROVIDER_RESULT_SCHEMA, result, "provider result")
    _schema(PROVIDER_PLAN_SCHEMA, plan, "provider execution plan")
    profile_path = BENCHMARK_ROOT / "profiles" / "graph500" / f"s{scale}-provider.json"
    try:
        profile_digest = hashlib.sha256(profile_path.read_bytes()).hexdigest()
    except OSError as error:
        raise StorageQualificationError("canonical provider profile is unavailable") from error
    identities = result.get("identities")
    if (
        result.get("status") != "passed"
        or result.get("rung") != f"S{scale}"
        or not isinstance(identities, Mapping)
        or identities.get("commit") != expected_commit
        or identities.get("profile_id") != f"graph500-s{scale}-provider"
        or identities.get("profile_sha256") != profile_digest
        or identities.get("image_digest") != expected_image_digest
        or plan.get("rung") != f"S{scale}"
        or plan.get("identities") != identities
    ):
        raise StorageQualificationError(
            "provider rung is not bound to the expected commit/profile/image"
        )
    artifacts = result.get("artifacts")
    artifact_paths = {
        "plan_sha256": base / f"s{scale}-plan.json",
        "benchexec_sha256": base / f"s{scale}-benchexec.json",
        "graphforge_sha256": base / f"s{scale}-graphforge.json",
        "rung_sha256": path,
    }
    try:
        expected_artifacts = {
            name: hashlib.sha256(artifact.read_bytes()).hexdigest()
            for name, artifact in artifact_paths.items()
        }
    except OSError as error:
        raise StorageQualificationError("provider rung bundle is incomplete") from error
    if (
        not isinstance(artifacts, Mapping)
        or dict(artifacts) != expected_artifacts
        or plan_digest != expected_artifacts["plan_sha256"]
        or rung_digest != expected_artifacts["rung_sha256"]
    ):
        raise StorageQualificationError(
            "provider rung bundle artifacts do not match the provider result"
        )
    return rung


def build(
    source_paths: Sequence[Path],
    *,
    provider_result_sha256: Sequence[str],
    expected_commit: str,
    expected_image_digest: str,
    volume_bytes: int,
    reserved_headroom_bytes: int,
) -> dict[str, Any]:
    """Build from two provider-result-bound rung bundle paths."""
    if COMMIT.fullmatch(expected_commit) is None:
        raise StorageQualificationError("expected commit must be a full Git object ID")
    if IMAGE_DIGEST.fullmatch(expected_image_digest) is None:
        raise StorageQualificationError("expected image must be an immutable Fly OCI digest")
    if len(source_paths) != 2:
        raise StorageQualificationError("exactly two adjacent observations are required")
    if len(provider_result_sha256) != 2:
        raise StorageQualificationError("exactly two ordered provider result anchors are required")
    if any(HEX_DIGEST.fullmatch(digest) is None for digest in provider_result_sha256):
        raise StorageQualificationError("provider result anchor must be a SHA-256 digest")
    source_rungs = [
        _bound_provider_rung(
            path,
            provider_result_sha256=digest,
            expected_commit=expected_commit,
            expected_image_digest=expected_image_digest,
        )
        for path, digest in zip(source_paths, provider_result_sha256, strict=True)
    ]
    return _build_qualification(
        source_rungs,
        volume_bytes=volume_bytes,
        reserved_headroom_bytes=reserved_headroom_bytes,
    )


def _build_qualification(
    source_rungs: Sequence[Mapping[str, Any]],
    *,
    volume_bytes: int,
    reserved_headroom_bytes: int,
) -> dict[str, Any]:
    """Build and semantically validate one exact `/3` qualification."""
    if len(source_rungs) != 2:
        raise StorageQualificationError("exactly two adjacent observations are required")
    volume = _integer(volume_bytes, "volume_bytes", positive=True)
    reserved = _integer(reserved_headroom_bytes, "reserved_headroom_bytes")
    rungs = [adapt_rung(value) for value in source_rungs]
    scales = tuple(item["scale"] for item in rungs)
    if scales not in ADJACENT_SOURCES:
        raise StorageQualificationError("rungs must be ordered adjacent S20/S22 or S22/S24")
    low, high = rungs
    delta_bytes = (
        high["totals"]["transient_peak_allocated_bytes"]
        - low["totals"]["transient_peak_allocated_bytes"]
    )
    delta_edges = high["live_edges"] - low["live_edges"]
    ratio_num = high["totals"]["transient_peak_allocated_bytes"]
    ratio_den = high["live_edges"]
    if delta_bytes > 0 and delta_bytes * ratio_den > ratio_num * delta_edges:
        ratio_num, ratio_den = delta_bytes, delta_edges
    projected_peak = ceil_ratio(ratio_num * S26_EDGES, ratio_den)
    latest = {item["category"]: item for item in high["artifacts"]}
    projected_nodes = ceil_ratio(
        latest["canonical_node_topology"]["current_retained_bytes"] * S26_NODES,
        high["live_nodes"],
    )
    projected_edges = ceil_ratio(
        latest["canonical_edge_topology"]["current_retained_bytes"] * S26_EDGES,
        high["live_edges"],
    )
    headroom = max(0, volume - projected_peak)
    qualification = {
        "schema": QUALIFICATION_CONTRACT,
        "rungs": rungs,
        "projection": {
            "target": "S26",
            "source_rungs": [low["id"], high["id"]],
            "rate": {
                "numerator_bytes": ratio_num,
                "denominator_count": ratio_den,
            },
            "projected_canonical_node_bytes": projected_nodes,
            "projected_canonical_edge_bytes": projected_edges,
            "projected_lifecycle_peak_bytes": projected_peak,
            "volume_bytes": volume,
            "reserved_headroom_bytes": reserved,
            "headroom_bytes": headroom,
            "decision": (
                "admit" if projected_peak <= volume and headroom >= reserved else "refuse"
            ),
        },
    }
    validate(qualification)
    return qualification


def validate(evidence: Mapping[str, Any]) -> None:
    """Apply the committed schema and the retired semantic reconciliation rules."""
    reject_unsanitized(evidence)
    _schema(QUALIFICATION_SCHEMA, evidence, "ladder qualification")
    rungs = evidence["rungs"]
    if len(rungs) != 2:
        raise StorageQualificationError("exactly two adjacent observations are required")
    scales = tuple(rung["scale"] for rung in rungs)
    if scales not in ADJACENT_SOURCES:
        raise StorageQualificationError("rungs must be ordered adjacent S20/S22 or S22/S24")
    for rung in rungs:
        if rung["id"] != f"S{rung['scale']}":
            raise StorageQualificationError("rung id and scale disagree")
        categories = [artifact["category"] for artifact in rung["artifacts"]]
        if len(categories) != len(set(categories)):
            raise StorageQualificationError("artifact categories must be complete and unique")
        phase_names = [phase["phase"] for phase in rung["phases"]]
        if set(phase_names) != set(APPLICATION_IO_PHASES) or len(phase_names) != len(
            set(phase_names)
        ):
            raise StorageQualificationError("application I/O phases must be complete and unique")
        for phase in rung["phases"]:
            observed = any(phase[field] != 0 for field in APPLICATION_IO_FIELDS)
            if phase["applicable"] != observed:
                raise StorageQualificationError(
                    "phase applicability contradicts source-owned counters"
                )
            if (phase["read_bytes"] == 0) != (phase["read_calls"] == 0):
                raise StorageQualificationError("phase read bytes and calls disagree")
            if (phase["write_bytes"] == 0) != (phase["write_calls"] == 0):
                raise StorageQualificationError("phase write bytes and calls disagree")
        if any(
            artifact["physical_objects"] > artifact["logical_references"]
            for artifact in rung["artifacts"]
        ):
            raise StorageQualificationError(
                "physical identities must be deduplicated from logical references"
            )
        expected_totals = {
            "logical_bytes": sum(item["logical_bytes"] for item in rung["artifacts"]),
            "allocated_bytes": sum(item["allocated_bytes"] for item in rung["artifacts"]),
            **{
                f"phase_{field}": sum(phase[field] for phase in rung["phases"])
                for field in APPLICATION_IO_FIELDS
            },
        }
        if any(rung["totals"][name] != value for name, value in expected_totals.items()):
            raise StorageQualificationError("artifact or phase totals do not reconcile")
        retained = rung["totals"]["current_retained_bytes"]
        transient = rung["totals"]["transient_peak_allocated_bytes"]
        retained_views = sum(item["current_retained_bytes"] for item in rung["artifacts"])
        if retained != rung["workspace_current_allocated_bytes"]:
            raise StorageQualificationError(
                "workspace numerator disagrees with retained identity union"
            )
        if transient < max(item["transient_peak_allocated_bytes"] for item in rung["artifacts"]):
            raise StorageQualificationError("lifecycle peak is below a category peak")
        if retained > retained_views or retained < max(
            item["current_retained_bytes"] for item in rung["artifacts"]
        ):
            raise StorageQualificationError(
                "native retained union is inconsistent with owner views"
            )
        if any(
            item["current_retained_bytes"] > item["allocated_bytes"] for item in rung["artifacts"]
        ):
            raise StorageQualificationError("retained allocation exceeds category allocation")
        if transient < retained:
            raise StorageQualificationError("lifecycle peak is below current retained allocation")
        source_project = rung["source_project_current_allocated_bytes"]
        selected_source = sum(
            item["allocated_bytes"]
            for item in rung["artifacts"]
            if item["source"] == "storage_owned_snapshot"
        )
        if source_project < selected_source or source_project > retained:
            raise StorageQualificationError("source project union is inconsistent")
        nodes = rung["live_nodes"]
        edges = rung["live_edges"]
        if nodes != 1 << rung["scale"] or not 0 < edges <= nodes * 16:
            raise StorageQualificationError("live denominators contradict the Graph500 envelope")
        by_category = {item["category"]: item for item in rung["artifacts"]}
        ratios = {
            "canonical_node_bytes_per_live_node": {
                "numerator_bytes": by_category["canonical_node_topology"]["logical_bytes"],
                "denominator_count": nodes,
            },
            "canonical_edge_bytes_per_live_edge": {
                "numerator_bytes": by_category["canonical_edge_topology"]["logical_bytes"],
                "denominator_count": edges,
            },
            "authoritative_project_bytes_per_live_edge": {
                "numerator_bytes": source_project,
                "denominator_count": edges,
            },
            "full_lifecycle_peak_bytes_per_live_edge": {
                "numerator_bytes": transient,
                "denominator_count": edges,
            },
        }
        if rung["ratios"] != ratios:
            raise StorageQualificationError("ratios must preserve exact reproducible denominators")
    rate = evidence["projection"]["rate"]
    numerator = rate["numerator_bytes"]
    denominator = rate["denominator_count"]
    for low, high in pairwise(rungs):
        delta_edges = high["live_edges"] - low["live_edges"]
        delta_bytes = (
            high["totals"]["transient_peak_allocated_bytes"]
            - low["totals"]["transient_peak_allocated_bytes"]
        )
        if delta_edges <= 0:
            raise StorageQualificationError("live-edge denominator must increase")
        if delta_bytes > 0 and numerator * delta_edges < delta_bytes * denominator:
            raise StorageQualificationError(
                "projection rate is below the observed adjacent-rung slope"
            )
        if (
            numerator * high["live_edges"]
            < high["totals"]["transient_peak_allocated_bytes"] * denominator
        ):
            raise StorageQualificationError(
                "projection rate is below the latest observed peak ratio"
            )
    projection = evidence["projection"]
    projected = ceil_ratio(numerator * S26_EDGES, denominator)
    if projection["source_rungs"] != [rungs[0]["id"], rungs[1]["id"]]:
        raise StorageQualificationError("projection must cite both adjacent source rungs")
    if projection["projected_lifecycle_peak_bytes"] != projected:
        raise StorageQualificationError("S26 projected peak is not reproducible")
    latest = {item["category"]: item for item in rungs[-1]["artifacts"]}
    if projection["projected_canonical_node_bytes"] != ceil_ratio(
        latest["canonical_node_topology"]["current_retained_bytes"] * S26_NODES,
        rungs[-1]["live_nodes"],
    ):
        raise StorageQualificationError("S26 canonical node projection is not reproducible")
    if projection["projected_canonical_edge_bytes"] != ceil_ratio(
        latest["canonical_edge_topology"]["current_retained_bytes"] * S26_EDGES,
        rungs[-1]["live_edges"],
    ):
        raise StorageQualificationError("S26 canonical edge projection is not reproducible")
    expected_headroom = max(0, projection["volume_bytes"] - projected)
    if projection["headroom_bytes"] != expected_headroom:
        raise StorageQualificationError("headroom does not reconcile")
    expected_decision = (
        "admit"
        if projected <= projection["volume_bytes"]
        and expected_headroom >= projection["reserved_headroom_bytes"]
        else "refuse"
    )
    if projection["decision"] != expected_decision:
        raise StorageQualificationError("S26 admission decision contradicts projected headroom")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("low", type=Path)
    parser.add_argument("high", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--low-result-sha256", required=True)
    parser.add_argument("--high-result-sha256", required=True)
    parser.add_argument("--volume-bytes", type=int, required=True)
    parser.add_argument("--reserved-headroom-bytes", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        qualification = build(
            [args.low, args.high],
            provider_result_sha256=[
                args.low_result_sha256,
                args.high_result_sha256,
            ],
            expected_commit=args.commit,
            expected_image_digest=args.image_digest,
            volume_bytes=args.volume_bytes,
            reserved_headroom_bytes=args.reserved_headroom_bytes,
        )
        publish_json_no_clobber(args.output, qualification)
        return 0
    except (OSError, StorageQualificationError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
