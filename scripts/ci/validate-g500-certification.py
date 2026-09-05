#!/usr/bin/env python3
"""Fail-closed semantic validator for sanitized #745 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

REQUIRED_PHASES = (
    "preflight",
    "generate",
    "ingest",
    "csr",
    "source_reopen",
    "source_query_1hop",
    "source_query_2hop",
    "export",
    "verify",
    "import",
    "imported_reopen",
    "imported_query_1hop",
    "imported_query_2hop",
    "drill_corruption",
    "drill_cancellation",
    "drill_resource_limit",
    "drill_interrupted_finalization",
)
CATEGORY_NAMES = (
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
FORBIDDEN_KEY = re.compile(
    r"(secret|credential|token|password|host_path|absolute_path|machine[_-]?id|volume[_-]?id|provider_resource_id)",
    re.I,
)
ABSOLUTE_PATH = re.compile(r"(?:^|[\s=:])(?:/|[A-Za-z]:[\\/])")
RAW_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)
NATIVE_OBJECT_IDENTITY = re.compile(
    r"(?<![0-9a-f])[0-9a-f]{16}:[0-9a-f]{32}(?![0-9a-f])",
    re.I,
)
ROOT = Path(__file__).resolve().parents[2]
PROFILE = ROOT / "crates/graphforge-api/tests/fixtures/scale_g500_certification.v1.json"
SCHEMA = ROOT / "docs/development/evidence/g500-certification.schema.json"
PROVIDER_RESULT_SCHEMA = ROOT / "benchmarks/schemas/progressive-provider-run-result.json"
RUN_COMMAND = (
    "cargo test -p graphforge-api --release --test scale_g500_ladder "
    "certification_target_live_full_lifecycle_evidence -- --ignored --exact "
    "--nocapture --test-threads=1"
)


class EvidenceError(ValueError):
    pass


def validate_schema(evidence: dict[str, Any]) -> None:
    try:
        contract = json.loads(SCHEMA.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(contract)
        Draft202012Validator(contract).validate(evidence)
    except (OSError, json.JSONDecodeError, SchemaError) as error:
        raise EvidenceError(f"committed evidence schema is invalid: {error}") from error
    except ValidationError as error:
        location = ".".join(str(item) for item in error.absolute_path) or "$"
        raise EvidenceError(f"evidence schema violation at {location}: {error.message}") from error


def update_authority_context(digest: Any, context: dict[str, Any], category: str) -> None:
    for field in (
        "contract",
        "generation_sha256",
        "owner",
        "receipt_authority_sha256",
        "native_identity_authority_sha256",
    ):
        value = context[field].encode()
        digest.update(len(value).to_bytes(16, "big"))
        digest.update(value)
    digest.update(context["version"].to_bytes(4, "big"))
    digest.update(context["rung"].to_bytes(8, "big"))
    digest.update(context["live_nodes"].to_bytes(8, "big"))
    digest.update(context["live_edges"].to_bytes(8, "big"))
    category_identity = context["native_category_identity_authority_sha256"][category].encode()
    digest.update(len(category_identity).to_bytes(16, "big"))
    digest.update(category_identity)


def category_commitment(context: dict[str, Any], category: str, totals: dict[str, int]) -> str:
    digest = hashlib.sha256()
    digest.update(b"graphforge-category-authority-v2\0")
    update_authority_context(digest, context, category)
    digest.update(category.encode())
    for field in (
        "logical_references",
        "logical_bytes",
        "physical_objects",
        "physical_logical_bytes",
        "allocated_bytes",
    ):
        digest.update(totals[field].to_bytes(8, "big"))
    return "sha256:" + digest.hexdigest()


def peak_commitment(context: dict[str, Any], category: str, allocated_bytes: int) -> str:
    digest = hashlib.sha256()
    digest.update(b"graphforge-category-peak-authority-v2\0")
    update_authority_context(digest, context, category)
    digest.update(category.encode())
    digest.update(allocated_bytes.to_bytes(8, "big"))
    return "sha256:" + digest.hexdigest()


def validate_cas_publication_components(storage: dict[str, Any]) -> None:
    construction = storage["construction"]
    components = construction["cas_publication_io"]
    maximum = (1 << 64) - 1

    def total(*values: int) -> int:
        value = sum(values)
        if value > maximum:
            raise EvidenceError("CAS component sum exceeds native u64")
        return value

    field_projection = {
        "read_bytes": "cas_application_read_bytes",
        "read_calls": "cas_application_read_operations",
        "write_bytes": "cas_application_write_bytes",
        "write_calls": "cas_application_write_operations",
    }
    phase = storage["application_io_phases"]["phases"]["cas_install_read_write"]
    for field, aggregate in field_projection.items():
        expected = total(
            *(
                component[field]
                for component in (
                    components[name] for name in ("payload", "manifest", "manifest_reads")
                )
            )
        )
        if construction[aggregate] != expected or phase[field] != expected:
            raise EvidenceError(f"CAS {field} components do not reconcile")
    synchronization = total(
        *(
            component[field]
            for component in (
                components[name] for name in ("payload", "manifest", "manifest_reads")
            )
            for field in ("file_fsync_calls", "directory_fsync_calls")
        )
    )
    if (
        construction["cas_fsync_operations"] != synchronization
        or phase["fsync_calls"] != synchronization
    ):
        raise EvidenceError("CAS synchronization components do not reconcile")
    paths = construction["canonical_artifact_objects"]
    if (
        components["publications"] != 1
        or components["initial_entries"] != 0
        or components["changed_paths"] != paths
    ):
        raise EvidenceError(
            "CAS growth proof requires one fresh publication of the complete changed inventory"
        )
    inventory_bytes = storage["application_io_phases"]["phases"]["publication_preauthentication"][
        "read_bytes"
    ]
    # Native maximal-encoding fixtures establish 1319 branch/header bytes and
    # 43 additional bytes per GraphFileEntry over ConstructionEncodedArtifact.
    node_bytes = total(inventory_bytes, 43 * paths, 1319)
    install_requests = total(67 * paths, 1)
    read_requests = total(130 * paths)
    manifest = components["manifest"]
    requests = total(manifest["installed_objects"], manifest["reused_objects"])
    if (
        requests < total(paths, 1)
        or manifest["read_calls"] < requests
        or components["manifest_reads"]["read_calls"] < paths
    ):
        raise EvidenceError("CAS manifest lacks mandatory bootstrap/update authentication")
    if (
        total(manifest["installed_objects"], manifest["reused_objects"]) > install_requests
        or manifest["read_bytes"] > total(install_requests * node_bytes)
        or manifest["write_bytes"] > total(install_requests * node_bytes)
        or manifest["write_bytes"] < manifest["installed_bytes"]
        or manifest["write_calls"] != manifest["install_attempts"]
        or components["manifest_reads"]["read_bytes"] > total(read_requests * node_bytes)
    ):
        raise EvidenceError("CAS manifest exceeds native path/object/encoded-inventory bounds")
    for component in (manifest, components["manifest_reads"]):
        if (
            not (component["read_bytes"] + 65535) // 65536
            <= component["read_calls"]
            <= component["read_bytes"]
        ):
            raise EvidenceError("CAS manifest reads violate native buffer bounds")
    payload = components["payload"]
    if (
        total(payload["installed_objects"], payload["reused_objects"])
        != construction["canonical_artifact_objects"]
    ):
        raise EvidenceError("CAS payload requests differ from canonical inventory")
    for name in ("payload", "manifest"):
        component = components[name]
        requests = total(component["installed_objects"], component["reused_objects"])
        if not component["installed_objects"] <= component["install_attempts"] <= requests:
            raise EvidenceError(f"CAS {name} install attempts differ from request inventory")
        if component["directory_fsync_calls"] != total(
            component["install_attempts"], component["install_attempts"]
        ):
            raise EvidenceError(f"CAS {name} namespace synchronization differs from installs")
        if component["file_fsync_calls"] < component["install_attempts"]:
            raise EvidenceError(f"CAS {name} lacks mandatory file synchronization")
        if name == "manifest" and component["file_fsync_calls"] != component["install_attempts"]:
            raise EvidenceError("CAS manifest file synchronization differs from installs")
    if any(
        value
        for field, value in components["manifest_reads"].items()
        if field not in ("read_bytes", "read_calls")
    ):
        raise EvidenceError("CAS manifest path authentication reports non-read work")


def validate_category_authority(
    storage: dict[str, Any],
    *,
    rung: int,
    live_nodes: int,
    live_edges: int,
) -> None:
    for owner in ("source", "clean_import"):
        reported = storage[owner]["categories"]
        authority = storage[owner]["category_authorities"]
        context = storage[owner]["category_authority_context"]
        if (
            context["owner"] != owner
            or context["rung"] != rung
            or context["live_nodes"] != live_nodes
            or context["live_edges"] != live_edges
        ):
            raise EvidenceError(f"{owner} category authority context differs")
        if reported != authority:
            raise EvidenceError(f"{owner} categories differ from native authority")
        for category in CATEGORY_NAMES:
            commitment = category_commitment(context, category, authority[category])
            if storage[owner]["category_authority_sha256"][category] != commitment:
                raise EvidenceError(f"{owner}.{category} authority commitment differs")

    construction = storage["construction"]
    source_context = storage["source"]["category_authority_context"]
    construction_context = construction["storage_category_authority_context"]
    if (
        construction_context["owner"] != "construction"
        or construction_context["rung"] != rung
        or construction_context["generation_sha256"] != source_context["generation_sha256"]
        or construction_context["live_nodes"] != live_nodes
        or construction_context["live_edges"] != live_edges
    ):
        raise EvidenceError("construction category authority context differs")
    if construction["storage_current"] != construction["storage_category_authorities"]:
        raise EvidenceError("construction categories differ from native authority")
    if (
        construction["storage_transient_peak_allocated_bytes"]
        != construction["storage_transient_peak_authorities"]
    ):
        raise EvidenceError("construction peaks differ from native authority")
    for category in CATEGORY_NAMES:
        current = category_commitment(
            construction_context,
            category,
            construction["storage_category_authorities"][category],
        )
        peak = peak_commitment(
            construction_context,
            category,
            construction["storage_transient_peak_authorities"][category],
        )
        if construction["storage_category_authority_sha256"][category] != current:
            raise EvidenceError(f"construction.{category} authority commitment differs")
        if construction["storage_transient_peak_authority_sha256"][category] != peak:
            raise EvidenceError(f"construction.{category} peak commitment differs")

    portable = storage["portable_package"]
    portable_context = portable["category_authority_context"]
    if (
        portable_context["owner"] != "portable_package"
        or portable_context["rung"] != rung
        or portable_context["generation_sha256"] != source_context["generation_sha256"]
        or portable_context["live_nodes"] != live_nodes
        or portable_context["live_edges"] != live_edges
    ):
        raise EvidenceError("portable category authority context differs")
    portable_commitment = category_commitment(
        portable_context, "portable_package", portable["category_authority"]
    )
    if portable["category_authority_sha256"] != portable_commitment:
        raise EvidenceError("portable package authority commitment differs")


def require_mapping(evidence: dict[str, Any], key: str, fields: tuple[str, ...]) -> dict[str, Any]:
    section = evidence.get(key)
    if not isinstance(section, dict):
        raise EvidenceError(f"missing or malformed section: {key}")
    absent = [field for field in fields if field not in section or section[field] is None]
    if absent:
        raise EvidenceError(f"missing {key} fields: " + ", ".join(absent))
    return section


def reject_sensitive(value: Any, trail: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if FORBIDDEN_KEY.search(key):
                raise EvidenceError(f"forbidden sensitive field at {trail}.{key}")
            reject_sensitive(child, f"{trail}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_sensitive(child, f"{trail}[{index}]")
    elif isinstance(value, str):
        if ABSOLUTE_PATH.search(value):
            raise EvidenceError(f"absolute host path at {trail}")
        if RAW_UUID.fullmatch(value):
            raise EvidenceError(f"raw UUID at {trail}")
        if NATIVE_OBJECT_IDENTITY.search(value):
            raise EvidenceError(f"raw native object identity at {trail}")


def validate_semantics(
    evidence: dict[str, Any],
    expected_sha: str | None,
) -> None:
    """Validate content after an external provider-result binding is established."""
    validate_schema(evidence)
    reject_sensitive(evidence)
    if evidence.get("schema") != "graphforge-billion-edge-certification-evidence/1":
        raise EvidenceError("unsupported evidence schema")
    if not expected_sha:
        raise EvidenceError("--expected-sha is required to pin the dispatched commit")
    if re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None:
        raise EvidenceError("--expected-sha must be an exact lowercase 40-hex commit")
    if evidence.get("git_sha") != expected_sha:
        raise EvidenceError("evidence git_sha does not match dispatched commit")
    expected_profile = "sha256:" + hashlib.sha256(PROFILE.read_bytes()).hexdigest()
    if evidence.get("profile_sha256") != expected_profile:
        raise EvidenceError("evidence profile does not match the committed certification profile")
    run = evidence.get("run", {})
    scale = run.get("scale")
    if scale not in (20, 22, 24, 26):
        raise EvidenceError("run scale is not a supported qualification rung")
    expected_run = {
        "command": RUN_COMMAND,
        "scale": scale,
        "edgefactor": 16,
        "seed": 1,
        "directionality": "undirected",
        "self_loops": "drop",
        "duplicates": "drop",
    }
    if evidence.get("run") != expected_run:
        raise EvidenceError("run command/profile is not the approved target-live contract")
    counts = require_mapping(
        evidence,
        "counts",
        (
            "raw_attempts",
            "self_loops_rejected",
            "duplicates_rejected",
            "live_unique_edges",
            "source_nodes",
            "source_edges",
            "imported_nodes",
            "imported_edges",
        ),
    )
    raw = counts.get("raw_attempts")
    loops = counts.get("self_loops_rejected")
    dupes = counts.get("duplicates_rejected")
    live = counts.get("live_unique_edges")
    if not all(isinstance(item, int) and item >= 0 for item in (raw, loops, dupes, live)):
        raise EvidenceError("counts must be non-negative integers")
    if raw != live + loops + dupes:
        raise EvidenceError("generator counts do not reconcile")
    if scale == 26 and live < 1_000_000_000:
        raise EvidenceError("S26 certification requires at least one billion live edges")
    if any(counts.get(key) != live for key in ("source_edges", "imported_edges")):
        raise EvidenceError("source/imported edge counts differ")
    if counts.get("source_nodes") != counts.get("imported_nodes"):
        raise EvidenceError("source/imported node counts differ")
    if counts.get("source_nodes") != 1 << scale:
        raise EvidenceError("source/imported node count does not match the declared scale")

    storage = evidence.get("storage_attribution", {})
    validate_cas_publication_components(storage)
    validate_category_authority(
        storage,
        rung=scale,
        live_nodes=counts["source_nodes"],
        live_edges=counts["source_edges"],
    )
    selected_source = storage.get("source", {}).get("allocated_bytes")
    source_project = storage.get("source_project_current_allocated_bytes")
    workspace = storage.get("workspace_current_allocated_bytes")
    peak = evidence.get("envelope", {}).get("peak_disk_bytes")
    if not all(
        isinstance(value, int) for value in (selected_source, source_project, workspace, peak)
    ):
        raise EvidenceError("storage union numerators must be integers")
    if not selected_source <= source_project <= workspace <= peak:
        raise EvidenceError(
            "selected source, source project, workspace, and peak unions do not reconcile"
        )

    identities = evidence.get("identities", {})
    for proof in (
        "source_export_generation_authenticated",
        "import_receipt_reopen_authenticated",
        "source_import_generations_distinct",
    ):
        if identities.get(proof) is not True:
            raise EvidenceError(f"generation proof is not authenticated: {proof}")
    if len({identities.get("package"), identities.get("transport")}) != 2:
        raise EvidenceError("semantic package and transport identities must be distinct")
    package = evidence.get("package", {})
    expected_package = {
        "contract": "graphforge-portable-verify/2",
        "format": "portable-project-v2-bundle",
        "class": "complete",
        "integrity": "verified",
        "compatibility": "supported",
        "policy": "complete-current-generation",
    }
    if package != expected_package:
        raise EvidenceError("portable-v2 package contract is incomplete or incompatible")
    authority = require_mapping(
        evidence, "authority", ("source_fingerprint", "imported_fingerprint")
    )
    if authority.get("source_fingerprint") != authority.get("imported_fingerprint"):
        raise EvidenceError("ontology/capability authority changed across import")
    equivalence = require_mapping(
        evidence,
        "equivalence",
        ("source_project_fingerprint", "imported_project_fingerprint"),
    )
    if equivalence.get("source_project_fingerprint") != equivalence.get(
        "imported_project_fingerprint"
    ):
        raise EvidenceError("source/imported project fingerprints differ")

    phases = evidence.get("phases")
    if not isinstance(phases, list):
        raise EvidenceError("phases must be an array")
    phase_ids = [phase.get("id") for phase in phases if isinstance(phase, dict)]
    if len(phase_ids) != len(phases):
        raise EvidenceError("every phase must be an object with an id")
    if len(set(phase_ids)) != len(phase_ids):
        raise EvidenceError("duplicate phase ids are forbidden")
    extras = [phase for phase in phase_ids if phase not in REQUIRED_PHASES]
    if extras:
        raise EvidenceError("unexpected phases: " + ", ".join(str(phase) for phase in extras))
    if tuple(phase_ids) != REQUIRED_PHASES:
        raise EvidenceError("phases must use the required lifecycle order")
    by_id = {phase.get("id"): phase for phase in phases if isinstance(phase, dict)}
    missing = [phase for phase in REQUIRED_PHASES if phase not in by_id]
    if missing:
        raise EvidenceError("missing phases: " + ", ".join(missing))
    failed = [phase for phase in phases if phase.get("status") != "pass"]
    if evidence.get("result") == "pass" and failed:
        raise EvidenceError("passing evidence contains non-passing phases")
    for query in ("source_query_1hop", "source_query_2hop"):
        imported = query.replace("source_", "imported_")
        if by_id[query].get("fingerprint") != by_id[imported].get("fingerprint"):
            raise EvidenceError(f"query fingerprint mismatch: {query}")
    envelope = evidence.get("envelope", {})
    if envelope.get("peak_rss_bytes", 2**64) > 137_438_953_472:
        raise EvidenceError("RSS envelope exceeded")
    if envelope.get("peak_disk_bytes", 2**64) > 1_099_511_627_776:
        raise EvidenceError("disk envelope exceeded")
    if envelope.get("wall_time_s", 2**64) > 14_400:
        raise EvidenceError("wall-time envelope exceeded")
    host = evidence.get("host", {})
    if host.get("os") != "Linux":
        raise EvidenceError("certification host must be Linux")
    provider = str(host.get("provider", "")).strip()
    region = str(host.get("region", "")).strip()
    sku = str(host.get("sku", "")).strip()
    os_image = str(host.get("os_image", "")).strip()
    placeholders = {"", "local", "localhost", "developer", "example", "generic", "test", "unknown"}
    if provider.casefold() in placeholders:
        raise EvidenceError("certification provider must identify provisioned infrastructure")
    if region.casefold() in placeholders | {"default", "global"}:
        raise EvidenceError("certification region must identify the exact provisioned region")
    if sku.casefold() in placeholders or sku.casefold() == "default":
        raise EvidenceError("certification SKU must identify the exact provisioned machine class")
    if os_image.casefold() in placeholders | {"default", "latest"} or os_image.casefold().endswith(
        ":latest"
    ):
        raise EvidenceError("certification OS image must be an immutable resolved identity")
    declared = (provider, region, sku, os_image)
    observed = tuple(host.get(key) for key in ("provider", "region", "sku", "os_image"))
    if declared != observed:
        raise EvidenceError(
            "provider, region, SKU, and OS image must not contain surrounding whitespace"
        )
    if re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z ._+:/()-]*", provider) is None:
        raise EvidenceError("certification provider contains unsupported characters")
    if re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z._+-]*", region) is None:
        raise EvidenceError("certification region contains unsupported characters")
    if re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z ._+:/()-]*", sku) is None:
        raise EvidenceError("certification SKU contains unsupported characters")
    if re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z ._+:/()@-]*", os_image) is None:
        raise EvidenceError("certification OS image contains unsupported characters")
    if re.fullmatch(r"[0-9A-Za-z._+-]+", str(host.get("kernel", ""))) is None:
        raise EvidenceError("host kernel release is malformed")
    memory_bytes = host.get("memory_bytes", 0)
    nvme_bytes = host.get("nvme_bytes", 0)
    if memory_bytes < envelope.get("peak_rss_bytes", 0):
        raise EvidenceError("observed RSS exceeds declared host memory")
    if nvme_bytes < envelope.get("peak_disk_bytes", 0):
        raise EvidenceError("observed storage peak exceeds declared host capacity")
    if evidence.get("result") != "pass" or evidence.get("first_failure") is not None:
        raise EvidenceError("certification evidence is not a pass")


def _decode_object(encoded: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceError(f"{label} is malformed") from error
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} root must be an object")
    return value


def validate(
    evidence_encoded: bytes,
    expected_sha: str | None,
    provider_result_encoded: bytes,
    expected_provider_result_sha256: str | None,
) -> None:
    """Validate evidence rooted in an externally authenticated provider result.

    The expected provider-result digest must come from trusted transport or an
    immutable manifest outside both documents. The provider result then binds
    the exact ordinary GraphForge evidence bytes through ``graphforge_sha256``.
    """
    if not expected_sha:
        raise EvidenceError("--expected-sha is required to pin the dispatched commit")
    if re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None:
        raise EvidenceError("--expected-sha must be an exact lowercase 40-hex commit")
    if (
        expected_provider_result_sha256 is None
        or re.fullmatch(r"[0-9a-f]{64}", expected_provider_result_sha256) is None
    ):
        raise EvidenceError("external provider-result SHA-256 is required")
    actual_provider_result_sha256 = hashlib.sha256(provider_result_encoded).hexdigest()
    if actual_provider_result_sha256 != expected_provider_result_sha256:
        raise EvidenceError("provider result does not match its external anchor")
    provider_result = _decode_object(provider_result_encoded, "provider result")
    try:
        provider_schema = json.loads(PROVIDER_RESULT_SCHEMA.read_text(encoding="utf-8"))
        Draft202012Validator(provider_schema).validate(provider_result)
    except (OSError, json.JSONDecodeError, SchemaError, ValidationError) as error:
        raise EvidenceError(f"provider result schema is invalid: {error}") from error
    evidence = _decode_object(evidence_encoded, "evidence")
    scale = evidence.get("run", {}).get("scale")
    identities = provider_result.get("identities", {})
    artifacts = provider_result.get("artifacts", {})
    if (
        provider_result.get("status") != "passed"
        or provider_result.get("rung") != f"S{scale}"
        or identities.get("commit") != expected_sha
        or not isinstance(artifacts, dict)
        or artifacts.get("graphforge_sha256") != hashlib.sha256(evidence_encoded).hexdigest()
    ):
        raise EvidenceError("ordinary evidence is not bound by the authenticated provider result")
    validate_semantics(evidence, expected_sha)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--provider-result", required=True, type=Path)
    parser.add_argument("--provider-result-sha256", required=True)
    args = parser.parse_args()
    validate(
        args.evidence.read_bytes(),
        args.expected_sha,
        args.provider_result.read_bytes(),
        args.provider_result_sha256,
    )
    print(f"valid #745 evidence: {args.evidence}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
