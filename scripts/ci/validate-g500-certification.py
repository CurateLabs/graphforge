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
FORBIDDEN_KEY = re.compile(
    r"(secret|credential|token|password|host_path|absolute_path|machine[_-]?id|volume[_-]?id|provider_resource_id)",
    re.I,
)
ABSOLUTE_PATH = re.compile(r"(?:^|[\s=:])(?:/|[A-Za-z]:[\\/])")
RAW_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
ROOT = Path(__file__).resolve().parents[2]
PROFILE = ROOT / "crates/graphforge-api/tests/fixtures/scale_g500_certification.v1.json"
SCHEMA = ROOT / "docs/development/evidence/g500-certification.schema.json"
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


def validate(evidence: dict[str, Any], expected_sha: str | None) -> None:
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--expected-sha", required=True)
    args = parser.parse_args()
    value = json.loads(args.evidence.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EvidenceError("evidence root must be an object")
    validate(value, args.expected_sha)
    print(f"valid #745 evidence: {args.evidence}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
