from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError
import pytest

SCRIPT = Path(__file__).with_name("validate-g500-certification.py")
SPEC = importlib.util.spec_from_file_location("g500_validator", SCRIPT)
assert SPEC and SPEC.loader
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)

SHA = "a" * 40
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
HEX_A = "a" * 64
HEX_B = "b" * 64
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
CATEGORY_AUTHORITY_CONTEXT_FIELDS = (
    "contract",
    "version",
    "rung",
    "generation_sha256",
    "owner",
    "receipt_authority_sha256",
    "native_identity_authority_sha256",
    "native_category_identity_authority_sha256",
    "live_nodes",
    "live_edges",
)


def artifact_totals(unit=1):
    return {
        "logical_references": unit,
        "logical_bytes": unit,
        "physical_objects": unit,
        "physical_logical_bytes": unit,
        "allocated_bytes": unit,
    }


def authority_context(owner, scale, live):
    identity_authority = (
        DIGEST_B if owner in ("source", "construction", "portable_package") else DIGEST_A
    )
    return {
        "contract": "graphforge-lifecycle-category-authority/2",
        "version": 1,
        "rung": scale,
        "generation_sha256": DIGEST_A,
        "owner": owner,
        "receipt_authority_sha256": (
            DIGEST_A if owner in ("source", "construction", "portable_package") else DIGEST_B
        ),
        "native_identity_authority_sha256": identity_authority,
        "native_category_identity_authority_sha256": dict.fromkeys(
            CATEGORY_NAMES, identity_authority
        ),
        "live_nodes": 1 << scale,
        "live_edges": live,
    }


def category_commitment(context, category, totals):
    return VALIDATOR.category_commitment(context, category, totals)


def peak_commitment(context, category, allocated_bytes):
    return VALIDATOR.peak_commitment(context, category, allocated_bytes)


def storage_attribution(scale, live, unit=1):
    categories = {
        name: artifact_totals(unit if index < 6 else 0) for index, name in enumerate(CATEGORY_NAMES)
    }

    def snapshot(owner):
        context = authority_context(owner, scale, live)
        return {
            "generation_manifest_sha256": [1] * 32,
            "categories": categories,
            "category_authorities": categories,
            "category_authority_context": context,
            "category_authority_sha256": {
                name: category_commitment(context, name, totals)
                for name, totals in categories.items()
            },
            "logical_references": 6 * unit,
            "logical_bytes": 6 * unit,
            "physical_objects": 6 * unit,
            "physical_logical_bytes": 6 * unit,
            "allocated_bytes": 6 * unit,
        }

    contract = json.loads(VALIDATOR.SCHEMA.read_text())
    construction = dict.fromkeys(contract["$defs"]["construction"]["required"], 0)
    construction["cas_publication_io"] = {
        name: dict.fromkeys(contract["$defs"]["casIoTotals"]["required"], 0)
        for name in ("payload", "manifest", "manifest_reads")
    }
    construction["cas_publication_io"].update(publications=1, initial_entries=0, changed_paths=0)
    construction["cas_publication_io"]["payload"]["read_bytes"] = unit
    construction["cas_publication_io"]["payload"]["read_calls"] = unit
    construction["cas_publication_io"]["manifest"].update(
        read_bytes=1, read_calls=1, reused_objects=1
    )
    construction["cas_application_read_bytes"] = unit + 1
    construction["cas_application_read_operations"] = unit + 1
    construction_context = authority_context("construction", scale, live)
    construction["storage_category_authority_context"] = construction_context
    construction["storage_current"] = {name: artifact_totals(unit) for name in CATEGORY_NAMES}
    construction["storage_category_authorities"] = {
        name: artifact_totals(unit) for name in CATEGORY_NAMES
    }
    construction["storage_transient_peak_allocated_bytes"] = dict.fromkeys(CATEGORY_NAMES, unit)
    construction["storage_transient_peak_authorities"] = dict.fromkeys(CATEGORY_NAMES, unit)
    construction["storage_category_authority_sha256"] = {
        name: category_commitment(construction_context, name, totals)
        for name, totals in construction["storage_category_authorities"].items()
    }
    construction["storage_transient_peak_authority_sha256"] = {
        name: peak_commitment(construction_context, name, value)
        for name, value in construction["storage_transient_peak_authorities"].items()
    }
    construction["storage_transient_peak_total_allocated_bytes"] = 10 * unit
    phase_names = contract["$defs"]["phaseMap"]["required"]
    phases = {}
    for name in phase_names:
        phases[name] = {
            "read_bytes": unit,
            "write_bytes": 0,
            "read_calls": unit,
            "write_calls": 0,
            "object_count": 0,
            "block_count": 0,
            "fsync_calls": unit if name == "fsync_synchronization" else 0,
        }
    phases["cas_install_read_write"]["read_bytes"] += 1
    phases["cas_install_read_write"]["read_calls"] += 1
    totals = {
        field: sum(values[field] for values in phases.values())
        for field in next(iter(phases.values()))
    }
    return {
        "source": snapshot("source"),
        "source_project_current_allocated_bytes": 7 * unit,
        "portable_package": {
            "category": "portable_package",
            "logical_bytes": unit,
            "allocated_bytes": unit,
            "logical_references": unit,
            "physical_objects": unit,
            "source": "portable_writer_receipt",
            "category_authority": artifact_totals(unit),
            "category_authority_context": authority_context("portable_package", scale, live),
            "category_authority_sha256": category_commitment(
                authority_context("portable_package", scale, live),
                "portable_package",
                artifact_totals(unit),
            ),
        },
        "clean_import": snapshot("clean_import"),
        "construction": construction,
        "application_io_phases": {"phases": phases, "totals": totals},
        "workspace_current_allocated_bytes": 14 * unit,
        "workspace_peak_allocated_bytes": 14 * unit,
        "workspace_components": {
            "source_project_and_construction": 7 * unit,
            "portable_package": unit,
            "clean_import_project": 2 * unit,
            "drill_project_and_construction": unit,
            "drill_package": unit,
            "corrupt_drill_package": 2 * unit,
        },
    }


def evidence(scale=26, unit=1):
    live = 1_000_000_000 if scale == 26 else (1 << scale) * 15
    phases = []
    for phase in VALIDATOR.REQUIRED_PHASES:
        fingerprint = DIGEST_A if "query_1hop" in phase else DIGEST_B
        phases.append(
            {
                "id": phase,
                "status": "pass",
                "elapsed_ms": 1,
                "rss_peak_bytes": 1,
                "disk_peak_bytes": 1,
                "fingerprint": fingerprint,
            }
        )
    return {
        "schema": "graphforge-billion-edge-certification-evidence/1",
        "git_sha": SHA,
        "profile_sha256": "sha256:" + hashlib.sha256(VALIDATOR.PROFILE.read_bytes()).hexdigest(),
        "run": {
            "command": VALIDATOR.RUN_COMMAND,
            "scale": scale,
            "edgefactor": 16,
            "seed": 1,
            "directionality": "undirected",
            "self_loops": "drop",
            "duplicates": "drop",
        },
        "host": {
            "provider": "Microsoft Azure",
            "region": "eastus",
            "sku": "Standard_L64s_v3",
            "os_image": "Canonical:ubuntu-24_04-lts:server:24.04.202608010",
            "os": "Linux",
            "kernel": "6",
            "filesystem": "xfs",
            "memory_bytes": 4_294_967_296,
            "nvme_bytes": 536_870_912_000,
        },
        "tools": {"rustc": "1.90"},
        "counts": {
            "raw_attempts": live + 2,
            "self_loops_rejected": 1,
            "duplicates_rejected": 1,
            "live_unique_edges": live,
            "source_nodes": 1 << scale,
            "source_edges": live,
            "imported_nodes": 1 << scale,
            "imported_edges": live,
        },
        "identities": {
            "source_export_generation_authenticated": True,
            "import_receipt_reopen_authenticated": True,
            "source_import_generations_distinct": True,
            "package": DIGEST_A,
            "transport": DIGEST_B,
        },
        "package": {
            "contract": "graphforge-portable-verify/2",
            "format": "portable-project-v2-bundle",
            "class": "complete",
            "integrity": "verified",
            "compatibility": "supported",
            "policy": "complete-current-generation",
        },
        "authority": {"source_fingerprint": DIGEST_A, "imported_fingerprint": DIGEST_A},
        "equivalence": {
            "source_project_fingerprint": DIGEST_A,
            "imported_project_fingerprint": DIGEST_A,
        },
        "storage_attribution": storage_attribution(scale, live, unit),
        "phases": phases,
        "envelope": {
            "peak_rss_bytes": 1,
            "peak_disk_bytes": 100 * unit,
            "peak_disk_source": "storage_owned_active_identity_union",
            "wall_time_s": 1,
        },
        "result": "pass",
        "first_failure": None,
    }


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def provider_result(evidence_encoded, scale=26):
    return {
        "schema": "graphforge-progressive-provider-run-result/1",
        "rung": f"S{scale}",
        "status": "passed",
        "failure": None,
        "identities": {
            "commit": SHA,
            "profile_id": f"graph500-s{scale}-provider",
            "profile_sha256": HEX_A,
            "image_digest": f"registry.fly.io/graphforge@sha256:{HEX_A}",
            "generator": DIGEST_A,
            "generator_executable_sha256": HEX_A,
            "gf_sha256": HEX_A,
            "certify_sha256": HEX_A,
            "benchexec_python_sha256": HEX_A,
            "benchexec_version": "3.31",
            "admitted_plan_sha256": HEX_A,
            "source_tree_sha256": HEX_A,
        },
        "artifacts": {
            "plan_sha256": HEX_A,
            "benchexec_sha256": HEX_A,
            "graphforge_sha256": hashlib.sha256(evidence_encoded).hexdigest(),
            "rung_sha256": HEX_B,
        },
        "claim": "engineering_evidence_only",
    }


def validate(
    value,
    expected_sha=SHA,
    *,
    evidence_encoded=None,
    provider=None,
    expected_provider_result_sha256=None,
):
    if evidence_encoded is None:
        evidence_encoded = encoded(value)
    if provider is None:
        provider = provider_result(evidence_encoded, value["run"]["scale"])
    provider_encoded = encoded(provider)
    if expected_provider_result_sha256 is None:
        expected_provider_result_sha256 = hashlib.sha256(provider_encoded).hexdigest()
    VALIDATOR.validate(
        evidence_encoded,
        expected_sha,
        provider_encoded,
        expected_provider_result_sha256,
    )


def test_accepts_complete_sanitized_evidence():
    schema = VALIDATOR.ROOT / "docs/development/evidence/g500-certification.schema.json"
    contract = json.loads(schema.read_text())
    Draft202012Validator.check_schema(contract)
    Draft202012Validator(contract).validate(evidence())
    validate(evidence())


def test_rejects_missing_external_provider_result_anchor():
    value = evidence()
    evidence_encoded = encoded(value)
    provider_encoded = encoded(provider_result(evidence_encoded))
    with pytest.raises(
        VALIDATOR.EvidenceError,
        match="external provider-result SHA-256 is required",
    ):
        VALIDATOR.validate(evidence_encoded, SHA, provider_encoded, None)


@pytest.mark.parametrize(
    "field",
    [
        "canonical_artifact_objects",
        "encode_output_fsync_operations",
        "encode_source_spool_fsync_operations",
        "encode_membership_fsync_operations",
        "encode_ordinal_fsync_operations",
        "hydration_files_copied",
        "hydration_file_fsync_operations",
        "hydration_directory_fsync_operations",
        "encode_output_write_operations",
        "encode_membership_write_operations",
        "encode_source_spool_write_operations",
        "encode_ordinal_artifact_write_operations",
        "encode_ordinal_publication_write_operations",
        "storage_category_authorities",
        "storage_transient_peak_authorities",
        "storage_category_authority_context",
        "storage_category_authority_sha256",
        "storage_transient_peak_authority_sha256",
    ],
)
def test_construction_authority_fields_are_required(field):
    value = evidence()
    value["storage_attribution"]["construction"].pop(field)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_construction_authority",
        "extra_construction_authority",
        "malformed_construction_authority",
        "missing_workspace_component",
        "extra_workspace_component",
        "malformed_workspace_component",
        "missing_workspace_peak",
        "malformed_category_commitment",
        "overflowing_native_counter",
    ],
)
def test_closed_storage_authority_schema_rejects_mutations(mutation):
    value = evidence()
    storage = value["storage_attribution"]
    if mutation == "missing_construction_authority":
        storage["construction"].pop("canonical_artifact_objects")
    elif mutation == "extra_construction_authority":
        storage["construction"]["unknown_authority"] = 1
    elif mutation == "malformed_construction_authority":
        storage["construction"]["hydration_file_fsync_operations"] = "1"
    elif mutation == "missing_workspace_component":
        storage["workspace_components"].pop("drill_package")
    elif mutation == "extra_workspace_component":
        storage["workspace_components"]["other"] = 1
    elif mutation == "malformed_workspace_component":
        storage["workspace_components"]["portable_package"] = "1"
    elif mutation == "missing_workspace_peak":
        storage.pop("workspace_peak_allocated_bytes")
    elif mutation == "malformed_category_commitment":
        storage["source"]["category_authority_sha256"]["topology_nodes"] = "sha256:bad"
    elif mutation == "overflowing_native_counter":
        storage["construction"]["canonical_artifact_objects"] = 1 << 64
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize("owner", ("source", "clean_import"))
@pytest.mark.parametrize(
    "field",
    ("category_authorities", "category_authority_context", "category_authority_sha256"),
)
def test_snapshot_authority_fields_are_independently_required(owner, field):
    value = evidence()
    value["storage_attribution"][owner].pop(field)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize(
    "field",
    ("category_authority", "category_authority_context", "category_authority_sha256"),
)
def test_portable_authority_fields_are_independently_required(field):
    value = evidence()
    value["storage_attribution"]["portable_package"].pop(field)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize(
    ("owner", "field"),
    (
        ("source", "category_authority_sha256"),
        ("clean_import", "category_authority_sha256"),
        ("construction", "storage_category_authority_sha256"),
        ("construction", "storage_transient_peak_authority_sha256"),
    ),
)
@pytest.mark.parametrize("category", CATEGORY_NAMES)
def test_every_commitment_category_member_is_required(owner, field, category):
    value = evidence()
    value["storage_attribution"][owner][field].pop(category)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize(
    ("owner", "context_field"),
    (
        ("source", "category_authority_context"),
        ("clean_import", "category_authority_context"),
        ("portable_package", "category_authority_context"),
        ("construction", "storage_category_authority_context"),
    ),
)
@pytest.mark.parametrize("category", CATEGORY_NAMES)
def test_every_native_category_identity_commitment_is_required(owner, context_field, category):
    value = evidence()
    value["storage_attribution"][owner][context_field][
        "native_category_identity_authority_sha256"
    ].pop(category)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize(
    ("owner", "context_field"),
    (
        ("source", "category_authority_context"),
        ("clean_import", "category_authority_context"),
        ("portable_package", "category_authority_context"),
        ("construction", "storage_category_authority_context"),
    ),
)
@pytest.mark.parametrize("field", CATEGORY_AUTHORITY_CONTEXT_FIELDS)
def test_every_category_authority_context_member_is_independently_required(
    owner, context_field, field
):
    value = evidence()
    value["storage_attribution"][owner][context_field].pop(field)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize("category", CATEGORY_NAMES)
def test_every_transient_authority_category_is_required(category):
    value = evidence()
    value["storage_attribution"]["construction"]["storage_transient_peak_authorities"].pop(category)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize(
    "component",
    (
        None,
        "source_project_and_construction",
        "portable_package",
        "clean_import_project",
        "drill_project_and_construction",
        "drill_package",
        "corrupt_drill_package",
    ),
)
def test_workspace_component_object_and_members_are_required(component):
    value = evidence()
    if component is None:
        value["storage_attribution"].pop("workspace_components")
    else:
        value["storage_attribution"]["workspace_components"].pop(component)
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


@pytest.mark.parametrize(
    "location",
    (
        "host_memory",
        "raw_attempts",
        "self_loops_rejected",
        "envelope_peak",
        "snapshot_total",
        "manifest_byte",
        "artifact_total",
        "construction_counter",
        "phase_io_counter",
        "phase_elapsed",
        "phase_rss",
        "phase_disk",
        "workspace_component",
    ),
)
def test_every_native_u64_schema_shape_rejects_above_u64(location):
    value = evidence()
    above_u64 = 1 << 64
    if location == "host_memory":
        value["host"]["memory_bytes"] = above_u64
    elif location in ("raw_attempts", "self_loops_rejected"):
        value["counts"][location] = above_u64
    elif location == "envelope_peak":
        value["envelope"]["peak_rss_bytes"] = above_u64
    elif location == "snapshot_total":
        value["storage_attribution"]["source"]["allocated_bytes"] = above_u64
    elif location == "manifest_byte":
        value["storage_attribution"]["source"]["generation_manifest_sha256"][0] = 256
    elif location == "artifact_total":
        value["storage_attribution"]["source"]["categories"]["topology_nodes"]["logical_bytes"] = (
            above_u64
        )
    elif location == "construction_counter":
        value["storage_attribution"]["construction"]["canonical_artifact_objects"] = above_u64
    elif location == "phase_io_counter":
        value["storage_attribution"]["application_io_phases"]["phases"]["append_merge"][
            "read_calls"
        ] = above_u64
    elif location == "phase_elapsed":
        value["phases"][0]["elapsed_ms"] = above_u64
    elif location == "phase_rss":
        value["phases"][0]["rss_peak_bytes"] = above_u64
    elif location == "phase_disk":
        value["phases"][0]["disk_peak_bytes"] = above_u64
    elif location == "workspace_component":
        value["storage_attribution"]["workspace_components"]["drill_package"] = above_u64
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(value)


def test_every_integer_schema_node_has_an_explicit_native_ceiling():
    schema = json.loads(VALIDATOR.SCHEMA.read_text())
    pending = [schema]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            if item.get("type") == "integer":
                assert "maximum" in item
                assert item["maximum"] <= (1 << 64) - 1
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


def test_rejects_synchronized_category_views_against_fixed_provider_result_anchor():
    original = evidence()
    original_encoded = encoded(original)
    anchored_provider = provider_result(original_encoded)
    anchored_provider_encoded = encoded(anchored_provider)
    external_provider_digest = hashlib.sha256(anchored_provider_encoded).hexdigest()
    value = json.loads(json.dumps(evidence()))
    for owner in ("source", "clean_import"):
        for view in ("categories", "category_authorities"):
            value["storage_attribution"][owner][view]["topology_nodes"]["logical_bytes"] -= 1
            value["storage_attribution"][owner][view]["topology_edges"]["logical_bytes"] += 1
        context = value["storage_attribution"][owner]["category_authority_context"]
        context["receipt_authority_sha256"] = "sha256:" + "f" * 64
        value["storage_attribution"][owner]["category_authority_sha256"] = {
            category: category_commitment(
                context,
                category,
                value["storage_attribution"][owner]["category_authorities"][category],
            )
            for category in CATEGORY_NAMES
        }
    forged_encoded = encoded(value)
    forged_provider = json.loads(json.dumps(anchored_provider))
    forged_provider["artifacts"]["graphforge_sha256"] = hashlib.sha256(forged_encoded).hexdigest()
    with pytest.raises(
        VALIDATOR.EvidenceError,
        match="provider result does not match its external anchor",
    ):
        validate(
            value,
            evidence_encoded=forged_encoded,
            provider=forged_provider,
            expected_provider_result_sha256=external_provider_digest,
        )


def test_provider_result_external_digest_authenticates_ordinary_receipt():
    value = evidence()
    evidence_encoded = encoded(value)
    provider = provider_result(evidence_encoded)
    provider_encoded = encoded(provider)
    wrong_external_digest = hashlib.sha256(provider_encoded + b"\n").hexdigest()
    with pytest.raises(
        VALIDATOR.EvidenceError,
        match="provider result does not match its external anchor",
    ):
        VALIDATOR.validate(
            evidence_encoded,
            SHA,
            provider_encoded,
            wrong_external_digest,
        )


def test_provider_result_graphforge_digest_binds_exact_ordinary_receipt_bytes():
    value = evidence()
    evidence_encoded = encoded(value)
    provider = provider_result(evidence_encoded)
    provider["artifacts"]["graphforge_sha256"] = HEX_B
    provider_encoded = encoded(provider)
    with pytest.raises(
        VALIDATOR.EvidenceError,
        match="ordinary evidence is not bound",
    ):
        VALIDATOR.validate(
            evidence_encoded,
            SHA,
            provider_encoded,
            hashlib.sha256(provider_encoded).hexdigest(),
        )


def test_authenticated_ordinary_receipt_must_match_sanitized_category_proof():
    value = evidence()
    value["storage_attribution"]["source"]["categories"]["topology_nodes"]["logical_bytes"] = 0
    with pytest.raises(
        VALIDATOR.EvidenceError,
        match="authority commitment differs",
    ):
        validate(value)


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("tools", "build", "018f6e45-7f12-7c00-8000-000000000001"),
        ("tools", "build", "00000000-0000-0000-0000-000000000000"),
        ("tools", "build", "/var/lib/graphforge/project"),
        (
            "tools",
            "build",
            "artifact=0011223344556677:00112233445566778899aabbccddeeff",
        ),
        ("tools", "machine_id", "redacted"),
        ("tools", "volume-id", "redacted"),
        ("tools", "provider_resource_id", "redacted"),
    ],
)
def test_recursive_sanitizer_rejects_raw_identity_path_and_sensitive_keys(section, key, value):
    unsafe = evidence()
    unsafe[section][key] = value
    with pytest.raises(VALIDATOR.EvidenceError):
        validate(unsafe)


@pytest.mark.parametrize(
    "tool_value",
    (
        "rustc 1.90.0 (1159e78c4 2026-08-01)",
        "graphforge-certify/0.5.2",
        "llvm:19.1.7",
        "sha256:aabbccddeeff00112233445566778899",
    ),
)
def test_recursive_sanitizer_accepts_legitimate_tool_versions(tool_value):
    value = evidence()
    value["tools"]["build"] = tool_value
    validate(value)


@pytest.mark.parametrize(
    "proof",
    [
        "source_export_generation_authenticated",
        "import_receipt_reopen_authenticated",
        "source_import_generations_distinct",
    ],
)
def test_generation_proofs_are_closed_and_required_true(proof):
    unsafe = evidence()
    unsafe["identities"][proof] = False
    with pytest.raises(VALIDATOR.EvidenceError):
        validate(unsafe)


@pytest.mark.parametrize(
    "mutation",
    [
        "short",
        "unreconciled",
        "run",
        "identity",
        "authority",
        "missing_authority",
        "path",
        "rss",
        "disk",
        "wall_time",
        "phase",
        "package",
        "equivalence",
        "missing_equivalence",
        "provider",
        "capacity",
        "failed_result",
        "missing_node_count",
        "extra_top_level",
        "duplicate_phase",
        "extra_phase",
        "out_of_order_phase",
        "placeholder_sku",
        "placeholder_region",
        "mutable_os_image",
        "provider_whitespace",
        "malformed_kernel",
    ],
)
def test_rejects_incomplete_or_unsafe_evidence(mutation):
    value = evidence()
    if mutation == "short":
        for key in ("raw_attempts", "live_unique_edges", "source_edges", "imported_edges"):
            value["counts"][key] -= 1
    if mutation == "unreconciled":
        value["counts"]["raw_attempts"] += 1
    if mutation == "run":
        value["run"]["seed"] = 2
    if mutation == "identity":
        value["identities"]["source_import_generations_distinct"] = False
    if mutation == "authority":
        value["authority"]["imported_fingerprint"] = DIGEST_B
    if mutation == "missing_authority":
        value.pop("authority")
    if mutation == "path":
        value["tools"]["rustc"] = "/usr/bin/rustc"
    if mutation == "rss":
        value["envelope"]["peak_rss_bytes"] = 137_438_953_473
    if mutation == "disk":
        value["envelope"]["peak_disk_bytes"] = 1_099_511_627_777
    if mutation == "wall_time":
        value["envelope"]["wall_time_s"] = 14_401
    if mutation == "phase":
        value["phases"].pop()
    if mutation == "package":
        value["package"]["class"] = "partial"
    if mutation == "equivalence":
        value["equivalence"]["imported_project_fingerprint"] = DIGEST_B
    if mutation == "missing_equivalence":
        value.pop("equivalence")
    if mutation == "provider":
        value["host"]["provider"] = "local"
    if mutation == "capacity":
        value["host"]["memory_bytes"] = 0
    if mutation == "failed_result":
        value["result"] = "fail"
        value["first_failure"] = "generate"
    if mutation == "missing_node_count":
        value["counts"].pop("source_nodes")
    if mutation == "extra_top_level":
        value["unexpected"] = True
    if mutation == "duplicate_phase":
        value["phases"][-1]["id"] = value["phases"][-2]["id"]
    if mutation == "extra_phase":
        value["phases"].append(
            {
                "id": "surprise",
                "status": "pass",
                "elapsed_ms": 1,
                "rss_peak_bytes": 1,
                "disk_peak_bytes": 1,
                "fingerprint": DIGEST_A,
            }
        )
    if mutation == "out_of_order_phase":
        value["phases"][0], value["phases"][1] = value["phases"][1], value["phases"][0]
    if mutation == "placeholder_sku":
        value["host"]["sku"] = "default"
    if mutation == "placeholder_region":
        value["host"]["region"] = "global"
    if mutation == "mutable_os_image":
        value["host"]["os_image"] = "Canonical:ubuntu:server:latest"
    if mutation == "provider_whitespace":
        value["host"]["provider"] = " Microsoft Azure"
    if mutation == "malformed_kernel":
        value["host"]["kernel"] = "6.8.0\nforged"
    with pytest.raises(VALIDATOR.EvidenceError):
        validate(value)


def test_requires_expected_sha():
    with pytest.raises(VALIDATOR.EvidenceError, match="expected-sha"):
        validate(evidence(), None)
    with pytest.raises(VALIDATOR.EvidenceError, match="40-hex"):
        validate(evidence(), "A" * 40)


@pytest.mark.parametrize(
    "field",
    [
        "read_bytes",
        "read_calls",
        "write_bytes",
        "write_calls",
        "file_fsync_calls",
        "directory_fsync_calls",
    ],
)
def test_rejects_unreconciled_manifest_operation_component(field):
    value = evidence()
    value["storage_attribution"]["construction"]["cas_publication_io"]["manifest"][field] += 1
    with pytest.raises(VALIDATOR.EvidenceError, match="CAS"):
        validate(value)


def test_rejects_manifest_path_read_misclassified_as_write():
    value = evidence()
    storage = value["storage_attribution"]
    storage["construction"]["cas_publication_io"]["manifest_reads"]["write_bytes"] = 1
    storage["construction"]["cas_application_write_bytes"] = 1
    storage["application_io_phases"]["phases"]["cas_install_read_write"]["write_bytes"] = 1
    with pytest.raises(VALIDATOR.EvidenceError, match="path authentication"):
        validate(value)


def test_rejects_overflowed_cas_component_sum():
    value = evidence()
    components = value["storage_attribution"]["construction"]["cas_publication_io"]
    components["manifest"]["read_bytes"] = (1 << 64) - 1
    with pytest.raises(VALIDATOR.EvidenceError, match="native u64"):
        validate(value)


@pytest.mark.parametrize("field", ["publications", "initial_entries", "changed_paths"])
def test_rejects_nonfresh_publication_using_fresh_control_bound(field):
    value = evidence()
    value["storage_attribution"]["construction"]["cas_publication_io"][field] += 1
    with pytest.raises(VALIDATOR.EvidenceError, match="one fresh publication"):
        validate(value)


def test_rejects_coherent_manifest_protocol_overcount():
    storage = evidence()["storage_attribution"]
    construction = storage["construction"]
    manifest = construction["cas_publication_io"]["manifest"]
    count = 67 * construction["canonical_artifact_objects"] + 2
    manifest.update(
        installed_objects=count,
        install_attempts=count,
        file_fsync_calls=count,
        directory_fsync_calls=2 * count,
    )
    manifest["read_calls"] = manifest["read_bytes"] = count + manifest["reused_objects"]
    for field, aggregate in (
        ("read_bytes", "cas_application_read_bytes"),
        ("read_calls", "cas_application_read_operations"),
    ):
        construction[aggregate] = (
            construction["cas_publication_io"]["payload"][field] + manifest[field]
        )
        storage["application_io_phases"]["phases"]["cas_install_read_write"][field] = construction[
            aggregate
        ]
    construction["cas_fsync_operations"] = 3 * count
    storage["application_io_phases"]["phases"]["cas_install_read_write"]["fsync_calls"] = 3 * count
    with pytest.raises(VALIDATOR.EvidenceError, match="native path/object"):
        VALIDATOR.validate_cas_publication_components(storage)


def test_rejects_coherent_omission_of_all_manifest_work():
    storage = evidence()["storage_attribution"]
    construction = storage["construction"]
    components = construction["cas_publication_io"]
    for name in ("manifest", "manifest_reads"):
        components[name] = dict.fromkeys(components[name], 0)
    phase = storage["application_io_phases"]["phases"]["cas_install_read_write"]
    for field, aggregate in (
        ("read_bytes", "cas_application_read_bytes"),
        ("read_calls", "cas_application_read_operations"),
    ):
        phase[field] = construction[aggregate] = components["payload"][field]
    with pytest.raises(VALIDATOR.EvidenceError, match="mandatory bootstrap"):
        VALIDATOR.validate_cas_publication_components(storage)
