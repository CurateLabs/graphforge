from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

from jsonschema import Draft202012Validator
import pytest

SCRIPT = Path(__file__).with_name("validate-g500-certification.py")
SPEC = importlib.util.spec_from_file_location("g500_validator", SCRIPT)
assert SPEC and SPEC.loader
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)
BUILDER_SCRIPT = Path(__file__).with_name("build-g500-ladder-qualification.py")
BUILDER_SPEC = importlib.util.spec_from_file_location("g500_builder", BUILDER_SCRIPT)
assert BUILDER_SPEC and BUILDER_SPEC.loader
BUILDER = importlib.util.module_from_spec(BUILDER_SPEC)
BUILDER_SPEC.loader.exec_module(BUILDER)
QUALIFICATION_SCRIPT = Path(__file__).with_name("validate-g500-ladder-qualification.py")
QUALIFICATION_SPEC = importlib.util.spec_from_file_location(
    "g500_qualification_validator", QUALIFICATION_SCRIPT
)
assert QUALIFICATION_SPEC and QUALIFICATION_SPEC.loader
QUALIFICATION = importlib.util.module_from_spec(QUALIFICATION_SPEC)
QUALIFICATION_SPEC.loader.exec_module(QUALIFICATION)

SHA = "a" * 40
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


def artifact_totals(unit=1):
    return {
        "logical_references": unit,
        "logical_bytes": unit,
        "physical_objects": unit,
        "physical_logical_bytes": unit,
        "allocated_bytes": unit,
    }


def storage_attribution(unit=1):
    category_names = (
        "topology_nodes", "topology_edges", "properties", "uuid_and_surrogates",
        "adjacency", "catalog_and_manifests", "construction_staging",
        "portable_package", "clean_imported_project", "other",
    )
    categories = {
        name: artifact_totals(unit if index < 6 else 0)
        for index, name in enumerate(category_names)
    }
    snapshot = {
        "generation_manifest_sha256": [1] * 32,
        "categories": categories,
        "logical_references": 6 * unit,
        "logical_bytes": 6 * unit,
        "physical_objects": 6 * unit,
        "physical_logical_bytes": 6 * unit,
        "allocated_bytes": 6 * unit,
    }
    contract = json.loads(VALIDATOR.SCHEMA.read_text())
    construction = {
        field: 0 for field in contract["$defs"]["construction"]["required"]
    }
    construction["storage_current"] = {
        name: artifact_totals(unit) for name in category_names
    }
    construction["storage_transient_peak_allocated_bytes"] = {
        name: unit for name in category_names
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
    totals = {
        field: sum(values[field] for values in phases.values())
        for field in next(iter(phases.values()))
    }
    return {
        "source": snapshot,
        "source_project_current_allocated_bytes": 7 * unit,
        "portable_package": {
            "category": "portable_package", "logical_bytes": unit,
            "allocated_bytes": unit, "logical_references": unit,
            "physical_objects": unit, "source": "portable_writer_receipt",
        },
        "clean_import": snapshot,
        "construction": construction,
        "application_io_phases": {"phases": phases, "totals": totals},
        "workspace_current_allocated_bytes": 14 * unit,
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
        "storage_attribution": storage_attribution(unit),
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


def test_accepts_complete_sanitized_evidence():
    schema = VALIDATOR.ROOT / "docs/development/evidence/g500-certification.schema.json"
    contract = json.loads(schema.read_text())
    Draft202012Validator.check_schema(contract)
    Draft202012Validator(contract).validate(evidence())
    VALIDATOR.validate(evidence(), SHA)


def test_actual_certification_contract_builds_and_validates_adjacent_qualification(
    tmp_path, monkeypatch
):
    low = evidence(20, 1)
    high = evidence(22, 4)
    for document in (low, high):
        VALIDATOR.validate(document, SHA)
    low_path = tmp_path / "s20.json"
    high_path = tmp_path / "s22.json"
    output = tmp_path / "qualification.json"
    low_path.write_text(json.dumps(low))
    high_path.write_text(json.dumps(high))
    monkeypatch.setattr(
        "sys.argv",
        [
            str(BUILDER_SCRIPT), str(low_path), str(high_path), str(output),
            "--volume-bytes", str(500 * 1024**3),
            "--reserved-headroom-bytes", str(75 * 1024**3),
        ],
    )
    BUILDER.main()
    qualification = json.loads(output.read_text())
    QUALIFICATION.validate(qualification)
    assert qualification["projection"]["source_rungs"] == ["S20", "S22"]
    for source, rung in zip((low, high), qualification["rungs"], strict=True):
        selected = source["storage_attribution"]["source"]["allocated_bytes"]
        project_union = rung["source_project_current_allocated_bytes"]
        assert project_union > selected
        assert rung["ratios"]["authoritative_project_bytes_per_live_edge"] == {
            "numerator_bytes": project_union,
            "denominator_count": rung["live_edges"],
        }


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("tools", "build", "018f6e45-7f12-7c00-8000-000000000001"),
        ("tools", "build", "00000000-0000-0000-0000-000000000000"),
        ("tools", "build", "/var/lib/graphforge/project"),
        ("tools", "machine_id", "redacted"),
        ("tools", "volume-id", "redacted"),
        ("tools", "provider_resource_id", "redacted"),
    ],
)
def test_recursive_sanitizer_rejects_raw_identity_path_and_sensitive_keys(
    section, key, value
):
    unsafe = evidence()
    unsafe[section][key] = value
    with pytest.raises(VALIDATOR.EvidenceError):
        VALIDATOR.validate(unsafe, SHA)


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
        VALIDATOR.validate(unsafe, SHA)


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
        VALIDATOR.validate(value, SHA)


def test_requires_expected_sha():
    with pytest.raises(VALIDATOR.EvidenceError, match="expected-sha"):
        VALIDATOR.validate(evidence(), None)
    with pytest.raises(VALIDATOR.EvidenceError, match="40-hex"):
        VALIDATOR.validate(evidence(), "A" * 40)
