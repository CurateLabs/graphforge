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

SHA = "a" * 40
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


def evidence():
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
            "scale": 26,
            "edgefactor": 16,
            "seed": 1,
            "directionality": "undirected",
            "self_loops": "drop",
            "duplicates": "drop",
        },
        "host": {
            "provider": "example",
            "sku": "cert",
            "os": "Linux",
            "kernel": "6",
            "filesystem": "xfs",
            "memory_bytes": 137_438_953_472,
            "nvme_bytes": 1_099_511_627_776,
        },
        "tools": {"rustc": "1.90"},
        "counts": {
            "raw_attempts": 1_000_000_002,
            "self_loops_rejected": 1,
            "duplicates_rejected": 1,
            "live_unique_edges": 1_000_000_000,
            "source_nodes": 67_108_864,
            "source_edges": 1_000_000_000,
            "imported_nodes": 67_108_864,
            "imported_edges": 1_000_000_000,
        },
        "identities": {
            "source_generation": "11111111-1111-1111-1111-111111111111",
            "package": DIGEST_A,
            "transport": DIGEST_B,
            "imported_generation": "22222222-2222-2222-2222-222222222222",
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
        "phases": phases,
        "envelope": {"peak_rss_bytes": 1, "peak_disk_bytes": 1, "wall_time_s": 1},
        "result": "pass",
        "first_failure": None,
    }


def test_accepts_complete_sanitized_evidence():
    schema = Path("docs/development/evidence/g500-certification.schema.json")
    contract = json.loads(schema.read_text())
    Draft202012Validator.check_schema(contract)
    Draft202012Validator(contract).validate(evidence())
    VALIDATOR.validate(evidence(), SHA)


@pytest.mark.parametrize("mutation", ["short", "identity", "authority", "path", "rss", "phase"])
def test_rejects_incomplete_or_unsafe_evidence(mutation):
    value = evidence()
    if mutation == "short":
        value["counts"]["live_unique_edges"] -= 1
    if mutation == "identity":
        value["identities"]["imported_generation"] = value["identities"]["source_generation"]
    if mutation == "authority":
        value["authority"]["imported_fingerprint"] = DIGEST_B
    if mutation == "path":
        value["tools"]["rustc"] = "/usr/bin/rustc"
    if mutation == "rss":
        value["envelope"]["peak_rss_bytes"] = 137_438_953_473
    if mutation == "phase":
        value["phases"].pop()
    with pytest.raises(VALIDATOR.EvidenceError):
        VALIDATOR.validate(value, SHA)
