#!/usr/bin/env python3
"""Validate the normative ADR 0022 contract fixtures without runtime semantics."""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import unicodedata

ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "multi-ontology-v1"
HEX_256 = re.compile(r"^[0-9a-f]{64}$")
MODES = {"exploratory", "advisory", "strict"}
REQUIRED_CODES = {
    "resolution.ambiguous",
    "bridge.endpoint_missing",
    "dependency.cycle",
    "dependency.missing",
    "dependency.in_use",
    "enforcement.violation",
    "inventory.generation_conflict",
    "resource.modules",
    "interchange.integrity",
    "lifecycle.cancelled",
    "interchange.unsupported_future",
}


def load(name: str) -> dict:
    text = (FIXTURES / name).read_text(encoding="utf-8")
    return json.loads(text, object_pairs_hook=_unique_object)


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        assert key not in result, f"duplicate JSON member: {key}"
        result[key] = value
    return result


def module_key(module: dict) -> tuple[bytes, bytes, bytes]:
    return tuple(
        module[key].encode() for key in ("ontology_id", "authored_version", "canonical_digest")
    )


def bridge_key(bridge: dict) -> tuple[bytes, bytes, bytes]:
    return tuple(
        bridge[key].encode() for key in ("bridge_id", "authored_version", "canonical_digest")
    )


def fingerprint(doc: dict) -> str:
    semantic = {
        "activation": sorted(
            doc["activation"],
            key=lambda item: (
                item["scope"].encode(),
                item["subject"].encode(),
                item["mode"].encode(),
            ),
        ),
        "bridges": sorted(doc["bridges"], key=bridge_key),
        "modules": sorted(doc["modules"], key=module_key),
    }
    canonical = json.dumps(
        semantic, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    return hashlib.sha256(b"graphforge-ontology-composition/1\0" + canonical).hexdigest()


canonical = load("canonical.json")
adversarial = load("adversarial.json")
assert canonical["version"] == adversarial["version"] == 1
assert canonical["profile_default"] in MODES
assert len(canonical["modules"]) == 6
assert canonical["modules"] == sorted(canonical["modules"], key=module_key)
assert canonical["bridges"] == sorted(canonical["bridges"], key=bridge_key)
for record in canonical["modules"]:
    assert record["ontology_id"].startswith("https://")
    assert unicodedata.normalize("NFC", record["ontology_id"]) == record["ontology_id"]
    assert HEX_256.fullmatch(record["canonical_digest"])
for record in canonical["bridges"]:
    assert record["provenance"].startswith("urn:")
    assert HEX_256.fullmatch(record["canonical_digest"])
for record in canonical["activation"]:
    assert record["mode"] in MODES and record["scope"] in {"module", "bridge"}

expected = fingerprint(canonical)
reversed_inventory = {
    **canonical,
    "modules": list(reversed(canonical["modules"])),
    "bridges": list(reversed(canonical["bridges"])),
}
assert fingerprint(reversed_inventory) == expected, "inventory order changed composition identity"
assert HEX_256.fullmatch(expected)
assert canonical["expected"]["ambiguous_unqualified"]["candidates"] == [
    "genealogy:Person",
    "provenance:Person",
]

cases = adversarial["cases"]
assert {case["code"] for case in cases} == REQUIRED_CODES
assert len({case["name"] for case in cases}) == len(cases)
assert all(case["preserves_authority"] is True for case in cases)
assert all(isinstance(value, int) and value > 0 for value in adversarial["limits"].values())
security_cases = adversarial["security_cases"]
assert {case["name"] for case in security_cases} == {
    "duplicate-json-member",
    "implicit-import-adoption",
    "inventory-order-precedence",
    "name-based-equivalence",
    "non-nfc-identifier",
    "runtime-catalog-id-in-semantic-identity",
    "unbounded-diagnostic-candidates",
}
assert all(case["disposition"] == "reject_before_mutation" for case in security_cases)
print(
    "multi-ontology contract fixtures: PASS "
    f"({len(cases)} typed and {len(security_cases)} security cases, fingerprint {expected})"
)
