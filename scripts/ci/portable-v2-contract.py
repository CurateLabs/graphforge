#!/usr/bin/env python3
"""Dependency-free consistency gate for the portable-v2 normative corpus."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/portable-v2"
SCHEMA = ROOT / "docs/contracts/graphforge-project-v2.schema.json"
DOMAIN = b"graphforge-project/2\0"


def canonical(value: object) -> bytes:
    # The golden vector uses the RFC 8785 common subset: integers, strings,
    # arrays, booleans and objects. Implementations still use a complete JCS
    # library for arbitrary schema-valid documents.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def canonical_ustar(path: str, payload: bytes) -> bytes:
    """Build the small-path normative ustar vector without using tar defaults."""
    header = bytearray(512)
    header[0 : len(path)] = path.encode()
    header[100:108] = b"0000644\0"
    header[108:116] = b"0000000\0"
    header[116:124] = b"0000000\0"
    header[124:136] = f"{len(payload):011o}\0".encode()
    header[136:148] = b"00000000000\0"
    header[148:156] = b"        "
    header[156:157] = b"0"
    header[257:263] = b"ustar\0"
    header[263:265] = b"00"
    header[148:156] = f"{sum(header):06o}\0 ".encode()
    padding = bytes((-len(payload)) % 512)
    return bytes(header) + payload + padding + bytes(1024)


def main() -> None:
    schema = json.loads(SCHEMA.read_text())
    assert schema["$id"].endswith("graphforge-project-v2.schema.json")
    manifest = json.loads((FIXTURES / "ontology-only.manifest.json").read_text())
    expected = manifest.pop("package_digest")
    actual = "sha256:" + hashlib.sha256(DOMAIN + canonical(manifest)).hexdigest()
    assert actual == expected, (actual, expected)

    cases = json.loads((FIXTURES / "cases.json").read_text())
    assert set(cases["positive_package_classes"]) == {
        "complete",
        "ontology-only",
        "component-selective",
        "graph-data-subset",
    }
    required_bundle_rules = {
        "utf8-path-order",
        "pax-length",
        "checksum",
        "size",
        "zero-padding",
        "two-zero-end-blocks",
        "no-trailing-bytes",
        "uncompressed",
    }
    assert required_bundle_rules <= set(cases["bundle_rules"])
    errors = dict(cases["negative_cases"])
    for name in (
        "absolute-path",
        "traversal",
        "symlink",
        "hard-link",
        "device",
        "fifo",
        "duplicate-normalized-path",
        "truncated-payload",
        "extra-file",
        "entry-count-overflow",
        "source-mutated",
    ):
        assert name in errors
    scale = cases["structural_cases"][0]
    assert scale["logical_edges"] >= 1_000_000_000
    assert scale["payload_bytes"] > 16 * 1024**3 - 1
    assert scale["requires_incremental_io"] is True
    byte_vectors = json.loads((FIXTURES / "bundle-byte-vectors.json").read_text())
    vector = byte_vectors["vectors"][0]
    archive = canonical_ustar(vector["path"], bytes.fromhex(vector["payload_hex"]))
    assert len(archive) == vector["archive_length"]
    assert hashlib.sha256(archive).hexdigest() == vector["archive_sha256"]
    vectors = json.loads((FIXTURES / "positive-vectors.json").read_text())
    assert {v["package_class"] for v in vectors["vectors"]} == set(
        cases["positive_package_classes"]
    )
    template = json.loads((FIXTURES / vectors["manifest_template"]).read_text())
    for vector in vectors["vectors"]:
        candidate = json.loads(json.dumps(template))
        candidate.pop("package_digest")
        candidate["package_class"] = vector["package_class"]
        if vector["package_class"] == "graph-data-subset":
            candidate["selection"]["graph_subset"] = {
                "closure": "induced-edges",
                "selector": "nodes:all",
            }
        digest = "sha256:" + hashlib.sha256(DOMAIN + canonical(candidate)).hexdigest()
        assert digest == vector["package_digest"]
        assert vector["representations"] == ["expanded", "bundle"]
    print("portable-v2 contract fixtures: PASS")


if __name__ == "__main__":
    main()
