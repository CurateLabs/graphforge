#!/usr/bin/env python3
"""Dependency-free consistency gate for the portable-v2 normative corpus."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import NoReturn

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/portable-v2"
SCHEMA = ROOT / "docs/contracts/graphforge-project-v2.schema.json"
COMPOSITION_SCHEMA = ROOT / "docs/contracts/graphforge-ontology-composition-v1.schema.json"
DOMAIN = b"graphforge-project/2\0"
COMPOSITION_DOMAIN = b"graphforge-ontology-composition/1\0"


def fail(message: str) -> NoReturn:
    raise SystemExit(f"portable-v2 contract error: {message}")


def reject_duplicate_members(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            fail(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def reject_surrogates(value: object) -> None:
    if isinstance(value, str):
        if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            fail("lone Unicode surrogate")
    elif isinstance(value, dict):
        for key, child in value.items():
            reject_surrogates(key)
            reject_surrogates(child)
    elif isinstance(value, list):
        for child in value:
            reject_surrogates(child)


def load_json(path: Path) -> object:
    value = json.loads(path.read_text(), object_pairs_hook=reject_duplicate_members)
    reject_surrogates(value)
    return value


def canonical(value: object) -> bytes:
    reject_surrogates(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def octal(value: int, width: int) -> bytes:
    encoded = f"{value:0{width - 1}o}\0".encode()
    if len(encoded) != width:
        fail(f"octal field overflow: value={value} width={width}")
    return encoded


def ustar_entry(name: str, payload: bytes, *, typeflag: bytes = b"0", prefix: str = "") -> bytes:
    name_bytes, prefix_bytes = name.encode(), prefix.encode()
    if len(name_bytes) > 100 or len(prefix_bytes) > 155:
        fail("ustar name/prefix overflow")
    header = bytearray(512)
    header[0 : len(name_bytes)] = name_bytes
    header[100:108] = b"0000644\0"
    header[108:116] = b"0000000\0"
    header[116:124] = b"0000000\0"
    header[124:136] = octal(len(payload), 12)
    header[136:148] = b"00000000000\0"
    header[148:156] = b"        "
    header[156:157] = typeflag
    header[257:263] = b"ustar\0"
    header[263:265] = b"00"
    header[345 : 345 + len(prefix_bytes)] = prefix_bytes
    header[148:156] = f"{sum(header):06o}\0 ".encode()
    return bytes(header) + payload + bytes((-len(payload)) % 512)


def pax_record(path: str) -> bytes:
    body = f"path={path}\n".encode()
    length = len(body) + 2
    while True:
        record = f"{length} ".encode() + body
        if len(record) == length:
            return record
        length = len(record)


def split_ustar_path(path: str) -> tuple[str, str] | None:
    encoded = path.encode()
    if len(encoded) <= 100:
        return path, ""
    for index in reversed([i for i, byte in enumerate(encoded) if byte == ord("/")]):
        prefix, name = encoded[:index], encoded[index + 1 :]
        if len(prefix) <= 155 and 0 < len(name) <= 100:
            return name.decode(), prefix.decode()
    return None


def canonical_tar(path: str, payload: bytes) -> bytes:
    split = split_ustar_path(path)
    entries = bytearray()
    if split is not None:
        name, prefix = split
        entries += ustar_entry(name, payload, prefix=prefix)
    else:
        suffix = hashlib.sha256(path.encode()).hexdigest()[:16]
        entries += ustar_entry(f"PaxHeaders/{suffix}", pax_record(path), typeflag=b"x")
        entries += ustar_entry(f"PaxFiles/{suffix}", payload)
    return bytes(entries) + bytes(1024)


EXPECTED_ERRORS = {
    "duplicate-json-member": "GF_INVALID_SEMANTIC_MANIFEST",
    "lone-unicode-surrogate": "GF_INVALID_SEMANTIC_MANIFEST",
    "unknown-major": "GF_UNSUPPORTED_FUTURE",
    "unknown-required-capability": "GF_UNSUPPORTED_FUTURE",
    "unknown-component-kind": "GF_UNSUPPORTED_FUTURE",
    "unknown-dependency-rule": "GF_UNSUPPORTED_FUTURE",
    "v1-on-v2-reader": "GF_UNSUPPORTED_LEGACY",
    "absolute-path": "GF_INVALID_PORTABLE_PATH",
    "traversal": "GF_INVALID_PORTABLE_PATH",
    "backslash-path": "GF_INVALID_PORTABLE_PATH",
    "non-nfc-path": "GF_INVALID_PORTABLE_PATH",
    "unicode-casefold-collision": "GF_DUPLICATE_PORTABLE_PATH",
    "duplicate-normalized-path": "GF_DUPLICATE_PORTABLE_PATH",
    "duplicate-participant-id": "GF_INVALID_DEPENDENCY_GRAPH",
    "unknown-required-dependency": "GF_INVALID_DEPENDENCY_GRAPH",
    "cyclic-required-dependency": "GF_INVALID_DEPENDENCY_GRAPH",
    "symlink": "GF_UNSUPPORTED_ENTRY_TYPE",
    "hard-link": "GF_UNSUPPORTED_ENTRY_TYPE",
    "device": "GF_UNSUPPORTED_ENTRY_TYPE",
    "fifo": "GF_UNSUPPORTED_ENTRY_TYPE",
    "sparse-tar": "GF_NONCANONICAL_BUNDLE",
    "global-pax": "GF_NONCANONICAL_BUNDLE",
    "compression": "GF_UNSUPPORTED_TRANSPORT",
    "duplicate-header": "GF_DUPLICATE_PORTABLE_PATH",
    "bad-header-field": "GF_NONCANONICAL_BUNDLE",
    "bad-padding": "GF_NONCANONICAL_BUNDLE",
    "bad-end-marker": "GF_TRUNCATED_BUNDLE",
    "trailing-bytes": "GF_NONCANONICAL_BUNDLE",
    "truncated-payload": "GF_TRUNCATED_BUNDLE",
    "missing-file": "GF_INTEGRITY_FAILED",
    "extra-file": "GF_CLOSED_WORLD_VIOLATION",
    "digest-mismatch": "GF_INTEGRITY_FAILED",
    "length-mismatch": "GF_INTEGRITY_FAILED",
    "entry-count-overflow": "GF_LIMIT_EXCEEDED",
    "entry-size-overflow": "GF_LIMIT_EXCEEDED",
    "total-size-overflow": "GF_LIMIT_EXCEEDED",
    "manifest-size-overflow": "GF_LIMIT_EXCEEDED",
    "path-size-overflow": "GF_LIMIT_EXCEEDED",
    "cancelled": "GF_CANCELLED",
    "source-mutated": "GF_SOURCE_CHANGED",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def require_invalid_json(raw: str, expected_fragment: str) -> None:
    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicate_members)
        reject_surrogates(value)
    except SystemExit as error:
        require(expected_fragment in str(error), f"wrong JSON rejection: {error}")
        return
    fail(f"hostile JSON accepted: {expected_fragment}")


def main() -> None:
    require_invalid_json('{"key":1,"key":2}', "duplicate JSON member")
    require_invalid_json('"\\ud800"', "lone Unicode surrogate")
    schema = load_json(SCHEMA)
    require(isinstance(schema, dict), "schema must be an object")
    require(schema.get("$id", "").endswith("graphforge-project-v2.schema.json"), "schema id")
    manifest = load_json(FIXTURES / "ontology-only.manifest.json")
    require(isinstance(manifest, dict), "manifest must be an object")
    expected = manifest.pop("package_digest")
    actual = "sha256:" + hashlib.sha256(DOMAIN + canonical(manifest)).hexdigest()
    require(actual == expected, f"manifest digest: {actual} != {expected}")

    cases = load_json(FIXTURES / "cases.json")
    require(isinstance(cases, dict), "cases must be an object")
    classes = {"complete", "ontology-only", "component-selective", "graph-data-subset"}
    require(set(cases["positive_package_classes"]) == classes, "package classes")
    required_rules = {
        "utf8-path-order",
        "ustar-name-prefix",
        "local-pax-path-only",
        "pax-length",
        "checksum",
        "size",
        "zero-padding",
        "two-zero-end-blocks",
        "no-trailing-bytes",
        "uncompressed",
    }
    require(required_rules <= set(cases["bundle_rules"]), "bundle rule coverage")
    require(dict(cases["negative_cases"]) == EXPECTED_ERRORS, "negative error mapping")
    scale = cases["structural_cases"][0]
    require(scale["logical_edges"] >= 1_000_000_000, "scale edge count")
    require(scale["payload_bytes"] > 16 * 1024**3 - 1, "scale payload size")
    require(scale["requires_incremental_io"] is True, "incremental I/O marker")
    require(0 < scale["max_copy_buffer_bytes"] <= 8 * 1024**2, "copy buffer bound")

    byte_vectors = load_json(FIXTURES / "bundle-byte-vectors.json")
    require(isinstance(byte_vectors, dict), "byte vectors must be an object")
    for vector in byte_vectors["vectors"]:
        archive = canonical_tar(vector["path"], bytes.fromhex(vector["payload_hex"]))
        require(len(archive) == vector["archive_length"], f"{vector['name']} length")
        require(
            hashlib.sha256(archive).hexdigest() == vector["archive_sha256"],
            f"{vector['name']} digest",
        )
        require(archive[-1024:] == bytes(1024), f"{vector['name']} end markers")

    vectors = load_json(FIXTURES / "positive-vectors.json")
    require(isinstance(vectors, dict), "positive vectors must be an object")
    require({v["package_class"] for v in vectors["vectors"]} == classes, "vector classes")
    template = load_json(FIXTURES / vectors["manifest_template"])
    require(isinstance(template, dict), "manifest template must be an object")
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
        require(digest == vector["package_digest"], f"{vector['package_class']} digest")
        require(vector["representations"] == ["expanded", "bundle"], "representations")

    multi = load_json(FIXTURES / "multi-ontology-vectors.json")
    require(isinstance(multi, dict), "multi-ontology vectors must be an object")
    require(multi["decision"] == "existing-versioned-compatibility", "M9 decision")
    require(multi["component"]["kind"] == "compatibility", "M9 component kind")
    require(multi["required_capability"] == "ontology-composition@1", "M9 capability")
    composition_schema = load_json(COMPOSITION_SCHEMA)
    require(isinstance(composition_schema, dict), "composition schema must be an object")
    require(
        composition_schema.get("$id", "").endswith(
            "graphforge-ontology-composition-v1.schema.json"
        ),
        "composition schema id",
    )
    composition = dict(multi["composition"])
    expected_composition = composition.pop("composition_digest")
    actual_composition = (
        "sha256:" + hashlib.sha256(COMPOSITION_DOMAIN + canonical(composition)).hexdigest()
    )
    require(actual_composition == expected_composition, "M9 composition digest")
    module_ids = [f"{item['module_id']}@{item['version']}" for item in composition["modules"]]
    bridge_ids = [
        f"{item['bridge_set_id']}@{item['version']}" for item in composition["bridge_sets"]
    ]
    require(module_ids == sorted(set(module_ids)), "M9 module identity order")
    require(bridge_ids == sorted(set(bridge_ids)), "M9 bridge identity order")
    active = composition["activation_profile"]
    require(active["active_modules"] == sorted(set(active["active_modules"])), "active modules")
    require(
        active["active_bridge_sets"] == sorted(set(active["active_bridge_sets"])),
        "active bridge sets",
    )
    require(set(active["active_modules"]) <= set(module_ids), "active module closure")
    require(set(active["active_bridge_sets"]) <= set(bridge_ids), "active bridge closure")
    for bridge in composition["bridge_sets"]:
        require(bridge["source_module"] in module_ids, "bridge source closure")
        require(bridge["target_module"] in module_ids, "bridge target closure")
    for feature_set in ("required_features", "optional_features"):
        values = composition[feature_set]
        require(values == sorted(set(values)), f"M9 {feature_set} order")
    forbidden = {
        "runtime_catalog_ids",
        "host_paths",
        "parser_versions",
        "machine_configuration",
        "session_state",
        "credentials",
        "tck_results",
    }
    require(set(multi["identity_exclusions"]) == forbidden, "M9 identity exclusions")
    require(set(multi["package_classes"]) == classes, "M9 closure classes")
    require(multi["representations"] == ["expanded", "bundle"], "M9 representations")
    require(all(not vector["mutation"] for vector in multi["negative_vectors"]), "M9 mutation")
    require(
        set(multi["older_v2_reader"].values()) == {"unsupported_future-before-payload"},
        "M9 older-reader behavior",
    )
    print("portable-v2 contract fixtures: PASS")


if __name__ == "__main__":
    main()
