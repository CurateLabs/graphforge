#!/usr/bin/env python3
"""Fail required CI when Rust/Python/Node parity decisions drift."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
RUST_MANIFEST = ROOT / "tests/contracts/non-cypher-rust-surface.json"
NODE_POLICY = ROOT / "crates/gf-bindings-node/tests/non-cypher-parity-policy.json"
PYTHON_GATE = ROOT / "crates/gf-bindings-py/tests/non_cypher_release.py"
PYTHON_STUB_GATE = ROOT / "crates/gf-bindings-py/tests/stub_surface.py"


def digest(values: set[str]) -> str:
    return hashlib.sha256(("\n".join(sorted(values)) + "\n").encode()).hexdigest()


def main() -> None:
    manifest = json.loads(RUST_MANIFEST.read_text())
    release = {
        method_id
        for group in manifest["method_evidence_groups"].values()
        for method_id in group["ids"]
    }
    node = json.loads(NODE_POLICY.read_text())
    assert len(release) == node["releaseSurfaceCount"]
    assert digest(release) == node["releaseSurfaceDigest"]
    assert set(node["rustEvidenceGroupMap"]) == set(manifest["method_evidence_groups"])
    assert set(node["rustEvidenceGroupMap"].values()) == set(node["evidence"])

    equivalent = set(node["classification"]["equivalent"])
    required_equivalent = set(node["requiredEquivalent"])
    adapters = set(node["classification"]["languageSpecific"])
    assert required_equivalent <= release
    assert required_equivalent <= equivalent
    assert not equivalent & adapters
    assert equivalent | adapters <= release
    defaults = node["classification"]["notExposedDefaults"]
    for method_id in release - equivalent - adapters:
        receiver = method_id.split(".", 1)[0]
        assert receiver in defaults, f"unclassified Node release entry: {method_id}"
    for method_id, rationale in node["classification"]["nodeOnly"].items():
        assert "." in method_id and rationale.strip()

    test_root = ROOT / "crates/gf-bindings-node/tests"
    for group, files in node["evidence"].items():
        assert files, f"{group} has no exact Node evidence"
        for filename, titles in files.items():
            source = (test_root / filename).read_text()
            assert titles, f"{group}/{filename} has no exact test identity"
            for title in titles:
                assert re.search(rf"\btest\(\s*['\"]{re.escape(title)}['\"]", source), (
                    f"stale Node evidence {filename}: {title}"
                )
            assert not re.search(r"\b(?:test|describe)\.skip\s*\(", source)

    spec = importlib.util.spec_from_file_location("python_binding_parity", PYTHON_GATE)
    assert spec and spec.loader
    python_gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(python_gate)
    python_gate.check_surface_projection()

    stub_spec = importlib.util.spec_from_file_location("python_stub_surface", PYTHON_STUB_GATE)
    assert stub_spec and stub_spec.loader
    stub_gate = importlib.util.module_from_spec(stub_spec)
    stub_spec.loader.exec_module(stub_gate)
    stub_gate.main()


if __name__ == "__main__":
    main()
