#!/usr/bin/env python3
"""Mutation tests for the non-Cypher binding parity policy."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CHECKER_PATH = ROOT / "scripts/ci/check-binding-parity-policy.py"
PYTHON_GATE_PATH = ROOT / "crates/graphforge-bindings-py/tests/non_cypher_release.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def expect_rejection(call, message: str) -> None:
    try:
        call()
    except AssertionError:
        return
    raise AssertionError(message)


def main() -> None:
    checker = load_module("binding_parity_checker", CHECKER_PATH)
    checker.main()

    node_policy = json.loads(checker.NODE_POLICY.read_text(encoding="utf-8"))
    node_policy["classification"]["equivalent"].append("GraphForge.add_nodes")
    with tempfile.TemporaryDirectory(prefix="gf-binding-parity-") as directory:
        mutated_policy = Path(directory) / "node-policy.json"
        mutated_policy.write_text(json.dumps(node_policy), encoding="utf-8")
        checker.NODE_POLICY = mutated_policy
        expect_rejection(
            checker.main,
            "Node parity accepted a removed Rust method based on name coincidence",
        )

    python_gate = load_module("python_binding_parity_mutation", PYTHON_GATE_PATH)
    python_gate.PYTHON_ONLY_METHODS = frozenset(
        python_gate.PYTHON_ONLY_METHODS - {"GraphForge.add_nodes", "GraphForge.add_edges"}
    )
    expect_rejection(
        python_gate._classification_report,
        "Python parity accepted unclassified language-specific convenience methods",
    )
    print("binding parity policy mutation tests passed")


if __name__ == "__main__":
    main()
