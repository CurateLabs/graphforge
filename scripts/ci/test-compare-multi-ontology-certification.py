#!/usr/bin/env python3
"""Mutation tests for the #843 four-surface certification comparator."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile

SCRIPT = Path(__file__).with_name("compare-multi-ontology-certification.py")
SPEC = importlib.util.spec_from_file_location("certification_comparator", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def report(surface: str, plan_digit: str = "3") -> dict[str, object]:
    value: dict[str, object] = {
        "contract": MODULE.CONTRACT,
        "surface": surface,
        "composition_before": "1" * 64,
        "composition_after": "2" * 64,
        "migration_plan_digest": plan_digit * 64,
        "module_ids": ["urn:graphforge:evidence@v1", "urn:graphforge:research@v1"],
        "bridge_ids": ["urn:graphforge:research-evidence@v1"],
        "retained_data": {"rows_scanned": 1, "name": "Ada", "birth_year": 1815},
        "cases": {},
    }
    value["cases"] = {
        "authority_reopened": {"composition_fingerprint": value["composition_after"]},
        "bridge_set_retained": {"bridge_ids": value["bridge_ids"]},
        "migration_receipt": {"plan_digest": value["migration_plan_digest"]},
        "module_set_retained": {"module_ids": value["module_ids"]},
        "retained_data_query": value["retained_data"],
    }
    return value


def compare(values: dict[str, dict[str, object]]) -> list[str]:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        paths = {}
        for surface, value in values.items():
            path = root / f"{surface}.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            paths[surface] = path
        return MODULE.compare(paths)


def main() -> None:
    baseline = {surface: report(surface) for surface in MODULE.SURFACES}
    assert compare(baseline) == []

    source_bound = {
        surface: report(surface, str(index + 3)) for index, surface in enumerate(MODULE.SURFACES)
    }
    assert compare(source_bound) == []

    mutated = {surface: report(surface) for surface in MODULE.SURFACES}
    mutated["node"]["migration_plan_digest"] = "9" * 64
    assert any("node: cases must bind exact" in error for error in compare(mutated))

    mutated = {surface: report(surface) for surface in MODULE.SURFACES}
    mutated["node"]["cases"] = {"authority_reopened": True}
    assert any("node: cases must bind exact" in error for error in compare(mutated))

    mutated = {surface: report(surface) for surface in MODULE.SURFACES}
    mutated["python"]["retained_data"] = {"rows_scanned": 0, "name": "", "birth_year": True}
    errors = compare(mutated)
    assert any("python: retained_data values are invalid" in error for error in errors)

    mutated = {surface: report(surface) for surface in MODULE.SURFACES}
    mutated["cli"]["module_ids"] = list(reversed(mutated["cli"]["module_ids"]))
    assert any("cli: module_ids must be" in error for error in compare(mutated))

    mutated = {surface: report(surface) for surface in MODULE.SURFACES}
    mutated["rust"]["composition_after"] = mutated["rust"]["composition_before"]
    assert any("migration did not change" in error for error in compare(mutated))

    print("multi-ontology certification comparator mutation tests: PASS")


if __name__ == "__main__":
    main()
