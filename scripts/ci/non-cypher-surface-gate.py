#!/usr/bin/env python3
"""Omission gate for the checked-in Rust non-Cypher release inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "tests/contracts/non-cypher-rust-surface.json"
API_SRC = ROOT / "crates/graphforge-api/src"
ALGORITHMS = ROOT / "crates/graphforge-core/src/algorithms.rs"
ALLOWED_CLASSIFICATIONS = {
    "release-tested",
    "internal-helper",
    "designed-only",
    "compatibility",
    "introspection",
}
SKIP_MARKERS = ("#[ignore", "@skip", "pytest.mark.skip", "pytest.skip(")


def _matching_brace(text: str, opening: int) -> int:
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    raise ValueError("unbalanced Rust braces")


def public_methods() -> set[str]:
    """Return receiver-qualified public methods from every graphforge-api impl block."""
    found: set[str] = set()
    impl_re = re.compile(r"\bimpl(?:\s*<[^>{}]*>)?\s+([A-Za-z_][\w:]*)[^{}]*\{")
    fn_re = re.compile(r"\bpub\s+(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^>{}]*>)?\s*\(")
    for path in sorted(API_SRC.glob("*.rs")):
        text = path.read_text()
        for function in re.findall(r"^pub\s+(?:async\s+)?fn\s+(\w+)\s*\(", text, re.M):
            found.add(f"crate.{function}")
        for match in impl_re.finditer(text):
            receiver = match.group(1).split("::")[-1]
            end = _matching_brace(text, match.end() - 1)
            for method in fn_re.findall(text[match.end() : end]):
                found.add(f"{receiver}.{method}")
    return found


def algorithm_registry() -> set[str]:
    """Expand the macro-owned closed algorithm registry to all 94 wire identities."""
    text = ALGORITHMS.read_text()
    result: set[str] = set()
    pattern = re.compile(
        r"algorithm_enum!\((\w+),.*?AlgorithmVerb::(\w+),\s*\{(.*?)\}\s*"
        r"(?:,\s*aliases\s*\{.*?\})?\s*\);",
        re.S,
    )
    for _enum, verb, body in pattern.findall(text):
        for wire in re.findall(r"\w+\s*=>\s*\"([^\"]+)\"", body):
            result.add(f"{verb.lower()}.{wire}")
    return result


def classified_methods(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Apply frozen receiver-aware rules; the source digest prevents wildcard omission."""
    policy = manifest.get("method_policy")
    if not isinstance(policy, dict):
        raise ValueError("method_policy must be an object")
    defaults = policy.get("receiver_defaults", {})
    overrides = policy.get("overrides", {})
    if not isinstance(defaults, dict) or not isinstance(overrides, dict):
        raise ValueError("method_policy defaults and overrides must be objects")
    result: dict[str, dict[str, Any]] = {}
    for method in public_methods():
        receiver = method.split(".", 1)[0]
        classification = overrides.get(method, defaults.get(receiver))
        if classification is None:
            raise ValueError(f"unclassified receiver/method: {method}")
        result[method] = {
            "classification": classification,
            "test_refs": [],
            "reason": policy.get("reasons", {}).get(classification),
        }
    return result


def method_digest(methods: set[str]) -> str:
    return hashlib.sha256(("\n".join(sorted(methods)) + "\n").encode()).hexdigest()


def test_body(path: Path, symbol: str) -> tuple[str, str] | None:
    text = path.read_text()
    pattern = re.compile(
        rf"^\s*(?:pub\s+)?(?:async\s+)?(?P<kind>fn|def)\s+{re.escape(symbol)}\s*\(",
        re.M,
    )
    match = pattern.search(text)
    if match is None:
        return None
    prefix = text[max(0, match.start() - 600) : match.start()]
    if match.group("kind") == "fn":
        test_attributes = list(re.finditer(r"#\[(?:tokio::)?test(?:\([^]]*\))?\]", prefix))
        if not test_attributes:
            return ("", "")
        attributes = prefix[test_attributes[-1].start() :]
        if "}" in attributes:
            return ("", "")
        opening = text.find("{", match.end())
        return (attributes, text[opening + 1 : _matching_brace(text, opening)])
    decorators = re.search(r"(?P<attrs>(?:\s*@[A-Za-z0-9_.()]+\s*)*)$", prefix)
    end = re.search(r"^def\s+", text[match.end() :], re.M)
    stop = match.end() + end.start() if end else len(text)
    return ((decorators.group("attrs") if decorators else ""), text[match.end() : stop])


def validate_test_refs(owner: str, refs: Any) -> tuple[list[str], list[str]]:
    """Require exact named Rust/Python test symbols, never broad file markers."""
    errors: list[str] = []
    if not isinstance(refs, list) or not refs:
        return ([f"{owner}: release evidence has no test_refs"], [])
    bodies: list[str] = []
    for ref in refs:
        if not isinstance(ref, dict) or set(ref) != {"path", "symbol"}:
            errors.append(f"{owner}: malformed or broad test reference")
            continue
        path = ROOT / ref["path"]
        if not path.is_file():
            errors.append(f"{owner}: stale test path {ref['path']}")
            continue
        symbol = ref["symbol"]
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", symbol):
            errors.append(f"{owner}: invalid exact test symbol {symbol!r}")
            continue
        resolved = test_body(path, symbol)
        if resolved is None:
            errors.append(f"{owner}: stale test symbol {symbol!r}")
            continue
        attributes, body = resolved
        is_rust_test = bool(re.search(r"#\[(?:tokio::)?test(?:\([^]]*\))?\]", attributes))
        is_python_test = symbol.startswith("test_")
        if not is_rust_test and not is_python_test:
            errors.append(f"{owner}: referenced symbol is not a test")
            continue
        if any(marker in attributes for marker in SKIP_MARKERS):
            errors.append(f"{owner}: referenced test is skipped")
            continue
        bodies.append(body)
    return errors, bodies


def _entries(manifest: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    values = manifest.get(key)
    if not isinstance(values, dict):
        raise ValueError(f"{key} must map classifications to groups")
    mapped: dict[str, dict[str, Any]] = {}
    for classification, group in values.items():
        if classification not in ALLOWED_CLASSIFICATIONS or not isinstance(group, dict):
            raise ValueError(f"invalid {key} classification group: {classification}")
        ids = group.get("ids")
        if not isinstance(ids, list) or not all(isinstance(value, str) for value in ids):
            raise ValueError(f"{key}.{classification}.ids must be strings")
        for entry_id in ids:
            if entry_id in mapped:
                raise ValueError(f"duplicate {key} id: {entry_id}")
            mapped[entry_id] = {
                "classification": classification,
                "test_refs": group.get("test_refs", []),
                "reason": group.get("reason"),
            }
    return mapped


def validate(manifest_path: Path = MANIFEST) -> list[str]:
    manifest = json.loads(manifest_path.read_text())
    errors: list[str] = []
    if manifest.get("contract_version") != 1:
        errors.append("contract_version must be 1")
    try:
        methods = classified_methods(manifest)
        algorithm = _entries(manifest, "algorithm_registry")
        search = _entries(manifest, "search_contracts")
    except ValueError as error:
        return [str(error)]

    actual_methods = public_methods()
    expected_digest = manifest.get("public_method_digest")
    actual_digest = method_digest(actual_methods)
    if expected_digest != actual_digest:
        errors.append(
            "public method inventory changed; classify the exact receiver-qualified delta "
            f"(expected {expected_digest}, found {actual_digest})"
        )
    overrides = set(manifest["method_policy"].get("overrides", {}))
    if stale_overrides := sorted(overrides - actual_methods):
        errors.append("stale method policy overrides: " + ", ".join(stale_overrides))

    assignments: dict[str, list[str]] = {}
    evidence_groups = manifest.get("method_evidence_groups")
    if not isinstance(evidence_groups, dict):
        errors.append("method_evidence_groups must be an object")
    else:
        for group_name, group in evidence_groups.items():
            if not isinstance(group, dict) or not isinstance(group.get("ids"), list):
                errors.append(f"method evidence group {group_name}: malformed ids")
                continue
            ref_errors, bodies = validate_test_refs(
                f"method evidence group {group_name}", group.get("test_refs")
            )
            errors.extend(ref_errors)
            combined_body = "\n".join(bodies)
            for method in group["ids"]:
                assignments.setdefault(method, []).append(group_name)
                receiver, name = method.split(".", 1)
                if receiver == "crate":
                    call = rf"\b{re.escape(name)}\s*\("
                elif receiver == "CheckpointView":
                    call = rf"\bview\s*\.\s*{re.escape(name)}\s*\("
                elif receiver == "OpenRouterProviderSession":
                    call = rf"\bsession\s*\.\s*{re.escape(name)}\s*\("
                else:
                    call = rf"\.\s*{re.escape(name)}\s*\("
                if bodies and re.search(call, combined_body) is None:
                    errors.append(
                        f"method evidence group {group_name}: {method} is not called "
                        "by its exact test refs"
                    )
    release_methods = {
        method for method, entry in methods.items() if entry["classification"] == "release-tested"
    }
    if missing := sorted(release_methods - assignments.keys()):
        errors.append("release-tested methods without evidence group: " + ", ".join(missing))
    if extra := sorted(assignments.keys() - release_methods):
        errors.append("non-release or stale methods in evidence groups: " + ", ".join(extra))
    duplicates = sorted(method for method, groups in assignments.items() if len(groups) != 1)
    if duplicates:
        errors.append("methods assigned to multiple evidence groups: " + ", ".join(duplicates))

    actual_algorithm = algorithm_registry()
    if len(actual_algorithm) != 94:
        errors.append(
            "source algorithm registry must contain exactly 94 entries, "
            f"found {len(actual_algorithm)}"
        )
    if missing := sorted(actual_algorithm - algorithm.keys()):
        errors.append("unclassified algorithm entries: " + ", ".join(missing))
    if stale := sorted(algorithm.keys() - actual_algorithm):
        errors.append("stale algorithm entries: " + ", ".join(stale))

    required_search = set(manifest.get("required_search_contracts", []))
    if missing := sorted(required_search - search.keys()):
        errors.append("missing required search contracts: " + ", ".join(missing))
    if stale := sorted(search.keys() - required_search):
        errors.append("undeclared search contracts: " + ", ".join(stale))

    search_assignments: dict[str, list[str]] = {}
    search_evidence = manifest.get("search_evidence_groups")
    if not isinstance(search_evidence, dict):
        errors.append("search_evidence_groups must be an object")
    else:
        for group_name, group in search_evidence.items():
            if not isinstance(group, dict) or not isinstance(group.get("ids"), list):
                errors.append(f"search evidence group {group_name}: malformed ids")
                continue
            ref_errors, _ = validate_test_refs(
                f"search evidence group {group_name}", group.get("test_refs")
            )
            errors.extend(ref_errors)
            for contract in group["ids"]:
                search_assignments.setdefault(contract, []).append(group_name)
    if missing := sorted(required_search - search_assignments.keys()):
        errors.append("search contracts without evidence group: " + ", ".join(missing))
    if extra := sorted(search_assignments.keys() - required_search):
        errors.append("stale search contracts in evidence groups: " + ", ".join(extra))
    duplicates = sorted(
        contract for contract, groups in search_assignments.items() if len(groups) != 1
    )
    if duplicates:
        errors.append(
            "search contracts assigned to multiple evidence groups: " + ", ".join(duplicates)
        )

    for group, entries in (
        ("methods", methods),
        ("algorithm_registry", algorithm),
        ("search_contracts", search),
    ):
        for entry_id, entry in entries.items():
            classification = entry.get("classification")
            if classification not in ALLOWED_CLASSIFICATIONS:
                errors.append(f"{group} {entry_id}: invalid classification {classification!r}")
                continue
            reason = entry.get("reason")
            if classification != "release-tested" and not isinstance(reason, str):
                errors.append(f"{group} {entry_id}: non-release entry requires reason")
        for classification, registry_group in manifest[group].items() if group != "methods" else []:
            if classification == "release-tested":
                ref_errors, _ = validate_test_refs(
                    f"{group}.{classification}", registry_group.get("test_refs")
                )
                errors.extend(ref_errors)
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--report", type=Path, help="write a SHA-bound JSON success report")
    args = parser.parse_args()
    try:
        errors = validate(args.manifest)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"non-Cypher surface gate: {error}", file=sys.stderr)
        return 1
    if errors:
        print("non-Cypher surface gate failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    manifest = json.loads(args.manifest.read_text())
    report = {
        "contract_version": 1,
        "git_sha": os.environ.get("GITHUB_SHA", "local"),
        "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        "method_inventory_sha256": method_digest(public_methods()),
        "methods": len(classified_methods(manifest)),
        "method_evidence_groups": len(manifest["method_evidence_groups"]),
        "method_evidence_assignments": sum(
            len(group["ids"]) for group in manifest["method_evidence_groups"].values()
        ),
        "method_evidence_test_refs": sum(
            len(group["test_refs"]) for group in manifest["method_evidence_groups"].values()
        ),
        "release_tested_methods": sum(
            entry["classification"] == "release-tested"
            for entry in classified_methods(manifest).values()
        ),
        "non_release_methods": sum(
            entry["classification"] != "release-tested"
            for entry in classified_methods(manifest).values()
        ),
        "algorithm_registry_entries": len(_entries(manifest, "algorithm_registry")),
        "search_contracts": len(_entries(manifest, "search_contracts")),
        "search_evidence_groups": len(manifest["search_evidence_groups"]),
        "outcome": "passed",
    }
    if args.report:
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        "non-Cypher surface gate passed: "
        f"{report['methods']} methods, {report['algorithm_registry_entries']} algorithm entries, "
        f"{report['search_contracts']} search contracts, sha={report['git_sha']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
