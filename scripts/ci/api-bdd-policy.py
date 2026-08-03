#!/usr/bin/env python3
"""Validate fail-closed public API BDD classifications and step sources."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import sys

LANGUAGES = {"rust", "python", "node"}
PRODUCT_TAGS = {
    "excluded-api-bdd": LANGUAGES,
    "excluded-node-api-bdd": {"node"},
}
BINDING_ONLY_SCENARIOS = {
    ("Graph Construction API", "Closed instances reject path selectors before coercion"),
    ("Lifecycle State", "LifecycleError on <method> after close"),
    ("Type Errors", "TypeError when add_edge source is not a NodeHandle"),
    ("Type Errors", "TypeError when add_edge destination is not a NodeHandle"),
    ("Type Errors", "TypeError on unsupported property value type"),
}
FORBIDDEN_SOURCE_PATTERNS = {
    "tests/features/conftest.py": (
        r"pytest\.xfail",
        r"pytest_runtest_makereport",
        r"wasxfail",
        r"_xfail_not_implemented",
        r"NotImplementedError",
    ),
    "tests/features/steps/api_steps.py": (
        r"pytest\.xfail",
        r"_xfail_not_implemented",
        r"NotImplementedError",
    ),
    "tests/features/node/step_definitions/api_steps.ts": (
        r'return\s+["\']pending["\']',
        r'new Error\(["\']not implemented["\']\)',
        r"pending skeleton",
    ),
    "crates/graphforge-api/tests/bdd/api_steps.rs": (
        r'last_error\s*=\s*Some\(["\'](?:lifecycle error:\s*)?not implemented',
        r"//\s*pending\b",
        r"pending skeleton",
    ),
}


@dataclass(frozen=True)
class Scenario:
    feature: str
    name: str
    tags: frozenset[str]
    path: str
    line: int


def parse_features(root: Path) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for path in sorted((root / "tests/features/api").glob("*.feature")):
        feature = None
        feature_tags: set[str] = set()
        pending_tags: set[str] = set()
        for line_no, raw in enumerate(path.read_text().splitlines(), 1):
            line = raw.strip()
            if line.startswith("@"):
                pending_tags.update(token.removeprefix("@") for token in line.split())
            elif line.startswith("Feature:"):
                feature = line.partition(":")[2].strip()
                feature_tags = set(pending_tags)
                pending_tags.clear()
            elif line.startswith(("Scenario:", "Scenario Outline:")):
                if feature is None:
                    raise ValueError(f"{path}:{line_no}: scenario precedes Feature")
                name = line.partition(":")[2].strip()
                scenarios.append(
                    Scenario(
                        feature,
                        name,
                        frozenset(feature_tags | pending_tags),
                        str(path.relative_to(root)),
                        line_no,
                    )
                )
                pending_tags.clear()
            elif line and not line.startswith("#"):
                pending_tags.clear()
    return scenarios


def load_inventory(root: Path) -> dict:
    path = root / "tests/contracts/api-bdd-exclusions.json"
    return json.loads(path.read_text())


def validate(root: Path, *, check_issues: bool = False) -> tuple[dict, list[str]]:
    errors: list[str] = []
    scenarios = parse_features(root)
    by_key = {(item.feature, item.name): item for item in scenarios}
    if len(by_key) != len(scenarios):
        errors.append("feature corpus contains duplicate feature/scenario keys")

    inventory = load_inventory(root)
    if not isinstance(inventory, dict):
        return {}, ["inventory must be an object"]
    if inventory.get("version") != 1:
        errors.append("inventory version must be 1")
    entries = inventory.get("exclusions")
    if not isinstance(entries, list):
        return {}, ["inventory exclusions must be a list"]

    inventory_keys: set[tuple[str, str]] = set()
    issue_numbers: set[int] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            errors.append(f"inventory exclusion at index {index} must be an object")
            continue
        key = (entry.get("feature"), entry.get("scenario"))
        if key in inventory_keys:
            errors.append(f"duplicate inventory entry: {key}")
            continue
        inventory_keys.add(key)
        scenario = by_key.get(key)
        if scenario is None:
            errors.append(f"stale inventory entry: {key}")
            continue
        issue = entry.get("issue")
        if not isinstance(issue, int) or issue <= 0:
            errors.append(f"{key}: issue must be a positive integer")
            continue
        issue_numbers.add(issue)
        languages = set(entry.get("languages", []))
        if not languages or not languages <= LANGUAGES:
            errors.append(f"{key}: invalid languages {sorted(languages)}")
        matching_tags = set(PRODUCT_TAGS) & set(scenario.tags)
        if len(matching_tags) != 1:
            errors.append(
                f"{scenario.path}:{scenario.line}: expected exactly one product exclusion tag"
            )
            continue
        tag = matching_tags.pop()
        if languages != PRODUCT_TAGS[tag]:
            errors.append(
                f"{key}: {tag} requires languages {sorted(PRODUCT_TAGS[tag])}, "
                f"got {sorted(languages)}"
            )
        if f"issue-{issue}" not in scenario.tags:
            errors.append(f"{key}: missing @issue-{issue}")
        issue_tags = {tag for tag in scenario.tags if re.fullmatch(r"issue-\d+", tag)}
        if issue_tags != {f"issue-{issue}"}:
            errors.append(f"{key}: expected exactly @issue-{issue}, got {sorted(issue_tags)}")
    for scenario in scenarios:
        product_tags = set(PRODUCT_TAGS) & set(scenario.tags)
        if product_tags and (scenario.feature, scenario.name) not in inventory_keys:
            errors.append(f"{scenario.path}:{scenario.line}: exclusion is missing from inventory")
        if {"skip-node", "skip-rust"} & set(scenario.tags):
            errors.append(f"{scenario.path}:{scenario.line}: stale language skip tag")
        if (
            "binding-only" in scenario.tags
            and (
                scenario.feature,
                scenario.name,
            )
            not in BINDING_ONLY_SCENARIOS
        ):
            errors.append(
                f"{scenario.path}:{scenario.line}: unapproved binding-only classification"
            )

    actual_binding_only = {
        (scenario.feature, scenario.name)
        for scenario in scenarios
        if "binding-only" in scenario.tags
    }
    for key in sorted(BINDING_ONLY_SCENARIOS - actual_binding_only):
        errors.append(f"missing binding-only classification: {key}")

    for relative, patterns in FORBIDDEN_SOURCE_PATTERNS.items():
        source = root / relative
        if not source.is_file():
            errors.append(f"{relative}: required step source is missing")
            continue
        text = source.read_text()
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                line = text.count("\n", 0, match.start()) + 1
                errors.append(f"{relative}:{line}: forbidden fail-open pattern {pattern!r}")

    if check_issues:
        for issue in sorted(issue_numbers):
            try:
                result = subprocess.run(
                    [
                        "gh",
                        "issue",
                        "view",
                        str(issue),
                        "--json",
                        "state",
                        "--jq",
                        ".state",
                    ],
                    cwd=root,
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=30,
                )
            except (OSError, subprocess.TimeoutExpired) as error:
                errors.append(f"issue #{issue} could not be verified: {error}")
                continue
            if result.returncode != 0:
                errors.append(f"issue #{issue} could not be verified: {result.stderr.strip()}")
            elif result.stdout.strip() != "OPEN":
                errors.append(f"issue #{issue} is not open")

    counts = {
        "scenarios": len(scenarios),
        "product_exclusions": len(entries),
        "binding_only_not_applicable_to_rust": sum(
            1 for scenario in scenarios if "binding-only" in scenario.tags
        ),
        "required": {
            language: sum(
                1
                for scenario in scenarios
                if "binding-only" not in scenario.tags or language != "rust"
                if not any(
                    tag in scenario.tags and language in languages
                    for tag, languages in PRODUCT_TAGS.items()
                )
            )
            for language in sorted(LANGUAGES)
        },
        "issues": sorted(issue_numbers),
    }
    return counts, errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--check-issues", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    counts, errors = validate(root, check_issues=args.check_issues)
    report = {"contract": "graphforge-api-bdd-policy/1", "counts": counts, "errors": errors}
    output = args.output or root / "target/api-bdd-policy.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if errors:
        for error in errors:
            print(f"api-bdd-policy: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
