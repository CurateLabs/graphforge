#!/usr/bin/env python3
"""Closed four-surface operation gate for composable multi-ontology (#842)."""

from __future__ import annotations

import argparse
from functools import cache
import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "tests/contracts/multi-ontology-surface-v1.json"
SURFACES = ("rust", "python", "node", "cli")
REQUIRED_OPERATIONS = {
    f"{domain}.{operation}"
    for domain, operations in {
        "module": (
            "list",
            "get",
            "inspect",
            "validate",
            "create_register",
            "import",
            "adopt",
            "preview_update",
            "update_replace",
            "preview_delete",
            "delete",
            "export",
        ),
        "bridge": (
            "list",
            "get",
            "inspect",
            "validate",
            "create_register",
            "import",
            "adopt",
            "preview_update",
            "update_replace",
            "preview_delete",
            "delete",
            "export",
        ),
        "activation": ("inspect", "change"),
        "composition": ("validate", "preflight", "resolution_explain"),
        "portable": (
            "inspect",
            "verify",
            "export",
            "import",
            "post_import_inspect",
            "post_import_adopt",
        ),
    }.items()
    for operation in operations
}
REQUIRED_CASES = {
    "positive_crud_import_export",
    "exact_identity_and_ambiguity",
    "dependency_blocked_deletion",
    "unsupported_future_portability",
    "cancellation",
    "idempotent_replay",
    "no_partial_import_or_authority_change",
    "bounded_structured_diagnostics",
    "deterministic_path_free_cli_json",
    "packaged_clean_install",
}
ASSERTION_MARKERS = {
    "rust_test": ("assert!", "assert_eq!", "assert_ne!", "matches!"),
    "python_test": ("assert ", "pytest.raises", "unittest"),
    "node_test": ("assert.", "assert("),
}
FORBIDDEN_OMISSION = re.compile(
    r"(?:^|[._/\-])(unexposed|unsupported|default|omit|none)(?:$|[._/\-])", re.I
)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def load_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    if not isinstance(value, dict):
        raise ValueError("surface inventory must be an object")
    return value


def _adapter_base(member: str) -> tuple[str, str | None]:
    match = re.fullmatch(r"(.+?)(?:\[([^][]+)\])?", member)
    if match is None:
        return member, None
    return match.group(1), match.group(2)


def _snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


@cache
def _surface_source(root: Path, surface: str) -> str:
    directories = {
        "rust": root / "crates/graphforge-api/src",
        "python": root / "crates/graphforge-bindings-py/src",
        "node": root / "crates/graphforge-bindings-node/src",
        "cli": root / "crates/graphforge-cli/src",
    }
    return "\n".join(path.read_text(encoding="utf-8") for path in directories[surface].glob("*.rs"))


def _source_contains_member(root: Path, surface: str, member: str) -> bool:
    base, _ = _adapter_base(member)
    if surface == "cli":
        text = _surface_source(root, surface).lower()
        # Clap derives kebab-case command names from enum variants. Requiring every
        # path segment prevents a stale command family/subcommand from passing.
        return all(
            re.search(rf"\b{re.escape(segment.replace('-', '_'))}\b", text.replace("-", "_"))
            for segment in base.split("/")
        )
    receiver, separator, name = base.partition(".")
    if not separator or not receiver or not name:
        return False
    if surface == "rust":
        expression = re.compile(rf"\bpub\s+(?:async\s+)?fn\s+{re.escape(name)}\s*\(")
        return expression.search(_surface_source(root, surface)) is not None
    if surface == "python":
        expression = re.compile(rf"\bfn\s+{re.escape(name)}\s*\(")
        return expression.search(_surface_source(root, surface)) is not None
    if surface == "node":
        rust_name = _snake(name)
        expressions = (
            re.compile(rf"\bpub\s+(?:async\s+)?fn\s+{re.escape(rust_name)}\s*\("),
            re.compile(rf"js_name\s*=\s*[\"']?{re.escape(name)}[\"']?"),
        )
        source = _surface_source(root, surface)
        return any(expression.search(source) for expression in expressions)
    raise AssertionError(surface)


def _matching_brace(text: str, opening: int) -> int:
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(opening, len(text)):
        char = text[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in "\"'`":
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
    raise ValueError("unbalanced test body")


def _test_body(path: Path, symbol: str, kind: str) -> str:
    text = path.read_text(encoding="utf-8")
    if kind == "rust_test":
        match = re.search(rf"\bfn\s+{re.escape(symbol)}\s*\(", text)
        if match is None:
            raise ValueError(f"stale Rust test symbol {symbol}")
        prefix = text[max(0, match.start() - 500) : match.start()]
        if not re.search(r"#\[(?:tokio::)?test(?:\([^]]*\))?\]\s*$", prefix):
            raise ValueError(f"{symbol} is not an exact Rust test")
        if "#[ignore" in prefix:
            raise ValueError(f"{symbol} is ignored")
        opening = text.find("{", match.end())
        return text[opening + 1 : _matching_brace(text, opening)]
    if kind == "python_test":
        match = re.search(
            rf"^(?P<indent>[ \t]*)def\s+{re.escape(symbol)}\s*\([^)]*\)"
            r"\s*(?:->\s*[^:]+)?\s*:",
            text,
            re.M,
        )
        if match is None:
            raise ValueError(f"stale Python test symbol {symbol}")
        prefix = text[max(0, match.start() - 300) : match.start()]
        if re.search(r"@pytest\.mark\.skip|@unittest\.skip", prefix):
            raise ValueError(f"{symbol} is skipped")
        indent = len(match.group("indent"))
        tail = text[match.end() :]
        end = re.search(rf"^(?: {{0,{indent}}})def\s+", tail, re.M)
        return tail[: end.start()] if end else tail
    if kind == "node_test":
        match = re.search(rf"\btest\(\s*['\"]{re.escape(symbol)}['\"]\s*,", text)
        if match is None:
            raise ValueError(f"stale Node test title {symbol}")
        if re.search(rf"\btest\.skip\(\s*['\"]{re.escape(symbol)}['\"]", text):
            raise ValueError(f"{symbol} is skipped")
        opening = text.find("{", match.end())
        return text[opening + 1 : _matching_brace(text, opening)]
    raise ValueError(f"unknown evidence kind {kind}")


def _validate_case_evidence(
    root: Path, surface: str, case: str, ref: object, errors: list[str]
) -> None:
    if not isinstance(ref, dict) or set(ref) != {"path", "symbol", "kind", "markers"}:
        errors.append(f"{case}/{surface}: malformed exact evidence reference")
        return
    if ref["kind"] not in ASSERTION_MARKERS:
        errors.append(f"{case}/{surface}: unsupported evidence kind {ref['kind']}")
        return
    markers = ref["markers"]
    if (
        not isinstance(markers, list)
        or len(markers) < 2
        or len(set(markers)) != len(markers)
        or not all(isinstance(marker, str) and marker.strip() for marker in markers)
        or case in markers
        or "multi-ontology-surface-v1.json" in markers
    ):
        errors.append(f"{case}/{surface}: requires at least two unique semantic markers")
        return
    path = root / ref["path"]
    if not path.is_file():
        errors.append(f"{case}/{surface}: stale evidence path {ref['path']}")
        return
    try:
        body = _test_body(path, ref["symbol"], ref["kind"])
    except (OSError, ValueError) as error:
        errors.append(f"{case}/{surface}: {error}")
        return
    compact = re.sub(r"\s+", " ", body).strip()
    if len(compact) < 40 or not any(token in body for token in ASSERTION_MARKERS[ref["kind"]]):
        errors.append(f"{case}/{surface}: empty or assertion-free evidence test")
    missing = [marker for marker in markers if marker not in body]
    if missing:
        errors.append(f"{case}/{surface}: evidence missing case markers: " + ", ".join(missing))
    without_inventory = body.replace("multi-ontology-surface-v1.json", "")
    if not any(marker in without_inventory for marker in markers):
        errors.append(f"{case}/{surface}: inventory-only evidence is forbidden")


def _validate_packaged_artifacts(manifest: dict[str, Any], root: Path, errors: list[str]) -> None:
    packaged = manifest.get("packaged_artifacts")
    expected = {"python_wheel", "node_package", "cli_binary"}
    if not isinstance(packaged, dict) or set(packaged) != expected:
        errors.append("packaged_artifacts must bind Python wheel, Node package, and CLI binary")
        return
    for artifact, ref in packaged.items():
        if not isinstance(ref, dict) or set(ref) != {
            "workflow",
            "oracle",
            "workflow_markers",
            "oracle_markers",
        }:
            errors.append(f"{artifact}: malformed packaged artifact evidence")
            continue
        workflow_markers = ref["workflow_markers"]
        oracle_markers = ref["oracle_markers"]
        if (
            not isinstance(workflow_markers, list)
            or len(workflow_markers) < 3
            or not isinstance(oracle_markers, list)
            or len(oracle_markers) < 2
            or not all(
                isinstance(marker, str) and marker
                for marker in [*workflow_markers, *oracle_markers]
            )
        ):
            errors.append(
                f"{artifact}: requires build, isolated install, and semantic execution markers"
            )
            continue
        path = root / ref["workflow"]
        oracle = root / ref["oracle"]
        if not path.is_file() or not oracle.is_file():
            errors.append(f"{artifact}: stale package workflow {ref['workflow']}")
            continue
        source = path.read_text(encoding="utf-8")
        missing = [marker for marker in workflow_markers if marker not in source]
        if missing:
            errors.append(f"{artifact}: package verification missing: " + ", ".join(missing))
        oracle_source = oracle.read_text(encoding="utf-8")
        missing = [marker for marker in oracle_markers if marker not in oracle_source]
        if missing:
            errors.append(f"{artifact}: packaged oracle missing semantics: " + ", ".join(missing))


def validate(manifest: dict[str, Any], root: Path = ROOT) -> list[str]:
    errors: list[str] = []
    if manifest.get("contract") != "graphforge-multi-ontology-four-surface/1":
        errors.append("wrong contract identity")
    if manifest.get("issue") != 842:
        errors.append("inventory must be bound to issue 842")
    policy = manifest.get("policy")
    if not isinstance(policy, dict) or policy.get("surfaces") != list(SURFACES):
        errors.append("policy must require rust, python, node, and cli in canonical order")
    elif policy.get("omission_default") != "forbidden":
        errors.append("default-unexposed policy is forbidden")

    operations = manifest.get("operations")
    if not isinstance(operations, list):
        return [*errors, "operations must be an array"]
    mapped: dict[str, dict[str, str]] = {}
    members: dict[str, dict[str, list[tuple[str, str | None, str]]]] = {
        surface: {} for surface in SURFACES
    }
    for index, operation in enumerate(operations):
        if not isinstance(operation, dict) or set(operation) != {"id", *SURFACES}:
            errors.append(f"operation {index} must contain only id and four surface mappings")
            continue
        operation_id = operation["id"]
        if not isinstance(operation_id, str) or not re.fullmatch(
            r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", operation_id
        ):
            errors.append(f"operation {index} has invalid id")
            continue
        if operation_id in mapped:
            errors.append(f"duplicate operation id: {operation_id}")
            continue
        mapped[operation_id] = operation
        for surface in SURFACES:
            member = operation[surface]
            if (
                not isinstance(member, str)
                or not member.strip()
                or FORBIDDEN_OMISSION.search(member)
            ):
                errors.append(f"{operation_id}/{surface}: missing or default-unexposed mapping")
                continue
            base, adapter = _adapter_base(member)
            members[surface].setdefault(base, []).append((member, adapter, operation_id))
            if not _source_contains_member(root, surface, member):
                errors.append(f"{operation_id}/{surface}: stale member {member}")

    actual = set(mapped)
    if missing := sorted(REQUIRED_OPERATIONS - actual):
        errors.append("missing canonical operations: " + ", ".join(missing))
    if extra := sorted(actual - REQUIRED_OPERATIONS):
        errors.append("undeclared canonical operations: " + ", ".join(extra))
    for surface, bases in members.items():
        for base, uses in bases.items():
            if len(uses) == 1:
                continue
            raw = [use[0] for use in uses]
            adapters = [use[1] for use in uses]
            if len(set(raw)) != len(raw) or any(adapter is None for adapter in adapters):
                errors.append(
                    f"{surface}: duplicate member mapping {base}: "
                    + ", ".join(use[2] for use in uses)
                )

    cases = manifest.get("required_conformance_cases")
    if not isinstance(cases, list) or not all(isinstance(case, str) for case in cases):
        errors.append("required_conformance_cases must be a string array")
    elif set(cases) != REQUIRED_CASES or len(cases) != len(set(cases)):
        errors.append("required conformance case inventory drifted")

    evidence = manifest.get("case_evidence")
    if not isinstance(evidence, dict) or set(evidence) != REQUIRED_CASES:
        errors.append("case_evidence must contain exactly all ten required cases")
    else:
        seen: set[tuple[str, str, str]] = set()
        for case in sorted(REQUIRED_CASES):
            case_refs = evidence[case]
            if not isinstance(case_refs, dict) or set(case_refs) != set(SURFACES):
                errors.append(f"{case}: evidence must contain exactly all four surfaces")
                continue
            for surface in SURFACES:
                ref = case_refs[surface]
                _validate_case_evidence(root, surface, case, ref, errors)
                if isinstance(ref, dict) and all(key in ref for key in ("path", "symbol", "kind")):
                    identity = (surface, ref["path"], ref["symbol"])
                    if identity in seen:
                        errors.append(
                            f"{case}/{surface}: test symbol reused across conformance cases"
                        )
                    seen.add(identity)
    _validate_packaged_artifacts(manifest, root, errors)
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    try:
        errors = validate(load_manifest(args.manifest), args.root)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        errors = [str(error)]
    if errors:
        print("multi-ontology four-surface gate: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(f"multi-ontology four-surface gate: PASS ({len(REQUIRED_OPERATIONS)} operations)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
