#!/usr/bin/env python3
"""Fail-closed source-backed property-overlay v1 contract gate (#940)."""

from __future__ import annotations

import argparse
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "tests/contracts/property-overlay-v1.json"
OVERLAY = Path("crates/graphforge-storage/src/property_overlay.rs")
LIB = Path("crates/graphforge-storage/src/lib.rs")
WRITER = Path("crates/graphforge-storage/src/writer.rs")
STORAGE_BUILD = Path("crates/graphforge-storage/BUILD.bazel")
ROOT_BUILD = Path("BUILD.bazel")


class ContractError(ValueError):
    """The frozen ledger or its Rust authority drifted."""


def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    if not isinstance(value, dict):
        raise ContractError("contract must be an object")
    return value


def block(text: str, start_pattern: str) -> str:
    masked = rust_mask(text)
    matches = list(re.finditer(start_pattern, masked))
    if len(matches) != 1:
        raise ContractError(f"missing or duplicate Rust block: {start_pattern}")
    match = matches[0]
    opening = masked.find("{", match.end())
    depth = 0
    for index in range(opening, len(text)):
        if masked[index] == "{":
            depth += 1
        elif masked[index] == "}":
            depth -= 1
            if depth == 0:
                return text[opening + 1 : index]
    raise ContractError(f"unterminated Rust block: {start_pattern}")


def call_block(text: str, start_pattern: str) -> str:
    match = re.search(start_pattern, text)
    if match is None:
        raise ContractError(f"missing Bazel call: {start_pattern}")
    opening = text.rfind("(", match.start(), match.end())
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "(":
            depth += 1
        elif text[index] == ")":
            depth -= 1
            if depth == 0:
                return text[opening + 1 : index]
    raise ContractError(f"unterminated Bazel call: {start_pattern}")


@lru_cache(maxsize=128)
def rust_mask(text: str) -> str:
    """Hide comments/literals while retaining offsets and code delimiters."""
    result = list(text)
    raw_pattern = re.compile(r'(?:br|r)(#*)"')
    char_pattern = re.compile(r"'(?:\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|[\s\S])|[^'\\\n])'")
    index = 0
    while index < len(text):
        start = index
        if text.startswith("//", index):
            end = text.find("\n", index)
            index = len(text) if end < 0 else end
        elif text.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(text) and depth:
                if text.startswith("/*", index):
                    depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ContractError("unterminated Rust comment")
        elif raw := raw_pattern.match(text, index):
            closing = '"' + raw.group(1)
            end = text.find(closing, index + len(raw.group(0)))
            if end < 0:
                raise ContractError("unterminated Rust raw string")
            index = end + len(closing)
        elif text[index] == '"':
            index += 1
            while index < len(text):
                if text[index] == "\\":
                    index += 2
                elif text[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            else:
                raise ContractError("unterminated Rust string")
        elif char := char_pattern.match(text, index):
            index += len(char.group(0))
        else:
            index += 1
            continue
        for position in range(start, min(index, len(text))):
            if text[position] != "\n":
                result[position] = " "
    return "".join(result)


def top_level_matches(text: str, pattern: str):
    masked = rust_mask(text)
    depths = []
    depth = 0
    for character in masked:
        depths.append(depth)
        depth += (character == "{") - (character == "}")
    if depth:
        raise ContractError("unbalanced Rust source")
    return [match for match in re.finditer(pattern, masked, re.M) if depths[match.start()] == 0]


def module_sources(root: Path, entry: Path) -> dict[tuple[str, ...], str]:
    """Read only production modules reachable from this authority root."""
    sources: dict[tuple[str, ...], str] = {}
    seen: set[Path] = set()
    entry_path = (root / entry).resolve()
    authority = entry_path.with_suffix("")

    def visit(path: Path, name: tuple[str, ...]) -> None:
        resolved = path.resolve()
        if (
            (resolved != entry_path and not resolved.is_relative_to(authority))
            or resolved in seen
            or not path.is_file()
        ):
            raise ContractError(f"invalid or duplicate Rust module: {path}")
        seen.add(resolved)
        text = path.read_text(encoding="utf-8")
        sources[name] = text
        directory = path.parent if path.name == "mod.rs" else path.with_suffix("")
        for match in top_level_matches(text, r"^(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;"):
            # Existing production ownership modules are unconditional. Test-only
            # descendants do not supply production symbols or implementations.
            prefix = rust_mask(text)[: match.start()].rstrip()
            attribute = re.search(r"((?:#\[[^\]]*\]\s*)+)$", prefix)
            attributes = attribute.group(1) if attribute else ""
            if re.search(r"#\[\s*cfg\s*\(\s*test\s*\)\s*\]", attributes):
                continue
            if attributes:
                raise ContractError(f"unsupported conditional/path module: {path}/{match.group(1)}")
            child = match.group(1)
            choices = [directory / f"{child}.rs", directory / child / "mod.rs"]
            present = [candidate for candidate in choices if candidate.is_file()]
            if len(present) != 1:
                raise ContractError(f"missing or ambiguous Rust module: {path}/{child}")
            visit(present[0], (*name, child))

    visit(root / entry, ())
    return sources


def explicit_pub_uses(text: str) -> dict[str, tuple[str, ...]]:
    exports: dict[str, tuple[str, ...]] = {}
    for match in top_level_matches(text, r"^pub\s+use\s+([^;]+);"):
        expression = re.sub(r"\s+", "", match.group(1))
        if "{" in expression:
            grouped = re.fullmatch(r"([\w:]+)::\{([\w,]+)\}", expression)
            if grouped is None:
                raise ContractError(f"unsupported explicit public export: {expression}")
            prefix = grouped.group(1).split("::")
            members = [member for member in grouped.group(2).split(",") if member]
        else:
            parts = expression.split("::")
            if len(parts) < 2 or any(not re.fullmatch(r"\w+", part) for part in parts):
                raise ContractError(f"unsupported explicit public export: {expression}")
            prefix, members = parts[:-1], [parts[-1]]
        if prefix[0] == "self":
            prefix = prefix[1:]
        for member in members:
            if member in exports:
                raise ContractError(f"duplicate public export: {member}")
            exports[member] = (*prefix, member)
    return exports


def pub_use_members(text: str, module: str) -> set[str]:
    members = {name for name, path in explicit_pub_uses(text).items() if path[:-1] == (module,)}
    if not members:
        raise ContractError(f"missing pub use block for {module}")
    return members


def module_public_symbols(sources: dict[tuple[str, ...], str]) -> set[str]:
    cache: dict[tuple[str, ...], set[str]] = {}
    resolving: set[tuple[str, ...]] = set()

    def visible(module: tuple[str, ...]) -> set[str]:
        if module in cache:
            return cache[module]
        if module not in sources or module in resolving:
            raise ContractError(f"unresolved or cyclic public module: {'::'.join(module)}")
        resolving.add(module)
        text = sources[module]
        declarations = [
            match.group(1)
            for match in top_level_matches(
                text, r"^pub\s+(?:const|struct|enum|fn|type|trait)\s+(\w+)"
            )
        ]
        if len(declarations) != len(set(declarations)):
            raise ContractError(f"duplicate public declaration in {module}")
        names = set(declarations)
        for name, target in explicit_pub_uses(text).items():
            if name in names or target[-1] not in visible((*module, *target[:-1])):
                raise ContractError(f"duplicate or unresolved public export: {module}/{name}")
            names.add(name)
        resolving.remove(module)
        cache[module] = names
        return names

    symbols = visible(())
    type_owners: dict[str, tuple[str, ...]] = {}
    for module, text in sources.items():
        for declaration in top_level_matches(
            text, r"^(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum)\s+(\w+)"
        ):
            name = declaration.group(1)
            if name in symbols:
                if name in type_owners:
                    raise ContractError(f"ambiguous public type owner: {name}")
                type_owners[name] = module
    methods: set[str] = set()
    for text in sources.values():
        for implementation in top_level_matches(text, r"^impl\s+(\w+)\s*\{"):
            owner = implementation.group(1)
            if owner not in symbols:
                continue
            body_text = block(text[implementation.start() :], rf"\Aimpl\s+{re.escape(owner)}\s*")
            for method in re.findall(
                r"^\s*pub fn\s+(\w+)\s*(?:<[^>]+>)?\s*\(", rust_mask(body_text), re.M
            ):
                qualified = f"{owner}::{method}"
                if qualified in methods:
                    raise ContractError(f"duplicate public method: {qualified}")
                methods.add(qualified)
    return symbols | methods


def rust_struct(text: str, name: str) -> dict[str, tuple[str, str]]:
    body = block(text, rf"pub struct {re.escape(name)}\s*")
    result: dict[str, tuple[str, str]] = {}
    docs: list[str] = []
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("///"):
            docs.append(stripped.removeprefix("///").strip())
            continue
        field = re.fullmatch(r"pub\s+(\w+)\s*:\s*([^,]+),", stripped)
        if field:
            result[field.group(1)] = (field.group(2).strip(), " ".join(docs))
            docs = []
        elif stripped and not stripped.startswith("#"):
            docs = []
    return result


def test_body(text: str, symbol: str) -> str:
    match = re.search(rf"\bfn\s+{re.escape(symbol)}\s*\(", text)
    if match is None:
        raise ContractError(f"stale evidence symbol: {symbol}")
    prefix = text[max(0, match.start() - 300) : match.start()]
    if not re.search(r"#\[test\]\s*$", prefix):
        raise ContractError(f"evidence symbol is not a Rust test: {symbol}")
    if "#[ignore" in prefix:
        raise ContractError(f"evidence test is ignored: {symbol}")
    return block(text[match.start() :], rf"fn\s+{re.escape(symbol)}\s*\(")


def bazel_list(call: str, attribute: str) -> set[str]:
    uncommented = re.sub(r"#[^\n]*", "", call)
    match = re.search(rf"\b{re.escape(attribute)}\s*=\s*\[(?P<body>.*?)\]", uncommented, re.S)
    if match is None:
        raise ContractError(f"Bazel call lacks {attribute} list")
    return set(re.findall(r'["\']([^"\']+)["\']', match.group("body")))


def normalized_source(text: str) -> str:
    """Normalize transport-only whitespace while freezing all executable evidence."""
    return "\n".join(line.rstrip() for line in text.replace("\r\n", "\n").splitlines()) + "\n"


def validate(root: Path, contract_path: Path) -> None:
    contract = load(contract_path)
    if set(contract) != {
        "contract",
        "issue",
        "platform",
        "authority",
        "format",
        "exports",
        "limits",
        "metrics",
        "evidence",
    }:
        raise ContractError("contract members differ from the frozen v1 schema")
    if contract["contract"] != "graphforge-property-overlay/1" or contract["issue"] != 940:
        raise ContractError("contract identity/version is not frozen v1 for #940")
    expected_platform = {
        "rss": (
            "Unix getrusage RUSAGE_SELF ru_maxrss with macOS bytes and other "
            + "Unix KiB normalization"
        ),
        "unsupported": "production RSS scale evidence is not compiled outside Unix",
    }
    if contract["platform"] != expected_platform:
        raise ContractError("production RSS platform scope differs from frozen v1")
    expected_authority = {
        "row_contract": "each fragment row is the complete property map for one changed UUID",
        "read_scope": "all authenticated generations and ordinals",
        "winner": "maximum numeric (generation, ordinal) per UUID",
        "unchanged_uuid": "an older row remains live until superseded",
        "tombstone": "a newer tombstone suppresses the UUID",
        "write_window": (
            "compose repeated SET and REMOVE operations once, then append only "
            + "changed UUID snapshots"
        ),
        "prior_fragments": (
            "must not be fully decoded or rewritten; only snapshots for changed "
            + "UUIDs may be decoded"
        ),
    }
    if contract["authority"] != expected_authority:
        raise ContractError("incremental all-generation authority differs from frozen v1")

    overlay_sources = module_sources(root, OVERLAY)
    writer_sources = module_sources(root, WRITER)
    overlay = overlay_sources[()]
    lib = (root / LIB).read_text(encoding="utf-8")
    writer = "\n".join(writer_sources[key] for key in sorted(writer_sources))
    storage_build = (root / STORAGE_BUILD).read_text(encoding="utf-8")
    root_build = (root / ROOT_BUILD).read_text(encoding="utf-8")

    expected_format = {
        "PROPERTY_OVERLAY_FORMAT": "full-snapshot-v1",
        "PROPERTY_OVERLAY_FORMAT_KEY": "graphforge.property_overlay",
        "PROPERTY_TOMBSTONE_FIELD": "__gf_property_tombstone",
        "fragment_filename": "{generation:020}-{ordinal:020}.parquet",
    }
    if contract["format"] != expected_format:
        raise ContractError("format/version constants differ from frozen v1")
    for name, value in expected_format.items():
        if name == "fragment_filename":
            if 'format!("{:020}-{:020}.parquet", self.generation, self.ordinal)' not in overlay:
                raise ContractError("canonical fragment filename source drifted")
        elif not re.search(rf'pub const {name}: &str = "{re.escape(value)}";', overlay):
            raise ContractError(f"Rust format constant drifted: {name}")

    exports = contract["exports"]
    if not isinstance(exports, dict) or set(exports) != {
        "property_overlay",
        "property_overlay_module",
        "authenticated_staging",
    }:
        raise ContractError("export groups differ from frozen v1")
    actual_overlay = pub_use_members(lib, "property_overlay")
    if actual_overlay != set(exports["property_overlay"]):
        raise ContractError("property-overlay public exports differ from ledger")
    if module_public_symbols(overlay_sources) != set(exports["property_overlay_module"]):
        raise ContractError("property-overlay module public surface differs from ledger")
    actual_writer = pub_use_members(lib, "writer")
    staged = {name for name in actual_writer if name.endswith("_properties_authenticated")}
    if staged != set(exports["authenticated_staging"]):
        raise ContractError("authenticated staging exports differ from ledger")
    writer_symbols = module_public_symbols(writer_sources)
    for name in staged:
        if name not in writer_symbols:
            raise ContractError(f"authenticated staging root export missing: {name}")
        if re.search(rf"pub fn {re.escape(name)}\s*\(", writer) is None:
            raise ContractError(f"authenticated staging implementation missing: {name}")
    for helper in (
        "stage_set_node_properties_from_inventory",
        "stage_remove_node_properties_from_inventory",
        "stage_set_edge_properties_from_inventory",
        "stage_remove_edge_properties_from_inventory",
    ):
        body_text = block(writer, rf"fn\s+{helper}\s*\(")
        if "read_authenticated_property_snapshots_for_inventory" not in body_text:
            raise ContractError(f"{helper} no longer uses targeted authenticated reads")
        if "visit_route" in body_text or "visit_authenticated_property_snapshots" in body_text:
            raise ContractError(f"{helper} introduced a full prior-fragment decode")

    limits = contract["limits"]
    source_limits = rust_struct(overlay, "PropertyOverlayLimits")
    if set(limits) != set(source_limits):
        raise ContractError("PropertyOverlayLimits fields differ from ledger")
    defaults = block(overlay, r"impl Default for PropertyOverlayLimits\s*")
    for name, spec in limits.items():
        if set(spec) != {"rust_type", "unit", "default_expression"} or not spec["unit"]:
            raise ContractError(f"malformed limit contract: {name}")
        if source_limits[name][0] != spec["rust_type"]:
            raise ContractError(f"Rust limit type drifted: {name}")
        expression = re.escape(spec["default_expression"]).replace(r"\ ", r"\s+")
        if re.search(rf"{name}:\s*{expression},", defaults) is None:
            raise ContractError(f"Rust limit default drifted: {name}")

    metrics = contract["metrics"]
    source_metrics = rust_struct(overlay, "PropertyOverlayMetrics")
    if set(metrics) != set(source_metrics):
        raise ContractError("PropertyOverlayMetrics fields differ from ledger")
    for name, spec in metrics.items():
        if set(spec) != {"unit", "aggregation", "source_markers"}:
            raise ContractError(f"malformed metric contract: {name}")
        if source_metrics[name][0] != "u64" or not spec["unit"] or not spec["aggregation"]:
            raise ContractError(f"metric lacks u64/unit/aggregation contract: {name}")
        for marker in spec["source_markers"]:
            if marker not in source_metrics[name][1]:
                raise ContractError(f"metric source semantics drifted: {name}/{marker}")
    if metrics["physical_rows"]["unit"] != "row-decode visits":
        raise ContractError("physical_rows must be frozen as row-decode visits")
    if metrics["physical_blocks"]["unit"] != "non-empty read operations":
        raise ContractError("physical_blocks must be frozen as actual non-empty reads")
    for name in (
        "authentication_block_equivalents",
        "authority_authentication_block_equivalents",
        "property_authentication_block_equivalents",
    ):
        if metrics[name]["unit"] != "64 KiB block-equivalents":
            raise ContractError(f"{name} must remain a byte-derived block-equivalent")
    for name in (
        "authentication_read_calls",
        "authority_authentication_read_calls",
        "property_authentication_read_calls",
    ):
        if metrics[name]["unit"] != "non-empty reads":
            raise ContractError(f"{name} must remain an actual non-empty read count")

    evidence = contract["evidence"]
    if not isinstance(evidence, dict) or not evidence:
        raise ContractError("acceptance evidence is empty")
    for case, reference in evidence.items():
        expected_members = {"path", "symbol", "markers"}
        if case == "production_bounded_scale":
            expected_members.add("source_sha256")
        if not isinstance(reference, dict) or set(reference) != expected_members:
            raise ContractError(f"malformed evidence: {case}")
        path = root / reference["path"]
        if not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
            raise ContractError(f"invalid evidence path: {case}")
        body = test_body(path.read_text(encoding="utf-8"), reference["symbol"])
        if not any(marker in body for marker in ("assert!", "assert_eq!", "assert_ne!")):
            raise ContractError(f"evidence has no assertion: {case}")
        for marker in reference["markers"]:
            if marker not in body:
                raise ContractError(f"evidence marker missing: {case}/{marker}")
        if case == "production_bounded_scale":
            digest = hashlib.sha256(
                normalized_source(path.read_text(encoding="utf-8")).encode()
            ).hexdigest()
            if digest != reference["source_sha256"]:
                raise ContractError(
                    "transitive production scale evidence differs from frozen ledger"
                )

    scale_source = (root / "crates/graphforge-storage/tests/property_overlay_scale.rs").read_text(
        encoding="utf-8"
    )
    if "#![cfg(unix)]" not in scale_source or "libc::RUSAGE_SELF" not in scale_source:
        raise ContractError("production RSS evidence lost explicit Unix getrusage scope")
    storage_rules = re.sub(r"#[^\n]*", "", storage_build)
    scale_target = call_block(
        storage_rules, r"gf_rust_integration_test\(\s*name\s*=\s*\"property_overlay_scale\""
    )
    if bazel_list(scale_target, "srcs") != {"tests/property_overlay_scale.rs"}:
        raise ContractError("property-overlay production scale Bazel source mapping drifted")
    suite = call_block(storage_rules, r"test_suite\(\s*name\s*=\s*\"storage_integration_tests\"")
    if ":property_overlay_scale" not in bazel_list(suite, "tests"):
        raise ContractError("production scale target left storage integration suite")
    root_rules = re.sub(r"#[^\n]*", "", root_build)
    integration_suite = call_block(root_rules, r"test_suite\(\s*name\s*=\s*\"integration_tests\"")
    if "//crates/graphforge-storage:storage_integration_tests" not in bazel_list(
        integration_suite, "tests"
    ):
        raise ContractError("storage integration suite left root integration tests")
    ci_suite = call_block(root_rules, r"test_suite\(\s*name\s*=\s*\"ci_rust_tests\"")
    if ":integration_tests" not in bazel_list(ci_suite, "tests"):
        raise ContractError("root integration tests left ci_rust_tests")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path)
    args = parser.parse_args()
    contract = args.contract or args.root / "tests/contracts/property-overlay-v1.json"
    try:
        validate(args.root.resolve(), contract.resolve())
    except (ContractError, OSError, json.JSONDecodeError) as error:
        print(f"property overlay contract gate: {error}", file=sys.stderr)
        return 1
    print("property overlay contract gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
