#!/usr/bin/env python3
"""Fail-closed source-backed property-overlay v1 contract gate (#940)."""

from __future__ import annotations

import argparse
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
    match = re.search(start_pattern, text)
    if match is None:
        raise ContractError(f"missing Rust block: {start_pattern}")
    opening = text.find("{", match.end())
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[opening + 1 : index]
    raise ContractError(f"unterminated Rust block: {start_pattern}")


def pub_use_members(text: str, module: str) -> set[str]:
    match = re.search(rf"pub use {re.escape(module)}::\{{(?P<body>.*?)\}};", text, re.S)
    if match is None:
        raise ContractError(f"missing pub use block for {module}")
    return {member.strip() for member in match.group("body").split(",") if member.strip()}


def module_public_symbols(text: str) -> set[str]:
    symbols = set(re.findall(r"^pub (?:const|struct|enum|fn)\s+(\w+)", text, re.M))
    for implementation in re.finditer(r"^impl\s+(\w+)\s*\{", text, re.M):
        owner = implementation.group(1)
        body_text = block(text[implementation.start() :], rf"impl\s+{re.escape(owner)}\s*")
        symbols.update(
            f"{owner}::{method}"
            for method in re.findall(r"^\s*pub fn\s+(\w+)\s*(?:<[^>]+>)?\s*\(", body_text, re.M)
        )
    return symbols


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


def validate(root: Path, contract_path: Path) -> None:
    contract = load(contract_path)
    if set(contract) != {
        "contract", "issue", "authority", "format", "exports", "limits", "metrics", "evidence"
    }:
        raise ContractError("contract members differ from the frozen v1 schema")
    if contract["contract"] != "graphforge-property-overlay/1" or contract["issue"] != 940:
        raise ContractError("contract identity/version is not frozen v1 for #940")
    expected_authority = {
        "row_contract": "each fragment row is the complete property map for one changed UUID",
        "read_scope": "all authenticated generations and ordinals",
        "winner": "maximum numeric (generation, ordinal) per UUID",
        "unchanged_uuid": "an older row remains live until superseded",
        "tombstone": "a newer tombstone suppresses the UUID",
        "write_window": "compose repeated SET and REMOVE operations once, then append only changed UUID snapshots",
        "prior_fragments": "must not be decoded or rewritten to publish an ordinary write window",
    }
    if contract["authority"] != expected_authority:
        raise ContractError("incremental all-generation authority differs from frozen v1")

    overlay = (root / OVERLAY).read_text(encoding="utf-8")
    lib = (root / LIB).read_text(encoding="utf-8")
    writer = (root / WRITER).read_text(encoding="utf-8")

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
    if module_public_symbols(overlay) != set(exports["property_overlay_module"]):
        raise ContractError("property-overlay module public surface differs from ledger")
    actual_writer = pub_use_members(lib, "writer")
    staged = {name for name in actual_writer if name.endswith("_properties_authenticated")}
    if staged != set(exports["authenticated_staging"]):
        raise ContractError("authenticated staging exports differ from ledger")
    for name in staged:
        if re.search(rf"pub fn {re.escape(name)}\s*\(", writer) is None:
            raise ContractError(f"authenticated staging implementation missing: {name}")

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

    evidence = contract["evidence"]
    if not isinstance(evidence, dict) or not evidence:
        raise ContractError("acceptance evidence is empty")
    for case, reference in evidence.items():
        if not isinstance(reference, dict) or set(reference) != {"path", "symbol", "markers"}:
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
