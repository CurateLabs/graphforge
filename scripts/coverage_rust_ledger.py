#!/usr/bin/env python3
"""Build and validate GraphForge's Rust per-surface coverage ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

BINDING_PATHS = {
    "python_adapter": "crates/graphforge-bindings-py/",
    "node_adapter": "crates/graphforge-bindings-node/",
}


class LedgerError(RuntimeError):
    """Coverage evidence is incomplete or inconsistent."""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_head(root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.PIPE
        ).strip()
    except (subprocess.CalledProcessError, OSError) as error:
        raise LedgerError("failed to resolve git HEAD") from error


def git_merge_base(root: Path, base_ref: str) -> str:
    try:
        return subprocess.check_output(
            ["git", "merge-base", "HEAD", base_ref],
            cwd=root,
            text=True,
            stderr=subprocess.PIPE,
        ).strip()
    except (subprocess.CalledProcessError, OSError) as error:
        raise LedgerError("failed to resolve Rust patch coverage base") from error


def validate_source_tree_clean(root: Path) -> None:
    try:
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=root,
            text=True,
            stderr=subprocess.PIPE,
        )
    except (subprocess.CalledProcessError, OSError) as error:
        raise LedgerError("failed to inspect coverage source tree") from error
    if status:
        raise LedgerError("coverage source tree has uncommitted changes")


def normalize_source(raw: str, root: Path) -> str | None:
    path = Path(raw)
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return None


def parse_lcov(path: Path, root: Path) -> dict[str, dict[int, int]]:
    if not path.is_file() or path.stat().st_size == 0:
        raise LedgerError(f"missing or empty lcov report: {path}")
    records: dict[str, dict[int, int]] = {}
    source: str | None = None
    saw_source = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line.startswith("SF:"):
            saw_source = True
            source = normalize_source(raw_line[3:], root)
            if source is not None:
                records.setdefault(source, {})
        elif raw_line.startswith("DA:"):
            if not saw_source:
                raise LedgerError(f"DA record precedes SF record in {path}")
            if source is None:
                continue
            fields = raw_line[3:].split(",")
            if len(fields) < 2:
                raise LedgerError(f"malformed DA record in {path}: {raw_line}")
            try:
                line, hits = int(fields[0]), int(fields[1])
            except ValueError as error:
                raise LedgerError(f"malformed DA record in {path}: {raw_line}") from error
            records[source][line] = max(records[source].get(line, 0), hits)
    if not records or not any(lines for lines in records.values()):
        raise LedgerError(f"lcov report has no executable lines: {path}")
    return records


def merge_records(*reports: dict[str, dict[int, int]]) -> dict[str, dict[int, int]]:
    merged: dict[str, dict[int, int]] = {}
    for report in reports:
        for source, lines in report.items():
            destination = merged.setdefault(source, {})
            for line, hits in lines.items():
                destination[line] = max(destination.get(line, 0), hits)
    return merged


def _rust_tokens(source: str, path: str) -> list[tuple[str, int]]:
    """Tokenize enough Rust syntax to locate cfg(test) item boundaries safely."""
    tokens: list[tuple[str, int]] = []
    index = 0
    line = 1
    length = len(source)

    def advance(end: int) -> None:
        nonlocal index, line
        line += source.count("\n", index, end)
        index = end

    while index < length:
        char = source[index]
        if char.isspace():
            advance(index + 1)
            continue
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            advance(length if end < 0 else end)
            continue
        if source.startswith("/*", index):
            start_line = line
            depth = 1
            cursor = index + 2
            while cursor < length and depth:
                if source.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif source.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            if depth:
                raise LedgerError(f"unclosed Rust block comment in {path}:{start_line}")
            advance(cursor)
            continue

        raw_start = index
        if source.startswith("br", index):
            raw_start += 1
        if raw_start < length and source[raw_start] == "r":
            cursor = raw_start + 1
            while cursor < length and source[cursor] == "#":
                cursor += 1
            if cursor < length and source[cursor] == '"':
                hashes = source[raw_start + 1 : cursor]
                terminator = '"' + hashes
                end = source.find(terminator, cursor + 1)
                if end < 0:
                    raise LedgerError(f"unclosed Rust raw string in {path}:{line}")
                advance(end + len(terminator))
                continue

        quote_index = index + int(char == "b" and index + 1 < length)
        if quote_index < length and source[quote_index] == '"':
            cursor = quote_index + 1
            while cursor < length:
                if source[cursor] == "\\":
                    cursor += 2
                elif source[cursor] == '"':
                    cursor += 1
                    break
                else:
                    cursor += 1
            else:
                raise LedgerError(f"unclosed Rust string in {path}:{line}")
            advance(cursor)
            continue
        if char == "'":
            lifetime_end = index + 1
            if lifetime_end < length and (
                source[lifetime_end] == "_" or source[lifetime_end].isalpha()
            ):
                lifetime_end += 1
                while lifetime_end < length and (
                    source[lifetime_end] == "_" or source[lifetime_end].isalnum()
                ):
                    lifetime_end += 1
                if lifetime_end >= length or source[lifetime_end] != "'":
                    tokens.append((char, line))
                    advance(index + 1)
                    continue
            cursor = index + 1
            escaped = False
            closed = False
            while cursor < length and source[cursor] != "\n":
                if not escaped and source[cursor] == "'":
                    advance(cursor + 1)
                    closed = True
                    break
                if not escaped and source[cursor] == "\\":
                    escaped = True
                else:
                    escaped = False
                cursor += 1
            if closed:
                continue

        token_line = line
        if char == "_" or char.isalpha():
            cursor = index + 1
            while cursor < length and (source[cursor] == "_" or source[cursor].isalnum()):
                cursor += 1
            tokens.append((source[index:cursor], token_line))
            advance(cursor)
        else:
            tokens.append((char, token_line))
            advance(index + 1)
    return tokens


def _cfg_test_analysis(source: str, path: str) -> tuple[set[int], set[str]]:
    """Return cfg(test) item lines and out-of-line module names."""
    tokens = _rust_tokens(source, path)
    delimiter_stack: list[tuple[str, int]] = []
    brace_close: dict[int, int] = {}
    pairs = {"}": "{", ")": "(", "]": "["}
    for position, (token, token_line) in enumerate(tokens):
        if token in {"{", "(", "["}:
            delimiter_stack.append((token, position))
        elif token in pairs:
            if not delimiter_stack or delimiter_stack[-1][0] != pairs[token]:
                raise LedgerError(f"unbalanced Rust delimiter in {path}:{token_line}")
            opening, opening_position = delimiter_stack.pop()
            if opening == "{":
                brace_close[opening_position] = position
    if delimiter_stack:
        _, opening_position = delimiter_stack[-1]
        _, token_line = tokens[opening_position]
        raise LedgerError(f"unbalanced Rust delimiter in {path}:{token_line}")

    excluded: set[int] = set()
    modules: set[str] = set()
    position = 0
    while position + 6 < len(tokens):
        values = [value for value, _ in tokens[position : position + 7]]
        if values != ["#", "[", "cfg", "(", "test", ")", "]"]:
            position += 1
            continue
        attribute_line = tokens[position][1]
        cursor = position + 7
        paren_depth = bracket_depth = 0
        boundary: int | None = None
        while cursor < len(tokens):
            token = tokens[cursor][0]
            if token == "(":
                paren_depth += 1
            elif token == ")":
                paren_depth -= 1
            elif token == "[":
                bracket_depth += 1
            elif token == "]":
                bracket_depth -= 1
            elif paren_depth == 0 and bracket_depth == 0 and token in ("{", ";"):
                boundary = cursor
                break
            cursor += 1
        if boundary is None or paren_depth or bracket_depth:
            raise LedgerError(f"ambiguous cfg(test) item in {path}:{attribute_line}")
        if tokens[boundary][0] == "{":
            close = brace_close.get(boundary)
            if close is None:
                raise LedgerError(f"unbalanced cfg(test) item in {path}:{attribute_line}")
            end_line = tokens[close][1]
            position = close + 1
        else:
            end_line = tokens[boundary][1]
            item_tokens = [value for value, _ in tokens[position + 7 : boundary]]
            if "mod" in item_tokens:
                module_index = item_tokens.index("mod")
                if module_index + 1 >= len(item_tokens):
                    raise LedgerError(f"ambiguous cfg(test) module in {path}:{attribute_line}")
                modules.add(item_tokens[module_index + 1])
            position = boundary + 1
        excluded.update(range(attribute_line, end_line + 1))
    return excluded, modules


def cfg_test_lines(source: str, path: str) -> set[int]:
    """Return source lines belonging to items gated exclusively by cfg(test)."""
    return _cfg_test_analysis(source, path)[0]


def production_records(records: dict[str, dict[int, int]], root: Path) -> dict[str, dict[int, int]]:
    """Filter test-only Rust code from production coverage records."""
    production: dict[str, dict[int, int]] = {}
    analyses: dict[str, tuple[set[int], set[str]]] = {}
    test_module_paths: set[str] = set()
    for path in records:
        parts = Path(path).parts
        if not path.endswith(".rs") or "src" not in parts:
            continue
        source_path = root / path
        try:
            source = source_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise LedgerError(f"failed to inspect Rust coverage source: {path}") from error
        analysis = _cfg_test_analysis(source, path)
        analyses[path] = analysis
        parent = Path(path).parent
        stem = Path(path).stem
        module_parent = parent if stem in {"lib", "main", "mod"} else parent / stem
        for module in analysis[1]:
            test_module_paths.add((module_parent / f"{module}.rs").as_posix())
            test_module_paths.add((module_parent / module / "mod.rs").as_posix())
            test_module_paths.add((module_parent / module).as_posix() + "/")
    for path, lines in records.items():
        parts = Path(path).parts
        if (
            len(parts) >= 3
            and parts[0] == "crates"
            and parts[2]
            in {
                "tests",
                "benches",
                "examples",
            }
        ):
            continue
        if path in test_module_paths or any(
            marker.endswith("/") and path.startswith(marker) for marker in test_module_paths
        ):
            continue
        if not path.endswith(".rs") or "src" not in parts:
            production[path] = lines
            continue
        excluded = analyses[path][0]
        retained = {line: hits for line, hits in lines.items() if line not in excluded}
        if retained:
            production[path] = retained
    return production


def select_surface(records: dict[str, dict[int, int]], surface: str) -> dict[str, dict[int, int]]:
    if surface == "core":
        return {
            path: lines
            for path, lines in records.items()
            if not any(path.startswith(prefix) for prefix in BINDING_PATHS.values())
        }
    prefix = BINDING_PATHS[surface]
    return {path: lines for path, lines in records.items() if path.startswith(prefix)}


def totals(records: dict[str, dict[int, int]]) -> dict[str, int | float]:
    measured = sum(len(lines) for lines in records.values())
    covered = sum(sum(1 for hits in lines.values() if hits > 0) for lines in records.values())
    if measured == 0:
        raise LedgerError("coverage surface has zero executable lines")
    return {
        "covered_lines": covered,
        "measured_lines": measured,
        "line_percent": round(covered * 100 / measured, 2),
        "source_files": len(records),
    }


def crate_totals(records: dict[str, dict[int, int]]) -> dict[str, dict[str, int | float]]:
    """Return deterministic totals for every non-binding production crate."""
    crates: dict[str, dict[str, dict[int, int]]] = {}
    for path, lines in records.items():
        parts = path.split("/")
        if len(parts) < 4 or parts[0] != "crates" or parts[2] != "src":
            continue
        crate = parts[1]
        if crate.startswith("graphforge-bindings-"):
            continue
        crates.setdefault(crate, {})[path] = lines
    if not crates:
        raise LedgerError("core coverage contains no production Rust crates")
    return {name: totals(crates[name]) for name in sorted(crates)}


def expected_production_crates(root: Path) -> set[str]:
    crates = {
        path.parent.name
        for path in (root / "crates").glob("*/Cargo.toml")
        if not path.parent.name.startswith("graphforge-bindings-")
        and any((path.parent / "src").rglob("*.rs"))
    }
    if not crates:
        raise LedgerError("workspace contains no non-binding production Rust crates")
    return crates


def patch_totals(
    records: dict[str, dict[int, int]], root: Path, base_ref: str
) -> tuple[str, dict[str, int | float]]:
    """Measure executable changed Rust lines against the merge base."""
    try:
        base_sha = git_merge_base(root, base_ref)
        diff = subprocess.check_output(
            ["git", "diff", "--unified=0", f"{base_sha}...HEAD", "--", "crates/**/*.rs"],
            cwd=root,
            text=True,
            stderr=subprocess.PIPE,
        )
    except (subprocess.CalledProcessError, OSError) as error:
        raise LedgerError("failed to resolve Rust patch coverage base") from error

    changed: dict[str, set[int]] = {}
    path: str | None = None
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
            continue
        if not line.startswith("@@") or path is None:
            continue
        match = re.search(r"\+(\d+)(?:,(\d+))?", line)
        if match is None:
            raise LedgerError("malformed Rust patch hunk")
        start = int(match.group(1))
        count = int(match.group(2) or "1")
        changed.setdefault(path, set()).update(range(start, start + count))

    measured = covered = 0
    for source, lines in records.items():
        for line in changed.get(source, set()):
            if line in lines:
                measured += 1
                covered += int(lines[line] > 0)
    percent = 100.0 if measured == 0 else round(covered * 100 / measured, 2)
    return base_sha, {
        "covered_lines": covered,
        "measured_lines": measured,
        "line_percent": percent,
        "source_files": sum(bool(changed.get(source)) for source in records),
    }


def write_lcov(records: dict[str, dict[int, int]], path: Path, root: Path) -> None:
    output: list[str] = []
    for source in sorted(records):
        output.append(f"SF:{root / source}")
        for line, hits in sorted(records[source].items()):
            output.append(f"DA:{line},{hits}")
        output.extend(("end_of_record", ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(output), encoding="utf-8")


def validate_evidence(evidence: dict[str, Any], root: Path, expected_sha: str) -> None:
    if evidence.get("source_sha") != expected_sha:
        raise LedgerError(
            "coverage evidence source SHA does not match HEAD: "
            f"expected {expected_sha}, got {evidence.get('source_sha')!r}"
        )
    if not evidence.get("rustc") or not evidence.get("cargo_llvm_cov"):
        raise LedgerError("coverage evidence is missing toolchain identity")
    for surface in ("python_adapter", "node_adapter"):
        item = evidence.get("surfaces", {}).get(surface)
        if not isinstance(item, dict):
            raise LedgerError(f"missing {surface} evidence")
        artifact = root / str(item.get("artifact", ""))
        runtime_artifact = root / str(item.get("runtime_artifact", ""))
        expected_hash = item.get("artifact_sha256")
        if not artifact.is_file() or not runtime_artifact.is_file():
            raise LedgerError(f"{surface} artifact evidence is missing")
        actual_hash = sha256(artifact)
        if not expected_hash or actual_hash != expected_hash:
            raise LedgerError(f"{surface} artifact hash is stale or incorrect")
        if sha256(runtime_artifact) != actual_hash:
            raise LedgerError(f"{surface} tests did not load the measured artifact")
        profiles = [root / str(value) for value in item.get("profiles", [])]
        if not profiles:
            raise LedgerError(f"{surface} contributed no runtime profiles")
        for profile in profiles:
            if not profile.is_file() or profile.stat().st_size == 0:
                raise LedgerError(f"{surface} profile is missing or empty: {profile}")
            if profile.stat().st_mtime_ns <= artifact.stat().st_mtime_ns:
                raise LedgerError(f"{surface} profile predates its instrumented artifact")


def build_ledger(args: argparse.Namespace) -> dict[str, Any]:
    root = args.root.resolve()
    validate_source_tree_clean(root)
    expected_sha = git_head(root)
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    validate_evidence(evidence, root, expected_sha)

    core_report = parse_lcov(args.core_lcov, root)
    python_report = parse_lcov(args.python_lcov, root)
    node_report = parse_lcov(args.node_lcov, root)
    merged = merge_records(core_report, python_report, node_report)
    if args.workspace_lcov is not None:
        write_lcov(merged, args.workspace_lcov, root)
    core_records = production_records(select_surface(core_report, "core"), root)
    patch_base_sha, patch = patch_totals(core_records, root, args.patch_base)
    surfaces = {
        "core": totals(core_records),
        "python_adapter": totals(select_surface(python_report, "python_adapter")),
        "node_adapter": totals(select_surface(node_report, "node_adapter")),
        "workspace": totals(merged),
    }
    return {
        "schema_version": 2,
        "source_sha": expected_sha,
        "patch_base_sha": patch_base_sha,
        "toolchain": {
            "rustc": evidence["rustc"],
            "cargo_llvm_cov": evidence["cargo_llvm_cov"],
        },
        "artifacts": {
            name: {
                "sha256": evidence["surfaces"][name]["artifact_sha256"],
                "profile_count": len(evidence["surfaces"][name]["profiles"]),
            }
            for name in ("python_adapter", "node_adapter")
        },
        "surfaces": surfaces,
        "crates": crate_totals(core_records),
        "patch": patch,
    }


def validate_floors(ledger: dict[str, Any], args: argparse.Namespace) -> None:
    root = args.root.resolve()
    expected_sha = git_head(root)
    if ledger.get("schema_version") != 2:
        raise LedgerError("unsupported or missing Rust coverage ledger schema")
    if ledger.get("source_sha") != expected_sha:
        raise LedgerError("Rust coverage ledger is stale for the current HEAD")
    expected_patch_base = git_merge_base(root, args.patch_base)
    if ledger.get("patch_base_sha") != expected_patch_base:
        raise LedgerError("Rust patch coverage ledger is stale for the current merge base")
    floors: dict[str, float | None] = {
        "core": args.core_floor,
        "python_adapter": args.python_floor,
        "node_adapter": args.node_floor,
        "workspace": None,
    }
    for surface, floor in floors.items():
        data = ledger.get("surfaces", {}).get(surface)
        validate_total(data, surface)
        percent = data["line_percent"]
        if floor is not None and float(percent) < floor:
            raise LedgerError(
                f"{surface} Rust coverage below {floor:.2f}% (got {float(percent):.2f}%)"
            )
    crates = ledger.get("crates")
    if not isinstance(crates, dict) or not crates:
        raise LedgerError("Rust coverage ledger is missing per-crate totals")
    expected_crates = expected_production_crates(root)
    actual_crates = set(crates)
    if actual_crates != expected_crates:
        missing = sorted(expected_crates - actual_crates)
        unexpected = sorted(actual_crates - expected_crates)
        raise LedgerError(
            "Rust coverage ledger crate inventory mismatch: "
            f"missing={missing}, unexpected={unexpected}"
        )
    for crate, data in sorted(crates.items()):
        validate_total(data, f"crate {crate}")
        if float(data["line_percent"]) < args.crate_floor:
            raise LedgerError(
                f"{crate} Rust coverage below {args.crate_floor:.2f}% "
                f"(got {float(data['line_percent']):.2f}%)"
            )
    patch = ledger.get("patch")
    validate_total(patch, "patch", allow_zero=True)
    if float(patch["line_percent"]) < args.patch_floor:
        raise LedgerError(
            f"changed Rust coverage below {args.patch_floor:.2f}% "
            f"(got {float(patch['line_percent']):.2f}%)"
        )


def validate_total(data: Any, name: str, *, allow_zero: bool = False) -> None:
    if not isinstance(data, dict):
        raise LedgerError(f"Rust coverage ledger is missing {name}")
    covered = data.get("covered_lines")
    measured = data.get("measured_lines")
    percent = data.get("line_percent")
    if (
        isinstance(covered, bool)
        or not isinstance(covered, int)
        or covered < 0
        or isinstance(measured, bool)
        or not isinstance(measured, int)
        or measured < int(not allow_zero)
        or covered > measured
        or isinstance(percent, bool)
        or not isinstance(percent, (int, float))
        or not math.isfinite(percent)
        or percent < 0
        or percent > 100
    ):
        raise LedgerError(f"Rust coverage ledger has malformed {name} totals")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--root", type=Path, required=True)
    result.add_argument("--ledger", type=Path, required=True)
    result.add_argument("--core-floor", type=float, default=95.0)
    result.add_argument("--crate-floor", type=float, default=80.0)
    result.add_argument("--patch-floor", type=float, default=90.0)
    result.add_argument("--patch-base", default="origin/main")
    result.add_argument("--python-floor", type=float, default=80.0)
    result.add_argument("--node-floor", type=float, default=80.0)
    result.add_argument("--build", action="store_true")
    result.add_argument("--core-lcov", type=Path)
    result.add_argument("--python-lcov", type=Path)
    result.add_argument("--node-lcov", type=Path)
    result.add_argument("--evidence", type=Path)
    result.add_argument("--workspace-lcov", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.build:
            required = (args.core_lcov, args.python_lcov, args.node_lcov, args.evidence)
            if any(value is None for value in required):
                raise LedgerError("ledger build requires all lcov reports and evidence")
            ledger = build_ledger(args)
            args.ledger.parent.mkdir(parents=True, exist_ok=True)
            args.ledger.write_text(
                json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        else:
            if not args.ledger.is_file():
                raise LedgerError(f"missing Rust coverage ledger: {args.ledger}")
            ledger = json.loads(args.ledger.read_text(encoding="utf-8"))
        validate_floors(ledger, args)
        report = [
            (name, ledger["surfaces"][name])
            for name in ("core", "python_adapter", "node_adapter", "workspace")
        ]
        crate_report = sorted(ledger["crates"].items())
        patch_report = ledger["patch"]
    except (LedgerError, json.JSONDecodeError, KeyError, TypeError) as error:
        print(f"Rust coverage evidence error: {error}", file=sys.stderr)
        return 1

    for name, data in report:
        print(
            f"{name}: {data['covered_lines']}/{data['measured_lines']} "
            f"lines ({data['line_percent']:.2f}%)"
        )
    for name, data in crate_report:
        print(
            f"crate {name}: {data['covered_lines']}/{data['measured_lines']} "
            f"lines ({data['line_percent']:.2f}%)"
        )
    print(
        f"patch: {patch_report['covered_lines']}/{patch_report['measured_lines']} "
        f"lines ({patch_report['line_percent']:.2f}%)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
