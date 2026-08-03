#!/usr/bin/env python3
"""Build and validate GraphForge's Rust per-surface coverage ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
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
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()


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
    expected_sha = git_head(root)
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    validate_evidence(evidence, root, expected_sha)

    core_report = parse_lcov(args.core_lcov, root)
    python_report = parse_lcov(args.python_lcov, root)
    node_report = parse_lcov(args.node_lcov, root)
    merged = merge_records(core_report, python_report, node_report)
    if args.workspace_lcov is not None:
        write_lcov(merged, args.workspace_lcov, root)
    surfaces = {
        "core": totals(select_surface(core_report, "core")),
        "python_adapter": totals(select_surface(python_report, "python_adapter")),
        "node_adapter": totals(select_surface(node_report, "node_adapter")),
        "workspace": totals(merged),
    }
    return {
        "schema_version": 1,
        "source_sha": expected_sha,
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
    }


def validate_floors(ledger: dict[str, Any], args: argparse.Namespace) -> None:
    expected_sha = git_head(args.root.resolve())
    if ledger.get("schema_version") != 1:
        raise LedgerError("unsupported or missing Rust coverage ledger schema")
    if ledger.get("source_sha") != expected_sha:
        raise LedgerError("Rust coverage ledger is stale for the current HEAD")
    floors: dict[str, float | None] = {
        "core": args.core_floor,
        "python_adapter": args.python_floor,
        "node_adapter": args.node_floor,
        "workspace": None,
    }
    for surface, floor in floors.items():
        data = ledger.get("surfaces", {}).get(surface)
        if not isinstance(data, dict):
            raise LedgerError(f"Rust coverage ledger is missing {surface}")
        covered = data.get("covered_lines")
        measured = data.get("measured_lines")
        percent = data.get("line_percent")
        if (
            not isinstance(covered, int)
            or covered < 0
            or not isinstance(measured, int)
            or measured <= 0
            or covered > measured
            or not isinstance(percent, (int, float))
        ):
            raise LedgerError(f"Rust coverage ledger has malformed {surface} totals")
        if floor is not None and float(percent) < floor:
            raise LedgerError(
                f"{surface} Rust coverage below {floor:.2f}% (got {float(percent):.2f}%)"
            )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--root", type=Path, required=True)
    result.add_argument("--ledger", type=Path, required=True)
    result.add_argument("--core-floor", type=float, default=85.0)
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
    except (LedgerError, json.JSONDecodeError, KeyError, TypeError) as error:
        print(f"Rust coverage evidence error: {error}", file=sys.stderr)
        return 1

    for name, data in report:
        print(
            f"{name}: {data['covered_lines']}/{data['measured_lines']} "
            f"lines ({data['line_percent']:.2f}%)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
