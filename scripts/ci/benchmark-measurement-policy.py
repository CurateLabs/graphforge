#!/usr/bin/env python3
"""Enforce the BenchExec/Divan benchmark measurement policy and inventory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

INVENTORY = "config/benchmark-measurement-inventory.json"

# Surfaces where independent benchmark clocks/sampling are forbidden unless inventoried.
SCAN_SUFFIXES = (
    "/benches/",
    "/tests/bench_",
    "/tests/merge_scaling_bench.rs",
    "/tests/persistent_adjacency.rs",
)

SIGNAL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "custom_wall_clock",
        re.compile(r"(?:std::time::)?Instant::now\s*\("),
    ),
    (
        "homegrown_statistics",
        re.compile(r"\bfn\s+median_\w+"),
    ),
    (
        "manual_sampling_loop",
        re.compile(r"\bsamples\.push\s*\("),
    ),
    (
        "custom_cpu_sampling",
        re.compile(r"\b(?:getrusage|process_cpu_time)\b"),
    ),
    (
        "product_semantic_counter_gate",
        re.compile(r"\bINGEST_GATE_ENV\b|\bGF_INGEST_FLOOR_GATE\b"),
    ),
)

DISPOSITIONS = {
    "framework_authority",
    "framework_consumer",
    "mixed_authority",
    "migrate",
    "diagnostic_only",
}

BOUNDARIES = {
    "in_process",
    "process_tree",
}


class PolicyError(ValueError):
    """Invalid inventory or unavailable repository authority."""


def tracked_paths(root: Path) -> set[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--cached"],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise PolicyError(f"cannot enumerate tracked files: {error}") from error
    return {
        path.decode("utf-8", errors="surrogateescape")
        for path in result.stdout.split(b"\0")
        if path
    }


def _path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise PolicyError(f"{label}: expected a repository-relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/")):
        raise PolicyError(f"{label}: invalid path {value!r}")
    return value


def load_inventory(root: Path) -> dict:
    path = root / INVENTORY
    if not path.is_file():
        raise PolicyError(f"missing inventory: {INVENTORY}")
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        raise PolicyError(f"{INVENTORY}: invalid JSON: {error}") from error
    if not isinstance(payload, dict):
        raise PolicyError(f"{INVENTORY}: expected an object")
    version = payload.get("version")
    if version != 1:
        raise PolicyError(f"{INVENTORY}: unsupported version {version!r}")
    policy_doc = payload.get("policy_doc")
    if not isinstance(policy_doc, str) or not policy_doc:
        raise PolicyError(f"{INVENTORY}: policy_doc must be a non-empty string")
    if not (root / policy_doc).is_file():
        raise PolicyError(f"{INVENTORY}: policy_doc {policy_doc!r} is missing")
    sites = payload.get("sites")
    if not isinstance(sites, list) or not sites:
        raise PolicyError(f"{INVENTORY}: sites must be a non-empty array")
    seen: set[str] = set()
    for index, site in enumerate(sites):
        label = f"{INVENTORY} sites[{index}]"
        if not isinstance(site, dict):
            raise PolicyError(f"{label}: expected an object")
        path_value = _path(site.get("path"), f"{label}.path")
        if path_value in seen:
            raise PolicyError(f"{label}: duplicate inventory path {path_value}")
        seen.add(path_value)
        boundary = site.get("boundary")
        if boundary not in BOUNDARIES:
            raise PolicyError(f"{label}: invalid boundary {boundary!r}")
        disposition = site.get("disposition")
        if disposition not in DISPOSITIONS:
            raise PolicyError(f"{label}: invalid disposition {disposition!r}")
        authority = site.get("authority")
        if not isinstance(authority, str) or not authority:
            raise PolicyError(f"{label}: authority must be a non-empty string")
        owner_issue = site.get("owner_issue")
        if owner_issue is not None and type(owner_issue) is not int:
            raise PolicyError(f"{label}: owner_issue must be an integer or null")
        allowed = site.get("allowed_signals", [])
        if not isinstance(allowed, list) or not all(isinstance(item, str) for item in allowed):
            raise PolicyError(f"{label}: allowed_signals must be a string array")
        unknown = sorted(set(allowed) - {name for name, _ in SIGNAL_PATTERNS})
        if unknown:
            raise PolicyError(f"{label}: unknown allowed_signals {unknown}")
        notes = site.get("notes")
        if not isinstance(notes, str) or not notes.strip():
            raise PolicyError(f"{label}: notes must be a non-empty string")
    return payload


def is_scanned_rust_benchmark(path: str) -> bool:
    if not path.endswith(".rs"):
        return False
    return any(marker in path for marker in SCAN_SUFFIXES)


def detect_signals(text: str) -> dict[str, list[int]]:
    hits: dict[str, list[int]] = {}
    for line_no, line in enumerate(text.splitlines(), 1):
        for signal, pattern in SIGNAL_PATTERNS:
            if pattern.search(line):
                hits.setdefault(signal, []).append(line_no)
    return hits


def inventory_by_path(payload: dict) -> dict[str, dict]:
    return {site["path"]: site for site in payload["sites"]}


def validate(root: Path) -> tuple[dict, list[str]]:
    errors: list[str] = []
    payload = load_inventory(root)
    tracked = tracked_paths(root)
    sites = inventory_by_path(payload)

    for path, _site in sorted(sites.items()):
        target = root / path
        descendants = [item for item in tracked if item.startswith(f"{path}/")]
        if path in tracked:
            continue
        if descendants:
            continue
        if target.is_file() or target.is_dir():
            errors.append(f"stale inventory entry: {path} is missing")
            continue
        errors.append(f"stale inventory entry: {path} is not tracked")

    scanned: list[str] = []
    for path in sorted(tracked):
        if is_scanned_rust_benchmark(path):
            scanned.append(path)

    for path in scanned:
        text = (root / path).read_text()
        signals = detect_signals(text)
        if not signals:
            continue
        site = sites.get(path)
        if site is None:
            joined = ", ".join(f"{signal}@{lines[0]}" for signal, lines in sorted(signals.items()))
            errors.append(
                f"{path}: unclassified benchmark measurement machinery ({joined}); "
                "add a reviewed inventory entry or migrate to BenchExec/Divan"
            )
            continue
        allowed = set(site.get("allowed_signals", []))
        if site["disposition"] == "framework_authority" and signals:
            errors.append(
                f"{path}: framework_authority site must not contain custom measurement "
                f"signals ({', '.join(sorted(signals))}); use Divan/BenchExec only"
            )
            continue
        for signal, lines in sorted(signals.items()):
            if signal not in allowed:
                errors.append(
                    f"{path}:{lines[0]}: {signal} is not allowed for disposition "
                    f"{site['disposition']} (owner #{site.get('owner_issue')}); "
                    "update the inventory when migrating or add the reviewed exception"
                )

    return payload, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="repository root (default: auto-detected)",
    )
    args = parser.parse_args()
    try:
        _, errors = validate(args.root)
    except PolicyError as error:
        print(error, file=sys.stderr)
        return 1
    if errors:
        print("benchmark measurement policy violations:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print(
        f"benchmark measurement policy: {len(load_inventory(args.root)['sites'])} "
        "inventory sites verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
