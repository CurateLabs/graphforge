#!/usr/bin/env python3
"""Fail-closed Cargo ↔ Bazel migration drift check (#11).

Checks exact registry package versions/download checksums in both lockfiles,
then compares workspace dependency/features from `cargo metadata --locked`
with the checked-in fingerprint under tools/bazel/drift/.

Ordinary Bazel compilation must not shell out to Cargo; this inventory tool is
allowed to read Cargo metadata so silent Cargo.toml/Cargo.lock feature drift
cannot pass unreviewed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

import tomllib

SCHEMA = "graphforge.cargo-feature-fingerprint.v1"
DEFAULT_FINGERPRINT = Path("tools/bazel/drift/cargo_feature_fingerprint.json")


def validate_locked_crates(root: Path) -> None:
    """Require Bazel to use Cargo's exact registry package versions and bytes."""
    cargo_lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    bazel_lock = json.loads((root / "cargo-bazel-lock.json").read_text(encoding="utf-8"))
    expected = {}
    for package in cargo_lock["package"]:
        source = package.get("source")
        if source is None:
            continue
        if source != "registry+https://github.com/rust-lang/crates.io-index":
            raise ValueError(f"unsupported Cargo dependency source: {source}")
        name, version = package["name"], package["version"]
        checksum = package.get("checksum")
        if not isinstance(checksum, str) or re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
            raise ValueError(f"invalid Cargo checksum for {name} {version}")
        key = f"{name} {version}"
        if key in expected:
            raise ValueError(f"duplicate Cargo package: {key}")
        expected[key] = {
            "url": f"https://static.crates.io/crates/{name}/{version}/download",
            "sha256": checksum,
        }

    observed = {}
    for key, package in bazel_lock["crates"].items():
        repository = package.get("repository")
        if repository is None:
            continue
        if key != f"{package['name']} {package['version']}":
            raise ValueError(f"Bazel package key disagrees with its identity: {key}")
        if not isinstance(repository, dict) or set(repository) != {"Http"}:
            raise ValueError(f"unsupported Bazel dependency source for {key}")
        observed[key] = repository["Http"]

    missing = sorted(expected.keys() - observed.keys())
    extra = sorted(observed.keys() - expected.keys())
    changed = sorted(
        key for key in expected.keys() & observed.keys() if expected[key] != observed[key]
    )
    if missing or extra or changed:
        raise ValueError(
            f"missing Bazel packages={missing}; stale/extra Bazel packages={extra}; "
            f"source/checksum mismatches={changed}"
        )


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def cargo_metadata(root: Path) -> dict[str, Any]:
    return json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version=1", "--locked"],
            cwd=root,
            text=True,
        )
    )


def build_entries(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    workspace_members = set(metadata["workspace_members"])
    entries: list[dict[str, Any]] = []
    for package in metadata["packages"]:
        if package["id"] not in workspace_members:
            continue
        deps = []
        for dep in package.get("dependencies", []):
            deps.append(
                {
                    "name": dep["name"],
                    "req": dep.get("req"),
                    "features": sorted(dep.get("features") or []),
                    "optional": bool(dep.get("optional")),
                    "uses_default_features": bool(dep.get("uses_default_features", True)),
                    "kind": dep.get("kind"),
                    "target": dep.get("target"),
                }
            )
        deps.sort(key=lambda item: (item["name"], item.get("kind") or "", str(item.get("target"))))
        entries.append(
            {
                "name": package["name"],
                "version": package["version"],
                "features": sorted((package.get("features") or {}).keys()),
                "dependencies": deps,
            }
        )
    entries.sort(key=lambda item: item["name"])
    return entries


def fingerprint_payload(entries: list[dict[str, Any]]) -> dict[str, Any]:
    entries_raw = json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
    return {
        "schema": SCHEMA,
        "sha256": hashlib.sha256(entries_raw).hexdigest(),
        "entries": entries,
    }


def write_fingerprint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fingerprint",
        type=Path,
        default=DEFAULT_FINGERPRINT,
        help="Checked-in fingerprint path (repo-relative or absolute)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Rewrite the fingerprint from current cargo metadata",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Repository root (defaults to graphforge checkout)",
    )
    args = parser.parse_args(argv)

    root = args.root.resolve() if args.root else repo_root()
    fingerprint_path = (
        args.fingerprint if args.fingerprint.is_absolute() else root / args.fingerprint
    )

    payload = fingerprint_payload(build_entries(cargo_metadata(root)))

    if args.write:
        write_fingerprint(fingerprint_path, payload)
        print(f"wrote {fingerprint_path} sha256={payload['sha256']}")
        return 0

    try:
        validate_locked_crates(root)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Cargo/Bazel locked dependency drift: {error}", file=sys.stderr)
        print(
            "Repin with: CARGO_BAZEL_REPIN=1 bazelisk mod deps --repo_env=CARGO_BAZEL_REPIN=1",
            file=sys.stderr,
        )
        return 1

    if not fingerprint_path.is_file():
        print(f"missing fingerprint: {fingerprint_path}", file=sys.stderr)
        print("run: python3 scripts/ci/cargo-bazel-drift-check.py --write", file=sys.stderr)
        return 1

    expected = json.loads(fingerprint_path.read_text(encoding="utf-8"))
    if expected.get("schema") != SCHEMA:
        print(
            f"unexpected fingerprint schema: {expected.get('schema')!r}",
            file=sys.stderr,
        )
        return 1

    if expected.get("sha256") != payload["sha256"] or expected.get("entries") != payload["entries"]:
        print(
            "Cargo dependency/feature graph drifted from Bazel migration fingerprint.",
            file=sys.stderr,
        )
        print(f"checked-in sha256: {expected.get('sha256')}", file=sys.stderr)
        print(f"current sha256:    {payload['sha256']}", file=sys.stderr)
        print(
            "Update intentionally with: python3 scripts/ci/cargo-bazel-drift-check.py --write",
            file=sys.stderr,
        )
        return 1

    print(f"cargo feature fingerprint ok sha256={payload['sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
