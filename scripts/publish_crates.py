#!/usr/bin/env python3
"""Publish the complete GraphForge crates.io surface in dependency order.

The command is resumable without permitting different bytes under an existing
version. Before each registry write it packages the crate locally and computes
the archive checksum. If that exact version already exists, publication only
continues when crates.io reports the same checksum.

Requires ``CARGO_REGISTRY_TOKEN`` in the environment. The maintained release
credential is projected from Pulumi ESC into the GitHub Actions secret used by
``publish.yaml``.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PLAN_SCRIPT = ROOT / "scripts" / "ci" / "crate-publish-plan.py"
CRATES_API = "https://crates.io/api/v1/crates"
USER_AGENT = "GraphForge crates.io publisher (github.com/CurateLabs/graphforge)"
_VERSION_MATCH = re.search(
    r'(?ms)^\[workspace\.package\].*?^version\s*=\s*"([^"]+)"',
    (ROOT / "Cargo.toml").read_text(encoding="utf-8"),
)
if _VERSION_MATCH is None:
    raise RuntimeError("Cargo.toml lacks [workspace.package] version")
VERSION = _VERSION_MATCH.group(1)


def load_plan_module():
    spec = importlib.util.spec_from_file_location("crate_publish_plan", PLAN_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {PLAN_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(command: list[str]) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def registry_json(path: str) -> dict[str, Any] | None:
    request = urllib.request.Request(
        f"{CRATES_API}/{path}",
        headers={"User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def version_record(name: str) -> dict[str, Any] | None:
    payload = registry_json(f"{name}/{VERSION}")
    if payload is None:
        return None
    return payload.get("version")


def owner_logins(name: str) -> set[str]:
    payload = registry_json(f"{name}/owners")
    if payload is None:
        return set()
    return {owner["login"] for owner in payload.get("users", [])}


def package_checksum(name: str, expected_checksum: str | None = None) -> str:
    run(
        [
            "cargo",
            "package",
            "-p",
            name,
            "--locked",
            "--no-verify",
        ]
    )
    configured_target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    target_dir = configured_target if configured_target.is_absolute() else ROOT / configured_target
    archive = target_dir / "package" / f"{name}-{VERSION}.crate"
    if not archive.is_file():
        raise RuntimeError(f"cargo package did not create {archive}")
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    if expected_checksum is not None and checksum != expected_checksum:
        raise RuntimeError(
            f"{name} {VERSION} packaged checksum {checksum} differs from "
            f"the certified release record {expected_checksum}"
        )
    return checksum


def wait_for_version(name: str, checksum: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        record = version_record(name)
        if record is not None:
            registry_checksum = record.get("checksum")
            if registry_checksum != checksum:
                raise RuntimeError(
                    f"{name} {VERSION} checksum mismatch: "
                    f"local={checksum} registry={registry_checksum}"
                )
            return
        time.sleep(3)
    raise RuntimeError(f"timed out waiting for crates.io to index {name} {VERSION}")


def publish_one(
    name: str,
    *,
    timeout_seconds: int,
    expected_checksum: str | None = None,
) -> str:
    checksum = package_checksum(name, expected_checksum)
    existing = version_record(name)
    if existing is not None:
        registry_checksum = existing.get("checksum")
        if registry_checksum != checksum:
            raise RuntimeError(
                f"refusing to resume {name} {VERSION}: existing checksum "
                f"{registry_checksum} differs from local {checksum}"
            )
        outcome = "already published; checksum matches"
    else:
        run(["cargo", "publish", "-p", name, "--locked"])
        wait_for_version(name, checksum, timeout_seconds)
        outcome = "published"

    owners = owner_logins(name)
    if "DecisionNerd" not in owners:
        raise RuntimeError(
            f"{name} {VERSION} is indexed but DecisionNerd is not an owner: {sorted(owners)}"
        )
    print(f"{name} {VERSION}: {outcome}; owner=DecisionNerd")
    return outcome


def release_record_checksums(record_path: Path, artifacts_dir: Path) -> dict[str, str]:
    try:
        record = json.loads(record_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read release record {record_path}: {error}") from error
    if record.get("schema") != "graphforge-release-record-v1":
        raise RuntimeError("unexpected release record schema")
    if record.get("version") != VERSION or record.get("tag") != f"v{VERSION}":
        raise RuntimeError("release record version/tag does not match the Cargo version")
    if (
        record.get("commit_sha")
        != subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip()
    ):
        raise RuntimeError("release record commit does not match the checked-out commit")

    checksums: dict[str, str] = {}
    artifacts_root = artifacts_dir.resolve()
    for item in record.get("artifacts", []):
        if item.get("surface") != "crates":
            continue
        name = item.get("name")
        relative = item.get("path")
        checksum = item.get("sha256")
        if not all(isinstance(value, str) for value in (name, relative, checksum)):
            raise RuntimeError("release record contains an invalid crates.io artifact")
        if item.get("version") != VERSION:
            raise RuntimeError(f"release record version mismatch for crate {name}")
        if name in checksums:
            raise RuntimeError(f"release record contains duplicate crate {name}")
        archive = (artifacts_dir / relative).resolve()
        if not archive.is_relative_to(artifacts_root):
            raise RuntimeError(f"certified crate archive escapes artifact root: {relative}")
        if not archive.is_file():
            raise RuntimeError(f"certified crate archive is missing: {relative}")
        actual = hashlib.sha256(archive.read_bytes()).hexdigest()
        if actual != checksum:
            raise RuntimeError(f"certified crate archive checksum mismatch: {relative}")
        checksums[name] = checksum
    return checksums


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--index-timeout",
        type=int,
        default=300,
        help="Seconds to wait for each successful upload to appear in crates.io",
    )
    parser.add_argument(
        "--release-record",
        type=Path,
        help="Certified graphforge-release-record-v1 JSON",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        help="Root containing the certified artifact paths",
    )
    args = parser.parse_args(argv)

    if (args.release_record is None) != (args.artifacts_dir is None):
        print("--release-record and --artifacts-dir must be provided together", file=sys.stderr)
        return 2

    if not os.environ.get("CARGO_REGISTRY_TOKEN", "").strip():
        print("CARGO_REGISTRY_TOKEN is required", file=sys.stderr)
        return 2

    plan = load_plan_module()
    crates = plan.load_workspace()
    order = plan.topological_publish_order(crates)
    if len(order) != 15:
        print(f"refusing unexpected publish-set size: {len(order)}", file=sys.stderr)
        return 2

    expected_checksums: dict[str, str] = {}
    if args.release_record is not None and args.artifacts_dir is not None:
        expected_checksums = release_record_checksums(args.release_record, args.artifacts_dir)
        if set(expected_checksums) != set(order):
            print(
                "release record crates do not match the complete publication plan",
                file=sys.stderr,
            )
            return 2

    check = subprocess.run(
        [sys.executable, str(PLAN_SCRIPT), "check"],
        cwd=ROOT,
        check=False,
    )
    if check.returncode != 0:
        return check.returncode

    print(f"Publishing {len(order)} crates in dependency order:")
    for index, name in enumerate(order, start=1):
        print(f"  {index:02d}. {name}")

    for name in order:
        publish_one(
            name,
            timeout_seconds=args.index_timeout,
            expected_checksum=expected_checksums.get(name),
        )

    print(f"crates.io publication complete: {len(order)} crates at {VERSION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
