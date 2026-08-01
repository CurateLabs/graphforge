#!/usr/bin/env python3
"""Publish the complete GraphForge crates.io surface in dependency order.

The command is resumable without permitting different bytes under an existing
version. Before each registry write it packages the crate locally and computes
the archive checksum. If that exact version already exists, publication only
continues when crates.io reports the same checksum.

Requires ``CARGO_REGISTRY_TOKEN`` in the environment. The maintained release
credential is projected from Pulumi ESC into the GitHub Actions secret used by
``publish.yaml``.

The token is normalized before ``cargo publish``: leading/trailing whitespace
and CR/LF are stripped. The value is never logged.
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


def normalize_registry_token(raw: str) -> str:
    """Return a cargo-safe registry token, or raise ValueError without echoing it.

    Cargo rejects tokens with non-printable / non-ISO-8859-1 characters. Secret
    projection and pasted GitHub secrets commonly introduce a trailing newline.
    """
    token = raw.strip()
    if not token:
        raise ValueError("CARGO_REGISTRY_TOKEN is empty after trim")
    # Printable ISO-8859-1 only: 0x20-0x7E and 0xA0-0xFF (not C0/C1/DEL).
    if any(not (0x20 <= ord(ch) <= 0x7E or 0xA0 <= ord(ch) <= 0xFF) for ch in token):
        raise ValueError("CARGO_REGISTRY_TOKEN contains non-printable or non-ISO-8859-1 characters")
    return token


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


def publish_one(
    name: str,
    *,
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
        owners = owner_logins(name)
        if "DecisionNerd" not in owners:
            raise RuntimeError(
                f"{name} {VERSION} is indexed but DecisionNerd is not an owner: {sorted(owners)}"
            )
        outcome = "already published; checksum and owner match"
    else:
        run(["cargo", "publish", "-p", name, "--locked"])
        outcome = "accepted; public checksum and owner verification required"

    print(f"{name} {VERSION}: {outcome}")
    return outcome


def publish_authorized(name: str, expected_checksum: str) -> str:
    """Execute one planner-authorized absent-node write without reclassification."""
    package_checksum(name, expected_checksum)
    run(["cargo", "publish", "-p", name, "--locked"])
    outcome = "accepted; public checksum and owner verification required"
    print(f"{name} {VERSION}: {outcome}")
    return outcome


def release_record_checksums(record_path: Path, artifacts_dir: Path) -> dict[str, str]:
    try:
        record = json.loads(record_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read release record {record_path}: {error}") from error
    if record.get("schema") not in {
        "graphforge-release-record-v1",
        "graphforge-release-candidate-v2",
    }:
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
    parser.add_argument("--crate", required=True, help="One planner-authorized crate name")
    parser.add_argument(
        "--release-record",
        type=Path,
        help="Certified release record or candidate-manifest JSON",
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

    try:
        os.environ["CARGO_REGISTRY_TOKEN"] = normalize_registry_token(
            os.environ.get("CARGO_REGISTRY_TOKEN", "")
        )
    except ValueError as error:
        print(str(error), file=sys.stderr)
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

    if args.crate not in order:
        print(f"requested crate is outside the publication plan: {args.crate}", file=sys.stderr)
        return 2
    expected = expected_checksums.get(args.crate)
    if expected is None:
        print(f"candidate checksum is missing for {args.crate}", file=sys.stderr)
        return 2
    publish_authorized(args.crate, expected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
