#!/usr/bin/env python3
"""Publish recorded npm tarballs with checksum-safe resumability."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_SCRIPT = ROOT / "scripts" / "ci" / "release-candidate.py"
sys.path.insert(0, str(ROOT / "scripts" / "ci"))
import release_action  # noqa: E402

REGISTRY = "https://registry.npmjs.org"
USER_AGENT = "GraphForge npm publisher (github.com/CurateLabs/graphforge)"
GROUPS = {
    "native": slice(0, 6),
    "cli": slice(6, 7),
    "skills": slice(7, 8),
}


def load_candidate_module():
    spec = importlib.util.spec_from_file_location("release_candidate", CANDIDATE_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {CANDIDATE_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _json(url: str) -> dict[str, Any] | None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise
    if not isinstance(payload, dict):
        raise RuntimeError(f"npm returned a non-object response for {url}")
    return payload


def published_integrity(name: str, version: str) -> str | None:
    encoded = urllib.parse.quote(name, safe="")
    record = _json(f"{REGISTRY}/{encoded}/{version}")
    if record is None:
        return None
    integrity = record.get("dist", {}).get("integrity")
    if not isinstance(integrity, str) or not integrity.strip():
        raise RuntimeError(f"npm {name}@{version} lacks dist.integrity")
    return integrity


def archive_matches_integrity(path: Path, integrity: str) -> bool:
    """Compare an archive to one or more npm Subresource Integrity digests."""
    data = path.read_bytes()
    supported = False
    for token in integrity.split():
        algorithm, separator, encoded = token.partition("-")
        if not separator or algorithm not in {"sha256", "sha384", "sha512"}:
            continue
        supported = True
        try:
            expected = base64.b64decode(encoded, validate=True)
        except ValueError as error:
            raise RuntimeError(f"npm returned malformed {algorithm} integrity") from error
        if hashlib.new(algorithm, data).digest() == expected:
            return True
    if not supported:
        raise RuntimeError("npm returned no supported integrity digest")
    return False


def publish_archive(path: Path) -> None:
    """Publish one retained tarball via npm trusted publishing (OIDC) + provenance."""
    subprocess.run(
        ["npm", "publish", str(path), "--access", "public", "--provenance"],
        cwd=ROOT,
        check=True,
        env=os.environ.copy(),
    )


def publish_one(item: dict[str, Any], artifacts_dir: Path) -> str:
    name = item["name"]
    version = item["version"]
    expected = item["sha256"]
    path = artifacts_dir / item["path"]
    existing = published_integrity(name, version)
    if existing is not None:
        if not archive_matches_integrity(path, existing):
            raise RuntimeError(
                f"refusing to resume {name}@{version}: registry integrity differs "
                f"from candidate sha256 {expected}"
            )
        outcome = "already published; integrity matches"
    else:
        publish_archive(path)
        outcome = "accepted; public verification required"
    print(f"{name}@{version}: {outcome}")
    return outcome


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-record", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--version", required=True)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--group", choices=tuple(GROUPS))
    selection.add_argument("--package")
    args = parser.parse_args(argv)

    candidate = load_candidate_module()
    try:
        record = json.loads(args.release_record.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"cannot read candidate manifest: {error}", file=sys.stderr)
        return 2
    release_action.validate_partition(
        record,
        args.artifacts_dir,
        "npm",
        expected_sha=args.expected_sha,
        version=args.version,
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
    by_name = {item["name"]: item for item in record["artifacts"] if item.get("surface") == "npm"}
    names = candidate.NPM_PACKAGES[GROUPS[args.group]] if args.group else (args.package,)
    if any(name not in candidate.NPM_PACKAGES for name in names):
        print("requested npm package is outside the candidate", file=sys.stderr)
        return 2
    for name in names:
        publish_one(by_name[name], args.artifacts_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
