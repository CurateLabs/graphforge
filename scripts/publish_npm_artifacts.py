#!/usr/bin/env python3
"""Publish recorded npm tarballs with checksum-safe resumability."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_SCRIPT = ROOT / "scripts" / "ci" / "release-candidate.py"
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


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def published_checksum(name: str, version: str) -> str | None:
    encoded = urllib.parse.quote(name, safe="")
    record = _json(f"{REGISTRY}/{encoded}/{version}")
    if record is None:
        return None
    tarball = record.get("dist", {}).get("tarball")
    if not isinstance(tarball, str) or not tarball.startswith("https://registry.npmjs.org/"):
        raise RuntimeError(f"npm {name}@{version} lacks a trusted registry tarball URL")
    request = urllib.request.Request(tarball, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return _sha256_bytes(response.read())


def publish_archive(path: Path) -> None:
    subprocess.run(
        ["npm", "publish", str(path), "--access", "public"],
        cwd=ROOT,
        check=True,
        env=os.environ.copy(),
    )


def publish_one(item: dict[str, Any], artifacts_dir: Path, timeout_seconds: int) -> str:
    name = item["name"]
    version = item["version"]
    expected = item["sha256"]
    path = artifacts_dir / item["path"]
    existing = published_checksum(name, version)
    if existing is not None:
        if existing != expected:
            raise RuntimeError(
                f"refusing to resume {name}@{version}: registry checksum {existing} "
                f"differs from candidate {expected}"
            )
        outcome = "already published; checksum matches"
    else:
        publish_archive(path)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            existing = published_checksum(name, version)
            if existing is not None:
                break
            time.sleep(3)
        if existing is None:
            raise RuntimeError(f"timed out waiting for npm to index {name}@{version}")
        if existing != expected:
            raise RuntimeError(
                f"npm indexed different bytes for {name}@{version}: "
                f"registry={existing} candidate={expected}"
            )
        outcome = "published"
    print(f"{name}@{version}: {outcome}")
    return outcome


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-record", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--group", choices=tuple(GROUPS), required=True)
    parser.add_argument("--index-timeout", type=int, default=300)
    args = parser.parse_args(argv)
    if not os.environ.get("NODE_AUTH_TOKEN", "").strip():
        print("NODE_AUTH_TOKEN is required", file=sys.stderr)
        return 2

    candidate = load_candidate_module()
    record = candidate.validate(
        args.release_record,
        args.artifacts_dir,
        args.expected_sha,
        args.version,
    )
    by_name = {item["name"]: item for item in record["artifacts"] if item.get("surface") == "npm"}
    names = candidate.NPM_PACKAGES[GROUPS[args.group]]
    for name in names:
        publish_one(by_name[name], args.artifacts_dir, args.index_timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
