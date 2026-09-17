#!/usr/bin/env python3
"""Publish recorded npm tarballs with checksum-safe resumability.

npm assigns the ``latest`` dist-tag whenever ``npm publish`` is run without
``--tag``, regardless of whether the version is a semver prerelease. cargo and
PyPI exclude prereleases from default resolution on their own; npm does not, and
it fails open silently. Every publication here therefore carries an explicit
``--tag`` derived from the version being published, never from an opt-in flag:

- release versions (``0.6.0``)          -> ``latest``
- semver prereleases (``0.6.0-rc.1``)   -> ``next``
- anything that is not semver           -> refuse to publish

``next`` is the conventional npm prerelease channel, so one channel name covers
every prerelease flavour (``-rc``, ``-beta``, ``-dev``) and ``npm install
@curatelabs/graphforge@next`` resolves a coherent set across all eight packages.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
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
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_SCRIPT = ROOT / "scripts" / "ci" / "release-candidate.py"
sys.path.insert(0, str(ROOT / "scripts" / "ci"))
import release_action  # noqa: E402

REGISTRY = "https://registry.npmjs.org"
RELEASE_DIST_TAG = "latest"
PRERELEASE_DIST_TAG = "next"
# Official semver 2.0.0 grammar (semver.org); the prerelease group decides the tag.
SEMVER = re.compile(
    r"(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?"
)
USER_AGENT = "GraphForge npm publisher (github.com/CurateLabs/graphforge)"
GROUPS = {
    "native": slice(0, 6),
    "cli": slice(6, 7),
    "skills": slice(7, 8),
}


class DistTagError(ValueError):
    """The version cannot be classified, so no dist-tag may be assumed."""


def dist_tag_for(version: str) -> str:
    """Return the npm dist-tag for one version, refusing anything unclassifiable.

    Fails closed: a version that is not valid semver raises instead of silently
    inheriting npm's ``latest`` default.
    """
    if not isinstance(version, str):
        raise DistTagError(f"version must be a string, got {type(version).__name__}")
    candidate = version.strip()
    match = SEMVER.fullmatch(candidate)
    if match is None:
        raise DistTagError(
            f"cannot classify version {version!r} as a release or a prerelease; "
            "refusing to let npm default it to 'latest'"
        )
    return PRERELEASE_DIST_TAG if match.group("prerelease") else RELEASE_DIST_TAG


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


def publish_archive(path: Path, dist_tag: str) -> None:
    """Publish one retained tarball via npm trusted publishing (OIDC) + provenance."""
    subprocess.run(
        [
            "npm",
            "publish",
            str(path),
            "--access",
            "public",
            "--provenance",
            "--tag",
            dist_tag,
        ],
        cwd=ROOT,
        check=True,
        env=os.environ.copy(),
    )


def publish_one(item: dict[str, Any], artifacts_dir: Path, dist_tag: str) -> str:
    name = item["name"]
    version = item["version"]
    expected = item["sha256"]
    path = artifacts_dir / item["path"]
    # The tarball's own version decides the tag; a manifest entry that disagrees
    # with the release version is a partition defect, not a publishable state.
    if dist_tag_for(version) != dist_tag:
        raise RuntimeError(
            f"refusing to publish {name}@{version} under dist-tag {dist_tag!r}: "
            f"its version derives {dist_tag_for(version)!r}"
        )
    existing = published_integrity(name, version)
    if existing is not None:
        if not archive_matches_integrity(path, existing):
            raise RuntimeError(
                f"refusing to resume {name}@{version}: registry integrity differs "
                f"from candidate sha256 {expected}"
            )
        outcome = "already published; integrity matches"
    else:
        publish_archive(path, dist_tag)
        outcome = "accepted; public verification required"
    print(f"{name}@{version} (dist-tag {dist_tag}): {outcome}")
    return outcome


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-record", type=Path)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--expected-sha")
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--dist-tag",
        help="Assert the dist-tag derived from --version; it never overrides the derivation.",
    )
    parser.add_argument(
        "--print-dist-tag",
        action="store_true",
        help="Print the dist-tag derived from --version and exit without publishing.",
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--group", choices=tuple(GROUPS))
    selection.add_argument("--package")
    args = parser.parse_args(argv)

    try:
        dist_tag = dist_tag_for(args.version)
    except DistTagError as error:
        print(f"refusing to publish: {error}", file=sys.stderr)
        return 2
    if args.dist_tag is not None and args.dist_tag != dist_tag:
        print(
            f"refusing to publish: --dist-tag {args.dist_tag!r} contradicts dist-tag "
            f"{dist_tag!r} derived from version {args.version!r}",
            file=sys.stderr,
        )
        return 2
    if args.print_dist_tag:
        print(dist_tag)
        return 0

    missing = [
        flag
        for flag, value in (
            ("--release-record", args.release_record),
            ("--artifacts-dir", args.artifacts_dir),
            ("--expected-sha", args.expected_sha),
        )
        if value is None
    ]
    if missing:
        print(f"publication requires {', '.join(missing)}", file=sys.stderr)
        return 2
    if args.group is None and args.package is None:
        print("publication requires one of --group or --package", file=sys.stderr)
        return 2

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
        publish_one(by_name[name], args.artifacts_dir, dist_tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
