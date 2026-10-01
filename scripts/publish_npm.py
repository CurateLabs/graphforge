#!/usr/bin/env python3
"""Publish the packed GraphForge npm tarballs in dependency order.

The native packages go first, then the main package that depends on them, then
the CLI and the agent skills. Re-running is safe: a package whose version is
already on the registry is skipped. Publishing uses npm trusted publishing
(OIDC) with provenance, so no token is read here.

npm assigns the ``latest`` dist-tag whenever ``npm publish`` runs without
``--tag``, so the tag is always passed and always derived from the version: a
prerelease must never become ``latest``.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = "https://registry.npmjs.org"
USER_AGENT = "GraphForge npm publisher (github.com/CurateLabs/graphforge)"
MAIN = "@curatelabs/graphforge"
# Published after every native package, in this order.
DEPENDENTS = (MAIN, "@curatelabs/graphforge-cli", "@curatelabs/graphforge-agent-skills")
RELEASE_DIST_TAG = "latest"
PRERELEASE_DIST_TAG = "next"
# Official semver 2.0.0 grammar (semver.org); the prerelease group decides the tag.
SEMVER = re.compile(
    r"(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+(?P<build>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?"
)


def workspace_version() -> str:
    match = re.search(
        r'(?ms)^\[workspace\.package\].*?^version\s*=\s*"([^"]+)"',
        (ROOT / "Cargo.toml").read_text(encoding="utf-8"),
    )
    if match is None:
        raise RuntimeError("Cargo.toml lacks [workspace.package] version")
    return match.group(1)


def dist_tag_for(version: str) -> str:
    """Return the npm dist-tag for a version, refusing anything unclassifiable."""
    match = SEMVER.fullmatch(version.strip())
    if match is None:
        raise ValueError(
            f"cannot classify version {version!r} as a release or a prerelease; "
            "refusing to let npm default it to 'latest'"
        )
    return PRERELEASE_DIST_TAG if match.group("prerelease") else RELEASE_DIST_TAG


def manifest(tarball: Path) -> tuple[str, str]:
    """Return the (name, version) recorded inside a packed tarball."""
    with tarfile.open(tarball, "r:gz") as archive:
        member = archive.extractfile("package/package.json")
        if member is None:
            raise RuntimeError(f"{tarball.name} has no package/package.json")
        data = json.load(member)
    return data["name"], data["version"]


def publish_order(directory: Path) -> list[tuple[Path, str, str]]:
    """Return every tarball as (path, name, version): natives first, then dependents."""
    packages = {}
    for tarball in sorted(directory.glob("*.tgz")):
        name, version = manifest(tarball)
        if name in packages:
            raise RuntimeError(f"two tarballs claim {name}")
        packages[name] = (tarball, name, version)
    missing = [name for name in DEPENDENTS if name not in packages]
    if missing:
        raise RuntimeError(f"missing packed packages: {missing}")
    natives = sorted(name for name in packages if name not in DEPENDENTS)
    if not natives or any(not name.startswith(f"{MAIN}-") for name in natives):
        raise RuntimeError(f"unexpected native package set: {natives}")
    return [packages[name] for name in (*natives, *DEPENDENTS)]


def is_published(name: str, version: str) -> bool:
    url = f"{REGISTRY}/{urllib.parse.quote(name, safe='')}/{version}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30):
            return True
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise


def publish(directory: Path, dist_tag: str) -> None:
    version = workspace_version()
    if dist_tag != dist_tag_for(version):
        raise RuntimeError(
            f"dist-tag {dist_tag!r} contradicts {dist_tag_for(version)!r} "
            f"derived from version {version!r}"
        )
    order = publish_order(directory)
    for tarball, _, packed_version in order:
        if packed_version != version:
            raise RuntimeError(f"{tarball.name} is {packed_version}, expected {version}")
    for tarball, name, _ in order:
        if is_published(name, version):
            print(f"{name}@{version}: already published, skipping", flush=True)
            continue
        command = [
            "npm",
            "publish",
            str(tarball),
            "--access",
            "public",
            "--provenance",
            "--tag",
            dist_tag,
        ]
        print("+ " + " ".join(command), flush=True)
        subprocess.run(command, cwd=ROOT, check=True, env=os.environ.copy())
        print(f"{name}@{version}: published under dist-tag {dist_tag}", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path, help="Directory of packed .tgz files")
    parser.add_argument("--dist-tag", help="Must equal the tag derived from the version")
    parser.add_argument("--list", action="store_true", help="Print the publish order and exit")
    parser.add_argument("--print-version", action="store_true")
    parser.add_argument("--print-dist-tag", action="store_true")
    args = parser.parse_args(argv)

    try:
        if args.print_version:
            print(workspace_version())
            return 0
        if args.print_dist_tag:
            print(dist_tag_for(workspace_version()))
            return 0
        if args.directory is None:
            parser.error("a directory of packed tarballs is required")
        if args.list:
            for tarball, name, version in publish_order(args.directory):
                print(f"{name}@{version}  {tarball.name}")
            return 0
        if args.dist_tag is None:
            parser.error("--dist-tag is required to publish")
        publish(args.directory, args.dist_tag)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"publish_npm: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
