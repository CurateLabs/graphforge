#!/usr/bin/env python3
"""Download and verify the Zachary karate-club archive used by visualization examples.

Raw archives stay under a local cache directory (gitignored). Only MANIFEST.json
is tracked in git.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import urllib.request
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
MANIFEST_PATH = HERE / "MANIFEST.json"
DEFAULT_CACHE = ROOT / ".cache"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def fetch_dataset(cache_dir: Path | None = None, *, force: bool = False) -> Path:
    """Return the extracted dataset directory after checksum verification."""
    manifest = load_manifest()
    cache = cache_dir or DEFAULT_CACHE
    cache.mkdir(parents=True, exist_ok=True)

    archive_name = manifest["archive"]["filename"]
    archive_path = cache / archive_name
    extract_dir = cache / "karate"
    expected_archive = manifest["archive"]["sha256"]

    if force or not archive_path.is_file() or _sha256(archive_path) != expected_archive:
        url = manifest["source_url"]
        print(f"Downloading {url} -> {archive_path}", file=sys.stderr)
        urllib.request.urlretrieve(url, archive_path)

    actual = _sha256(archive_path)
    if actual != expected_archive:
        raise SystemExit(
            f"Archive checksum mismatch for {archive_path}: expected {expected_archive}, got {actual}"
        )

    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(extract_dir)

    members = manifest["archive"]["members"]
    for relative, expected in members.items():
        member_path = extract_dir / relative
        if not member_path.is_file():
            raise SystemExit(f"Missing archive member after extract: {relative}")
        actual_member = _sha256(member_path)
        if actual_member != expected:
            raise SystemExit(
                f"Member checksum mismatch for {relative}: expected {expected}, got {actual_member}"
            )

    return extract_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE,
        help="Directory for the downloaded archive (default: examples/visualization/.cache)",
    )
    parser.add_argument("--force", action="store_true", help="Re-download even if cached")
    args = parser.parse_args()
    path = fetch_dataset(args.cache_dir, force=args.force)
    print(path)


if __name__ == "__main__":
    main()
