#!/usr/bin/env python3
"""Record checksums and a contents inventory for release-candidate artifacts.

Builds a JSON record suitable for the M1 release close (#192), the GitHub
Release outcome (#194), and post-release clean-env checksum matching (#167).
Does not create the GitHub Release.

Usage:
    python3 scripts/record_release_artifacts.py \\
      --dist-dir dist \\
      --version 0.5.0 \\
      --out docs/releases/records/v0.5.0-artifacts.json

    # Or record npm pack / cargo package outputs already on disk:
    python3 scripts/record_release_artifacts.py --dist-dir target/release-artifacts --version 0.5.0
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RELEASE_RECORD_SCHEMA = "graphforge-release-record-v1"


def _git_sha() -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".whl"):
        return "python-wheel"
    if name.endswith(".tar.gz") and "graphforge" in name and "gf-" not in name:
        return "python-sdist"
    if name.endswith(".crate"):
        return "rust-crate"
    if name.endswith(".tgz") or (name.endswith(".tar.gz") and "graphforge" in name):
        return "npm-tarball"
    if name.endswith(".node"):
        return "node-addon"
    if "sbom" in name or name.endswith(".spdx.json") or name.endswith(".cdx.json"):
        return "sbom"
    if "provenance" in name:
        return "provenance"
    return "other"


def artifact_identity(path: Path, artifact_class: str, version: str) -> tuple[str, str]:
    """Return the public registry surface and package name for an artifact."""
    if artifact_class in {"python-wheel", "python-sdist"}:
        return "pypi", "graphforge"
    if artifact_class == "npm-tarball":
        suffix = f"-{version}.tgz"
        normalized = path.name
        if normalized.startswith("graphforge-") and normalized.endswith(suffix):
            package = normalized[len("graphforge-") : -len(suffix)]
            return "npm", f"@graphforge/{package}"
        return "npm", path.stem
    return "github", path.name


def scan_dist(dist_dir: Path, version: str) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    if not dist_dir.exists():
        return artifacts
    for path in sorted(dist_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        rel = str(path.relative_to(dist_dir))
        artifact_class = classify(path)
        surface, name = artifact_identity(path, artifact_class, version)
        artifacts.append(
            {
                "path": rel,
                "class": artifact_class,
                "surface": surface,
                "name": name,
                "version": version,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return artifacts


def build_record(
    *,
    version: str,
    dist_dir: Path,
    notes: str | None,
) -> dict[str, Any]:
    artifacts = scan_dist(dist_dir, version)
    commit_sha = _git_sha()
    return {
        "schema": RELEASE_RECORD_SCHEMA,
        "version": version,
        "tag": f"v{version}",
        "commit_sha": commit_sha,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "dist_dir": str(dist_dir),
        "same_tagged_commit_policy": (
            "Every first-party publishable artifact for this version must be built "
            f"from commit_sha {commit_sha} (the eventual v{version} tag target) "
            "or have an explicit "
            "reproducible link recorded in notes/links."
        ),
        "licenses": {
            "first_party_spdx": "Apache-2.0",
            "license_files": ["LICENSE", "NOTICE"],
            "third_party_notices": "legal/THIRD_PARTY_NOTICES.md",
            "related_issues": ["#218", "#200"],
        },
        "artifacts": artifacts,
        "contents_summary": {
            "counts_by_class": _counts(artifacts),
            "total_artifacts": len(artifacts),
        },
        "sbom_provenance": {
            "configured": any(item["class"] in {"sbom", "provenance"} for item in artifacts),
            "note": (
                "Attach workflow-produced SBOM/provenance here when the release "
                "process emits them; leave empty list when not configured."
            ),
        },
        "notes": notes or "",
        "links": {
            "publishing": "docs/engineering/PUBLISHING.md",
            "third_party": "legal/THIRD_PARTY_NOTICES.md",
            "parent_tracker": "#192",
            "execution_tracker": "#194",
        },
    }


def _counts(artifacts: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in artifacts:
        counts[item["class"]] = counts.get(item["class"], 0) + 1
    return dict(sorted(counts.items()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="Release version, e.g. 0.5.0")
    parser.add_argument(
        "--dist-dir",
        type=Path,
        required=True,
        help="Directory of built artifacts to hash",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Write JSON record (default: stdout)",
    )
    parser.add_argument("--notes", default="", help="Free-form lineage notes")
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Permit writing a template record when dist-dir has no files yet",
    )
    args = parser.parse_args(argv)

    dist_dir = args.dist_dir if args.dist_dir.is_absolute() else ROOT / args.dist_dir
    record = build_record(version=args.version, dist_dir=dist_dir, notes=args.notes)
    if not record["artifacts"] and not args.allow_empty:
        print(
            "record-release-artifacts: no files under dist-dir "
            f"{dist_dir} (pass --allow-empty for a template)",
            file=sys.stderr,
        )
        return 1
    text = json.dumps(record, indent=2) + "\n"
    if args.out:
        out = args.out if args.out.is_absolute() else ROOT / args.out
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        print(f"record-release-artifacts: wrote {out}")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
