#!/usr/bin/env python3
"""Create the canonical manifest for exact, already-built release artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

from release_candidate_manifest import SCHEMA as RELEASE_RECORD_SCHEMA  # noqa: E402, F401
from release_candidate_manifest import (  # noqa: E402, F401
    artifact_identity,
    build_manifest,
    classify,
    scan_dist,
    sha256_file,
)


def _git_value(*args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout).strip())
    return result.stdout.strip()


def _git_sha() -> str:
    return _git_value("rev-parse", "--verify", "HEAD")


def _git_recorded_at() -> str:
    return _git_value("show", "-s", "--format=%cI", "HEAD")


def build_record(
    *,
    version: str,
    dist_dir: Path,
    notes: str | None,
    commit_sha: str | None = None,
    recorded_at: str | None = None,
) -> dict[str, object]:
    """Build a deterministic manifest; strict completeness is a separate validation step."""
    return build_manifest(
        version=version,
        dist_dir=dist_dir,
        commit_sha=commit_sha or _git_sha(),
        recorded_at=recorded_at or _git_recorded_at(),
        notes=notes or "",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="One root release version")
    parser.add_argument("--dist-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, help="Write JSON manifest (default: stdout)")
    parser.add_argument("--notes", default="", help="Non-sensitive candidate lineage notes")
    parser.add_argument(
        "--recorded-at",
        help="ISO-8601 retention start (default: source commit time)",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Permit a schema template with no artifacts; it cannot pass candidate validation",
    )
    args = parser.parse_args(argv)

    dist_dir = args.dist_dir if args.dist_dir.is_absolute() else ROOT / args.dist_dir
    record = build_record(
        version=args.version,
        dist_dir=dist_dir,
        notes=args.notes,
        recorded_at=args.recorded_at,
    )
    if not record["artifacts"] and not args.allow_empty:
        print(
            f"record-release-artifacts: no files under dist-dir {dist_dir}",
            file=sys.stderr,
        )
        return 1
    output = json.dumps(record, indent=2, sort_keys=True) + "\n"
    if args.out:
        out = args.out if args.out.is_absolute() else ROOT / args.out
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(output, encoding="utf-8")
        print(f"record-release-artifacts: wrote {out}")
    else:
        sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
