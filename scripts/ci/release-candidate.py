#!/usr/bin/env python3
"""Validate and query the immutable partitioned release candidate."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from release_candidate_manifest import (  # noqa: F401
    CRATES,
    NPM_PACKAGES,
    SCHEMA,
    CandidateError,
    npm_paths,
    validate,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "npm-paths"))
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--as-of",
        help="ISO-8601 retention check time (default: current UTC time)",
    )
    args = parser.parse_args(argv)
    try:
        as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")) if args.as_of else None
        manifest = validate(
            args.record,
            args.artifacts_dir,
            args.expected_sha,
            args.version,
            as_of=as_of,
        )
        if args.command == "npm-paths":
            print("\n".join(npm_paths(manifest)))
        else:
            print(
                f"release-candidate: valid version={args.version} "
                f"sha={args.expected_sha} artifacts={len(manifest['artifacts'])} "
                f"nodes={len(manifest['nodes'])} groups=4"
            )
        return 0
    except (CandidateError, ValueError) as error:
        print(f"release-candidate: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
