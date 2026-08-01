#!/usr/bin/env python3
"""List a crates node and its crates dependencies for pre-authorize refresh."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--node", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"crate-authorize-refresh-nodes: {error}", file=sys.stderr)
        return 1
    if not args.node.startswith("crates:"):
        print(
            f"crate-authorize-refresh-nodes: node must be a crates node: {args.node}",
            file=sys.stderr,
        )
        return 1
    edges = manifest.get("dependencies")
    if not isinstance(edges, list):
        print("crate-authorize-refresh-nodes: candidate dependencies are invalid", file=sys.stderr)
        return 1
    deps = sorted(
        {
            edge["requires"]
            for edge in edges
            if isinstance(edge, dict)
            and edge.get("from") == args.node
            and isinstance(edge.get("requires"), str)
            and edge["requires"].startswith("crates:")
        }
    )
    for item in [args.node, *deps]:
        print(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
