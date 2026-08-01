#!/usr/bin/env python3
"""Enforce ADR 0014 workspace dependency directions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

DOMAIN_CRATES = {"graphforge-provenance", "graphforge-knowledge"}
GRAPH_CRATES = {
    "graphforge-ast",
    "graphforge-core",
    "graphforge-cypher",
    "graphforge-exec",
    "graphforge-io",
    "graphforge-ir",
    "graphforge-ontology",
    "graphforge-plan",
    "graphforge-rel",
    "graphforge-search",
    "graphforge-storage",
}
THIN_ADAPTERS = {"graphforge-bindings-py", "graphforge-bindings-node", "graphforge-cli"}


def load_metadata(path: Path | None) -> dict[str, Any]:
    if path is not None:
        return json.loads(path.read_text(encoding="utf-8"))
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def violations(metadata: dict[str, Any]) -> list[str]:
    packages = {
        package["name"]: {dependency["name"] for dependency in package["dependencies"]}
        for package in metadata["packages"]
    }
    errors: list[str] = []

    for crate in sorted(GRAPH_CRATES & packages.keys()):
        for domain in sorted(DOMAIN_CRATES & packages[crate]):
            errors.append(f"{crate} must not depend on {domain}")

    for domain in sorted(DOMAIN_CRATES & packages.keys()):
        workspace_dependencies = packages[domain] & packages.keys()
        for dependency in sorted(workspace_dependencies - {"graphforge-core"}):
            errors.append(f"{domain} may depend on graphforge-core only, not {dependency}")

    for adapter in sorted(THIN_ADAPTERS & packages.keys()):
        for domain in sorted(DOMAIN_CRATES & packages[adapter]):
            errors.append(f"{adapter} must depend on graphforge-api, not directly on {domain}")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path)
    args = parser.parse_args()
    errors = violations(load_metadata(args.metadata))
    if errors:
        print("ADR 0014 dependency violations:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("ADR 0014 domain dependency directions passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
