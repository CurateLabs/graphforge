#!/usr/bin/env python3
"""Classify whether changed paths can affect M6 durable walltime."""

from __future__ import annotations

from pathlib import PurePosixPath
import sys

RELEVANT_PREFIXES = (
    "crates/graphforge-core/",
    "crates/graphforge-filesystem/",
    "crates/graphforge-storage/",
    ".cargo/",
)
RELEVANT_FILES = {
    "Cargo.lock",
    "Cargo.toml",
    "rust-toolchain.toml",
}
KNOWN_IRRELEVANT_PREFIXES = (
    ".github/",
    "docs/",
    "docs-site/",
    "legal/",
    "packages/",
    "scripts/",
    "tests/",
    "tools/",
)


def requires_macro(path: str) -> bool:
    """Return true for relevant or unknown paths; unknowns fail closed."""
    path = path.strip().removeprefix("./")
    if not path:
        return False
    if path in RELEVANT_FILES or path.startswith(RELEVANT_PREFIXES):
        return True
    if path.startswith("crates/"):
        return False
    if path.startswith(KNOWN_IRRELEVANT_PREFIXES):
        return False
    if PurePosixPath(path).suffix.lower() == ".md":
        return False
    return True


def main() -> int:
    paths = [line.strip() for line in sys.stdin if line.strip()]
    print("true" if any(requires_macro(path) for path in paths) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
