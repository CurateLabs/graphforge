#!/usr/bin/env python3
"""Keep ``docs/`` a documentation tree, not an evidence archive (#1625).

Three rules, all fail-closed:

1. **No evidence-shaped files.** A tracked file under ``docs/`` must be prose or
   an image, unless it lives in one of the structured-data directories that code
   or CI reads (contract schemas, discovery artifacts, release records) or is one
   of the generated schema inventories. Raw measurement output -- JSON, logs,
   receipts, patches, digests -- belongs on the issue or pull request that
   produced it, or on a release artifact; the repository keeps the method and
   the content digests, not the results.
2. **No oversize files.** Nothing under ``docs/`` may exceed ``MAX_FILE_BYTES``.
   The largest genuine page (the algorithms chapter) is well under the bound; a
   dump is not.
3. **No unreferenced development docs.** Every ``docs/development/*.md`` is named
   by at least one other tracked text file (another doc, the site allowlist,
   ``AGENTS.md``, ``CONTRIBUTING.md``, a Makefile, a script). A page nothing
   points at is either folded into its topic page or deleted.

Usage::

    python3 scripts/ci/docs-tree-policy.py check    # fail closed
    python3 scripts/ci/docs-tree-policy.py report   # print the census and exit 0
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]

DOCS = "docs"
DEVELOPMENT = "docs/development"
MAX_FILE_BYTES = 300_000

# Prose and images are documentation. Everything else needs an allowlisted home.
PROSE_SUFFIXES = frozenset({".md", ".mdx", ".txt"})
IMAGE_SUFFIXES = frozenset({".png", ".svg", ".jpg", ".jpeg", ".gif", ".webp"})
SITE_SUFFIXES = frozenset({".css"})

# Structured data that code or CI reads from under docs/. Anything added
# here must have a reader; evidence never does.
STRUCTURED_DATA_DIRECTORIES = (
    "docs/contracts",
    "docs/reference/discovery",
    "docs/reference/hub-publish",
    "docs/releases/records",
)
STRUCTURED_DATA_FILES = frozenset(
    {
        "docs/reference/epistemic-schema-inventory.json",
        "docs/reference/epistemic-schema-inventory.sha256",
        "docs/reference/knowledge-schema-inventory.json",
        "docs/reference/knowledge-schema-inventory.sha256",
    }
)

# Files whose bytes are searched for references to development docs.
REFERENCE_SUFFIXES = frozenset(
    {
        ".md",
        ".mdx",
        ".yml",
        ".yaml",
        ".py",
        ".mjs",
        ".js",
        ".ts",
        ".toml",
        ".rs",
        ".sh",
        ".json",
        ".txt",
    }
)
REFERENCE_NAMES = frozenset({"Makefile", "AGENTS.md", "CONTRIBUTING.md", "README.md"})


@dataclass
class Census:
    evidence_shaped: list[str] = field(default_factory=list)
    oversize: list[tuple[str, int]] = field(default_factory=list)
    unreferenced: list[str] = field(default_factory=list)
    development_docs: int = 0
    docs_files: int = 0

    @property
    def problems(self) -> list[str]:
        out = [
            f"{path}: evidence-shaped file under docs/; attach it to the issue or PR "
            "instead (see scripts/ci/docs-tree-policy.py)"
            for path in self.evidence_shaped
        ]
        out += [
            f"{path}: {size} bytes exceeds the docs/ bound of {MAX_FILE_BYTES}"
            for path, size in self.oversize
        ]
        out += [
            f"{path}: no other tracked file references it; link it from a topic page, "
            "fold it in, or delete it"
            for path in self.unreferenced
        ]
        return out


def tracked_files(root: Path) -> list[str]:
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=root, text=True)
    return [entry for entry in output.split("\0") if entry]


def _under(path: str, directory: str) -> bool:
    return path == directory or path.startswith(directory + "/")


def is_docs_file(path: str) -> bool:
    return _under(path, DOCS)


def is_allowed_shape(path: str) -> bool:
    """Whether a tracked docs/ file is prose, an image, or allowlisted data."""
    pure = PurePosixPath(path)
    suffix = pure.suffix.lower()
    if suffix in PROSE_SUFFIXES or suffix in IMAGE_SUFFIXES or suffix in SITE_SUFFIXES:
        return True
    if path in STRUCTURED_DATA_FILES:
        return True
    return any(_under(path, directory) for directory in STRUCTURED_DATA_DIRECTORIES)


def is_development_doc(path: str) -> bool:
    pure = PurePosixPath(path)
    return str(pure.parent) == DEVELOPMENT and pure.suffix == ".md"


def is_reference_source(path: str) -> bool:
    pure = PurePosixPath(path)
    return pure.suffix.lower() in REFERENCE_SUFFIXES or pure.name in REFERENCE_NAMES


def unreferenced_development_docs(root: Path, files: Iterable[str]) -> list[str]:
    """Development docs whose file name appears in no other tracked text file."""
    files = list(files)
    docs = [path for path in files if is_development_doc(path)]
    if not docs:
        return []
    names = {PurePosixPath(path).name.encode(): path for path in docs}
    referenced: set[str] = set()
    for source in files:
        if not is_reference_source(source):
            continue
        try:
            data = (root / source).read_bytes()
        except OSError:
            continue
        for name, path in names.items():
            if path == source or path in referenced:
                continue
            if name in data:
                referenced.add(path)
    return sorted(set(docs) - referenced)


def census(root: Path, files: Iterable[str] | None = None) -> Census:
    files = list(tracked_files(root) if files is None else files)
    result = Census()
    for path in files:
        if not is_docs_file(path):
            continue
        result.docs_files += 1
        if not is_allowed_shape(path):
            result.evidence_shaped.append(path)
        size = (root / path).stat().st_size
        if size > MAX_FILE_BYTES:
            result.oversize.append((path, size))
        if is_development_doc(path):
            result.development_docs += 1
    result.unreferenced = unreferenced_development_docs(root, files)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "report"))
    args = parser.parse_args(argv)
    result = census(ROOT)
    print(
        f"docs-tree-policy: {result.docs_files} tracked files under docs/, "
        f"{result.development_docs} development docs, "
        f"{len(result.unreferenced)} unreferenced, "
        f"{len(result.evidence_shaped)} evidence-shaped, {len(result.oversize)} oversize"
    )
    problems = result.problems
    for problem in problems:
        print(f"  - {problem}", file=sys.stderr if args.command == "check" else sys.stdout)
    if args.command == "check" and problems:
        print("docs-tree-policy: docs/ violates the evidence policy (#1625)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
