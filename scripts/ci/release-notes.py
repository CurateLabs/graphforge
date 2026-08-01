#!/usr/bin/env python3
"""Extract exact release notes from the canonical CHANGELOG section."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
CHANGELOG = ROOT / "CHANGELOG.md"
DOCS_CHANGELOG = ROOT / "docs" / "reference" / "changelog.md"


def extract(text: str, version: str) -> str:
    match = re.search(
        rf"(?ms)^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}\s*\n"
        r"(?P<body>.*?)(?=^## \[|^\[[^\n]+\]:|\Z)",
        text,
    )
    if match is None:
        raise ValueError(f"CHANGELOG lacks a dated [{version}] section")
    body = match.group("body").strip()
    if not body:
        raise ValueError(f"CHANGELOG [{version}] section is empty")
    return body + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    canonical = CHANGELOG.read_text(encoding="utf-8")
    if DOCS_CHANGELOG.read_text(encoding="utf-8") != canonical:
        print("release-notes: docs changelog does not mirror CHANGELOG.md", file=sys.stderr)
        return 1
    try:
        notes = extract(canonical, args.version)
    except ValueError as error:
        print(f"release-notes: {error}", file=sys.stderr)
        return 1
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(notes, encoding="utf-8")
        print(f"release-notes: wrote {args.out}")
    else:
        sys.stdout.write(notes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
