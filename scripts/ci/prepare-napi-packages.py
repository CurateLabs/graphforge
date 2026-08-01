#!/usr/bin/env python3
"""Add the required legal inventory to generated native npm packages."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil

LEGAL_FILES = ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md")


def prepare(npm_dir: Path, legal_dir: Path) -> None:
    package_dirs = sorted(path.parent for path in npm_dir.glob("*/package.json"))
    if not package_dirs:
        raise ValueError(f"no generated npm packages under {npm_dir}")
    for source_name in LEGAL_FILES:
        if not (legal_dir / source_name).is_file():
            raise ValueError(f"legal source is missing: {legal_dir / source_name}")
    for package_dir in package_dirs:
        manifest_path = package_dir / "package.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        files = manifest.get("files")
        if not isinstance(files, list):
            raise ValueError(f"{manifest_path} files must be an array")
        for source_name in LEGAL_FILES:
            shutil.copyfile(legal_dir / source_name, package_dir / source_name)
            if source_name not in files:
                files.append(source_name)
        manifest["files"] = files
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--npm-dir", type=Path, required=True)
    parser.add_argument("--legal-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        prepare(args.npm_dir, args.legal_dir)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"prepare-napi-packages: {error}") from error


if __name__ == "__main__":
    main()
