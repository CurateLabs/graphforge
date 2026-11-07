#!/usr/bin/env python3
"""Package a prebuilt gf executable and its legal notices for a GitHub Release."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parents[2]


def package(binary: Path, target: str, version: str, out: Path) -> Path:
    """Write one platform archive and its sha256sum-compatible checksum."""
    out.mkdir(parents=True, exist_ok=True)
    windows = target.endswith("windows-msvc")
    name = f"gf-{version}-{target}"
    files = [(binary, "gf.exe" if windows else "gf")]
    files.extend((ROOT / name, name) for name in ("LICENSE", "NOTICE"))
    files.append((ROOT / "crates/graphforge-cli/THIRD_PARTY_NOTICES.md", "THIRD_PARTY_NOTICES.md"))
    archive = out / (name + (".zip" if windows else ".tar.gz"))
    if windows:
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as output:
            for source, member in files:
                output.write(source, member)
    else:
        with tarfile.open(archive, "w:gz") as output:
            for source, member in files:
                output.add(source, arcname=member)
    with archive.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    archive.with_name(archive.name + ".sha256").write_text(
        f"{digest}  {archive.name}\n", encoding="utf-8"
    )
    return archive


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--target", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    print(package(args.binary, args.target, args.version, args.out))


if __name__ == "__main__":
    main()
