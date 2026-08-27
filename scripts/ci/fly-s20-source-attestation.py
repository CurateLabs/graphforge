#!/usr/bin/env python3
"""Derive the Fly S20 source snapshot identity from build-context bytes."""

from __future__ import annotations

import hashlib
from pathlib import Path
import stat
import subprocess
import sys

EXCLUDED_TOP_LEVEL = {
    ".git",
    ".venv",
    "target",
    "build",
    "dist",
    "node_modules",
    "fly-qualification-evidence.json",
}


def snapshot_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    files: list[tuple[Path, Path]] = []
    if (root / ".git").exists():
        tracked = subprocess.run(
            ["git", "ls-files", "-z", "--cached"],
            cwd=root,
            check=True,
            capture_output=True,
        ).stdout.split(b"\0")
        for encoded in tracked:
            if not encoded:
                continue
            relative = Path(encoded.decode("utf-8"))
            path = root / relative
            if not (path.is_symlink() or path.is_file()):
                raise RuntimeError(f"tracked source is missing: {relative.as_posix()}")
            files.append((relative, path))
    else:
        for path in root.rglob("*"):
            relative = path.relative_to(root)
            first = relative.parts[0]
            if first in EXCLUDED_TOP_LEVEL or first.startswith("bazel-"):
                continue
            if path.name.startswith(".env") or path.name.endswith((".env", ".pem", ".key")):
                continue
            if path.is_symlink() or path.is_file():
                files.append((relative, path))
    for relative, path in sorted(files):
        encoded = relative.as_posix().encode()
        executable = bool(path.lstat().st_mode & stat.S_IXUSR)
        content = path.readlink().as_posix().encode() if path.is_symlink() else path.read_bytes()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(b"x" if executable else b"-")
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return "sha256:" + digest.hexdigest()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: fly-s20-source-attestation.py ROOT")
    print(snapshot_sha256(Path(sys.argv[1]).resolve()))
