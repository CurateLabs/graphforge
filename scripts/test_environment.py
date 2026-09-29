#!/usr/bin/env python3
"""Select and check temporary storage before running durable GraphForge tests.

This is an environment prerequisite check, not a substitute for Rust filesystem
admission. GF_TEST_TMPDIR is an explicit override; ambient TMPDIR is not used.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
# Keep fixtures outside the checkout: a temporary non-repository must not
# discover the enclosing worktree's .git directory or shared build cache.
DEFAULT_TEMP_ROOT = ROOT.parent / f".{ROOT.name}-test-tmp"
TEMP_VARIABLES = ("TMPDIR", "TMP", "TEMP")


def prepare(root: Path) -> Path:
    """Require writable native storage, rejecting Linux mount-boundary ancestors."""
    root = root.absolute()
    if any(character in str(root) for character in "\r\n"):
        raise ValueError("test temporary root must not contain newlines")
    root.mkdir(parents=True, exist_ok=True)
    root = root.resolve()
    if sys.platform == "linux":
        filesystem = subprocess.check_output(
            ["findmnt", "--noheadings", "--output", "FSTYPE", "--target", str(root)],
            text=True,
        ).strip()
        if filesystem not in {"ext4", "xfs", "btrfs"}:
            raise ValueError(
                f"durable test root {root} uses {filesystem}; set GF_TEST_TMPDIR to "
                "ext4/xfs/btrfs storage on the process-root volume"
            )
        device = root.stat().st_dev
        if any(parent.stat().st_dev != device for parent in root.parents):
            raise ValueError(
                f"durable test root {root} crosses a volume boundary; set GF_TEST_TMPDIR "
                "to native storage on the process-root volume"
            )
    # Exercise actual create/write/link/unlink permissions before launching tests.
    with tempfile.TemporaryDirectory(prefix="preflight-", dir=root) as raw:
        probe = Path(raw)
        source = probe / "source"
        source.write_bytes(b"graphforge test environment\n")
        os.link(source, probe / "linked")
    return root


def environment(root: Path) -> dict[str, str]:
    """Propagate one checked temporary root to Rust, Python and Node processes."""
    return dict(os.environ, **{name: str(root) for name in TEMP_VARIABLES})


def command_with_environment(command: list[str], root: Path) -> list[str]:
    """Bazel isolates test environments and needs explicit writable storage."""
    if len(command) >= 2 and Path(command[0]).name in {"bazel", "bazelisk"}:
        if command[1] == "test":
            return (
                command[:2]
                + [f"--test_env={name}={root}" for name in TEMP_VARIABLES]
                + [f"--test_tmpdir={root / 'bazel'}", f"--sandbox_writable_path={root}"]
                + command[2:]
            )
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--github-env", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        root = prepare(args.root or Path(os.environ.get("GF_TEST_TMPDIR", DEFAULT_TEMP_ROOT)))
        print(f"GraphForge test temporary root: {root}", file=sys.stderr)
        if args.github_env:
            with args.github_env.open("a", encoding="utf-8") as output:
                for name in TEMP_VARIABLES:
                    output.write(f"{name}={root}\n")
        command = args.command
        if command[:1] == ["--"]:
            command = command[1:]
        if command:
            return subprocess.run(
                command_with_environment(command, root), env=environment(root), check=False
            ).returncode
        return 0
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"GraphForge test filesystem preflight failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
