"""Console entry point for the Rust-owned GraphForge CLI."""

from __future__ import annotations

from collections.abc import Sequence
import sys

from graphforge._graphforge_rs import _cli_execute


def main(argv: Sequence[str] | None = None) -> int:
    """Run the native CLI and preserve its output and exit status exactly."""
    arguments = list(sys.argv if argv is None else argv)
    exit_code, stdout, stderr = _cli_execute(arguments)
    if stdout:
        sys.stdout.buffer.write(stdout)
        sys.stdout.buffer.flush()
    if stderr:
        sys.stderr.buffer.write(stderr)
        sys.stderr.buffer.flush()
    return exit_code
