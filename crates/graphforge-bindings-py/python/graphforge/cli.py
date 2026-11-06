"""Console entry point for the Rust-owned GraphForge CLI."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
import sys

from graphforge._graphforge_rs import _cli_execute_process


def main(argv: Sequence[str] | None = None) -> int:
    """Run the native CLI and preserve its output and exit status exactly."""
    arguments = list(sys.argv if argv is None else argv)
    packaged_skills = Path(__file__).resolve().parent / "_project_skills"
    arguments[1:1] = ["--skills-bundle-dir", str(packaged_skills)]
    return _cli_execute_process(arguments)
