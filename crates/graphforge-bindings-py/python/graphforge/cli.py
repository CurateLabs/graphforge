"""Console entry point for the Rust-owned GraphForge CLI."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
import signal
import sys

from graphforge._graphforge_rs import _cli_execute_process


def main(argv: Sequence[str] | None = None) -> int:
    """Run the native CLI and preserve its output and exit status exactly."""
    arguments = list(sys.argv if argv is None else argv)
    packaged_skills = Path(__file__).resolve().parent / "_project_skills"
    arguments[1:1] = ["--skills-bundle-dir", str(packaged_skills)]
    return _cli_execute_process(arguments)


def console() -> int:
    """Run an installed console with native Ctrl-C handling.

    Python defers its default interrupt until native execution returns. Console
    commands instead use the OS default while Rust runs. Inherited ignore and
    custom handlers remain intact; embedded callers continue to use main().
    """
    if signal.getsignal(signal.SIGINT) is not signal.default_int_handler:
        return main()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        return main()
    finally:
        signal.signal(signal.SIGINT, signal.default_int_handler)
